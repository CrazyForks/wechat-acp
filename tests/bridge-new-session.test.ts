import assert from "node:assert/strict";
import { test } from "node:test";
import type { ContentBlock } from "@agentclientprotocol/sdk";

import { WeChatAcpBridge } from "../src/bridge.js";
import type { ResetSessionResult } from "../src/acp/session.js";
import { BRIDGE_COMMANDS, defaultConfig } from "../src/config.js";
import type { InjectedMessage } from "../src/inject/types.js";
import { MessageType, type WeixinMessage } from "../src/weixin/types.js";

class TestBridge extends WeChatAcpBridge {
  readonly enqueued: string[] = [];
  readonly buffered: Array<{ contextToken: string; prompt: ContentBlock[] }> = [];
  readonly resetUsers: string[] = [];
  readonly sent: Array<{ contextToken: string; segment: string }> = [];
  readonly events: string[] = [];
  private readonly promptGenerations = new Map<string, number>();
  enqueueGate: Promise<void> | undefined;
  sendBehavior: (
    contextToken: string,
    segment: string,
  ) => boolean | Promise<boolean> = () => true;
  resetResult: ResetSessionResult = {
    hadActiveSession: true,
    cancelledTurn: false,
    cancelledPendingCreation: false,
    droppedQueueCount: 0,
  };
  resetBehavior: (userId: string) => Promise<ResetSessionResult> = async () =>
    this.resetResult;
  injectionTargetBehavior: (
    job: InjectedMessage,
  ) => Promise<{ userId: string; contextToken: string }> = async (job) => ({
    userId: job.target,
    contextToken: job.contextToken ?? "injected-context",
  });

  protected override async enqueueMessage(
    _msg: WeixinMessage,
    _userId: string,
    contextToken: string,
    isCurrent: () => boolean = () => true,
    _replyGeneration?: number,
  ): Promise<void> {
    this.events.push(`enqueue-start:${contextToken}`);
    await this.enqueueGate;
    if (!isCurrent()) {
      this.events.push(`enqueue-discarded:${contextToken}`);
      return;
    }
    this.enqueued.push(contextToken);
    this.events.push(`enqueue-done:${contextToken}`);
  }

  protected override async resetUserSession(
    userId: string,
  ): Promise<ResetSessionResult> {
    this.events.push(`reset:${userId}`);
    this.resetUsers.push(userId);
    return this.resetBehavior(userId);
  }

  protected override async enqueueBufferedPrompt(
    _userId: string,
    contextToken: string,
    prompt: ContentBlock[],
    _replyGeneration?: number,
  ): Promise<void> {
    this.buffered.push({ contextToken, prompt });
  }

  protected override async sendTextSegment(
    _userId: string,
    contextToken: string,
    segment: string,
  ): Promise<boolean> {
    this.sent.push({ contextToken, segment });
    return this.sendBehavior(contextToken, segment);
  }

  protected override resolveInjectedTarget(
    job: InjectedMessage,
  ): Promise<{ userId: string; contextToken: string }> {
    return this.injectionTargetBehavior(job);
  }

  beginPrompt(contextToken: string): void {
    this.beginAgentPrompt("user", contextToken);
    this.promptGenerations.set(
      contextToken,
      this.messageGenerationForUser("user"),
    );
  }

  queueAgentReply(contextToken: string, text: string): Promise<void> {
    return this.sendAgentReply(
      "user",
      contextToken,
      text,
      this.promptGenerations.get(contextToken),
    );
  }
}

function textMessage(text: string, contextToken: string): WeixinMessage {
  return {
    from_user_id: "user",
    context_token: contextToken,
    message_type: MessageType.USER,
    item_list: [{ type: 1, text_item: { text } }],
  };
}

function makeBridge(): TestBridge {
  const config = defaultConfig();
  config.storage.stateFile = undefined;
  config.commandAliases = {
    [BRIDGE_COMMANDS.acpNew]: ["/acp-clear"],
  };
  return new TestBridge(config, () => {});
}

test("acp-new and its alias reset without enqueueing an ACP prompt", async () => {
  const bridge = makeBridge();

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-new"),
  );
  await bridge.handleMessage(textMessage("/acp-clear", "context-alias"));

  assert.deepEqual(bridge.enqueued, []);
  assert.deepEqual(bridge.resetUsers, ["user", "user"]);
  assert.equal(bridge.sent.length, 2);
  assert.ok(
    bridge.sent.every(({ segment }) =>
      segment.includes("ACP session cleared")
    ),
  );
});

test("acp-new rejects arguments without resetting the session", async () => {
  const bridge = makeBridge();

  await bridge.handleMessage(
    textMessage(`${BRIDGE_COMMANDS.acpNew} now`, "context-invalid"),
  );

  assert.deepEqual(bridge.resetUsers, []);
  assert.deepEqual(bridge.enqueued, []);
  assert.match(bridge.sent[0]!.segment, /Unknown argument/);
  assert.match(bridge.sent[0]!.segment, /Usage/);
});

test("acp-new preempts an older message still being prepared", async () => {
  const bridge = makeBridge();
  let releaseEnqueue!: () => void;
  bridge.enqueueGate = new Promise<void>((resolve) => {
    releaseEnqueue = resolve;
  });

  const prompt = bridge.handleMessage(
    textMessage("old prompt", "context-prompt"),
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  const reset = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  await new Promise<void>((resolve) => setImmediate(resolve));

  assert.deepEqual(bridge.resetUsers, ["user"]);
  releaseEnqueue();
  await Promise.all([prompt, reset]);
  assert.deepEqual(bridge.events, [
    "enqueue-start:context-prompt",
    "reset:user",
    "enqueue-discarded:context-prompt",
  ]);
  assert.deepEqual(bridge.enqueued, []);
});

test("acp-new discards a pending multi-part prompt buffer", async () => {
  const bridge = makeBridge();

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.promptStart, "context-start"),
  );
  await bridge.handleMessage(textMessage("buffered text", "context-buffer"));
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-new"),
  );
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.promptDone, "context-done"),
  );

  assert.deepEqual(bridge.enqueued, []);
  assert.ok(
    bridge.sent.some(({ segment }) =>
      segment.includes("Dropped 1 buffered content block")
    ),
  );
  assert.ok(
    bridge.sent.some(({ segment }) => segment.includes("Nothing buffered")),
  );
});

test("acp-new detaches an old stuck buffer flush", async () => {
  const bridge = makeBridge();
  const internal = bridge as unknown as {
    bufferFlushing: Map<string, Promise<void>>;
  };
  internal.bufferFlushing.set("user", new Promise<void>(() => {}));

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-new"),
  );
  await bridge.handleMessage(
    textMessage("fresh prompt", "context-prompt"),
  );

  assert.deepEqual(bridge.enqueued, ["context-prompt"]);
});

test("a config update that finishes after reset cannot send a stale reply", async () => {
  const bridge = makeBridge();
  let updateStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    updateStarted = resolve;
  });
  let finishUpdate!: () => void;
  const finished = new Promise<void>((resolve) => {
    finishUpdate = resolve;
  });
  const option = {
    id: "feature",
    name: "Feature",
    description: "Feature toggle",
    type: "boolean" as const,
    currentValue: false,
  };
  (
    bridge as unknown as {
      sessionManager: {
        getSessionConfigOptions(): typeof option[];
        setSessionConfigOption(): Promise<typeof option[]>;
      };
    }
  ).sessionManager = {
    getSessionConfigOptions: () => [option],
    setSessionConfigOption: async () => {
      updateStarted();
      await finished;
      return [option];
    },
  };

  const update = bridge.handleMessage(
    textMessage(
      `${BRIDGE_COMMANDS.acpConfig} set feature true`,
      "context-config",
    ),
  );
  await started;
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-new"),
  );
  finishUpdate();
  await update;

  assert.equal(
    bridge.sent.some(({ segment }) => segment.includes("Updated ACP config")),
    false,
  );
  assert.ok(
    bridge.sent.some(({ segment }) => segment.includes("ACP session cleared")),
  );
});

test("only the latest concurrent reset reports a shared cleanup failure", async () => {
  const bridge = makeBridge();
  let failReset!: (err: Error) => void;
  const cleanup = new Promise<ResetSessionResult>((_resolve, reject) => {
    failReset = reject;
  });
  bridge.resetBehavior = async () => cleanup;

  const first = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset-1"),
  );
  const second = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset-2"),
  );
  failReset(new Error("lease cleanup failed"));
  await Promise.all([first, second]);

  assert.equal(
    bridge.sent.filter(({ segment }) =>
      segment.includes("Could not fully clear")
    ).length,
    1,
  );
  assert.equal(
    bridge.sent.some(({ segment }) => segment.includes("ACP session cleared")),
    false,
  );
  assert.equal(bridge.sent[0]!.contextToken, "context-reset-2");
});

test("an old buffered append cannot affect a new buffer after reset", async () => {
  const bridge = makeBridge();
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.promptStart, "context-old-start"),
  );
  const internal = bridge as unknown as {
    messageBuffers: Map<string, {
      blocks: ContentBlock[];
      pending: Promise<void>;
    }>;
  };
  const oldBuffer = internal.messageBuffers.get("user")!;
  oldBuffer.blocks.push(
    ...Array.from({ length: 50 }, () => ({
      type: "text" as const,
      text: "old",
    })),
  );
  let releaseOldBuffer!: () => void;
  oldBuffer.pending = new Promise<void>((resolve) => {
    releaseOldBuffer = resolve;
  });
  await bridge.handleMessage(
    textMessage("old buffered text", "context-old-append"),
  );
  const oldAppend = oldBuffer.pending;

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.promptStart, "context-new-start"),
  );
  releaseOldBuffer();
  await oldAppend;
  await bridge.handleMessage(
    textMessage("fresh buffered text", "context-new-content"),
  );
  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.promptDone, "context-new-done"),
  );

  assert.equal(
    bridge.sent.some(({ segment }) => segment.includes("Buffer is full")),
    false,
  );
  assert.deepEqual(bridge.buffered, [{
    contextToken: "context-new-done",
    prompt: [{ type: "text", text: "fresh buffered text" }],
  }]);
});

test("a queued old agent reply is discarded at the reset boundary", async () => {
  const bridge = makeBridge();
  let blockerStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    blockerStarted = resolve;
  });
  let releaseBlocker!: () => void;
  const blocked = new Promise<void>((resolve) => {
    releaseBlocker = resolve;
  });
  bridge.sendBehavior = async (contextToken) => {
    if (contextToken === "context-blocker") {
      blockerStarted();
      await blocked;
    }
    return true;
  };

  const blocker = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpMore, "context-blocker"),
  );
  await started;
  bridge.beginPrompt("context-old");
  const oldReply = bridge.queueAgentReply(
    "context-old",
    "OLD SESSION REPLY",
  );
  const reset = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  releaseBlocker();
  await Promise.all([blocker, oldReply, reset]);

  assert.equal(
    bridge.sent.some(({ segment }) => segment.includes("OLD SESSION REPLY")),
    false,
  );
  assert.ok(
    bridge.sent.some(({ segment }) => segment.includes("ACP session cleared")),
  );
});

test("a late old agent callback cannot adopt the reset generation", async () => {
  const bridge = makeBridge();
  bridge.beginPrompt("context-old");

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  await bridge.queueAgentReply("context-old", "LATE OLD REPLY");

  assert.equal(
    bridge.sent.some(({ segment }) => segment === "LATE OLD REPLY"),
    false,
  );
});

test("an in-flight old text reply stops before its next segment after reset", async () => {
  const bridge = makeBridge();
  const firstSegment = "a".repeat(4000);
  let firstStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    firstStarted = resolve;
  });
  let releaseFirst!: () => void;
  const blocked = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  bridge.sendBehavior = async (contextToken, segment) => {
    if (contextToken === "context-old" && segment === firstSegment) {
      firstStarted();
      await blocked;
    }
    return true;
  };

  bridge.beginPrompt("context-old");
  const oldReply = bridge.queueAgentReply(
    "context-old",
    `${firstSegment}\nold second segment`,
  );
  await started;
  const reset = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  releaseFirst();
  await Promise.all([oldReply, reset]);

  assert.equal(
    bridge.sent.some(({ segment }) => segment === "old second segment"),
    false,
  );
});

test("acp-new discards acp-more queued behind an in-flight reply", async () => {
  const bridge = makeBridge();
  let blockerStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    blockerStarted = resolve;
  });
  let releaseBlocker!: () => void;
  const blocked = new Promise<void>((resolve) => {
    releaseBlocker = resolve;
  });
  bridge.sendBehavior = async (contextToken) => {
    if (contextToken === "context-blocker") {
      blockerStarted();
      await blocked;
    }
    return true;
  };

  bridge.beginPrompt("context-blocker");
  const blocker = bridge.queueAgentReply("context-blocker", "blocking reply");
  await started;
  const more = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpMore, "context-more"),
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  const reset = bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  releaseBlocker();
  await Promise.all([blocker, more, reset]);

  assert.equal(
    bridge.sent.some(({ contextToken }) => contextToken === "context-more"),
    false,
  );
});

test("acp-new discards an injection admitted before target resolution", async () => {
  const bridge = makeBridge();
  let resolutionStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    resolutionStarted = resolve;
  });
  let finishResolution!: () => void;
  const resolution = new Promise<void>((resolve) => {
    finishResolution = resolve;
  });
  bridge.injectionTargetBehavior = async () => {
    resolutionStarted();
    await resolution;
    return { userId: "user", contextToken: "context-injected" };
  };
  const injected: string[] = [];
  const internal = bridge as unknown as {
    config: { storage: { stateFile?: string } };
    sessionManager: {
      enqueueAndWait(
        userId: string,
        message: { contextToken: string },
      ): Promise<void>;
    };
    enqueueInjectedMessage(job: InjectedMessage): Promise<void>;
  };
  internal.config.storage.stateFile = "state.json";
  internal.sessionManager = {
    enqueueAndWait: async (_userId, message) => {
      injected.push(message.contextToken);
    },
  };
  const injection = internal.enqueueInjectedMessage({
    id: "injection-before-reset",
    createdAt: new Date().toISOString(),
    target: "last-active-user",
    text: "old injected prompt",
    source: "cli",
  });
  await started;

  await bridge.handleMessage(
    textMessage(BRIDGE_COMMANDS.acpNew, "context-reset"),
  );
  finishResolution();

  await assert.rejects(injection, /discarded because the target ACP session was reset/);
  assert.deepEqual(injected, []);
});
