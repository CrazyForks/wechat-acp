import assert from "node:assert/strict";
import type { ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { test } from "node:test";

import {
  SessionManager,
  type UserSession,
} from "../src/acp/session.js";
import { killAgentAndWait } from "../src/acp/agent-manager.js";

function makeProcess(
  kills: string[],
  opts?: { exitOnKill?: boolean; initiallyKilled?: boolean },
): ChildProcess {
  const emitter = new EventEmitter();
  const state: {
    killed: boolean;
    exitCode: number | null;
    signalCode: NodeJS.Signals | null;
  } = {
    killed: opts?.initiallyKilled ?? false,
    exitCode: null,
    signalCode: null,
  };
  const proc = emitter as unknown as ChildProcess;
  Object.defineProperties(proc, {
    killed: { get: () => state.killed },
    exitCode: { get: () => state.exitCode },
    signalCode: { get: () => state.signalCode },
  });
  proc.kill = (signal: NodeJS.Signals | number = "SIGTERM") => {
    const normalizedSignal =
      typeof signal === "string" ? signal : "SIGTERM";
    kills.push(normalizedSignal);
    state.killed = true;
    if (opts?.exitOnKill !== false) {
      queueMicrotask(() => {
        state.signalCode = normalizedSignal;
        emitter.emit("exit", null, normalizedSignal);
        emitter.emit("close", null, normalizedSignal);
      });
    }
    return true;
  };
  return proc;
}

function makeSession(
  userId: string,
  opts?: {
    process?: ChildProcess;
    processing?: boolean;
    queue?: UserSession["queue"];
    close?: () => Promise<void>;
  },
): UserSession {
  return {
    userId,
    contextToken: `${userId}-context`,
    client: {} as never,
    agentInfo: {
      process: opts?.process ?? makeProcess([]),
      connection: {
        closed: new Promise<void>(() => {}),
      } as never,
      sessionId: `${userId}-session`,
      configOptions: [],
      sessionOutcome: "new",
    },
    mcpLease: opts?.close
      ? { mcpServer: {} as never, close: opts.close }
      : undefined,
    configOptions: [],
    queue: opts?.queue ?? [],
    processing: opts?.processing ?? false,
    lastActivity: Date.now(),
    createdAt: Date.now(),
  };
}

function makeManager(opts?: {
  removePersistedSessionId?: (userId: string) => Promise<void>;
  onReply?: (userId: string, contextToken: string, text: string) => Promise<void>;
  getPersistedSessionId?: (userId: string) => Promise<string | undefined>;
  createMcpLease?: () => {
    mcpServer: never;
    close(): Promise<void>;
  };
  agentShutdownTimeoutMs?: number;
  maxConcurrentUsers?: number;
  idleTimeoutMs?: number;
  killAgentProcess?: (
    process: ChildProcess,
    timeoutMs?: number,
  ) => Promise<void>;
}): SessionManager {
  return new SessionManager({
    agentCommand: "unused",
    agentArgs: [],
    agentCwd: process.cwd(),
    idleTimeoutMs: opts?.idleTimeoutMs ?? 0,
    maxConcurrentUsers: opts?.maxConcurrentUsers ?? 3,
    resumePolicy: opts?.getPersistedSessionId ? "auto" : "off",
    getPersistedSessionId: opts?.getPersistedSessionId,
    removePersistedSessionId: opts?.removePersistedSessionId,
    createMcpLease: opts?.createMcpLease,
    agentShutdownTimeoutMs: opts?.agentShutdownTimeoutMs,
    killAgentProcess:
      opts?.killAgentProcess ??
      ((process, timeoutMs) =>
        killAgentAndWait(process, timeoutMs, { platform: "linux" })),
    showThoughts: false,
    log: () => {},
    onReply: opts?.onReply ?? (async () => {}),
    sendTyping: async () => {},
  });
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function makeExitingProcess(): {
  process: ChildProcess;
  emitExit: () => void;
} {
  const process = new EventEmitter() as ChildProcess;
  let exitCode: number | null = null;
  Object.defineProperties(process, {
    pid: { value: 4242 },
    killed: { value: false },
    exitCode: { get: () => exitCode },
    signalCode: { value: null },
  });
  return {
    process,
    emitExit: () => {
      exitCode = 0;
      process.emit("exit", 0, null);
    },
  };
}

function makeControlledTurn(
  process: ChildProcess,
  replyText: string,
  connectionClosed = new Promise<void>(() => {}),
): {
  session: UserSession;
  started: Promise<void>;
  resolvePrompt: () => void;
  completion: Promise<void>;
} {
  const started = deferred<void>();
  const prompt = deferred<{ stopReason: "end_turn" }>();
  const completion = deferred<void>();
  const session = makeSession("target", {
    process,
    processing: true,
    queue: [{
      prompt: [],
      contextToken: "context",
      completion: {
        resolve: () => completion.resolve(undefined),
        reject: completion.reject,
      },
    }],
  });
  session.client = {
    beginTurn: async () => {},
    flush: async () => replyText,
    hasProducedMessage: replyText.length > 0,
  } as never;
  session.agentInfo.connection = {
    closed: connectionClosed,
    prompt: async () => {
      started.resolve(undefined);
      return prompt.promise;
    },
  } as never;
  return {
    session,
    started: started.promise,
    resolvePrompt: () => prompt.resolve({ stopReason: "end_turn" }),
    completion: completion.promise,
  };
}

function lifecycleInternals(manager: SessionManager): {
  sessions: Map<string, UserSession>;
  handleAgentExit(userId: string, process: ChildProcess): void;
  processQueue(session: UserSession): Promise<void>;
} {
  return manager as unknown as {
    sessions: Map<string, UserSession>;
    handleAgentExit(userId: string, process: ChildProcess): void;
    processQueue(session: UserSession): Promise<void>;
  };
}

test("different users can create sessions concurrently within capacity", async () => {
  const manager = makeManager();
  const started: string[] = [];
  const releases = new Map<string, () => void>();
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };
  internal.createSession = async (userId) => {
    started.push(userId);
    await new Promise<void>((resolve) => {
      releases.set(userId, resolve);
    });
    return makeSession(userId);
  };

  const first = internal.getOrCreateSession("user-1", "context-1");
  const second = internal.getOrCreateSession("user-2", "context-2");
  await new Promise<void>((resolve) => setImmediate(resolve));

  assert.deepEqual(started, ["user-1", "user-2"]);
  releases.get("user-1")!();
  releases.get("user-2")!();
  assert.equal((await first).userId, "user-1");
  assert.equal((await second).userId, "user-2");
});

test("a session pending first enqueue cannot be evicted for another user", async () => {
  const kills: string[] = [];
  const manager = new SessionManager({
    agentCommand: "unused",
    agentArgs: [],
    agentCwd: process.cwd(),
    idleTimeoutMs: 0,
    maxConcurrentUsers: 1,
    showThoughts: false,
    log: () => {},
    onReply: async () => {},
    sendTyping: async () => {},
  });
  const firstSession = makeSession("user-1", {
    process: makeProcess(kills),
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    pendingSessions: Map<string, {
      promise: Promise<UserSession>;
      abortController: AbortController;
      cancelled: boolean;
    }>;
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };
  internal.sessions.set("user-1", firstSession);
  internal.pendingSessions.set("user-1", {
    promise: Promise.resolve(firstSession),
    abortController: new AbortController(),
    cancelled: false,
  });

  await assert.rejects(
    internal.getOrCreateSession("user-2", "context-2"),
    /Maximum concurrent sessions reached/,
  );
  assert.equal(manager.getSession("user-1"), firstSession);
  assert.deepEqual(kills, []);
});

test("reset removes only the target session and clears persisted state", async () => {
  const removed: string[] = [];
  const kills: string[] = [];
  const closed: string[] = [];
  const rejected: string[] = [];
  const manager = makeManager({
    removePersistedSessionId: async (userId) => {
      removed.push(userId);
    },
  });
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set(
    "target",
    makeSession("target", {
      process: makeProcess(kills),
      processing: true,
      queue: [{
        prompt: [],
        contextToken: "queued-context",
        completion: {
          resolve: () => {},
          reject: (err) => {
            rejected.push(String(err));
          },
        },
      }],
      close: async () => {
        closed.push("target");
      },
    }),
  );
  const other = makeSession("other");
  sessions.set("other", other);

  const result = await manager.resetSession("target");

  assert.deepEqual(result, {
    hadActiveSession: true,
    cancelledTurn: true,
    cancelledPendingCreation: false,
    droppedQueueCount: 1,
  });
  assert.equal(manager.getSession("target"), undefined);
  assert.equal(manager.getSession("other"), other);
  assert.deepEqual(kills, ["SIGTERM"]);
  assert.deepEqual(closed, ["target"]);
  assert.deepEqual(removed, ["target"]);
  assert.match(rejected[0]!, /session reset/i);
});

test("reset clears persisted state even without an active session", async () => {
  const removed: string[] = [];
  const manager = makeManager({
    removePersistedSessionId: async (userId) => {
      removed.push(userId);
    },
  });

  const result = await manager.resetSession("target");

  assert.deepEqual(result, {
    hadActiveSession: false,
    cancelledTurn: false,
    cancelledPendingCreation: false,
    droppedQueueCount: 0,
  });
  assert.deepEqual(removed, ["target"]);
});

test("reset aborts pending creation without sending a session error", async () => {
  const replies: string[] = [];
  const removed: string[] = [];
  const manager = makeManager({
    removePersistedSessionId: async (userId) => {
      removed.push(userId);
    },
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  let creationStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    creationStarted = resolve;
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
  };
  internal.createSession = async (userId, _contextToken, signal) => {
    creationStarted();
    await new Promise<void>((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error("aborted"));
        return;
      }
      signal.addEventListener(
        "abort",
        () => reject(new Error("aborted")),
        { once: true },
      );
    });
    return makeSession(userId);
  };

  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context",
  });
  await started;
  const reset = manager.resetSession("target");

  await assert.rejects(enqueue, /session reset/i);
  assert.deepEqual(await reset, {
    hadActiveSession: false,
    cancelledTurn: false,
    cancelledPendingCreation: true,
    droppedQueueCount: 0,
  });
  assert.deepEqual(replies, []);
  assert.deepEqual(removed, ["target"]);
  assert.equal(manager.getSession("target"), undefined);
});

test("reset rejects every prompt queued before the reset boundary", async () => {
  const manager = makeManager();
  let createCalls = 0;
  let creationStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    creationStarted = resolve;
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
  };
  internal.createSession = async (_userId, _contextToken, signal) => {
    createCalls++;
    creationStarted();
    await new Promise<void>((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error("aborted"));
        return;
      }
      signal.addEventListener(
        "abort",
        () => reject(new Error("aborted")),
        { once: true },
      );
    });
    throw new Error("unreachable");
  };

  const first = manager.enqueue("target", {
    prompt: [],
    contextToken: "context-1",
  });
  const second = manager.enqueue("target", {
    prompt: [],
    contextToken: "context-2",
  });
  await started;
  const reset = manager.resetSession("target");

  await assert.rejects(first, /session reset/i);
  await assert.rejects(second, /session reset/i);
  await reset;
  assert.equal(createCalls, 1);
  assert.equal(manager.getSession("target"), undefined);
});

test("reset invalidates callbacks before lifecycle cleanup starts", async () => {
  const manager = makeManager();
  const session = makeSession("target", {
    process: makeProcess([]),
  });
  session.lifecycleGeneration = 0;
  let releaseLifecycle!: () => void;
  const priorLifecycle = new Promise<void>((resolve) => {
    releaseLifecycle = resolve;
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    userLifecycleChains: Map<string, Promise<void>>;
    runIfCurrent(
      session: UserSession,
      task: () => Promise<void>,
    ): Promise<void>;
  };
  internal.sessions.set("target", session);
  internal.userLifecycleChains.set("target", priorLifecycle);
  let callbackRan = false;

  const reset = manager.resetSession("target");
  await internal.runIfCurrent(session, async () => {
    callbackRan = true;
  });

  assert.equal(callbackRan, false);
  assert.equal(manager.getSession("target"), session);
  releaseLifecycle();
  await reset;
});

test("reset reports a pending creation lease cleanup failure", async () => {
  const manager = makeManager();
  let creationStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    creationStarted = resolve;
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
  };
  internal.createSession = async (userId, _contextToken, signal) => {
    creationStarted();
    await new Promise<void>((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
    return makeSession(userId, {
      close: async () => {
        throw new Error("lease cleanup failed");
      },
    });
  };

  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context",
  });
  await started;
  const reset = manager.resetSession("target");

  await assert.rejects(enqueue, /session reset/i);
  await assert.rejects(reset, /fully reset/i);
  assert.equal(manager.getSession("target"), undefined);
});

test("reset reports a late-created process that does not exit", async () => {
  const kills: string[] = [];
  const manager = makeManager({ agentShutdownTimeoutMs: 10 });
  let creationStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    creationStarted = resolve;
  });
  let finishCreation!: () => void;
  const creationFinished = new Promise<void>((resolve) => {
    finishCreation = resolve;
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
  };
  internal.createSession = async (userId) => {
    creationStarted();
    await creationFinished;
    return makeSession(userId, {
      process: makeProcess(kills, { exitOnKill: false }),
    });
  };

  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context",
  });
  await started;
  const reset = manager.resetSession("target");
  finishCreation();

  await assert.rejects(enqueue, /session reset/i);
  await assert.rejects(reset, /fully reset/i);
  assert.deepEqual(kills, ["SIGTERM", "SIGTERM"]);
  assert.equal(manager.getSession("target"), undefined);
});

test("reset closes a lease when persisted session lookup fails", async () => {
  let lookupStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    lookupStarted = resolve;
  });
  let rejectLookup!: (err: Error) => void;
  const lookup = new Promise<string | undefined>((_resolve, reject) => {
    rejectLookup = reject;
  });
  let closeCalls = 0;
  const manager = makeManager({
    getPersistedSessionId: async () => {
      lookupStarted();
      return lookup;
    },
    createMcpLease: () => ({
      mcpServer: {} as never,
      close: async () => {
        closeCalls++;
      },
    }),
  });

  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context",
  });
  await started;
  const reset = manager.resetSession("target");
  rejectLookup(new Error("state lookup failed"));

  await assert.rejects(enqueue, /session reset/i);
  assert.equal((await reset).cancelledPendingCreation, true);
  assert.equal(closeCalls, 1);
});

test("ordinary session creation retains failed cleanup for acp-new", async () => {
  let closeCalls = 0;
  const manager = makeManager({
    getPersistedSessionId: async () => {
      throw new Error("state lookup failed");
    },
    createMcpLease: () => ({
      mcpServer: {} as never,
      close: async () => {
        closeCalls++;
        throw new Error("lease cleanup failed");
      },
    }),
  });
  const internal = manager as unknown as {
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };

  await assert.rejects(
    internal.getOrCreateSession("target", "context"),
    /cleanup also failed/i,
  );
  await assert.rejects(manager.resetSession("target"), /fully reset/i);
  assert.equal(closeCalls, 2);
});

test("reset during beginTurn rejects the active completion", async () => {
  const completions: string[] = [];
  const manager = makeManager();
  let beginStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    beginStarted = resolve;
  });
  let finishBegin!: () => void;
  const beginFinished = new Promise<void>((resolve) => {
    finishBegin = resolve;
  });
  const session = makeSession("target", {
    process: makeProcess([]),
    processing: true,
    queue: [{
      prompt: [],
      contextToken: "old-context",
      completion: {
        resolve: () => {
          completions.push("resolved");
        },
        reject: () => {
          completions.push("rejected");
        },
      },
    }],
  });
  session.client = {
    beginTurn: async () => {
      beginStarted();
      await beginFinished;
    },
    flush: async () => "",
    hasProducedMessage: false,
  } as never;
  session.agentInfo.connection = {
    closed: new Promise<void>(() => {}),
    prompt: async () => ({ stopReason: "end_turn" }),
  } as never;
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set("target", session);
  const processing = (
    manager as unknown as {
      processQueue(session: UserSession): Promise<void>;
    }
  ).processQueue(session);
  await started;

  await manager.resetSession("target");
  assert.deepEqual(completions, ["rejected"]);
  finishBegin();
  await processing;

  assert.deepEqual(completions, ["rejected"]);
});

test("concurrent resets share the same cleanup failure", async () => {
  let closeCalls = 0;
  const manager = makeManager();
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set(
    "target",
    makeSession("target", {
      close: async () => {
        closeCalls++;
        throw new Error("lease cleanup failed");
      },
    }),
  );

  const first = manager.resetSession("target");
  const second = manager.resetSession("target");

  assert.equal(first, second);
  await assert.rejects(first, /fully reset/i);
  await assert.rejects(second, /fully reset/i);
  assert.equal(closeCalls, 1);
});

test("a later reset still reports an earlier process cleanup failure", async () => {
  const kills: string[] = [];
  const manager = makeManager({ agentShutdownTimeoutMs: 5 });
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set(
    "target",
    makeSession("target", {
      process: makeProcess(kills, { exitOnKill: false }),
    }),
  );

  await assert.rejects(manager.resetSession("target"), /fully reset/i);
  await assert.rejects(manager.resetSession("target"), /fully reset/i);

  assert.deepEqual(kills, ["SIGTERM", "SIGTERM"]);
});

test("a persistence-only cleanup failure does not consume session capacity", async () => {
  const manager = makeManager({
    maxConcurrentUsers: 1,
    removePersistedSessionId: async () => {
      throw new Error("state write failed");
    },
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };
  internal.createSession = async (userId) => makeSession(userId);

  await assert.rejects(manager.resetSession("user-1"), /fully reset/i);
  await assert.rejects(
    internal.getOrCreateSession("user-1", "context-1"),
    /cleanup is incomplete/i,
  );
  assert.equal(
    (await internal.getOrCreateSession("user-2", "context-2")).userId,
    "user-2",
  );
});

test("SessionManager.stop retries cleanup retained by a failed reset", async () => {
  let closeCalls = 0;
  const manager = makeManager();
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set(
    "target",
    makeSession("target", {
      close: async () => {
        closeCalls++;
        if (closeCalls === 1) {
          throw new Error("lease cleanup failed");
        }
      },
    }),
  );

  await assert.rejects(manager.resetSession("target"), /fully reset/i);
  await manager.stop();
  assert.equal(closeCalls, 2);
});

test("idle cleanup retains failures for a later acp-new retry", async () => {
  const kills: string[] = [];
  let closeCalls = 0;
  const manager = makeManager({ idleTimeoutMs: 1 });
  const session = makeSession("target", {
    process: makeProcess(kills),
    close: async () => {
      closeCalls++;
      if (closeCalls === 1) {
        throw new Error("lease cleanup failed");
      }
    },
  });
  session.lastActivity = 0;
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    cleanupIdleSessions(): void;
  };
  internal.sessions.set("target", session);

  internal.cleanupIdleSessions();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(manager.getSession("target"), undefined);
  assert.equal(closeCalls, 1);
  assert.deepEqual(kills, ["SIGTERM"]);

  await manager.resetSession("target");
  assert.equal(closeCalls, 2);
});

test("natural wrapper exit retains process-group cleanup for retry", async () => {
  const process = new EventEmitter() as ChildProcess;
  Object.defineProperties(process, {
    pid: { value: 4242 },
    exitCode: { value: 0 },
    signalCode: { value: null },
  });
  let processCleanupCalls = 0;
  const manager = makeManager({
    killAgentProcess: async (candidate) => {
      assert.equal(candidate, process);
      processCleanupCalls++;
      if (processCleanupCalls === 1) {
        throw new Error("process group cleanup failed");
      }
    },
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    handleAgentExit(userId: string, process: ChildProcess): void;
  };
  internal.sessions.set("target", makeSession("target", { process }));

  internal.handleAgentExit("target", process);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(manager.getSession("target"), undefined);
  assert.equal(processCleanupCalls, 1);

  await manager.resetSession("target");
  assert.equal(processCleanupCalls, 2);
});

test("natural wrapper exit before prompt dispatch rejects the active completion", async () => {
  const process = new EventEmitter() as ChildProcess;
  Object.defineProperties(process, {
    pid: { value: 4242 },
    exitCode: { value: 0 },
    signalCode: { value: null },
  });
  const manager = makeManager({
    killAgentProcess: async () => {},
  });
  let rejectCompletion!: (err: unknown) => void;
  const completion = new Promise<void>((_resolve, reject) => {
    rejectCompletion = reject;
  });
  const session = makeSession("target", {
    process,
    processing: true,
  });
  session.activeMessage = {
    prompt: [],
    contextToken: "context",
    completion: {
      resolve: () => {},
      reject: rejectCompletion,
    },
  };
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    handleAgentExit(userId: string, process: ChildProcess): void;
  };
  internal.sessions.set("target", session);
  const rejected = assert.rejects(completion, /Agent process exited/);

  internal.handleAgentExit("target", process);

  await rejected;
  assert.equal(session.activeMessage.completion, undefined);
});

test("natural wrapper exit drains a buffered final response", async () => {
  const exiting = makeExitingProcess();
  const replies: string[] = [];
  const manager = makeManager({
    killAgentProcess: async () => {},
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  const turn = makeControlledTurn(
    exiting.process,
    "buffered final response",
  );
  const internal = lifecycleInternals(manager);
  internal.sessions.set("target", turn.session);
  const processing = internal.processQueue(turn.session);
  await turn.started;

  internal.handleAgentExit("target", exiting.process);
  exiting.emitExit();
  turn.resolvePrompt();

  await turn.completion;
  await processing;
  assert.deepEqual(replies, ["buffered final response"]);
  assert.equal(manager.getSession("target"), undefined);
});

test("reset waits for cleanup of a turn draining after process exit", async () => {
  const exiting = makeExitingProcess();
  let processCleanupCalls = 0;
  let closeStarted!: () => void;
  const startedClosing = new Promise<void>((resolve) => {
    closeStarted = resolve;
  });
  let releaseClose!: () => void;
  const closeReleased = new Promise<void>((resolve) => {
    releaseClose = resolve;
  });
  const manager = makeManager({
    killAgentProcess: async () => {
      processCleanupCalls++;
    },
  });
  let closeCalls = 0;
  const turn = makeControlledTurn(exiting.process, "obsolete response");
  turn.session.mcpLease = {
    mcpServer: {} as never,
    close: async () => {
      closeCalls++;
      closeStarted();
      await closeReleased;
    },
  };
  const internal = lifecycleInternals(manager);
  internal.sessions.set("target", turn.session);
  const rejected = assert.rejects(turn.completion, /reset/i);
  const processing = internal.processQueue(turn.session);
  await turn.started;

  internal.handleAgentExit("target", exiting.process);
  exiting.emitExit();
  let resetSettled = false;
  const reset = manager.resetSession("target").then((result) => {
    resetSettled = true;
    return result;
  });
  await startedClosing;

  assert.equal(resetSettled, false);
  assert.equal(processCleanupCalls, 1);
  releaseClose();
  const result = await reset;
  await rejected;
  turn.resolvePrompt();
  await processing;

  assert.equal(result.hadActiveSession, true);
  assert.equal(result.cancelledTurn, true);
  assert.equal(processCleanupCalls, 1);
  assert.equal(closeCalls, 1);
});

test("connection closure after process exit rejects an unanswered prompt", async () => {
  const exiting = makeExitingProcess();
  const replies: string[] = [];
  const manager = makeManager({
    killAgentProcess: async () => {},
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  const closed = deferred<void>();
  const turn = makeControlledTurn(exiting.process, "", closed.promise);
  const internal = lifecycleInternals(manager);
  internal.sessions.set("target", turn.session);
  const rejected = assert.rejects(turn.completion, /connection closed/i);
  const processing = internal.processQueue(turn.session);
  await turn.started;

  internal.handleAgentExit("target", exiting.process);
  exiting.emitExit();
  closed.resolve(undefined);

  await rejected;
  await processing;
  assert.deepEqual(replies, []);
});

test("replacement permanently suppresses a buffered response from an exited process", async () => {
  const exiting = makeExitingProcess();
  const replies: string[] = [];
  const manager = makeManager({
    maxConcurrentUsers: 1,
    killAgentProcess: async () => {},
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  const turn = makeControlledTurn(
    exiting.process,
    "obsolete buffered response",
  );
  const internal = lifecycleInternals(manager);
  const replacement = makeSession("target");
  const createdUsers: string[] = [];
  const creation = internal as typeof internal & {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(
      userId: string,
      contextToken: string,
    ): Promise<UserSession>;
  };
  creation.createSession = async (userId) => {
    createdUsers.push(userId);
    return userId === "target" ? replacement : makeSession(userId);
  };
  internal.sessions.set("target", turn.session);
  const rejected = assert.rejects(turn.completion, /replaced/i);
  const processing = internal.processQueue(turn.session);
  await turn.started;

  internal.handleAgentExit("target", exiting.process);
  exiting.emitExit();
  await assert.rejects(
    creation.getOrCreateSession("other", "other-context"),
    /Maximum concurrent sessions reached/,
  );
  assert.deepEqual(createdUsers, []);
  assert.equal(
    await creation.getOrCreateSession("target", "replacement-context"),
    replacement,
  );
  assert.deepEqual(createdUsers, ["target"]);
  internal.sessions.delete("target");
  turn.resolvePrompt();

  await rejected;
  await processing;
  assert.deepEqual(replies, []);
  assert.equal(manager.getSession("target"), undefined);
});

test("session creation rejects a process that already exited", async () => {
  const process = new EventEmitter() as ChildProcess;
  Object.defineProperties(process, {
    pid: { value: 4242 },
    exitCode: { value: 0 },
    signalCode: { value: null },
  });
  let processCleanupCalls = 0;
  const manager = makeManager({
    killAgentProcess: async (candidate) => {
      assert.equal(candidate, process);
      processCleanupCalls++;
    },
  });
  const internal = manager as unknown as {
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(
      userId: string,
      contextToken: string,
    ): Promise<UserSession>;
  };
  internal.createSession = async () =>
    makeSession("target", { process });

  await assert.rejects(
    internal.getOrCreateSession("target", "context"),
    /Agent process exited/,
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(manager.getSession("target"), undefined);
  assert.equal(processCleanupCalls, 1);
});

test("capacity eviction waits for old resources to close", async () => {
  let releaseLease!: () => void;
  const leaseClosed = new Promise<void>((resolve) => {
    releaseLease = resolve;
  });
  const manager = makeManager({ maxConcurrentUsers: 1 });
  const oldSession = makeSession("old", {
    close: async () => leaseClosed,
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };
  internal.sessions.set("old", oldSession);
  let createStarted = false;
  internal.createSession = async (userId) => {
    createStarted = true;
    return makeSession(userId);
  };

  const creation = internal.getOrCreateSession("new", "context-new");
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(createStarted, false);
  assert.equal(manager.getSession("old"), undefined);

  releaseLease();
  assert.equal((await creation).userId, "new");
  assert.equal(createStarted, true);
});

test("prompt failure cleanup retains process and lease failures", async () => {
  const process = makeProcess([], {
    exitOnKill: false,
    initiallyKilled: true,
  });
  let processCleanupCalls = 0;
  let closeCalls = 0;
  const manager = makeManager({
    killAgentProcess: async () => {
      processCleanupCalls++;
    },
  });
  const session = makeSession("target", {
    process,
    processing: true,
    queue: [{ prompt: [], contextToken: "context" }],
    close: async () => {
      closeCalls++;
      if (closeCalls === 1) {
        throw new Error("lease cleanup failed");
      }
    },
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    processQueue(session: UserSession): Promise<void>;
  };
  internal.sessions.set("target", session);

  await internal.processQueue(session);
  assert.equal(manager.getSession("target"), undefined);
  assert.equal(processCleanupCalls, 1);
  assert.equal(closeCalls, 1);

  await manager.resetSession("target");
  assert.equal(closeCalls, 2);
});

test("reset suppresses an obsolete enqueue cleanup error", async () => {
  const replies: string[] = [];
  let releaseCleanup!: () => void;
  const cleanupReleased = new Promise<void>((resolve) => {
    releaseCleanup = resolve;
  });
  let closeCalls = 0;
  const manager = makeManager({
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  const internal = manager as unknown as {
    getOrCreateCleanupState(userId: string): {
      mcpLeases: Set<{
        mcpServer: never;
        close(): Promise<void>;
      }>;
    };
    retryCleanupState(userId: string): Promise<void>;
  };
  internal.getOrCreateCleanupState("target").mcpLeases.add({
    mcpServer: {} as never,
    close: async () => {
      closeCalls++;
      if (closeCalls === 1) {
        await cleanupReleased;
        throw new Error("lease cleanup failed");
      }
    },
  });
  const backgroundCleanup = internal.retryCleanupState("target");
  void backgroundCleanup.catch(() => {});
  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context-old",
  });
  await new Promise<void>((resolve) => setImmediate(resolve));

  const reset = manager.resetSession("target");
  releaseCleanup();

  await assert.rejects(enqueue, /session reset/i);
  await assert.rejects(backgroundCleanup, /fully reset/i);
  await reset;
  assert.equal(closeCalls, 2);
  assert.deepEqual(replies, []);
});

test("reset prevents obsolete creation after cleanup succeeds", async () => {
  let releaseCleanup!: () => void;
  const cleanupReleased = new Promise<void>((resolve) => {
    releaseCleanup = resolve;
  });
  const manager = makeManager();
  let createStarted = false;
  const internal = manager as unknown as {
    getOrCreateCleanupState(userId: string): {
      mcpLeases: Set<{
        mcpServer: never;
        close(): Promise<void>;
      }>;
    };
    retryCleanupState(userId: string): Promise<void>;
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
  };
  internal.createSession = async (userId) => {
    createStarted = true;
    return makeSession(userId);
  };
  internal.getOrCreateCleanupState("target").mcpLeases.add({
    mcpServer: {} as never,
    close: async () => cleanupReleased,
  });
  const backgroundCleanup = internal.retryCleanupState("target");
  const enqueue = manager.enqueue("target", {
    prompt: [],
    contextToken: "context-old",
  });
  await new Promise<void>((resolve) => setImmediate(resolve));

  const reset = manager.resetSession("target");
  releaseCleanup();

  await backgroundCleanup;
  await assert.rejects(enqueue, /session reset/i);
  await reset;
  assert.equal(createStarted, false);
});

test("persistence failure does not block an unrelated capacity eviction", async () => {
  let releaseLease!: () => void;
  const leaseReleased = new Promise<void>((resolve) => {
    releaseLease = resolve;
  });
  const manager = makeManager({
    maxConcurrentUsers: 1,
    removePersistedSessionId: async () => {
      throw new Error("state write failed");
    },
  });
  const oldSession = makeSession("old", {
    close: async () => leaseReleased,
  });
  const internal = manager as unknown as {
    sessions: Map<string, UserSession>;
    createSession(
      userId: string,
      contextToken: string,
      signal: AbortSignal,
    ): Promise<UserSession>;
    getOrCreateSession(userId: string, contextToken: string): Promise<UserSession>;
  };
  internal.sessions.set("old", oldSession);
  internal.createSession = async (userId) => makeSession(userId);

  const creation = internal.getOrCreateSession("new", "context-new");
  await new Promise<void>((resolve) => setImmediate(resolve));
  const reset = manager.resetSession("old");
  releaseLease();

  assert.equal((await creation).userId, "new");
  await assert.rejects(reset, /fully reset/i);
  assert.equal(manager.getSession("new")?.userId, "new");
});

test("an old turn cannot reply or remove the replacement session after reset", async () => {
  const replies: string[] = [];
  const completions: string[] = [];
  const kills: string[] = [];
  const manager = makeManager({
    onReply: async (_userId, _contextToken, text) => {
      replies.push(text);
    },
  });
  let promptStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    promptStarted = resolve;
  });
  let finishPrompt!: () => void;
  const promptFinished = new Promise<void>((resolve) => {
    finishPrompt = resolve;
  });
  const oldSession = makeSession("target", {
    process: makeProcess(kills),
    processing: true,
    queue: [{
      prompt: [],
      contextToken: "old-context",
      completion: {
        resolve: () => {
          completions.push("resolved");
        },
        reject: () => {
          completions.push("rejected");
        },
      },
    }],
  });
  oldSession.client = {
    beginTurn: async () => {},
    flush: async () => "old reply",
    hasProducedMessage: true,
  } as never;
  oldSession.agentInfo.connection = {
    closed: new Promise<void>(() => {}),
    prompt: async () => {
      promptStarted();
      await promptFinished;
      return { stopReason: "end_turn" };
    },
  } as never;
  const sessions = (
    manager as unknown as { sessions: Map<string, UserSession> }
  ).sessions;
  sessions.set("target", oldSession);
  const processing = (
    manager as unknown as {
      processQueue(session: UserSession): Promise<void>;
    }
  ).processQueue(oldSession);
  await started;

  await manager.resetSession("target");
  const replacement = makeSession("target");
  sessions.set("target", replacement);
  finishPrompt();
  await processing;

  assert.deepEqual(kills, ["SIGTERM"]);
  assert.deepEqual(replies, []);
  assert.deepEqual(completions, ["rejected"]);
  assert.equal(manager.getSession("target"), replacement);
});
