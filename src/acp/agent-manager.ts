/**
 * Spawn and manage ACP agent subprocesses.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { Writable, Readable } from "node:stream";
import * as acp from "@agentclientprotocol/sdk";
import packageJson from "../../package.json" with { type: "json" };
import type { WeChatAcpClient } from "./client.js";
import type { SessionResumePolicy } from "../config.js";
import { trackException } from "../telemetry/index.js";

export type AgentSessionOutcome = "new" | "loaded" | "unsupported" | "not_found";

export interface AgentProcessInfo {
  process: ChildProcess;
  connection: acp.ClientSideConnection;
  sessionId: string;
  configOptions: acp.SessionConfigOption[];
  sessionOutcome: AgentSessionOutcome;
}

export class AgentProcessCleanupError extends AggregateError {
  constructor(
    startupError: unknown,
    cleanupError: unknown,
    readonly process: ChildProcess,
  ) {
    super(
      [startupError, cleanupError],
      "Agent startup failed and the process could not be stopped",
    );
    this.name = "AgentProcessCleanupError";
  }
}

export async function spawnAgent(params: {
  command: string;
  args: string[];
  cwd: string;
  env?: Record<string, string>;
  client: WeChatAcpClient;
  mcpServers?: acp.McpServer[];
  resumePolicy?: SessionResumePolicy;
  persistedSessionId?: string;
  signal?: AbortSignal;
  log: (msg: string) => void;
}): Promise<AgentProcessInfo> {
  const {
    command,
    args,
    cwd,
    env,
    client,
    mcpServers = [],
    resumePolicy = "off",
    persistedSessionId,
    signal,
    log,
  } = params;
  if (signal?.aborted) {
    throw new Error("Agent spawn aborted");
  }

  // On Windows, shell mode avoids EINVAL/ENOENT for command shims like npx/claude/gemini.
  const useShell = process.platform === "win32";

  log(`Spawning agent: ${command} ${args.join(" ")} (cwd: ${cwd}, shell=${useShell})`);

  const proc = spawn(command, args, {
    stdio: ["pipe", "pipe", "inherit"],
    cwd,
    env: { ...process.env, ...env },
    shell: useShell,
    windowsHide: true,
  });

  proc.on("error", (err) => {
    log(`Agent process error: ${String(err)}`);
    trackException(err, "agent_spawn");
  });

  proc.on("exit", (code, signal) => {
    log(`Agent process exited: code=${code} signal=${signal}`);
  });

  try {
    if (!proc.stdin || !proc.stdout) {
      const err = new Error("Failed to get agent process stdio");
      trackException(err, "agent_spawn");
      throw err;
    }

    const input = Writable.toWeb(proc.stdin);
    const output = Readable.toWeb(proc.stdout) as ReadableStream<Uint8Array>;
    const stream = acp.ndJsonStream(input, output);

    const connection = new acp.ClientSideConnection(() => client, stream);

    // Initialize
    log("Initializing ACP connection...");
    const initResult = await abortable(
      connection.initialize({
        protocolVersion: acp.PROTOCOL_VERSION,
        clientInfo: {
          name: packageJson.name,
          title: packageJson.name,
          version: packageJson.version,
        },
        clientCapabilities: {
          fs: {
            readTextFile: true,
            writeTextFile: true,
          },
        },
      }),
      signal,
    );
    log(`ACP initialized (protocol v${initResult.protocolVersion})`);

    // Create or load session
    const supportsHttpMcp = initResult.agentCapabilities?.mcpCapabilities?.http === true;
    const sessionMcpServers = supportsHttpMcp
      ? mcpServers.filter(
          (server): server is acp.McpServerHttp & { type: "http" } =>
            "type" in server && server.type === "http",
        )
      : [];
    if (mcpServers.length > 0 && !supportsHttpMcp) {
      log("Agent does not advertise HTTP MCP support; file attachments are unavailable");
    }

    let sessionOutcome: AgentSessionOutcome = "new";
    if (persistedSessionId && resumePolicy !== "off") {
      if (initResult.agentCapabilities?.loadSession !== true) {
        if (resumePolicy === "required") {
          throw new Error(
            `Agent does not support loading persisted ACP session ${persistedSessionId}`,
          );
        }
        sessionOutcome = "unsupported";
        log("Agent does not advertise session/load; creating a new ACP session");
      } else {
        log(`Loading ACP session: ${persistedSessionId}`);
        client.beginSessionReplay();
        try {
          const loadResult = await abortable(
            connection.loadSession({
              cwd,
              mcpServers: sessionMcpServers,
              sessionId: persistedSessionId,
            }),
            signal,
          );
          await client.endSessionReplay();
          log(`ACP session loaded: ${persistedSessionId}`);
          return {
            process: proc,
            connection,
            sessionId: persistedSessionId,
            configOptions: loadResult.configOptions ?? [],
            sessionOutcome: "loaded",
          };
        } catch (err) {
          await client.endSessionReplay();
          if (resumePolicy !== "auto" || !isResourceNotFound(err)) {
            throw normalizeError(err);
          }
          sessionOutcome = "not_found";
          log(`Persisted ACP session not found: ${persistedSessionId}; creating a new session`);
        }
      }
    }

    log("Creating ACP session...");
    const sessionResult = await abortable(
      connection.newSession({
        cwd,
        mcpServers: sessionMcpServers,
      }),
      signal,
    );
    log(`ACP session created: ${sessionResult.sessionId}`);

    return {
      process: proc,
      connection,
      sessionId: sessionResult.sessionId,
      configOptions: sessionResult.configOptions ?? [],
      sessionOutcome,
    };
  } catch (err) {
    try {
      await killAgentAndWait(proc);
    } catch (cleanupErr) {
      throw new AgentProcessCleanupError(err, cleanupErr, proc);
    }
    throw err;
  }

  function isResourceNotFound(err: unknown): boolean {
    return (
      typeof err === "object" &&
      err !== null &&
      "code" in err &&
      err.code === -32002
    );
  }

  function normalizeError(err: unknown): Error {
    if (err instanceof Error) return err;
    if (
      typeof err === "object" &&
      err !== null &&
      "message" in err &&
      typeof err.message === "string"
    ) {
      return new Error(err.message, { cause: err });
    }
    return new Error(String(err), { cause: err });
  }
}

export function killAgent(proc: ChildProcess): void {
  if (proc.exitCode === null && proc.signalCode === null) {
    proc.kill("SIGTERM");
    // Force kill after 5s if still alive
    setTimeout(() => {
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill("SIGKILL");
      }
    }, 5_000).unref();
  }
}

export async function killAgentAndWait(
  proc: ChildProcess,
  timeoutMs = 6_000,
  options: {
    platform?: NodeJS.Platform;
    killWindowsProcessTree?: (pid: number, timeoutMs: number) => Promise<void>;
  } = {},
): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;

  if ((options.platform ?? process.platform) === "win32") {
    if (proc.pid === undefined) {
      throw new Error("Cannot stop agent process tree without a process ID");
    }
    try {
      await (
        options.killWindowsProcessTree ?? killWindowsProcessTree
      )(proc.pid, timeoutMs);
    } catch (err) {
      if (proc.exitCode === null && proc.signalCode === null) {
        throw err;
      }
    }
    return;
  }

  let settle!: () => void;
  const exited = new Promise<void>((resolve) => {
    settle = resolve;
    proc.once("exit", settle);
    proc.once("close", settle);
  });
  killAgent(proc);
  if (proc.exitCode !== null || proc.signalCode !== null) {
    proc.off("exit", settle);
    proc.off("close", settle);
    return;
  }

  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      exited,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`Agent process did not exit within ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
    proc.off("exit", settle);
    proc.off("close", settle);
  }
}

async function killWindowsProcessTree(
  pid: number,
  timeoutMs: number,
): Promise<void> {
  const taskkill = spawn(
    "taskkill.exe",
    ["/PID", String(pid), "/T", "/F"],
    {
      stdio: ["ignore", "ignore", "pipe"],
      windowsHide: true,
    },
  );
  let stderr = "";
  taskkill.stderr?.setEncoding("utf8");
  taskkill.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
  });

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (err?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      taskkill.off("error", onError);
      taskkill.off("close", onClose);
      if (err) reject(err);
      else resolve();
    };
    const onError = (err: Error) => {
      finish(new Error(`Failed to start taskkill for agent process ${pid}`, {
        cause: err,
      }));
    };
    const onClose = (code: number | null) => {
      if (code === 0) {
        finish();
        return;
      }
      const detail = stderr.trim();
      finish(
        new Error(
          `taskkill failed for agent process ${pid} with exit code ${String(code)}${detail ? `: ${detail}` : ""}`,
        ),
      );
    };
    const timeout = setTimeout(() => {
      taskkill.kill();
      finish(
        new Error(
          `Agent process tree ${pid} did not exit within ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    timeout.unref();
    taskkill.once("error", onError);
    taskkill.once("close", onClose);
  });
}

function abortable<T>(
  operation: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (!signal) return operation;
  if (signal.aborted) return Promise.reject(new Error("Agent spawn aborted"));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(new Error("Agent spawn aborted"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      },
    );
  });
}
