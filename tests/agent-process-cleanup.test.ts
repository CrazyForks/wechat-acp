import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

import { killAgentAndWait } from "../src/acp/agent-manager.js";

function makeRunningProcess(pid: number, signals: string[]): ChildProcess {
  const proc = new EventEmitter() as ChildProcess;
  Object.defineProperties(proc, {
    pid: { value: pid },
    exitCode: { value: null },
    signalCode: { value: null },
  });
  proc.kill = (signal: NodeJS.Signals | number = "SIGTERM") => {
    signals.push(String(signal));
    return true;
  };
  return proc;
}

test("Windows cleanup awaits the process-tree terminator", async () => {
  const wrapperSignals: string[] = [];
  const proc = makeRunningProcess(4242, wrapperSignals);
  const calls: Array<{ pid: number; timeoutMs: number }> = [];

  await killAgentAndWait(proc, 1234, {
    platform: "win32",
    killWindowsProcessTree: async (pid, timeoutMs) => {
      calls.push({ pid, timeoutMs });
    },
  });

  assert.deepEqual(calls, [{ pid: 4242, timeoutMs: 1234 }]);
  assert.deepEqual(wrapperSignals, []);
});

test("Windows cleanup surfaces process-tree termination failures", async () => {
  const proc = makeRunningProcess(4242, []);

  await assert.rejects(
    killAgentAndWait(proc, 1234, {
      platform: "win32",
      killWindowsProcessTree: async () => {
        throw new Error("tree termination failed");
      },
    }),
    /tree termination failed/,
  );
});

test("Windows cleanup retains taskkill failure if the wrapper exits before retry", async () => {
  const state = { exitCode: null as number | null };
  const proc = new EventEmitter() as ChildProcess;
  Object.defineProperties(proc, {
    pid: { value: 4242 },
    exitCode: { get: () => state.exitCode },
    signalCode: { value: null },
  });
  let attempts = 0;

  await assert.rejects(
    killAgentAndWait(proc, 1234, {
      platform: "win32",
      killWindowsProcessTree: async () => {
        attempts++;
        throw new Error("tree termination failed");
      },
    }),
    /tree termination failed/,
  );
  state.exitCode = 0;
  await assert.rejects(
    killAgentAndWait(proc, 1234, {
      platform: "win32",
      killWindowsProcessTree: async () => {
        attempts++;
      },
    }),
    /tree termination failed/,
  );
  assert.equal(attempts, 1);
});

test("Windows cleanup retries taskkill while the wrapper is running", async () => {
  const proc = makeRunningProcess(4242, []);
  let attempts = 0;
  const cleanup = async () => {
    attempts++;
    if (attempts === 1) {
      throw new Error("tree termination failed");
    }
  };

  await assert.rejects(
    killAgentAndWait(proc, 1234, {
      platform: "win32",
      killWindowsProcessTree: cleanup,
    }),
    /tree termination failed/,
  );
  await killAgentAndWait(proc, 1234, {
    platform: "win32",
    killWindowsProcessTree: cleanup,
  });
  assert.equal(attempts, 2);
});

test("Windows cleanup accepts a natural wrapper exit without a taskkill failure", async () => {
  const state = { exitCode: 0 as number | null };
  const proc = new EventEmitter() as ChildProcess;
  Object.defineProperties(proc, {
    pid: { value: 4242 },
    exitCode: { get: () => state.exitCode },
    signalCode: { value: null },
  });

  await killAgentAndWait(proc, 1234, { platform: "win32" });
});

test("POSIX cleanup targets the process group after wrapper exit", async () => {
  const state = { exitCode: 0 as number | null };
  const proc = new EventEmitter() as ChildProcess;
  Object.defineProperties(proc, {
    pid: { value: 4242 },
    exitCode: { get: () => state.exitCode },
    signalCode: { value: null },
  });
  const calls: Array<{ pid: number; timeoutMs: number }> = [];

  await killAgentAndWait(proc, 1234, {
    platform: "linux",
    killPosixProcessGroup: async (pid, timeoutMs) => {
      calls.push({ pid, timeoutMs });
    },
  });

  assert.deepEqual(calls, [{ pid: 4242, timeoutMs: 1234 }]);
});

test(
  "Windows cleanup terminates a real shell process tree",
  { skip: process.platform !== "win32" },
  async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "wechat-acp-tree-"));
    const scriptPath = path.join(dir, "agent.js");
    const pidPath = path.join(dir, "agent.pid");
    await fs.writeFile(
      scriptPath,
      'require("node:fs").writeFileSync(process.argv[2], String(process.pid)); setInterval(() => {}, 1000);',
    );
    const wrapper = spawn("node", [scriptPath, pidPath], {
      shell: true,
      stdio: "ignore",
      windowsHide: true,
    });
    let agentPid: number | undefined;

    try {
      agentPid = Number(await waitForFile(pidPath));
      assert.equal(Number.isInteger(agentPid), true);
      assert.notEqual(agentPid, wrapper.pid);

      await killAgentAndWait(wrapper, 5_000);
      await waitForExit(agentPid);
      assert.equal(isProcessRunning(agentPid), false);
    } finally {
      if (agentPid !== undefined && isProcessRunning(agentPid)) {
        process.kill(agentPid, "SIGKILL");
      }
      if (wrapper.exitCode === null && wrapper.signalCode === null) {
        wrapper.kill("SIGKILL");
      }
      await fs.rm(dir, { recursive: true, force: true });
    }
  },
);

test(
  "POSIX cleanup terminates descendants after the wrapper exits",
  { skip: process.platform === "win32" },
  async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "wechat-acp-group-"));
    const scriptPath = path.join(dir, "wrapper.cjs");
    const pidPath = path.join(dir, "agent.pid");
    await fs.writeFile(
      scriptPath,
      [
        'const { spawn } = require("node:child_process");',
        'const fs = require("node:fs");',
        'const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });',
        'fs.writeFileSync(process.argv[2], String(child.pid));',
        "child.unref();",
      ].join("\n"),
    );
    const wrapper = spawn(process.execPath, [scriptPath, pidPath], {
      detached: true,
      stdio: "ignore",
    });
    let agentPid: number | undefined;

    try {
      agentPid = Number(await waitForFile(pidPath));
      assert.equal(Number.isInteger(agentPid), true);
      if (wrapper.exitCode === null) {
        await once(wrapper, "exit");
      }
      assert.equal(isProcessRunning(agentPid), true);

      await killAgentAndWait(wrapper, 2_000);
      await waitForExit(agentPid);
      assert.equal(isProcessRunning(agentPid), false);
    } finally {
      if (agentPid !== undefined && isProcessRunning(agentPid)) {
        process.kill(agentPid, "SIGKILL");
      }
      if (wrapper.exitCode === null && wrapper.signalCode === null) {
        wrapper.kill("SIGKILL");
      }
      await fs.rm(dir, { recursive: true, force: true });
    }
  },
);

async function waitForFile(filePath: string): Promise<string> {
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      return await fs.readFile(filePath, "utf8");
    } catch (err) {
      if (
        typeof err !== "object" ||
        err === null ||
        !("code" in err) ||
        err.code !== "ENOENT"
      ) {
        throw err;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  throw new Error("Timed out waiting for the agent process ID");
}

async function waitForExit(pid: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (!isProcessRunning(pid)) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for agent process ${pid} to exit`);
}

function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    if (
      typeof err === "object" &&
      err !== null &&
      "code" in err &&
      err.code === "ESRCH"
    ) {
      return false;
    }
    throw err;
  }
}
