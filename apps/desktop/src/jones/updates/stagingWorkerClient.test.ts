// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Forks the real worker entry and drives its timers outside Effect.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  JONES_ACTIONS_RECEIPT_FILE,
  JonesActionsError,
  createJonesStageAttempt,
  publishJonesCompletedStage,
} from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  createJonesStagingWorker,
  forkJonesStagingChild,
  type JonesStagingChild,
} from "./stagingWorkerClient.ts";

const handle = "a".repeat(64);
const workerEntry = NodePath.join(import.meta.dirname, "jonesStagingWorker.ts");

const fixtures = new Set<string>();
afterEach(async () => {
  for (const root of fixtures) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    fixtures.delete(root);
  }
});

/** A scripted child: the test decides what it replies and when it exits. */
function fakeChild() {
  const messageListeners: Array<(message: unknown) => void> = [];
  const exitListeners: Array<(code: number | null, signal: string | null) => void> = [];
  const received = Promise.withResolvers<void>();
  const sent: unknown[] = [];
  const signals: string[] = [];
  const child: JonesStagingChild = {
    pid: 4242,
    send: (message) => {
      sent.push(message);
      received.resolve();
    },
    onMessage: (listener) => messageListeners.push(listener),
    onExit: (listener) => exitListeners.push(listener),
    signal: (signal) => signals.push(signal),
  };
  return {
    child,
    ready: received.promise,
    sent,
    signals,
    reply: (message: unknown) => messageListeners.forEach((listener) => listener(message)),
    exit: (code: number | null, signal: string | null = null) =>
      exitListeners.forEach((listener) => listener(code, signal)),
  };
}

const reclaim = { kind: "reclaim", updatesRoot: "/unused", handle } as const;

describe("createJonesStagingWorker", () => {
  it("runs one job at a time, each in its own child", async () => {
    const scripts = [fakeChild(), fakeChild()] as const;
    const children: Array<ReturnType<typeof fakeChild>> = [];
    const worker = createJonesStagingWorker(() => {
      const next = scripts[children.length]!;
      children.push(next);
      return next.child;
    });
    const first = worker.run(reclaim);
    const second = worker.run(reclaim);
    await scripts[0].ready;
    expect(children).toHaveLength(1);
    expect(children[0]!.sent).toEqual([{ protocol: 1, job: reclaim }]);

    children[0]!.reply({ type: "result", value: { status: "absent" } });
    children[0]!.exit(0);
    await expect(first).resolves.toEqual({ status: "absent" });
    await scripts[1].ready;
    expect(children).toHaveLength(2);
    children[1]!.exit(1);
    await expect(second).rejects.toThrow("exited before replying (code 1)");
  });

  it("forwards the verifying phase and rebuilds typed failures", async () => {
    const scripts = [fakeChild(), fakeChild()] as const;
    const children: Array<ReturnType<typeof fakeChild>> = [];
    const worker = createJonesStagingWorker(() => {
      const next = scripts[children.length]!;
      children.push(next);
      return next.child;
    });
    const phases: string[] = [];
    const prepare = worker.run(reclaim, () => {
      phases.push("verifying");
    });
    await scripts[0].ready;
    children[0]!.reply({ type: "phase", phase: "verifying" });
    children[0]!.reply({
      type: "error",
      error: { name: "JonesActionsError", message: "blocked", reason: "integrity" },
    });
    const failure = await prepare.catch((cause: unknown) => cause);
    expect(phases).toEqual(["verifying"]);
    expect(failure).toBeInstanceOf(JonesActionsError);
    expect((failure as JonesActionsError).reason).toBe("integrity");

    const coded = worker.run(reclaim);
    await scripts[1].ready;
    children[1]!.reply({
      type: "error",
      error: { name: "Error", message: "missing", code: "ENOENT" },
    });
    await expect(coded).rejects.toMatchObject({ message: "missing", code: "ENOENT" });
  });

  it("terminates a timed-out job's process group, escalating after the grace period", async () => {
    const child = fakeChild();
    const worker = createJonesStagingWorker(() => child.child, { jobTimeoutMs: 10, graceMs: 10 });
    vi.useFakeTimers();
    try {
      const timedOut = expect(worker.run(reclaim)).rejects.toThrow("timed out");
      await child.ready;
      await vi.advanceTimersByTimeAsync(10);
      await timedOut;
      expect(child.signals).toEqual(["SIGTERM"]);
      await vi.advanceTimersByTimeAsync(30);
      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
    } finally {
      child.exit(null, "SIGKILL");
      await worker.close();
      vi.useRealTimers();
    }
  });

  it("stops the running job on close and refuses later jobs", async () => {
    const child = fakeChild();
    const worker = createJonesStagingWorker(() => child.child, { graceMs: 1000 });
    const running = worker.run(reclaim);
    await child.ready;
    const closing = worker.close();
    expect(child.signals).toEqual(["SIGTERM"]);
    child.exit(null, "SIGTERM");
    await closing;
    await expect(running).rejects.toThrow("exited before replying (SIGTERM)");
    expect(child.signals).toEqual(["SIGTERM"]);
    await expect(worker.run(reclaim)).rejects.toThrow("closed");
  });
});

describe("jonesStagingWorker process", () => {
  it("reclaims a replaced payload in a separate process and reports refusals", async () => {
    const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
    await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
    const home = await NodeFSP.mkdtemp(NodePath.join(parent, "staging-worker-"));
    fixtures.add(home);
    const updatesRoot = NodePath.join(home, "jones-updates");
    const artifacts = NodePath.join(updatesRoot, "artifacts");
    await NodeFSP.mkdir(artifacts, { recursive: true });
    const attempt = await createJonesStageAttempt(artifacts, handle);
    await NodeFSP.writeFile(NodePath.join(attempt.directory, "qualified.dmg"), "dmg");
    await NodeFSP.writeFile(NodePath.join(attempt.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
    await publishJonesCompletedStage(
      artifacts,
      handle,
      attempt.directory,
      JONES_ACTIONS_RECEIPT_FILE,
    );

    const pids: Array<number | undefined> = [];
    const worker = createJonesStagingWorker(() => {
      const child = forkJonesStagingChild(workerEntry, {
        execPath: process.execPath,
        electron: false,
      });
      pids.push(child.pid);
      return child;
    });
    try {
      await expect(worker.run({ kind: "reclaim", updatesRoot, handle })).resolves.toMatchObject({
        status: "reclaimed",
      });
      await expect(NodeFSP.lstat(attempt.attemptRoot)).rejects.toMatchObject({ code: "ENOENT" });
      await expect(
        worker.run({ kind: "reclaim", updatesRoot, handle: "../outside" }),
      ).rejects.toThrow("Invalid Jones stage handle.");
      expect(pids).toHaveLength(2);
      expect(pids.every((pid) => pid !== undefined && pid !== process.pid)).toBe(true);
    } finally {
      await worker.close();
    }
  });
});
