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
const token = "11111111-1111-4111-8111-111111111111";
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
  let groupAlive = true;
  const messageListeners: Array<(message: unknown) => void> = [];
  const exitListeners: Array<(code: number | null, signal: string | null) => void> = [];
  const errorListeners: Array<(error: Error) => void> = [];
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
    onError: (listener) => errorListeners.push(listener),
    groupAbsent: () => !groupAlive,
    signal: (signal) => signals.push(signal),
  };
  return {
    child,
    ready: received.promise,
    sent,
    signals,
    reply: (message: Record<string, unknown>) => {
      const job = (sent[0] as { job?: { token?: string } } | undefined)?.job;
      messageListeners.forEach((listener) => listener({ jobId: job?.token, ...message }));
    },
    error: (error: Error) => errorListeners.forEach((listener) => listener(error)),
    groupAlive: (alive: boolean) => { groupAlive = alive; },
    leaderExit: (code: number | null, signal: string | null = null) => exitListeners.forEach((listener) => listener(code, signal)),
    exit: (code: number | null, signal: string | null = null) => {
      groupAlive = false;
      exitListeners.forEach((listener) => listener(code, signal));
    },
  };
}

const reclaim = { kind: "reclaim", token, updatesRoot: "/unused", handle } as const;
const prepareJob = { kind: "prepare", token, platform: "darwin", roots: { artifacts: "/unused", apps: "/unused" }, candidate: {} } as unknown as Extract<import("./stagingJobs.ts").JonesStagingJob, { kind: "prepare" }>;

describe("createJonesStagingWorker", () => {
  it("keeps the queue occupied after a result until actual exit", async () => {
    const scripts = [fakeChild(), fakeChild()]; let spawned = 0;
    const worker = createJonesStagingWorker(() => scripts[spawned++]!.child);
    const first = worker.run(reclaim), second = worker.run({ ...reclaim, token: "22222222-2222-4222-8222-222222222222" });
    await scripts[0]!.ready;
    expect(scripts[0]!.sent).toEqual([{ protocol: 2, job: reclaim }]);
    scripts[0]!.reply({ type: "result", value: { status: "absent" } });
    await Promise.resolve(); expect(spawned).toBe(1);
    scripts[0]!.exit(0); await expect(first).resolves.toEqual({ status: "absent" });
    await scripts[1]!.ready; scripts[1]!.exit(1);
    await expect(second).rejects.toThrow("exited before replying (code 1)");
  });

  it("does not treat an IPC error or a spawned child error as exit", async () => {
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child);
    const pending = worker.run(reclaim); let settled = false;
    void pending.then(() => { settled = true; }, () => { settled = true; });
    await child.ready;
    child.reply({ type: "error", error: { name: "JonesActionsError", message: "blocked", reason: "integrity" } });
    await Promise.resolve(); expect(settled).toBe(false);
    child.exit(1);
    await expect(pending).rejects.toBeInstanceOf(JonesActionsError);
    expect(worker.reaped(token)).toBe(true);
  });

  it("holds the slot on a child error with a PID and rejects only after that child exits", async () => {
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child);
    const pending = worker.run(reclaim); await child.ready;
    child.error(Object.assign(new Error("missing"), { code: "ENOENT" }));
    expect(worker.reaped(token)).toBe(false);
    child.exit(1); await expect(pending).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("stops before admission, escalates only the captured preparation group and holds an unproved exit", async () => {
    vi.useFakeTimers();
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child, { stopGraceMs: 15, reapMs: 5 });
    try {
      const pending = worker.run(prepareJob); const failure = expect(pending).rejects.toMatchObject({ name: "JonesStagingExitUnproved" });
      await child.ready;
      expect(await worker.cancel("wrong-token")).toBe("not-running");
      const stopping = worker.cancel(token);
      expect(child.signals).toEqual(["SIGTERM"]);
      child.reply({ type: "admit-request" });
      expect(child.sent.at(-1)).toEqual({ type: "admit", jobId: token, decision: "stop" });
      await vi.advanceTimersByTimeAsync(15); expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      await vi.advanceTimersByTimeAsync(5); expect(await stopping).toBe("exit-unproved"); await failure;
      expect(worker.heldWorker()).toEqual({ pid: 4242, processGroup: 4242 });
      await expect(worker.run(reclaim)).rejects.toThrow("exit is unproved");
      child.exit(null, "SIGKILL"); expect(worker.reaped(token)).toBe(true); expect(worker.heldWorker()).toBeUndefined();
    } finally { child.exit(null, "SIGKILL"); await worker.close(); vi.useRealTimers(); }
  });

  it("grants mount/copy exactly once and rejects Stop after admission, including a delayed phase", async () => {
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child);
    const verifying = vi.fn(); const pending = worker.run(prepareJob, verifying); await child.ready;
    child.reply({ type: "phase", phase: "verifying", jobId: "stale" });
    expect(verifying).not.toHaveBeenCalled();
    child.reply({ type: "admit-request" });
    await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
    expect(verifying).toHaveBeenCalledTimes(1);
    expect(child.sent.at(-1)).toEqual({ type: "admit", jobId: token, decision: "continue" });
    expect(await worker.cancel(token)).toBe("not-stoppable-phase"); expect(child.signals).toEqual([]);
    child.reply({ type: "admit-request" }); expect(verifying).toHaveBeenCalledTimes(1);
    child.reply({ type: "result", value: {} }); expect(await worker.cancel(token)).toBe("completed");
    child.exit(0); await pending;
  });

  it("holds Stop and queued work when the leader exits before its group, then releases only on positive absence", async () => {
    vi.useFakeTimers();
    const children = [fakeChild(), fakeChild()]; let spawned = 0;
    const worker = createJonesStagingWorker(() => children[spawned++]!.child, { stopGraceMs: 15, reapMs: 5 });
    const child = children[0]!;
    try {
      const failure = expect(worker.run(prepareJob)).rejects.toMatchObject({ name: "JonesStagingExitUnproved", copyAdmitted: false });
      const queued = worker.run({ ...reclaim, token: "22222222-2222-4222-8222-222222222222" });
      await child.ready;
      const stopping = worker.cancel(token); let settled = false;
      void stopping.then(() => { settled = true; });
      await vi.advanceTimersByTimeAsync(10); child.leaderExit(null, "SIGTERM");
      expect(worker.reaped(token)).toBe(false); expect(spawned).toBe(1); expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(5);
      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      await vi.advanceTimersByTimeAsync(5);
      expect(await stopping).toBe("exit-unproved"); await failure;
      expect(worker.reaped(token)).toBe(false); expect(spawned).toBe(1);
      expect(worker.heldWorker()).toEqual({ pid: 4242, processGroup: 4242 });
      await expect(worker.run(reclaim)).rejects.toThrow("exit is unproved");
      child.groupAlive(false);
      await vi.advanceTimersByTimeAsync(100);
      expect(worker.reaped(token)).toBe(true);
      await children[1]!.ready; expect(spawned).toBe(2);
      children[1]!.reply({ type: "result", value: { status: "absent" } }); children[1]!.exit(0); await queued;
    } finally { child.groupAlive(false); child.exit(null, "SIGKILL"); children[1]!.exit(0); await worker.close(); vi.useRealTimers(); }
  });

  it.each(["EPERM", "EIO"])("retains a leader-exited group when its absence witness fails with %s", async (code) => {
    vi.useFakeTimers();
    const child = fakeChild(); let absent = false;
    const groupAbsent = () => {
      if (absent) return true;
      throw Object.assign(new Error("unproved"), { code });
    };
    const worker = createJonesStagingWorker(() => ({ ...child.child, groupAbsent }), { graceMs: 5, reapMs: 5 });
    try {
      const failure = expect(worker.run(prepareJob)).rejects.toMatchObject({ name: "JonesStagingExitUnproved" });
      await child.ready; child.leaderExit(0);
      await vi.advanceTimersByTimeAsync(10); await failure;
      expect(worker.reaped(token)).toBe(false);
      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      absent = true; expect(worker.reaped(token)).toBe(true);
    } finally { absent = true; child.exit(0); await worker.close(); vi.useRealTimers(); }
  });

  it("releases a proved never-spawned job without inferring a group from a missing PID", async () => {
    const child = fakeChild(); const groupAbsent = vi.fn(() => false);
    const worker = createJonesStagingWorker(() => ({ ...child.child, pid: undefined, groupAbsent }));
    const failure = expect(worker.run(prepareJob)).rejects.toThrow("spawn failed");
    await child.ready; child.error(new Error("spawn failed")); await failure;
    expect(worker.reaped(token)).toBe(true); expect(groupAbsent).not.toHaveBeenCalled(); await worker.close();
  });

  it("retains admitted copy uncertainty independently of group release and never forces that group", async () => {
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child);
    try {
      const failure = expect(worker.run(prepareJob)).rejects.toMatchObject({ name: "JonesStagingExitUnproved", copyAdmitted: true });
      await child.ready; child.reply({ type: "admit-request" });
      await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
      child.leaderExit(1); await failure;
      expect(worker.reaped(token)).toBe(false); expect(child.signals).toEqual([]);
      expect(await worker.cancel(token)).toBe("not-stoppable-phase");
      child.groupAlive(false); expect(worker.reaped(token)).toBe(true);
    } finally { child.groupAlive(false); child.exit(1); await worker.close(); }
  });

  it("proves cooperative Stop only on exit and remembers cancellation before a queued spawn", async () => {
    const first = fakeChild(); let spawned = 0;
    const worker = createJonesStagingWorker(() => { spawned++; return first.child; });
    const firstJob = worker.run(reclaim), secondJob = worker.run({ ...prepareJob, token: "22222222-2222-4222-8222-222222222222" });
    const stopped = expect(secondJob).rejects.toMatchObject({ name: "JonesStagingStopped" });
    await first.ready;
    expect(await worker.cancel("22222222-2222-4222-8222-222222222222")).toBe("stopped");
    first.reply({ type: "result", value: { status: "absent" } }); first.exit(0); await firstJob; await stopped;
    expect(spawned).toBe(1);
    expect(worker.reaped("22222222-2222-4222-8222-222222222222")).toBe(true);
  });

  it("bounds timeout termination without releasing the slot early", async () => {
    vi.useFakeTimers(); const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child, { jobTimeoutMs: 10, graceMs: 10, reapMs: 5 });
    try {
      const failure = expect(worker.run(reclaim)).rejects.toThrow("timed out"); await child.ready;
      await vi.advanceTimersByTimeAsync(10); expect(child.signals).toEqual(["SIGTERM"]);
      await vi.advanceTimersByTimeAsync(10); expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      child.exit(null, "SIGKILL"); await failure;
    } finally { child.exit(null, "SIGKILL"); await worker.close(); vi.useRealTimers(); }
  });

  it("keeps the existing close grace and refuses later jobs", async () => {
    const child = fakeChild(); const worker = createJonesStagingWorker(() => child.child, { graceMs: 1000 });
    const pending = worker.run(reclaim); await child.ready;
    const closing = worker.close(); expect(child.signals).toEqual(["SIGTERM"]);
    child.exit(null, "SIGTERM"); await closing;
    await expect(pending).rejects.toThrow("exited before replying (SIGTERM)");
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

it("keeps cooperative Stop pending until the captured child really exits", async () => {
  const child = fakeChild(), worker = createJonesStagingWorker(() => child.child);
  const pending = worker.run(prepareJob); const stopped = expect(pending).rejects.toMatchObject({ name: "JonesStagingStopped" });
  await child.ready;
  let settled = false;
  const cancellation = worker.cancel(token).then((result) => { settled = true; return result; });
  await Promise.resolve(); expect(settled).toBe(false); expect(worker.reaped(token)).toBe(false);
  child.exit(null, "SIGTERM");
  expect(await cancellation).toBe("stopped"); await stopped;
  expect(child.signals).toEqual(["SIGTERM"]);
});
