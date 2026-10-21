// @effect-diagnostics nodeBuiltinImport:off globalDate:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  QualifiedRuntimeBlockedError,
  withQualifiedRuntimeLock,
} from "../../cloud/qualifiedRuntime.ts";
import {
  PREPARATION_LOCK,
  preparationJobsDirectory,
  type PreparationJob,
  type PreparationPhase,
} from "./protocol.ts";
import { adoptVerifiedStage } from "./rebind.ts";
import {
  makePreparationSupervisor,
  PreparationWorkerError,
  resolvePreparationWorkerCommand,
  type PreparationWorkerExit,
} from "./supervisor.ts";
import {
  actionsFixture,
  alive,
  BASELINE,
  fakeWorkerCommand,
  fixtureHost,
  makePreparationHome,
  sourceWorkerCommand,
  stageFixtureRuntime,
  VERSION,
  type ActionsFixture,
  type PreparationHome,
} from "./Preparation.testkit.ts";

const homes: PreparationHome[] = [];
/** PIDs the stand-in tools recorded; a failed test must not leave them running. */
const spawned: number[] = [];
afterEach(async () => {
  for (const pid of spawned.splice(0))
    if (alive(pid))
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // It exited in between.
      }
  for (const home of homes.splice(0)) await home.remove();
});
async function home(): Promise<PreparationHome> {
  const created = await makePreparationHome();
  homes.push(created);
  return created;
}
const jobsLeft = (home: PreparationHome) =>
  NodeFSP.readdir(preparationJobsDirectory(home.base)).catch(() => []);
const verifyJob = (stagedHandle: string): PreparationJob => ({
  kind: "verify",
  stagedHandle,
  activeVersion: BASELINE,
  host: fixtureHost,
});
const stageJob = (home: PreparationHome, actions: ActionsFixture): PreparationJob => ({
  kind: "stage",
  candidate: actions.candidate,
  cacheRoot: home.cacheRoot,
  dbPath: home.dbPath,
  activeVersion: BASELINE,
  host: fixtureHost,
});
async function hangingGh(actions: ActionsFixture): Promise<{ gh: number; worker: number }> {
  await vi.waitFor(
    async () => expect(await NodeFSP.readFile(actions.pids, "utf8").catch(() => "")).toMatch(/\n/),
    { timeout: 20_000, interval: 25 },
  );
  const [gh, worker] = (await NodeFSP.readFile(actions.pids, "utf8"))
    .trim()
    .split(/\s+/)
    .map(Number);
  if (gh === undefined || worker === undefined)
    throw new Error("The stand-in gh recorded no PIDs.");
  spawned.push(gh, worker);
  return { gh, worker };
}
const blocked = (reason: QualifiedRuntimeBlockedError["reason"]) => (cause: unknown) =>
  cause instanceof QualifiedRuntimeBlockedError && cause.reason === reason;
const stoppedFor = (reason: PreparationWorkerError["reason"]) => (cause: unknown) =>
  cause instanceof PreparationWorkerError && cause.reason === reason;

describe("preparation worker command", () => {
  it("starts a single executable as itself and a script install through its entry", () => {
    expect(
      resolvePreparationWorkerCommand({
        isExecutable: true,
        execPath: "/opt/jones/t3",
        entryPath: undefined,
        niceAvailable: true,
      }),
    ).toEqual({ file: "/usr/bin/nice", args: ["-n", "19", "/opt/jones/t3"] });
    expect(
      resolvePreparationWorkerCommand({
        isExecutable: false,
        execPath: "/usr/bin/node",
        entryPath: "/srv/jones/dist/bin.mjs",
        entryExists: () => true,
        niceAvailable: false,
      }),
    ).toEqual({ file: "/usr/bin/node", args: ["/srv/jones/dist/bin.mjs"] });
    for (const entryPath of [undefined, "dist/bin.mjs"])
      expect(
        resolvePreparationWorkerCommand({
          isExecutable: false,
          execPath: "/usr/bin/node",
          entryPath,
          entryExists: () => true,
        }),
      ).toBeUndefined();
    expect(
      resolvePreparationWorkerCommand({
        isExecutable: false,
        execPath: "/usr/bin/node",
        entryPath: "/srv/jones/dist/bin.mjs",
        entryExists: () => false,
      }),
    ).toBeUndefined();
  });
});

describe("real preparation worker", () => {
  it("verifies a staged runtime in its own low-priority process and leaves no job storage", async () => {
    const fixture = await home();
    const staged = await stageFixtureRuntime(fixture);
    const exits: PreparationWorkerExit[] = [];
    const phases: PreparationPhase[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      observe: (exit) => exits.push(exit),
    });
    const outcome = await supervisor.run(verifyJob(staged.stagedHandle), {
      onPhase: (phase) => phases.push(phase),
    });
    expect(outcome).toEqual({ kind: "verify", staged });
    if (outcome.kind !== "verify") throw new Error("Expected a verify outcome.");
    await expect(
      adoptVerifiedStage({
        baseDir: fixture.base,
        activeVersion: BASELINE,
        stagedHandle: staged.stagedHandle,
        staged: outcome.staged,
      }),
    ).resolves.toEqual(staged);
    expect(phases).toEqual(["verifying"]);
    expect(exits).toHaveLength(1);
    expect(exits[0]).toMatchObject({
      kind: "verify",
      code: 0,
      signal: null,
      stopped: undefined,
      reaped: true,
      priority: NodeOS.constants.priority.PRIORITY_LOW,
    });
    expect(exits[0]?.pid).not.toBe(process.pid);
    expect(await jobsLeft(fixture)).toEqual([]);
  });

  it("reports a tampered staged runtime as an integrity failure and sweeps after it", async () => {
    const fixture = await home();
    const staged = await stageFixtureRuntime(fixture);
    await NodeFSP.writeFile(
      NodePath.join(fixture.base, "runtime", "versions", VERSION, "client", "index.html"),
      "tampered",
    );
    const exits: PreparationWorkerExit[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      observe: (exit) => exits.push(exit),
    });
    await expect(supervisor.run(verifyJob(staged.stagedHandle))).rejects.toSatisfy(
      blocked("integrity-mismatch"),
    );
    await vi.waitFor(() => expect(exits.map((exit) => exit.kind)).toEqual(["verify", "sweep"]), {
      timeout: 20_000,
    });
    await supervisor.sweep();
    expect(await jobsLeft(fixture)).toEqual([]);
  });

  it("refuses to run while another process owns the home's preparation lock", async () => {
    const fixture = await home();
    const staged = await stageFixtureRuntime(fixture);
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
    });
    await withQualifiedRuntimeLock(fixture.base, PREPARATION_LOCK, () =>
      expect(supervisor.run(verifyJob(staged.stagedHandle))).rejects.toSatisfy(blocked("busy")),
    );
    await expect(supervisor.run(verifyJob(staged.stagedHandle))).resolves.toMatchObject({
      kind: "verify",
    });
    await supervisor.sweep();
    expect(await jobsLeft(fixture)).toEqual([]);
  });

  it("runs one worker at a time for a host", async () => {
    const fixture = await home();
    const staged = await stageFixtureRuntime(fixture);
    const spans: Array<readonly [number, number]> = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      observe: (exit) => spans.push([Date.now() - exit.durationMs, Date.now()]),
    });
    await Promise.all([
      supervisor.run(verifyJob(staged.stagedHandle)),
      supervisor.run(verifyJob(staged.stagedHandle)),
      supervisor.sweep(),
    ]);
    expect(spans).toHaveLength(3);
    for (let index = 1; index < spans.length; index += 1)
      expect(spans[index]![0]).toBeGreaterThanOrEqual(spans[index - 1]![1]);
  });

  it("cancels a download by stopping the worker's whole group, then sweeps its storage", async () => {
    const fixture = await home();
    const actions = await actionsFixture(fixture, { hang: true });
    const exits: PreparationWorkerExit[] = [];
    const phases: PreparationPhase[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      env: actions.env,
      observe: (exit) => exits.push(exit),
    });
    const controller = new AbortController();
    const running = supervisor.run(stageJob(fixture, actions), {
      signal: controller.signal,
      onPhase: (phase) => phases.push(phase),
    });
    const { gh, worker } = await hangingGh(actions);
    expect(phases).toEqual(["downloading"]);
    const cancelled = Date.now();
    controller.abort(new Error("superseded by a newer target"));
    await expect(running).rejects.toThrow("superseded by a newer target");
    // The caller is released at once; the queue still reaps the worker behind it.
    expect(Date.now() - cancelled).toBeLessThan(1_000);
    await vi.waitFor(() => expect(exits.map((exit) => exit.kind)).toEqual(["stage", "sweep"]), {
      timeout: 20_000,
    });
    await supervisor.sweep();
    expect(exits[0]).toMatchObject({ pid: worker, stopped: "cancelled", reaped: true });
    expect(alive(worker)).toBe(false);
    expect(alive(gh)).toBe(false);
    expect(await jobsLeft(fixture)).toEqual([]);
    expect(
      await NodeFSP.readdir(NodePath.join(fixture.cacheRoot, "attempts")).catch(() => []),
    ).toEqual([]);
  });

  it("stops by itself when its server dies, and the next server sweeps what it left", async () => {
    const fixture = await home();
    const actions = await actionsFixture(fixture, { hang: true });
    const config = NodePath.join(fixture.tools, "parent.json");
    await NodeFSP.writeFile(
      config,
      JSON.stringify({
        supervisor: new URL("./supervisor.ts", import.meta.url).href,
        baseDir: fixture.base,
        cacheRoot: fixture.cacheRoot,
        command: sourceWorkerCommand,
        job: stageJob(fixture, actions),
      }),
    );
    const script = NodePath.join(fixture.tools, "parent.mjs");
    await NodeFSP.writeFile(
      script,
      `import * as fs from "node:fs";
const config = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { makePreparationSupervisor } = await import(config.supervisor);
makePreparationSupervisor({ baseDir: config.baseDir, cacheRoot: config.cacheRoot, command: config.command })
  .run(config.job)
  .catch(() => {});
setInterval(() => {}, 1000);
`,
    );
    const parent = NodeChildProcess.spawn(process.execPath, [script, config], {
      env: actions.env,
      stdio: "ignore",
    });
    if (parent.pid !== undefined) spawned.push(parent.pid);
    const parentExit = new Promise((resolve) => parent.once("exit", resolve));
    const { gh, worker } = await hangingGh(actions);
    parent.kill("SIGKILL");
    await parentExit;
    await vi.waitFor(
      () => {
        expect(alive(worker)).toBe(false);
        expect(alive(gh)).toBe(false);
      },
      { timeout: 20_000, interval: 50 },
    );
    const [jobId, ...others] = await jobsLeft(fixture);
    expect(others).toEqual([]);
    const result = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(preparationJobsDirectory(fixture.base), String(jobId), "result.json"),
        "utf8",
      ),
    ) as unknown;
    expect(result).toMatchObject({
      ok: false,
      error: { message: "Update preparation stopped because its server exited." },
    });
    const next = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
    });
    await expect(next.sweep()).resolves.toMatchObject({ kind: "sweep", removed: 1, failed: 0 });
    expect(await jobsLeft(fixture)).toEqual([]);
  });
});

describe("misbehaving preparation workers", () => {
  const limits = {
    heartbeatTimeoutMs: 400,
    graceMs: 300,
    reapMs: 2_000,
    deadlineMs: { sweep: 1_500 },
  };
  async function fake(mode: string) {
    const fixture = await home();
    const exits: PreparationWorkerExit[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: await fakeWorkerCommand(fixture),
      env: { ...process.env, FAKE_WORKER_MODE: mode, FAKE_WORKER_RECORD: fixture.tools },
      limits,
      observe: (exit) => exits.push(exit),
    });
    const grandchild = async () => {
      const pid = Number(
        await NodeFSP.readFile(NodePath.join(fixture.tools, "grandchild"), "utf8"),
      );
      spawned.push(pid);
      return pid;
    };
    return { fixture, exits, supervisor, grandchild };
  }

  it.each([
    ["stops heartbeating and ignores SIGTERM", "stall", "stalled"],
    ["floods its event stream", "flood", "output-limit"],
    ["writes an unbounded event line", "long-line", "output-limit"],
    ["claims another process's identity", "bad-ready", "protocol"],
    ["exits without a result", "crash", "crashed"],
    ["outlives its deadline", "heartbeat", "deadline"],
  ] as const)("stops a worker that %s", async (_label, mode, reason) => {
    const { fixture, exits, supervisor, grandchild } = await fake(mode);
    await expect(supervisor.sweep()).rejects.toSatisfy(stoppedFor(reason));
    expect(exits).toHaveLength(1);
    expect(exits[0]).toMatchObject({ reaped: true });
    if (mode === "stall") {
      expect(exits[0]?.signal).toBe("SIGKILL");
      const pid = await grandchild();
      await vi.waitFor(() => expect(alive(pid)).toBe(false), { timeout: 5_000 });
    }
    expect(await jobsLeft(fixture)).toEqual([]);
  });

  it("kills a group member the worker left behind after a successful result", async () => {
    const { fixture, supervisor, grandchild } = await fake("orphan");
    await expect(supervisor.sweep()).resolves.toEqual({
      kind: "sweep",
      removed: 0,
      retained: 0,
      failed: 0,
    });
    const pid = await grandchild();
    await vi.waitFor(() => expect(alive(pid)).toBe(false), { timeout: 5_000 });
    expect(await jobsLeft(fixture)).toEqual([]);
  });

  it("reports a worker that cannot start", async () => {
    const fixture = await home();
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: { file: NodePath.join(fixture.tools, "missing-worker"), args: [] },
    });
    await expect(supervisor.sweep()).rejects.toSatisfy(stoppedFor("spawn-failed"));
    expect(await jobsLeft(fixture)).toEqual([]);
  });
});
