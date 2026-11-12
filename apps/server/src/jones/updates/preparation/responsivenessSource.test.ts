// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { expect, it, onTestFinished } from "vite-plus/test";
import { preparationLifecycleJobIds } from "./lifecycle.ts";
import {
  BASELINE,
  fixtureHost,
  makePreparationHome,
  sourceWorkerCommand,
} from "./Preparation.testkit.ts";
import {
  makePreparationSupervisor,
  type PreparationJoin,
  type PreparationWorkerExit,
} from "./supervisor.ts";
import {
  decodeResponsivenessExit,
  decodeResponsivenessFrame,
  joinResponsivenessJobs,
  PAYLOAD_BYTES,
  ResponsivenessSourceError,
  startResponsivenessSource,
} from "./responsivenessSource.ts";

const id = "b00b7327-997d-4101-b3f0-41fb3538589c";
const cost = {
  elapsedMs: 100,
  activeMs: 10,
  cpuMs: 10,
  utilization: 0.1,
  maxDelayMs: 1,
  ticks: 20,
  expectedTicks: 20,
};
const frame = () => ({
  schema: 1,
  status: "measured",
  payloadBytes: PAYLOAD_BYTES,
  worker: { ...cost },
  inProcess: { ...cost },
  homeRoot: "/synthetic/home",
  cleanup: {
    removed: true,
    jobs: [{ jobId: id, kind: "completed" }],
    workers: [{ jobId: id, kind: "verify", pid: 12345, code: 0, signal: null, reaped: true }],
  },
});

it("validates a bounded measurement and rejects malformed, extra, invalid or uncertain evidence", () => {
  // Decoder data only; this fixture never supplies the performance comparison.
  expect(decodeResponsivenessExit(JSON.stringify(frame()), 0, null)).toEqual(frame());
  for (const invalid of [
    "",
    "{}",
    JSON.stringify(frame()) + JSON.stringify(frame()),
    "x".repeat(65537),
    JSON.stringify({ ...frame(), unexpected: true }),
    JSON.stringify({ ...frame(), payloadBytes: 1 }),
    JSON.stringify({ ...frame(), worker: { ...cost, expectedTicks: 19 } }),
    JSON.stringify({ ...frame(), worker: { ...cost, activeMs: null } }),
    JSON.stringify({ ...frame(), worker: { ...cost, cpuMs: null } }),
    JSON.stringify({ ...frame(), worker: { ...cost, utilization: 2 } }),
    JSON.stringify({ ...frame(), cleanup: { ...frame().cleanup, removed: false } }),
    JSON.stringify({
      ...frame(),
      cleanup: { ...frame().cleanup, jobs: [{ jobId: id, kind: "unreaped" }] },
    }),
    JSON.stringify({ ...frame(), cleanup: { ...frame().cleanup, workers: [] } }),
    JSON.stringify({
      ...frame(),
      cleanup: { ...frame().cleanup, workers: [{ ...frame().cleanup.workers[0], reaped: false }] },
    }),
    JSON.stringify({
      ...frame(),
      cleanup: {
        ...frame().cleanup,
        workers: [{ ...frame().cleanup.workers[0], jobId: NodeCrypto.randomUUID() }],
      },
    }),
  ])
    expect(() => decodeResponsivenessFrame(invalid)).toThrow();
  expect(() => decodeResponsivenessExit(JSON.stringify(frame()), 1, null)).toThrow();
  expect(() => decodeResponsivenessExit(JSON.stringify(frame()), 0, "SIGTERM")).toThrow();
});

it("does not enumerate or clean material until the captured settlement resolves", async () => {
  const home = await makePreparationHome();
  const supervisor = makePreparationSupervisor({
    baseDir: home.base,
    cacheRoot: home.cacheRoot,
    command: sourceWorkerCommand,
    env: { ...process.env, XDG_CACHE_HOME: NodePath.join(home.root, "compile-cache") },
  });
  let resolve!: (join: PreparationJoin) => void;
  const settled = new Promise<PreparationJoin>((done) => {
    resolve = done;
  });
  let enumerated = false;
  const joining = joinResponsivenessJobs({
    supervisor,
    captured: [settled],
    failed: false,
    cacheRoot: home.cacheRoot,
    jobIds: async () => {
      enumerated = true;
      return [];
    },
  });
  await Promise.resolve();
  expect(enumerated).toBe(false);
  resolve({ jobId: id, kind: "not-started" });
  expect(await joining).toEqual([]);
  expect(enumerated).toBe(true);
  await home.remove();
});

it("joins a real failed source job and its queued automatic sweep before the final queue barrier", async () => {
  const home = await makePreparationHome();
  const exits: PreparationWorkerExit[] = [];
  const supervisor = makePreparationSupervisor({
    baseDir: home.base,
    cacheRoot: home.cacheRoot,
    command: sourceWorkerCommand,
    env: { ...process.env, XDG_CACHE_HOME: NodePath.join(home.root, "compile-cache") },
    observe: (exit) => exits.push(exit),
  });
  const running = supervisor.start(
    {
      kind: "verify",
      stagedHandle: "not-a-staged-handle",
      activeVersion: BASELINE,
      host: fixtureHost,
    },
    { jobId: NodeCrypto.randomUUID() },
  );
  await expect(running.outcome).rejects.toThrow();
  const joins = await joinResponsivenessJobs({
    supervisor,
    captured: [running.settled],
    failed: true,
    cacheRoot: home.cacheRoot,
    jobIds: () => preparationLifecycleJobIds(home.base),
  });
  expect(joins).toHaveLength(3);
  expect(joins.filter((join) => join.kind === "failed")).toHaveLength(1);
  expect(joins.filter((join) => join.kind === "completed")).toHaveLength(2);
  expect(exits.map((exit) => exit.kind)).toEqual(["verify", "sweep", "sweep"]);
  expect(exits.every((exit) => exit.reaped && exit.pid !== undefined)).toBe(true);
  expect(await supervisor.unreleasedJobs?.()).toEqual([]);
  const cache = NodePath.join(home.root, "compile-cache", "t3code", "compile-cache");
  expect(await NodeFSP.realpath(cache)).toBe(cache);
  expect((await NodeFSP.stat(cache)).isDirectory()).toBe(true);
  await home.remove();
});

it("refuses unknown ownership without removing the captured material", async () => {
  const home = await makePreparationHome();
  const supervisor = makePreparationSupervisor({
    baseDir: home.base,
    cacheRoot: home.cacheRoot,
    command: sourceWorkerCommand,
    env: { ...process.env, XDG_CACHE_HOME: NodePath.join(home.root, "compile-cache") },
  });
  await expect(
    joinResponsivenessJobs({
      supervisor,
      captured: [],
      failed: false,
      cacheRoot: home.cacheRoot,
      jobIds: async () => [id],
    }),
  ).rejects.toThrow("unconfirmed");
  expect((await NodeFSP.stat(home.root)).isDirectory()).toBe(true);
  // This negative case never starts a job; only its own inert synthetic material is removed.
  expect(await preparationLifecycleJobIds(home.base)).toEqual([]);
  await home.remove();
});

it("cancels the actual captured helper during setup and joins it before removing its task scratch", async () => {
  const controller = new AbortController();
  const helper = await startResponsivenessSource({
    signal: controller.signal,
    onHome: () => controller.abort(),
  });
  onTestFinished(() => helper.close());
  const error: unknown = await helper.result.catch((cause) => cause);
  expect(error).toBeInstanceOf(ResponsivenessSourceError);
  if (!(error instanceof ResponsivenessSourceError))
    throw new Error("Expected captured helper failure.");
  expect(error.receipt?.status).toBe("failed");
  expect(error.receipt?.primary).toContain("cancelled");
  expect(error.receipt?.cleanup).toEqual({ removed: true, jobs: [], workers: [] });
  await helper.close();
  await expect(NodeFSP.lstat(helper.scratch)).rejects.toHaveProperty("code", "ENOENT");
});
