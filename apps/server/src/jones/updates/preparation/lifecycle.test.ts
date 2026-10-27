// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import { preparationLifecycle, preparationLifecycleJobIds } from "./lifecycle.ts";

const jobId = "99999999-9999-4999-8999-999999999999";
const worker = { pid: 54321, processGroup: 54321 };
async function fixture(run: (baseDir: string) => Promise<void>) {
  const baseDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-preparation-lifecycle-"));
  try { await run(baseDir); }
  finally { await NodeFSP.rm(baseDir, { recursive: true, force: true }); }
}

it("keeps native effects held after group exit and lost result, until exact cleanup proof", async () => fixture(async (base) => {
  const lifecycle = preparationLifecycle(base, jobId);
  await lifecycle.reserve("stage");
  await lifecycle.launch({ kind: "stage", state: "started", worker });
  await lifecycle.effects(worker.pid, "native-effects");
  expect(await lifecycle.release(() => true)).toBe("cleanup-unconfirmed");
  expect(await preparationLifecycle(base, jobId).release(() => true)).toBe("cleanup-unconfirmed");
  await lifecycle.effects(worker.pid, "clean");
  expect(await lifecycle.release(() => false)).toBe("unreaped");
  expect(await lifecycle.release(() => true)).toBe("released");
  expect(await preparationLifecycle(base, jobId).release(() => false)).toBe("released");
}));

it("reconciles a transfer cancellation only after the captured whole group is gone", async () => fixture(async (base) => {
  const lifecycle = preparationLifecycle(base, jobId);
  await lifecycle.reserve("stage");
  await lifecycle.launch({ kind: "stage", state: "started", worker });
  await lifecycle.effects(worker.pid, "transfer");
  expect(await lifecycle.release(() => false)).toBe("unreaped");
  const scratch = NodePath.join(base, "runtime", "jones-preparation", "jobs", jobId);
  await NodeFSP.mkdir(scratch, { recursive: true });
  await NodeFSP.rm(scratch, { recursive: true });
  expect(await preparationLifecycle(base, jobId).release(() => false)).toBe("unreaped");
  expect(await preparationLifecycleJobIds(base)).toContain(jobId);
  expect(await preparationLifecycle(base, jobId).release(() => true)).toBe("released");
}));

it("distinguishes never spawned from a missing identity or missing native phase", async () => fixture(async (base) => {
  const lifecycle = preparationLifecycle(base, jobId);
  expect(await lifecycle.release(() => true)).toBe("unknown");
  await lifecycle.reserve("stage");
  expect(await lifecycle.release(() => false)).toBe("not-started");
  await lifecycle.launch({ kind: "stage", state: "spawning" });
  expect(await lifecycle.release(() => true)).toBe("unknown");
  await lifecycle.launch({ kind: "stage", state: "started", worker });
  expect(await lifecycle.release(() => true)).toBe("cleanup-unconfirmed");
  await lifecycle.effects(worker.pid + 1, "clean");
  expect(await lifecycle.release(() => true)).toBe("unknown");
}));
