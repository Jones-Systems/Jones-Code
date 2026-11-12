// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it, vi } from "vite-plus/test";
import {
  preparationLifecycle,
  preparationLifecycleJobIds,
  readTerminalRelease,
} from "./lifecycle.ts";

const jobId = "99999999-9999-4999-8999-999999999999";
const worker = { pid: 54321, processGroup: 54321 };
async function fixture(run: (baseDir: string) => Promise<void>) {
  const baseDir = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-preparation-lifecycle-"),
  );
  try {
    await run(baseDir);
  } finally {
    await NodeFSP.rm(baseDir, { recursive: true, force: true });
  }
}

it("keeps native effects held after group exit and lost result, until exact cleanup proof", async () =>
  fixture(async (base) => {
    const lifecycle = preparationLifecycle(base, jobId);
    const launch = await lifecycle.reserve("stage");
    await launch.started(worker);
    await lifecycle.effects(worker.pid, "native-effects");
    expect(await lifecycle.release(() => true)).toBe("cleanup-unconfirmed");
    expect(await preparationLifecycle(base, jobId).release(() => true)).toBe("cleanup-unconfirmed");
    await lifecycle.effects(worker.pid, "clean");
    expect(await lifecycle.release(() => false)).toBe("unreaped");
    expect(await lifecycle.release(() => true)).toBe("released");
    expect(await preparationLifecycle(base, jobId).release(() => false)).toBe("released");
  }));

it("serializes identity saving before release and refuses every launch write after release", async () =>
  fixture(async (base) => {
    const lifecycle = preparationLifecycle(base, jobId);
    const launch = await lifecycle.reserve("verify");
    await launch.spawning();
    const saved = launch.started(worker);
    const released = lifecycle.release(() => true);
    await saved;
    expect(await released).toBe("released");
    const file = NodePath.join(base, "runtime", "jones-preparation", "lifecycle", `${jobId}.json`);
    const bytes = await NodeFSP.readFile(file, "utf8");
    for (const write of [launch.spawning, () => launch.started(worker), launch.notStarted])
      await expect(write()).rejects.toThrow("cannot be replaced");
    await expect(preparationLifecycle(base, jobId).reserve("verify")).rejects.toMatchObject({
      code: "EEXIST",
    });
    expect(await NodeFSP.readFile(file, "utf8")).toBe(bytes);
    expect(await readTerminalRelease(base, jobId)).toBe(true);
  }));

it("fast-reads only bounded owner-controlled exact release records without following links", async () =>
  fixture(async (base) => {
    const directory = NodePath.join(base, "runtime", "jones-preparation", "lifecycle");
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    const file = NodePath.join(directory, `${jobId}.json`);
    const released = { schema: 1, jobId, kind: "stage", state: "released", worker };
    expect(await readTerminalRelease(base, jobId)).toBe(false);
    for (const value of [
      null,
      [],
      {},
      { ...released, schema: 2 },
      { ...released, jobId: "88888888-8888-4888-8888-888888888888" },
      { ...released, kind: "unknown" },
      { ...released, worker: undefined },
      { ...released, worker: { pid: 1, processGroup: 1 } },
      { ...released, worker: { pid: worker.pid, processGroup: worker.pid + 1 } },
      ...["not-started", "spawning", "started"].map((state) => ({ ...released, state })),
    ]) {
      await NodeFSP.writeFile(file, JSON.stringify(value), { mode: 0o600 });
      expect(await readTerminalRelease(base, jobId), JSON.stringify(value)).toBe(false);
    }
    for (const bytes of ["{", `${" ".repeat(4097)}${JSON.stringify(released)}`]) {
      await NodeFSP.writeFile(file, bytes);
      expect(await readTerminalRelease(base, jobId)).toBe(false);
    }
    await NodeFSP.writeFile(file, JSON.stringify(released));
    expect(await readTerminalRelease(base, jobId)).toBe(true);
    if (typeof process.getuid === "function") {
      const uid = (await NodeFSP.stat(file)).uid;
      const owner = vi.spyOn(process, "getuid").mockReturnValue(uid + 1);
      try {
        expect(await readTerminalRelease(base, jobId)).toBe(false);
      } finally {
        owner.mockRestore();
      }
    }
    await NodeFSP.chmod(file, 0o622);
    expect(await readTerminalRelease(base, jobId)).toBe(false);
    await NodeFSP.chmod(file, 0o600);
    const target = NodePath.join(directory, "target.json");
    await NodeFSP.rename(file, target);
    await NodeFSP.symlink(target, file);
    expect(await readTerminalRelease(base, jobId)).toBe(false);
    await NodeFSP.unlink(file);
    await NodeFSP.mkdir(file);
    expect(await readTerminalRelease(base, jobId)).toBe(false);
  }));

it("reconciles a transfer cancellation only after the captured whole group is gone", async () =>
  fixture(async (base) => {
    const lifecycle = preparationLifecycle(base, jobId);
    const launch = await lifecycle.reserve("stage");
    await launch.started(worker);
    await lifecycle.effects(worker.pid, "transfer");
    expect(await lifecycle.release(() => false)).toBe("unreaped");
    const scratch = NodePath.join(base, "runtime", "jones-preparation", "jobs", jobId);
    await NodeFSP.mkdir(scratch, { recursive: true });
    await NodeFSP.rm(scratch, { recursive: true });
    expect(await preparationLifecycle(base, jobId).release(() => false)).toBe("unreaped");
    expect(await preparationLifecycleJobIds(base)).toContain(jobId);
    expect(await preparationLifecycle(base, jobId).release(() => true)).toBe("released");
  }));

it("distinguishes never spawned from a missing identity or missing native phase", async () =>
  fixture(async (base) => {
    const lifecycle = preparationLifecycle(base, jobId);
    expect(await lifecycle.release(() => true)).toBe("unknown");
    const launch = await lifecycle.reserve("stage");
    expect(await lifecycle.release(() => false)).toBe("not-started");
    await launch.spawning();
    expect(await lifecycle.release(() => true)).toBe("unknown");
    await launch.started(worker);
    expect(await lifecycle.release(() => true)).toBe("cleanup-unconfirmed");
    await lifecycle.effects(worker.pid + 1, "clean");
    expect(await lifecycle.release(() => true)).toBe("unknown");
  }));
