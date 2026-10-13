// @effect-diagnostics nodeBuiltinImport:off - owned subprocess qualification at the test runner boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodeEffectPath from "@effect/platform-node/NodePath";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { it as effectIt } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";
import {
  placementArguments,
  placementCommand,
  readProcessPlacementBinding,
  type CgroupBinding,
  type ProcessPlacementBinding,
} from "./processPlacement.ts";
import * as ProcessPlacementSpawner from "./processPlacementSpawner.ts";

const linux = HostProcessPlatform.defaultValue() === "linux";
const fixtureText = process.env.JONES_PLACEMENT_CGROUP_FIXTURE;
const payload = NodeURL.fileURLToPath(new URL("./qualificationPayload.mjs", import.meta.url));
const source = NodeURL.fileURLToPath(
  new URL("../../../../../native/process-placement/placement.c", import.meta.url),
);
const argumentsToPreserve = ["argument with spaces", "$HOME", "", "--", "quote'\"\\"];
const nodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodeEffectPath.layer)),
);
const placedServices = (binding: ProcessPlacementBinding | undefined) =>
  ProcessPlacementSpawner.layer(binding).pipe(Layer.provide(nodeServices));
let scratch: string;
let helper: string;
let binding: ProcessPlacementBinding;
let kernelBinding: ProcessPlacementBinding | undefined;

function identity(path: string): CgroupBinding {
  const stat = NodeFS.statSync(path, { bigint: true });
  return { path, device: String(stat.dev), inode: String(stat.ino) };
}
function helperBinding(path: string) {
  const stat = identity(path);
  return {
    helperPath: path,
    helperDevice: stat.device,
    helperInode: stat.inode,
    helperSha256: NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex"),
  };
}

// Supplying this manifest is an explicit assertion of task ownership, not discovery
// or permission to create/delete cgroups. All three already-existing groups must
// be empty, UID-owned cgroup v2 directories with pinned identities. Tests move
// only their captured children; the fixture supplier retains directory cleanup.
function suppliedFixture(text: string | undefined, executable: ProcessPlacementBinding) {
  if (text === undefined) return undefined;
  const fixture = JSON.parse(text) as {
    version: number;
    ownership: string;
    taskId: string;
    ownerUid: number;
    root: CgroupBinding;
    control: CgroupBinding;
    workload: CgroupBinding;
  };
  if (
    fixture.version !== 1 ||
    fixture.ownership !== "task-owned-disposable" ||
    fixture.ownerUid !== process.getuid?.() ||
    !/^[a-zA-Z0-9-]{8,80}$/.test(fixture.taskId) ||
    !fixture.root?.path.startsWith("/sys/fs/cgroup/") ||
    NodePath.basename(fixture.root.path) !== `jones-placement-qualification-${fixture.taskId}` ||
    fixture.control?.path !== `${fixture.root.path}/control` ||
    fixture.workload?.path !== `${fixture.root.path}/workload`
  ) {
    throw new Error("Explicit task-owned delegated cgroup fixture required");
  }
  const result = readProcessPlacementBinding({
    T3_PROCESS_PLACEMENT: JSON.stringify({
      ...executable,
      control: fixture.control,
      workload: fixture.workload,
    }),
  })!;
  for (const group of [fixture.root, fixture.control, fixture.workload]) {
    const stat = NodeFS.lstatSync(group.path);
    if (
      NodeFS.realpathSync(group.path) !== group.path ||
      !stat.isDirectory() ||
      stat.uid !== fixture.ownerUid ||
      (stat.mode & 0o022) !== 0 ||
      NodeFS.statfsSync(group.path).type !== 0x63677270 ||
      JSON.stringify(identity(group.path)) !==
        JSON.stringify({ path: group.path, device: group.device, inode: group.inode })
    ) {
      throw new Error("Unsafe or stale delegated cgroup fixture");
    }
    if (
      NodeFS.readFileSync(`${group.path}/cgroup.procs`, "utf8").trim() !== "" ||
      !/^populated 0$/m.test(NodeFS.readFileSync(`${group.path}/cgroup.events`, "utf8"))
    ) {
      throw new Error("Delegated fixture is occupied");
    }
    NodeFS.accessSync(`${group.path}/cgroup.procs`, NodeFS.constants.W_OK);
    if (group !== fixture.root) NodeFS.accessSync(`${group.path}/cpu.max`, NodeFS.constants.R_OK);
  }
  return result;
}

interface Receipt {
  pid: number;
  membership: string;
  descendant: { pid: number; membership: string };
  args: string[];
  cwd: string;
  environment: string;
}
function assertPlaced(receipt: Receipt, expected: CgroupBinding) {
  const membership = `0::${expected.path.slice("/sys/fs/cgroup".length)}\n`;
  expect(receipt.membership).toBe(membership);
  expect(receipt.descendant.membership).toBe(membership);
  for (const pid of [receipt.pid, receipt.descendant.pid]) {
    expect(NodeFS.readFileSync(`/proc/${pid}/cgroup`, "utf8")).toBe(membership);
    expect(NodeFS.readFileSync(`${expected.path}/cgroup.procs`, "utf8").split("\n")).toContain(
      String(pid),
    );
  }
}
function assertReaped(sentinel: string, receipt: Receipt) {
  expect(NodeFS.readFileSync(`${sentinel}.reaped`, "utf8")).toBe(String(receipt.descendant.pid));
  expect(NodeFS.existsSync(`/proc/${receipt.pid}`)).toBe(false);
  expect(NodeFS.existsSync(`/proc/${receipt.descendant.pid}`)).toBe(false);
}
function sentinel() {
  return NodePath.join(scratch, `payload-${NodeCrypto.randomUUID()}`);
}
function rejectNative(candidate: ProcessPlacementBinding, expected: RegExp) {
  const marker = sentinel();
  const result = NodeChildProcess.spawnSync(
    candidate.helperPath,
    [...placementArguments(candidate, "workload"), process.execPath, payload, "sentinel", marker],
    { encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL" },
  );
  expect(result.error).toBeUndefined();
  expect(result.status).toBe(125);
  expect(result.stdout).toBe("");
  expect(result.stderr).toMatch(expected);
  expect(NodeFS.existsSync(marker)).toBe(false);
}

async function exercisePipeline(candidate: ProcessPlacementBinding | undefined) {
  const marker = sentinel();
  const args = [payload, "parent", marker, ...argumentsToPreserve];
  const command =
    candidate === undefined
      ? { command: process.execPath, args }
      : placementCommand(process.execPath, args, "workload", candidate);
  const child = NodeChildProcess.spawn(command.command, command.args, {
    cwd: scratch,
    env: { PATH: process.env.PATH, QUALIFICATION_VALUE: "exact value" },
    stdio: ["pipe", "pipe", "pipe", "ipc"],
  });
  const closed = NodeEvents.EventEmitter.once(child, "close");
  let stdout = "",
    stderr = "";
  child.stdout!.on("data", (chunk) => {
    stdout += String(chunk);
  });
  child.stderr!.on("data", (chunk) => {
    stderr += String(chunk);
  });
  try {
    const [message] = await NodeEvents.EventEmitter.once(child, "message", {
      signal: AbortSignal.timeout(5_000),
    });
    const receipt = message.ready as Receipt;
    expect(receipt.pid).toBe(child.pid);
    if (candidate !== undefined) assertPlaced(receipt, candidate.workload);
    expect(receipt.args).toEqual(argumentsToPreserve);
    expect(receipt.cwd).toBe(scratch);
    expect(receipt.environment).toBe("exact value");
    const echoed = NodeEvents.EventEmitter.once(child, "message", {
      signal: AbortSignal.timeout(5_000),
    });
    child.send({ nonce: "ipc-survived-exec" });
    expect((await echoed)[0]).toEqual({ echo: { nonce: "ipc-survived-exec" } });
    child.stdin!.write("preserved stdin\nexit:23\n");
    expect(await closed).toEqual([23, null]);
    expect(stdout).toContain("stdin:preserved stdin\n");
    expect(stderr).toBe("qualification-stderr\n");
    assertReaped(marker, receipt);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await closed;
  }
}

const cancelPipeline = (candidate: ProcessPlacementBinding | undefined) =>
  Effect.gen(function* () {
    const marker = sentinel();
    const ready = yield* Deferred.make<Receipt>();
    const fiber = yield* Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const handle = yield* spawner.spawn(
        ChildProcess.make(process.execPath, [payload, "parent", marker], { stdin: "pipe" }),
      );
      const lines = yield* handle.stdout.pipe(
        Stream.decodeText,
        Stream.splitLines,
        Stream.take(1),
        Stream.runCollect,
      );
      const receipt = JSON.parse(lines[0]!) as Receipt;
      expect(receipt.pid).toBe(Number(handle.pid));
      if (candidate !== undefined) assertPlaced(receipt, candidate.workload);
      yield* Deferred.succeed(ready, receipt);
      yield* Effect.never;
    }).pipe(
      Effect.scoped,
      Effect.provide(placedServices(candidate)),
      Effect.provideService(HostProcessPlatform, "linux"),
      Effect.forkScoped,
    );
    const receipt = yield* Deferred.await(ready);
    yield* Fiber.interrupt(fiber);
    assertReaped(marker, receipt);
  }).pipe(Effect.scoped, Effect.timeout("10 seconds"));

beforeAll(() => {
  if (!linux) return;
  scratch = NodeFS.mkdtempSync(
    NodePath.join(
      process.env.JONES_PLACEMENT_TEST_SCRATCH ?? NodeOS.tmpdir(),
      "placement-qualification-",
    ),
  );
  try {
    helper = NodePath.join(scratch, "placement");
    NodeChildProcess.execFileSync(
      "cc",
      ["-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", helper],
      {
        env: { ...process.env, TMPDIR: scratch },
        timeout: 15_000,
      },
    );
    NodeFS.chmodSync(helper, 0o755);
    binding = {
      version: 1,
      ...helperBinding(helper),
      control: {
        path: `/sys/fs/cgroup/jones-unavailable-${NodeCrypto.randomUUID()}/control`,
        device: "0",
        inode: "1",
      },
      workload: { path: "/sys/fs/cgroup/unused", device: "0", inode: "2" },
    };
    binding = {
      ...binding,
      workload: { ...binding.workload, path: `${NodePath.dirname(binding.control.path)}/workload` },
    };
    kernelBinding = suppliedFixture(fixtureText, binding);
  } catch (cause) {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
    throw cause;
  }
});
afterAll(() => {
  try {
    if (kernelBinding !== undefined) {
      for (const group of [kernelBinding.control, kernelBinding.workload]) {
        expect(identity(group.path)).toEqual(group);
        expect(NodeFS.readFileSync(`${group.path}/cgroup.procs`, "utf8").trim()).toBe("");
        expect(NodeFS.readFileSync(`${group.path}/cgroup.events`, "utf8")).toMatch(
          /^populated 0$/m,
        );
      }
    }
  } finally {
    if (scratch !== undefined) {
      NodeFS.rmSync(scratch, { recursive: true, force: true });
      expect(NodeFS.existsSync(scratch)).toBe(false);
    }
  }
});

describe.skipIf(!linux)("source qualification (no delegated cgroup required)", () => {
  it("unplaced harness baseline preserves real argv, stdio, IPC and exit (not kernel placement proof)", () =>
    exercisePipeline(undefined));
  effectIt.live(
    "unplaced spawner interruption reaps its real payload and descendant (not provider cancellation proof)",
    () => cancelPipeline(undefined),
  );
  it("reports absent kernel fixture without discovering or borrowing a group", () => {
    expect(suppliedFixture(undefined, binding)).toBeUndefined();
  });
  it("refuses an ordinary directory as an explicitly supplied positive fixture", () => {
    expect(() =>
      suppliedFixture(
        JSON.stringify({
          version: 1,
          ownership: "task-owned-disposable",
          taskId: "ordinary-directory",
          ownerUid: process.getuid?.(),
          root: identity(scratch),
        }),
        binding,
      ),
    ).toThrow("Explicit task-owned");
  });
  it.each(["helperSha256", "helperDevice", "helperInode"] as const)(
    "native helper rejects stale %s before the payload sentinel",
    (field) => {
      rejectNative(
        { ...binding, [field]: field === "helperSha256" ? "0".repeat(64) : "0" },
        /helper (hash|identity) changed/,
      );
    },
  );
  it("native missing cgroup fails without executing an unplaced payload", () => {
    rejectNative(binding, /cgroup unavailable or symlinked/);
  });
  it("native ordinary-directory target fails without executing an unplaced payload", () => {
    rejectNative({ ...binding, workload: identity(scratch) }, /cgroup must be beneath/);
  });
  it("helper replacement rejects the stale generation and rollback restores the original identity", () => {
    const active = NodePath.join(scratch, "active-helper");
    const replacement = NodePath.join(scratch, "replacement-helper");
    const rollback = NodePath.join(scratch, "rollback-helper");
    NodeFS.copyFileSync(helper, active);
    NodeFS.copyFileSync(helper, replacement);
    const old = { ...binding, ...helperBinding(active) };
    NodeFS.renameSync(active, rollback);
    NodeFS.renameSync(replacement, active);
    rejectNative(old, /helper identity changed/);
    rejectNative({ ...binding, ...helperBinding(active) }, /cgroup unavailable or symlinked/);
    NodeFS.renameSync(active, replacement);
    NodeFS.renameSync(rollback, active);
    expect(helperBinding(active)).toEqual({
      helperPath: old.helperPath,
      helperDevice: old.helperDevice,
      helperInode: old.helperInode,
      helperSha256: old.helperSha256,
    });
    expect(helperBinding(active).helperInode).toBe(old.helperInode);
    rejectNative(old, /cgroup unavailable or symlinked/);
  });

  effectIt.live("real spawner rejects missing helper and group without a payload fallback", () =>
    Effect.gen(function* () {
      for (const candidate of [binding, { ...binding, helperPath: `${helper}.missing` }]) {
        const marker = sentinel();
        const exit = yield* Effect.gen(function* () {
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          return yield* spawner.spawn(
            ChildProcess.make(process.execPath, [payload, "sentinel", marker]),
          );
        }).pipe(
          Effect.scoped,
          Effect.provide(placedServices(candidate)),
          Effect.provideService(HostProcessPlatform, "linux"),
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);
        expect(NodeFS.existsSync(marker)).toBe(false);
      }
    }),
  );
});

// No cgroup is created, discovered, or borrowed. Missing authority is a visible
// skip; a supplied but unsafe/stale fixture fails setup, rather than passing.
describe.skipIf(!linux || fixtureText === undefined)(
  `kernel qualification (${fixtureText === undefined ? "UNAVAILABLE: no explicit JONES_PLACEMENT_CGROUP_FIXTURE supplied" : "explicit task-owned fixture"})`,
  () => {
    it("places helper -> payload -> descendant before readiness, preserving PID, argv, cwd, env, stdio, IPC and exit", () =>
      exercisePipeline(kernelBinding));

    effectIt.live(
      "placement spawner interruption reaps the captured payload and its descendant",
      () => cancelPipeline(kernelBinding),
    );

    it("qualifies replacement and rollback helper generations with real placed payloads", async () => {
      const active = NodePath.join(scratch, "kernel-active-helper");
      const replacement = NodePath.join(scratch, "kernel-replacement-helper");
      const rollback = NodePath.join(scratch, "kernel-rollback-helper");
      NodeFS.copyFileSync(helper, active);
      NodeFS.copyFileSync(helper, replacement);
      const original = { ...kernelBinding!, ...helperBinding(active) };
      await exercisePipeline(original);
      NodeFS.renameSync(active, rollback);
      NodeFS.renameSync(replacement, active);
      rejectNative(original, /helper identity changed/);
      await exercisePipeline({ ...kernelBinding!, ...helperBinding(active) });
      NodeFS.renameSync(active, replacement);
      NodeFS.renameSync(rollback, active);
      await exercisePipeline(original);
    });

    it("refuses a stale cgroup generation and a missing payload without fallback", () => {
      rejectNative(
        { ...kernelBinding!, workload: { ...kernelBinding!.workload, inode: "1" } },
        /stale cgroup identity/,
      );
      const command = placementCommand(`${scratch}/missing-payload`, [], "workload", kernelBinding);
      const result = NodeChildProcess.spawnSync(command.command, command.args, {
        encoding: "utf8",
        timeout: 5_000,
      });
      expect(result.status).toBe(125);
      expect(result.stderr).toContain("payload execution failed");
      expect(result.stdout).toBe("");
    });
  },
);
