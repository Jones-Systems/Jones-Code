// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import { jonesCandidateHandle, type JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import { withQualifiedRuntimeLock } from "../../cloud/qualifiedRuntime.ts";
import { preparationLifecycle } from "./lifecycle.ts";
import { PREPARATION_LOCK, preparationJobLock } from "./protocol.ts";
import { sweepPreparationLeftovers } from "./sweep.ts";
import {
  makePreparationSupervisor,
  type PreparationJoin,
  type PreparationWorkerExit,
} from "./supervisor.ts";
import {
  actionsFixture,
  BASELINE,
  fixtureHost,
  makePreparationHome,
  sourceWorkerCommand,
} from "./Preparation.testkit.ts";
import {
  inspectHeldPreparationCleanup,
  recoverNativeEffects,
  type NativeEffectsRecoveryDependencies,
  type NativeEffectsRecoveryRequest,
} from "./nativeEffectsRecovery.ts";

const jobId = "99999999-9999-4999-8999-999999999999";
const worker = { pid: 54321, processGroup: 54321 };
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

async function fixture(
  run: (f: Awaited<ReturnType<typeof setup>>) => Promise<void>,
  installedSource = "317c4df82c77d06922a6723e318e09b0de20ff99",
) {
  const created = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-native-recovery-"));
  try {
    await run(await setup(await NodeFSP.realpath(created), installedSource));
  } finally {
    await NodeFSP.rm(created, { recursive: true, force: true });
  }
}

async function setup(base: string, installedSource: string) {
  const runtime = NodePath.join(base, "runtime");
  const root = NodePath.join(runtime, "jones-preparation");
  const jobDir = NodePath.join(root, "jobs", jobId);
  const lifecycleDir = NodePath.join(root, "lifecycle");
  const cache = NodePath.join(runtime, "jones-actions");
  const candidate: JonesStagedArtifact["candidate"] = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source: "1".repeat(40),
    tree: "2".repeat(40),
    installedSource,
    workflow: ".github/workflows/artifact-desktop-mac.yml",
    workflowId: 1,
    runId: 2,
    runAttempt: 1,
    ciRunId: 3,
    artifactId: 4,
    artifactName: "desktop-mac-arm64",
    artifactDigest: hash("archive"),
    artifactBytes: 7,
    expiresAt: "2027-01-01T00:00:00Z",
    version: "0.1.2-preview.20261102.1",
    platform: "darwin",
    architecture: "arm64",
  };
  const handle = jonesCandidateHandle(candidate);
  const stage = NodePath.join(cache, "attempts", "stage-fixture", handle);
  const mountOwner = NodePath.join(runtime, ".jones-dmg-mount-fixture");
  const mount = NodePath.join(mountOwner, "volume");
  for (const path of [jobDir, lifecycleDir, stage, NodePath.join(cache, "completed"), mount])
    await NodeFSP.mkdir(path, { recursive: true, mode: 0o700 });
  const lifecycle = NodePath.join(lifecycleDir, `${jobId}.json`);
  const effects = NodePath.join(lifecycleDir, `${jobId}.effects.json`);
  const payload = NodePath.join(stage, "candidate.dmg");
  const receipt = {
    schema: 1,
    repository: candidate.repository,
    source: candidate.source,
    tree: candidate.tree,
    workflow: candidate.workflow,
    event: "push",
    ref: "refs/heads/main",
    runId: "2",
    runAttempt: "1",
    version: candidate.version,
    platform: "darwin",
    architecture: "arm64",
    artifact: "candidate.dmg",
    sha256: hash("payload"),
  } as const;
  const artifact: JonesStagedArtifact = {
    schema: 1,
    source: "jones-actions",
    channel: "jones-main",
    stagedHandle: handle,
    candidate,
    receipt,
    payloadPath: payload,
  };
  const write = async (path: string, value: unknown) => {
    const bytes = `${JSON.stringify(value)}\n`;
    await NodeFSP.writeFile(path, bytes, { mode: 0o600 });
    return hash(bytes);
  };
  const expected = {
    request: await write(NodePath.join(jobDir, "request.json"), {
      protocol: 1,
      jobId,
      baseDir: base,
      deadlineMs: 1000,
      processGroup: true,
      job: {
        kind: "stage",
        candidate,
        cacheRoot: cache,
        dbPath: NodePath.join(base, "state.sqlite"),
        activeVersion: candidate.version,
        host: { platform: "darwin", architecture: "arm64" },
      },
    }),
    result: await write(NodePath.join(jobDir, "result.json"), {
      protocol: 1,
      jobId,
      ok: false,
      error: {
        name: "QualifiedRuntimeBlockedError",
        reason: "busy",
        message: `DMG mount ownership needs reconciliation at ${mount}; its directory was retained. Staging=mount cleanup=1/inventory failed-detach=none.`,
      },
    }),
    lifecycle: await write(lifecycle, {
      schema: 1,
      jobId,
      kind: "stage",
      state: "started",
      worker,
    }),
    effects: await write(effects, { schema: 1, jobId, pid: worker.pid, phase: "native-effects" }),
    index: await write(NodePath.join(cache, "completed", `${handle}.json`), {
      schema: 1,
      handle,
      directory: stage,
    }),
    receipt: await write(NodePath.join(stage, "jones-actions-stage.json"), artifact),
    payload: receipt.sha256,
    archive: candidate.artifactDigest,
  };
  await NodeFSP.writeFile(payload, "payload", { mode: 0o600 });
  await NodeFSP.writeFile(NodePath.join(stage, "github-artifact.zip"), "archive", { mode: 0o600 });
  const other = NodePath.join(base, "other.dmg");
  await NodeFSP.writeFile(other, "other", { mode: 0o600 });
  let passes = 0;
  const dependencies: {
    -readonly [K in keyof NativeEffectsRecoveryDependencies]: NativeEffectsRecoveryDependencies[K];
  } = {
    platform: "darwin",
    architecture: "arm64",
    absent: () => true,
    validateArtifact: async () => artifact,
    plist: async (command, args) => {
      if (command.endsWith("diskutil"))
        return {
          WholeDisk: true,
          DeviceNode: args[2],
          TotalSize: 9000,
          Size: 9000,
          IOKitSize: 9000,
        };
      if (args[0] === "imageinfo")
        return {
          Format: "UDZO",
          Properties: { Encrypted: false, Partitioned: false },
          Segments: [payload],
          "Size Information": { "Total Bytes": 1000 },
        };
      passes++;
      return {
        images: [
          {
            "image-path": other,
            "system-entities": [
              { "dev-entry": "/dev/disk8" },
              { "dev-entry": "/dev/disk9s1", "mount-point": "/Volumes/other" },
            ],
          },
        ],
      };
    },
  };
  const input: NativeEffectsRecoveryRequest = {
    schema: 1,
    baseDir: base,
    jobId,
    worker,
    mountPath: mount,
    expected,
  };
  const inspect = () => recoverNativeEffects(input, "inspect", undefined, dependencies);
  const apply = (proof: string) => recoverNativeEffects(input, "apply", proof, dependencies);
  return {
    base,
    input,
    dependencies,
    candidate,
    inspect,
    apply,
    lifecycle,
    effects,
    payload,
    cache,
    handle,
    passes: () => passes,
    write,
  };
}

it("inspects owned 0755 shared ancestors without writing proof or changing lifecycle", async () =>
  fixture(async (f) => {
    await NodeFSP.chmod(f.base, 0o755);
    await NodeFSP.chmod(NodePath.join(f.base, "runtime"), 0o755);
    const lifecycle = await NodeFSP.readFile(f.lifecycle);
    const effects = await NodeFSP.readFile(f.effects);
    const inspected = await f.inspect();
    expect(inspected.state).toBe("eligible");
    expect(f.passes()).toBe(2);
    expect(await NodeFSP.readFile(f.lifecycle)).toEqual(lifecycle);
    expect(await NodeFSP.readFile(f.effects)).toEqual(effects);
    await expect(NodeFSP.lstat(inspected.proofPath)).rejects.toMatchObject({ code: "ENOENT" });
  }));

it("derives read-only native proof only for the exact held operation", async () =>
  fixture(async (f) => {
    const lifecycle = await NodeFSP.readFile(f.lifecycle);
    const effects = await NodeFSP.readFile(f.effects);
    const commands: string[] = [];
    const plist = f.dependencies.plist!;
    f.dependencies.plist = (command, args) => {
      commands.push(`${NodePath.basename(command)} ${args[0]}`);
      return plist(command, args);
    };
    const held = {
      phase: "held",
      code: "cleanup-unconfirmed",
      jobId,
      expectedInstalledSource: f.candidate.installedSource,
      action: {
        kind: "download",
        sourceSha: f.candidate.source,
        artifactId: f.candidate.artifactId,
      },
    } as const;
    const inspect = (receipt: typeof held) =>
      inspectHeldPreparationCleanup(f.base, receipt, f.dependencies);
    expect(await inspect({ ...held, action: { ...held.action, artifactId: 5 } })).toBeUndefined();
    expect(commands).toEqual([]);
    const recover = await inspect(held);
    expect(recover).toBeTypeOf("function");
    expect(commands).toEqual([
      "hdiutil imageinfo",
      "hdiutil info",
      "diskutil info",
      "diskutil info",
      "hdiutil info",
      "diskutil info",
      "diskutil info",
    ]);
    expect(await NodeFSP.readFile(f.lifecycle)).toEqual(lifecycle);
    expect(await NodeFSP.readFile(f.effects)).toEqual(effects);
    await expect(
      NodeFSP.lstat(NodePath.join(f.base, "runtime", "jones-preparation-recovery")),
    ).rejects.toMatchObject({ code: "ENOENT" });
    expect(
      await recover!(() => {
        throw new Error("Consent expired.");
      }),
    ).toBe(false);
    expect(await NodeFSP.readFile(f.lifecycle)).toEqual(lifecycle);
    f.dependencies.checkpoint = async (phase) => {
      if (phase === "release-durable") throw new Error("Lost release response.");
    };
    expect(await recover!(() => {})).toBe(true);
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("released");
    expect(await NodeFSP.readFile(f.effects)).toEqual(effects);
  }));

it.each([
  ["base", 0o775],
  ["base", 0o777],
  ["runtime", 0o775],
  ["runtime", 0o777],
] as const)("holds a writable %s shared ancestor with mode %s", async (ancestor, mode) =>
  fixture(async (f) => {
    const path = ancestor === "base" ? f.base : NodePath.join(f.base, "runtime");
    await NodeFSP.chmod(path, mode);
    await expect(f.inspect()).rejects.toThrow();
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
  }),
);

it("holds a private preparation directory with mode 0755", async () =>
  fixture(async (f) => {
    await NodeFSP.chmod(NodePath.join(f.base, "runtime", "jones-preparation"), 0o755);
    await expect(f.inspect()).rejects.toThrow();
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
  }));

it.each([
  "317c4df82c77d06922a6723e318e09b0de20ff99",
  "8dd90ff9e3d41e7ba9ac1c81bceb4f973734314b",
  "245409b7feefd2509b12f2fb7e48e6ec5e8d0b50",
])(
  "releases producer %s only after durable proof, preserving native effects and identity",
  async (installedSource) =>
    fixture(async (f) => {
      expect(await preparationLifecycle(f.base, jobId).release(() => true)).toBe(
        "cleanup-unconfirmed",
      );
      await NodeFSP.link(
        NodePath.join(f.cache, "completed", `${f.handle}.json`),
        NodePath.join(f.base, "index-link"),
      );
      const inspected = await f.inspect();
      expect(inspected).toMatchObject({
        state: "eligible",
        evidence: { producerContract: { source: installedSource } },
      });
      expect(f.passes()).toBe(2);
      expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
      const proof = String(inspected.proofSha256);
      expect((await f.apply(proof)).state).toBe("released");
      expect(await preparationLifecycle(f.base, jobId).release(() => false)).toBe("released");
      expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8"))).toEqual({
        schema: 1,
        jobId,
        kind: "stage",
        state: "released",
        worker,
      });
      expect(JSON.parse(await NodeFSP.readFile(f.effects, "utf8")).phase).toBe("native-effects");
      expect((await f.apply(proof)).state).toBe("released");
      expect(await NodeFSP.readFile(f.payload, "utf8")).toBe("payload");
    }, installedSource),
);

it.each(["proof-durable", "release-prepared", "release-durable"] as const)(
  "retries a crash at %s with the same exact proof",
  async (phase) =>
    fixture(async (f) => {
      const proof = String((await f.inspect()).proofSha256);
      f.dependencies.checkpoint = async (at) => {
        if (at === phase) throw new Error("simulated crash");
      };
      await expect(f.apply(proof)).rejects.toThrow("simulated crash");
      delete f.dependencies.checkpoint;
      expect((await f.apply(proof)).state).toBe("released");
    }),
);

it.each([PREPARATION_LOCK, preparationJobLock(jobId)])(
  "refuses a competing %s lock",
  async (name) =>
    fixture(async (f) => {
      await withQualifiedRuntimeLock(f.base, name, async () => {
        await expect(f.inspect()).rejects.toThrow();
      });
    }),
);

it.each([worker.pid, -worker.processGroup])("holds a live or reused identity %s", async (live) =>
  fixture(async (f) => {
    f.dependencies.absent = (pid) => pid !== live;
    await expect(f.inspect()).rejects.toThrow();
  }),
);

it("rechecks liveness and all captured bytes after proof persistence", async () =>
  fixture(async (f) => {
    const proof = String((await f.inspect()).proofSha256);
    f.dependencies.checkpoint = async () => {
      f.dependencies.absent = () => false;
    };
    await expect(f.apply(proof)).rejects.toThrow();
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
    f.dependencies.absent = () => true;
    f.dependencies.checkpoint = async () => {
      await NodeFSP.appendFile(f.effects, " ");
    };
    await expect(f.apply(proof)).rejects.toThrow();
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
  }));

it.each([
  "equal",
  "conflict",
  "partial",
  "changing",
  "error",
  "candidate-path",
  "candidate-mount",
  "missing-capacity",
  "partitioned",
  "multi-segment",
])("holds %s inventories without changing lifecycle", async (kind) =>
  fixture(async (f) => {
    const original = f.dependencies.plist!;
    let pass = 0;
    f.dependencies.plist = async (command, args) => {
      if (kind === "error") throw new Error("command failed");
      const value = (await original(command, args)) as Record<string, unknown>;
      if (command.endsWith("diskutil")) {
        if (kind === "equal") return { ...value, TotalSize: 1000, Size: 1000, IOKitSize: 1000 };
        if (kind === "conflict") return { ...value, IOKitSize: 9001 };
        if (kind === "missing-capacity") return { ...value, WholeDisk: undefined };
        return value;
      }
      if (args[0] !== "info") {
        if (kind === "partitioned")
          return { ...value, Properties: { Encrypted: false, Partitioned: true } };
        if (kind === "multi-segment") return { ...value, Segments: [f.payload, f.payload] };
        return value;
      }
      pass++;
      if (kind === "partial") return { images: [{ "image-path": f.payload }] };
      if (kind === "changing" && pass === 2) return { images: [] };
      if (kind === "candidate-path")
        return { images: [{ "image-path": f.payload, "system-entities": [] }] };
      if (kind === "candidate-mount")
        return {
          images: [
            {
              "image-path": NodePath.join(f.base, "other.dmg"),
              "system-entities": [{ "dev-entry": "/dev/disk8", "mount-point": f.input.mountPath }],
            },
          ],
        };
      return value;
    };
    await expect(f.inspect()).rejects.toThrow();
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
  }),
);

it("rejects changed artifact bytes even when native geometry remains disjoint", async () =>
  fixture(async (f) => {
    const original = f.dependencies.validateArtifact!;
    let validations = 0;
    f.dependencies.validateArtifact = async (...args) => {
      if (++validations === 2) await NodeFSP.appendFile(f.payload, "changed");
      return original(...args);
    };
    await expect(f.inspect()).rejects.toThrow();
  }));

it("requires the exact inspect proof and rejects request CAS mismatch", async () =>
  fixture(async (f) => {
    await expect(f.apply("0".repeat(64))).rejects.toThrow();
    await expect(
      recoverNativeEffects(
        { ...f.input, expected: { ...f.input.expected, request: "0".repeat(64) } },
        "inspect",
        undefined,
        f.dependencies,
      ),
    ).rejects.toThrow();
  }));

it.each(["producer", "shadow"])(
  "holds a stage request outside the verified %s contract",
  async (kind) =>
    fixture(async (f) => {
      const path = NodePath.join(
        f.base,
        "runtime",
        "jones-preparation",
        "jobs",
        jobId,
        "request.json",
      );
      const request = JSON.parse(await NodeFSP.readFile(path, "utf8"));
      if (kind === "producer") request.job.candidate.installedSource = "0".repeat(40);
      else request.job.shadow = true;
      const requestHash = await f.write(path, request);
      await expect(
        recoverNativeEffects(
          { ...f.input, expected: { ...f.input.expected, request: requestHash } },
          "inspect",
          undefined,
          f.dependencies,
        ),
      ).rejects.toThrow();
    }),
);

it("retains lifecycle when full artifact qualification refuses", async () =>
  fixture(async (f) => {
    f.dependencies.validateArtifact = async () => {
      throw new Error("invalid archive digest");
    };
    await expect(f.inspect()).rejects.toThrow("invalid archive digest");
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
  }));

it.each([
  "unqualified 47cad979",
  "abrupt native kill (missing result)",
  "missing effects",
] as const)(
  "synthetic interrupted staging holds %s with evidence retained and zero stage replay",
  async (scenario) =>
    fixture(
      async (f) => {
        const jobs = NodePath.join(f.base, "runtime", "jones-preparation", "jobs");
        const result = NodePath.join(jobs, jobId, "result.json");
        if (scenario === "abrupt native kill (missing result)") await NodeFSP.unlink(result);
        if (scenario === "missing effects") await NodeFSP.unlink(f.effects);
        const files = [
          NodePath.join(jobs, jobId, "request.json"),
          f.lifecycle,
          ...(scenario === "missing effects" ? [] : [f.effects]),
          f.payload,
          NodePath.join(NodePath.dirname(f.payload), "github-artifact.zip"),
          NodePath.join(NodePath.dirname(f.payload), "jones-actions-stage.json"),
          NodePath.join(f.cache, "completed", `${f.handle}.json`),
          ...(scenario === "abrupt native kill (missing result)" ? [] : [result]),
        ];
        const before = await Promise.all(files.map((file) => NodeFSP.readFile(file)));
        let nativeCalls = 0;
        let artifactValidations = 0;
        f.dependencies.plist = async () => {
          nativeCalls++;
          throw new Error("Unqualified evidence must not reach native inspection.");
        };
        f.dependencies.validateArtifact = async () => {
          artifactValidations++;
          throw new Error("Unqualified evidence must not reach artifact validation.");
        };
        const launches: string[] = [];
        const supervisor = makePreparationSupervisor({
          baseDir: f.base,
          cacheRoot: f.cache,
          command: { file: NodePath.join(f.base, "must-not-launch"), args: [] },
          observe: (exit) => launches.push(exit.kind),
        });
        try {
          for (let repeat = 0; repeat < 2; repeat++) {
            expect(await preparationLifecycle(f.base, jobId).release(() => true)).toBe(
              "cleanup-unconfirmed",
            );
            await expect(f.inspect()).rejects.toThrow();
            await expect(f.apply("0".repeat(64))).rejects.toThrow();
            const replay = supervisor.start(
              {
                kind: "stage",
                candidate: f.candidate,
                cacheRoot: f.cache,
                dbPath: NodePath.join(f.base, "state.sqlite"),
                activeVersion: f.candidate.version,
                host: { platform: "darwin", architecture: "arm64" },
              },
              { jobId: NodeCrypto.randomUUID() },
            );
            await expect(replay.outcome).rejects.toMatchObject({ reason: "unreaped" });
            expect((await replay.settled).kind).toBe("not-started");
            const sweep = await sweepPreparationLeftovers({
              baseDir: f.base,
              cacheRoot: f.cache,
              keepJob: NodeCrypto.randomUUID(),
            });
            expect(sweep.retainedJobIds).toContain(jobId);
            expect(await Promise.all(files.map((file) => NodeFSP.readFile(file)))).toEqual(before);
          }
        } finally {
          // Join failure cleanup before the fixture removes its synthetic root, even on assertion failure.
          await supervisor.sweep().catch(() => undefined);
        }
        expect(launches).not.toContain("stage");
        expect(nativeCalls).toBe(0);
        expect(artifactValidations).toBe(0);
        expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
        await expect(
          NodeFSP.lstat(
            NodePath.join(f.base, "runtime", "jones-preparation-recovery", `${jobId}.json`),
          ),
        ).rejects.toMatchObject({ code: "ENOENT" });
        if (scenario === "abrupt native kill (missing result)")
          await expect(NodeFSP.lstat(result)).rejects.toMatchObject({ code: "ENOENT" });
        if (scenario === "missing effects")
          await expect(NodeFSP.lstat(f.effects)).rejects.toMatchObject({ code: "ENOENT" });
      },
      scenario === "unqualified 47cad979" ? "47cad979dcb769e511581e6de9e1aa5be99fd461" : undefined,
    ),
);

it("synthetic qualified cleanup inspects, guards consent, retains proof and admits a fresh source-worker stage", async () => {
  const home = await makePreparationHome();
  let supervisor: ReturnType<typeof makePreparationSupervisor> | undefined;
  let settled: Promise<PreparationJoin> | undefined;
  const controller = new AbortController();
  const exits: PreparationWorkerExit[] = [];
  try {
    const f = await setup(home.base, "8dd90ff9e3d41e7ba9ac1c81bceb4f973734314b");
    const effects = await NodeFSP.readFile(f.effects);
    const held = {
      phase: "held",
      code: "cleanup-unconfirmed",
      jobId,
      expectedInstalledSource: f.candidate.installedSource,
      action: {
        kind: "download",
        sourceSha: f.candidate.source,
        artifactId: f.candidate.artifactId,
      },
    } as const;
    const inspected = await f.inspect();
    expect(inspected.state).toBe("eligible");
    const recover = await inspectHeldPreparationCleanup(home.base, held, f.dependencies);
    expect(recover).toBeTypeOf("function");
    expect(
      await recover!(() => {
        throw new Error("Consent no longer covers this operation.");
      }),
    ).toBe(false);
    expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
    expect(await recover!(() => {})).toBe(true);
    const proof = await NodeFSP.readFile(inspected.proofPath);
    expect(JSON.parse(proof.toString()).proofSha256).toBe(inspected.proofSha256);
    expect(await NodeFSP.readFile(f.effects)).toEqual(effects);
    const sweep = await sweepPreparationLeftovers({
      baseDir: home.base,
      cacheRoot: home.cacheRoot,
      keepJob: NodeCrypto.randomUUID(),
    });
    expect(sweep.failed).toBe(0);
    expect(sweep.retainedJobIds).not.toContain(jobId);
    await expect(
      NodeFSP.lstat(NodePath.join(home.base, "runtime", "jones-preparation", "jobs", jobId)),
    ).rejects.toMatchObject({ code: "ENOENT" });
    const actions = await actionsFixture(home);
    supervisor = makePreparationSupervisor({
      baseDir: home.base,
      cacheRoot: home.cacheRoot,
      command: sourceWorkerCommand,
      env: actions.env,
      observe: (exit) => exits.push(exit),
      limits: { graceMs: 1_000, reapMs: 5_000 },
    });
    const restage = supervisor.start(
      {
        kind: "stage",
        candidate: actions.candidate,
        cacheRoot: home.cacheRoot,
        dbPath: home.dbPath,
        activeVersion: BASELINE,
        host: fixtureHost,
      },
      {
        jobId: NodeCrypto.randomUUID(),
        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(45_000)]),
      },
    );
    settled = restage.settled;
    const outcome = await restage.outcome;
    expect((await restage.settled).kind).toBe("completed");
    expect(outcome.staged.receipt.sourceSha).toBe(actions.candidate.source);
    expect(await NodeFSP.readFile(inspected.proofPath)).toEqual(proof);
    expect(await NodeFSP.readFile(f.effects)).toEqual(effects);
  } finally {
    controller.abort(new Error("Synthetic restage cleanup"));
    try {
      const join = await settled;
      if (join !== undefined)
        expect(["completed", "stopped", "not-started"]).toContain(join.kind);
      if (supervisor) {
        expect((await supervisor.sweep()).failed).toBe(0);
        expect(await supervisor.unreleasedJobs?.()).toEqual([]);
      }
      for (const exit of exits) {
        expect(exit.reaped).toBe(true);
        const workerPid = exit.pid;
        if (workerPid === undefined || workerPid <= 1)
          throw new Error("Restage worker identity is unproved.");
        for (const pid of [workerPid, -workerPid]) {
          let absent = false;
          try {
            process.kill(pid, 0);
          } catch (cause) {
            absent = (cause as NodeJS.ErrnoException).code === "ESRCH";
          }
          expect(absent).toBe(true);
        }
      }
    } catch (cause) {
      throw new Error(`Restage cleanup unproved; retained ${home.root}`, { cause });
    }
    await home.remove();
  }
}, 60_000);

it("holds a symlinked effects record even when the target has the expected bytes", async () =>
  fixture(async (f) => {
    const original = `${f.effects}.original`;
    await NodeFSP.rename(f.effects, original);
    await NodeFSP.symlink(original, f.effects);
    await expect(f.inspect()).rejects.toThrow();
  }));

it.each(["proof", "release temporary"] as const)(
  "syncs a retained %s after write-before-sync failure before releasing lifecycle",
  async (kind) =>
    fixture(async (f) => {
      const inspected = await f.inspect();
      const proof = String(inspected.proofSha256);
      const target =
        kind === "proof" ? inspected.proofPath : `${f.lifecycle}.recovery-${proof}.partial`;
      let failedSyncs = 0;
      const checkpoints: string[] = [];
      f.dependencies.checkpoint = async (phase) => {
        checkpoints.push(phase);
      };
      f.dependencies.syncFile = async (file, descriptor) => {
        if (file === target) {
          failedSyncs++;
          throw new Error("simulated file sync failure");
        }
        await descriptor.sync();
      };
      await expect(f.apply(proof)).rejects.toThrow("simulated file sync failure");
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toBeDefined();
      expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
      await expect(f.apply(proof)).rejects.toThrow("simulated file sync failure");
      expect(failedSyncs).toBe(2);
      expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
      expect(await preparationLifecycle(f.base, jobId).release(() => true)).toBe(
        "cleanup-unconfirmed",
      );
      expect(checkpoints).not.toContain("release-prepared");
      expect(checkpoints).not.toContain("release-durable");

      const events: string[] = [];
      f.dependencies.syncFile = async (file, descriptor) => {
        await descriptor.sync();
        if (file === target) events.push("retained-file-synced");
      };
      f.dependencies.checkpoint = async (phase) => {
        if (phase === "release-prepared") {
          expect(events).toEqual(["retained-file-synced"]);
          expect(JSON.parse(await NodeFSP.readFile(f.lifecycle, "utf8")).state).toBe("started");
          events.push(phase);
        } else if (phase === "release-durable") {
          events.push(phase);
        }
      };
      expect((await f.apply(proof)).state).toBe("released");
      expect(events).toEqual(["retained-file-synced", "release-prepared", "release-durable"]);
    }),
);
