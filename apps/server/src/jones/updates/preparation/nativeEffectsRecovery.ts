/* oxlint-disable t3code/no-global-process-runtime -- Standalone maintenance uses captured native host and process identity. */
// @effect-diagnostics nodeBuiltinImport:off - Standalone native maintenance boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  jonesCandidateHandle,
  requireJonesStageDirectory,
  validateJonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";

const HASH = /^[a-f0-9]{64}$/;
const LIMIT = 1024 * 1024;
const COMMAND = "__jones-reconcile-preparation";
// Geometry proves absence only for this producer's fixed, readonly, whole-image attachment contract.
const PRODUCER_SOURCE = "317c4df82c77d06922a6723e318e09b0de20ff99";
const digest = (value: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const fail = (): never => {
  throw new Error("Native preparation recovery is unproved.");
};
const record = (value: unknown): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return fail();
  return value as Record<string, unknown>;
};
const positive = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0;
const absolute = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length <= 4096 &&
  NodePath.isAbsolute(value) &&
  NodePath.resolve(value) === value;

export interface NativeEffectsRecoveryRequest {
  readonly schema: 1;
  readonly baseDir: string;
  readonly jobId: string;
  readonly worker: { readonly pid: number; readonly processGroup: number };
  readonly mountPath: string;
  readonly expected: {
    readonly request: string;
    readonly result: string;
    readonly lifecycle: string;
    readonly effects: string;
    readonly index: string;
    readonly receipt: string;
    readonly payload: string;
    readonly archive: string;
  };
}

function decode(
  value: unknown,
  isPreparationJobId: (id: string) => boolean,
): NativeEffectsRecoveryRequest {
  const r = record(value);
  const worker = record(r.worker);
  const expected = record(r.expected);
  if (
    r.schema !== 1 ||
    !absolute(r.baseDir) ||
    typeof r.jobId !== "string" ||
    !isPreparationJobId(r.jobId) ||
    !positive(worker.pid) ||
    worker.pid <= 1 ||
    worker.processGroup !== worker.pid ||
    !absolute(r.mountPath) ||
    !["request", "result", "lifecycle", "effects", "index", "receipt", "payload", "archive"].every(
      (key) => typeof expected[key] === "string" && HASH.test(expected[key] as string),
    )
  )
    return fail();
  return r as unknown as NativeEffectsRecoveryRequest;
}

async function directory(
  path: string,
  kind: "private" | "shared-ancestor" = "private",
): Promise<void> {
  const stat = await NodeFSP.lstat(path);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    (stat.mode & (kind === "shared-ancestor" ? 0o022 : 0o077)) !== 0 ||
    stat.uid !== process.getuid?.() ||
    (await NodeFSP.realpath(path)) !== path
  )
    return fail();
}

const recordIdentity = (stat: NodeFS.Stats) => [
  stat.dev,
  stat.ino,
  stat.nlink,
  stat.size,
  stat.mtimeMs,
  stat.ctimeMs,
];

async function read(file: string, limit = LIMIT, maxLinks = 1) {
  await directory(NodePath.dirname(file));
  const fd = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const before = await fd.stat();
    if (
      !before.isFile() ||
      before.nlink < 1 ||
      before.nlink > maxLinks ||
      before.size < 1 ||
      before.size > limit ||
      (before.mode & 0o077) !== 0 ||
      before.uid !== process.getuid?.()
    )
      return fail();
    const bytes = Buffer.alloc(before.size);
    const { bytesRead } = await fd.read(bytes, 0, bytes.length, 0);
    const after = await fd.stat();
    if (
      bytesRead !== before.size ||
      JSON.stringify(recordIdentity(before)) !== JSON.stringify(recordIdentity(after))
    )
      return fail();
    return {
      value: JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown,
      sha256: digest(bytes),
      identity: recordIdentity(before),
    };
  } finally {
    await fd.close();
  }
}

const absent = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ESRCH";
  }
};

async function command(
  executable: string,
  args: readonly string[],
  stdin?: string,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(
      executable,
      [...args],
      {
        encoding: "buffer",
        timeout: 30_000,
        maxBuffer: 8 * LIMIT,
        killSignal: "SIGKILL",
      },
      (error, stdout) => {
        if (error !== null) {
          reject(new Error("Native inventory command failed."));
          return;
        }
        try {
          resolve(new TextDecoder("utf-8", { fatal: true }).decode(stdout));
        } catch {
          reject(new Error("Native inventory encoding is invalid."));
        }
      },
    );
    child.stdin?.on("error", () => child.kill("SIGKILL"));
    child.stdin?.end(stdin);
  });
}

async function plist(executable: string, args: readonly string[]): Promise<unknown> {
  const xml = await command(executable, args);
  return JSON.parse(
    await command("/usr/bin/plutil", ["-convert", "json", "-o", "-", "-"], xml),
  ) as unknown;
}

export interface NativeEffectsRecoveryDependencies {
  readonly platform?: string;
  readonly architecture?: string;
  readonly absent?: (pid: number) => boolean;
  readonly plist?: typeof plist;
  readonly validateArtifact?: typeof validateJonesStagedArtifact;
  readonly syncFile?: (file: string, descriptor: NodeFSP.FileHandle) => Promise<void>;
  readonly checkpoint?: (
    phase: "proof-durable" | "release-prepared" | "release-durable",
  ) => Promise<void>;
}

async function syncDirectory(path: string) {
  const fd = await NodeFSP.open(path, "r");
  try {
    await fd.sync();
  } finally {
    await fd.close();
  }
}

async function createProof(
  file: string,
  value: unknown,
  syncFile: NonNullable<NativeEffectsRecoveryDependencies["syncFile"]>,
) {
  const bytes = `${JSON.stringify(value)}\n`;
  if (Buffer.byteLength(bytes) > LIMIT) return fail();
  let created: NodeFSP.FileHandle | undefined;
  try {
    created = await NodeFSP.open(file, "wx", 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") return fail();
  }
  if (created !== undefined) {
    try {
      await created.writeFile(bytes);
      await syncFile(file, created);
    } finally {
      await created.close();
    }
  } else {
    const retained = await read(file);
    if (retained.sha256 !== digest(bytes)) return fail();
    const descriptor = await NodeFSP.open(
      file,
      NodeFS.constants.O_RDWR | NodeFS.constants.O_NOFOLLOW,
    );
    try {
      const expectedIdentity = JSON.stringify(retained.identity);
      const matches = (stat: NodeFS.Stats) =>
        stat.isFile() &&
        stat.uid === process.getuid?.() &&
        (stat.mode & 0o077) === 0 &&
        JSON.stringify(recordIdentity(stat)) === expectedIdentity;
      if (!matches(await descriptor.stat())) return fail();
      // Readable bytes can survive a failed sync; retry must make this exact inode durable.
      await syncFile(file, descriptor);
      if (!matches(await descriptor.stat()) || !matches(await NodeFSP.lstat(file))) return fail();
      await directory(NodePath.dirname(file));
    } finally {
      await descriptor.close();
    }
  }
  await syncDirectory(NodePath.dirname(file));
}

/** Maintenance proves disjoint geometry; it never claims that the worker cleaned native effects. */
export async function recoverNativeEffects(
  value: unknown,
  mode: "inspect" | "apply",
  expectedProof?: string,
  dependencies: NativeEffectsRecoveryDependencies = {},
) {
  const { withQualifiedRuntimeLock } = await import("../../cloud/qualifiedRuntime.ts");
  const {
    decodePreparationRequest,
    decodePreparationResult,
    isPreparationJobId,
    preparationJobLock,
    preparationJobsDirectory,
    PREPARATION_LOCK,
  } = await import("./protocol.ts");
  const input = decode(value, isPreparationJobId);
  if (
    (dependencies.platform ?? process.platform) !== "darwin" ||
    (dependencies.architecture ?? process.arch) !== "arm64"
  )
    return fail();
  await directory(input.baseDir, "shared-ancestor");
  const runtime = NodePath.join(input.baseDir, "runtime");
  await directory(runtime, "shared-ancestor");
  const root = NodePath.join(runtime, "jones-preparation");
  await directory(root);
  return withQualifiedRuntimeLock(input.baseDir, PREPARATION_LOCK, () =>
    withQualifiedRuntimeLock(input.baseDir, preparationJobLock(input.jobId), async () => {
      const lifecyclePath = NodePath.join(root, "lifecycle", `${input.jobId}.json`);
      const effectsPath = NodePath.join(root, "lifecycle", `${input.jobId}.effects.json`);
      const proofRoot = NodePath.join(runtime, "jones-preparation-recovery");
      const proofPath = NodePath.join(proofRoot, `${input.jobId}.json`);
      const planSha256 = digest(JSON.stringify(input));
      const gone = () => {
        const check = dependencies.absent ?? absent;
        if (!check(input.worker.pid) || !check(-input.worker.processGroup)) return fail();
      };
      const lifecycle = await read(lifecyclePath, 4096);
      const launch = record(lifecycle.value);
      const worker = record(launch.worker);
      if (
        launch.schema !== 1 ||
        launch.jobId !== input.jobId ||
        launch.kind !== "stage" ||
        worker.pid !== input.worker.pid ||
        worker.processGroup !== input.worker.processGroup
      )
        return fail();
      gone();
      if (launch.state === "released") {
        const saved = record((await read(proofPath)).value);
        if (
          saved.schema !== 1 ||
          saved.planSha256 !== planSha256 ||
          saved.releasedSha256 !== lifecycle.sha256 ||
          (mode === "apply" && saved.proofSha256 !== expectedProof)
        )
          return fail();
        const { proofSha256, ...evidence } = saved;
        if (digest(JSON.stringify(evidence)) !== proofSha256) return fail();
        return { schema: 1, state: "released", proofSha256, proofPath };
      }
      if (launch.state !== "started" || lifecycle.sha256 !== input.expected.lifecycle)
        return fail();
      const jobDir = NodePath.join(preparationJobsDirectory(input.baseDir), input.jobId);
      const requestFile = NodePath.join(jobDir, "request.json");
      const resultFile = NodePath.join(jobDir, "result.json");
      const request = await read(requestFile, 64 * 1024);
      const result = await read(resultFile, 256 * 1024);
      const effects = await read(effectsPath, 4096);
      const job = decodePreparationRequest(request.value, jobDir);
      const outcome = decodePreparationResult(result.value, input.jobId, "stage");
      const native = record(effects.value);
      if (
        request.sha256 !== input.expected.request ||
        result.sha256 !== input.expected.result ||
        effects.sha256 !== input.expected.effects ||
        job?.job.kind !== "stage" ||
        job.baseDir !== input.baseDir ||
        !job.processGroup ||
        job.job.host.platform !== "darwin" ||
        job.job.host.architecture !== "arm64" ||
        outcome?.ok !== false ||
        outcome.error.name !== "QualifiedRuntimeBlockedError" ||
        outcome.error.reason !== "busy" ||
        native.schema !== 1 ||
        native.jobId !== input.jobId ||
        native.pid !== input.worker.pid ||
        native.phase !== "native-effects"
      )
        return fail();
      const requestKeys = Object.keys(record(record(request.value).job)).sort();
      if (
        job.job.cacheRoot !== NodePath.join(runtime, "jones-actions") ||
        job.job.candidate.installedSource !== PRODUCER_SOURCE ||
        JSON.stringify(requestKeys) !==
          JSON.stringify(["activeVersion", "cacheRoot", "candidate", "dbPath", "host", "kind"])
      )
        return fail();
      const handle = jonesCandidateHandle(job.job.candidate);
      const indexPath = NodePath.join(job.job.cacheRoot, "completed", `${handle}.json`);
      const index = await read(indexPath, 4096, 2);
      const selection = record(index.value);
      if (
        index.sha256 !== input.expected.index ||
        selection.schema !== 1 ||
        selection.handle !== handle ||
        !absolute(selection.directory)
      )
        return fail();
      const stage = await requireJonesStageDirectory(
        job.job.cacheRoot,
        handle,
        selection.directory,
      );
      await directory(stage);
      const receiptPath = NodePath.join(stage, "jones-actions-stage.json");
      const receipt = await read(receiptPath, 128 * 1024);
      if (receipt.sha256 !== input.expected.receipt) return fail();
      const payload = record(receipt.value).payloadPath;
      if (
        !absolute(payload) ||
        NodePath.dirname(payload) !== stage ||
        (await NodeFSP.realpath(payload)) !== payload
      )
        return fail();
      const payloadStat = await NodeFSP.lstat(payload);
      const archivePath = NodePath.join(stage, "github-artifact.zip");
      const archiveStat = await NodeFSP.lstat(archivePath);
      const fileIdentity = (s: typeof payloadStat) => [
        s.dev,
        s.ino,
        s.nlink,
        s.size,
        s.mtimeMs,
        s.ctimeMs,
      ];
      for (const stat of [payloadStat, archiveStat]) {
        if (
          !stat.isFile() ||
          stat.isSymbolicLink() ||
          (stat.mode & 0o077) !== 0 ||
          stat.uid !== process.getuid?.()
        )
          return fail();
      }
      const validate = dependencies.validateArtifact ?? validateJonesStagedArtifact;
      const artifact = await validate(stage, job.job.candidate);
      if (
        artifact.payloadPath !== payload ||
        artifact.stagedHandle !== handle ||
        artifact.receipt.sha256 !== input.expected.payload ||
        artifact.candidate.artifactDigest !== input.expected.archive ||
        artifact.candidate.platform !== "darwin" ||
        artifact.candidate.architecture !== "arm64"
      )
        return fail();
      const mountOwner = NodePath.dirname(input.mountPath);
      if (
        NodePath.dirname(mountOwner) !== runtime ||
        !/^\.jones-dmg-mount-[a-zA-Z0-9]+$/.test(NodePath.basename(mountOwner)) ||
        NodePath.basename(input.mountPath) !== "volume"
      )
        return fail();
      await directory(mountOwner);
      await directory(input.mountPath);
      const nativePlist = dependencies.plist ?? plist;
      const image = record(await nativePlist("/usr/bin/hdiutil", ["imageinfo", "-plist", payload]));
      const properties = record(image.Properties);
      const size = record(image["Size Information"]);
      if (
        image.Format !== "UDZO" ||
        properties.Encrypted !== false ||
        properties.Partitioned !== false ||
        !Array.isArray(image.Segments) ||
        image.Segments.length !== 1 ||
        image.Segments[0] !== payload ||
        !positive(size["Total Bytes"])
      )
        return fail();
      const virtualBytes = size["Total Bytes"];
      const inventory = async () => {
        const raw = record(await nativePlist("/usr/bin/hdiutil", ["info", "-plist"]));
        if (!Array.isArray(raw.images) || raw.images.length > 1024) return fail();
        const images = [];
        let entitiesCount = 0;
        for (const item of raw.images) {
          const attached = record(item);
          const path = attached["image-path"];
          const entities = attached["system-entities"];
          if (
            !absolute(path) ||
            path === payload ||
            !Array.isArray(entities) ||
            entities.length === 0 ||
            (entitiesCount += entities.length) > 8192
          )
            return fail();
          // Existing image paths must resolve; missing aliases cannot prove disjoint identity.
          const attachedStat = await NodeFSP.stat(path);
          if (
            (await NodeFSP.realpath(path)) === payload ||
            (attachedStat.dev === payloadStat.dev && attachedStat.ino === payloadStat.ino)
          )
            return fail();
          const roots = new Set<string>();
          const projected = entities.map((entry) => {
            const entity = record(entry);
            const device = entity["dev-entry"];
            const mount = entity["mount-point"];
            if (
              typeof device !== "string" ||
              !/^\/dev\/disk[0-9]+(?:s[0-9]+)*$/.test(device) ||
              (mount !== undefined &&
                (!absolute(mount) ||
                  mount === input.mountPath ||
                  mount === mountOwner ||
                  mount.startsWith(`${mountOwner}/`)))
            )
              return fail();
            roots.add(device.replace(/s[0-9].*$/, ""));
            return { device, ...(mount === undefined ? {} : { mount }) };
          });
          const capacities = [];
          for (const device of [...roots].sort()) {
            const disk = record(
              await nativePlist("/usr/sbin/diskutil", ["info", "-plist", device]),
            );
            if (
              disk.WholeDisk !== true ||
              disk.DeviceNode !== device ||
              !positive(disk.TotalSize) ||
              disk.Size !== disk.TotalSize ||
              disk.IOKitSize !== disk.TotalSize ||
              disk.TotalSize === virtualBytes
            )
              return fail();
            capacities.push({ device, bytes: disk.TotalSize });
          }
          images.push({
            path,
            identity: [attachedStat.dev, attachedStat.ino],
            entities: projected.sort((a, b) => a.device.localeCompare(b.device)),
            capacities,
          });
        }
        return images.sort((a, b) => a.path.localeCompare(b.path));
      };
      const first = await inventory();
      const second = await inventory();
      if (JSON.stringify(first) !== JSON.stringify(second)) return fail();
      await validate(stage, job.job.candidate);
      const snapshots = [
        [requestFile, request],
        [resultFile, result],
        [lifecyclePath, lifecycle],
        [effectsPath, effects],
        [indexPath, index],
        [receiptPath, receipt],
      ] as const;
      const fresh = async () => {
        gone();
        for (const [path, original] of snapshots) {
          const current = await read(path, LIMIT, path === indexPath ? 2 : 1);
          if (JSON.stringify(current) !== JSON.stringify(original)) return fail();
        }
        for (const [path, original] of [
          [payload, payloadStat],
          [archivePath, archiveStat],
        ] as const) {
          if (
            JSON.stringify(fileIdentity(await NodeFSP.lstat(path))) !==
            JSON.stringify(fileIdentity(original))
          )
            return fail();
        }
        await directory(mountOwner);
        await directory(input.mountPath);
      };
      await fresh();
      const released = { ...launch, state: "released" };
      const releasedBytes = `${JSON.stringify(released)}\n`;
      const evidence = {
        schema: 1,
        planSha256,
        input,
        virtualBytes,
        inventory: first,
        producerContract: {
          source: PRODUCER_SOURCE,
          command: "/usr/bin/hdiutil",
          attachArguments: [
            "attach",
            "-readonly",
            "-nobrowse",
            "-noautoopen",
            "-mountpoint",
            input.mountPath,
            payload,
          ],
        },
        original: { request: job, result: outcome, lifecycle: launch, effects: native },
        files: snapshots.map(([path, snapshot]) => ({
          path,
          sha256: snapshot.sha256,
          identity: snapshot.identity,
        })),
        artifact: {
          handle,
          directory: stage,
          receipt: artifact.receipt,
          payloadIdentity: fileIdentity(payloadStat),
          archiveIdentity: fileIdentity(archiveStat),
        },
        releasedSha256: digest(releasedBytes),
        disposition: "native-effects-disjoint",
      };
      const proofSha256 = digest(JSON.stringify(evidence));
      if (mode === "inspect")
        return { schema: 1, state: "eligible", proofSha256, proofPath, evidence };
      if (expectedProof !== proofSha256) return fail();
      await NodeFSP.mkdir(proofRoot, { mode: 0o700 }).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "EEXIST") throw error;
      });
      await directory(proofRoot);
      await syncDirectory(runtime);
      const syncFile: NonNullable<NativeEffectsRecoveryDependencies["syncFile"]> =
        dependencies.syncFile ?? ((_file, descriptor) => descriptor.sync());
      await createProof(proofPath, { ...evidence, proofSha256 }, syncFile);
      await dependencies.checkpoint?.("proof-durable");
      await fresh();
      const temporary = `${lifecyclePath}.recovery-${proofSha256}.partial`;
      await createProof(temporary, released, syncFile);
      await fresh();
      await dependencies.checkpoint?.("release-prepared");
      await fresh();
      await NodeFSP.rename(temporary, lifecyclePath);
      await syncDirectory(NodePath.dirname(lifecyclePath));
      await dependencies.checkpoint?.("release-durable");
      if ((await read(lifecyclePath)).sha256 !== digest(releasedBytes)) return fail();
      return { schema: 1, state: "released", proofSha256, proofPath };
    }),
  );
}

export async function runNativeEffectsRecoveryCommand(args: readonly string[]): Promise<void> {
  try {
    if (args.length === 1 && args[0] === "--probe") {
      process.stdout.write(
        `${JSON.stringify({ schema: 1, command: COMMAND, maintenanceOnly: true })}\n`,
      );
      return;
    }
    if (
      (args[0] !== "inspect" && args[0] !== "apply") ||
      args[1] !== "--request" ||
      !absolute(args[2]) ||
      (args[0] === "inspect"
        ? args.length !== 3
        : args.length !== 5 || args[3] !== "--expect-proof" || !HASH.test(args[4] ?? ""))
    )
      return fail();
    const request = await read(args[2], 64 * 1024);
    const result = await recoverNativeEffects(request.value, args[0], args[4]);
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch {
    process.stdout.write(`${JSON.stringify({ schema: 1, state: "held" })}\n`);
    process.exitCode = 2;
  }
}
