// @effect-diagnostics nodeBuiltinImport:off -- This build-time receiver hashes bounded source and emitted bytes; it never imports the candidate.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

export interface FunctionalPreparationGraphBinding {
  readonly policySha256: string;
  readonly graphSha256: string;
  readonly entrySha256: string;
}

const releaseManifests = [
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
];
const sourceFiles = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  ...releaseManifests,
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  ...[
    "offlinePreparationEntry.ts",
    "offlinePreparation.ts",
    "offlinePreparationCore.ts",
    "offlinePreparationPlan.ts",
    "offlinePreparationWorker.ts",
  ].map((name) => `apps/server/src/jones/updates/${name}`),
];
const digest = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const record = (value: unknown): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Preparation policy requires an object.");
  return value as Record<string, unknown>;
};
const decode = (bytes: Uint8Array): unknown =>
  JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
const same = NodeUtil.isDeepStrictEqual;

// Reviewed receiver contract for the exact worker bytes pinned below, equal on hosted Ubuntu and Darwin.
// Fixed failure phases use a private local registry and never serialize raw exceptions.
// The lazy __esmMin initializer caches its result/error; __exportAll installs local getters.
// Effect allocates local schemas/caches, inspects Error stacks and uses optional async_hooks.
// Its scheduler/fiber effects are deferred to invocation. No external dynamic loader remains.
// Native worker dispatch separately requires SEA/Node 26.8.2/Darwin arm64 and inherited owner
// descriptors before bounded IPC, retained SQLite scopes and CLOSED_RESULT. This policy gives
// the worker no producer download, child-process, tool, cold-owner, activation or state admission.
const workerPolicy = {
  schema: "jones.preparation-worker-policy/v1",
  entry: {
    sha256: "7582c14901fda82b21e121f6a42f67bf485860dad47738b5dbbfa8e3ed52cc0d",
    size: 1888224,
  },
  staticImports: [
    "node:fs",
    "node:fs/promises",
    "node:crypto",
    "node:path",
    "node:sea",
    "node:url",
    "node:sqlite",
  ],
  optionalBuiltin: "node:async_hooks",
  generatedRuntime: {
    sha256: "d0cade7c803777eac7670c49f9841f6991e8089cea038bcfc91f1fd8b4523e09",
    startByte: 311,
    endByte: 771,
  },
  receiver: "inherited-native-owner-before-sqlite-and-ipc;closed-result-after-scope-close",
  initializers:
    "local-schema-cache-stack-detection;deferred-fiber-scheduler;fixed-optional-async-hooks",
  externalDynamicImports: false,
  producerPermissions: false,
  nativeColdOwnerQualified: false,
  stateAdmissionAllowed: false,
  activationAllowed: false,
} as const;

/** Only existing release-stamp fields may differ from the actual committed manifest. */
export function verifyPreparationSourceOverlay(
  file: string,
  original: unknown,
  actual: unknown,
  identity: { sha: string; tree: string },
): string {
  if (!releaseManifests.includes(file)) throw new Error("Unadmitted source overlay.");
  const before = { ...record(original) },
    after = { ...record(actual) };
  const version = after.version;
  if (typeof version !== "string" || version.length === 0)
    throw new Error("Release manifest version is absent.");
  if (["apps/server/package.json", "apps/desktop/package.json"].includes(file)) {
    if (
      after.jonesSource !== undefined &&
      !same(after.jonesSource, {
        repository: "Jones-Systems/Jones-Code",
        ...identity,
      })
    )
      throw new Error("Stamped source identity differs from observed Git source.");
    if (after.startupGateProtocol !== undefined && after.startupGateProtocol !== 1)
      throw new Error("Unadmitted startup metadata.");
    delete before.jonesSource;
    delete after.jonesSource;
    delete before.startupGateProtocol;
    delete after.startupGateProtocol;
  }
  delete before.version;
  delete after.version;
  if (!same(before, after))
    throw new Error("Preparation source has changes beyond release stamping.");
  return version;
}

function readSource(root: string, relative: string): Buffer {
  const path = NodePath.join(root, relative);
  if (NodeFS.realpathSync(path) !== path) throw new Error("Preparation source path is aliased.");
  const fd = NodeFS.openSync(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  let primary: unknown, bytes: Buffer | undefined;
  try {
    const before = NodeFS.fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.nlink !== 1n || before.size > 8n * 1024n * 1024n)
      throw new Error("Preparation source is not a bounded regular file.");
    const buffer = Buffer.alloc(Number(before.size) + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = NodeFS.readSync(fd, buffer, offset, buffer.length - offset, offset);
      if (count === 0) break;
      offset += count;
    }
    const after = NodeFS.fstatSync(fd, { bigint: true }),
      current = NodeFS.lstatSync(path, { bigint: true });
    if (
      BigInt(offset) !== before.size ||
      [after, current].some(
        (value) =>
          value.dev !== before.dev ||
          value.ino !== before.ino ||
          value.size !== before.size ||
          value.mtimeNs !== before.mtimeNs ||
          value.ctimeNs !== before.ctimeNs ||
          value.nlink !== before.nlink,
      )
    )
      throw new Error("Preparation source changed while hashing.");
    bytes = buffer.subarray(0, offset);
  } catch (cause) {
    primary = cause;
  }
  try {
    NodeFS.closeSync(fd);
  } catch (cause) {
    throw Object.assign(
      new AggregateError(
        primary === undefined ? [cause] : [primary, cause],
        "Preparation policy input descriptor closure is unproved.",
        { cause: primary ?? cause },
      ),
      { preserveScratch: true },
    );
  }
  if (primary !== undefined) throw primary;
  return bytes!;
}

/** Exact-byte worker permission only. The producer separately observes Git originals and identity
 * before native work and again before staging; this return value is never a build handle. */
export function verifyFunctionalPreparationGraph(input: {
  readonly repoRoot: string;
  readonly reportBytes: Uint8Array;
  readonly entryBytes: Uint8Array;
}): FunctionalPreparationGraphBinding {
  if (
    input.entryBytes.length !== workerPolicy.entry.size ||
    digest(input.entryBytes) !== workerPolicy.entry.sha256
  )
    throw new Error("Functional preparation entry is outside the reviewed worker policy.");
  if (input.reportBytes.length === 0 || input.reportBytes.length > 16 * 1024 * 1024)
    throw new Error("Preparation graph exceeds its bound.");
  const report = record(decode(input.reportBytes));
  if (
    report.schemaVersion !== 1 ||
    report.classification !== "preparation-candidate-graph" ||
    report.qualification !== "unqualified" ||
    report.executed !== false ||
    report.preparationCapable !== false ||
    report.productionInvocation !== false ||
    report.entry !== "src/jones/updates/offlinePreparationEntry.ts"
  )
    throw new Error("Preparation graph is outside the nonexecuted observation contract.");
  const emitted = record(report.emitted),
    syntax = record(emitted.syntax),
    imports = syntax.imports;
  if (
    emitted.format !== "esm" ||
    emitted.fileName !== "offlinePreparationEntry.mjs" ||
    emitted.mapPresent !== false ||
    !same(emitted.assets, []) ||
    !same(emitted.files, [{ fileName: "offlinePreparationEntry.mjs", ...workerPolicy.entry }]) ||
    !Array.isArray(imports) ||
    imports.length !== 8
  )
    throw new Error("Preparation emitted inventory differs from the reviewed worker.");
  for (const [index, item] of imports.entries()) {
    const observed = record(item);
    if (
      observed.kind !== (index < 7 ? "static" : "process-builtin") ||
      observed.specifier !==
        (index < 7 ? workerPolicy.staticImports[index] : workerPolicy.optionalBuiltin) ||
      observed.computed !== false ||
      observed.builtin !== true
    )
      throw new Error("Preparation import inventory differs from the reviewed worker.");
  }
  // Metadata-only node:module and self-import are recorded discrepancies, never runtime permission.
  const evidence = record(emitted.importEvidence);
  if (
    !same(record(evidence.static).emitted, [...workerPolicy.staticImports].sort()) ||
    !same(record(evidence.static).metadataOnly, ["node:module"]) ||
    !same(record(evidence.dynamic).emitted, []) ||
    !same(record(evidence.dynamic).metadataOnly, ["offlinePreparationEntry.mjs"])
  )
    throw new Error("Preparation import evidence differs from the reviewed worker.");
  const runtime = record(emitted.generatedRuntime),
    expectedRuntime = workerPolicy.generatedRuntime;
  if (
    runtime.availability !== "observed" ||
    runtime.evidence !== "emitted-top-level-region" ||
    runtime.startByte !== expectedRuntime.startByte ||
    runtime.endByte !== expectedRuntime.endByte ||
    runtime.size !== 460 ||
    runtime.sha256 !== expectedRuntime.sha256 ||
    digest(input.entryBytes.subarray(expectedRuntime.startByte, expectedRuntime.endByte)) !==
      expectedRuntime.sha256
  )
    throw new Error("Preparation generated runtime differs from the reviewed worker.");
  const source = record(report.source),
    hashes = record(source.hashes),
    overlays = source.buildOnlyOverlays;
  if (
    source.repository !== "Jones-Systems/Jones-Code" ||
    typeof source.sha !== "string" ||
    !/^[a-f0-9]{40}$/.test(source.sha) ||
    typeof source.tree !== "string" ||
    !/^[a-f0-9]{40}$/.test(source.tree) ||
    typeof source.version !== "string" ||
    source.version.length === 0 ||
    source.version.length > 128 ||
    !Array.isArray(overlays) ||
    new Set(overlays).size !== overlays.length ||
    overlays.some(
      (file: unknown) => typeof file !== "string" || !releaseManifests.includes(file),
    ) ||
    !same(Object.keys(hashes).sort(), [...sourceFiles].sort())
  )
    throw new Error("Preparation source inventory differs from the fixed contract.");
  const root = NodeFS.realpathSync(input.repoRoot);
  for (const file of sourceFiles) {
    const bytes = readSource(root, file);
    if (digest(bytes) !== hashes[file])
      throw new Error(`Preparation graph source bytes are stale: ${file}`);
    if (releaseManifests.includes(file)) {
      const manifest = decode(bytes);
      if (
        verifyPreparationSourceOverlay(file, manifest, manifest, {
          sha: source.sha,
          tree: source.tree,
        }) !== source.version
      )
        throw new Error("Preparation graph version differs from release manifests.");
    }
  }
  // Tool/patch observations remain diagnostic. This policy does not attest the collector or toolchain.
  return {
    policySha256: digest(JSON.stringify(workerPolicy)),
    graphSha256: digest(input.reportBytes),
    entrySha256: workerPolicy.entry.sha256,
  };
}
