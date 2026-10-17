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
// __commonJSMin/__toESM wrap only the bundled CommonJS extend package; require_extend() is
// that local initializer call, never a module loader. Effect allocates local schemas/caches,
// inspects Error stacks and uses optional async_hooks. Its scheduler/fiber effects are deferred
// to invocation. Upstream nodeSqliteClient statically binds util and worker_threads, but
// constructs reader/checkpoint Workers only for readerWorker/checkpointWorker options, which
// the worker's sole layer({ filename, readonly }) call never sets. Upstream ThreadFind adds the
// unified/remark pipeline: import builds in-memory processors; VFile, and its process.cwd()
// read, exists only while thread search processes text, which preparation never invokes.
// node:module and the self dynamic import are bundler metadata, not emitted loading.
// Native worker dispatch separately requires SEA/Node 26.8.2/Darwin arm64 and inherited owner
// descriptors before bounded IPC, retained SQLite scopes and CLOSED_RESULT. This policy gives
// the worker no producer download, child-process, tool, cold-owner, activation or state admission.
const workerImport = (
  kind: "static" | "process-builtin" | "require",
  specifier: string | null,
  expressionSha256: string,
) => ({
  kind,
  specifier,
  computed: specifier === null,
  builtin: specifier !== null,
  expressionSha256,
});
const asyncHooksImport = workerImport(
  "process-builtin",
  "node:async_hooks",
  "d64bd1adb71bdcc572de3468d66276ee84b3df3e7df69858acd26bde23a60ddb",
);
const workerPolicy = {
  schema: "jones.preparation-worker-policy/v2",
  entry: {
    sha256: "6ba71f75fc17d4d8059afa1b170b5e97f7494f6f828f3299066f4e8a7cf9a03f",
    size: 2917415,
  },
  // Exact syntax records in emitted order; each hash binds one reviewed source expression.
  imports: [
    workerImport(
      "static",
      "node:fs",
      "b6471bb6742ff95f8b90244d773ca1c10cc1db5944b113871b5a68c8cbb7d5f7",
    ),
    workerImport(
      "static",
      "node:fs/promises",
      "1f29457074f2f8d43846ad4a6d8ed6be9ef55c3ec889034c42124eece4c51ca9",
    ),
    workerImport(
      "static",
      "node:crypto",
      "68ea541bde6e48ae4d48e87096319c19cb79eff795f410e570728487674bfc30",
    ),
    workerImport(
      "static",
      "node:path",
      "9bdcd607570c70a7543e1c3e6960556cc1cd881e04f742ba1df616cdd1af45a5",
    ),
    workerImport(
      "static",
      "node:path",
      "4640f7f903e0f3b35bbfb2245c2e02ff59c9da4552512bcd105e55de7bdde77c",
    ),
    workerImport(
      "static",
      "node:sea",
      "598d1644c9184ec281b26857374b87c2584583cdb661aa37d865d659f3782cca",
    ),
    workerImport(
      "static",
      "node:url",
      "945f2c2f0419c74ddaefb5d7910178df8de728174d3b31c98a1b5dedb5f05efb",
    ),
    workerImport(
      "static",
      "node:url",
      "abfa2dd0cf2eaa2177cdf7efbbd423ec718fedbe133c8eb9c8730e82a358401b",
    ),
    workerImport(
      "static",
      "node:sqlite",
      "2bd5504e1d0bb27346554ed60f076385cb4382e78c43a191bf44f3ca1ab93154",
    ),
    workerImport(
      "static",
      "node:util",
      "a138b11f80981629c94d05920e4c2e87f7d8c56dc577c4daaf6301e1ab935ee9",
    ),
    workerImport(
      "static",
      "node:worker_threads",
      "57702c143ca0016269f6a75a49258b783d817f1f9e106e5f3d644f934bf7f743",
    ),
    workerImport(
      "static",
      "node:process",
      "2de05ae74318d70e976279b10d352685d0a77c26ae477e009ebb6390b452eda1",
    ),
    asyncHooksImport,
    // sha256("require_extend()"): the bundler-local CommonJS initializer, not a computed loader.
    workerImport(
      "require",
      null,
      "08c78723a6d6d64cbdf26ff77b54a66ef44596967d50811517d940773f4266b6",
    ),
  ],
  // Bundler-reported imports; node:module and the self import are metadata-only evidence.
  bundlerImports: [
    "node:module",
    "node:fs",
    "node:fs/promises",
    "node:crypto",
    "node:path",
    "node:sea",
    "node:url",
    "node:sqlite",
    "node:util",
    "node:worker_threads",
    "node:process",
  ],
  bundlerDynamicImports: ["offlinePreparationEntry.mjs"],
  staticEvidence: {
    emitted: [
      "node:crypto",
      "node:fs",
      "node:fs/promises",
      "node:path",
      "node:process",
      "node:sea",
      "node:sqlite",
      "node:url",
      "node:util",
      "node:worker_threads",
    ],
    reported: [
      "node:crypto",
      "node:fs",
      "node:fs/promises",
      "node:module",
      "node:path",
      "node:process",
      "node:sea",
      "node:sqlite",
      "node:url",
      "node:util",
      "node:worker_threads",
    ],
    metadataOnly: ["node:module"],
    syntaxOnly: [],
  },
  dynamicEvidence: {
    emitted: [],
    reported: ["offlinePreparationEntry.mjs"],
    metadataOnly: ["offlinePreparationEntry.mjs"],
    syntaxOnly: [],
  },
  processBuiltinCalls: [asyncHooksImport],
  generatedRuntime: {
    sha256: "12e51bf9c791c00b848088823f4f70e11310a9a7ba55d0cf2a74d9daa57119be",
    startByte: 532,
    endByte: 2070,
  },
  receiver: "inherited-native-owner-before-sqlite-and-ipc;closed-result-after-scope-close",
  initializers: [
    "local-schema-cache-stack-detection",
    "deferred-fiber-scheduler",
    "fixed-optional-async-hooks",
    "local-commonjs-extend",
    "in-memory-markdown-processors",
  ],
  workerThreads: "imported-never-constructed-without-reader-or-checkpoint-option",
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
    !same(emitted.imports, workerPolicy.bundlerImports) ||
    !same(emitted.dynamicImports, workerPolicy.bundlerDynamicImports) ||
    !Array.isArray(imports) ||
    imports.length !== workerPolicy.imports.length
  )
    throw new Error("Preparation emitted inventory differs from the reviewed worker.");
  // Each record must equal its reviewed expression exactly; no other computed loader is admitted.
  for (const [index, item] of imports.entries()) {
    if (!same(record(item), workerPolicy.imports[index]))
      throw new Error("Preparation import inventory differs from the reviewed worker.");
  }
  // Metadata-only node:module and self-import are recorded discrepancies, never runtime permission.
  const evidence = record(emitted.importEvidence);
  if (
    !same(Object.keys(evidence).sort(), [
      "dynamic",
      "processBuiltinCalls",
      "qualification",
      "static",
      "syntax",
    ]) ||
    evidence.qualification !== "unqualified" ||
    !same(evidence.syntax, syntax) ||
    !same(evidence.static, workerPolicy.staticEvidence) ||
    !same(evidence.dynamic, workerPolicy.dynamicEvidence) ||
    !same(evidence.processBuiltinCalls, workerPolicy.processBuiltinCalls)
  )
    throw new Error("Preparation import evidence differs from the reviewed worker.");
  const runtime = record(emitted.generatedRuntime),
    expectedRuntime = workerPolicy.generatedRuntime;
  if (
    runtime.availability !== "observed" ||
    runtime.evidence !== "emitted-top-level-region" ||
    runtime.startByte !== expectedRuntime.startByte ||
    runtime.endByte !== expectedRuntime.endByte ||
    runtime.size !== expectedRuntime.endByte - expectedRuntime.startByte ||
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
