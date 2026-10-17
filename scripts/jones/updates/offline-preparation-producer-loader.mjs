import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeProcess from "node:process";

const producerDirectory = "apps/server/dist-exe/preparation-producer-candidate";
const producerFile = `${producerDirectory}/offlinePreparationProducer.mjs`;
const reportFile = `${producerDirectory}/producer-candidate-graph.json`;
const capturedProducerFile = "scripts/jones/updates/offlinePreparationProducer.mjs";
const producerEntry = "scripts/jones/updates/offline-preparation-producer-entry.ts";
const releaseManifests = [
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
];
const sharedInputs = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  ...releaseManifests,
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
];
// This fixed access contract is independent of the unexecuted collector and its report.
const producerInputs = [
  ...sharedInputs,
  "scripts/package.json",
  producerEntry,
  "scripts/jones/updates/functional-policy.ts",
  "scripts/jones/updates/legacy-bootstrap-bundle.ts",
];
const qualifierInputs = [
  ...sharedInputs,
  ...[
    "offlinePreparationEntry.ts",
    "offlinePreparation.ts",
    "offlinePreparationCore.ts",
    "offlinePreparationPlan.ts",
    "offlinePreparationWorker.ts",
  ].map((name) => `apps/server/src/jones/updates/${name}`),
  "scripts/jones/updates/legacy-preparation-native-fixture.mjs",
  "scripts/jones/updates/legacy-preparation-native-fixture.d.mts",
  "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs",
  "scripts/jones/updates/legacy-identity-native-fixture.mjs",
  ...[
    "jonesLegacyBootstrapSource.ts",
    "jonesLegacyMaintenanceSource.ts",
    "jonesNativeHelperSource.ts",
    "jonesPairedStateSource.ts",
    "jonesColdPairSource.ts",
    "jonesLegacyIdentityNative.c",
    "jonesColdReceiverSource.ts",
    "jonesColdOwnerSource.ts",
    "jonesPreparationWorkerSource.ts",
  ].map((name) => `apps/desktop/src/jones/updates/${name}`),
  "apps/server/src/jones/updates/offlinePreparationWorker.test.ts",
];
const sourceInputs = [...new Set([...producerInputs, ...qualifierInputs])].sort();
const sourceLimit = 8 * 1024 * 1024;
const producerLimit = 32 * 1024 * 1024;
const reportLimit = 16 * 1024 * 1024;
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const contains = (parent, child) => {
  const relative = NodePath.relative(parent, child);
  return (
    relative === "" ||
    (relative !== ".." &&
      !relative.startsWith(`..${NodePath.sep}`) &&
      !NodePath.isAbsolute(relative))
  );
};
const overlaps = (left, right) => contains(left, right) || contains(right, left);
const identity = (stat) => ({
  device: String(stat.dev),
  inode: String(stat.ino),
  uid: Number(stat.uid),
});
const sameIdentity = (left, right) =>
  left.dev === right.dev && left.ino === right.ino && left.uid === right.uid;
const sameFile = (left, right) =>
  sameIdentity(left, right) &&
  left.size === right.size &&
  left.mode === right.mode &&
  left.nlink === right.nlink &&
  left.mtimeNs === right.mtimeNs &&
  left.ctimeNs === right.ctimeNs;

function contentBinding(value, limit) {
  if (
    !isObject(value) ||
    typeof value.sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(value.sha256) ||
    !Number.isSafeInteger(value.size) ||
    value.size < 0 ||
    value.size > limit
  )
    throw new Error("Producer content binding is invalid or exceeds its bound.");
  return { sha256: value.sha256, size: value.size };
}

function validateReport(report) {
  if (
    !isObject(report) ||
    report.schemaVersion !== 1 ||
    report.classification !== "preparation-producer-candidate-graph" ||
    report.qualification !== "unqualified" ||
    report.executed !== false ||
    report.emittedProducerExecuted !== false ||
    report.preparationCapable !== false ||
    report.productionInvocation !== false ||
    report.completeDependencyClosure !== false ||
    report.loadedCollectorBytesAttested !== false ||
    report.observation !== "partial" ||
    report.entry !== producerEntry
  )
    throw new Error("Producer report is outside the held observation contract.");
  const source = report.source;
  const late = report.lateQualifierSources;
  if (
    !isObject(source) ||
    source.repository !== "Jones-Systems/Jones-Code" ||
    typeof source.sha !== "string" ||
    !/^[a-f0-9]{40}$/.test(source.sha) ||
    typeof source.tree !== "string" ||
    !/^[a-f0-9]{40}$/.test(source.tree) ||
    !Array.isArray(source.buildOnlyOverlays) ||
    new Set(source.buildOnlyOverlays).size !== source.buildOnlyOverlays.length ||
    source.buildOnlyOverlays.some((file) => !releaseManifests.includes(file)) ||
    !isObject(late) ||
    late.basis !== "fixed-late-qualifier-file-inputs" ||
    late.computedImportsResolved !== false ||
    late.completeDependencyClosure !== false
  )
    throw new Error("Producer source identity or late-source contract is invalid.");
  const bindings = new Map();
  const inventory = (rows, expected) => {
    if (!Array.isArray(rows) || rows.length !== expected.length)
      throw new Error("Producer source inventory must match the fixed contract.");
    const remaining = new Set(expected);
    for (const row of rows) {
      if (
        !isObject(row) ||
        !remaining.delete(row.path) ||
        !isObject(row.content) ||
        row.content.availability !== "observed" ||
        row.content.path !== row.path
      )
        throw new Error("Producer source inventory must match the fixed contract.");
      const content = contentBinding(row.content, sourceLimit);
      const previous = bindings.get(row.path);
      if (previous && (previous.sha256 !== content.sha256 || previous.size !== content.size))
        throw new Error("Producer source inventories disagree.");
      bindings.set(row.path, content);
    }
  };
  inventory(source.inputs, producerInputs);
  inventory(late.files, qualifierInputs);
  if (bindings.size !== sourceInputs.length)
    throw new Error("Producer source union differs from the fixed contract.");
  const emitted = report.emitted;
  if (
    !isObject(emitted) ||
    emitted.format !== "esm" ||
    emitted.fileName !== "offlinePreparationProducer.mjs" ||
    !Array.isArray(emitted.files) ||
    emitted.files.length !== 1 ||
    emitted.files[0]?.fileName !== emitted.fileName
  )
    throw new Error("Producer output must be the fixed single emitted file.");
  const producer = contentBinding(emitted, producerLimit);
  const output = contentBinding(emitted.files[0], producerLimit);
  if (producer.sha256 !== output.sha256 || producer.size !== output.size)
    throw new Error("Producer output bindings disagree.");
  return {
    bindings,
    producer,
    document: report,
    reportedGitIdentity: {
      repository: source.repository,
      sha: source.sha,
      tree: source.tree,
      attested: false,
    },
  };
}

function uncertainCustody(cause, seen = new Set()) {
  if (typeof cause !== "object" || cause === null) return false;
  if (seen.has(cause)) return false;
  if (seen.size >= 64) return true;
  seen.add(cause);
  if (
    cause.preserveScratch === true ||
    cause.preserveStageRoot === true ||
    typeof cause.retainedRoot === "string"
  )
    return true;
  const nested = [cause.cause, ...(cause instanceof AggregateError ? cause.errors : [])];
  return nested.some((error) => uncertainCustody(error, seen));
}

// Reviewed 3741dcc producer: import initializes in-memory Effect schemas/caches/prototypes,
// Error stack detection, scheduler closures and the optional literal async_hooks lookup.
// Its only import-time filesystem work resolves this unique captured code root and hashes
// the fixed raw inputs below. Downloads, children, timers and native qualifiers are deferred
// to the fixed build methods. These producer permissions never extend the worker policy.
const reviewedProducer = {
  sha256: "1af15bf7cafcec1899ae09a7ee1a299640e8db17aeedb5a13f372bb98afb9c47",
  size: 851654,
};
const reviewedRawSources = {
  "apps/desktop/src/jones/updates/jonesColdOwnerSource.ts":
    "345dd6d1869f2ce854e5e6655f6cab6d0787c401280b277606898d8952dcdc81",
  "apps/desktop/src/jones/updates/jonesColdPairSource.ts":
    "d89981bb526e6b7c774c8dff57e55b5b90361e5eec5deef16a1c70fa44b8f4a0",
  "apps/desktop/src/jones/updates/jonesColdReceiverSource.ts":
    "32f5b684fb78f691556cd68a498ac980e4dc48ffd073c2a85fa2b7a00925e763",
  "apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts":
    "41b5162af49abb44c16ec483670bb84051fff5411ccbba601b814f786c30360c",
  "apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c":
    "7a1f37ca8853c798a156be5af1e9dd45de1551fc52f11d59eb0d10f903de33e3",
  "apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts":
    "fcc87f71203dcc7d559dc082bba7cc495254871cced4daeffaee9376f5820804",
  "apps/desktop/src/jones/updates/jonesNativeHelperSource.ts":
    "938368a8fbe4da2377a6f90a64f9a3fa57c1b7459d745330d30cac57df1f55fc",
  "apps/desktop/src/jones/updates/jonesPairedStateSource.ts":
    "362644e48e6f3f5fc31d753fbcb4954671ad296d8ac414ac351e40c03c0820a7",
  "apps/desktop/src/jones/updates/jonesPreparationWorkerSource.ts":
    "5c2f2da19235042dd924121f68d33ec5e1b762eb56ae94cc52d9687f203e3fa7",
  "apps/server/src/jones/updates/offlinePreparation.ts":
    "03d4626f3f1760dab4a6b2a5129c87af6ea8289ed651339ca5e795f449509326",
  "apps/server/src/jones/updates/offlinePreparationCore.ts":
    "0ef63d0c2fad4d2e3163c254364d97f8a280551deeb861e46a6a548aa9d53bdb",
  "apps/server/src/jones/updates/offlinePreparationEntry.ts":
    "fa93192f5731f7f3e26527ad6b469051d7a89b1da7ace639afaba59ad9fd3774",
  "apps/server/src/jones/updates/offlinePreparationPlan.ts":
    "3af9931a1136b162ee94d9d8b0f62c2b538824b0cfb917598c7e423b0deb884b",
  "apps/server/src/jones/updates/offlinePreparationWorker.test.ts":
    "ef9e17332a72236db597c1f30110b6092e5aef9127e2b14e5ae571d77d8831f7",
  "apps/server/src/jones/updates/offlinePreparationWorker.ts":
    "4f01c770a4fe2df67387c73c48465769f87d9df4317d65e987b72a9465c548c6",
  "apps/server/vite.config.ts": "5daa69483a678795fe458b1d34b7c3457aac4988be22e83804c65f50645ba944",
  "package.json": "79c403b31234443118e5207a8e8693af2c9e541b24f664d23fa4017d40ff2186",
  "pnpm-lock.yaml": "6017f11e0032db8fbbc84086cd4cc0ae6f312f4ae99384e91d733f7b3c7f43aa",
  "pnpm-workspace.yaml": "3a93aa85c1edb088a14afba8659404a2a321817ce6590589bb055fa0ecf8d103",
  "scripts/jones/updates/functional-policy.ts":
    "9684a2198f0e6772f96734b754d56836a095c245664599a15621199c4433d513",
  "scripts/jones/updates/legacy-bootstrap-bundle.ts":
    "539a2d53107a927047191b9496a3f404d06d74a2abc7962d32f390b52b35e467",
  "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs":
    "a6ae27b479f7802180e7dff7dcc3bcf5ccc1cb5cd5a7dbe6782152f5b7b29bbb",
  "scripts/jones/updates/legacy-identity-native-fixture.mjs":
    "916901550d251c2b81893bb9e273997081b3a99f2d051b7298a2fe76bc611032",
  "scripts/jones/updates/legacy-preparation-native-fixture.d.mts":
    "6d0d5a36bfa5ea815af8b13c8f432a6ec61d2ffb04c0d1579a5d5ca9cb852043",
  "scripts/jones/updates/legacy-preparation-native-fixture.mjs":
    "d43e1527d749df5e19730e145a13d75898c47c0172dff9c2e700e22c8c555f92",
  "scripts/jones/updates/offline-preparation-build.ts":
    "42aa94a85e8e71a188aec142e60069509e7cf1019c5d6ce9abe099e5dc789812",
  "scripts/jones/updates/offline-preparation-producer-entry.ts":
    "779e2bb0d298a04b17debed2de6088fec1aefcb52b57e47e0664e027b5bbd247",
  "scripts/package.json": "021407078841ca3f0fdb4d6347ba8989c826b80127268844ff2fd257647ba504",
  "vite.config.ts": "c86e359d60e69e65f2415af82e3b22917306802dd2beba2bbb333ac2a3ca5992",
};
const reviewedImports = [
  {
    kind: "static",
    specifier: "node:fs",
    computed: false,
    builtin: true,
    expressionSha256: "b6471bb6742ff95f8b90244d773ca1c10cc1db5944b113871b5a68c8cbb7d5f7",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 0,
      end: 34,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:child_process",
    computed: false,
    builtin: true,
    expressionSha256: "8f9b1ef7c3883923fb3b83b8fa8d82d89136e42f21c609af33657c291bfe4160",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 35,
      end: 90,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:crypto",
    computed: false,
    builtin: true,
    expressionSha256: "68ea541bde6e48ae4d48e87096319c19cb79eff795f410e570728487674bfc30",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 91,
      end: 133,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:path",
    computed: false,
    builtin: true,
    expressionSha256: "9bdcd607570c70a7543e1c3e6960556cc1cd881e04f742ba1df616cdd1af45a5",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 134,
      end: 172,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:os",
    computed: false,
    builtin: true,
    expressionSha256: "8f7e813debf01e244983cbf3c211f442317ea2a6a00ef6380bb85bd6ef13a1fa",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 173,
      end: 207,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:timers",
    computed: false,
    builtin: true,
    expressionSha256: "8f4ae7b420f8928b9452e0cc4720c899ffdfff3a2e112edfdd1570414a87fe0a",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 208,
      end: 250,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:http",
    computed: false,
    builtin: true,
    expressionSha256: "e6b3de84783c7483a046ef20eb753a1ab2f09b3e55ee603fc9f433dd24f5e5f9",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 251,
      end: 289,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:url",
    computed: false,
    builtin: true,
    expressionSha256: "945f2c2f0419c74ddaefb5d7910178df8de728174d3b31c98a1b5dedb5f05efb",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 290,
      end: 326,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:https",
    computed: false,
    builtin: true,
    expressionSha256: "e8067f7a1a1da4e9ce4d24897dbc3cd21d39b80e5aeeed387fdfa7974592163e",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 327,
      end: 367,
      endExclusive: true,
    },
  },
  {
    kind: "static",
    specifier: "node:util",
    computed: false,
    builtin: true,
    expressionSha256: "a138b11f80981629c94d05920e4c2e87f7d8c56dc577c4daaf6301e1ab935ee9",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 368,
      end: 406,
      endExclusive: true,
    },
  },
  {
    kind: "process-builtin",
    specifier: "node:async_hooks",
    computed: false,
    builtin: true,
    expressionSha256: "d64bd1adb71bdcc572de3468d66276ee84b3df3e7df69858acd26bde23a60ddb",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 163229,
      end: 163287,
      endExclusive: true,
    },
  },
  {
    kind: "dynamic",
    specifier: null,
    computed: true,
    builtin: false,
    expressionSha256: "416696edd01b5442c66e01cc2ad20fd95530a586d89267d76cb1486471830294",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 834894,
      end: 835022,
      endExclusive: true,
    },
  },
  {
    kind: "dynamic",
    specifier: null,
    computed: true,
    builtin: false,
    expressionSha256: "1983888e443a0d38c12ad7306647753e2c85ebdc83a59a8714571fafa4bfa928",
    location: {
      file: "offlinePreparationProducer.mjs",
      unit: "utf16-code-units",
      origin: 0,
      start: 835224,
      end: 835350,
      endExclusive: true,
    },
  },
];
const reviewedLateModulesSha256 =
  "9c9a575588fb9456f5468b6e48901b90ff4941063f9f2677709e9640176826ea";

function requireProducerExecutionContract(capture) {
  const report = capture.report.document;
  const emitted = report.emitted;
  if (
    !NodeUtil.isDeepStrictEqual(capture.report.producer, reviewedProducer) ||
    !capture.producerBytes
      .toString("utf8")
      .endsWith("export { buildQualifiedPreparation, stageQualifiedPreparation };") ||
    !NodeUtil.isDeepStrictEqual(emitted.syntax?.imports, reviewedImports) ||
    emitted.syntax?.importsComplete !== true ||
    emitted.syntax?.importCount !== 13 ||
    emitted.mapPresent !== false ||
    !NodeUtil.isDeepStrictEqual(emitted.assets, []) ||
    !NodeUtil.isDeepStrictEqual(emitted.generatedRuntime, {
      availability: "unavailable",
      reason: "generated-region-not-observed",
    }) ||
    hash(Buffer.from(JSON.stringify(report.lateQualifierSources.modules))) !==
      reviewedLateModulesSha256
  )
    throw new Error("The preparation producer differs from the reviewed execution contract.");
  for (const [file, sha256] of Object.entries(reviewedRawSources))
    if (capture.report.bindings.get(file)?.sha256 !== sha256)
      throw new Error(`Producer raw source differs from the reviewed execution contract: ${file}`);
  // Exact emitted bytes bind the two exports and both computed imports to captured fixed
  // qualifier paths. The report remains an unqualified toolchain observation, not authority.
}

function verifyExecutionOverlay(file, original, actual, identity) {
  if (!releaseManifests.includes(file) || !isObject(original) || !isObject(actual))
    throw new Error("Unadmitted producer source overlay.");
  const before = { ...original },
    after = { ...actual };
  const version = after.version;
  if (
    typeof version !== "string" ||
    version.length > 128 ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(version)
  )
    throw new Error("Producer source requires a bounded preview version.");
  if (file === "apps/server/package.json" || file === "apps/desktop/package.json") {
    if (
      !NodeUtil.isDeepStrictEqual(after.jonesSource, {
        repository: "Jones-Systems/Jones-Code",
        ...identity,
      }) ||
      after.startupGateProtocol !== 1
    )
      throw new Error("Producer source stamp differs from the observed Git identity.");
    delete before.jonesSource;
    delete after.jonesSource;
    delete before.startupGateProtocol;
    delete after.startupGateProtocol;
  }
  delete before.version;
  delete after.version;
  if (!NodeUtil.isDeepStrictEqual(before, after))
    throw new Error("Producer source has changes beyond release stamping.");
  return version;
}

// A lost close leaves the actual child retained; no PID receipt becomes signalling authority.
const retainedGitChildren = new Set();
async function readGit(root, args, signal) {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.default.spawn(
      "/usr/bin/git",
      [
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-C",
        root,
        ...args,
      ],
      {
        cwd: root,
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
          LC_ALL: "C",
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_TERMINAL_PROMPT: "0",
          GIT_NO_REPLACE_OBJECTS: "1",
          GIT_OPTIONAL_LOCKS: "0",
        },
      },
    );
    retainedGitChildren.add(child);
    let failure,
      failed = false,
      exited = false,
      finished = false,
      bytes = 0,
      killTimer,
      custodyTimer;
    const output = [];
    const finish = (cause, value, uncertain = false, rejected = cause !== undefined) => {
      if (finished) return;
      finished = true;
      clearTimeout(deadline);
      clearTimeout(killTimer);
      clearTimeout(custodyTimer);
      signal?.removeEventListener("abort", cancelled);
      if (!uncertain) retainedGitChildren.delete(child);
      if (rejected) reject(cause);
      else resolve(value);
    };
    const stop = (cause) => {
      if (!failed) {
        failure = cause;
        failed = true;
      }
      if (killTimer !== undefined || finished) return;
      try {
        child.kill("SIGTERM");
      } catch (error) {
        failure ??= error;
      }
      killTimer = setTimeout(() => {
        try {
          child.kill("SIGKILL");
        } catch (error) {
          failure ??= error;
        }
      }, 1000);
      custodyTimer = setTimeout(
        () =>
          finish(
            Object.assign(
              new Error("Producer Git child closure is unproved.", { cause: failure }),
              { preserveScratch: true, childPid: child.pid },
            ),
            undefined,
            true,
          ),
        5000,
      );
    };
    const cancelled = () => stop(signal.reason);
    const deadline = setTimeout(
      () => stop(new Error("Producer Git observation timed out.")),
      20000,
    );
    child.once("error", (cause) => {
      if (!failed) {
        failure = cause;
        failed = true;
      }
    });
    child.once("exit", () => {
      exited = true;
    });
    const receive = (chunk, stdout) => {
      bytes += chunk.length;
      if (bytes > 2 * 1024 * 1024) stop(new Error("Producer Git output exceeds its bound."));
      else if (stdout) output.push(chunk);
    };
    child.stdout.on("data", (chunk) => receive(chunk, true));
    child.stderr.on("data", (chunk) => receive(chunk, false));
    child.stdout.once("error", stop);
    child.stderr.once("error", stop);
    child.once("close", (code) => {
      if (!exited && child.pid !== undefined) {
        finish(
          Object.assign(new Error("Producer Git exit custody is unproved.", { cause: failure }), {
            preserveScratch: true,
            childPid: child.pid,
          }),
          undefined,
          true,
        );
      } else if (failed) finish(failure, undefined, false, true);
      else
        finish(
          code === 0 ? undefined : new Error("Producer Git observation failed."),
          Buffer.concat(output),
        );
    });
    signal?.addEventListener("abort", cancelled, { once: true });
    if (signal?.aborted) cancelled();
  });
}

async function verifyExecutionSource(capture, expected, signal) {
  const git = async (...args) =>
    new TextDecoder("utf-8", { fatal: true }).decode(
      await readGit(capture.sourceRoot, args, signal),
    );
  const root = (await git("rev-parse", "--show-toplevel")).trim();
  const sha = (await git("rev-parse", "HEAD")).trim();
  const tree = (await git("rev-parse", "HEAD^{tree}")).trim();
  if (
    (await NodeFSP.default.realpath(root)) !== capture.sourceRoot ||
    sha !== expected.sourceSha ||
    tree !== expected.sourceTree
  )
    throw new Error("Producer checkout identity differs from the captured request.");
  if ((await git("ls-files", "--others", "--exclude-standard", "-z")) !== "")
    throw new Error("Producer checkout has untracked source.");
  const changed = (
    await git(
      "diff",
      "--no-ext-diff",
      "--no-textconv",
      "--name-only",
      "--no-renames",
      "-z",
      "HEAD",
      "--",
    )
  )
    .split("\0")
    .filter(Boolean)
    .sort();
  if (
    !NodeUtil.isDeepStrictEqual(changed, [...releaseManifests].sort()) ||
    !NodeUtil.isDeepStrictEqual(
      [...capture.report.document.source.buildOnlyOverlays].sort(),
      changed,
    )
  )
    throw new Error("Producer checkout must contain exactly the four recorded release overlays.");
  if (capture.report.document.source.version !== expected.version)
    throw new Error("Producer graph preview version differs from the requested version.");
  for (const file of releaseManifests) {
    const original = JSON.parse(await git("show", `HEAD:${file}`));
    const actual = JSON.parse(capture.manifests.get(file).toString("utf8"));
    if (verifyExecutionOverlay(file, original, actual, { sha, tree }) !== expected.version)
      throw new Error("Producer release manifest versions differ.");
  }
  await capture.verifyCurrent();
}

async function requireHandleRejection(producer, candidate) {
  let inspected = false;
  const forbidden = () => {
    inspected = true;
    throw new Error("Rejected handle reached staging inputs.");
  };
  try {
    await producer.stageQualifiedPreparation({
      handle: candidate,
      get expectedSource() {
        return forbidden();
      },
      get stageResourcesDir() {
        return forbidden();
      },
      get signal() {
        return forbidden();
      },
    });
  } catch (cause) {
    if (
      !inspected &&
      cause instanceof Error &&
      cause.message === "Preparation handle is absent, expired, or consumed."
    )
      return;
    throw Object.assign(new Error("Preparation handle rejection was not proved.", { cause }), {
      preserveScratch: true,
    });
  }
  throw Object.assign(new Error("Preparation rejected-handle call unexpectedly succeeded."), {
    preserveScratch: true,
  });
}

/** Diagnostic capture only; successful return follows complete cleanup without importing producer bytes. */
export async function capturePreparationProducer(options) {
  return withCapturedPreparationProducer(options, async (capture) => capture.observation);
}

/** Own one captured module through build and one-shot staging; no capability or namespace escapes. */
export async function buildAndStagePreparationProducer(options) {
  if (
    !isObject(options) ||
    Object.keys(options).some(
      (key) =>
        !["sourceRoot", "stageRoot", "signal", "expectedSource", "stageResourcesDir"].includes(key),
    )
  )
    throw new Error("Producer staging accepts only fixed source, stage and cancellation inputs.");
  const expected = options.expectedSource;
  if (
    !isObject(expected) ||
    Object.keys(expected).sort().join(",") !== "repository,sourceSha,sourceTree,version" ||
    expected.repository !== "Jones-Systems/Jones-Code" ||
    typeof expected.sourceSha !== "string" ||
    !/^[a-f0-9]{40}$/.test(expected.sourceSha) ||
    typeof expected.sourceTree !== "string" ||
    !/^[a-f0-9]{40}$/.test(expected.sourceTree) ||
    typeof expected.version !== "string" ||
    expected.version.length > 128 ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(expected.version) ||
    typeof options.stageResourcesDir !== "string" ||
    !NodePath.isAbsolute(options.stageResourcesDir)
  )
    throw new Error(
      "Producer staging requires exact source identity and an absolute resource directory.",
    );
  const expectedSource = Object.freeze({ ...expected });
  const stageResourcesDir = options.stageResourcesDir;
  const signal = options.signal;
  return withCapturedPreparationProducer(
    { sourceRoot: options.sourceRoot, stageRoot: options.stageRoot, signal },
    async (capture) => {
      const resources = await NodeFSP.default.realpath(stageResourcesDir);
      if (resources === capture.stageRoot || !contains(capture.stageRoot, resources))
        throw new Error("Preparation resources must be inside the captured packaging stage.");
      if (
        expectedSource.sourceSha !== capture.observation.reportedGitIdentity.sha ||
        expectedSource.sourceTree !== capture.observation.reportedGitIdentity.tree
      )
        throw new Error("Producer capture differs from the requested source identity.");
      await capture.verifyCurrent();
      requireProducerExecutionContract(capture);
      if (
        NodeProcess.platform !== "darwin" ||
        NodeProcess.arch !== "arm64" ||
        process.versions.node !== "24.13.1" ||
        process.execArgv.length !== 0 ||
        process.env.NODE_OPTIONS ||
        process.env.NODE_PATH
      )
        throw new Error(
          "Producer execution requires fresh pinned Node 24.13.1 on Darwin arm64 without preload options.",
        );
      await verifyExecutionSource(capture, expectedSource, signal);

      const controller = new AbortController();
      const cancel = () => controller.abort(signal.reason);
      signal?.addEventListener("abort", cancel, { once: true });
      try {
        signal?.throwIfAborted();
        capture.beginExecution();
        let producer;
        try {
          // The path is unique per capture. Keep this exact namespace for both calls.
          producer = await import(
            NodeURL.pathToFileURL(NodePath.join(capture.codeRoot, capturedProducerFile)).href
          );
        } catch (cause) {
          throw Object.assign(new Error("Producer import effects are uncertain.", { cause }), {
            preserveScratch: true,
          });
        }
        await verifyExecutionSource(capture, expectedSource, signal);
        if (
          Object.keys(producer).sort().join(",") !==
            "buildQualifiedPreparation,stageQualifiedPreparation" ||
          typeof producer.buildQualifiedPreparation !== "function" ||
          typeof producer.stageQualifiedPreparation !== "function"
        )
          throw Object.assign(new Error("Captured producer has an unexpected module interface."), {
            preserveScratch: true,
          });
        const handle = await producer.buildQualifiedPreparation({
          repoRoot: capture.sourceRoot,
          stageRoot: capture.stageRoot,
          signal: controller.signal,
        });
        await verifyExecutionSource(capture, expectedSource, signal);
        // These are derived from the real issued object inside its one module lifetime.
        // A lookalike must be rejected before inspecting any destination or source arguments.
        for (const recreated of [
          Object.assign({}, handle),
          structuredClone(handle),
          JSON.parse(JSON.stringify(handle)),
          Object.create(handle),
        ])
          await requireHandleRejection(producer, recreated);
        await verifyExecutionSource(capture, expectedSource, signal);
        const staged = await producer.stageQualifiedPreparation({
          handle,
          expectedSource,
          stageResourcesDir: resources,
          signal: controller.signal,
        });
        await requireHandleRejection(producer, handle);
        await verifyExecutionSource(capture, expectedSource, signal);
        return { ...staged, handleConsumption: { recreatedRejected: 4, consumedRejected: true } };
      } finally {
        signal?.removeEventListener("abort", cancel);
        // Revokes any issued but unconsumed module-private handle before code cleanup.
        controller.abort(new Error("Captured producer lifetime ended."));
      }
    },
  );
}

// Only the fixed operations above choose a consumer; callers cannot inject code into this lifetime.
async function withCapturedPreparationProducer(options, consume) {
  if (
    !isObject(options) ||
    Object.keys(options).some((key) => !["sourceRoot", "stageRoot", "signal"].includes(key))
  )
    throw new Error("Producer capture accepts only sourceRoot, stageRoot and signal.");
  const { signal } = options;
  if (signal !== undefined && !(signal instanceof AbortSignal))
    throw new Error("Invalid capture signal.");
  const checkAbort = () => signal?.throwIfAborted();
  checkAbort();
  const uid = process.getuid?.();
  if (uid === undefined || NodeFS.constants.O_NOFOLLOW === undefined)
    throw new Error("Owned no-follow producer capture is unavailable on this platform.");
  const ownedDirectory = (stat) =>
    stat.isDirectory() && stat.uid === BigInt(uid) && (stat.mode & 0o022n) === 0n;
  const ownedFile = (stat, limit) =>
    stat.isFile() &&
    stat.uid === BigInt(uid) &&
    stat.nlink === 1n &&
    (stat.mode & 0o022n) === 0n &&
    stat.size >= 0n &&
    stat.size <= BigInt(limit);
  const root = async (value) => {
    if (typeof value !== "string" || !NodePath.isAbsolute(value))
      throw new Error("Capture roots must be absolute directories.");
    const name = await NodeFSP.default.realpath(value);
    const stat = await NodeFSP.default.lstat(name, { bigint: true });
    if (!ownedDirectory(stat)) throw new Error("Capture root is not an owned directory.");
    return { path: name, stat };
  };
  const source = await root(options.sourceRoot);
  const stage = await root(options.stageRoot);
  if (source.path === stage.path)
    throw new Error("Producer source and stage roots must be distinct.");
  const temporary = await NodeFSP.default.realpath(NodeOS.tmpdir());
  const prefix = NodePath.join(temporary, "jones-preparation-producer-");
  if (overlaps(source.path, prefix) || overlaps(stage.path, prefix))
    throw new Error("Producer scratch placement overlaps a source or stage root.");
  checkAbort();

  let scratchRoot;
  let scratchStat;
  let custodyUnknown = false;
  let executionAttempted = false;
  let primary;
  let failed = false;
  let result;
  const handles = [];
  const sourceDirectories = new Map([[source.path, source.stat]]);
  const directories = new Map();
  const captured = [];
  const inputs = [];
  const custodyError = (message, cause) => {
    custodyUnknown = true;
    return Object.assign(new Error(message, { cause }), {
      preserveScratch: true,
      retainedRoot: scratchRoot,
    });
  };
  const checkDirectory = async (name, original, scratch = false) => {
    try {
      const current = await NodeFSP.default.lstat(name, { bigint: true });
      if (!ownedDirectory(current) || !sameIdentity(original, current))
        throw new Error(`Directory identity changed: ${name}`);
    } catch (cause) {
      if (scratch) throw custodyError("Producer scratch directory custody is uncertain.", cause);
      throw cause;
    }
  };
  const checkSourceParents = async (name) => {
    await checkDirectory(source.path, source.stat);
    let parent = source.path;
    for (const part of NodePath.relative(source.path, NodePath.dirname(name))
      .split(NodePath.sep)
      .filter(Boolean)) {
      parent = NodePath.join(parent, part);
      const stat = await NodeFSP.default.lstat(parent, { bigint: true });
      if (!ownedDirectory(stat))
        throw new Error("Producer source parent is not an owned directory.");
      const previous = sourceDirectories.get(parent);
      if (previous && !sameIdentity(previous, stat))
        throw new Error("Producer source parent identity changed.");
      sourceDirectories.set(parent, stat);
    }
  };
  const readBytes = async (handle, size) => {
    const bytes = Buffer.alloc(size);
    let offset = 0;
    while (offset < size) {
      checkAbort();
      const read = await handle.read(bytes, offset, size - offset, offset);
      if (read.bytesRead === 0) throw new Error("Producer input changed during capture.");
      offset += read.bytesRead;
    }
    checkAbort();
    return bytes;
  };
  const checkInput = async (input, reread = false) => {
    await checkSourceParents(input.path);
    const descriptor = await input.handle.stat({ bigint: true });
    const pathname = await NodeFSP.default.lstat(input.path, { bigint: true });
    if (
      !ownedFile(descriptor, input.limit) ||
      !ownedFile(pathname, input.limit) ||
      !sameFile(input.stat, descriptor) ||
      !sameFile(descriptor, pathname)
    )
      throw new Error(`Producer input identity or bytes changed: ${input.relative}`);
    if (reread && hash(await readBytes(input.handle, input.size)) !== input.sha256)
      throw new Error(`Producer input bytes changed: ${input.relative}`);
  };
  const readInput = async (relative, limit, expected) => {
    checkAbort();
    const name = NodePath.join(source.path, relative);
    await checkSourceParents(name);
    const before = await NodeFSP.default.lstat(name, { bigint: true });
    if (!ownedFile(before, limit))
      throw new Error(`Producer input is not a bounded owned regular file: ${relative}`);
    const handle = await NodeFSP.default.open(
      name,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    handles.push(handle);
    const stat = await handle.stat({ bigint: true });
    if (!ownedFile(stat, limit) || !sameFile(before, stat))
      throw new Error(`Producer input replaced before capture: ${relative}`);
    const size = Number(stat.size);
    const input = { relative, path: name, limit, handle, stat, size, sha256: "" };
    const bytes = await readBytes(handle, size);
    input.sha256 = hash(bytes);
    await checkInput(input);
    if (expected && (expected.size !== size || expected.sha256 !== input.sha256))
      throw new Error(`Producer input differs from its report: ${relative}`);
    inputs.push(input);
    return { input, bytes };
  };
  const makeParents = async (name) => {
    let parent = scratchRoot;
    for (const part of NodePath.relative(scratchRoot, NodePath.dirname(name))
      .split(NodePath.sep)
      .filter(Boolean)) {
      await checkDirectory(parent, directories.get(parent), true);
      parent = NodePath.join(parent, part);
      if (directories.has(parent)) continue;
      try {
        await NodeFSP.default.mkdir(parent, { mode: 0o700 });
        const stat = await NodeFSP.default.lstat(parent, { bigint: true });
        if (!ownedDirectory(stat)) throw new Error("Created capture parent is not owned.");
        directories.set(parent, stat);
      } catch (cause) {
        throw custodyError("Producer capture parent creation has uncertain custody.", cause);
      }
    }
    await checkDirectory(parent, directories.get(parent), true);
  };
  const checkCapturedParents = async (name) => {
    let parent = scratchRoot;
    await checkDirectory(parent, scratchStat, true);
    for (const part of NodePath.relative(scratchRoot, NodePath.dirname(name))
      .split(NodePath.sep)
      .filter(Boolean)) {
      parent = NodePath.join(parent, part);
      await checkDirectory(parent, directories.get(parent), true);
    }
  };
  const copy = async ({ input, bytes }, relative) => {
    checkAbort();
    const name = NodePath.join(scratchRoot, relative);
    await makeParents(name);
    let handle;
    try {
      handle = await NodeFSP.default.open(name, "wx+", 0o600);
      handles.push(handle);
      const initial = await handle.stat({ bigint: true });
      if (!ownedFile(initial, input.limit)) throw new Error("Captured file is not owned.");
      const output = { path: name, relative, stat: initial, handle, input };
      captured.push(output);
      await handle.writeFile(bytes);
      await handle.chmod(0o400);
      output.stat = await handle.stat({ bigint: true });
      if (
        !ownedFile(output.stat, input.limit) ||
        !sameIdentity(initial, output.stat) ||
        Number(output.stat.size) !== input.size ||
        hash(await readBytes(handle, input.size)) !== input.sha256
      )
        throw new Error("Captured file differs from its source bytes.");
    } catch (cause) {
      if (signal?.aborted && cause === signal.reason) throw cause;
      throw custodyError("Producer byte copy has uncertain custody.", cause);
    }
    await checkInput(input);
  };
  const checkScratch = async () => {
    try {
      if (!scratchStat) throw custodyError("Producer scratch identity was not captured.");
      for (const [name, stat] of directories) {
        await checkDirectory(name, stat, true);
        const expected = [...directories.keys(), ...captured.map((file) => file.path)]
          .filter((child) => child !== name && NodePath.dirname(child) === name)
          .map((child) => NodePath.basename(child))
          .sort();
        const actual = (await NodeFSP.default.readdir(name)).sort();
        if (JSON.stringify(actual) !== JSON.stringify(expected))
          throw custodyError("Producer scratch contains an unowned entry.");
      }
      for (const output of captured) {
        const stat = await NodeFSP.default.lstat(output.path, { bigint: true });
        if (!ownedFile(stat, output.input.limit) || !sameFile(output.stat, stat))
          throw custodyError("Captured producer file identity changed.");
      }
    } catch (cause) {
      throw custodyError("Producer scratch observation has uncertain custody.", cause);
    }
  };
  const verifyCurrent = async () => {
    checkAbort();
    for (const input of inputs) {
      await checkInput(input, true);
      await checkInput(input);
    }
    await checkDirectory(stage.path, stage.stat);
    await checkScratch();
    for (const output of captured) {
      if (hash(await readBytes(output.handle, output.input.size)) !== output.input.sha256)
        throw custodyError("Captured producer bytes changed during its lifetime.");
    }
    await checkScratch();
    checkAbort();
  };

  // Cleanup custody begins immediately after allocation, before any artifact or source is read.
  try {
    scratchRoot = await NodeFSP.default.mkdtemp(prefix);
    scratchStat = await NodeFSP.default.lstat(scratchRoot, { bigint: true });
    if (
      !ownedDirectory(scratchStat) ||
      overlaps(source.path, scratchRoot) ||
      overlaps(stage.path, scratchRoot)
    )
      throw custodyError("Producer scratch is not an owned disjoint directory.");
    directories.set(scratchRoot, scratchStat);
    const reportInput = await readInput(reportFile, reportLimit);
    const report = validateReport(
      JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(reportInput.bytes)),
    );
    await copy(reportInput, reportFile);
    const producer = await readInput(producerFile, producerLimit, report.producer);
    await copy(producer, capturedProducerFile);
    const manifests = new Map();
    for (const relative of sourceInputs) {
      const observed = await readInput(relative, sourceLimit, report.bindings.get(relative));
      if (releaseManifests.includes(relative)) manifests.set(relative, observed.bytes);
      await copy(observed, relative);
    }
    await verifyCurrent();
    const observation = {
      status: "held",
      reason: "producer-execution-closure-unqualified",
      imported: false,
      qualification: "unqualified",
      completeDependencyClosure: false,
      loadedCollectorBytesAttested: false,
      sourceRoot: source.path,
      sourceIdentity: identity(source.stat),
      reportedGitIdentity: report.reportedGitIdentity,
      codeRoot: scratchRoot,
      codeIdentity: identity(scratchStat),
      stageRoot: stage.path,
      stageIdentity: identity(stage.stat),
      files: captured.map((output) => ({
        sourcePath: output.input.relative,
        capturedPath: output.relative,
        size: output.input.size,
        sha256: output.input.sha256,
        sourceIdentity: identity(output.input.stat),
        capturedIdentity: identity(output.stat),
      })),
    };
    result = await consume({
      observation,
      report,
      producerBytes: producer.bytes,
      manifests,
      sourceRoot: source.path,
      codeRoot: scratchRoot,
      stageRoot: stage.path,
      verifyCurrent,
      beginExecution: () => {
        executionAttempted = true;
      },
    });
  } catch (cause) {
    failed = true;
    primary = cause;
    if (uncertainCustody(cause)) custodyUnknown = true;
  }
  const cleanupFailures = [];
  for (const handle of handles.reverse()) {
    try {
      await handle.close();
    } catch (cause) {
      cleanupFailures.push(custodyError("Producer descriptor close is uncertain.", cause));
    }
  }
  if (scratchRoot && !custodyUnknown) {
    try {
      await checkScratch();
      for (const output of [...captured].reverse()) {
        await checkCapturedParents(output.path);
        const stat = await NodeFSP.default.lstat(output.path, { bigint: true });
        if (!sameFile(output.stat, stat))
          throw custodyError("Captured file changed before cleanup.");
        await NodeFSP.default.unlink(output.path);
      }
      for (const [name, stat] of [...directories].reverse()) {
        if (name !== scratchRoot) await checkCapturedParents(name);
        await checkDirectory(name, stat, true);
        await NodeFSP.default.rmdir(name);
      }
      const remaining = await NodeFSP.default.lstat(scratchRoot).catch((cause) => {
        if (cause.code === "ENOENT") return undefined;
        throw cause;
      });
      if (remaining) throw custodyError("Producer scratch removal was not observed.");
    } catch (cause) {
      cleanupFailures.push(custodyError("Producer scratch cleanup is uncertain.", cause));
    }
  }
  if (!failed && signal?.aborted) {
    failed = true;
    primary = signal.reason;
  }
  const cleanup = {
    disposition: custodyUnknown ? "retained-uncertain" : scratchRoot ? "removed" : "not-created",
    ...(scratchRoot ? { root: scratchRoot } : {}),
  };
  if (failed || cleanupFailures.length) {
    const error = cleanupFailures.length
      ? new AggregateError(
          failed ? [primary, ...cleanupFailures] : cleanupFailures,
          "Producer capture or cleanup failed.",
          failed ? { cause: primary } : undefined,
        )
      : primary;
    // Abort reasons may be primitive or frozen; preserve them as the cause instead of replacing them.
    const diagnostic =
      error instanceof Error && Object.isExtensible(error)
        ? error
        : new Error("Producer capture failed.", { cause: error });
    Object.assign(diagnostic, {
      cleanup,
      ...(custodyUnknown ? { preserveScratch: true, retainedRoot: scratchRoot } : {}),
      ...(executionAttempted ? { preserveStageRoot: true, stageRoot: stage.path } : {}),
    });
    throw diagnostic;
  }
  return { ...result, cleanup };
}
