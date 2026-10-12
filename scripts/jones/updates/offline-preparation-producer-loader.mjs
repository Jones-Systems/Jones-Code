import fs from "node:fs/promises";
import { constants } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import os from "node:os";

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
    "jonesColdOwnerSource.ts",
    "jonesPreparationWorkerSource.ts",
  ].map((name) => `apps/desktop/src/jones/updates/${name}`),
  "apps/server/src/jones/updates/offlinePreparationWorker.test.ts",
];
const sourceInputs = [...new Set([...producerInputs, ...qualifierInputs])].sort();
const sourceLimit = 8 * 1024 * 1024;
const producerLimit = 32 * 1024 * 1024;
const reportLimit = 16 * 1024 * 1024;
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const contains = (parent, child) => {
  const relative = path.relative(parent, child);
  return (
    relative === "" ||
    (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
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
    source.buildOnlyOverlays.length !== 0 ||
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
  if (bindings.size !== 32)
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
    reportedGitIdentity: {
      repository: source.repository,
      sha: source.sha,
      tree: source.tree,
      attested: false,
    },
  };
}

/** Captures diagnostic bytes only. No report, copied module or caller option can authorize execution. */
export async function capturePreparationProducer(options) {
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
  if (uid === undefined || constants.O_NOFOLLOW === undefined)
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
    if (typeof value !== "string" || !path.isAbsolute(value))
      throw new Error("Capture roots must be absolute directories.");
    const name = await fs.realpath(value);
    const stat = await fs.lstat(name, { bigint: true });
    if (!ownedDirectory(stat)) throw new Error("Capture root is not an owned directory.");
    return { path: name, stat };
  };
  const source = await root(options.sourceRoot);
  const stage = await root(options.stageRoot);
  const temporary = await fs.realpath(os.tmpdir());
  const prefix = path.join(temporary, "jones-preparation-producer-");
  if (overlaps(source.path, prefix) || overlaps(stage.path, prefix))
    throw new Error("Producer scratch placement overlaps a source or stage root.");
  checkAbort();

  let scratchRoot;
  let scratchStat;
  let custodyUnknown = false;
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
      const current = await fs.lstat(name, { bigint: true });
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
    for (const part of path
      .relative(source.path, path.dirname(name))
      .split(path.sep)
      .filter(Boolean)) {
      parent = path.join(parent, part);
      const stat = await fs.lstat(parent, { bigint: true });
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
    const pathname = await fs.lstat(input.path, { bigint: true });
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
    const name = path.join(source.path, relative);
    await checkSourceParents(name);
    const before = await fs.lstat(name, { bigint: true });
    if (!ownedFile(before, limit))
      throw new Error(`Producer input is not a bounded owned regular file: ${relative}`);
    const handle = await fs.open(
      name,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
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
    for (const part of path
      .relative(scratchRoot, path.dirname(name))
      .split(path.sep)
      .filter(Boolean)) {
      await checkDirectory(parent, directories.get(parent), true);
      parent = path.join(parent, part);
      if (directories.has(parent)) continue;
      try {
        await fs.mkdir(parent, { mode: 0o700 });
        const stat = await fs.lstat(parent, { bigint: true });
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
    for (const part of path
      .relative(scratchRoot, path.dirname(name))
      .split(path.sep)
      .filter(Boolean)) {
      parent = path.join(parent, part);
      await checkDirectory(parent, directories.get(parent), true);
    }
  };
  const copy = async ({ input, bytes }, relative) => {
    checkAbort();
    const name = path.join(scratchRoot, relative);
    await makeParents(name);
    let handle;
    try {
      handle = await fs.open(name, "wx+", 0o600);
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
          .filter((child) => child !== name && path.dirname(child) === name)
          .map((child) => path.basename(child))
          .sort();
        const actual = (await fs.readdir(name)).sort();
        if (JSON.stringify(actual) !== JSON.stringify(expected))
          throw custodyError("Producer scratch contains an unowned entry.");
      }
      for (const output of captured) {
        const stat = await fs.lstat(output.path, { bigint: true });
        if (!ownedFile(stat, output.input.limit) || !sameFile(output.stat, stat))
          throw custodyError("Captured producer file identity changed.");
      }
    } catch (cause) {
      throw custodyError("Producer scratch observation has uncertain custody.", cause);
    }
  };

  // Cleanup custody begins immediately after allocation, before any artifact or source is read.
  try {
    scratchRoot = await fs.mkdtemp(prefix);
    scratchStat = await fs.lstat(scratchRoot, { bigint: true });
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
    for (const relative of sourceInputs) {
      const observed = await readInput(relative, sourceLimit, report.bindings.get(relative));
      await copy(observed, relative);
    }
    for (const input of inputs) {
      await checkInput(input, true);
      await checkInput(input);
    }
    await checkDirectory(stage.path, stage.stat);
    await checkScratch();
    for (const output of captured) {
      if (hash(await readBytes(output.handle, output.input.size)) !== output.input.sha256)
        throw custodyError("Captured producer bytes changed before completion.");
    }
    await checkScratch();
    checkAbort();
    result = {
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
  } catch (cause) {
    failed = true;
    primary = cause;
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
        const stat = await fs.lstat(output.path, { bigint: true });
        if (!sameFile(output.stat, stat))
          throw custodyError("Captured file changed before cleanup.");
        await fs.unlink(output.path);
      }
      for (const [name, stat] of [...directories].reverse()) {
        if (name !== scratchRoot) await checkCapturedParents(name);
        await checkDirectory(name, stat, true);
        await fs.rmdir(name);
      }
      const remaining = await fs.lstat(scratchRoot).catch((cause) => {
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
    });
    throw diagnostic;
  }
  return { ...result, cleanup };
}
