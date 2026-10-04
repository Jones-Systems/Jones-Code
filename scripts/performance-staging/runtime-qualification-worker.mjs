import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeURL from "node:url";
import * as NodeWorkerThreads from "node:worker_threads";

const sourceRoot = "/home/malcolmjones/Projects/Jones-Code-performance-worktrees-20261002/lease";
const sourceRevision = "da5f4aee0035beec471b38598eaa2857d1e5155c";
const runtimeRoot = "/home/malcolmjones/Projects/Jones-Code-performance-runtime-20261002";
const executable = NodePath.join(runtimeRoot, "bin/node");
const diagnostic = process.env.JONES_RUNTIME_NODE24_DIAGNOSTIC === "1";
const diagnosticExecutable =
  "/home/malcolmjones/.local/lib/nodejs/node-v24.19.0-linux-x64/bin/node";
const sourceFiles = {
  "apps/server/src/orchestration/Layers/OrchestrationEngine.test.ts":
    "4ac4c2c68639c2ba5bdf420b0b12c308d857c48cabab12238afac90f3cb16074",
  "apps/server/src/orchestration/WorktreeOwnershipLease.test.ts":
    "ee83e6b796f73114366e776a7883807ae9eb10c73f677f602f22d3a168fa7402",
  "apps/server/src/persistence/Migrations/007_JonesThreadCreationLookupIndex.test.ts":
    "1725b2bd0f60cdee4c746728d3b798d8d3e181285f50287c5ee5cb09a06674ed",
  "packages/shared/src/nodeSqliteClient.ts":
    "3688cd43a89aba4365ea2f936a68f27409dd9ff22c18a049ee81d26781ea2ff3",
  "apps/server/src/persistence/Layers/Sqlite.ts":
    "bdac47b5fa4ba90e0aef139e15b7cd7d21b583f11b8adee21ef99f5f3f7e6306",
  "apps/server/src/project/RepositoryIdentityResolver.ts":
    "3162c59dd5c0e2db1afd5a41dc20e5f902d2bf34fc439650821c40eefe76bd84",
  "apps/server/src/orchestration/Layers/OrchestrationEngine.ts":
    "8881d4553d3016d804856e4bbccf1339427749a6a1c5c5e2eefaf7df4201ceb6",
  "apps/server/src/persistence/Migrations.ts":
    "dee2431b564320af682b4b84757ba27546c17c311f94005ba5c9814a94a1a4e4",
  "apps/server/src/testUtils/gitConfig.setup.ts":
    "944843403a60be4fd93bddc90053a8d026cebfa5ba327e61e93ff57f516aea98",
  "packages/shared/src/testing/longTempDir.ts":
    "75354c1032b8dc5ca70b196856dfe9ccb19c7901dc1cff77db3ad511cd5562a5",
};
const engineModule = "apps/server/src/orchestration/Layers/OrchestrationEngine.test.ts";
const leaseModule = "apps/server/src/orchestration/WorktreeOwnershipLease.test.ts";
const indexModule =
  "apps/server/src/persistence/Migrations/007_JonesThreadCreationLookupIndex.test.ts";
const cases = [
  [engineModule, "acquires worktree ownership after a foreign commit during path preparation"],
  [leaseModule, "rotates same-owner generations and never grants expiry-only takeover"],
  [leaseModule, "does not recover a retained lease for a recreated thread id"],
  [leaseModule, "does not create a lease without an authoritative creation event"],
  [
    indexModule,
    "adds migration 7 covering partial creation index without changing event or migration history",
  ],
  [
    indexModule,
    "appends migration 7 to a populated fork-6 database without changing existing state",
  ],
  [indexModule, "uses a covering creation lookup as unrelated thread history grows"],
];
const modules = [engineModule, leaseModule, indexModule];
const limit = 48 * 1024;
const phaseJournalLimit = 8 * 1024;
let runnerAttempted = false;

function requireCondition(condition, message) {
  if (!condition) throw new Error(message);
}

function recordPhase(phase) {
  const root = NodeFS.realpathSync(process.cwd());
  requireCondition(
    root === process.cwd() &&
      NodePath.dirname(root) === NodePath.join(runtimeRoot, "evidence") &&
      /^source-qualification-[a-f0-9-]{36}$/.test(NodePath.basename(root)) &&
      process.env.JONES_PERFORMANCE_RUNTIME_REQUEST === NodePath.join(root, "request.json"),
    "unavailable: phase journal is outside the invocation",
  );
  requireCondition(/^[a-z][a-z-]{0,63}$/.test(phase), "invalid phase journal label");
  const path = NodePath.join(
    root,
    NodeWorkerThreads.isMainThread ? "phases-main.jsonl" : "phases-setup.jsonl",
  );
  const line = `${JSON.stringify({ phase, pid: process.pid, threadId: NodeWorkerThreads.threadId })}\n`;
  const size = Buffer.byteLength(line);
  requireCondition(size <= 256, "phase journal record limit");
  const info = NodeFS.lstatSync(path);
  requireCondition(
    info.isFile() && !info.isSymbolicLink() && info.nlink === 1,
    "unavailable: unsupported phase journal",
  );
  const fd = NodeFS.openSync(
    path,
    NodeFS.constants.O_WRONLY | NodeFS.constants.O_APPEND | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const opened = NodeFS.fstatSync(fd);
    requireCondition(
      opened.dev === info.dev &&
        opened.ino === info.ino &&
        opened.nlink === 1 &&
        opened.size + size <= phaseJournalLimit,
      "unavailable: phase journal changed or exceeded its bound",
    );
    requireCondition(NodeFS.writeSync(fd, line) === size, "incomplete phase journal write");
  } finally {
    NodeFS.closeSync(fd);
  }
}

function readBounded(path, maximum = 4 * 1024 * 1024) {
  const info = NodeFS.lstatSync(path);
  requireCondition(
    info.isFile() && !info.isSymbolicLink() && info.size <= maximum,
    `unavailable: unsupported or oversized input ${path}`,
  );
  return NodeFS.readFileSync(path);
}

function sha256(bytes) {
  return NodeCrypto.createHash("sha256").update(bytes).digest("hex");
}

function checkSource() {
  requireCondition(NodeFS.realpathSync(sourceRoot) === sourceRoot, "unavailable: source alias");
  const hashes = Object.fromEntries(
    Object.entries(sourceFiles).map(([relative, expected]) => {
      const actual = sha256(readBounded(NodePath.join(sourceRoot, relative)));
      requireCondition(actual === expected, `unavailable: source bytes changed: ${relative}`);
      return [relative, actual];
    }),
  );
  return {
    repository: "Jones-Systems/Jones-Code",
    worktreePath: sourceRoot,
    boundRevision: sourceRevision,
    verifiedFileSha256: hashes,
  };
}

function readRequest() {
  const root = NodeFS.realpathSync(process.cwd());
  const parent = NodePath.join(runtimeRoot, "evidence");
  requireCondition(
    NodePath.dirname(root) === parent &&
      /^source-qualification-[a-f0-9-]{36}$/.test(NodePath.basename(root)),
    "unavailable: a fresh qualification root is required",
  );
  const path = process.env.JONES_PERFORMANCE_RUNTIME_REQUEST;
  requireCondition(path === NodePath.join(root, "request.json"), "unavailable: request location");
  const input = JSON.parse(readBounded(path, 16 * 1024).toString("utf8"));
  if (diagnostic) {
    requireCondition(
      input.schema === "jones-performance-node24-diagnostic-request/v1" &&
        input.ownedRootPath === root &&
        input.sourceRevision === sourceRevision &&
        input.executablePath === diagnosticExecutable &&
        input.nodeVersion === "24.19.0",
      "unavailable: Node 24 diagnostic request binding",
    );
  } else {
    requireCondition(
      input.schema === "jones-performance-source-runtime-request/v1" &&
        input.ownedRootPath === root &&
        input.sourceRevision === sourceRevision &&
        input.executablePath === executable &&
        input.nodeVersion === "26.8.2",
      "unavailable: request binding",
    );
  }
  for (const [key, relative] of Object.entries({
    HOME: "home",
    TMPDIR: "tmp",
    TMP: "tmp",
    TEMP: "tmp",
    XDG_CACHE_HOME: "cache",
    XDG_CONFIG_HOME: "home/config",
    XDG_DATA_HOME: "home/data",
  })) {
    requireCondition(
      process.env[key] === NodePath.join(root, relative),
      `unavailable: ${key} is outside the invocation`,
    );
  }
  if (diagnostic) {
    requireCondition(
      process.execPath === diagnosticExecutable && process.versions.node === "24.19.0",
      "unavailable: actual worker differs from the Node 24 diagnostic validation target",
    );
  } else {
    requireCondition(
      process.execPath === executable && process.versions.node === "26.8.2",
      "unavailable: actual worker is not the bound Node 26.8.2 executable",
    );
  }
  return input;
}

function workerIdentity() {
  return {
    execPath: process.execPath,
    nodeVersion: process.versions.node,
    pid: process.pid,
    threadId: NodeWorkerThreads.threadId,
    cwd: process.cwd(),
  };
}

async function installObservationSetup(input) {
  recordPhase("setup-source-check-before");
  checkSource();
  recordPhase("setup-source-check-after");
  const require = NodeModule.createRequire(NodePath.join(sourceRoot, "apps/server/package.json"));
  const apiPath = NodePath.join(sourceRoot, "node_modules/vite-plus/dist/test/index.js");
  recordPhase("setup-api-import-before");
  const { vi, expect } = await import(NodeURL.pathToFileURL(apiPath).href);
  recordPhase("setup-api-import-after");
  recordPhase("setup-effect-imports-before");
  const [Effect, Layer, SqlClient] = await Promise.all(
    ["effect/Effect", "effect/Layer", "effect/unstable/sql/SqlClient"].map((specifier) =>
      vi.importActual(require.resolve(specifier)),
    ),
  );
  recordPhase("setup-effect-imports-after");
  const identities = new WeakMap();
  const observationPath = NodePath.join(input.ownedRootPath, "observations.jsonl");

  // Observe only layer acquisition. A query in the ownership transaction would change the lock test.
  const observe = (original, phase, filename) =>
    Layer.provideMerge(
      Layer.effectDiscard(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          let connectionId = identities.get(sql);
          if (!connectionId) {
            connectionId = NodeCrypto.randomUUID();
            identities.set(sql, connectionId);
          }
          yield* Effect.sync(() =>
            recordPhase(
              phase === "adapter-acquired"
                ? "adapter-fingerprint-before"
                : "persistence-fingerprint-before",
            ),
          );
          const engine = yield* sql.unsafe(
            "SELECT sqlite_version() AS version, sqlite_source_id() AS source_id",
          );
          const pragmas = {};
          for (const name of [
            "journal_mode",
            "synchronous",
            "foreign_keys",
            "busy_timeout",
            "journal_size_limit",
            "wal_autocheckpoint",
          ]) {
            pragmas[name] = yield* sql.unsafe(`PRAGMA ${name}`);
          }
          yield* Effect.sync(() => {
            recordPhase(
              phase === "adapter-acquired"
                ? "adapter-fingerprint-after"
                : "persistence-fingerprint-after",
            );
            const state = expect.getState();
            const module = NodePath.relative(sourceRoot, state.testPath ?? "");
            requireCondition(modules.includes(module), "unavailable: adapter test-module identity");
            requireCondition(
              typeof engine[0]?.version === "string" && typeof engine[0]?.source_id === "string",
              "unavailable: adapter fingerprint",
            );
            const entry = {
              ...workerIdentity(),
              module,
              caseName: state.currentTestName ?? null,
              connectionId,
              phase,
              filename,
              engine: engine[0],
              pragmas,
            };
            const line = `${JSON.stringify(entry)}\n`;
            const size = NodeFS.existsSync(observationPath)
              ? NodeFS.statSync(observationPath).size
              : 0;
            requireCondition(size + Buffer.byteLength(line) <= limit, "observation output limit");
            NodeFS.appendFileSync(observationPath, line, { mode: 0o600 });
          });
        }),
      ),
      original,
    );

  const adapterPath = NodePath.join(sourceRoot, "packages/shared/src/nodeSqliteClient.ts");
  vi.doMock(adapterPath, async () => {
    recordPhase("adapter-module-import-before");
    const actual = await vi.importActual(adapterPath);
    recordPhase("adapter-module-import-after");
    return {
      ...actual,
      layer: (config) => {
        requireCondition(
          config.filename === ":memory:" ||
            (NodePath.isAbsolute(config.filename) &&
              config.filename.startsWith(`${input.ownedRootPath}${NodePath.sep}`)),
          "unavailable: adapter database is outside the fresh invocation",
        );
        recordPhase("adapter-layer-requested");
        const original = actual.layer(config);
        recordPhase("adapter-layer-built");
        return observe(original, "adapter-acquired", config.filename);
      },
    };
  });
  const persistencePath = NodePath.join(sourceRoot, "apps/server/src/persistence/Layers/Sqlite.ts");
  vi.doMock(persistencePath, async () => {
    recordPhase("persistence-module-import-before");
    const actual = await vi.importActual(persistencePath);
    recordPhase("persistence-module-import-after");
    return {
      ...actual,
      makeSqlitePersistenceLive: (path) =>
        observe(actual.makeSqlitePersistenceLive(path), "persistence-ready", path),
      SqlitePersistenceMemory: observe(
        actual.SqlitePersistenceMemory,
        "persistence-ready",
        ":memory:",
      ),
    };
  });
  const resolverPath = NodePath.join(
    sourceRoot,
    "apps/server/src/project/RepositoryIdentityResolver.ts",
  );
  vi.doMock(resolverPath, async () => {
    recordPhase("resolver-module-import-before");
    const actual = await vi.importActual(resolverPath);
    recordPhase("resolver-module-import-after");
    return {
      ...actual,
      layer: Layer.succeed(actual.RepositoryIdentityResolver, {
        resolve: () => Effect.succeed(null),
      }),
    };
  });
  recordPhase("setup-delegations-registered");
}

async function runQualification(input) {
  recordPhase("source-check-before");
  const source = checkSource();
  recordPhase("source-check-after");
  const apiPath = NodePath.join(sourceRoot, "node_modules/vite-plus/dist/test/node.js");
  requireCondition(
    sha256(readBounded(apiPath)) ===
      "e0bd06983d1e9152044c31cff3470176fa475b54c8f7f19d629b46031f586dd0",
    "unavailable: runner entry changed",
  );
  const require = NodeModule.createRequire(NodePath.join(sourceRoot, "apps/server/package.json"));
  const vitestPackagePath = require.resolve("vitest/package.json", {
    paths: [NodePath.dirname(NodeFS.realpathSync(apiPath))],
  });
  const vitestRoot = NodePath.dirname(vitestPackagePath);
  const packageBytes = readBounded(vitestPackagePath);
  requireCondition(
    sha256(packageBytes) === "a28126d97bcaf567da5bed69443b7f3bcd9a7a8c38c8b66e554686b6bb2c10e0" &&
      JSON.parse(packageBytes).version === "4.1.11",
    "unavailable: installed runner package changed",
  );
  const implementation = NodePath.join(vitestRoot, "dist/chunks/cli-api.CnMVyzaz.js");
  requireCondition(
    sha256(readBounded(implementation)) ===
      "a236001d048380e2c67d05423fc9ea3f26b07ee019ba8d6e622082f29d49102e",
    "unavailable: inspected runner implementation changed",
  );
  const viteEntry = require.resolve("vite", { paths: [vitestRoot] });
  const viteImplementation = NodePath.join(NodePath.dirname(viteEntry), "chunks/node.js");
  requireCondition(
    sha256(readBounded(viteImplementation)) ===
      "c36767bdbb4e0ca1d51c6954e6892ef9afc4ea2ecba111f9c8466f5d3126f5fd",
    "unavailable: inspected Vite environment/listener implementation changed",
  );
  recordPhase("runner-pins-checked");
  recordPhase("runner-import-before");
  const { startVitest } = await import(NodeURL.pathToFileURL(apiPath).href);
  recordPhase("runner-import-after");
  requireCondition(typeof startVitest === "function", "unavailable: supported source runner API");
  const results = [];
  const logs = [];
  let logBytes = 0;
  let logsLimited = false;
  let closeErrors = false;
  let runner;
  let runReason;
  let filteredCases = 0;
  let unhandledErrors = [];
  let cancellation;
  let cancelled = false;
  let cancellationFailure;
  let primaryError;
  let runnerClosed = false;
  const text = (value) => String(value?.stack ?? value?.message ?? value).slice(0, 2048);
  const recordLog = (stream, content) => {
    const bytes = Buffer.byteLength(content);
    logBytes += bytes;
    if (stream === "stderr" || content.includes("error during close")) closeErrors = true;
    if (logBytes <= 16 * 1024) logs.push({ stream, content });
    else logsLimited = true;
  };
  const stream = (name) =>
    new NodeStream.Writable({
      write(chunk, _encoding, done) {
        recordLog(name, chunk.toString("utf8"));
        done();
      },
    });
  const cancel = () => {
    cancelled = true;
    cancellation ??= runner?.cancelCurrentRun("keyboard-input").catch((error) => {
      cancellationFailure = text(error);
    });
  };
  process.once("SIGTERM", cancel);
  process.once("SIGINT", cancel);
  const reporter = {
    onInit(context) {
      runner = context;
      recordPhase("runner-initialized");
      if (cancelled) cancel();
    },
    onUserConsoleLog(log) {
      recordLog(log.type === "stderr" ? "stderr" : "console", log.content);
    },
    onTestCaseReady(test) {
      if (
        cases.some(
          ([file, name]) =>
            file === NodePath.relative(sourceRoot, test.module.moduleId) && name === test.name,
        )
      ) {
        recordPhase("selected-case-ready");
      }
    },
    onTestCaseResult(test) {
      const module = NodePath.relative(sourceRoot, test.module.moduleId);
      const result = test.result();
      if (cases.some(([file, name]) => file === module && name === test.name)) {
        recordPhase("selected-case-result");
        results.push({
          module,
          name: test.name,
          fullName: test.fullName,
          state: result.state,
          errors: (result.errors ?? []).map(text),
        });
      } else if (result.state !== "skipped") {
        primaryError ??= new Error(`unexpected executed case: ${module}: ${test.name}`);
      }
    },
    onTestRunEnd(testModules, errors, reason) {
      recordPhase("test-run-ended");
      runReason = reason;
      unhandledErrors = errors.map(text);
      for (const module of testModules) {
        for (const test of module.children.allTests()) {
          if (
            !cases.some(
              ([file, name]) =>
                file === NodePath.relative(sourceRoot, module.moduleId) && name === test.name,
            )
          )
            filteredCases += 1;
        }
      }
    },
  };
  // The installed middleware/disabled-WebSocket route creates no listener and reads no source .env.
  const settings = {
    root: sourceRoot,
    config: false,
    run: true,
    watch: false,
    api: false,
    ui: false,
    browser: { enabled: false },
    environment: "node",
    pool: "threads",
    maxWorkers: 1,
    fileParallelism: false,
    maxConcurrency: 1,
    isolate: true,
    allowOnly: false,
    passWithNoTests: false,
    coverage: { enabled: false },
    include: modules,
    testNamePattern: new RegExp(
      `(?:^| )(?:${cases
        .map(([, name]) => name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join("|")})$`,
    ),
    setupFiles: [
      NodePath.join(sourceRoot, "packages/shared/src/testing/longTempDir.ts"),
      NodePath.join(sourceRoot, "apps/server/src/testUtils/gitConfig.setup.ts"),
      NodeURL.fileURLToPath(import.meta.url),
    ],
    sequence: { concurrent: false, setupFiles: "list" },
    reporters: [reporter],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    cache: { dir: NodePath.join(input.ownedRootPath, "cache/vitest") },
  };
  try {
    recordPhase("runner-start-before");
    runnerAttempted = true;
    runner = await startVitest(
      "test",
      modules,
      settings,
      {
        cacheDir: NodePath.join(input.ownedRootPath, "cache/vite"),
        envDir: false,
        publicDir: false,
        server: { middlewareMode: true, ws: false, hmr: false, watch: null },
      },
      { stdout: stream("stdout"), stderr: stream("stderr") },
    );
    recordPhase("runner-start-returned");
  } catch (error) {
    primaryError ??= error;
  } finally {
    try {
      await cancellation;
      if (runner) {
        try {
          recordPhase("runner-close-before");
        } catch (error) {
          primaryError ??= error;
        }
        await runner.close();
        try {
          recordPhase("runner-close-after");
        } catch (error) {
          primaryError ??= error;
        }
      }
      runnerClosed = !!runner && !closeErrors && !logsLimited && !cancellationFailure;
    } catch (error) {
      primaryError ??= error;
    }
    process.removeListener("SIGTERM", cancel);
    process.removeListener("SIGINT", cancel);
  }
  let fingerprints = [];
  try {
    checkSource();
    fingerprints = readBounded(NodePath.join(input.ownedRootPath, "observations.jsonl"), limit)
      .toString("utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    requireCondition(
      results.length === cases.length &&
        cases.every(
          ([module, name]) =>
            results.filter(
              (result) =>
                result.module === module && result.name === name && result.state === "passed",
            ).length === 1,
        ),
      "missing, failed or skipped selected obligation",
    );
    requireCondition(
      modules.every((module) =>
        fingerprints.some((entry) => entry.module === module && entry.phase === "adapter-acquired"),
      ),
      "missing real adapter connection fingerprint",
    );
    requireCondition(
      fingerprints.some(
        (entry) =>
          entry.module === engineModule &&
          entry.phase === "persistence-ready" &&
          entry.filename !== ":memory:" &&
          fingerprints.some(
            (adapter) =>
              adapter.module === engineModule &&
              adapter.phase === "adapter-acquired" &&
              adapter.connectionId === entry.connectionId,
          ),
      ),
      "missing post-setup Engine connection fingerprint",
    );
    requireCondition(
      fingerprints.every(
        (entry) =>
          (diagnostic
            ? entry.execPath === diagnosticExecutable && entry.nodeVersion === "24.19.0"
            : entry.execPath === executable && entry.nodeVersion === "26.8.2") &&
          entry.pid === process.pid &&
          entry.threadId > 0,
      ),
      "adapter worker executable/version identity differs",
    );
    requireCondition(
      runReason === "passed" &&
        !unhandledErrors.length &&
        !cancelled &&
        !cancellationFailure &&
        !logsLimited &&
        !closeErrors &&
        runnerClosed,
      "source runner did not complete cleanly",
    );
  } catch (error) {
    primaryError ??= error;
  }
  return {
    schema: diagnostic
      ? "jones-performance-node24-diagnostic-runtime/v1"
      : "jones-performance-source-runtime/v1",
    surface: diagnostic ? "node-source-diagnostic" : "node-source-tests",
    ...(diagnostic ? { node26Qualification: "unverified" } : {}),
    outcome: primaryError ? "failed" : "passed",
    source,
    runtime: workerIdentity(),
    runner: {
      entry: apiPath,
      entrySha256: sha256(readBounded(apiPath)),
      version: "4.1.11",
      implementationSha256: sha256(readBounded(implementation)),
      viteImplementationSha256: sha256(readBounded(viteImplementation)),
      pool: "threads",
      maxWorkers: 1,
    },
    fixture: {
      nullRepositoryIdentity: true,
      privateCwd: input.ownedRootPath,
      environmentRoutingIsSandbox: false,
      rawForeignWriterFingerprintObserved: false,
    },
    selectedCases: cases.map(([module, name]) => ({ module, name })),
    cases: results,
    filteredCases,
    runReason,
    fingerprints,
    logs,
    logBytes,
    logsLimited,
    unhandledErrors,
    runnerAttempted,
    runnerClosed,
    closeErrors,
    cancelled,
    cancellationFailure: cancellationFailure ?? null,
    error: primaryError ? text(primaryError) : null,
    compiledQualification: "unknown/held",
    productionSQLiteQualification: "unverified",
  };
}

recordPhase("module-entry");
if (!NodeWorkerThreads.isMainThread) {
  const input = readRequest();
  recordPhase("request-validated");
  await installObservationSetup(input);
} else {
  let report;
  try {
    const input = readRequest();
    recordPhase("request-validated");
    report = await runQualification(input);
  } catch (error) {
    report = {
      schema: diagnostic
        ? "jones-performance-node24-diagnostic-runtime/v1"
        : "jones-performance-source-runtime/v1",
      surface: diagnostic ? "node-source-diagnostic" : "node-source-tests",
      ...(diagnostic ? { node26Qualification: "unverified" } : {}),
      outcome: runnerAttempted ? "unknown" : "unavailable",
      runtime: workerIdentity(),
      runnerAttempted,
      runnerClosed: !runnerAttempted,
      error: String(error?.stack ?? error).slice(0, 4096),
      compiledQualification: "unknown/held",
      productionSQLiteQualification: "unverified",
    };
  }
  try {
    recordPhase("report-before");
  } catch (error) {
    report.outcome = "failed";
    report.error ??= String(error?.stack ?? error).slice(0, 4096);
  }
  const bytes = `${JSON.stringify(report)}\n`;
  if (Buffer.byteLength(bytes) > limit) {
    process.stdout.write(
      `${JSON.stringify({
        schema: report.schema,
        outcome: "unknown",
        error: "complete qualification report exceeds output budget",
        runnerClosed: false,
      })}\n`,
    );
    process.exitCode = 1;
  } else {
    process.stdout.write(bytes);
    process.exitCode = report.outcome === "passed" ? 0 : 1;
  }
}
