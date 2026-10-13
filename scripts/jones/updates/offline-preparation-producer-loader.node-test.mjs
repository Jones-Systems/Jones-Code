import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  buildAndStagePreparationProducer,
  capturePreparationProducer,
} from "./offline-preparation-producer-loader.mjs";

const artifactDirectory = "apps/server/dist-exe/preparation-producer-candidate";
const producerPath = `${artifactDirectory}/offlinePreparationProducer.mjs`;
const reportPath = `${artifactDirectory}/producer-candidate-graph.json`;
const emittedPath = "scripts/jones/updates/offlinePreparationProducer.mjs";
const sharedPaths = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
];
const producerPaths = [
  ...sharedPaths,
  "scripts/package.json",
  "scripts/jones/updates/offline-preparation-producer-entry.ts",
  "scripts/jones/updates/functional-policy.ts",
  "scripts/jones/updates/legacy-bootstrap-bundle.ts",
];
const qualifierPaths = [
  ...sharedPaths,
  ...[
    "offlinePreparationEntry.ts",
    "offlinePreparation.ts",
    "offlinePreparationCore.ts",
    "offlinePreparationPlan.ts",
    "offlinePreparationWorker.ts",
    "offlinePreparationWorker.test.ts",
  ].map((name) => `apps/server/src/jones/updates/${name}`),
  ...[
    "legacy-preparation-native-fixture.mjs",
    "legacy-preparation-native-fixture.d.mts",
    "legacy-cold-maintenance-fixture.mjs",
    "legacy-identity-native-fixture.mjs",
  ].map((name) => `scripts/jones/updates/${name}`),
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
];
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const content = (bytes) => ({ sha256: sha256(bytes), size: bytes.length });
const sameIdentity = (left, right) =>
  left.dev === right.dev && left.ino === right.ino && left.uid === right.uid;

function fixtureCustodyUncertain(cause, seen = new Set()) {
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
  return nested.some((error) => fixtureCustodyUncertain(error, seen));
}

async function withFixture(t, run) {
  const parent = await NodeFSP.default.realpath(NodeOS.tmpdir());
  const fixtureRoot = await NodeFSP.default.mkdtemp(
    NodePath.join(parent, "jones-producer-capture-test-"),
  );
  const allocated = new Map([[fixtureRoot, undefined]]);
  let primary;
  let failed = false;
  try {
    allocated.set(fixtureRoot, await NodeFSP.default.lstat(fixtureRoot, { bigint: true }));
    const originalMkdtemp = NodeFSP.default.mkdtemp;
    t.mock.method(NodeFSP.default, "mkdtemp", async (...args) => {
      const name = await originalMkdtemp(...args);
      allocated.set(name, undefined);
      allocated.set(name, await NodeFSP.default.lstat(name, { bigint: true }));
      return name;
    });
    const sourceRoot = NodePath.join(fixtureRoot, "source");
    const stageRoot = NodePath.join(fixtureRoot, "stage");
    await NodeFSP.default.mkdir(sourceRoot, { mode: 0o700 });
    await NodeFSP.default.mkdir(stageRoot, { mode: 0o700 });
    const bytes = new Map(
      [...new Set([...producerPaths, ...qualifierPaths])].map((name, index) => [
        name,
        Buffer.from(`// synthetic source ${index}: π\r\n`),
      ]),
    );
    bytes.set(producerPath, Buffer.from('throw new Error("producer sentinel executed");\n'));
    const row = (name) => ({
      path: name,
      content: { availability: "observed", path: name, ...content(bytes.get(name)) },
    });
    const producer = content(bytes.get(producerPath));
    const report = {
      schemaVersion: 1,
      classification: "preparation-producer-candidate-graph",
      qualification: "unqualified",
      executed: false,
      emittedProducerExecuted: false,
      preparationCapable: false,
      productionInvocation: false,
      completeDependencyClosure: false,
      loadedCollectorBytesAttested: false,
      observation: "partial",
      entry: "scripts/jones/updates/offline-preparation-producer-entry.ts",
      source: {
        repository: "Jones-Systems/Jones-Code",
        sha: "a".repeat(40),
        tree: "b".repeat(40),
        buildOnlyOverlays: [],
        inputs: producerPaths.map(row),
      },
      lateQualifierSources: {
        basis: "fixed-late-qualifier-file-inputs",
        computedImportsResolved: false,
        completeDependencyClosure: false,
        files: qualifierPaths.map(row),
      },
      emitted: {
        format: "esm",
        fileName: "offlinePreparationProducer.mjs",
        ...producer,
        files: [{ fileName: "offlinePreparationProducer.mjs", ...producer }],
      },
    };
    const writeReport = async () => {
      bytes.set(reportPath, Buffer.from(JSON.stringify(report)));
      await NodeFSP.default.writeFile(
        NodePath.join(sourceRoot, reportPath),
        bytes.get(reportPath),
        { mode: 0o600 },
      );
    };
    for (const [name, data] of bytes) {
      await NodeFSP.default.mkdir(NodePath.dirname(NodePath.join(sourceRoot, name)), {
        recursive: true,
        mode: 0o700,
      });
      await NodeFSP.default.writeFile(NodePath.join(sourceRoot, name), data, {
        flag: "wx",
        mode: 0o600,
      });
    }
    await writeReport();
    await run({ sourceRoot, stageRoot, fixtureRoot, bytes, report, writeReport, allocated });
  } catch (cause) {
    failed = true;
    primary = cause;
  } finally {
    t.mock.restoreAll();
    // Escaped custody uncertainty covers this fixture's source and captured roots.
    // Teardown has no descriptor-closure evidence that could override the creator.
    if (failed && fixtureCustodyUncertain(primary))
      throw Object.assign(new Error("Capture fixture custody remains unresolved.", { cause: primary }), {
        preserveScratch: true,
        retainedRoots: [...allocated.keys()],
      });
    const failures = [];
    for (const [name, original] of [...allocated].reverse()) {
      try {
        const current = await NodeFSP.default.lstat(name, { bigint: true }).catch((cause) => {
          if (cause.code === "ENOENT") return undefined;
          throw cause;
        });
        if (!current) continue;
        if (!original || !current.isDirectory() || !sameIdentity(original, current))
          throw Object.assign(new Error(`Test scratch custody changed: ${name}`), {
            retainedRoot: name,
            preserveScratch: true,
          });
        await NodeFSP.default.rm(name, { recursive: true });
      } catch (cause) {
        failures.push(cause);
      }
    }
    if (failures.length)
      throw new AggregateError(
        failed ? [primary, ...failures] : failures,
        "Capture test cleanup failed.",
        { cause: primary },
      );
    if (failed) throw primary;
  }
}

async function stagingInput({ sourceRoot, stageRoot, report }) {
  const stageResourcesDir = NodePath.join(stageRoot, "resources");
  await NodeFSP.default.mkdir(stageResourcesDir, { mode: 0o700 });
  return {
    sourceRoot,
    stageRoot,
    stageResourcesDir,
    expectedSource: {
      repository: report.source.repository,
      sourceSha: report.source.sha,
      sourceTree: report.source.tree,
      version: "0.0.45-preview.20261012.1.1",
    },
  };
}

NodeTest.test(
  "build-and-stage retains the complete capture during its consumer and refuses before import",
  (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      const realpath = NodeFSP.default.realpath;
      let capturedRoot;
      t.mock.method(NodeFSP.default, "realpath", async (name, ...args) => {
        if (name === input.stageResourcesDir) {
          capturedRoot = [...fixture.allocated.keys()].find((root) => root !== fixture.fixtureRoot);
          NodeAssert.ok(capturedRoot);
          for (const [relative, bytes] of fixture.bytes) {
            const captured = relative === producerPath ? emittedPath : relative;
            NodeAssert.deepEqual(
              await NodeFSP.default.readFile(NodePath.join(capturedRoot, captured)),
              bytes,
            );
          }
        }
        return realpath(name, ...args);
      });
      await NodeAssert.rejects(buildAndStagePreparationProducer(input), (error) => {
        NodeAssert.match(error.message, /producer execution contract is unqualified/);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        NodeAssert.equal(error.cleanup.root, capturedRoot);
        NodeAssert.equal(error.preserveStageRoot, undefined);
        return true;
      });
      await NodeAssert.rejects(NodeFSP.default.lstat(capturedRoot), { code: "ENOENT" });
      NodeAssert.deepEqual(await NodeFSP.default.readdir(input.stageResourcesDir), []);
    }),
);

NodeTest.test(
  "cancellation waits for the captured consumer before closing and removing its tree",
  (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      const realpath = NodeFSP.default.realpath;
      const entered = Promise.withResolvers();
      const release = Promise.withResolvers();
      const controller = new AbortController();
      const reason = Object.freeze(new Error("cancel retained capture"));
      let capturedRoot;
      t.mock.method(NodeFSP.default, "realpath", async (name, ...args) => {
        if (name === input.stageResourcesDir) {
          capturedRoot = [...fixture.allocated.keys()].find((root) => root !== fixture.fixtureRoot);
          entered.resolve();
          await release.promise;
        }
        return realpath(name, ...args);
      });
      const pending = buildAndStagePreparationProducer({ ...input, signal: controller.signal });
      const rejected = NodeAssert.rejects(pending, (error) => {
        NodeAssert.equal(error.cause, reason);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      try {
        await Promise.race([entered.promise, pending]);
        controller.abort(reason);
        NodeAssert.ok((await NodeFSP.default.lstat(capturedRoot)).isDirectory());
        NodeAssert.deepEqual(
          await NodeFSP.default.readFile(NodePath.join(capturedRoot, emittedPath)),
          fixture.bytes.get(producerPath),
        );
      } finally {
        release.resolve();
        await rejected;
      }
      await NodeAssert.rejects(NodeFSP.default.lstat(capturedRoot), { code: "ENOENT" });
    }),
);

NodeTest.test(
  "build-and-stage rechecks original sources after capture and before its execution gate",
  (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      const realpath = NodeFSP.default.realpath;
      t.mock.method(NodeFSP.default, "realpath", async (name, ...args) => {
        if (name === input.stageResourcesDir)
          await NodeFSP.default.writeFile(NodePath.join(input.sourceRoot, "package.json"), "changed");
        return realpath(name, ...args);
      });
      await NodeAssert.rejects(buildAndStagePreparationProducer(input), (error) => {
        NodeAssert.match(error.message, /input identity or bytes changed/);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      NodeAssert.deepEqual(await NodeFSP.default.readdir(input.stageResourcesDir), []);
    }),
);

NodeTest.test(
  "build-and-stage cannot accept a caller module, handle, callback or admission flag",
  (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      for (const extra of [
        { module: {} },
        { handle: {} },
        { importURL: "file:///outside" },
        { callback: () => NodeAssert.fail("caller callback executed") },
        { admitted: true },
      ])
        await NodeAssert.rejects(
          buildAndStagePreparationProducer({ ...input, ...extra }),
          /accepts only fixed source/,
        );
      NodeAssert.equal(fixture.allocated.size, 1);
    }),
);

for (const mismatch of ["source", "resources"]) {
  NodeTest.test(`build-and-stage refuses mismatched ${mismatch} before its execution gate`, (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      if (mismatch === "source") input.expectedSource.sourceSha = "c".repeat(40);
      else input.stageResourcesDir = fixture.sourceRoot;
      await NodeAssert.rejects(buildAndStagePreparationProducer(input), (error) => {
        NodeAssert.match(error.message, /requested source identity|inside the captured packaging stage/);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
    }),
  );
}

NodeTest.test(
  "consumer refusal survives descriptor cleanup uncertainty without discarding captured code",
  (t) =>
    withFixture(t, async (fixture) => {
      const input = await stagingInput(fixture);
      const open = NodeFSP.default.open;
      t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
        const handle = await open(name, ...args);
        if (name === NodePath.join(input.sourceRoot, reportPath)) {
          const close = handle.close.bind(handle);
          t.mock.method(handle, "close", async () => {
            await close();
            throw new Error("synthetic close observation lost");
          });
        }
        return handle;
      });
      await NodeAssert.rejects(buildAndStagePreparationProducer(input), (error) => {
        NodeAssert.ok(error instanceof AggregateError);
        NodeAssert.match(error.cause.message, /producer execution contract is unqualified/);
        NodeAssert.equal(error.errors[0], error.cause);
        NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
        NodeAssert.equal(error.preserveScratch, true);
        NodeAssert.equal(error.preserveStageRoot, undefined);
        return true;
      });
      const capturedRoot = [...fixture.allocated.keys()].find((root) => root !== fixture.fixtureRoot);
      NodeAssert.deepEqual(
        await NodeFSP.default.readFile(NodePath.join(capturedRoot, emittedPath)),
        fixture.bytes.get(producerPath),
      );
    }),
);

NodeTest.test(
  "capture copies exact bytes into distinct roots, returns held and never executes the sentinel",
  (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, bytes }) => {
      const unlink = NodeFSP.default.unlink;
      const copied = new Map();
      t.mock.method(NodeFSP.default, "unlink", async (name) => {
        copied.set(name, await NodeFSP.default.readFile(name));
        return unlink(name);
      });
      const result = await capturePreparationProducer({ sourceRoot, stageRoot });
      NodeAssert.equal(result.status, "held");
      NodeAssert.equal(result.reason, "producer-execution-closure-unqualified");
      NodeAssert.equal(result.imported, false);
      NodeAssert.equal(result.qualification, "unqualified");
      NodeAssert.equal(result.completeDependencyClosure, false);
      NodeAssert.equal(result.loadedCollectorBytesAttested, false);
      NodeAssert.equal(result.reportedGitIdentity.attested, false);
      NodeAssert.equal(result.reportedGitIdentity.sha, "a".repeat(40));
      NodeAssert.equal(result.sourceRoot, sourceRoot);
      NodeAssert.equal(result.stageRoot, stageRoot);
      NodeAssert.equal(new Set([sourceRoot, stageRoot, result.codeRoot]).size, 3);
      NodeAssert.notDeepEqual(result.sourceIdentity, result.codeIdentity);
      NodeAssert.equal(result.files.length, 34);
      NodeAssert.deepEqual(result.cleanup, { disposition: "removed", root: result.codeRoot });
      await NodeAssert.rejects(NodeFSP.default.lstat(result.codeRoot), { code: "ENOENT" });
      for (const file of result.files) {
        NodeAssert.deepEqual(
          copied.get(NodePath.join(result.codeRoot, file.capturedPath)),
          bytes.get(file.sourcePath),
        );
        NodeAssert.deepEqual(content(bytes.get(file.sourcePath)), {
          sha256: file.sha256,
          size: file.size,
        });
        NodeAssert.notDeepEqual(file.sourceIdentity, file.capturedIdentity);
      }
      NodeAssert.equal(
        result.files.find((file) => file.sourcePath === producerPath).capturedPath,
        emittedPath,
      );
      NodeAssert.equal(
        NodePath.resolve(result.codeRoot, NodePath.dirname(emittedPath), "../../.."),
        result.codeRoot,
      );
      NodeAssert.deepEqual(await NodeFSP.default.readdir(stageRoot), []);
    }),
);

NodeTest.test("concurrent captures own different roots and clean up independently", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const results = await Promise.all([
      capturePreparationProducer({ sourceRoot, stageRoot }),
      capturePreparationProducer({ sourceRoot, stageRoot }),
    ]);
    NodeAssert.notEqual(results[0].codeRoot, results[1].codeRoot);
    for (const result of results) {
      NodeAssert.equal(result.cleanup.disposition, "removed");
      await NodeAssert.rejects(NodeFSP.default.lstat(result.codeRoot), { code: "ENOENT" });
    }
  }),
);

for (const [name, mutate] of [
  ["extra path", (report) => report.source.inputs.push({ path: "../../outside" })],
  [
    "replacement path",
    (report) => {
      report.source.inputs[0].path = "../../outside";
    },
  ],
  [
    "duplicate path",
    (report) => {
      report.source.inputs[0] = report.source.inputs[1];
    },
  ],
  [
    "content path expansion",
    (report) => {
      report.source.inputs[0].content.path = "/outside";
    },
  ],
  [
    "unavailable source",
    (report) => {
      report.source.inputs[0].content.availability = "unavailable";
    },
  ],
  [
    "oversized source",
    (report) => {
      report.source.inputs[0].content.size = 8 * 1024 * 1024 + 1;
    },
  ],
  [
    "disagreeing inventories",
    (report) => {
      report.source.inputs[0].content.sha256 = "f".repeat(64);
    },
  ],
  [
    "qualified report",
    (report) => {
      report.qualification = "qualified";
    },
  ],
  [
    "claimed closure",
    (report) => {
      report.completeDependencyClosure = true;
    },
  ],
  [
    "claimed attestation",
    (report) => {
      report.loadedCollectorBytesAttested = true;
    },
  ],
  [
    "expanded outputs",
    (report) => {
      report.emitted.files.push({ fileName: "../other.mjs" });
    },
  ],
]) {
  NodeTest.test(`refuses ${name} before opening any producer or source file`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, report, writeReport }) => {
      mutate(report);
      await writeReport();
      const open = NodeFSP.default.open;
      const reads = [];
      t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
        if (name.startsWith(`${sourceRoot}${NodePath.sep}`))
          reads.push(NodePath.relative(sourceRoot, name));
        return open(name, ...args);
      });
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      NodeAssert.deepEqual(reads, [reportPath]);
    }),
  );
}

for (const kind of ["source", "producer", "report"]) {
  NodeTest.test(
    `refuses ${kind} replacement after opening its descriptor and removes owned scratch`,
    (t) =>
      withFixture(t, async ({ sourceRoot, stageRoot, bytes }) => {
        const relative =
          kind === "source" ? "package.json" : kind === "producer" ? producerPath : reportPath;
        const target = NodePath.join(sourceRoot, relative);
        const open = NodeFSP.default.open;
        let replaced = false;
        t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
          const handle = await open(name, ...args);
          if (name === target && !replaced) {
            replaced = true;
            await NodeFSP.default.rename(target, `${target}.original`);
            await NodeFSP.default.writeFile(target, bytes.get(relative), {
              flag: "wx",
              mode: 0o600,
            });
          }
          return handle;
        });
        await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
          NodeAssert.match(error.message, /identity|replaced/);
          NodeAssert.equal(error.cleanup.disposition, "removed");
          return true;
        });
        NodeAssert.equal(replaced, true);
      }),
  );
}

NodeTest.test("same-size source mutation after copying fails the final source recheck", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, bytes, allocated }) => {
    const open = NodeFSP.default.open;
    let mutated = false;
    t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
      const handle = await open(name, ...args);
      if (
        !mutated &&
        allocated.has(NodePath.dirname(name)) &&
        NodePath.basename(name) === "vite.config.ts"
      ) {
        mutated = true;
        const target = NodePath.join(sourceRoot, "package.json");
        await NodeFSP.default.writeFile(
          target,
          Buffer.alloc(bytes.get("package.json").length, 120),
        );
      }
      return handle;
    });
    await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      NodeAssert.match(error.message, /changed|differs/);
      NodeAssert.equal(error.cleanup.disposition, "removed");
      return true;
    });
    NodeAssert.equal(mutated, true);
  }),
);

for (const kind of ["missing", "symlink", "hardlink"]) {
  NodeTest.test(`refuses a ${kind} source without modifying its target`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, fixtureRoot }) => {
      const target = NodePath.join(sourceRoot, "package.json");
      const original = await NodeFSP.default.readFile(target);
      const outside = NodePath.join(fixtureRoot, "outside");
      await NodeFSP.default.writeFile(outside, original, { flag: "wx", mode: 0o600 });
      await NodeFSP.default.unlink(target);
      if (kind === "symlink") await NodeFSP.default.symlink(outside, target);
      if (kind === "hardlink") await NodeFSP.default.link(outside, target);
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      NodeAssert.deepEqual(await NodeFSP.default.readFile(outside), original);
    }),
  );
}

NodeTest.test("capture refuses stage and scratch overlap before allocating a root", (t) =>
  withFixture(t, async ({ sourceRoot, allocated }) => {
    await NodeAssert.rejects(
      capturePreparationProducer({
        sourceRoot,
        stageRoot: await NodeFSP.default.realpath(NodeOS.tmpdir()),
      }),
      /overlaps|owned directory/,
    );
    NodeAssert.equal(allocated.size, 1);
  }),
);

for (const alias of [false, true]) {
  NodeTest.test(
    `capture rejects ${alias ? "aliased" : "identical"} source and stage roots before allocation`,
    (t) =>
      withFixture(t, async ({ sourceRoot, fixtureRoot, allocated }) => {
        const stageRoot = alias ? NodePath.join(fixtureRoot, "source-alias") : sourceRoot;
        if (alias) await NodeFSP.default.symlink(sourceRoot, stageRoot);
        await NodeAssert.rejects(
          capturePreparationProducer({ sourceRoot, stageRoot }),
          /source and stage roots must be distinct/,
        );
        NodeAssert.equal(allocated.size, 1);
      }),
  );
}

NodeTest.test(
  "escaped descriptor uncertainty retains fixture roots until the owning test closes the descriptor",
  async (t) => {
    let retained;
    let closeDescriptor;
    const allocated = new Map();
    try {
      await NodeAssert.rejects(
        withFixture(t, async ({ sourceRoot, stageRoot, allocated: roots }) => {
          const open = NodeFSP.default.open;
          t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
            const handle = await open(name, ...args);
            if (name === NodePath.join(sourceRoot, reportPath)) {
              closeDescriptor = handle.close.bind(handle);
              t.mock.method(handle, "close", async () => {
                throw new Error("descriptor remains open");
              });
            }
            return handle;
          });
          try {
            await capturePreparationProducer({ sourceRoot, stageRoot });
          } finally {
            for (const [name, stat] of roots) allocated.set(name, stat);
          }
        }),
        (error) => {
          retained = error;
          NodeAssert.equal(error.preserveScratch, true);
          NodeAssert.equal(error.cause.preserveScratch, true);
          NodeAssert.equal(error.cause.cleanup.disposition, "retained-uncertain");
          NodeAssert.deepEqual(error.retainedRoots, [...allocated.keys()]);
          return true;
        },
      );
      NodeAssert.equal(allocated.size, 2);
      for (const [name, stat] of allocated)
        NodeAssert.ok(sameIdentity(stat, await NodeFSP.default.lstat(name, { bigint: true })));
    } finally {
      // This test owns the injected failure and the captured descriptor, unlike fixture teardown.
      if (closeDescriptor) await closeDescriptor();
      for (const [name, stat] of [...allocated].reverse()) {
        NodeAssert.ok(sameIdentity(stat, await NodeFSP.default.lstat(name, { bigint: true })));
        await NodeFSP.default.rm(name, { recursive: true });
      }
    }
    NodeAssert.ok(retained);
  },
);

for (const [name, relative, limit] of [
  ["report", reportPath, 16 * 1024 * 1024],
  ["producer", producerPath, 32 * 1024 * 1024],
  ["source", "package.json", 8 * 1024 * 1024],
]) {
  NodeTest.test(`refuses an oversized ${name} before opening its bytes`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot }) => {
      const target = NodePath.join(sourceRoot, relative);
      await NodeFSP.default.truncate(target, limit + 1);
      const open = NodeFSP.default.open;
      let opened = false;
      t.mock.method(NodeFSP.default, "open", async (file, ...args) => {
        if (file === target) opened = true;
        return open(file, ...args);
      });
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.match(error.message, /bounded owned regular file/);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      NodeAssert.equal(opened, false);
    }),
  );
}

NodeTest.test("caller import URLs, callbacks and admission flags are outside the API", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    for (const option of [
      { importURL: "file:///outside" },
      { callback: () => NodeAssert.fail("callback executed") },
      { admitted: true },
    ])
      await NodeAssert.rejects(
        capturePreparationProducer({ sourceRoot, stageRoot, ...option }),
        /accepts only/,
      );
    NodeAssert.equal(allocated.size, 1);
  }),
);

NodeTest.test("pre-cancellation preserves the exact reason and creates no scratch", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const reason = Object.freeze(new Error("capture cancelled before allocation"));
    await NodeAssert.rejects(
      capturePreparationProducer({ sourceRoot, stageRoot, signal: AbortSignal.abort(reason) }),
      (error) => error === reason,
    );
    NodeAssert.equal(allocated.size, 1);
  }),
);

NodeTest.test(
  "cancellation during capture cleans its tree and preserves a frozen abort reason",
  (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot }) => {
      const controller = new AbortController();
      const reason = Object.freeze(new Error("owned capture cancellation"));
      const open = NodeFSP.default.open;
      t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
        const handle = await open(name, ...args);
        if (name === NodePath.join(sourceRoot, producerPath)) controller.abort(reason);
        return handle;
      });
      await NodeAssert.rejects(
        capturePreparationProducer({ sourceRoot, stageRoot, signal: controller.signal }),
        (error) => {
          NodeAssert.equal(error.cause, reason);
          NodeAssert.equal(error.cleanup.disposition, "removed");
          return true;
        },
      );
    }),
);

NodeTest.test(
  "cleanup failure retains the exact root and preserves the original read failure",
  (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot }) => {
      const primary = new Error("synthetic source open failure");
      const cleanup = new Error("synthetic cleanup failure");
      const open = NodeFSP.default.open;
      t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
        if (name === NodePath.join(sourceRoot, producerPath)) throw primary;
        return open(name, ...args);
      });
      t.mock.method(NodeFSP.default, "rmdir", async () => {
        throw cleanup;
      });
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.ok(error instanceof AggregateError);
        NodeAssert.equal(error.cause, primary);
        NodeAssert.equal(error.errors[0], primary);
        NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
        NodeAssert.equal(error.retainedRoot, error.cleanup.root);
        NodeAssert.equal(error.preserveScratch, true);
        return true;
      });
    }),
);

NodeTest.test("cancellation during cleanup finishes cleanup before rejecting", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const controller = new AbortController();
    const reason = new Error("cancel during cleanup");
    const unlink = NodeFSP.default.unlink;
    t.mock.method(NodeFSP.default, "unlink", async (...args) => {
      controller.abort(reason);
      return unlink(...args);
    });
    await NodeAssert.rejects(
      capturePreparationProducer({ sourceRoot, stageRoot, signal: controller.signal }),
      (error) => {
        NodeAssert.equal(error, reason);
        NodeAssert.equal(error.cleanup.disposition, "removed");
        return true;
      },
    );
  }),
);

NodeTest.test(
  "descriptor close uncertainty retains scratch after an otherwise complete capture",
  (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot }) => {
      const open = NodeFSP.default.open;
      const closeFailure = new Error("close completion unknown");
      t.mock.method(NodeFSP.default, "open", async (name, ...args) => {
        const handle = await open(name, ...args);
        if (name === NodePath.join(sourceRoot, reportPath)) {
          const close = handle.close.bind(handle);
          t.mock.method(handle, "close", async () => {
            await close();
            throw closeFailure;
          });
        }
        return handle;
      });
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.ok(error instanceof AggregateError);
        NodeAssert.equal(error.errors[0].cause, closeFailure);
        NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
        NodeAssert.equal(error.preserveScratch, true);
        return true;
      });
    }),
);

NodeTest.test("scratch root replacement is retained and never followed during cleanup", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const readdir = NodeFSP.default.readdir;
    let replaced;
    let replacementStat;
    t.mock.method(NodeFSP.default, "readdir", async (name, ...args) => {
      const actual = await readdir(name, ...args);
      if (!replaced && allocated.has(name) && name !== NodePath.dirname(sourceRoot)) {
        replaced = name;
        await NodeFSP.default.rename(name, `${name}.displaced`);
        await NodeFSP.default.mkdir(name, { mode: 0o700 });
        replacementStat = await NodeFSP.default.lstat(name, { bigint: true });
      }
      return actual;
    });
    try {
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
        NodeAssert.equal(error.retainedRoot, replaced);
        return true;
      });
      NodeAssert.deepEqual(await NodeFSP.default.readdir(replaced), []);
      NodeAssert.equal((await NodeFSP.default.lstat(`${replaced}.displaced`)).isDirectory(), true);
    } finally {
      if (replaced) {
        NodeAssert.ok(
          sameIdentity(replacementStat, await NodeFSP.default.lstat(replaced, { bigint: true })),
        );
        await NodeFSP.default.rmdir(replaced);
        await NodeFSP.default.rename(`${replaced}.displaced`, replaced);
      }
    }
  }),
);

NodeTest.test("captured byte mutation retains the tree and cannot produce a held result", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const readdir = NodeFSP.default.readdir;
    let mutated = false;
    t.mock.method(NodeFSP.default, "readdir", async (name, ...args) => {
      const actual = await readdir(name, ...args);
      if (!mutated && allocated.has(name) && name !== NodePath.dirname(sourceRoot)) {
        mutated = true;
        const target = NodePath.join(name, emittedPath);
        const bytes = await NodeFSP.default.readFile(target);
        await NodeFSP.default.chmod(target, 0o600);
        await NodeFSP.default.writeFile(target, Buffer.alloc(bytes.length, 120));
        await NodeFSP.default.chmod(target, 0o400);
      }
      return actual;
    });
    await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
      NodeAssert.equal(error.preserveScratch, true);
      return true;
    });
    NodeAssert.equal(mutated, true);
  }),
);

NodeTest.test(
  "an unowned scratch entry makes retention sticky even after that entry disappears",
  (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
      const readdir = NodeFSP.default.readdir;
      let observed = false;
      t.mock.method(NodeFSP.default, "readdir", async (name, ...args) => {
        const actual = await readdir(name, ...args);
        if (!observed && allocated.has(name) && name !== NodePath.dirname(sourceRoot)) {
          observed = true;
          const unexpected = NodePath.join(name, "unowned-entry");
          await NodeFSP.default.writeFile(unexpected, "test-owned unexpected entry", {
            flag: "wx",
            mode: 0o600,
          });
          const snapshot = await readdir(name, ...args);
          await NodeFSP.default.unlink(unexpected);
          return snapshot;
        }
        return actual;
      });
      await NodeAssert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        NodeAssert.equal(error.preserveScratch, true);
        NodeAssert.equal(error.cleanup.disposition, "retained-uncertain");
        return true;
      });
      NodeAssert.equal(observed, true);
      for (const name of allocated.keys())
        NodeAssert.equal((await NodeFSP.default.lstat(name)).isDirectory(), true);
    }),
);
