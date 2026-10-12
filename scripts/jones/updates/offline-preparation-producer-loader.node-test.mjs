import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { capturePreparationProducer } from "./offline-preparation-producer-loader.mjs";

const artifactDirectory = "apps/server/dist-exe/preparation-producer-candidate";
const producerPath = `${artifactDirectory}/offlinePreparationProducer.mjs`;
const reportPath = `${artifactDirectory}/producer-candidate-graph.json`;
const emittedPath = "scripts/jones/updates/offlinePreparationProducer.mjs";
const sharedPaths = [
  "pnpm-lock.yaml", "pnpm-workspace.yaml", "package.json",
  "apps/server/package.json", "apps/desktop/package.json", "apps/web/package.json",
  "packages/contracts/package.json", "apps/server/vite.config.ts", "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
];
const producerPaths = [
  ...sharedPaths, "scripts/package.json",
  "scripts/jones/updates/offline-preparation-producer-entry.ts",
  "scripts/jones/updates/functional-policy.ts", "scripts/jones/updates/legacy-bootstrap-bundle.ts",
];
const qualifierPaths = [
  ...sharedPaths,
  ...["offlinePreparationEntry.ts", "offlinePreparation.ts", "offlinePreparationCore.ts",
    "offlinePreparationPlan.ts", "offlinePreparationWorker.ts", "offlinePreparationWorker.test.ts"]
    .map((name) => `apps/server/src/jones/updates/${name}`),
  ...["legacy-preparation-native-fixture.mjs", "legacy-preparation-native-fixture.d.mts",
    "legacy-cold-maintenance-fixture.mjs", "legacy-identity-native-fixture.mjs"]
    .map((name) => `scripts/jones/updates/${name}`),
  ...["jonesLegacyBootstrapSource.ts", "jonesLegacyMaintenanceSource.ts", "jonesNativeHelperSource.ts",
    "jonesPairedStateSource.ts", "jonesColdPairSource.ts", "jonesLegacyIdentityNative.c",
    "jonesColdOwnerSource.ts", "jonesPreparationWorkerSource.ts"]
    .map((name) => `apps/desktop/src/jones/updates/${name}`),
];
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const content = (bytes) => ({ sha256: sha256(bytes), size: bytes.length });
const sameIdentity = (left, right) => left.dev === right.dev && left.ino === right.ino && left.uid === right.uid;

async function withFixture(t, run) {
  const parent = await fs.realpath(os.tmpdir());
  const fixtureRoot = await fs.mkdtemp(path.join(parent, "jones-producer-capture-test-"));
  const allocated = new Map([[fixtureRoot, undefined]]);
  let primary;
  let failed = false;
  try {
    allocated.set(fixtureRoot, await fs.lstat(fixtureRoot, { bigint: true }));
    const originalMkdtemp = fs.mkdtemp;
    t.mock.method(fs, "mkdtemp", async (...args) => {
      const name = await originalMkdtemp(...args);
      allocated.set(name, undefined);
      allocated.set(name, await fs.lstat(name, { bigint: true }));
      return name;
    });
    const sourceRoot = path.join(fixtureRoot, "source");
    const stageRoot = path.join(fixtureRoot, "stage");
    await fs.mkdir(sourceRoot, { mode: 0o700 });
    await fs.mkdir(stageRoot, { mode: 0o700 });
    const bytes = new Map([...new Set([...producerPaths, ...qualifierPaths])]
      .map((name, index) => [name, Buffer.from(`// synthetic source ${index}: π\r\n`)]));
    bytes.set(producerPath, Buffer.from('throw new Error("producer sentinel executed");\n'));
    const row = (name) => ({ path: name, content: { availability: "observed", path: name, ...content(bytes.get(name)) } });
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
      source: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40),
        buildOnlyOverlays: [], inputs: producerPaths.map(row) },
      lateQualifierSources: { basis: "fixed-late-qualifier-file-inputs", computedImportsResolved: false,
        completeDependencyClosure: false, files: qualifierPaths.map(row) },
      emitted: { format: "esm", fileName: "offlinePreparationProducer.mjs", ...producer,
        files: [{ fileName: "offlinePreparationProducer.mjs", ...producer }] },
    };
    const writeReport = async () => {
      bytes.set(reportPath, Buffer.from(JSON.stringify(report)));
      await fs.writeFile(path.join(sourceRoot, reportPath), bytes.get(reportPath), { mode: 0o600 });
    };
    for (const [name, data] of bytes) {
      await fs.mkdir(path.dirname(path.join(sourceRoot, name)), { recursive: true, mode: 0o700 });
      await fs.writeFile(path.join(sourceRoot, name), data, { flag: "wx", mode: 0o600 });
    }
    await writeReport();
    await run({ sourceRoot, stageRoot, fixtureRoot, bytes, report, writeReport, allocated });
  } catch (cause) {
    failed = true;
    primary = cause;
  } finally {
    t.mock.restoreAll();
    const failures = [];
    for (const [name, original] of [...allocated].reverse()) {
      try {
        const current = await fs.lstat(name, { bigint: true }).catch((cause) => {
          if (cause.code === "ENOENT") return undefined;
          throw cause;
        });
        if (!current) continue;
        if (!original || !current.isDirectory() || !sameIdentity(original, current))
          throw Object.assign(new Error(`Test scratch custody changed: ${name}`), { retainedRoot: name, preserveScratch: true });
        await fs.rm(name, { recursive: true });
      } catch (cause) { failures.push(cause); }
    }
    if (failures.length) throw new AggregateError(failed ? [primary, ...failures] : failures, "Capture test cleanup failed.", { cause: primary });
    if (failed) throw primary;
  }
}

test("capture copies exact bytes into distinct roots, returns held and never executes the sentinel", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, bytes }) => {
    const unlink = fs.unlink;
    const copied = new Map();
    t.mock.method(fs, "unlink", async (name) => {
      copied.set(name, await fs.readFile(name));
      return unlink(name);
    });
    const result = await capturePreparationProducer({ sourceRoot, stageRoot });
    assert.equal(result.status, "held");
    assert.equal(result.reason, "producer-execution-closure-unqualified");
    assert.equal(result.imported, false);
    assert.equal(result.qualification, "unqualified");
    assert.equal(result.completeDependencyClosure, false);
    assert.equal(result.loadedCollectorBytesAttested, false);
    assert.equal(result.reportedGitIdentity.attested, false);
    assert.equal(result.reportedGitIdentity.sha, "a".repeat(40));
    assert.equal(result.sourceRoot, sourceRoot);
    assert.equal(result.stageRoot, stageRoot);
    assert.equal(new Set([sourceRoot, stageRoot, result.codeRoot]).size, 3);
    assert.notDeepEqual(result.sourceIdentity, result.codeIdentity);
    assert.equal(result.files.length, 34);
    assert.deepEqual(result.cleanup, { disposition: "removed", root: result.codeRoot });
    await assert.rejects(fs.lstat(result.codeRoot), { code: "ENOENT" });
    for (const file of result.files) {
      assert.deepEqual(copied.get(path.join(result.codeRoot, file.capturedPath)), bytes.get(file.sourcePath));
      assert.deepEqual(content(bytes.get(file.sourcePath)), { sha256: file.sha256, size: file.size });
      assert.notDeepEqual(file.sourceIdentity, file.capturedIdentity);
    }
    assert.equal(result.files.find((file) => file.sourcePath === producerPath).capturedPath, emittedPath);
    assert.equal(path.resolve(result.codeRoot, path.dirname(emittedPath), "../../.."), result.codeRoot);
    assert.deepEqual(await fs.readdir(stageRoot), []);
  }));

test("concurrent captures own different roots and clean up independently", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const results = await Promise.all([
      capturePreparationProducer({ sourceRoot, stageRoot }),
      capturePreparationProducer({ sourceRoot, stageRoot }),
    ]);
    assert.notEqual(results[0].codeRoot, results[1].codeRoot);
    for (const result of results) {
      assert.equal(result.cleanup.disposition, "removed");
      await assert.rejects(fs.lstat(result.codeRoot), { code: "ENOENT" });
    }
  }));

for (const [name, mutate] of [
  ["extra path", (report) => report.source.inputs.push({ path: "../../outside" })],
  ["replacement path", (report) => { report.source.inputs[0].path = "../../outside"; }],
  ["duplicate path", (report) => { report.source.inputs[0] = report.source.inputs[1]; }],
  ["content path expansion", (report) => { report.source.inputs[0].content.path = "/outside"; }],
  ["unavailable source", (report) => { report.source.inputs[0].content.availability = "unavailable"; }],
  ["oversized source", (report) => { report.source.inputs[0].content.size = 8 * 1024 * 1024 + 1; }],
  ["disagreeing inventories", (report) => { report.source.inputs[0].content.sha256 = "f".repeat(64); }],
  ["qualified report", (report) => { report.qualification = "qualified"; }],
  ["claimed closure", (report) => { report.completeDependencyClosure = true; }],
  ["claimed attestation", (report) => { report.loadedCollectorBytesAttested = true; }],
  ["expanded outputs", (report) => { report.emitted.files.push({ fileName: "../other.mjs" }); }],
]) {
  test(`refuses ${name} before opening any producer or source file`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, report, writeReport }) => {
      mutate(report);
      await writeReport();
      const open = fs.open;
      const reads = [];
      t.mock.method(fs, "open", async (name, ...args) => {
        if (name.startsWith(`${sourceRoot}${path.sep}`)) reads.push(path.relative(sourceRoot, name));
        return open(name, ...args);
      });
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        assert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      assert.deepEqual(reads, [reportPath]);
    }));
}

for (const kind of ["source", "producer", "report"]) {
  test(`refuses ${kind} replacement after opening its descriptor and removes owned scratch`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, bytes }) => {
      const relative = kind === "source" ? "package.json" : kind === "producer" ? producerPath : reportPath;
      const target = path.join(sourceRoot, relative);
      const open = fs.open;
      let replaced = false;
      t.mock.method(fs, "open", async (name, ...args) => {
        const handle = await open(name, ...args);
        if (name === target && !replaced) {
          replaced = true;
          await fs.rename(target, `${target}.original`);
          await fs.writeFile(target, bytes.get(relative), { flag: "wx", mode: 0o600 });
        }
        return handle;
      });
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        assert.match(error.message, /identity|replaced/);
        assert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      assert.equal(replaced, true);
    }));
}

test("same-size source mutation after copying fails the final source recheck", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, bytes, allocated }) => {
    const open = fs.open;
    let mutated = false;
    t.mock.method(fs, "open", async (name, ...args) => {
      const handle = await open(name, ...args);
      if (!mutated && allocated.has(path.dirname(name)) && path.basename(name) === "vite.config.ts") {
        mutated = true;
        const target = path.join(sourceRoot, "package.json");
        await fs.writeFile(target, Buffer.alloc(bytes.get("package.json").length, 120));
      }
      return handle;
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      assert.match(error.message, /changed|differs/);
      assert.equal(error.cleanup.disposition, "removed");
      return true;
    });
    assert.equal(mutated, true);
  }));

for (const kind of ["missing", "symlink", "hardlink"]) {
  test(`refuses a ${kind} source without modifying its target`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot, fixtureRoot }) => {
      const target = path.join(sourceRoot, "package.json");
      const original = await fs.readFile(target);
      const outside = path.join(fixtureRoot, "outside");
      await fs.writeFile(outside, original, { flag: "wx", mode: 0o600 });
      await fs.unlink(target);
      if (kind === "symlink") await fs.symlink(outside, target);
      if (kind === "hardlink") await fs.link(outside, target);
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        assert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      assert.deepEqual(await fs.readFile(outside), original);
    }));
}

test("capture refuses stage and scratch overlap before allocating a root", (t) =>
  withFixture(t, async ({ sourceRoot, allocated }) => {
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot: await fs.realpath(os.tmpdir()) }), /overlaps|owned directory/);
    assert.equal(allocated.size, 1);
  }));

for (const [name, relative, limit] of [
  ["report", reportPath, 16 * 1024 * 1024],
  ["producer", producerPath, 32 * 1024 * 1024],
  ["source", "package.json", 8 * 1024 * 1024],
]) {
  test(`refuses an oversized ${name} before opening its bytes`, (t) =>
    withFixture(t, async ({ sourceRoot, stageRoot }) => {
      const target = path.join(sourceRoot, relative);
      await fs.truncate(target, limit + 1);
      const open = fs.open;
      let opened = false;
      t.mock.method(fs, "open", async (file, ...args) => {
        if (file === target) opened = true;
        return open(file, ...args);
      });
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        assert.match(error.message, /bounded owned regular file/);
        assert.equal(error.cleanup.disposition, "removed");
        return true;
      });
      assert.equal(opened, false);
    }));
}

test("caller import URLs, callbacks and admission flags are outside the API", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    for (const option of [{ importURL: "file:///outside" }, { callback: () => assert.fail("callback executed") }, { admitted: true }])
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot, ...option }), /accepts only/);
    assert.equal(allocated.size, 1);
  }));

test("pre-cancellation preserves the exact reason and creates no scratch", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const reason = Object.freeze(new Error("capture cancelled before allocation"));
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot, signal: AbortSignal.abort(reason) }), (error) => error === reason);
    assert.equal(allocated.size, 1);
  }));

test("cancellation during capture cleans its tree and preserves a frozen abort reason", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const controller = new AbortController();
    const reason = Object.freeze(new Error("owned capture cancellation"));
    const open = fs.open;
    t.mock.method(fs, "open", async (name, ...args) => {
      const handle = await open(name, ...args);
      if (name === path.join(sourceRoot, producerPath)) controller.abort(reason);
      return handle;
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot, signal: controller.signal }), (error) => {
      assert.equal(error.cause, reason);
      assert.equal(error.cleanup.disposition, "removed");
      return true;
    });
  }));

test("cleanup failure retains the exact root and preserves the original read failure", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const primary = new Error("synthetic source open failure");
    const cleanup = new Error("synthetic cleanup failure");
    const open = fs.open;
    t.mock.method(fs, "open", async (name, ...args) => {
      if (name === path.join(sourceRoot, producerPath)) throw primary;
      return open(name, ...args);
    });
    t.mock.method(fs, "rmdir", async () => { throw cleanup; });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.cause, primary);
      assert.equal(error.errors[0], primary);
      assert.equal(error.cleanup.disposition, "retained-uncertain");
      assert.equal(error.retainedRoot, error.cleanup.root);
      assert.equal(error.preserveScratch, true);
      return true;
    });
  }));

test("cancellation during cleanup finishes cleanup before rejecting", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const controller = new AbortController();
    const reason = new Error("cancel during cleanup");
    const unlink = fs.unlink;
    t.mock.method(fs, "unlink", async (...args) => {
      controller.abort(reason);
      return unlink(...args);
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot, signal: controller.signal }), (error) => {
      assert.equal(error, reason);
      assert.equal(error.cleanup.disposition, "removed");
      return true;
    });
  }));

test("descriptor close uncertainty retains scratch after an otherwise complete capture", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot }) => {
    const open = fs.open;
    const closeFailure = new Error("close completion unknown");
    t.mock.method(fs, "open", async (name, ...args) => {
      const handle = await open(name, ...args);
      if (name === path.join(sourceRoot, reportPath)) {
        const close = handle.close.bind(handle);
        t.mock.method(handle, "close", async () => {
          await close();
          throw closeFailure;
        });
      }
      return handle;
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors[0].cause, closeFailure);
      assert.equal(error.cleanup.disposition, "retained-uncertain");
      assert.equal(error.preserveScratch, true);
      return true;
    });
  }));

test("scratch root replacement is retained and never followed during cleanup", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const readdir = fs.readdir;
    let replaced;
    let replacementStat;
    t.mock.method(fs, "readdir", async (name, ...args) => {
      const actual = await readdir(name, ...args);
      if (!replaced && allocated.has(name) && name !== path.dirname(sourceRoot)) {
        replaced = name;
        await fs.rename(name, `${name}.displaced`);
        await fs.mkdir(name, { mode: 0o700 });
        replacementStat = await fs.lstat(name, { bigint: true });
      }
      return actual;
    });
    try {
      await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
        assert.equal(error.cleanup.disposition, "retained-uncertain");
        assert.equal(error.retainedRoot, replaced);
        return true;
      });
      assert.deepEqual(await fs.readdir(replaced), []);
      assert.equal((await fs.lstat(`${replaced}.displaced`)).isDirectory(), true);
    } finally {
      if (replaced) {
        assert.ok(sameIdentity(replacementStat, await fs.lstat(replaced, { bigint: true })));
        await fs.rmdir(replaced);
        await fs.rename(`${replaced}.displaced`, replaced);
      }
    }
  }));

test("captured byte mutation retains the tree and cannot produce a held result", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const readdir = fs.readdir;
    let mutated = false;
    t.mock.method(fs, "readdir", async (name, ...args) => {
      const actual = await readdir(name, ...args);
      if (!mutated && allocated.has(name) && name !== path.dirname(sourceRoot)) {
        mutated = true;
        const target = path.join(name, emittedPath);
        const bytes = await fs.readFile(target);
        await fs.chmod(target, 0o600);
        await fs.writeFile(target, Buffer.alloc(bytes.length, 120));
        await fs.chmod(target, 0o400);
      }
      return actual;
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      assert.equal(error.cleanup.disposition, "retained-uncertain");
      assert.equal(error.preserveScratch, true);
      return true;
    });
    assert.equal(mutated, true);
  }));

test("an unowned scratch entry makes retention sticky even after that entry disappears", (t) =>
  withFixture(t, async ({ sourceRoot, stageRoot, allocated }) => {
    const readdir = fs.readdir;
    let observed = false;
    t.mock.method(fs, "readdir", async (name, ...args) => {
      const actual = await readdir(name, ...args);
      if (!observed && allocated.has(name) && name !== path.dirname(sourceRoot)) {
        observed = true;
        const unexpected = path.join(name, "unowned-entry");
        await fs.writeFile(unexpected, "test-owned unexpected entry", { flag: "wx", mode: 0o600 });
        const snapshot = await readdir(name, ...args);
        await fs.unlink(unexpected);
        return snapshot;
      }
      return actual;
    });
    await assert.rejects(capturePreparationProducer({ sourceRoot, stageRoot }), (error) => {
      assert.equal(error.preserveScratch, true);
      assert.equal(error.cleanup.disposition, "retained-uncertain");
      return true;
    });
    assert.equal(observed, true);
    for (const name of allocated.keys())
      assert.equal((await fs.lstat(name)).isDirectory(), true);
  }));
