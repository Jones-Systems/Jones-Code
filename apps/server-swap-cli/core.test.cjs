"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { DatabaseSync } = require("node:sqlite");
const F = require("./files.cjs");
const { createEngine, acquireLocks, verifyBackup } = require("./core.cjs");
const Native = require("./native.cjs");

function fixture(t, options = {}) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "server-swap-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const home = path.join(root, "home");
  const userdata = path.join(home, "userdata");
  fs.mkdirSync(userdata, { recursive: true });
  fs.writeFileSync(path.join(userdata, "environment-id"), "fixture-environment\n");
  fs.writeFileSync(path.join(userdata, "settings.json"), '{"theme":"dark"}\n');
  const database = new DatabaseSync(path.join(userdata, "statev2.sqlite"));
  database.exec(
    "PRAGMA journal_mode=WAL; CREATE TABLE history(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO history(value) VALUES ('retained-history');",
  );
  database.close();
  function artifact(file, value, mode = 0o600) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, value, { mode });
    return { path: file, sha256: F.hashFile(file) };
  }
  const serviceConfig = path.join(root, "installed.service");
  function release(distribution, version) {
    const payloadRoot = options.launcher
      ? path.join(home, "runtime/versions", version)
      : path.join(root, `${distribution}-${version}`);
    const executable = artifact(
      path.join(payloadRoot, "t3"),
      `fixture executable ${distribution} ${version}\n`,
      0o700,
    );
    const argv = [
      executable.path,
      ...(options.launcher ? ["__service-launcher"] : ["--no-browser"]),
    ];
    const config = artifact(
      path.join(root, `${distribution}-${version}.service`),
      `[Service]\nExecStart=${argv.join(" ")}\nEnvironment=T3CODE_HOME=${home}\nKillMode=control-group\n`,
    );
    return {
      distribution,
      version,
      executable,
      argv,
      config,
      payload: { root: payloadRoot, files: [{ path: "t3", sha256: executable.sha256 }] },
    };
  }
  const source = release(options.sourceDistribution || "jones", "1.2.3");
  const target = release(options.targetDistribution || "t3", "4.5.6");
  fs.copyFileSync(source.config.path, serviceConfig);
  const qualificationPath = path.join(root, "qualification.json");
  let raw = {
    schema: "server-swap-plan/1",
    id: "fixture-swap",
    host: os.hostname(),
    uid: process.getuid(),
    platform: "linux",
    arch: "arm64",
    home,
    stateDir: path.join(root, "operations"),
    environmentId: "fixture-environment",
    service: {
      manager: "systemd-user",
      name: "fixture.service",
      configPath: serviceConfig,
      dropIns: [],
    },
    source,
    target,
    qualification: { path: qualificationPath, sha256: "0".repeat(64) },
    descriptorUrl: "http://127.0.0.1:7777/.well-known/t3/environment",
    stateFiles: [...F.STATE_FILES],
    timeoutMs: 1000,
  };
  if (options.launcher) {
    raw.runtimeState = {
      source: artifact(
        path.join(root, "original-runtime.json"),
        JSON.stringify({ protocol: 4, activeVersion: source.version }),
      ),
      target: artifact(
        path.join(root, "target-runtime.json"),
        JSON.stringify({ protocol: 4, activeVersion: target.version }),
      ),
    };
    fs.copyFileSync(raw.runtimeState.source.path, path.join(home, "runtime/service-state.json"));
  }
  const evidence = artifact(
    path.join(root, "compatibility-evidence.txt"),
    "synthetic fixture qualification only\n",
  );
  const planPath = path.join(root, "plan.json");
  function writePlan() {
    raw = F.validatePlan(raw);
    const qualification = {
      schema: "server-swap-qualification/1",
      bindingDigest: F.bindingDigest(raw),
    };
    for (const group of ["migrations", "events", "settings", "clients"])
      qualification[group] = { status: "passed", summary: "Synthetic fixture evidence", evidence };
    qualification.migrations.requiredAdditionalBytes = 0;
    raw.qualification = artifact(qualificationPath, JSON.stringify(qualification));
    fs.writeFileSync(planPath, JSON.stringify(raw));
    return F.loadPlan(planPath);
  }
  let p = writePlan();
  let active = true;
  const calls = [];
  let stopError = false,
    startError = false,
    targetProofError = false,
    stopHook;
  const service = {
    async observe() {
      return { active, stopped: !active, pid: active ? 12345 : 0, reloadNeeded: false };
    },
    async detached() {},
    async stop() {
      calls.push("stop");
      active = false;
      stopHook?.();
      if (stopError) throw new Error("lost stop response");
    },
    async reload() {
      calls.push("reload");
    },
    async start() {
      calls.push("start");
      active = true;
      if (startError) throw new Error("lost start response");
    },
  };
  const engine = createEngine({
    verifyHost() {},
    acquireLocks() {
      return () => {};
    },
    serviceAdapter() {
      return service;
    },
    ownershipPaths() {
      return [
        path.join(home, ".fixture-owner.json"),
        path.join(root, "fixture-service-owner.json"),
      ];
    },
    async validateConfig(plan, selected) {
      await Native.validateConfig(plan, selected, async (file, args) => {
        assert.equal(file, "/usr/bin/id");
        assert.equal(args[0], "-u");
        return { stdout: `${plan.uid}\n`, code: 0 };
      });
    },
    async quiescence(plan) {
      assert.equal(active, false);
      return Native.inventory(plan);
    },
    async runtimeIdentity(plan, selected) {
      if (!active || (selected.version === plan.target.version && targetProofError))
        throw new Error("runtime identity not proved");
      return { runtimePid: 12345, version: selected.version, environmentId: plan.environmentId };
    },
  });
  return {
    root,
    home,
    userdata,
    planPath,
    raw,
    get plan() {
      return p;
    },
    engine,
    calls,
    service,
    writePlan() {
      p = writePlan();
      return p;
    },
    artifact,
    set stopError(value) {
      stopError = value;
    },
    set startError(value) {
      startError = value;
    },
    set targetProofError(value) {
      targetProofError = value;
    },
    set stopHook(value) {
      stopHook = value;
    },
  };
}

test("swap retains a real SQLite WAL-mode database, settings, absence records and paired source config", async (t) => {
  const f = fixture(t);
  const result = await f.engine.apply(f.planPath);
  assert.equal(result.status, "committed");
  assert.deepEqual(f.calls, ["stop", "reload", "start"]);
  const receipt = verifyBackup(f.plan);
  assert.equal(
    receipt.records.find((record) => record.name === "userdata/state.sqlite").absent,
    true,
  );
  const copy = new DatabaseSync(
    path.join(f.plan.transactionDir, "backup/userdata/statev2.sqlite"),
    { readOnly: true },
  );
  try {
    assert.equal(copy.prepare("SELECT value FROM history").get().value, "retained-history");
  } finally {
    copy.close();
  }
  assert.equal(
    fs.readFileSync(path.join(f.plan.transactionDir, "backup/userdata/settings.json"), "utf8"),
    '{"theme":"dark"}\n',
  );
  assert.equal(F.hashFile(f.plan.service.configPath), f.plan.target.config.sha256);
  await assert.rejects(f.engine.apply(f.planPath), /already exists/);
  assert.equal(f.calls.filter((call) => call === "start").length, 1);
});

test("changed compatibility binding or payload blocks before stopping", async (t) => {
  const f = fixture(t);
  fs.appendFileSync(f.plan.target.executable.path, "changed");
  await assert.rejects(f.engine.apply(f.planPath), /digest mismatch/);
  assert.deepEqual(f.calls, []);
});

test("unlisted database blocks before stopping", async (t) => {
  const f = fixture(t);
  fs.writeFileSync(path.join(f.userdata, "other.sqlite"), "unaccounted");
  await assert.rejects(f.engine.apply(f.planPath), /Unlisted database/);
  assert.deepEqual(f.calls, []);
});

test("lost stop response reconciles stopped state and resumes without repeating stop", async (t) => {
  const f = fixture(t);
  f.stopError = true;
  await assert.rejects(f.engine.apply(f.planPath), /lost stop response/);
  assert.equal((await f.engine.status(f.planPath)).status, "unknown");
  assert.equal((await f.engine.reconcile(f.planPath)).phase, "stopped");
  assert.equal((await f.engine.resume(f.planPath)).status, "committed");
  assert.deepEqual(f.calls, ["stop", "reload", "start"]);
});

test("lost start response is never replayed and successful observation reconciles commit", async (t) => {
  const f = fixture(t);
  f.startError = true;
  await assert.rejects(f.engine.apply(f.planPath), /lost start response/);
  assert.equal((await f.engine.status(f.planPath)).status, "recovery-required");
  assert.equal((await f.engine.reconcile(f.planPath)).status, "committed");
  assert.equal(f.calls.filter((call) => call === "start").length, 1);
});

test("uncertain target start retains target config and changed database without rollback", async (t) => {
  const f = fixture(t);
  f.startError = true;
  f.targetProofError = true;
  const oldHash = F.hashFile(path.join(f.userdata, "statev2.sqlite"));
  await assert.rejects(f.engine.apply(f.planPath), /lost start response/);
  const database = new DatabaseSync(path.join(f.userdata, "statev2.sqlite"));
  database.exec("INSERT INTO history(value) VALUES ('target-write');");
  database.close();
  assert.equal((await f.engine.reconcile(f.planPath)).status, "recovery-required");
  await assert.rejects(f.engine.resume(f.planPath), /reconciled/);
  assert.notEqual(F.hashFile(path.join(f.userdata, "statev2.sqlite")), oldHash);
  assert.equal(F.hashFile(f.plan.service.configPath), f.plan.target.config.sha256);
  assert.equal(f.calls.filter((call) => call === "start").length, 1);
});

test("journal cannot be reused by a changed exact plan", async (t) => {
  const f = fixture(t);
  f.stopError = true;
  await assert.rejects(f.engine.apply(f.planPath));
  f.raw.timeoutMs = 2000;
  f.writePlan();
  await assert.rejects(f.engine.status(f.planPath), /another exact plan/);
});

test("clean native metadata transition preserves the paired original metadata backup", async (t) => {
  const f = fixture(t, { launcher: true });
  assert.equal((await f.engine.apply(f.planPath)).status, "committed");
  assert.equal(
    F.hashFile(path.join(f.home, "runtime/service-state.json")),
    f.plan.runtimeState.target.sha256,
  );
  assert.equal(
    F.hashFile(path.join(f.plan.transactionDir, "backup/runtime/service-state.json")),
    f.plan.runtimeState.source.sha256,
  );
});

test("native source pending update cannot be discarded", async (t) => {
  const f = fixture(t, { launcher: true });
  f.raw.runtimeState.source = f.artifact(
    f.raw.runtimeState.source.path,
    JSON.stringify({
      protocol: 4,
      activeVersion: f.raw.source.version,
      update: { status: "pending" },
    }),
  );
  f.writePlan();
  await assert.rejects(f.engine.apply(f.planPath), /pending or unknown/);
  assert.deepEqual(f.calls, []);
});

test("native pending update blocks a direct target without runtime-state transition", async (t) => {
  const f = fixture(t);
  fs.mkdirSync(path.join(f.home, "runtime"));
  fs.writeFileSync(
    path.join(f.home, "runtime/service-state.json"),
    JSON.stringify({ protocol: 4, activeVersion: "1.2.3", update: { status: "pending" } }),
  );
  await assert.rejects(f.engine.apply(f.planPath), /pending or unknown/);
  assert.deepEqual(f.calls, []);
});

test("exclusive SQLite ownership releases automatically without deleting persistent locks", (t) => {
  const f = fixture(t);
  const lockDirectory = path.join(f.root, "locks");
  const release = acquireLocks(f.plan, lockDirectory);
  const lockPath = path.join(f.home, ".server-swap-lock.sqlite");
  const inode = fs.statSync(lockPath).ino;
  try {
    assert.throws(() => acquireLocks(f.plan, lockDirectory), /ownership unavailable/);
  } finally {
    release();
  }
  const again = acquireLocks(f.plan, lockDirectory);
  try {
    assert.equal(fs.statSync(lockPath).ino, inode);
  } finally {
    again();
  }
});

test("state mutation after backup blocks target start", async (t) => {
  const f = fixture(t);
  f.service.reload = async () => {
    fs.appendFileSync(path.join(f.userdata, "settings.json"), "changed");
  };
  await assert.rejects(f.engine.apply(f.planPath), /digest mismatch/);
  assert.equal(f.calls.includes("start"), false);
  assert.ok(fs.existsSync(path.join(f.plan.transactionDir, "backup/receipt.json")));
});

test("hashing a large state file uses bounded chunks", (t) => {
  const f = fixture(t);
  const file = path.join(f.root, "large");
  const fd = fs.openSync(file, "w");
  try {
    for (let i = 0; i < 24; i++) fs.writeSync(fd, Buffer.alloc(1024 * 1024, i));
  } finally {
    fs.closeSync(fd);
  }
  assert.match(F.hashFile(file), /^[a-f0-9]{64}$/);
  assert.throws(() => F.checked({ path: file, sha256: F.hashFile(file) }), /16 MiB/);
  assert.doesNotThrow(() => F.verifyArtifact({ path: file, sha256: F.hashFile(file) }));
});

test("unsafe target tags, state placement, launcher metadata and payload symlinks fail closed", (t) => {
  const f = fixture(t);
  assert.throws(
    () => F.validatePlan({ ...f.raw, target: { ...f.raw.target, version: "latest" } }),
    /Exact/,
  );
  assert.throws(
    () => F.validatePlan({ ...f.raw, stateDir: path.join(f.home, "operations") }),
    /outside/,
  );
  assert.throws(
    () =>
      F.validatePlan({
        ...f.raw,
        target: { ...f.raw.target, argv: [f.raw.target.argv[0], "__service-launcher"] },
      }),
    /metadata|service-state/,
  );
  fs.symlinkSync(f.raw.source.executable.path, path.join(f.raw.target.payload.root, "alias"));
  assert.throws(() => F.verifyPayload(f.plan.target), /symlink/);
});

function selectDropIn(f) {
  const base = f.artifact(
    path.join(f.root, "host-tooling-t3.service"),
    "[Service]\nUser=fixture-operator\nExecStart=/opt/base/t3 --no-browser\nEnvironment=T3CODE_HOME=/opt/base-home\nKillMode=control-group\n",
  );
  const source = f.artifact(
    f.raw.source.config.path,
    `[Service]\nExecStart=\nExecStart=${f.raw.source.argv.join(" ")}\nEnvironment=T3CODE_HOME=${f.home}\n`,
  );
  const target = f.artifact(
    f.raw.target.config.path,
    `[Service]\nExecStart=\nExecStart=${f.raw.target.argv.join(" ")}\nEnvironment=T3CODE_HOME=${f.home}\n`,
  );
  const selected = f.artifact(
    path.join(f.root, "host-tooling-t3.service.d/jones-code.conf"),
    fs.readFileSync(source.path),
  );
  const policy = f.artifact(
    path.join(f.root, "host-tooling-t3.service.d/resources.conf"),
    "[Service]\nMemoryMax=8G\n",
  );
  f.raw.service = {
    ...f.raw.service,
    manager: "systemd-system",
    name: "host-tooling-t3.service",
    configPath: selected.path,
    fragment: base,
    dropIns: [selected, policy],
  };
  f.raw.source.config = source;
  f.raw.target.config = target;
  f.writePlan();
  return { base, selected, policy };
}

test("selected ExecStart override swap preserves named User, base fragment and other drop-ins", async (t) => {
  const f = fixture(t);
  const files = selectDropIn(f);
  assert.equal((await f.engine.apply(f.planPath)).status, "committed");
  assert.equal(F.hashFile(files.selected.path), f.plan.target.config.sha256);
  assert.equal(F.hashFile(files.base.path), files.base.sha256);
  assert.equal(F.hashFile(files.policy.path), files.policy.sha256);
  assert.equal(
    F.hashFile(path.join(f.plan.transactionDir, "backup/original-service-config")),
    f.plan.source.config.sha256,
  );
  assert.doesNotThrow(() => Native.verifyLiveConfigIdentity(f.plan));
  assert.equal((await f.engine.reconcile(f.planPath)).status, "committed");
});

test("drift in a bound systemd fragment blocks the selected drop-in swap before stop", async (t) => {
  const f = fixture(t);
  const files = selectDropIn(f);
  fs.appendFileSync(files.base.path, "User=someone-else\n");
  await assert.rejects(f.engine.apply(f.planPath), /digest mismatch/);
  assert.deepEqual(f.calls, []);
});

test("a selected live drop-in cannot jump to the target hash before install intent", (t) => {
  const f = fixture(t);
  const files = selectDropIn(f);
  fs.copyFileSync(f.plan.target.config.path, files.selected.path);
  assert.throws(() => Native.verifyLiveConfigIdentity(f.plan), /journal phase/);
});

test("ambiguous duplicate drop-in names and order are rejected", (t) => {
  const f = fixture(t);
  selectDropIn(f);
  assert.throws(
    () =>
      F.validatePlan({
        ...f.plan,
        service: { ...f.plan.service, dropIns: [...f.plan.service.dropIns].reverse() },
      }),
    /lexicographic/,
  );
  const duplicate = {
    ...f.plan.service.dropIns[0],
    path: path.join(f.root, "other/jones-code.conf"),
  };
  assert.throws(
    () =>
      F.validatePlan({
        ...f.plan,
        service: { ...f.plan.service, dropIns: [f.plan.service.dropIns[0], duplicate] },
      }),
    /unique names/,
  );
});

test("unresolved prior transaction fences a different id and state directory after worker exit", async (t) => {
  const f = fixture(t);
  f.stopError = true;
  await assert.rejects(f.engine.apply(f.planPath), /lost stop response/);
  const first = F.readJson(path.join(f.home, ".fixture-owner.json"));
  f.raw.id = "different-swap";
  f.raw.stateDir = path.join(f.root, "different-operations");
  f.writePlan();
  await assert.rejects(f.engine.apply(f.planPath), /unresolved prior transaction/);
  assert.equal(F.readJson(path.join(f.home, ".fixture-owner.json")).planDigest, first.planDigest);
  assert.deepEqual(f.calls, ["stop"]);
});

for (const operation of ["reconcile", "resume"]) {
  test(`${operation}: failed revalidation of a historical commit cannot produce a current committed status`, async (t) => {
    const f = fixture(t);
    assert.equal((await f.engine.apply(f.planPath)).status, "committed");
    f.targetProofError = true;
    assert.equal((await f.engine.status(f.planPath)).status, "committed-unverified");
    const reconciliation = await f.engine[operation](f.planPath);
    assert.equal(reconciliation.status, "unknown");
    assert.equal(reconciliation.phase, "committed");
    assert.equal((await f.engine.status(f.planPath)).status, "unknown");
    assert.equal(F.readJson(path.join(f.home, ".fixture-owner.json")).status, "unknown");
  });

  test(`${operation}: committed payload drift found during preflight also invalidates its durable status and owner`, async (t) => {
    const f = fixture(t);
    assert.equal((await f.engine.apply(f.planPath)).status, "committed");
    fs.appendFileSync(f.plan.target.executable.path, "drift");
    assert.equal((await f.engine[operation](f.planPath)).status, "unknown");
    assert.equal((await f.engine.status(f.planPath)).status, "unknown");
    assert.equal(F.readJson(path.join(f.home, ".fixture-owner.json")).status, "unknown");
  });
}

for (const [sourceDistribution, targetDistribution] of [
  ["t3", "jones"],
  ["jones", "jones"],
]) {
  test(`qualified ${sourceDistribution} to ${targetDistribution} exact-version swap retains the source pair`, async (t) => {
    const f = fixture(t, { sourceDistribution, targetDistribution });
    const result = await f.engine.apply(f.planPath);
    assert.equal(result.status, "committed");
    assert.equal(verifyBackup(f.plan).source.distribution, sourceDistribution);
    assert.equal(result.identity.version, f.plan.target.version);
    assert.equal(F.hashFile(f.plan.service.configPath), f.plan.target.config.sha256);
    assert.deepEqual(f.calls, ["stop", "reload", "start"]);
  });
}

test("known before-start failure can resume the same plan after restoring its exact inputs", async (t) => {
  const f = fixture(t);
  const settings = path.join(f.userdata, "settings.json");
  f.service.reload = async () => {
    fs.appendFileSync(settings, "changed");
  };
  await assert.rejects(f.engine.apply(f.planPath), /digest mismatch/);
  assert.equal((await f.engine.status(f.planPath)).status, "failed-before-start");
  fs.copyFileSync(path.join(f.plan.transactionDir, "backup/userdata/settings.json"), settings);
  assert.equal((await f.engine.resume(f.planPath)).status, "committed");
  assert.equal(f.calls.filter((call) => call === "stop").length, 1);
  assert.equal(f.calls.filter((call) => call === "start").length, 1);
});

module.exports = { fixture };
