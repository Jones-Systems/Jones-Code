"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const F = require("./files.cjs");
const P = require("./prepare.cjs");

function fixture(t, sourceDistribution = "jones", targetDistribution = "t3", native = false) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "server-swap-prepare-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  function file(relative, bytes, mode = 0o600) {
    const name = path.join(root, relative);
    fs.mkdirSync(path.dirname(name), { recursive: true });
    fs.writeFileSync(name, bytes, { mode });
    return name;
  }
  const home = path.join(root, "home");
  function release(distribution, version) {
    const payloadRoot = native ? `home/runtime/versions/${version}` : `${distribution}-${version}`;
    const executable = file(`${payloadRoot}/t3`, `${distribution} ${version}\n`, 0o700);
    file(`${payloadRoot}/lib/z.js`, "synthetic library\n");
    file(`${payloadRoot}/lib/a.js`, "synthetic library a\n");
    return {
      distribution,
      version,
      executable,
      argv: [executable, native ? "__service-launcher" : "--no-browser"],
      config: file(`${distribution}-${version}.service`, `[Service]\nExecStart=${executable}\n`),
      payload: { root: path.join(root, payloadRoot) },
    };
  }
  const source = release(sourceDistribution, "1.2.3");
  const target = release(targetDistribution, "4.5.6");
  const request = {
    schema: "server-swap-plan/1",
    id: "fixture-preparation",
    host: "synthetic-host",
    uid: 1000,
    platform: "linux",
    arch: "x64",
    home,
    stateDir: path.join(root, "operations"),
    environmentId: "synthetic-environment",
    service: {
      manager: "systemd-user",
      name: "fixture.service",
      configPath: file("installed.service", fs.readFileSync(source.config)),
    },
    source,
    target,
    descriptorUrl: "http://127.0.0.1:7777/.well-known/t3/environment",
    stateFiles: ["userdata/extra.json"],
  };
  if (native)
    request.runtimeState = {
      source: file(
        "source-state.json",
        JSON.stringify({ protocol: 4, activeVersion: source.version }),
      ),
      target: file(
        "target-state.json",
        JSON.stringify({ protocol: 4, activeVersion: target.version }),
      ),
    };
  const requestPath = path.join(root, "request.json");
  const preparationPath = path.join(root, "preparation.json");
  const qualificationPath = path.join(root, "qualification.json");
  const planPath = path.join(root, "plan.json");
  function writeRequest() {
    fs.writeFileSync(requestPath, JSON.stringify(request));
  }
  function prepare() {
    writeRequest();
    return P.main(["prepare", requestPath, preparationPath]);
  }
  function qualify(change = () => {}) {
    const preparation = F.readJson(preparationPath);
    const evidencePath = file(
      "evidence.txt",
      "External synthetic fixture evidence; not an operational qualification.\n",
    );
    const evidence = { path: evidencePath, sha256: F.hashFile(evidencePath) };
    const qualification = {
      schema: "server-swap-qualification/1",
      bindingDigest: preparation.bindingDigest,
    };
    for (const group of ["migrations", "events", "settings", "clients"]) {
      qualification[group] = {
        status: "passed",
        summary: "Synthetic external qualification fixture",
        evidence,
      };
    }
    qualification.migrations.requiredAdditionalBytes = 0;
    change(qualification);
    fs.writeFileSync(qualificationPath, JSON.stringify(qualification));
  }
  return {
    root,
    request,
    requestPath,
    preparationPath,
    qualificationPath,
    planPath,
    file,
    writeRequest,
    prepare,
    qualify,
    finalize: () => P.main(["finalize", preparationPath, qualificationPath, planPath]),
  };
}

for (const [source, target] of [
  ["jones", "t3"],
  ["t3", "jones"],
  ["jones", "jones"],
]) {
  test(`prepare and externally qualify ${source} to ${target} without changing installed config`, (t) => {
    const f = fixture(t, source, target);
    const installed = fs.readFileSync(f.request.service.configPath);
    const prepared = f.prepare();
    assert.equal(prepared.status, "prepared");
    assert.equal(prepared.digest, F.hashFile(f.preparationPath));
    const preparation = F.readJson(f.preparationPath);
    assert.equal(preparation.schema, "server-swap-preparation/1");
    assert.equal(preparation.candidatePlan.qualification, undefined);
    assert.equal(fs.existsSync(f.qualificationPath), false);
    assert.equal(fs.existsSync(f.planPath), false);
    assert.throws(() => F.loadPlan(f.preparationPath), /Unsupported plan schema/);
    assert.deepEqual(
      preparation.candidatePlan.target.payload.files.map((entry) => entry.path),
      ["lib/a.js", "lib/z.js", "t3"],
    );
    assert.deepEqual(preparation.candidatePlan.stateFiles, [
      ...F.STATE_FILES,
      "userdata/extra.json",
    ]);
    f.qualify();
    const finalized = f.finalize();
    assert.equal(finalized.status, "plan-created");
    assert.equal(finalized.digest, F.hashFile(f.planPath));
    const plan = F.loadPlan(f.planPath);
    assert.equal(plan.qualification.path, f.qualificationPath);
    assert.equal(F.bindingDigest(plan), prepared.bindingDigest);
    assert.doesNotThrow(() => F.verifyQualification(plan));
    assert.deepEqual(fs.readFileSync(f.request.service.configPath), installed);
    assert.equal(fs.statSync(f.preparationPath).mode & 0o777, 0o600);
    assert.equal(fs.statSync(f.planPath).mode & 0o777, 0o600);
  });
}

for (const scenario of [
  "absent",
  "wrong-binding",
  "missing-group",
  "missing-evidence",
  "changed-evidence",
  "not-passed",
]) {
  test(`finalize rejects ${scenario} qualification and creates no plan`, (t) => {
    const f = fixture(t);
    f.prepare();
    if (scenario !== "absent")
      f.qualify((q) => {
        if (scenario === "wrong-binding") q.bindingDigest = "0".repeat(64);
        if (scenario === "missing-group") delete q.clients;
        if (scenario === "missing-evidence")
          q.events.evidence.path = path.join(f.root, "missing-evidence");
        if (scenario === "changed-evidence") fs.appendFileSync(q.events.evidence.path, "changed");
        if (scenario === "not-passed") q.settings.status = "unavailable";
      });
    assert.throws(f.finalize);
    assert.equal(fs.existsSync(f.planPath), false);
  });
}

for (const scenario of [
  "config",
  "executable",
  "added-payload",
  "removed-payload",
  "runtime-state",
  "fragment",
  "drop-in",
]) {
  test(`finalize rejects prepared ${scenario} drift without recomputing the binding`, (t) => {
    const f = fixture(t, "jones", "t3", scenario === "runtime-state");
    if (scenario === "fragment" || scenario === "drop-in") {
      f.request.service.fragment = f.file("base.service", "[Service]\nKillMode=control-group\n");
      f.request.service.configPath = f.file(
        "service.d/selected.conf",
        fs.readFileSync(f.request.source.config),
      );
      f.request.service.dropIns = [f.request.service.configPath];
    }
    f.prepare();
    f.qualify();
    const before = fs.readFileSync(f.preparationPath);
    if (scenario === "config") fs.appendFileSync(f.request.target.config, "changed");
    if (scenario === "executable") fs.appendFileSync(f.request.target.executable, "changed");
    if (scenario === "added-payload")
      fs.writeFileSync(path.join(f.request.target.payload.root, "extra"), "changed");
    if (scenario === "removed-payload")
      fs.unlinkSync(path.join(f.request.target.payload.root, "lib/a.js"));
    if (scenario === "runtime-state") fs.appendFileSync(f.request.runtimeState.target, "changed");
    if (scenario === "fragment") fs.appendFileSync(f.request.service.fragment, "changed");
    if (scenario === "drop-in") fs.appendFileSync(f.request.service.configPath, "changed");
    assert.throws(f.finalize, /digest mismatch|inventory differs/);
    assert.equal(fs.existsSync(f.planPath), false);
    assert.deepEqual(fs.readFileSync(f.preparationPath), before);
  });
}

for (const scenario of ["config", "payload", "payload-root", "request"]) {
  test(`prepare rejects a symlink ${scenario} without output`, (t) => {
    const f = fixture(t);
    const link = path.join(f.root, "link");
    if (scenario === "config") {
      fs.symlinkSync(f.request.target.config, link);
      f.request.target.config = link;
    }
    if (scenario === "payload")
      fs.symlinkSync(f.request.source.config, path.join(f.request.target.payload.root, "alias"));
    if (scenario === "payload-root") {
      fs.symlinkSync(f.request.target.payload.root, link);
      f.request.target.payload.root = link;
    }
    f.writeRequest();
    if (scenario === "request") fs.symlinkSync(f.requestPath, link);
    assert.throws(
      () => P.prepare(scenario === "request" ? link : f.requestPath, f.preparationPath),
      /canonical|symlink/,
    );
    assert.equal(fs.existsSync(f.preparationPath), false);
  });
}

test("prepare and finalize never overwrite an existing output", (t) => {
  const f = fixture(t);
  f.prepare();
  f.qualify();
  f.finalize();
  const preparation = fs.readFileSync(f.preparationPath);
  const plan = fs.readFileSync(f.planPath);
  assert.throws(f.prepare, /EEXIST/);
  assert.throws(f.finalize, /EEXIST/);
  assert.deepEqual(fs.readFileSync(f.preparationPath), preparation);
  assert.deepEqual(fs.readFileSync(f.planPath), plan);
});

test("changed preparation binding and output in the state home are rejected", (t) => {
  const f = fixture(t);
  f.prepare();
  f.qualify();
  const preparation = F.readJson(f.preparationPath);
  preparation.candidatePlan.target.version = "9.9.9";
  fs.writeFileSync(f.preparationPath, JSON.stringify(preparation));
  assert.throws(f.finalize, /binding digest mismatch/);
  assert.equal(fs.existsSync(f.planPath), false);
  fs.mkdirSync(f.request.home);
  const output = path.join(f.request.home, "preparation.json");
  assert.throws(() => P.prepare(f.requestPath, output), /outside state home/);
  assert.equal(fs.existsSync(output), false);
});

test("prepare rejects supplied qualification and main rejects malformed arguments", (t) => {
  const f = fixture(t);
  f.request.qualification = { path: f.qualificationPath, sha256: "0".repeat(64) };
  assert.throws(f.prepare, /separately at finalize/);
  assert.equal(fs.existsSync(f.preparationPath), false);
  assert.throws(() => P.main(["prepare", f.requestPath]), /Expected prepare/);
});

test("separate staged native runtime metadata survives prepare and finalize", (t) => {
  const f = fixture(t, "jones", "t3", true);
  f.prepare();
  f.qualify();
  f.finalize();
  const plan = F.loadPlan(f.planPath);
  assert.doesNotThrow(() => F.verifyRuntimeState(plan));
  assert.equal(plan.runtimeState.source.path, f.request.runtimeState.source);
  assert.equal(fs.existsSync(path.join(f.request.home, "runtime/service-state.json")), false);
});

test("native same-version payload reuse remains unsupported", (t) => {
  const f = fixture(t, "jones", "t3", true);
  f.request.target = { ...f.request.source, distribution: "t3" };
  assert.throws(f.prepare, /disjoint/);
  assert.equal(fs.existsSync(f.preparationPath), false);
});

test("missing installed configuration cannot be used as preparation output", (t) => {
  const f = fixture(t);
  fs.unlinkSync(f.request.service.configPath);
  f.writeRequest();
  assert.throws(
    () => P.prepare(f.requestPath, f.request.service.configPath),
    /installed service configuration/,
  );
  assert.equal(fs.existsSync(f.request.service.configPath), false);
});
