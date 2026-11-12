"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { readHostProcess } = require("./host-process.mjs");
const { verifyHost } = require("./files.cjs");

test("production identity is synchronous and cannot be supplied by its caller", () => {
  const actual = readHostProcess();
  assert.equal(typeof actual.platform, "string");
  assert.equal(typeof actual.arch, "string");
  assert.equal(actual.then, undefined);
  assert.deepEqual(readHostProcess({ platform: "fixture", arch: "fixture" }), actual);
});

test("host identity mismatches throw synchronously before state access", () => {
  const actual = readHostProcess();
  const plan = { ...actual, host: os.hostname(), uid: process.getuid(), home: "/absent-fixture" };
  for (const change of [
    { platform: actual.platform === "linux" ? "darwin" : "linux" },
    { arch: actual.arch === "arm64" ? "x64" : "arm64" },
    { host: "mismatched-fixture-host" },
    { uid: -1 },
  ]) {
    assert.throws(() => verifyHost({ ...plan, ...change }), /exact host, uid, platform and architecture/);
  }
});

test("matching runtime still requires a canonical owned home and exact environment", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "swap-host-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  fs.mkdirSync(path.join(root, "userdata"));
  fs.writeFileSync(path.join(root, "userdata/environment-id"), "synthetic-environment\n");
  const plan = {
    ...readHostProcess(),
    host: os.hostname(),
    uid: process.getuid(),
    home: root,
    environmentId: "synthetic-environment",
  };
  assert.equal(verifyHost(plan), undefined);
  assert.throws(() => verifyHost({ ...plan, environmentId: "other-environment" }), /environment ID mismatch/);
  const alias = path.join(root, "alias");
  fs.symlinkSync(root, alias);
  assert.throws(() => verifyHost({ ...plan, home: alias }), /canonical and owned/);
});
