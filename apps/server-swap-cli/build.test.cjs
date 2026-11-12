"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { builtinModules } = require("node:module");
const { spawnSync } = require("node:child_process");
const { bundleHostProcess, bundle } = require("./build.cjs");
const { readHostProcess } = require("./host-process.mjs");

test("compiled canonical bridge is synchronous, builtin-only and matches source identity", () => {
  const compiled = { exports: {} };
  const builtinOnly = (id) => {
    assert.ok(id.startsWith("node:") && builtinModules.includes(id.slice(5)), id);
    return process.getBuiltinModule(id);
  };
  new Function("module", "exports", "require", bundleHostProcess(__dirname))(
    compiled,
    compiled.exports,
    builtinOnly,
  );
  const actual = compiled.exports.readHostProcess();
  assert.equal(actual.then, undefined);
  assert.deepEqual(actual, readHostProcess());
});

test("existing loader dispatches the complete bundle away from source and dependencies", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "swap-bundle-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const entry = path.join(root, "bundle.cjs");
  fs.writeFileSync(entry, bundle(__dirname), { mode: 0o600 });
  const env = { ...process.env, PATH: root };
  delete env.NODE_PATH;
  delete env.NODE_OPTIONS;
  function run(file) {
    const result = spawnSync(process.execPath, [file, "--help"], {
      cwd: root,
      env,
      encoding: "utf8",
      timeout: 15000,
    });
    assert.equal(result.status, 0, result.stderr || result.error?.message);
    return JSON.parse(result.stdout);
  }
  assert.deepEqual(run(entry), run(path.join(__dirname, "cli.cjs")));
  assert.deepEqual(fs.readdirSync(root), ["bundle.cjs"]);
});
