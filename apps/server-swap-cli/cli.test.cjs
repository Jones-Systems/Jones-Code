"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");
const F = require("./files.cjs");

test("CLI prepares, finalizes, and runs a synthetic two-host fleet through the executable entry point", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "server-swap-cli-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  function file(name, content, mode = 0o600) {
    const filename = path.join(root, name);
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, content, { mode });
    return filename;
  }
  function release(distribution, version) {
    const executable = file(`${distribution}/t3`, "synthetic executable", 0o700);
    return {
      distribution,
      version,
      executable,
      argv: [executable],
      config: file(`${distribution}.service`, "[Service]\n"),
      payload: { root: path.dirname(executable) },
    };
  }
  const request = file(
    "request.json",
    JSON.stringify({
      schema: "server-swap-plan/1",
      id: "cli-fixture",
      host: "fixture-host",
      uid: 1000,
      platform: "linux",
      arch: "x64",
      home: path.join(root, "private-home"),
      stateDir: path.join(root, "operations"),
      environmentId: "fixture-environment",
      descriptorUrl: "http://127.0.0.1:3773/.well-known/t3/environment",
      service: {
        manager: "systemd-user",
        name: "fixture.service",
        configPath: path.join(root, "installed.service"),
      },
      source: release("jones", "1.0.0"),
      target: release("t3", "2.0.0"),
    }),
  );
  const env = { ...process.env, PATH: `${root}/bin:/usr/bin:/bin`, SWAP_FIXTURE: root };
  function run(args, expected = 0) {
    const binary = process.env.SERVER_SWAP_TEST_BINARY;
    const result = spawnSync(
      binary || process.execPath,
      binary ? args : [path.join(__dirname, "cli.cjs"), ...args],
      {
        env,
        encoding: "utf8",
        timeout: 10000,
      },
    );
    assert.equal(result.status, expected, result.stderr || result.error?.message);
    return JSON.parse(expected === 1 ? result.stderr : result.stdout);
  }
  assert.ok(run(["--help"]).usage.some((value) => value.includes("fleet run")));
  const preparedPath = path.join(root, "preparation.json");
  const prepared = run(["prepare", request, preparedPath]);
  assert.equal(prepared.status, "prepared");
  const evidencePath = file(
    "evidence.txt",
    "Synthetic external compatibility evidence for this test only",
  );
  const qualification = {
    schema: "server-swap-qualification/1",
    bindingDigest: prepared.bindingDigest,
  };
  for (const group of ["migrations", "events", "settings", "clients"]) {
    qualification[group] = {
      status: "passed",
      summary: "Synthetic fixture",
      evidence: { path: evidencePath, sha256: F.hashFile(evidencePath) },
    };
  }
  const qualificationPath = file("qualification.json", JSON.stringify(qualification));
  const planPath = path.join(root, "plan.json");
  const finalized = run(["finalize", preparedPath, qualificationPath, planPath]);
  assert.equal(finalized.digest, F.hashFile(planPath));
  assert.equal(run(["finalize", preparedPath, qualificationPath, planPath], 1).status, "error");
  env.SWAP_DIGEST = finalized.digest;
  file(
    "bin/ssh",
    `#!/bin/sh
case "$*" in *fixture-test*) marker="$SWAP_FIXTURE/test-started";; *) marker="$SWAP_FIXTURE/mini-started";; esac
case "$*" in
  *"'worker' 'launch'"*) : > "$marker"; status=launched;;
  *) if [ -f "$marker" ]; then status=committed; else status=not-started; fi;;
esac
printf '{"status":"%s","planDigest":"%s"}\\n' "$status" "$SWAP_DIGEST"
`,
    0o700,
  );
  const manifest = file(
    "fleet.json",
    JSON.stringify({
      schema: "server-swap-fleet/v1",
      targets: ["test", "mini"].map((id) => ({
        id,
        ssh: `fixture-${id}`,
        worker: "/fixture/server-swap",
        plan: "/fixture/plan.json",
        planDigest: finalized.digest,
      })),
    }),
  );
  const fleet = run(["fleet", "run", manifest, "5000", "100"]);
  assert.equal(fleet.status, "committed");
  assert.equal(fleet.targets.length, 2);
  assert.match(fleet.laptop, /manually/);
  assert.ok(fs.existsSync(path.join(root, "test-started")));
  assert.ok(fs.existsSync(path.join(root, "mini-started")));
  env.SWAP_DIGEST = "0".repeat(64);
  assert.equal(run(["fleet", "run", manifest, "5000", "100"], 2).status, "unknown");
  assert.equal(fs.existsSync(path.join(root, "private-home")), false);
  assert.equal(fs.existsSync(path.join(root, "installed.service")), false);
});
