import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

const workflow = NodeFS.readFileSync(
  new URL("../../../.github/workflows/jones-macos-dependency-cache.yml", import.meta.url),
  "utf8",
);
const shell = workflow.split("        run: |\n")[1];
NodeAssert.ok(shell);

const qualifiedHead = "a".repeat(40);
const lock = "lockfileVersion: '9.0'\n";
const digest = NodeCrypto.createHash("sha256")
  .update(NodeCrypto.createHash("sha256").update(lock).digest())
  .digest("hex");
const ref = "refs/heads/main";
const prefix = "vite-plus-macOS-arm64-pnpm-";
const old = { id: 1, key: prefix + "b".repeat(64), ref, size_in_bytes: 100 };
const current = { id: 2, key: prefix + digest, ref, size_in_bytes: 100 };
const unrelated = [
  { ...old, id: 3, ref: "refs/pull/1162/merge" },
  { ...old, id: 4, key: old.key.replace("macOS-arm64", "Linux-x64") },
  { ...old, id: 5, key: prefix + "unrecognized" },
];

function runRotation(
  t,
  { archives = [old, current, ...unrelated], advanced = false, lost = false } = {},
) {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-cache-rotation-"));
  t.after(() => NodeFS.rmSync(dir, { recursive: true, force: true }));
  const deletes = NodePath.join(dir, "deletes");
  NodeFS.writeFileSync(deletes, "");
  NodeFS.writeFileSync(
    NodePath.join(dir, "gh"),
    `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
const endpoint = args.find((arg) => arg.startsWith("repos/"));
const fixture = JSON.parse(process.env.ROTATION_FIXTURE);
if (endpoint.endsWith("/git/ref/heads/main")) {
  console.log(fixture.advanced ? "c".repeat(40) : process.env.QUALIFIED_HEAD);
} else if (endpoint.endsWith("/contents/pnpm-lock.yaml?ref=" + process.env.QUALIFIED_HEAD)) {
  process.stdout.write(fixture.lock);
} else if (endpoint.endsWith("/actions/caches") && args.includes("--slurp")) {
  console.log(JSON.stringify([{ actions_caches: fixture.archives }]));
} else if (endpoint.endsWith("/actions/caches")) {
  console.log(JSON.stringify({ actions_caches: fixture.lost ? [] : fixture.archives }));
} else if (args.includes("DELETE") && /\\/actions\\/caches\\/\\d+$/.test(endpoint)) {
  fs.appendFileSync(fixture.deletes, endpoint.split("/").at(-1) + "\\n");
} else {
  process.exit(2);
}
`,
    { mode: 0o700 },
  );
  const run = NodeChildProcess.spawnSync("bash", ["-c", shell], {
    encoding: "utf8",
    env: {
      ...process.env,
      PATH: dir + NodePath.delimiter + process.env.PATH,
      GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
      QUALIFIED_HEAD: qualifiedHead,
      ROTATION_FIXTURE: JSON.stringify({ archives, advanced, lost, lock, deletes }),
    },
  });
  return { status: run.status, deleted: NodeFS.readFileSync(deletes, "utf8"), stderr: run.stderr };
}

NodeTest.test("retires a superseded main archive by ID and preserves all other scopes", (t) => {
  const result = runRotation(t);
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.deleted, "1\n");
});

NodeTest.test("retains old archives until a current main archive exists", (t) => {
  const result = runRotation(t, { archives: [old, ...unrelated] });
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.deleted, "");
});

NodeTest.test("preserves archives on an outdated CI head", (t) => {
  const result = runRotation(t, { advanced: true });
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.deleted, "");
});

NodeTest.test("stops before deletion if the replacement disappears", (t) => {
  const result = runRotation(t, { lost: true });
  NodeAssert.equal(result.status, 1, result.stderr);
  NodeAssert.equal(result.deleted, "");
});
