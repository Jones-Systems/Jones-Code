import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeOS from "node:os";
import { buildPlacement } from "./build.mjs";

async function fixture(action) {
  const root = NodeFS.mkdtempSync(
    NodePath.join(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), ".build-test-"),
  );
  try {
    const source = NodePath.join(root, "input.c");
    const compiler = NodePath.join(root, "compiler.mjs");
    NodeFS.writeFileSync(source, "captured native source\n");
    NodeFS.writeFileSync(
      compiler,
      `import {writeFileSync} from 'node:fs';
const args=process.argv.slice(2); const mode=args.shift();
if(mode==='fail') process.exit(23);
if(mode==='wait') { process.on('SIGTERM',()=>process.exit(143)); setInterval(()=>{},1000); }
else writeFileSync(args[args.indexOf('-o')+1], 'compiled fixture');\n`,
    );
    const build = (outputDir, mode = "ok", signal, removeScratch) =>
      buildPlacement({
        platform: "linux",
        architecture: "fixture",
        source,
        outputDir,
        compiler: process.execPath,
        compilerArgs: [compiler, mode],
        signal,
        removeScratch,
      });
    await action({ root, source, build });
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
  NodeAssert.equal(NodeFS.existsSync(root), false);
}
const sha = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");

NodeTest.test("success retains only helper and hash manifest and removes owned build scratch", () =>
  fixture(async ({ root, source, build }) => {
    const output = NodePath.join(root, "output");
    await build(output);
    NodeAssert.deepEqual(NodeFS.readdirSync(output).sort(), [
      "process-placement-linux-fixture",
      "process-placement-linux-fixture.json",
    ]);
    const manifest = JSON.parse(
      NodeFS.readFileSync(NodePath.join(output, "process-placement-linux-fixture.json"), "utf8"),
    );
    NodeAssert.equal(manifest.sourceSha256, sha(NodeFS.readFileSync(source)));
    NodeAssert.equal(
      manifest.helperSha256,
      sha(NodeFS.readFileSync(NodePath.join(output, "process-placement-linux-fixture"))),
    );
  }),
);
NodeTest.test(
  "compiler failure preserves status and prior output while removing build scratch",
  () =>
    fixture(async ({ root, build }) => {
      const output = NodePath.join(root, "output");
      await build(output);
      await NodeAssert.rejects(build(output, "fail"), (cause) => cause.exitCode === 23);
      NodeAssert.equal(NodeFS.readdirSync(output).length, 2);
      NodeAssert.equal(
        NodeFS.readFileSync(NodePath.join(output, "process-placement-linux-fixture"), "utf8"),
        "compiled fixture",
      );
    }),
);
NodeTest.test("normal cancellation reaps the compiler before removing its captured scratch", () =>
  fixture(async ({ root, build }) => {
    const output = NodePath.join(root, "output");
    const controller = new AbortController();
    const pending = build(output, "wait", controller.signal);
    controller.abort();
    await NodeAssert.rejects(pending, (cause) => cause.exitCode === 143);
    NodeAssert.deepEqual(NodeFS.readdirSync(output), []);
  }),
);
NodeTest.test("concurrent build invocations keep separate captured scratch roots", () =>
  fixture(async ({ root, build }) => {
    const one = NodePath.join(root, "one"),
      two = NodePath.join(root, "two");
    await Promise.all([
      build(one),
      build(two, "fail").catch((cause) => NodeAssert.equal(cause.exitCode, 23)),
    ]);
    NodeAssert.equal(NodeFS.readdirSync(one).length, 2);
    NodeAssert.deepEqual(NodeFS.readdirSync(two), []);
  }),
);

NodeTest.test("cleanup failure fails success and does not hide an original compiler failure", () =>
  fixture(async ({ root, build }) => {
    const cleanupFailure = new Error("fixture cleanup failure");
    const removeScratch = () => {
      throw cleanupFailure;
    };
    await NodeAssert.rejects(
      build(NodePath.join(root, "success"), "ok", undefined, removeScratch),
      (cause) => cause === cleanupFailure,
    );
    await NodeAssert.rejects(
      build(NodePath.join(root, "failed"), "fail", undefined, removeScratch),
      (cause) => cause.exitCode === 23 && cause.cleanupError === cleanupFailure,
    );
  }),
);

NodeTest.test(
  "native descendant walk rejects escapes and symlinks without placement writes",
  {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Plain MJS native compiler test has no Effect runtime.
    skip: NodeOS.platform() !== "linux",
  },
  async () =>
    fixture(async ({ root }) => {
      const native = NodeURL.fileURLToPath(new URL("./placement.c", import.meta.url));
      const harness = NodePath.join(root, "descendant-test.c");
      const binary = NodePath.join(root, "descendant-test");
      NodeFS.writeFileSync(
        harness,
        `#define main placement_main
#include ${JSON.stringify(native)}
#undef main
int main(int argc, char **argv) {
  if (argc != 3) return 2;
  const char *suffix = descendant_suffix(argv[1], argv[2]);
  int parent = open_directory(argv[1]);
  int member = open_descendant(parent, suffix);
  close(member); close(parent); return 0;
}
`,
      );
      const compiled = NodeChildProcess.spawnSync(
        "cc",
        ["-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", harness, "-o", binary],
        {
          encoding: "utf8",
          timeout: 30000,
          env: { ...process.env, TMPDIR: root },
        },
      );
      NodeAssert.equal(compiled.status, 0, compiled.stderr);
      const parent = NodePath.join(root, "workload");
      NodeFS.mkdirSync(NodePath.join(parent, "owned", "session.scope"), { recursive: true });
      NodeFS.symlinkSync(NodePath.join(parent, "owned"), NodePath.join(parent, "linked"));
      const procs = NodePath.join(parent, "cgroup.procs");
      NodeFS.writeFileSync(procs, "untouched fixture\n");
      const run = (member) =>
        NodeChildProcess.spawnSync(binary, [parent, member], {
          encoding: "utf8",
          timeout: 5000,
        });
      NodeAssert.equal(run(NodePath.join(parent, "owned", "session.scope")).status, 0);
      for (const member of [
        parent,
        parent + "-x/child",
        parent + "/../workload/owned",
        parent + "/owned/",
        parent + "/owned//session.scope",
        parent + "/missing",
        parent + "/linked/session.scope",
      ]) {
        const result = run(member);
        NodeAssert.equal(result.status, 125, member + ": " + result.stderr);
      }
      NodeAssert.equal(NodeFS.readFileSync(procs, "utf8"), "untouched fixture\n");
    }),
);
