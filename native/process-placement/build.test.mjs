import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeCrypto from "node:crypto";
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
