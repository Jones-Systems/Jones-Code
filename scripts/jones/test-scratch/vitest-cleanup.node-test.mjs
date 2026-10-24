import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

const require = NodeModule.createRequire(import.meta.url);
const vitePlus = require.resolve("vite-plus/package.json");
const vitestPackage = NodeModule.createRequire(vitePlus).resolve("vitest/package.json");
const storage = NodePath.resolve(import.meta.dirname, "../../../node_modules");
let interrupted = false;

for (const mode of ["pass", "fail"]) {
  NodeTest.test(`installed Vitest removes temporary modules after ${mode}`, async (t) => {
    NodeAssert.equal(interrupted, false, "Earlier fixture was cancelled");
    const filesystem = NodeFS.statfsSync(storage);
    NodeAssert.ok(
      ![0x01021994, 0x858458f6].includes(filesystem.type),
      "Regression fixtures require disk-backed workspace storage, not tmpfs/ramfs",
    );
    const root = NodeFS.mkdtempSync(NodePath.join(storage, ".jones-vitest-cleanup-"));
    NodeFS.chmodSync(root, 0o700);
    let child;
    let exited = false;
    let primary;
    let cancelled = false;
    let timer;
    const cancel = () => {
      cancelled = true;
      interrupted = true;
      if (child?.connected) child.send("cancel");
    };
    process.on("SIGINT", cancel);
    process.on("SIGTERM", cancel);
    try {
      const scratch = NodePath.join(root, "tmp");
      NodeFS.mkdirSync(scratch);
      child = NodeChildProcess.fork(
        new URL("./vitest-cleanup-worker.mjs", import.meta.url),
        [root, vitestPackage, mode],
        {
          env: { ...process.env, TMPDIR: scratch, TMP: scratch, TEMP: scratch },
          stdio: ["ignore", "pipe", "pipe", "ipc"],
        },
      );
      let output = "";
      for (const stream of [child.stdout, child.stderr]) {
        stream.on("data", (chunk) => {
          output = (output + chunk).slice(-8192);
        });
      }
      const result = await new Promise((resolve, reject) => {
        child.once("error", reject);
        child.once("close", (code, signal) => {
          exited = true;
          resolve({ code, signal });
        });
        timer = setTimeout(() => {
          cancel();
          timer = setTimeout(() => {
            child.unref();
            child.stdout.destroy();
            child.stderr.destroy();
            if (child.connected) child.disconnect();
            reject(new Error(`Vitest did not exit after cancellation; retained ${root}`));
          }, 10_000);
        }, 60_000);
      });
      NodeAssert.equal(cancelled, false, `Regression cancelled: ${output}`);
      NodeAssert.equal(result.signal, null, output);
      NodeAssert.equal(result.code, mode === "pass" ? 0 : 1, output);
      const active = JSON.parse(NodeFS.readFileSync(NodePath.join(root, "active.json"), "utf8"));
      const closed = JSON.parse(NodeFS.readFileSync(NodePath.join(root, "closed.json"), "utf8"));
      const afterExit = NodeFS.readdirSync(scratch);
      t.diagnostic(
        `Vitest ${closed.version}, ${mode}: ${active.length} active transformed SHA1 files; ${closed.remaining.length} roots after close; ${afterExit.length} roots after exit`,
      );
      NodeAssert.ok(
        active.length > 0,
        "Must exercise temporary module allocation before asserting cleanup",
      );
      NodeAssert.equal(closed.closed, true, "Vitest context must close before cleanup is checked");
      NodeAssert.deepEqual(
        { afterClose: closed.remaining, afterExit },
        { afterClose: [], afterExit: [] },
        "Temporary module roots remain after context.close() or child exit",
      );
    } catch (error) {
      primary = error;
    } finally {
      clearTimeout(timer);
      process.off("SIGINT", cancel);
      process.off("SIGTERM", cancel);
      try {
        NodeAssert.ok(!child || exited, `Child liveness unknown; retained ${root}`);
        if (child) {
          const closed = JSON.parse(
            NodeFS.readFileSync(NodePath.join(root, "closed.json"), "utf8"),
          );
          NodeAssert.equal(closed.closed, true, `Consumers not proved closed; retained ${root}`);
        }
        NodeFS.rmSync(root, { recursive: true, force: true });
        NodeAssert.equal(NodeFS.existsSync(root), false);
      } catch (cleanupError) {
        primary = primary
          ? new AggregateError([primary, cleanupError], "Regression failed; cleanup also failed")
          : cleanupError;
      }
    }
    if (primary) throw primary;
  });
}
