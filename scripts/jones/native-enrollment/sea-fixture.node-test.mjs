import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

const executable = process.env.JONES_SQLITE_SEA_FIXTURE;
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Set the exact built fixture executable.",
);

NodeTest.test(
  "SEA SQLite semantics and isolated success/failure/cancellation cleanup",
  { timeout: 30000 },
  async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-sea-check-"));
    const env = { PATH: "", HOME: root, TMPDIR: root, TEMP: root };
    let held;
    let closed;
    try {
      held = NodeChildProcess.spawn(executable, ["--hold"], {
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      closed = NodeEvents.once(held, "close");
      await NodeEvents.once(held.stdout, "data", { signal: AbortSignal.timeout(5000) });
      const heldDirectories = NodeFS.readdirSync(root);
      NodeAssert.equal(heldDirectories.length, 1);
      const success = NodeChildProcess.spawnSync(executable, [], {
        env,
        encoding: "utf8",
        timeout: 15000,
      });
      NodeAssert.equal(success.status, 0, success.stderr);
      const receipt = JSON.parse(success.stdout);
      NodeAssert.equal(receipt.identity.sea, true);
      NodeAssert.equal(receipt.identity.node, "26.12.1");
      NodeAssert.equal(receipt.identity.sqlite, "3.54.0");
      NodeAssert.equal(receipt.sqliteChecks, "passed");
      NodeAssert.deepEqual(NodeFS.readdirSync(root), heldDirectories);
      const failure = NodeChildProcess.spawnSync(executable, ["--fail"], {
        env,
        encoding: "utf8",
        timeout: 10000,
      });
      NodeAssert.notEqual(failure.status, 0);
      NodeAssert.match(failure.stderr, /synthetic qualification failure/);
      NodeAssert.deepEqual(NodeFS.readdirSync(root), heldDirectories);
      held.kill("SIGTERM");
      const [code] = await closed;
      NodeAssert.equal(code, 143);
      NodeAssert.deepEqual(NodeFS.readdirSync(root), []);
      process.stdout.write(`[native-enrollment-sea] ${JSON.stringify(receipt)}\n`);
    } finally {
      if (held && held.exitCode === null && held.signalCode === null) held.kill("SIGTERM");
      if (closed) await closed;
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);
