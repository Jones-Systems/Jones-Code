import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  DIAGNOSTIC_LIMIT,
  captureProcess,
  captureSyntheticLogs,
  probeReadiness,
  writeStartupDiagnostics,
} from "./startup-diagnostics.mjs";

function childFixture() {
  return Object.assign(new NodeEvents.EventEmitter(), {
    stdout: new NodeEvents.EventEmitter(),
    stderr: new NodeEvents.EventEmitter(),
    exitCode: null,
    signalCode: null,
  });
}

NodeTest.test("process output retains only each stream's last MiB and records exit/errors", () => {
  const child = childFixture();
  const capture = captureProcess(child);
  try {
    child.stdout.emit("data", Buffer.alloc(DIAGNOSTIC_LIMIT + 20, 65));
    child.stdout.emit("data", Buffer.from("tail"));
    child.stderr.emit("data", Buffer.from("separate stderr"));
    child.stderr.emit("error", new Error("stream error"));
    child.emit("error", new Error("child error"));
    child.emit("exit", 17, null);
    const value = capture.snapshot();
    NodeAssert.equal(value.prelaunchOutput, "unavailable");
    NodeAssert.equal(value.streams.stdout.bytes.length, DIAGNOSTIC_LIMIT);
    NodeAssert.equal(value.streams.stdout.bytes.subarray(-4).toString(), "tail");
    NodeAssert.equal(value.streams.stdout.totalBytes, DIAGNOSTIC_LIMIT + 24);
    NodeAssert.equal(value.streams.stdout.truncated, true);
    NodeAssert.equal(value.streams.stderr.bytes.toString(), "separate stderr");
    NodeAssert.equal(value.streams.stderr.truncated, false);
    NodeAssert.equal(value.streams.stderr.error, "stream error");
    NodeAssert.equal(value.error, "child error");
    NodeAssert.deepEqual(value.exit, { code: 17, signal: null });
    NodeAssert.equal(value.exitObserved, true);
  } finally {
    capture.dispose();
  }
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.stdout.listenerCount("data"), 0);
});

NodeTest.test("missing process pipes remain explicitly unavailable", () => {
  const capture = captureProcess(null);
  NodeAssert.equal(capture.snapshot().streams.stdout.status, "unavailable");
  NodeAssert.equal(capture.snapshot().streams.stderr.retainedBytes, 0);
  capture.dispose();
});

async function fixture(fn) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-startup-diagnostics-"));
  try {
    await fn(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

NodeTest.test("only the two synthetic log tails survive scratch cleanup with size metadata", () =>
  fixture(async (root) => {
    const scratch = NodePath.join(root, "scratch");
    const logs = NodePath.join(scratch, "t3home/userdata/logs");
    const artifacts = NodePath.join(root, "artifacts");
    await NodeFSP.mkdir(logs, { recursive: true });
    await NodeFSP.mkdir(artifacts);
    await NodeFSP.writeFile(
      NodePath.join(logs, "desktop.trace.ndjson"),
      Buffer.concat([Buffer.alloc(DIAGNOSTIC_LIMIT, 65), Buffer.from("tail")]),
    );
    await NodeFSP.writeFile(NodePath.join(logs, "ambient.log"), "must not collect");
    const captured = await captureSyntheticLogs(scratch);
    NodeAssert.deepEqual(Object.keys(captured), ["desktop.trace.ndjson", "server-child.log"]);
    NodeAssert.equal(captured["desktop.trace.ndjson"].sizeBytes, DIAGNOSTIC_LIMIT + 4);
    NodeAssert.equal(captured["desktop.trace.ndjson"].retainedBytes, DIAGNOSTIC_LIMIT);
    NodeAssert.equal(captured["desktop.trace.ndjson"].bytes.subarray(-4).toString(), "tail");
    NodeAssert.equal(captured["server-child.log"].status, "missing");
    const child = childFixture();
    const processCapture = captureProcess(child);
    try {
      child.stdout.emit("data", Buffer.from("startup output"));
      await writeStartupDiagnostics({
        artifacts,
        processCapture,
        logs: captured,
        readiness: { status: "timed-out" },
        phase: "first-window",
      });
    } finally {
      processCapture.dispose();
    }
    await NodeFSP.rm(scratch, { recursive: true });
    const report = JSON.parse(
      await NodeFSP.readFile(NodePath.join(artifacts, "startup-diagnostics.json")),
    );
    NodeAssert.equal(report.logs["desktop.trace.ndjson"].truncated, true);
    NodeAssert.equal(report.logs["server-child.log"].artifact, null);
    NodeAssert.equal(report.process.streams.stdout.bytes, undefined);
    NodeAssert.equal(
      await NodeFSP.readFile(
        NodePath.join(artifacts, report.process.streams.stdout.artifact),
        "utf8",
      ),
      "startup output",
    );
    NodeAssert.equal(
      (await NodeFSP.stat(NodePath.join(artifacts, report.logs["desktop.trace.ndjson"].artifact)))
        .size,
      DIAGNOSTIC_LIMIT,
    );
    NodeAssert.equal(
      (await NodeFSP.stat(NodePath.join(artifacts, "startup-diagnostics.json"))).mode & 0o777,
      0o600,
    );
  }),
);

NodeTest.test("synthetic log symlinks are rejected without following them", () =>
  fixture(async (root) => {
    const logs = NodePath.join(root, "t3home/userdata/logs");
    await NodeFSP.mkdir(logs, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(root, "other"), "not a log");
    await NodeFSP.symlink(NodePath.join(root, "other"), NodePath.join(logs, "server-child.log"));
    NodeAssert.equal((await captureSyntheticLogs(root))["server-child.log"].status, "unavailable");
    await NodeFSP.rm(logs, { recursive: true });
    await NodeFSP.symlink(root, logs);
    NodeAssert.equal((await captureSyntheticLogs(root))["server-child.log"].status, "unavailable");
  }),
);

NodeTest.test("readiness probe returns only readiness and bounded BrowserWindow IDs", async () => {
  const value = await probeReadiness({
    evaluate: async (fn) =>
      fn({
        app: { isReady: () => true },
        BrowserWindow: { getAllWindows: () => Array.from({ length: 101 }, (_, id) => ({ id })) },
      }),
  });
  NodeAssert.equal(value.ready, true);
  NodeAssert.equal(value.windowCount, 101);
  NodeAssert.equal(value.windowIds.length, 100);
  NodeAssert.equal(value.windowIdsTruncated, true);
  NodeAssert.deepEqual(
    await probeReadiness({
      evaluate: async () => {
        throw new Error("closed");
      },
    }),
    { status: "unavailable", error: "closed" },
  );
});

NodeTest.test("an unresponsive evaluation has a one-second best-effort deadline", async () => {
  const value = await probeReadiness({ evaluate: () => new Promise(() => {}) });
  NodeAssert.deepEqual(value, { status: "timed-out", timeoutMs: 1000 });
});
