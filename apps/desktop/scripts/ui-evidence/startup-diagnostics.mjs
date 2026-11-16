import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const DIAGNOSTIC_LIMIT = 1024 * 1024;
const logNames = ["desktop.trace.ndjson", "server-child.log"];
const errorText = (error) => String(error?.message ?? error).slice(0, 4096);

function tailBuffer() {
  let bytes = Buffer.alloc(0);
  let totalBytes = 0;
  return {
    append(chunk) {
      const next = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      totalBytes += next.length;
      bytes =
        next.length >= DIAGNOSTIC_LIMIT
          ? Buffer.from(next.subarray(next.length - DIAGNOSTIC_LIMIT))
          : Buffer.concat([
              bytes.subarray(Math.max(0, bytes.length + next.length - DIAGNOSTIC_LIMIT)),
              next,
            ]);
    },
    snapshot() {
      return {
        bytes,
        totalBytes,
        retainedBytes: bytes.length,
        truncated: totalBytes > bytes.length,
      };
    },
  };
}

export function captureProcess(child) {
  const streams = {};
  const listeners = [];
  const state = {
    collection: "after-electron-launch-returned",
    prelaunchOutput: "unavailable",
    exit: child ? { code: child.exitCode ?? null, signal: child.signalCode ?? null } : null,
    exitObserved: false,
    error: null,
  };
  const listen = (target, event, listener) => {
    target.on(event, listener);
    listeners.push(() => target.removeListener(event, listener));
  };
  for (const name of ["stdout", "stderr"]) {
    const tail = tailBuffer();
    streams[name] = { tail, status: child?.[name] ? "capturing" : "unavailable", error: null };
    if (!child?.[name]) continue;
    listen(child[name], "data", (chunk) => tail.append(chunk));
    listen(child[name], "error", (error) => {
      streams[name].status = "error";
      streams[name].error = errorText(error);
    });
  }
  if (child) {
    listen(child, "exit", (code, signal) => {
      state.exit = { code, signal };
      state.exitObserved = true;
    });
    listen(child, "error", (error) => {
      state.error = errorText(error);
    });
  }
  return {
    snapshot() {
      return {
        ...state,
        streams: Object.fromEntries(
          Object.entries(streams).map(([name, stream]) => [
            name,
            {
              status: stream.status,
              error: stream.error,
              ...stream.tail.snapshot(),
            },
          ]),
        ),
      };
    },
    dispose() {
      for (const remove of listeners) remove();
    },
  };
}

export async function probeReadiness(app) {
  let timer;
  try {
    return await Promise.race([
      Promise.resolve()
        .then(() =>
          app.evaluate(({ app, BrowserWindow }) => {
            const windows = BrowserWindow.getAllWindows();
            return {
              ready: app.isReady(),
              windowCount: windows.length,
              windowIds: windows.slice(0, 100).map((window) => window.id),
              windowIdsTruncated: windows.length > 100,
            };
          }),
        )
        .then(
          (value) => ({ status: "observed", ...value }),
          (error) => ({ status: "unavailable", error: errorText(error) }),
        ),
      new Promise((resolve) => {
        timer = setTimeout(() => resolve({ status: "timed-out", timeoutMs: 1000 }), 1000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function captureSyntheticLogs(scratchRoot = "/scratch") {
  const directory = NodePath.join(scratchRoot, "t3home/userdata/logs");
  const results = {};
  for (const name of logNames) {
    let file;
    try {
      // Never follow a redirected parent or file into another scratch resource.
      for (const relative of ["t3home", "t3home/userdata", "t3home/userdata/logs"]) {
        const info = await NodeFSP.lstat(NodePath.join(scratchRoot, relative));
        if (!info.isDirectory()) throw new Error("Synthetic log parent is not a directory");
      }
      file = await NodeFSP.open(
        NodePath.join(directory, name),
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
      );
      const info = await file.stat();
      if (!info.isFile()) throw new Error("Synthetic log is not a regular file");
      const size = Math.min(info.size, DIAGNOSTIC_LIMIT);
      const bytes = Buffer.alloc(size);
      const { bytesRead } = await file.read(bytes, 0, size, info.size - size);
      results[name] = {
        status: "captured",
        sizeBytes: info.size,
        retainedBytes: bytesRead,
        truncated: info.size > bytesRead,
        bytes: bytes.subarray(0, bytesRead),
      };
    } catch (error) {
      results[name] = {
        status: error.code === "ENOENT" ? "missing" : "unavailable",
        sizeBytes: null,
        retainedBytes: 0,
        truncated: null,
        error: errorText(error),
      };
    } finally {
      try {
        await file?.close();
      } catch (error) {
        results[name].closeError = errorText(error);
      }
    }
  }
  return results;
}

export async function writeStartupDiagnostics({
  artifacts,
  processCapture,
  logs,
  readiness,
  phase,
}) {
  const snapshot = processCapture?.snapshot() ?? {
    collection: "launch-did-not-return",
    prelaunchOutput: "unavailable",
    streams: {},
  };
  const report = {
    schema: "jones-code-startup-diagnostics/v1",
    limitBytesPerStreamOrLog: DIAGNOSTIC_LIMIT,
    phase,
    readiness,
    process: snapshot,
    logs: {},
  };
  for (const [name, entry] of [...Object.entries(snapshot.streams), ...Object.entries(logs)]) {
    const { bytes, ...metadata } = entry;
    const artifact = bytes ? `startup-${name}.log` : null;
    if (artifact)
      await NodeFSP.writeFile(NodePath.join(artifacts, artifact), bytes, {
        mode: 0o600,
        flag: "wx",
      });
    if (name === "stdout" || name === "stderr") snapshot.streams[name] = { ...metadata, artifact };
    else report.logs[name] = { ...metadata, artifact };
  }
  await NodeFSP.writeFile(
    NodePath.join(artifacts, "startup-diagnostics.json"),
    JSON.stringify(report, null, 2),
    { mode: 0o600, flag: "wx" },
  );
  return "startup-diagnostics.json";
}
