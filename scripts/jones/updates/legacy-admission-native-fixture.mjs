import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const source = NodeURL.fileURLToPath(new URL("./legacy-admission-native.c", import.meta.url));
const LIMIT = 1024 * 1024;

function abortIfRequested(signal) {
  if (signal?.aborted) throw signal.reason ?? new Error("Native admission fixture cancelled.");
}

function identity(info) {
  return { device: String(info.dev), inode: String(info.ino), size: String(info.size) };
}

async function inspect(handle) {
  const before = await handle.stat({ bigint: true });
  NodeAssert.ok(before.isFile() && before.nlink === 1n && before.size <= 4n * 1024n * 1024n);
  NodeAssert.equal(before.uid, BigInt(process.getuid()));
  const bytes = Buffer.alloc(Number(before.size));
  const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
  NodeAssert.equal(bytesRead, bytes.length);
  const after = await handle.stat({ bigint: true });
  NodeAssert.deepEqual(identity(after), identity(before));
  NodeAssert.equal(after.mtimeNs, before.mtimeNs);
  NodeAssert.equal(after.ctimeNs, before.ctimeNs);
  return {
    ...identity(after),
    sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
    mode: Number(after.mode & 0o777n),
  };
}

async function syncDirectory(path) {
  const handle = await NodeFSP.open(path, NodeFS.constants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

// This fixture-scoped gate is intentionally not an installed admission capability.
export async function gateFixtureExecutable(executable) {
  const handle = await NodeFSP.open(
    executable,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  const journal = `${executable}.gate.json`;
  try {
    const original = await inspect(handle);
    NodeAssert.ok(original.mode & 0o111);
    const record = { schema: 1, executable, original, gatedMode: original.mode & ~0o111 };
    const file = await NodeFSP.open(journal, "wx", 0o600);
    try {
      await file.writeFile(`${JSON.stringify(record)}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    await syncDirectory(NodePath.dirname(journal));
    await handle.chmod(record.gatedMode);
    await handle.sync();
    NodeAssert.deepEqual(await inspect(handle), { ...original, mode: record.gatedMode });
    return journal;
  } finally {
    await handle.close();
  }
}

export async function recoverFixtureExecutable(executable) {
  const journal = `${executable}.gate.json`;
  const journalHandle = await NodeFSP.open(
    journal,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  let record;
  try {
    const info = await journalHandle.stat();
    NodeAssert.ok(info.isFile() && info.size < 4096 && info.nlink === 1);
    record = JSON.parse(await journalHandle.readFile("utf8"));
  } finally {
    await journalHandle.close();
  }
  NodeAssert.equal(record.schema, 1);
  NodeAssert.equal(record.executable, executable);
  NodeAssert.equal(record.gatedMode, record.original.mode & ~0o111);
  NodeAssert.ok(
    Number.isInteger(record.original.mode) &&
      record.original.mode > 0 &&
      record.original.mode <= 0o777,
  );
  const handle = await NodeFSP.open(
    executable,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const current = await inspect(handle);
    const expected = { ...record.original, mode: current.mode };
    NodeAssert.deepEqual(
      current,
      expected,
      "Executable identity or content changed; mode restoration refused.",
    );
    NodeAssert.ok(
      current.mode === record.gatedMode || current.mode === record.original.mode,
      "Executable mode changed outside the recorded gate; restoration refused.",
    );
    await handle.chmod(record.original.mode);
    await handle.sync();
    NodeAssert.deepEqual(await inspect(handle), record.original);
    return {
      status: "restored",
      originalMode: record.original.mode,
      identity: identity(await handle.stat()),
    };
  } finally {
    await handle.close();
  }
}

export async function withNativeAdmissionScratch(
  body,
  { temporaryDirectory = NodeOS.tmpdir(), signal } = {},
) {
  abortIfRequested(signal);
  const root = await NodeFSP.mkdtemp(NodePath.join(temporaryDirectory, "jones-native-admission-"));
  const owned = await NodeFSP.lstat(root, { bigint: true });
  let failure;
  let result;
  try {
    result = await body(root);
  } catch (error) {
    failure = error;
  }
  if (failure?.preserveScratch) {
    failure.scratchRoot = root;
    throw failure;
  }
  try {
    const current = await NodeFSP.lstat(root, { bigint: true });
    NodeAssert.equal(current.dev, owned.dev);
    NodeAssert.equal(current.ino, owned.ino);
    NodeAssert.ok(current.isDirectory());
    await NodeFSP.rm(root, { recursive: true });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  } catch (cleanup) {
    if (failure) throw new AggregateError([failure, cleanup], failure.message, { cause: failure });
    throw cleanup;
  }
  if (failure) throw failure;
  return result;
}

async function runOwned(command, args, { root, signal, onLine, nativeCustodian = false } = {}) {
  abortIfRequested(signal);
  const child = NodeChildProcess.spawn(command, args, {
    cwd: root,
    env: {
      PATH: "/usr/bin:/bin",
      HOME: root,
      TMPDIR: root,
      CLANG_MODULE_CACHE_PATH: NodePath.join(root, "clang-cache"),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let pending = "";
  let failure;
  let sequence = Promise.resolve();
  const stop = (error) => {
    failure ??= error;
    child.stdin.end();
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  };
  const cancelled = () => stop(signal.reason ?? new Error("Native admission fixture cancelled."));
  signal?.addEventListener("abort", cancelled, { once: true });
  const timer = setTimeout(
    () => stop(new Error("Owned native fixture exceeded 30 seconds.")),
    30_000,
  );
  child.stdin.on("error", (error) => {
    failure ??= error;
  });
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
    pending += chunk;
    if (stdout.length > LIMIT) return stop(new Error("Native fixture output exceeded its bound."));
    let split;
    while ((split = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, split);
      pending = pending.slice(split + 1);
      if (onLine) sequence = sequence.then(() => onLine(line, child)).catch(stop);
    }
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
    if (stderr.length > LIMIT) stop(new Error("Native fixture diagnostics exceeded their bound."));
  });
  try {
    const result = await new Promise((resolve) => {
      child.once("error", (error) => {
        failure ??= error;
      });
      child.once("close", (code, exitSignal) => resolve({ code, signal: exitSignal }));
    });
    await sequence;
    if (nativeCustodian && (result.signal !== null || result.code === 2)) {
      failure ??= new Error("Native child custody ended without proved cleanup.");
      failure.preserveScratch = true;
    }
    if (failure) throw failure;
    if (result.code !== 0)
      throw new Error(
        `Owned native command failed (${result.code}, ${result.signal}): ${stderr.trim()}`,
      );
    return { ...result, stdout, stderr };
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancelled);
  }
}

export async function runLegacyAdmissionNativeFixture({
  signal,
  temporaryDirectory,
  onSuspended,
} = {}) {
  abortIfRequested(signal);
  if (process.platform !== "darwin")
    return {
      schema: 1,
      status: "unsupported",
      reason: "darwin-required",
      platform: process.platform,
      activationAllowed: false,
      productionCustodyQualified: false,
    };
  const started = performance.now();
  const scenarios = await withNativeAdmissionScratch(
    async (root) => {
      const controller = NodePath.join(root, "controller");
      await runOwned(
        "/usr/bin/clang",
        ["-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", controller],
        { root, signal },
      );
      const outcomes = [];
      for (const command of ["resume", "drain", "missing-custody"]) {
        abortIfRequested(signal);
        const executable = NodePath.join(root, `writer-${command}`);
        const marker = NodePath.join(root, `write-${command}`);
        await NodeFSP.copyFile(controller, executable, NodeFS.constants.COPYFILE_EXCL);
        await NodeFSP.chmod(executable, 0o700);
        const events = [];
        await runOwned(controller, [executable, marker], {
          root,
          signal,
          nativeCustodian: true,
          onLine: async (line, child) => {
            const event = JSON.parse(line);
            events.push(event);
            if (event.event === "suspended") {
              NodeAssert.ok(Number.isInteger(event.pid) && event.pid > 0);
              await gateFixtureExecutable(executable);
              await onSuspended?.({ scenario: command, pid: event.pid });
              abortIfRequested(signal);
              child.stdin.end(`${command}\n`);
            }
          },
        });
        NodeAssert.equal(events.length, 2);
        NodeAssert.equal(events[0].event, "suspended");
        const completion = events[1];
        NodeAssert.equal(completion.event, "complete");
        NodeAssert.equal(completion.freshLaunchErrno, NodeOS.constants.errno.EACCES);
        NodeAssert.equal(completion.reaped, true);
        NodeAssert.equal(completion.custodyRefused, command === "missing-custody");
        if (command === "resume")
          NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "native-write\n");
        else await NodeAssert.rejects(NodeFSP.lstat(marker), { code: "ENOENT" });
        NodeAssert.equal((await recoverFixtureExecutable(executable)).originalMode, 0o700);
        outcomes.push({
          scenario: command,
          freshLaunchDenied: true,
          capturedChildReaped: true,
          writeObserved: command === "resume",
          custodyRefused: completion.custodyRefused,
          originalModeRestored: true,
        });
      }
      return outcomes;
    },
    { signal, temporaryDirectory },
  );
  return {
    schema: 1,
    status: "passed",
    platform: process.platform,
    scenarios,
    activationAllowed: false,
    productionCustodyQualified: false,
    claim: "fixture-owned-processes-only",
    packagedArtifactQualified: false,
    elapsedMs: Math.round(performance.now() - started),
  };
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const cancellation = new AbortController();
  const handlers = new Map(
    ["SIGINT", "SIGTERM", "SIGHUP"].map((name) => [
      name,
      () => cancellation.abort(new Error(`Fixture received ${name}.`)),
    ]),
  );
  for (const [name, handler] of handlers) process.on(name, handler);
  try {
    NodeAssert.equal(
      process.argv.length,
      2,
      "The fixture accepts no installed paths or arguments.",
    );
    const result = await runLegacyAdmissionNativeFixture({ signal: cancellation.signal });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    if (result.status === "unsupported") process.exitCode = 77;
  } catch (error) {
    process.stderr.write(
      `${JSON.stringify({ schema: 1, status: "failed", message: error.message, retainedScratch: error.scratchRoot, activationAllowed: false })}\n`,
    );
    process.exitCode = 1;
  } finally {
    for (const [name, handler] of handlers) process.removeListener(name, handler);
  }
}
