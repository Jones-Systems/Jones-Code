import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const source = NodeURL.fileURLToPath(new URL("./legacy-process-custody-native.c", import.meta.url));
const scenarios = new Set(["run", "missing-symbol", "missing-flavor"]);
const outputLimit = 64 * 1024;
const readyCallbackTimeoutMs = 1_000;

function checkCancellation(signal) {
  if (signal?.aborted) throw signal.reason ?? new Error("Process custody fixture cancelled.");
}

export async function withProcessCustodyScratch(
  body,
  { temporaryDirectory = NodeOS.tmpdir(), signal } = {},
) {
  checkCancellation(signal);
  const root = await NodeFSP.mkdtemp(NodePath.join(temporaryDirectory, "jones-process-custody-"));
  const owner = await NodeFSP.lstat(root, { bigint: true });
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
    NodeAssert.ok(current.isDirectory() && current.ino === owner.ino && current.dev === owner.dev);
    await NodeFSP.rm(root, { recursive: true });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  } catch (cleanup) {
    if (failure) throw new AggregateError([failure, cleanup], failure.message, { cause: failure });
    throw cleanup;
  }
  if (failure) throw failure;
  return result;
}

async function ownedCommand(command, args, { root, signal, native = false, onReady } = {}) {
  checkCancellation(signal);
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
  let readySeen = false;
  let readyPending = false;
  let readyTimeout;
  let closed = false;
  let forcedEnd;
  let settle;
  const events = [];
  const cancelReady = () => {
    readyPending = false;
    clearTimeout(readyTimeout);
  };
  const stop = (error) => {
    failure ??= error;
    cancelReady();
    if (closed) return;
    child.stdin.end();
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    forcedEnd ??= setTimeout(() => {
      failure.preserveScratch = true;
      child.stdin.destroy();
      child.stdout.destroy();
      child.stderr.destroy();
      child.unref();
      settle({ code: null, signal: null, custodyUnknown: true });
    }, 7_000);
  };
  const cancelled = () => stop(signal.reason ?? new Error("Process custody fixture cancelled."));
  signal?.addEventListener("abort", cancelled, { once: true });
  const timeout = setTimeout(
    () => stop(new Error("Process custody command exceeded 30 seconds.")),
    30_000,
  );
  child.stdin.on("error", (error) => {
    stop(error);
  });
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
    pending += chunk;
    if (stdout.length > outputLimit)
      return stop(new Error("Process custody output exceeded its bound."));
    let boundary;
    while (native && (boundary = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, boundary);
      pending = pending.slice(boundary + 1);
      try {
        const event = JSON.parse(line);
        events.push(event);
        if (event.event !== "ready") continue;
        NodeAssert.equal(readySeen, false, "Native custody emitted duplicate readiness.");
        readySeen = true;
        NodeAssert.ok(Number.isInteger(event.pid) && event.pid > 0);
        NodeAssert.ok(Number.isInteger(event.pidversion) && event.pidversion > 0);
        if (failure || closed) continue;
        readyPending = true;
        readyTimeout = setTimeout(() => {
          const error = new Error("Process custody readiness callback exceeded 1 second.");
          error.code = "ERR_PROCESS_CUSTODY_READY_TIMEOUT";
          stop(error);
        }, readyCallbackTimeoutMs);
        // Callback settlement cannot hold up native cleanup evidence. Cancellation,
        // timeout and close invalidate the continuation, including late rejection.
        Promise.resolve()
          .then(() => {
            if (readyPending) return onReady?.(event);
          })
          .then(() => {
            if (!readyPending) return;
            checkCancellation(signal);
            cancelReady();
            child.stdin.end("R");
          })
          .catch((error) => {
            if (!closed && !failure) stop(error);
          });
      } catch (error) {
        stop(error);
      }
    }
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
    if (stderr.length > outputLimit)
      stop(new Error("Process custody diagnostics exceeded their bound."));
  });
  try {
    const result = await new Promise((resolve) => {
      settle = resolve;
      child.once("error", (error) => {
        failure ??= error;
      });
      child.once("close", (code, exitSignal) => {
        closed = true;
        cancelReady();
        resolve({ code, signal: exitSignal });
      });
    });
    if (native) requireNativeCleanup({ ...result, events }, failure);
    if (failure) throw failure;
    return { ...result, events, stdout, stderr };
  } finally {
    closed = true;
    cancelReady();
    clearTimeout(timeout);
    clearTimeout(forcedEnd);
    signal?.removeEventListener("abort", cancelled);
  }
}

function requireNativeCleanup(result, failure) {
  const final = result.events.at(-1);
  const cleanupProved = final?.event === "result" && final.cleanupProved === true;
  if (!cleanupProved) {
    failure ??= new Error("Native process custody ended without proved child cleanup.");
    failure.preserveScratch = true;
  }
  if (failure) {
    failure.nativeCleanup = {
      cleanupProved,
      result: final ?? null,
      exitCode: result.code,
      exitSignal: result.signal,
    };
    throw failure;
  }
}

export function validateProcessCustodyResult(result) {
  requireNativeCleanup(result);
  const final = result.events.at(-1);
  NodeAssert.equal(result.signal, null);
  NodeAssert.equal(final?.event, "result");
  NodeAssert.equal(final.cleanupProved, true);
  if (final.status === "unsupported") {
    NodeAssert.equal(result.code, 77);
    NodeAssert.equal(
      result.events.some((event) => event.event === "proof"),
      false,
    );
    return { status: "unsupported", reason: final.reason };
  }
  NodeAssert.equal(result.code, 0, `${final.reason}: ${result.stderr}`);
  NodeAssert.equal(final.status, "passed");
  NodeAssert.deepEqual(
    result.events.map((event) => event.event),
    ["ready", "proof", "result"],
  );
  const [ready, proof] = result.events;
  NodeAssert.ok(Number.isInteger(proof.afterPidversion) && proof.afterPidversion > 0);
  NodeAssert.match(proof.beforeUniqueId, /^[1-9][0-9]*$/);
  NodeAssert.match(proof.afterUniqueId, /^[1-9][0-9]*$/);
  NodeAssert.equal(proof.samePid, ready.pid);
  NodeAssert.equal(proof.beforePidversion, ready.pidversion);
  NodeAssert.notEqual(proof.beforePidversion, proof.afterPidversion);
  NodeAssert.equal(proof.beforeUniqueId, ready.uniqueId);
  NodeAssert.equal(proof.wrongTokenErrno, NodeOS.constants.errno.ESRCH);
  NodeAssert.equal(proof.staleTokenErrno, NodeOS.constants.errno.ESRCH);
  for (const field of [
    "stopObserved",
    "continueSurvivor",
    "wrongTokenSurvivor",
    "staleTokenSurvivor",
    "termReaped",
  ])
    NodeAssert.equal(proof[field], true);
  return { status: "passed", proof };
}

export async function runLegacyProcessCustodyFixture({
  scenario = "run",
  signal,
  temporaryDirectory,
  onReady,
} = {}) {
  NodeAssert.ok(scenarios.has(scenario), "Only fixed synthetic custody scenarios are supported.");
  checkCancellation(signal);
  const metadata = {
    schema: 1,
    platform: process.platform,
    kernel: NodeOS.release(),
    osVersion: NodeOS.version(),
    architecture: process.arch,
    nodeVersion: process.version,
    symbol: "proc_signal_with_audittoken",
    identitySchema: {
      flavor: 17,
      bytes: 56,
      idversionOffset: 32,
      auditTokenBytes: 32,
      pidIndex: 5,
      pidversionIndex: 7,
    },
    activationAllowed: false,
    productionMembershipQualified: false,
    hardenedAppQualified: false,
    pidOnlyAdmissionFallback: false,
  };
  if (process.platform !== "darwin")
    return { ...metadata, status: "unsupported", reason: "darwin-required" };
  const started = performance.now();
  return await withProcessCustodyScratch(
    async (root) => {
      const binary = NodePath.join(root, "custody-console");
      const compile = await ownedCommand(
        "/usr/bin/clang",
        ["-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", binary],
        { root, signal },
      );
      NodeAssert.equal(compile.code, 0, compile.stderr);
      NodeAssert.equal(compile.signal, null);
      const result = await ownedCommand(binary, [scenario], {
        root,
        signal,
        native: true,
        onReady,
      });
      const evidence = validateProcessCustodyResult(result);
      return {
        ...metadata,
        ...evidence,
        scenario,
        cleanupProved: true,
        sourceSha256: NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(source))
          .digest("hex"),
        binarySha256: NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(binary))
          .digest("hex"),
        elapsedMs: Math.round(performance.now() - started),
        limits: [
          "private-Apple-API",
          "captured-console-child-only",
          "no-process-discovery",
          "no-code-signing-proof",
          "no-production-admission",
        ],
      };
    },
    { signal, temporaryDirectory },
  );
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const cancellation = new AbortController();
  const handlers = new Map(
    ["SIGINT", "SIGTERM", "SIGHUP"].map((name) => [
      name,
      () => cancellation.abort(new Error(`Process custody fixture received ${name}.`)),
    ]),
  );
  for (const [name, handler] of handlers) process.on(name, handler);
  try {
    NodeAssert.equal(
      process.argv.length,
      2,
      "The qualifier accepts no production paths or process IDs.",
    );
    const result = await runLegacyProcessCustodyFixture({ signal: cancellation.signal });
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
