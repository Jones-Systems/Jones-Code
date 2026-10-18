import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

const commandPhaseNames = new Set([
  "fixture",
  "identity-compile",
  "identity-boot",
  "identity-boot-reference",
  "identity-inspect",
  "image-create",
  "image-attach",
  "image-detach",
  "observer-compile",
  "observer-wrapper-compile",
  "observer-driver",
]);
// Keep the original error identity, including frozen cancellation reasons.
// Diagnostics retain only these closed phase names, never argv, env or streams.
const commandFailurePhases = new WeakMap();
function phaseFailure(cause, phase) {
  if (cause !== null && (typeof cause === "object" || typeof cause === "function"))
    commandFailurePhases.set(cause, phase);
  return cause;
}

function custodyEvidence(cause) {
  const pending = [cause];
  const seen = new Set();
  const receipts = new Map();
  const phases = new Set();
  let preserveScratch = false;
  while (pending.length && seen.size < 64) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    const phase = commandFailurePhases.get(current);
    if (phase) phases.add(phase);
    preserveScratch ||= current.preserveScratch === true;
    if (current.commandCustody)
      receipts.set(current.commandCustody.commandId, current.commandCustody);
    pending.push(current.cause);
    if (current instanceof AggregateError) pending.push(...current.errors.slice(0, 32));
  }
  return { preserveScratch, receipts: [...receipts.values()], phases: [...phases] };
}

export function nativeFixtureFailureReport(cause) {
  const evidence = custodyEvidence(cause);
  return {
    status: "failed",
    message: String(cause.message).slice(0, 512),
    retainedScratch: cause.scratchRoot,
    commandCustody: evidence.receipts,
    commandPhases: evidence.phases,
    activationAllowed: false,
  };
}

export async function withNativeFixtureScratch(
  body,
  { temporaryDirectory = NodeOS.tmpdir() } = {},
) {
  const parent = await NodeFSP.realpath(temporaryDirectory);
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "jones-identity-native-"));
  // Emit the literal ownership locator before body can launch any subprocess.
  process.stderr.write(`${JSON.stringify({ event: "identity-fixture-root", root })}\n`);
  const owner = await NodeFSP.lstat(root, { bigint: true });
  let failure;
  let result;
  try {
    result = await body(root);
  } catch (cause) {
    failure = cause;
  }
  if (custodyEvidence(failure).preserveScratch) {
    failure.preserveScratch = true;
    failure.scratchRoot = root;
    throw failure;
  }
  try {
    const current = await NodeFSP.lstat(root, { bigint: true });
    NodeAssert.ok(current.isDirectory() && current.dev === owner.dev && current.ino === owner.ino);
    await NodeFSP.rm(root, { recursive: true, force: false });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  } catch (cleanup) {
    const retained = failure
      ? new AggregateError([failure, cleanup], failure.message, { cause: failure })
      : cleanup;
    retained.scratchRoot = root;
    throw retained;
  }
  if (failure) throw failure;
  return result;
}

export async function runNativeFixtureCommand(
  executable,
  args,
  root,
  { signal, fds = [], spawn = NodeChildProcess.spawn, phase: requestedPhase = "fixture" } = {},
) {
  signal?.throwIfAborted();
  const phase = commandPhaseNames.has(requestedPhase) ? requestedPhase : "fixture";
  const commandId = NodeCrypto.randomUUID();
  const createdAt = new Date().toISOString();
  let child;
  try {
    child = spawn(executable, args, {
      cwd: root,
      env: {
        PATH: "/usr/bin:/bin",
        HOME: root,
        TMPDIR: root,
        CLANG_MODULE_CACHE_PATH: NodePath.join(root, "clang-cache"),
      },
      stdio: ["ignore", "pipe", "pipe", ...fds],
    });
  } catch (cause) {
    throw phaseFailure(cause, phase);
  }
  let stdout = "";
  let stderr = "";
  let failure;
  let escalation;
  let custodyDeadline;
  let closed = false;
  let exited = false;
  let spawned = false;
  let stopping = false;
  let unresolved = false;
  const signalAttempts = [];
  const commandErrors = [];
  let activeAttempt;
  let finish;
  const completion = new Promise((resolve) => {
    finish = resolve;
  });
  const unknownCustody = () => {
    unresolved = true;
    finish({ code: null, stoppedBy: null });
  };
  const recordError = (cause) => {
    failure ??= cause;
    commandErrors.push(cause);
    if (activeAttempt) activeAttempt.errors.push(cause);
  };
  const sendSignal = (kind) => {
    if (closed || child.exitCode !== null || child.signalCode !== null) return;
    const attempt = { signal: kind, returned: null, errors: [] };
    signalAttempts.push(attempt);
    activeAttempt = attempt;
    try {
      attempt.returned = child.kill(kind);
    } catch (cause) {
      recordError(cause);
    } finally {
      activeAttempt = undefined;
    }
  };
  const stop = (cause) => {
    failure ??= cause;
    if (stopping) return;
    stopping = true;
    custodyDeadline ??= setTimeout(unknownCustody, 6000);
    sendSignal("SIGTERM");
    escalation = setTimeout(() => sendSignal("SIGKILL"), 3000);
  };
  const abort = () => stop(signal.reason ?? new Error("Native identity fixture cancelled."));
  signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(
    () => stop(new Error(`Native identity command (${phase}) exceeded 30 seconds.`)),
    30000,
  );
  child.stdout.on("data", (bytes) => {
    if (Buffer.byteLength(stdout) + bytes.length > 128 * 1024)
      stop(new Error("Native identity output exceeded its bound."));
    else stdout += bytes;
  });
  child.stderr.on("data", (bytes) => {
    if (Buffer.byteLength(stderr) + bytes.length > 16 * 1024)
      stop(new Error("Native identity diagnostics exceeded their bound."));
    else stderr += bytes;
  });
  // kill() may emit synchronously more than once. Keep this listener for the
  // captured handle's lifetime, including after an unresolved command returns.
  child.on("error", recordError);
  child.once("spawn", () => {
    spawned = true;
  });
  child.once("exit", () => {
    exited = true;
  });
  child.once("close", (code, stoppedBy) => {
    closed = true;
    finish({ code, stoppedBy });
  });
  if (signal?.aborted) abort();
  try {
    const result = await completion;
    if (unresolved) {
      const errorSummary = (cause) => ({
        name: String(cause.name ?? "Error").slice(0, 48),
        code: String(cause.code ?? "unknown").slice(0, 48),
      });
      const receipt = {
        schema: 1,
        commandId,
        phase,
        createdAt,
        observedAt: new Date().toISOString(),
        driverPid: process.pid,
        controllerPid: child.pid ?? null,
        identityScope: "captured-child-handle-only",
        pidMayBeReused: true,
        permitsPidSignalling: false,
        permitsProcessAdoption: false,
        spawnObserved: spawned,
        exitObserved: exited,
        closeObserved: closed,
        exitCode: child.exitCode,
        signalCode: child.signalCode,
        driverWillReleaseHandle: true,
        signalAttempts: signalAttempts.map((attempt) => ({
          signal: attempt.signal,
          returned: attempt.returned,
          errors: attempt.errors.slice(0, 32).map(errorSummary),
          errorCount: attempt.errors.length,
        })),
        errors: commandErrors.slice(0, 32).map(errorSummary),
        errorCount: commandErrors.length,
        evidencePath: NodePath.join(root, `identity-command-${commandId}.json`),
      };
      const failures = failure
        ? [failure, ...commandErrors.filter((item) => item !== failure)]
        : commandErrors;
      const custody = new AggregateError(
        failures,
        "Native command closure is unproved; exact scratch retained.",
        { cause: failure },
      );
      custody.preserveScratch = true;
      custody.commandCustody = { ...receipt, persisted: false };
      try {
        const evidence = await NodeFSP.open(receipt.evidencePath, "wx", 0o600);
        try {
          const encoded = `${JSON.stringify(receipt)}\n`;
          NodeAssert.ok(
            Buffer.byteLength(encoded) <= 16 * 1024,
            "Command custody receipt exceeded its bound.",
          );
          await evidence.writeFile(encoded);
          await evidence.sync();
        } finally {
          await evidence.close();
        }
        const directory = await NodeFSP.open(
          root,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
        custody.commandCustody.persisted = true;
      } catch (cause) {
        custody.errors.push(cause);
        custody.commandCustody.persistenceError = errorSummary(cause);
      } finally {
        // The receipt records observations, never authority to signal a saved
        // PID. Persist it before relinquishing this actual captured handle.
        child.stdout.destroy();
        child.stderr.destroy();
        child.unref();
      }
      throw phaseFailure(custody, phase);
    }
    if (failure) {
      const errors = commandErrors.filter((item) => item !== failure);
      if (errors.length)
        throw phaseFailure(
          new AggregateError([failure, ...errors], failure.message, { cause: failure }),
          phase,
        );
      throw phaseFailure(failure, phase);
    }
    return { ...result, stdout, stderr, phase };
  } finally {
    clearTimeout(timeout);
    clearTimeout(escalation);
    clearTimeout(custodyDeadline);
    signal?.removeEventListener("abort", abort);
  }
}
