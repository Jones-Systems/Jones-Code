import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { HostProcessPlatform } from "../../packages/shared/src/hostProcess.ts";

import { ownedChildCustody } from "./guard.mjs";

const terminalOutcomes = new Set([
  "success",
  "failed",
  "timed_out",
  "cancelled",
  "output_limited",
  "spawn_refused",
]);

function validOptions(options) {
  for (const key of ["timeoutMs", "terminateGraceMs", "reapTimeoutMs", "maxOutputBytes"]) {
    if (!Number.isSafeInteger(options[key]) || options[key] <= 0 || options[key] > 2_147_483_647)
      return false;
  }
  if (
    typeof options.executable !== "string" ||
    !NodePath.isAbsolute(options.executable) ||
    options.executable.includes("\0")
  )
    return false;
  if (
    Buffer.byteLength(options.executable) > 4096 ||
    (options.signal !== undefined && !(options.signal instanceof AbortSignal))
  )
    return false;
  if (
    !Array.isArray(options.args) ||
    options.args.length > 128 ||
    options.args.some((arg) => typeof arg !== "string" || arg.includes("\0")) ||
    options.args.reduce((bytes, arg) => bytes + Buffer.byteLength(arg), 0) > 64 * 1024
  )
    return false;
  if (!options.env || typeof options.env !== "object" || Array.isArray(options.env)) return false;
  const entries = Object.entries(options.env);
  return (
    entries.length <= 128 &&
    entries.every(
      ([key, value]) =>
        key.length > 0 &&
        !key.includes("=") &&
        !key.includes("\0") &&
        typeof value === "string" &&
        !value.includes("\0"),
    ) &&
    entries.reduce(
      (bytes, [key, value]) => bytes + Buffer.byteLength(key) + Buffer.byteLength(value),
      0,
    ) <=
      16 * 1024
  );
}

function processStartIdentity(pid) {
  if (HostProcessPlatform.defaultValue() !== "linux" || !Number.isInteger(pid)) return null;
  try {
    const text = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
    return { platform: "linux", startTicks: fields[19] };
  } catch {
    return null;
  }
}

function decodeOutput(chunks) {
  const bytes = Buffer.concat(chunks);
  const text = bytes.toString("utf8");
  if (Buffer.byteLength(text) <= bytes.length) return { text, shortened: false };
  const characters = [];
  let retainedBytes = 0;
  for (const character of text) {
    const size = Buffer.byteLength(character);
    if (retainedBytes + size > bytes.length) break;
    retainedBytes += size;
    characters.push(character);
  }
  return { text: characters.join(""), shortened: true };
}

export async function runOwnedChild(options) {
  let custody;
  try {
    custody = ownedChildCustody.begin(options.owner);
  } catch {
    return Object.freeze({
      schema: "jones-performance-child/v1",
      binding: null,
      outcome: "unknown",
      pid: null,
      startIdentity: null,
      exitCode: null,
      signal: null,
      stopReason: "ownership_unproved",
      observedBytes: 0,
      capturedBytes: 0,
      stdout: "",
      stderr: "",
      truncated: false,
      terminated: false,
      escalated: false,
      closed: false,
      reaped: false,
    });
  }
  const base = {
    schema: "jones-performance-child/v1",
    binding: custody.binding,
    pid: null,
    startIdentity: null,
    exitCode: null,
    signal: null,
    stopReason: null,
    observedBytes: 0,
    capturedBytes: 0,
    stdout: "",
    stderr: "",
    truncated: false,
    terminated: false,
    escalated: false,
    closed: false,
    reaped: false,
  };
  if (!validOptions(options)) {
    return ownedChildCustody.finish(custody.token, {
      ...base,
      outcome: "spawn_refused",
      stopReason: "invalid_options",
      closed: true,
      reaped: true,
    });
  }
  if (options.signal?.aborted) {
    return ownedChildCustody.finish(custody.token, {
      ...base,
      outcome: "cancelled",
      stopReason: "cancelled",
      closed: true,
      reaped: true,
    });
  }
  return await new Promise((resolve) => {
    let child;
    let timeoutTimer;
    let graceTimer;
    let reapTimer;
    let done = false;
    let stopReason = null;
    let spawnError = null;
    let observedBytes = 0;
    let capturedBytes = 0;
    let terminated = false;
    let escalated = false;
    const stdout = [];
    const stderr = [];

    const finish = (closed, exitCode = null, exitSignal = null) => {
      if (done) return;
      done = true;
      clearTimeout(timeoutTimer);
      clearTimeout(graceTimer);
      clearTimeout(reapTimer);
      options.signal?.removeEventListener("abort", cancel);
      child?.stdout?.removeListener("data", onStdout);
      child?.stderr?.removeListener("data", onStderr);
      child?.removeListener("close", onClose);
      child?.removeListener("error", onError);
      let outcome = !closed
        ? "unknown"
        : spawnError
          ? "spawn_refused"
          : (stopReason ?? (exitCode === 0 ? "success" : "failed"));
      if (closed && !terminalOutcomes.has(outcome)) outcome = "unknown";
      const stdoutText = decodeOutput(stdout);
      const stderrText = decodeOutput(stderr);
      resolve(
        ownedChildCustody.finish(custody.token, {
          ...base,
          pid: child?.pid ?? null,
          startIdentity: base.startIdentity,
          outcome,
          exitCode,
          signal: exitSignal,
          stopReason: stopReason ?? (spawnError ? "spawn_refused" : null),
          observedBytes,
          capturedBytes,
          stdout: stdoutText.text,
          stderr: stderrText.text,
          truncated: observedBytes > capturedBytes || stdoutText.shortened || stderrText.shortened,
          terminated,
          escalated,
          closed,
          reaped: closed,
        }),
      );
    };
    const stop = (reason) => {
      if (done || stopReason) return;
      stopReason = reason;
      if (!child?.pid) return;
      // Signal only this captured leaf. The caller has audited away descendants and provider/network work.
      try {
        terminated = child.kill("SIGTERM");
      } catch {
        terminated = false;
      }
      graceTimer = setTimeout(() => {
        if (done) return;
        try {
          escalated = child.kill("SIGKILL");
        } catch {
          escalated = false;
        }
        reapTimer = setTimeout(() => finish(false), options.reapTimeoutMs);
      }, options.terminateGraceMs);
    };
    const capture = (chunks, bytes) => {
      if (done) return;
      observedBytes += bytes.length;
      const remaining = Math.max(0, options.maxOutputBytes - capturedBytes);
      if (remaining) {
        const retained = bytes.subarray(0, Math.min(bytes.length, remaining));
        chunks.push(Buffer.from(retained));
        capturedBytes += retained.length;
      }
      if (observedBytes > options.maxOutputBytes) stop("output_limited");
    };
    const onStdout = (bytes) => capture(stdout, bytes);
    const onStderr = (bytes) => capture(stderr, bytes);
    const onClose = (code, signal) => finish(true, code, signal);
    const onError = (error) => {
      if (!child?.pid) {
        spawnError = error;
        reapTimer = setTimeout(() => finish(false), options.reapTimeoutMs);
      } else stop("failed");
    };
    const cancel = () => stop("cancelled");
    try {
      child = NodeChildProcess.spawn(options.executable, options.args, {
        cwd: custody.rootPath,
        env: { ...options.env },
        shell: false,
        detached: false,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch {
      spawnError = true;
      finish(true);
      return;
    }
    base.startIdentity = processStartIdentity(child.pid);
    child.stdout.on("data", onStdout);
    child.stderr.on("data", onStderr);
    child.once("close", onClose);
    child.on("error", onError);
    timeoutTimer = setTimeout(() => stop("timed_out"), options.timeoutMs);
    options.signal?.addEventListener("abort", cancel, { once: true });
    if (options.signal?.aborted) cancel();
  });
}
