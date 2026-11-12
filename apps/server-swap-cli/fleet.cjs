"use strict";

const fs = require("node:fs");
const crypto = require("node:crypto");
const { performance } = require("node:perf_hooks");
const { setTimeout: sleep } = require("node:timers/promises");
const { remote, remoteArgs } = require("./transport.cjs");

function loadFleet(file) {
  const value = JSON.parse(fs.readFileSync(file, "utf8"));
  if (
    value.schema !== "server-swap-fleet/v1" ||
    !Array.isArray(value.targets) ||
    !value.targets.length ||
    value.targets.length > 32
  ) {
    throw new Error("Expected server-swap-fleet/v1 with 1–32 ordered targets");
  }
  const ids = new Set();
  const bindings = new Set();
  for (const target of value.targets) {
    if (
      typeof target.id !== "string" ||
      !/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(target.id) ||
      ids.has(target.id)
    ) {
      throw new Error("Each fleet target needs a unique id");
    }
    remoteArgs(target, "status");
    if (typeof target.planDigest !== "string" || !/^[a-f0-9]{64}$/.test(target.planDigest)) {
      throw new Error("Each target must pin the SHA-256 planDigest returned by plan");
    }
    const binding = `${target.ssh}\0${target.plan}`;
    if (bindings.has(binding)) throw new Error("Duplicate host/plan binding");
    ids.add(target.id);
    bindings.add(binding);
  }
  return value;
}

async function runFleet(manifest, operation, invoke = remote) {
  if (!["plan", "apply", "status", "resume", "reconcile"].includes(operation))
    throw new Error("Unsupported fleet command");
  const results = [];
  for (const target of manifest.targets) {
    let observation = await invoke(
      target,
      ["plan", "reconcile"].includes(operation) ? operation : "status",
    );
    if (observation.planDigest !== target.planDigest) {
      results.push({
        id: target.id,
        observation: { status: "unknown", reason: "Missing or changed host plan digest" },
      });
      break;
    }
    if (
      ["apply", "resume", "status"].includes(operation) &&
      ["committed", "committed-unverified"].includes(observation.status)
    ) {
      observation = await invoke(target, "reconcile");
      if (observation.planDigest !== target.planDigest)
        observation = { status: "unknown", reason: "Reconciliation plan digest mismatch" };
    }
    const state = observation.status;
    if (["apply", "resume"].includes(operation) && state === "not-started") {
      observation = await invoke(target, "launch");
      if (observation.planDigest !== target.planDigest)
        observation = { status: "unknown", reason: "Launch receipt plan digest mismatch" };
    }
    results.push({ id: target.id, observation });
    // Launch is durably claimed once on the host, including across lost SSH responses.
    if (!["status", "plan"].includes(operation) && observation.status !== "committed") break;
  }
  const complete =
    results.length === manifest.targets.length &&
    results.every((r) => r.observation.status === "committed");
  const ready =
    operation === "plan" &&
    results.length === manifest.targets.length &&
    results.every((r) => r.observation.status === "ready");
  return {
    status: complete ? "committed" : ready ? "ready" : "pending",
    targets: results,
    remaining: manifest.targets.slice(results.length).map((t) => t.id),
    laptop: complete
      ? "Remote swaps committed. Switch the laptop application manually and verify Connections."
      : "Keep the laptop switch pending.",
  };
}

async function main(argv) {
  if (argv[0] === "run") {
    if (argv.length !== 4 || !argv.slice(2).every((value) => /^\d+$/.test(value))) {
      throw new Error("usage: fleet run /absolute/fleet.json deadline-ms poll-ms");
    }
    const manifest = loadFleet(argv[1]);
    return {
      ...(await runUntilComplete(manifest, {
        timeoutMs: Number(argv[2]),
        pollMs: Number(argv[3]),
      })),
      fleetDigest: crypto.createHash("sha256").update(JSON.stringify(manifest)).digest("hex"),
    };
  }
  if (argv.length !== 2)
    throw new Error("usage: fleet plan|apply|status|resume|reconcile fleet.json");
  const manifest = loadFleet(argv[1]);
  const result = await runFleet(manifest, argv[0]);
  return {
    ...result,
    fleetDigest: crypto.createHash("sha256").update(JSON.stringify(manifest)).digest("hex"),
  };
}

async function runUntilComplete(manifest, options, dependencies = {}) {
  const { timeoutMs, pollMs } = options;
  if (
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1000 ||
    timeoutMs > 1800000 ||
    !Number.isInteger(pollMs) ||
    pollMs < 100 ||
    pollMs > 30000 ||
    pollMs > timeoutMs
  ) {
    throw new Error(
      "Fleet deadline must be 1000–1800000 ms; polling must be 100–30000 ms and within the deadline",
    );
  }
  const now = dependencies.now || (() => performance.now());
  const pause = dependencies.sleep || sleep;
  const invoke =
    dependencies.invoke ||
    ((target, command, remaining) => remote(target, command, undefined, { timeoutMs: remaining }));
  const deadline = now() + timeoutMs;
  const results = [];
  function report(status, stoppedAt, reason) {
    return {
      status,
      targets: results,
      remaining: manifest.targets.slice(results.length).map((target) => target.id),
      ...(stoppedAt ? { stoppedAt } : {}),
      ...(reason ? { reason } : {}),
      laptop:
        status === "committed"
          ? "Remote swaps committed. Switch the laptop application manually and verify Connections."
          : "Keep the laptop switch pending.",
    };
  }
  async function observe(target, command) {
    if (now() >= deadline)
      return {
        status: "timed-out",
        reason: "Fleet deadline reached; host workers may still be running",
      };
    let observation;
    try {
      observation = await invoke(target, command, Math.max(1, Math.ceil(deadline - now())));
    } catch {
      return {
        status: "unknown",
        reason: "Transport outcome unavailable; reconcile before continuing",
      };
    }
    if (now() >= deadline)
      return {
        status: "timed-out",
        reason: "Fleet deadline reached; host workers may still be running",
      };
    if (!observation || observation.planDigest !== target.planDigest) {
      return { status: "unknown", reason: "Missing or changed host plan digest" };
    }
    return observation;
  }
  for (const target of manifest.targets) {
    let launched = false;
    const record = { id: target.id, observation: null };
    results.push(record);
    while (true) {
      let observation = await observe(target, "status");
      if (["committed", "committed-unverified"].includes(observation.status))
        observation = await observe(target, "reconcile");
      if (observation.status === "not-started" && !launched) {
        // One attempt per invocation; the host's durable claim also fences restarts.
        launched = true;
        observation = await observe(target, "launch");
      }
      record.observation = observation;
      if (observation.status === "committed") break;
      if (!["launched", "in-progress", "not-started"].includes(observation.status)) {
        const status = [
          "unknown",
          "failed-before-start",
          "recovery-required",
          "timed-out",
        ].includes(observation.status)
          ? observation.status
          : "unknown";
        return report(
          status,
          target.id,
          observation.reason || observation.error || "Host requires inspection before continuing",
        );
      }
      if (now() >= deadline)
        return report(
          "timed-out",
          target.id,
          "Host workers may still be running; rerun fleet run to observe them",
        );
      await pause(Math.min(pollMs, Math.max(0, deadline - now())));
    }
  }
  // Recheck earlier hosts after the last one finishes before offering the laptop handoff.
  for (let index = 0; index < manifest.targets.length; index++) {
    const target = manifest.targets[index];
    const observation = await observe(target, "reconcile");
    results[index].observation = observation;
    if (observation.status !== "committed") {
      const status = ["unknown", "failed-before-start", "recovery-required", "timed-out"].includes(
        observation.status,
      )
        ? observation.status
        : "unknown";
      return report(
        status,
        target.id,
        observation.reason || observation.error || "Final runtime verification failed",
      );
    }
  }
  return report("committed");
}

module.exports = { loadFleet, runFleet, runUntilComplete, main };
