import * as NodeAssert from "node:assert";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import { sha256 } from "../../../apps/desktop/scripts/ui-evidence/provenance.mjs";
import {
  readNativeEvidence,
  validateNativeBoundaryFiles,
} from "../../../apps/desktop/scripts/ui-evidence/native-workflow-boundary.mjs";

export const nativeVariants = [
  { id: "positive", group: "sidebar", scenario: "sidebar-pin-restart.mjs", expectedCode: 0 },
  { id: "negative", group: "sidebar", scenario: "negative.mjs", expectedCode: 1 },
  { id: "two-clients", group: "observer", scenario: "two-clients-one-server.mjs", expectedCode: 0 },
  {
    id: "self-configure",
    group: "self_configure",
    scenario: "thread-self-configure.mjs",
    expectedCode: 0,
  },
  {
    id: "queue-workflow",
    group: "queue",
    scenario: "queue-edit-cancel.mjs",
    queueCase: "workflow",
    expectedCode: 0,
  },
  {
    id: "queue-no-edit-negative",
    group: "queue",
    scenario: "queue-edit-cancel.mjs",
    queueCase: "no-edit-negative",
    expectedCode: 1,
  },
  {
    id: "two-servers",
    group: "servers",
    scenario: "two-servers-one-client.mjs",
    secretService: true,
    expectedCode: 0,
  },
];

const variantFor = (id) => {
  const variant = nativeVariants.find((value) => value.id === id);
  NodeAssert.strict.ok(variant, "Unknown native workflow variant");
  return variant;
};
const save = (file, value) =>
  NodeFSP.writeFile(file, `${JSON.stringify(value, null, 2)}\n`, {
    flag: "wx",
    mode: 0o600,
    flush: true,
  });

export function nativeAdmission(id, rows, cancelled = false) {
  if (cancelled) return "cancelled";
  const index = nativeVariants.findIndex((value) => value.id === id);
  NodeAssert.strict.ok(index >= 0, "Unknown native workflow variant");
  if (rows.some((row) => row.id === id)) return "first-attempt-already-recorded";
  for (const previous of nativeVariants.slice(0, index)) {
    const row = rows.find((value) => value.id === previous.id);
    if (
      !row ||
      row.safe !== true ||
      !Number.isInteger(row.oracleCode) ||
      !["passed", "expected-negative", "failed"].includes(row.status)
    )
      return `unproved-boundary:${previous.id}`;
  }
  return null;
}

async function sampleNativeResources(id, output) {
  const cpu = async () => {
    const fields = (await NodeFSP.readFile("/proc/stat", "utf8"))
      .split("\n")[0]
      .trim()
      .split(/\s+/)
      .slice(1, 9)
      .map(Number);
    NodeAssert.strict.equal(fields.length, 8);
    NodeAssert.strict.ok(fields.every(Number.isFinite));
    return { total: fields.reduce((a, b) => a + b, 0), idle: fields[3] + fields[4] };
  };
  const before = await cpu();
  await new Promise((resolve) => setTimeout(resolve, 1000));
  const after = await cpu();
  NodeAssert.strict.ok(after.total > before.total, "CPU sample unavailable");
  const cpuUsedPercent = 100 * (1 - (after.idle - before.idle) / (after.total - before.total));
  const memory = await NodeFSP.readFile("/proc/meminfo", "utf8");
  const availableKiB = Number(memory.match(/^MemAvailable:\s+(\d+)/m)?.[1]);
  const logicalCores = (await NodeFSP.readFile("/proc/stat", "utf8"))
    .split("\n")
    .filter((line) => /^cpu\d+\s/.test(line)).length;
  const sample = { cpuUsedPercent, logicalCores, oneCorePercent: 100 / logicalCores, availableKiB };
  await save(NodePath.join(output, `${id}-admission.json`), sample);
  NodeAssert.strict.ok(
    logicalCores > 0 &&
      Number.isFinite(cpuUsedPercent) &&
      cpuUsedPercent >= 0 &&
      cpuUsedPercent < 85 &&
      availableKiB > 0,
    "Native run not admitted; no retry",
  );
}

async function childIdentity(pid) {
  const raw = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
  const fields = raw
    .slice(raw.lastIndexOf(")") + 2)
    .trim()
    .split(/\s+/);
  NodeAssert.strict.match(fields[19] ?? "", /^\d+$/);
  return { pid, pgid: Number(fields[2]), startTicks: fields[19] };
}
function groupAbsent(pgid) {
  try {
    process.kill(-pgid, 0);
    return false;
  } catch (error) {
    if (error.code === "ESRCH") return true;
    throw error;
  }
}

// The pipe admits exactly one captured child. Signals go to that owned group
// before awaiting its close and before any boundary/registry cleanup read.
export async function runOwnedNative({
  argv,
  output,
  id,
  signals = process,
  launch = NodeChildProcess.spawn,
  captureIdentity = childIdentity,
  absent = groupAbsent,
  signalGroup = (pgid, signal) => process.kill(-pgid, signal),
  journal = save,
}) {
  let child, identity, cancelled, escalation, signalError, gateError, exit;
  let admitted = false;
  let signalWork = Promise.resolve();
  const forwarded = [];
  const forward = (signal) => {
    cancelled ??= signal;
    signalWork = signalWork
      .then(async () => {
        if (!child || child.exitCode !== null || child.signalCode !== null) return;
        NodeAssert.strict.deepEqual(
          await captureIdentity(child.pid),
          identity,
          "Captured child identity changed",
        );
        signalGroup(identity.pgid, signal);
        forwarded.push(signal);
        escalation ??= setTimeout(() => {
          signalWork = signalWork
            .then(async () => {
              if (child.exitCode !== null || child.signalCode !== null) return;
              NodeAssert.strict.deepEqual(await captureIdentity(child.pid), identity);
              signalGroup(identity.pgid, "SIGKILL");
              forwarded.push("SIGKILL");
            })
            .catch((error) => {
              signalError ??= error.message;
            });
        }, 5000);
      })
      .catch((error) => {
        signalError ??= error.message;
      });
  };
  const interrupt = () => forward("SIGINT");
  const terminate = () => forward("SIGTERM");
  signals.on("SIGINT", interrupt);
  signals.on("SIGTERM", terminate);
  let stdout, stderr;
  try {
    stdout = await NodeFSP.open(NodePath.join(output, `${id}-result.json`), "wx", 0o600);
    stderr = await NodeFSP.open(NodePath.join(output, `${id}-stderr.log`), "wx", 0o600);
    if (cancelled) return { code: cancelled === "SIGINT" ? 130 : 143, cancelled, started: false };
    child = launch(
      process.execPath,
      [NodeURL.fileURLToPath(import.meta.url), "child", JSON.stringify(argv)],
      { detached: true, stdio: ["ignore", stdout.fd, stderr.fd, "pipe"], env: process.env },
    );
    child.stdio[3].on?.("error", (error) => {
      gateError ??= error.message;
    });
    const closed = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    // Attach the observer before any asynchronous capture or gate write.
    closed.catch(() => {});
    try {
      identity = await captureIdentity(child.pid);
      NodeAssert.strict.equal(identity.pgid, child.pid);
      await journal(NodePath.join(output, `${id}-started.json`), { identity, argv, attempts: 1 });
      if (cancelled) {
        forward(cancelled);
        child.stdio[3].end();
      } else {
        admitted = true;
        child.stdio[3].end("1");
      }
    } catch (error) {
      child.stdio[3].end();
      await closed;
      throw error;
    }
    exit = await closed;
    await signalWork;
    return {
      code: exit.code ?? (cancelled === "SIGINT" ? 130 : cancelled ? 143 : 1),
      signal: exit.signal,
      cancelled: cancelled ?? null,
      started: admitted,
      identity,
      groupAbsent: absent(identity.pgid),
      forwarded,
      signalError: signalError ?? gateError,
    };
  } catch (error) {
    return {
      code: exit?.code ?? null,
      cancelled: cancelled ?? null,
      started: admitted,
      identity,
      groupAbsent: false,
      forwarded,
      signalError: error.message,
    };
  } finally {
    clearTimeout(escalation);
    signals.off("SIGINT", interrupt);
    signals.off("SIGTERM", terminate);
    await stdout?.close();
    await stderr?.close();
  }
}

export async function attemptNativeVariant({
  variant,
  rows,
  expected,
  cancelled = false,
  admit = sampleNativeResources,
  execute = runOwnedNative,
  validate = validateNativeBoundaryFiles,
  persist = async () => {},
}) {
  const row = {
    id: variant.id,
    journey: variant.group,
    status: "not-executed/blocked",
    safe: false,
    attempted: false,
    oracleCode: null,
    nativeCode: null,
    evidence: {
      result: `${variant.id}-result.json`,
      manifest: `${variant.id}/manifest.json`,
      inner: `${variant.id}/inner.json`,
    },
    reason: nativeAdmission(variant.id, rows, cancelled),
  };
  try {
    if (row.reason) {
      if (cancelled) row.status = "cancelled";
      return row;
    }
    await admit(variant.id, expected.output);
    const argv = [
      "run",
      "--source",
      expected.source,
      "--scenario",
      expected.scenario,
      "--build-receipt",
      NodePath.join(expected.output, "build/receipt.json"),
      "--out",
      expected.artifacts,
      ...(variant.queueCase ? ["--queue-case", variant.queueCase] : []),
    ];
    const controller = await execute({ argv, output: expected.output, id: variant.id });
    row.attempted = controller.started;
    row.controller = controller;
    row.nativeCode = controller.code;
    row.status = controller.cancelled
      ? "cancelled"
      : controller.started
        ? "failed"
        : "not-executed/blocked";
    if (controller.cancelled) {
      row.reason = "cancelled";
      return row;
    }
    NodeAssert.strict.equal(controller.groupAbsent, true, "Owned controller group closure unproved");
    NodeAssert.strict.equal(controller.signalError, undefined);
    const proof = await validate({ ...expected, exitCode: controller.code });
    NodeAssert.strict.equal(proof.safe, true);
    row.safe = true;
    row.evidence = proof.evidence ?? row.evidence;
    row.runId = proof.runId;
    row.reason = proof.kind;
  } catch (error) {
    row.reason = error.message;
  } finally {
    await persist(row);
  }
  return row;
}

export function finishNativeVariant(variant, attempt, oracleCode) {
  NodeAssert.strict.equal(attempt.id, variant.id);
  NodeAssert.strict.ok(Number.isInteger(oracleCode) && oracleCode >= 0);
  const passed =
    attempt.attempted === true &&
    attempt.safe === true &&
    attempt.nativeCode === variant.expectedCode &&
    oracleCode === 0;
  return {
    ...attempt,
    oracleCode,
    status: passed ? (variant.expectedCode === 1 ? "expected-negative" : "passed") : attempt.status,
    oracle: passed ? "satisfied" : "failed",
  };
}

export function aggregateNative(rows, cancelled = false, cleanup = { outcome: "unknown" }) {
  const variants = nativeVariants.map(
    (variant) =>
      rows.find((row) => row.id === variant.id) ?? {
        id: variant.id,
        journey: variant.group,
        status: cancelled ? "cancelled" : "not-executed/blocked",
        safe: false,
        attempted: false,
        reason: cancelled ? "job-cancelled" : "missing-required-evidence",
      },
  );
  const complete = variants.every(
    (row) =>
      row.safe === true &&
      row.oracleCode === 0 &&
      ["passed", "expected-negative"].includes(row.status),
  );
  return {
    schema: "jones-native-workflow-aggregate/v1",
    status:
      complete && !cancelled && cleanup.outcome === "complete" && cleanup.remainingRoots === 0
        ? "passed"
        : "failed",
    jobCleanup: cleanup,
    variants,
  };
}

export async function readNativeWorkflowRows(output) {
  const rows = [];
  for (const variant of nativeVariants) {
    let attempt;
    for (const suffix of ["attempt", "outcome"]) {
      let captured;
      try {
        const value = await readNativeEvidence(NodePath.join(output, `${variant.id}-${suffix}.json`));
        captured = value;
        NodeAssert.strict.equal(value.id, variant.id);
        if (suffix === "outcome") {
          NodeAssert.strict.ok(attempt, "Outcome without first attempt");
          NodeAssert.strict.deepEqual(value, finishNativeVariant(variant, attempt, value.oracleCode));
        } else if (value.safe === true) {
          NodeAssert.strict.equal(value.attempted, true);
          NodeAssert.strict.ok([0, 1].includes(value.nativeCode));
          NodeAssert.strict.equal(value.oracleCode, null);
          NodeAssert.strict.equal(value.controller.groupAbsent, true);
          NodeAssert.strict.equal(value.controller.signalError, undefined);
          NodeAssert.strict.equal(value.controller.identity.pid, value.controller.identity.pgid);
          NodeAssert.strict.match(value.controller.identity.startTicks, /^\d+$/);
          NodeAssert.strict.match(value.runId, /^[a-f0-9-]{36}$/);
          for (const [key, file] of Object.entries({
            result: `${variant.id}-result.json`,
            manifest: `${variant.id}/manifest.json`,
            inner: `${variant.id}/inner.json`,
            registry: `${variant.id}-boundary/registry.json`,
          })) {
            NodeAssert.strict.equal(value.evidence[key].file, file);
            await readNativeEvidence(NodePath.join(output, file));
            NodeAssert.strict.equal(
              sha256(await NodeFSP.readFile(NodePath.join(output, file))),
              value.evidence[key].sha256,
            );
          }
          NodeAssert.strict.equal(
            sha256(await NodeFSP.readFile(NodePath.join(output, "build/receipt.json"))),
            value.evidence.buildReceiptSha256,
          );
        }
        attempt = value;
      } catch (error) {
        if (error.code !== "ENOENT" || captured !== undefined) {
          attempt = {
            ...(attempt ?? captured),
            id: variant.id,
            journey: variant.group,
            safe: false,
            status: "failed",
            oracleCode: null,
            reason: `invalid-evidence:${error.message}`,
          };
          break;
        }
      }
    }
    if (!attempt) {
      for (const suffix of ["started.json", "result.json", "admission.json", "exit-code.txt"]) {
        const exists = await NodeFSP.lstat(NodePath.join(output, `${variant.id}-${suffix}`)).then(
          () => true,
          (error) => {
            if (error.code === "ENOENT") return false;
            throw error;
          },
        );
        if (exists) {
          attempt = {
            id: variant.id,
            journey: variant.group,
            safe: false,
            status: "failed",
            oracleCode: null,
            attempted: "unknown",
            reason: "unrecorded-first-attempt; reconciliation required",
          };
          break;
        }
      }
    }
    if (attempt) rows.push(attempt);
  }
  return rows;
}

async function expectedVariant(variant) {
  const output = NodePath.resolve(process.env.JCUE_OUTPUT);
  const source = NodePath.resolve(process.env.GITHUB_WORKSPACE);
  const scenario =
    variant.id === "negative"
      ? NodePath.join(output, variant.scenario)
      : NodePath.join(source, "apps/desktop/scripts/ui-evidence/scenarios", variant.scenario);
  return {
    ...variant,
    output,
    source,
    scenario,
    artifacts: NodePath.join(output, variant.id),
    scenarioSha256: sha256(await NodeFSP.readFile(scenario)),
    head: process.env.JCUE_HARNESS_OID,
    electron: process.env.JCUE_ELECTRON_VERSION,
    stateHome: NodePath.resolve(process.env.XDG_STATE_HOME),
  };
}

async function main([command, id, value]) {
  if (command === "child") {
    const gate = Buffer.alloc(1);
    if (NodeFS.readSync(3, gate, 0, 1, null) !== 1 || gate[0] !== 49) return 125;
    const { main: run } = await import("../../../apps/desktop/scripts/ui-evidence.mjs");
    const result = await run(JSON.parse(id));
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return result.code;
  }
  const output = NodePath.resolve(process.env.JCUE_OUTPUT);
  const rows = await readNativeWorkflowRows(output);
  if (command === "aggregate") {
    const cleanup = await readNativeEvidence(NodePath.join(output, "cleanup.json")).catch((error) => ({
      outcome: "unknown",
      reason: error.message,
    }));
    const report = aggregateNative(rows, process.env.JCUE_CANCELLED === "true", cleanup);
    await save(NodePath.join(output, "native-aggregate.json"), report);
    return report.status === "passed" ? 0 : 1;
  }
  if (command === "gate") {
    const selected = nativeVariants.filter((variant) => variant.group === id);
    NodeAssert.strict.ok(selected.length > 0);
    const safe = selected.every((variant) =>
      rows.some(
        (row) => row.id === variant.id && row.safe === true && Number.isInteger(row.oracleCode),
      ),
    );
    await NodeFSP.appendFile(process.env.GITHUB_OUTPUT, `safe=${safe}\n`);
    return 0;
  }
  const variant = variantFor(id);
  if (command === "ready") {
    NodeAssert.strict.equal(nativeAdmission(id, rows), null);
    return 0;
  }
  if (command === "finish") {
    const attempt = await readNativeEvidence(NodePath.join(output, `${id}-attempt.json`));
    await save(
      NodePath.join(output, `${id}-outcome.json`),
      finishNativeVariant(variant, attempt, Number(value)),
    );
    return 0;
  }
  const expected = await expectedVariant(variant);
  let attempt;
  if (command === "run") {
    attempt = await attemptNativeVariant({
      variant,
      rows,
      expected,
      persist: (row) => save(NodePath.join(output, `${id}-attempt.json`), row),
    });
    if (attempt.nativeCode !== null)
      await NodeFSP.writeFile(NodePath.join(output, `${id}-exit-code.txt`), `${attempt.nativeCode}\n`, {
        flag: "wx",
        mode: 0o600,
      });
  } else if (command === "external") {
    NodeAssert.strict.equal(id, "two-servers");
    const controller = await readNativeEvidence(NodePath.join(output, `${id}-controller.json`));
    const started = await readNativeEvidence(NodePath.join(output, `${id}-started.json`));
    NodeAssert.strict.deepEqual(controller.identity, started.identity);
    NodeAssert.strict.equal(started.wholeRunCapSeconds, 390);
    NodeAssert.strict.equal(started.argv[6], expected.scenario);
    // JE03's retained Python controller already made this one attempt. Adopt
    // that exact journal for validation only; this command never starts native work.
    const preceding = rows.filter(
      (row) => row.id !== id || row.reason !== "unrecorded-first-attempt; reconciliation required",
    );
    attempt = await attemptNativeVariant({
      variant,
      rows: preceding,
      expected,
      admit: async () => {},
      execute: async () => ({
        started: true,
        code: controller.exitCode,
        cancelled: controller.cancelled,
        groupAbsent: controller.groupAbsent && controller.childReaped,
        signalError: controller.cleanupError,
        identity: controller.identity,
      }),
      persist: (row) => save(NodePath.join(output, `${id}-attempt.json`), row),
    });
    await save(
      NodePath.join(output, `${id}-outcome.json`),
      finishNativeVariant(variant, attempt, controller.status === "passed" ? 0 : 1),
    );
  } else throw new Error("Unknown native workflow evidence command");
  return attempt.safe ? 0 : attempt.nativeCode || 1;
}

if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
