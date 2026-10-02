#!/usr/bin/env node

// @effect-diagnostics globalTimers:off -- The native CLI bounds stdin and its owned child with a deadline cleared in finally; an Effect timeout cannot preempt synchronous SQLite.
import * as NodeCrypto from "node:crypto";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeProcess from "node:process";

import type { ClosedFixtureConsumerOutcome } from "../../../scripts/performance-staging/fixtures.mjs";
import {
  createOwnedRoot,
  disposeOwnedRoot,
  type OwnedChildReceipt,
  type OwnedRoot,
} from "../../../scripts/performance-staging/guard.mjs";
import { runOwnedChild } from "../../../scripts/performance-staging/lifecycle.mjs";
import {
  childSummary,
  component,
  countTables,
  defaultHealthLimits,
  emptyHealthResults,
  encodeHealthEnvelope,
  envelopeReserveBytes,
  healthExitCode,
  healthPolicy,
  healthTaskRef,
  HealthInputError,
  isRecord,
  parentRuntime,
  parseHealthArguments,
  parseReceiptInput,
  reapTimeoutMs,
  reasonNames,
  repository,
  resultNames,
  scriptDirectory,
  settingNames,
  shutdownReserveMs,
  terminateGraceMs,
  toolSource,
  workerInputBytes,
  workerPath,
  type HealthEnvelope,
  type HealthReason,
  type HealthReceiptInput,
  type HealthRequest,
  type HealthResults,
  type HealthStatus,
  type HealthWorkerFrame,
} from "./jones-sqlite-support.ts";

const statuses: readonly HealthStatus[] = [
  "completed",
  "partial",
  "refused",
  "interrupted",
  "failed",
];
const componentStatuses = [
  "completed",
  "omitted",
  "unavailable",
  "interrupted",
  "truncated",
  "failed",
];

function text(value: unknown): value is string {
  return typeof value === "string" && !value.includes("\0") && Buffer.byteLength(value) <= 512;
}
function decimal(value: unknown): value is string {
  return typeof value === "string" && /^[0-9]+$/.test(value);
}
function digest(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}
function closed(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  return (
    isRecord(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => key in value)
  );
}
function reason(value: unknown): value is HealthReason | null {
  return value === null || reasonNames.some((name) => name === value);
}
function array(value: unknown, max: number, test: (item: unknown) => boolean): boolean {
  return Array.isArray(value) && value.length <= max && value.every(test);
}
function sidecar(value: unknown): boolean {
  return (
    closed(value, ["kind", "present", "bytes", "sha256"]) &&
    ["main", "wal", "shm", "journal"].includes(String(value.kind)) &&
    typeof value.present === "boolean" &&
    (value.present
      ? decimal(value.bytes) && digest(value.sha256)
      : value.bytes === null && value.sha256 === null)
  );
}

function validResultData(
  name: keyof HealthResults,
  value: unknown,
  request: HealthRequest,
): boolean {
  if (value === null) return true;
  switch (name) {
    case "metadata":
      return (
        closed(value, ["connectionSettingsOnly", "settings", "schema", "sidecars"]) &&
        value.connectionSettingsOnly === true &&
        closed(value.settings, settingNames) &&
        Object.values(value.settings).every(
          (setting) =>
            setting === null ||
            typeof setting === "boolean" ||
            (typeof setting === "number" && Number.isFinite(setting)) ||
            text(setting),
        ) &&
        array(
          value.schema,
          request.limits.maxRecords,
          (row) =>
            closed(row, ["name", "type"]) &&
            text(row.name) &&
            ["table", "index", "view", "trigger"].includes(String(row.type)),
        ) &&
        array(value.sidecars, 4, sidecar)
      );
    case "counts":
      return array(
        value,
        countTables.length,
        (row) =>
          closed(row, ["name", "status", "count"]) &&
          countTables.some(([key]) => key === row.name) &&
          ["completed", "unavailable", "failed"].includes(String(row.status)) &&
          (row.status === "completed" ? decimal(row.count) : row.count === null),
      );
    case "allocation":
      return (
        closed(value, ["scope", "excludes", "rows"]) &&
        value.scope === "btree-pages-only" &&
        value.excludes === "freelist-and-other-non-btree-pages" &&
        array(
          value.rows,
          request.limits.maxRecords,
          (row) =>
            closed(row, ["name", "pages", "bytes", "payloadBytes", "unusedBytes"]) &&
            text(row.name) &&
            [row.pages, row.bytes, row.payloadBytes, row.unusedBytes].every(decimal),
        )
      );
    case "integrity":
      return (
        closed(value, ["check", "passed", "observedViolations", "diagnostics"]) &&
        value.check === "full-integrity-check" &&
        (value.passed === null || typeof value.passed === "boolean") &&
        decimal(value.observedViolations) &&
        array(
          value.diagnostics,
          request.limits.maxDiagnostics,
          (row) => closed(row, ["code"]) && row.code === "integrity_violation",
        )
      );
    case "foreignKeys":
      return (
        closed(value, ["passed", "observedViolations", "diagnostics"]) &&
        (value.passed === null || typeof value.passed === "boolean") &&
        decimal(value.observedViolations) &&
        array(
          value.diagnostics,
          request.limits.maxDiagnostics,
          (row) =>
            closed(row, ["table", "parent", "constraintIndex"]) &&
            text(row.table) &&
            text(row.parent) &&
            decimal(row.constraintIndex),
        )
      );
    case "readonly":
      return (
        closed(value, ["unchanged", "beforeReceiptSha256", "afterReceiptSha256"]) &&
        (value.unchanged === null || typeof value.unchanged === "boolean") &&
        digest(value.beforeReceiptSha256) &&
        (value.afterReceiptSha256 === null || digest(value.afterReceiptSha256))
      );
  }
}

function decodeFrame(value: unknown, request: HealthRequest): HealthWorkerFrame {
  if (!isRecord(value)) throw new HealthInputError("invalid_worker_output");
  if (
    value.kind === "runtime" &&
    closed(value, ["kind", "runtime"]) &&
    closed(value.runtime, ["node", "platform", "arch", "sqliteVersion", "sqliteSourceId"]) &&
    [value.runtime.node, value.runtime.platform, value.runtime.arch].every(text) &&
    [value.runtime.sqliteVersion, value.runtime.sqliteSourceId].every(
      (item) => item === null || text(item),
    )
  )
    return value as unknown as HealthWorkerFrame;
  if (
    value.kind === "fixture" &&
    closed(value, ["kind", "fixture"]) &&
    closed(value.fixture, [
      "receiptSha256",
      "source",
      "rootId",
      "producerStep",
      "closureId",
      "closedLayout",
      "verifiedFiles",
      "verifiedBytes",
    ]) &&
    value.fixture.receiptSha256 === request.fixtureReceiptSha256 &&
    JSON.stringify(value.fixture.source) === JSON.stringify(request.fixtureBinding) &&
    [value.fixture.rootId, value.fixture.producerStep, value.fixture.closureId].every(text) &&
    decimal(value.fixture.verifiedFiles) &&
    decimal(value.fixture.verifiedBytes) &&
    array(value.fixture.closedLayout, 4, sidecar)
  )
    return value as unknown as HealthWorkerFrame;
  if (
    value.kind === "result" &&
    closed(value, ["kind", "name", "result"]) &&
    resultNames.some((name) => name === value.name) &&
    closed(value.result, ["status", "reason", "data"]) &&
    componentStatuses.includes(String(value.result.status)) &&
    reason(value.result.reason) &&
    validResultData(value.name as keyof HealthResults, value.result.data, request)
  )
    return value as unknown as HealthWorkerFrame;
  if (
    value.kind === "complete" &&
    closed(value, ["kind", "status", "reason"]) &&
    statuses.some((status) => status === value.status) &&
    reason(value.reason)
  )
    return value as unknown as HealthWorkerFrame;
  throw new HealthInputError("invalid_worker_output");
}

function initialReport(request: HealthRequest | null): HealthEnvelope {
  const limits = request?.limits ?? defaultHealthLimits;
  return {
    schema: "jones.sqlite-health/v1",
    status: "refused",
    reason: null,
    toolSource: toolSource(),
    fixture: null,
    runtime: parentRuntime(),
    limits: {
      ...limits,
      maxWorkerInputBytes: workerInputBytes,
      maxCapturedChildBytes: limits.maxOutputBytes - envelopeReserveBytes,
    },
    elapsedMs: 0,
    results: emptyHealthResults(request?.include ?? []),
    child: childSummary(null),
    cleanup: {
      status: "completed",
      reason: null,
      supervisorRoot: null,
      fixtureOwnership: "retained-input",
    },
  };
}

export interface HealthRunOptions {
  readonly signal?: AbortSignal;
  readonly startedAtMs?: number;
  readonly runChild?: typeof runOwnedChild;
}

// Supply the independently pinned producer digest; report status does not prove resource closure.
export function sqliteHealthConsumerOutcome(
  fixtureReceiptSha256: string,
  report: HealthEnvelope,
): ClosedFixtureConsumerOutcome<HealthEnvelope> {
  const release =
    report.child.closed &&
    report.child.reaped &&
    report.child.outcome !== "unknown" &&
    report.cleanup.status === "completed" &&
    report.cleanup.supervisorRoot === null;
  return {
    schema: "jones-performance-fixture-consumer/v1",
    fixtureReceiptSha256,
    disposition: release ? "release" : "retain",
    value: report,
  };
}

export async function runSqliteHealth(
  request: HealthRequest,
  fixture: HealthReceiptInput,
  options: HealthRunOptions = {},
): Promise<HealthEnvelope> {
  const startedAtMs = options.startedAtMs ?? NodePerfHooks.performance.now();
  let report = initialReport(request);
  let owner: OwnedRoot | null = null;
  let child: OwnedChildReceipt | null = null;
  let supervisorRoot: string | null = null;
  let started = false;
  const policy = healthPolicy();
  try {
    const payload = JSON.stringify({
      schema: "jones.sqlite-health-worker-input/v1",
      request,
      fixture,
      policy,
    });
    const args = [workerPath, "health", payload];
    if (
      Buffer.byteLength(payload) > workerInputBytes ||
      args.reduce((bytes, arg) => bytes + Buffer.byteLength(arg), 0) > 64 * 1024
    )
      throw new HealthInputError("worker_input_too_large");
    const timeoutMs = Math.floor(
      request.limits.deadlineMs -
        (NodePerfHooks.performance.now() - startedAtMs) -
        shutdownReserveMs,
    );
    if (options.signal?.aborted || timeoutMs < 1)
      throw new HealthInputError(options.signal?.aborted ? "cancelled" : "worker_deadline");
    const childName = `.jones-sqlite-health-${NodeCrypto.randomUUID()}`;
    supervisorRoot = `${scriptDirectory}${childName}`;
    owner = createOwnedRoot({
      parentPath: scriptDirectory,
      childName,
      binding: {
        repository,
        sourceRevision: request.fixtureBinding.sourceRevision,
        taskRef: healthTaskRef,
        runId: childName,
      },
      policy,
    });
    supervisorRoot = owner.creationReceipt.canonicalRootPath;
    const remainingMs = Math.floor(
      request.limits.deadlineMs -
        (NodePerfHooks.performance.now() - startedAtMs) -
        shutdownReserveMs,
    );
    if (options.signal?.aborted || remainingMs < 1)
      throw new HealthInputError(options.signal?.aborted ? "cancelled" : "worker_deadline");
    started = true;
    child = await (options.runChild ?? runOwnedChild)({
      owner,
      executable: NodeProcess.execPath,
      args,
      env: { HOME: policy.homePath, NODE_NO_WARNINGS: "1", LANG: "C" },
      timeoutMs: remainingMs,
      terminateGraceMs,
      reapTimeoutMs,
      maxOutputBytes: report.limits.maxCapturedChildBytes,
      ...(options.signal ? { signal: options.signal } : {}),
    });
    report = { ...report, child: childSummary(child) };
    const results = { ...report.results };
    const seen = new Set<keyof HealthResults>();
    let terminal: Extract<HealthWorkerFrame, { kind: "complete" }> | null = null;
    let invalidOutput = false;
    for (const line of child.stdout.split("\n")) {
      if (!line) continue;
      try {
        if (terminal) throw new HealthInputError("invalid_worker_output");
        const value: unknown = JSON.parse(line);
        const frame = decodeFrame(value, request);
        if (frame.kind === "runtime") report = { ...report, runtime: frame.runtime };
        else if (frame.kind === "fixture") report = { ...report, fixture: frame.fixture };
        else if (frame.kind === "result") {
          if (seen.has(frame.name)) throw new HealthInputError("invalid_worker_output");
          seen.add(frame.name);
          Object.assign(results, { [frame.name]: frame.result });
        } else terminal = frame;
      } catch {
        invalidOutput = true;
        break;
      }
    }
    report = { ...report, results };
    if (child.outcome === "unknown" || !child.closed || !child.reaped) {
      report = { ...report, status: "failed", reason: "child_reap_unknown" };
    } else if (
      child.outcome === "timed_out" ||
      child.outcome === "cancelled" ||
      child.outcome === "output_limited"
    ) {
      const stopReason =
        child.outcome === "timed_out"
          ? "worker_deadline"
          : child.outcome === "cancelled"
            ? "cancelled"
            : "output_limit";
      const pending = Object.fromEntries(
        resultNames.map((name) => [
          name,
          results[name].status === "interrupted" || name === "readonly"
            ? component("interrupted", stopReason)
            : results[name],
        ]),
      ) as unknown as HealthResults;
      report = {
        ...report,
        status: child.outcome === "output_limited" ? "partial" : "interrupted",
        reason: stopReason,
        results: pending,
      };
    } else if (child.outcome === "spawn_refused") {
      report = { ...report, status: "refused", reason: "child_spawn_refused" };
    } else if (invalidOutput || !terminal) {
      report = {
        ...report,
        status: "failed",
        reason: invalidOutput ? "invalid_worker_output" : "worker_failed",
      };
    } else {
      const expectedExit =
        terminal.status === "completed"
          ? 0
          : terminal.status === "failed"
            ? 1
            : terminal.status === "refused"
              ? 2
              : 3;
      report =
        child.exitCode === expectedExit
          ? { ...report, status: terminal.status, reason: terminal.reason }
          : { ...report, status: "failed", reason: "worker_failed" };
    }
    if (
      Buffer.byteLength(child.stderr) > request.limits.maxStderrBytes &&
      report.status !== "failed"
    )
      report = { ...report, status: "partial", reason: "stderr_budget_exceeded" };
  } catch (error) {
    const inputReason = error instanceof HealthInputError ? error.reason : "setup_failed";
    report = {
      ...report,
      status:
        inputReason === "cancelled" || inputReason === "worker_deadline"
          ? "interrupted"
          : "refused",
      reason: inputReason,
    };
  } finally {
    if (owner && (!started || (child?.closed && child.reaped && child.outcome !== "unknown"))) {
      const cleanup = disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] });
      report = {
        ...report,
        cleanup: {
          status: cleanup.outcome === "complete" ? "completed" : cleanup.outcome,
          reason:
            cleanup.outcome === "complete"
              ? null
              : cleanup.outcome === "retained"
                ? "cleanup_retained"
                : "cleanup_unknown",
          supervisorRoot: cleanup.outcome === "complete" ? null : supervisorRoot,
          fixtureOwnership: "retained-input",
        },
      };
      if (cleanup.outcome !== "complete" && report.status === "completed")
        report = {
          ...report,
          status: "failed",
          reason: cleanup.outcome === "retained" ? "cleanup_retained" : "cleanup_unknown",
        };
    } else if (supervisorRoot) {
      report = {
        ...report,
        cleanup: {
          status: "unknown",
          reason: started ? "child_reap_unknown" : "cleanup_unknown",
          supervisorRoot,
          fixtureOwnership: "retained-input",
        },
      };
    }
  }
  const elapsedMs = NodePerfHooks.performance.now() - startedAtMs;
  if (elapsedMs > request.limits.deadlineMs && report.status === "completed")
    report = { ...report, status: "interrupted", reason: "deadline_exceeded" };
  return { ...report, elapsedMs };
}

export async function readReceiptStdin(signal: AbortSignal): Promise<string> {
  if (NodeProcess.stdin.isTTY) throw new HealthInputError("invalid_receipt_input");
  const chunks: Buffer[] = [];
  let retained = 0;
  const abort = () => NodeProcess.stdin.destroy(new HealthInputError("worker_deadline"));
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  try {
    for await (const chunk of NodeProcess.stdin) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      if (retained + bytes.length > workerInputBytes)
        throw new HealthInputError("receipt_input_too_large");
      chunks.push(bytes);
      retained += bytes.length;
    }
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
    } catch {
      throw new HealthInputError("invalid_receipt_input");
    }
  } finally {
    signal.removeEventListener("abort", abort);
  }
}

if (import.meta.main) {
  const startedAtMs = NodePerfHooks.performance.now();
  let request: HealthRequest | null = null;
  let report: HealthEnvelope;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  const cancel = () => controller.abort(new HealthInputError("cancelled"));
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    request = parseHealthArguments(NodeProcess.argv.slice(2));
    timer = setTimeout(
      () => controller.abort(new HealthInputError("worker_deadline")),
      request.limits.deadlineMs,
    );
    const input = parseReceiptInput(
      await readReceiptStdin(controller.signal),
      request.fixtureBinding,
    );
    report = await runSqliteHealth(request, input, { signal: controller.signal, startedAtMs });
  } catch (error) {
    const failure =
      controller.signal.aborted && controller.signal.reason instanceof HealthInputError
        ? controller.signal.reason.reason
        : error instanceof HealthInputError
          ? error.reason
          : "invalid_receipt_input";
    report = {
      ...initialReport(request),
      status: failure === "worker_deadline" || failure === "cancelled" ? "interrupted" : "refused",
      reason: failure,
      elapsedMs: NodePerfHooks.performance.now() - startedAtMs,
    };
  } finally {
    clearTimeout(timer);
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
  const encoded = encodeHealthEnvelope(report);
  NodeProcess.stdout.write(encoded.text);
  process.exitCode = healthExitCode(encoded.report);
}
