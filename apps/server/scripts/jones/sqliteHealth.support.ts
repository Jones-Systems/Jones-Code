// @effect-diagnostics nodeBuiltinImport:off -- This native CLI hashes fixed source files and constructs the canonical guard's explicit path policy without an Effect runtime.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeURL from "node:url";

import type {
  OwnedChildReceipt,
  PerformanceBinding,
  StagingPolicy,
  SyntheticFixtureReceipt,
} from "../../../../scripts/jones/performance/guard.mjs";

export const healthTaskRef = "spec.jones-performance-portfolio#task.d-health.001";
export const repository = "Jones-Systems/Jones-Code";
const scriptDirectory = NodeURL.fileURLToPath(new URL("./", import.meta.url));
export const worktreePath = NodePath.resolve(scriptDirectory, "../../../..");
export const workerPath = NodeURL.fileURLToPath(
  new URL("./sqliteHealth.worker.ts", import.meta.url),
);
export const workerInputBytes = 48 * 1024;
export const envelopeReserveBytes = 8 * 1024;
export const shutdownReserveMs = 500;
export const terminateGraceMs = 50;
export const reapTimeoutMs = 250;

export const includeNames = ["counts", "allocation", "integrity", "foreign-keys"] as const;
export type HealthInclude = (typeof includeNames)[number];
export type HealthStatus = "completed" | "partial" | "refused" | "interrupted" | "failed";
export type ComponentStatus =
  | "completed"
  | "omitted"
  | "unavailable"
  | "interrupted"
  | "truncated"
  | "failed";

export const reasonNames = [
  "invalid_arguments",
  "invalid_receipt_input",
  "receipt_input_too_large",
  "worker_input_too_large",
  "fixture_root_mismatch",
  "fixture_refused",
  "fixture_changed",
  "readonly_open_unavailable",
  "readonly_layout_unqualified",
  "sqlite_query_failed",
  "sqlite_close_failed",
  "dbstat_unavailable",
  "tables_missing",
  "record_limit",
  "diagnostic_limit",
  "integrity_violations",
  "foreign_key_violations",
  "worker_deadline",
  "cancelled",
  "output_limit",
  "stderr_budget_exceeded",
  "invalid_worker_output",
  "worker_failed",
  "child_spawn_refused",
  "child_reap_unknown",
  "cleanup_retained",
  "cleanup_unknown",
  "setup_failed",
  "final_report_limit",
  "deadline_exceeded",
] as const;
export type HealthReason = (typeof reasonNames)[number];

export class HealthInputError extends Error {
  readonly reason: HealthReason;
  constructor(reason: HealthReason) {
    super(reason);
    this.reason = reason;
  }
}

export interface HealthLimits {
  readonly deadlineMs: number;
  readonly maxOutputBytes: number;
  readonly maxStderrBytes: number;
  readonly maxRecords: number;
  readonly maxDiagnostics: number;
}

export const defaultHealthLimits: HealthLimits = {
  deadlineMs: 5_000,
  maxOutputBytes: 256 * 1024,
  maxStderrBytes: 16 * 1024,
  maxRecords: 128,
  maxDiagnostics: 64,
};

export type HealthSchemaProfile = "legacy-v1" | "orchestration-v2";

export interface HealthRequest {
  readonly schemaProfile: HealthSchemaProfile;
  readonly fixtureRoot: string;
  readonly fixtureReceiptSha256: string;
  readonly fixtureBinding: PerformanceBinding;
  readonly include: readonly HealthInclude[];
  readonly limits: HealthLimits;
}

// This is the guard /v1 receipt; fixture release acknowledges the separate producer pin.
export interface HealthReceiptInput {
  readonly receipt: SyntheticFixtureReceipt;
  readonly expectedBinding: PerformanceBinding;
}

export interface HealthWorkerInput {
  readonly schema: "jones.sqlite-health-worker-input/v1";
  readonly request: HealthRequest;
  readonly fixture: HealthReceiptInput;
  readonly policy: StagingPolicy;
}

export interface Component<T> {
  readonly status: ComponentStatus;
  readonly reason: HealthReason | null;
  readonly data: T | null;
}

export interface HealthRuntime {
  readonly node: string;
  readonly platform: string;
  readonly arch: string;
  readonly sqliteVersion: string | null;
  readonly sqliteSourceId: string | null;
}

export interface SidecarSummary {
  readonly kind: "main" | "wal" | "shm" | "journal";
  readonly present: boolean;
  readonly bytes: string | null;
  readonly sha256: string | null;
}

export interface HealthFixtureSummary {
  readonly receiptSha256: string;
  readonly source: PerformanceBinding;
  readonly rootId: string;
  readonly producerStep: string;
  readonly closureId: string;
  readonly closedLayout: readonly SidecarSummary[];
  readonly verifiedFiles: string;
  readonly verifiedBytes: string;
}

export const settingNames = [
  "journalMode",
  "synchronous",
  "foreignKeys",
  "queryOnly",
  "busyTimeoutMs",
  "pageSizeBytes",
  "pageCount",
  "freelistCount",
  "walAutoCheckpointPages",
  "autoVacuum",
  "encoding",
] as const;

export interface MetadataData {
  readonly connectionSettingsOnly: true;
  readonly settings: Readonly<
    Record<(typeof settingNames)[number], string | boolean | number | null>
  >;
  readonly schema: readonly { readonly name: string; readonly type: string }[];
  readonly sidecars: readonly SidecarSummary[];
}

export const countTables = [
  ["events", "orchestration_events"],
  ["commandReceipts", "orchestration_command_receipts"],
  ["projects", "projection_projects"],
  ["threads", "projection_threads"],
  ["messages", "projection_thread_messages"],
  ["activities", "projection_thread_activities"],
  ["turns", "projection_turns"],
  ["projectionState", "projection_state"],
  ["leases", "worktree_ownership_leases"],
] as const;
export const v2CountTables = [
  ["events", "orchestration_events"],
  ["commandReceipts", "orchestration_command_receipts"],
  ["projects", "projection_projects"],
  ["threads", "orchestration_v2_projection_threads"],
  ["messages", "orchestration_v2_projection_messages"],
  ["runs", "orchestration_v2_projection_runs"],
  ["effectOutbox", "orchestration_v2_effect_outbox"],
  ["projectionState", "orchestration_v2_projection_metadata"],
  ["leases", "worktree_ownership_leases"],
] as const;
export const healthCountTables = (profile: HealthSchemaProfile) =>
  profile === "orchestration-v2" ? v2CountTables : countTables;
export type CountName = (typeof countTables | typeof v2CountTables)[number][0];
export interface CountEntry {
  readonly name: CountName;
  readonly status: "completed" | "unavailable" | "failed";
  readonly count: string | null;
}

export interface AllocationData {
  readonly scope: "btree-pages-only";
  readonly excludes: "freelist-and-other-non-btree-pages";
  readonly rows: readonly {
    readonly name: string;
    readonly pages: string;
    readonly bytes: string;
    readonly payloadBytes: string;
    readonly unusedBytes: string;
  }[];
}

export interface IntegrityData {
  readonly check: "full-integrity-check";
  readonly passed: boolean | null;
  readonly observedViolations: string;
  readonly diagnostics: readonly { readonly code: "integrity_violation" }[];
}

export interface ForeignKeysData {
  readonly passed: boolean | null;
  readonly observedViolations: string;
  readonly diagnostics: readonly {
    readonly table: string;
    readonly parent: string;
    readonly constraintIndex: string;
  }[];
}

export interface ReadonlyData {
  readonly unchanged: boolean | null;
  readonly beforeReceiptSha256: string;
  readonly afterReceiptSha256: string | null;
}

export interface HealthResults {
  readonly metadata: Component<MetadataData>;
  readonly counts: Component<readonly CountEntry[]>;
  readonly allocation: Component<AllocationData>;
  readonly integrity: Component<IntegrityData>;
  readonly foreignKeys: Component<ForeignKeysData>;
  readonly readonly: Component<ReadonlyData>;
}

export type HealthResultName = keyof HealthResults;
export const resultNames: readonly HealthResultName[] = [
  "metadata",
  "counts",
  "allocation",
  "integrity",
  "foreignKeys",
  "readonly",
];

export type HealthWorkerFrame =
  | { readonly kind: "runtime"; readonly runtime: HealthRuntime }
  | { readonly kind: "fixture"; readonly fixture: HealthFixtureSummary }
  | {
      readonly kind: "result";
      readonly name: HealthResultName;
      readonly result: HealthResults[HealthResultName];
    }
  | {
      readonly kind: "complete";
      readonly status: HealthStatus;
      readonly reason: HealthReason | null;
    };

export interface HealthEnvelope {
  readonly schema: "jones.sqlite-health/v1";
  readonly schemaProfile: HealthSchemaProfile;
  readonly status: HealthStatus;
  readonly reason: HealthReason | null;
  readonly toolSource: {
    readonly invocation: "native-node-typescript";
    readonly sourceRevision: null;
    readonly buildIdentity: null;
    readonly files: readonly { readonly name: string; readonly sha256: string | null }[];
  };
  readonly fixture: HealthFixtureSummary | null;
  readonly runtime: HealthRuntime;
  readonly limits: HealthLimits & {
    readonly maxWorkerInputBytes: number;
    readonly maxCapturedChildBytes: number;
  };
  readonly elapsedMs: number;
  readonly results: HealthResults;
  readonly child: {
    readonly pid: number | null;
    readonly outcome: OwnedChildReceipt["outcome"] | "not_started";
    readonly exitCode: number | null;
    readonly signal: string | null;
    readonly terminated: boolean;
    readonly escalated: boolean;
    readonly closed: boolean;
    readonly reaped: boolean;
    readonly capturedBytes: string;
    readonly observedBytes: string;
    readonly stderrBytes: string;
    readonly truncated: boolean;
  };
  readonly cleanup: {
    readonly status: "completed" | "retained" | "unknown";
    readonly reason: HealthReason | null;
    readonly supervisorRoot: string | null;
    readonly fixtureOwnership: "retained-input";
  };
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function boundedInteger(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback;
  if (!/^[1-9][0-9]*$/.test(value)) throw new HealthInputError("invalid_arguments");
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max)
    throw new HealthInputError("invalid_arguments");
  return parsed;
}

export function parseHealthArguments(args: readonly string[]): HealthRequest {
  const allowed = new Set([
    "--fixture-root",
    "--fixture-receipt-sha256",
    "--fixture-binding-json",
    "--include",
    "--schema-profile",
    "--deadline-ms",
    "--max-output-bytes",
    "--max-stderr-bytes",
    "--max-records",
    "--max-diagnostics",
  ]);
  const flags = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (
      !flag ||
      !allowed.has(flag) ||
      flags.has(flag) ||
      !value ||
      value.startsWith("--") ||
      value.includes("\0") ||
      Buffer.byteLength(value) > 4096
    )
      throw new HealthInputError("invalid_arguments");
    flags.set(flag, value);
  }
  const fixtureRoot = flags.get("--fixture-root");
  const fixtureReceiptSha256 = flags.get("--fixture-receipt-sha256");
  const fixtureBinding = parseIndependentBinding(flags.get("--fixture-binding-json"));
  if (
    !fixtureRoot ||
    !NodePath.isAbsolute(fixtureRoot) ||
    fixtureRoot.split(NodePath.sep).includes("..") ||
    !fixtureReceiptSha256 ||
    !/^[a-f0-9]{64}$/.test(fixtureReceiptSha256)
  )
    throw new HealthInputError("invalid_arguments");
  const requested = flags.has("--include") ? flags.get("--include")!.split(",") : [];
  if (
    requested.some((item) => !includeNames.some((name) => name === item)) ||
    new Set(requested).size !== requested.length
  )
    throw new HealthInputError("invalid_arguments");
  const schemaProfile = flags.get("--schema-profile") ?? "orchestration-v2";
  if (schemaProfile !== "legacy-v1" && schemaProfile !== "orchestration-v2")
    throw new HealthInputError("invalid_arguments");
  const include = includeNames.filter((name) => requested.includes(name));
  const limits = {
    deadlineMs: boundedInteger(
      flags.get("--deadline-ms"),
      include.length ? 60_000 : 5_000,
      1_000,
      300_000,
    ),
    maxOutputBytes: boundedInteger(
      flags.get("--max-output-bytes"),
      256 * 1024,
      16 * 1024,
      1024 * 1024,
    ),
    maxStderrBytes: boundedInteger(flags.get("--max-stderr-bytes"), 16 * 1024, 1, 64 * 1024),
    maxRecords: boundedInteger(flags.get("--max-records"), 128, 1, 256),
    maxDiagnostics: boundedInteger(flags.get("--max-diagnostics"), 64, 1, 128),
  };
  return { fixtureRoot, fixtureReceiptSha256, fixtureBinding, schemaProfile, include, limits };
}

function parseIndependentBinding(text: string | undefined): PerformanceBinding {
  let binding: unknown;
  try {
    binding = text === undefined ? null : JSON.parse(text);
  } catch {
    throw new HealthInputError("invalid_arguments");
  }
  if (!isRecord(binding)) throw new HealthInputError("invalid_arguments");
  if (
    Object.keys(binding).length !== 4 ||
    binding.repository !== repository ||
    typeof binding.sourceRevision !== "string" ||
    !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(binding.sourceRevision) ||
    typeof binding.taskRef !== "string" ||
    typeof binding.runId !== "string" ||
    [binding.taskRef, binding.runId].some(
      (value) => value.length === 0 || value.includes("\0") || Buffer.byteLength(value) > 512,
    )
  )
    throw new HealthInputError("invalid_arguments");
  return {
    repository,
    sourceRevision: binding.sourceRevision,
    taskRef: binding.taskRef,
    runId: binding.runId,
  };
}

export function parseReceiptInput(
  text: string,
  expectedBinding: PerformanceBinding,
): HealthReceiptInput {
  if (Buffer.byteLength(text) > workerInputBytes)
    throw new HealthInputError("receipt_input_too_large");
  let input: unknown;
  try {
    input = JSON.parse(text);
  } catch {
    throw new HealthInputError("invalid_receipt_input");
  }
  if (!isRecord(input)) throw new HealthInputError("invalid_receipt_input");
  // Only the canonical guard qualifies the receipt, filesystem identities and provenance.
  return { receipt: input as unknown as SyntheticFixtureReceipt, expectedBinding };
}

export function healthPolicy(): StagingPolicy {
  const homePath = NodeOS.homedir();
  return {
    homePath,
    worktreePaths: [worktreePath],
    protectedPaths: [
      NodePath.join(homePath, ".t3"),
      NodePath.join(homePath, ".codex"),
      NodePath.join(homePath, ".ssh"),
      NodePath.join(homePath, ".config"),
      NodePath.join(worktreePath, ".t3"),
    ],
  };
}

export function parentRuntime(): HealthRuntime {
  return {
    node: NodeProcess.versions.node,
    platform: NodeProcess.platform,
    arch: NodeProcess.arch,
    sqliteVersion: null,
    sqliteSourceId: null,
  };
}

export function toolSource(): HealthEnvelope["toolSource"] {
  const sources = [
    ["sqliteHealth.ts", new URL("./sqliteHealth.ts", import.meta.url)],
    ["sqliteHealth.support.ts", new URL("./sqliteHealth.support.ts", import.meta.url)],
    ["sqliteHealth.worker.ts", new URL("./sqliteHealth.worker.ts", import.meta.url)],
    [
      "jones/performance/guard.mjs",
      new URL("../../../../scripts/jones/performance/guard.mjs", import.meta.url),
    ],
    [
      "jones/performance/lifecycle.mjs",
      new URL("../../../../scripts/jones/performance/lifecycle.mjs", import.meta.url),
    ],
  ] as const;
  return {
    invocation: "native-node-typescript",
    sourceRevision: null,
    buildIdentity: null,
    files: sources.map(([name, path]) => {
      try {
        return {
          name,
          sha256: NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex"),
        };
      } catch {
        return { name, sha256: null };
      }
    }),
  };
}

export function component<T>(
  status: ComponentStatus,
  reason: HealthReason | null = null,
  data: T | null = null,
): Component<T> {
  return { status, reason, data };
}

export function emptyHealthResults(include: readonly HealthInclude[]): HealthResults {
  return {
    metadata: component("unavailable"),
    counts: component(include.includes("counts") ? "interrupted" : "omitted"),
    allocation: component(include.includes("allocation") ? "interrupted" : "omitted"),
    integrity: component(include.includes("integrity") ? "interrupted" : "omitted"),
    foreignKeys: component(include.includes("foreign-keys") ? "interrupted" : "omitted"),
    readonly: component("unavailable"),
  };
}

export function childSummary(child: OwnedChildReceipt | null): HealthEnvelope["child"] {
  return {
    pid: child?.pid ?? null,
    outcome: child?.outcome ?? "not_started",
    exitCode: child?.exitCode ?? null,
    signal: child?.signal ?? null,
    terminated: child?.terminated ?? false,
    escalated: child?.escalated ?? false,
    closed: child?.closed ?? true,
    reaped: child?.reaped ?? true,
    capturedBytes: String(child?.capturedBytes ?? 0),
    observedBytes: String(child?.observedBytes ?? 0),
    stderrBytes: String(Buffer.byteLength(child?.stderr ?? "")),
    truncated: child?.truncated ?? false,
  };
}

export function healthExitCode(report: HealthEnvelope): number {
  if (report.cleanup.status === "unknown") return 4;
  if (report.status === "completed") return 0;
  if (report.status === "refused") return 2;
  if (report.status === "partial" || report.status === "interrupted") return 3;
  return 1;
}

export function encodeHealthEnvelope(report: HealthEnvelope): {
  readonly report: HealthEnvelope;
  readonly text: string;
} {
  let candidate = report;
  let text = `${JSON.stringify(candidate)}\n`;
  if (Buffer.byteLength(text) > report.limits.maxOutputBytes) {
    candidate = {
      ...report,
      status: "partial",
      reason: "final_report_limit",
      results: Object.fromEntries(
        resultNames.map((name) => [
          name,
          report.results[name].status === "omitted"
            ? report.results[name]
            : component("truncated", "final_report_limit"),
        ]),
      ) as unknown as HealthResults,
    };
    text = `${JSON.stringify(candidate)}\n`;
  }
  return { report: candidate, text };
}
