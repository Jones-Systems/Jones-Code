import {
  CAPTURE_IDENTITY_FILES,
  CAPTURE_SCHEMA,
  type CaptureFilters,
  type CaptureHeader,
  type CaptureModule,
  type CaptureRecord,
  type CaptureTerminal,
  type ModuleEvent,
} from "./capture.ts";
import {
  DIGEST_PATTERN,
  MAX_WEIGHT_ENTRIES,
  MAX_WEIGHT_SECONDS,
  RUN_ID_PATTERN,
  SHA_PATTERN,
  WEIGHTS_SCHEMA,
  compareStrings,
  exactObject,
  manifestDigest,
  parseWeightsInput,
  safeRelativePath,
  serializeWeightsEnvelope,
  type WeightsEnvelope,
} from "./envelope.ts";

export type TimingRejectionCode =
  | "no-captures"
  | "truncated"
  | "malformed"
  | "unsupported-version"
  | "unsafe-path"
  | "source-mismatch"
  | "parallel-execution"
  | "filtered-invocation"
  | "shard-count-mismatch"
  | "duplicate-shard"
  | "missing-shard"
  | "mixed-identity"
  | "shard-not-passed"
  | "module-error"
  | "event-order"
  | "overlapping-modules"
  | "unlisted-file"
  | "duplicate-file"
  | "incomplete-coverage"
  | "out-of-range";

/** A capture set that cannot be trusted as one complete, unfiltered measurement. */
export class TimingRejection extends Error {
  readonly code: TimingRejectionCode;

  constructor(code: TimingRejectionCode, message: string) {
    super(`${code}: ${message}`);
    this.name = "TimingRejection";
    this.code = code;
  }
}

export interface ShardOverhead {
  shardIndex: number;
  files: number;
  /** Run start to the first file being queued: pool and global setup. */
  startupMs: number;
  /** Last file end to the terminal record: teardown and reporting. */
  finalTailMs: number;
  elapsedMs: number;
}

function reject(code: TimingRejectionCode, message: string): never {
  throw new TimingRejection(code, message);
}

/** Runs a format check from the envelope helpers, reporting its failure under one code. */
function checked<T>(code: TimingRejectionCode, check: () => T): T {
  try {
    return check();
  } catch (error) {
    if (error instanceof TimingRejection) throw error;
    return reject(code, error instanceof Error ? error.message : String(error));
  }
}

function string(value: unknown, what: string, pattern?: RegExp): string {
  if (typeof value !== "string" || (pattern !== undefined && !pattern.test(value))) {
    reject("malformed", `invalid ${what}`);
  }
  return value;
}

function nonEmpty(value: unknown, what: string): string {
  return string(value, what, /./);
}

function positiveInteger(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    reject("malformed", `invalid ${what}`);
  }
  return value;
}

function count(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    reject("malformed", `invalid ${what}`);
  }
  return value;
}

function time(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    reject("malformed", `invalid ${what}`);
  }
  return value;
}

function strings(value: unknown, what: string): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    reject("malformed", `invalid ${what}`);
  }
  return value;
}

function paths(value: unknown, what: string): string[] {
  const result = strings(value, what).map((path) =>
    checked("unsafe-path", () => safeRelativePath(path)),
  );
  if (new Set(result).size !== result.length) reject("duplicate-file", `duplicate path in ${what}`);
  return result;
}

function record(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    reject("malformed", `${what} is not an object`);
  }
  return value as Record<string, unknown>;
}

function shape(value: unknown, keys: readonly string[], what: string) {
  return checked("malformed", () => exactObject(value, keys, what));
}

const HEADER_KEYS = [
  "type",
  "schema",
  "checkoutSha",
  "githubSha",
  "repository",
  "workflowRef",
  "event",
  "ref",
  "runId",
  "runAttempt",
  "shardIndex",
  "shardCount",
  "runner",
  "tools",
  "identities",
  "fileParallelism",
  "filters",
  "weightsInput",
  "manifest",
] as const;

function parseFilters(value: unknown): CaptureFilters {
  const filters = shape(
    value,
    [
      "filenamePatterns",
      "testNamePattern",
      "projects",
      "tagsFilter",
      "changed",
      "related",
      "targetedFiles",
    ],
    "filters",
  );
  const result: CaptureFilters = {
    filenamePatterns: strings(filters.filenamePatterns, "filenamePatterns"),
    testNamePattern:
      filters.testNamePattern === null ? null : string(filters.testNamePattern, "testNamePattern"),
    projects: strings(filters.projects, "projects"),
    tagsFilter: strings(filters.tagsFilter, "tagsFilter"),
    changed: filters.changed === null ? null : string(filters.changed, "changed"),
    related: strings(filters.related, "related"),
    targetedFiles: count(filters.targetedFiles, "targetedFiles"),
  };
  if (
    result.filenamePatterns.length > 0 ||
    result.testNamePattern !== null ||
    result.projects.length > 0 ||
    result.tagsFilter.length > 0 ||
    result.changed !== null ||
    result.related.length > 0 ||
    result.targetedFiles > 0
  ) {
    reject("filtered-invocation", "the measured run narrowed which files or tests ran");
  }
  return result;
}

function parseHeader(value: Record<string, unknown>): CaptureHeader {
  if (value.schema !== CAPTURE_SCHEMA) reject("unsupported-version", "unknown capture schema");
  const header = shape(value, HEADER_KEYS, "header");
  const runner = shape(header.runner, ["image", "imageVersion", "release"], "runner");
  const tools = shape(header.tools, ["node", "vitest", "vitePlus"], "tools");
  const identities = shape(header.identities, CAPTURE_IDENTITY_FILES, "identities");
  const manifest = paths(header.manifest, "manifest");
  if (manifest.length === 0 || manifest.length > MAX_WEIGHT_ENTRIES) {
    reject("malformed", "manifest size out of range");
  }
  if (header.fileParallelism !== false) reject("parallel-execution", "files did not run serially");
  const result: CaptureHeader = {
    type: "header",
    schema: CAPTURE_SCHEMA,
    checkoutSha: string(header.checkoutSha, "checkoutSha", SHA_PATTERN),
    githubSha: string(header.githubSha, "githubSha", SHA_PATTERN),
    repository: string(header.repository, "repository", /^[\w.-]+\/[\w.-]+$/),
    workflowRef: nonEmpty(header.workflowRef, "workflowRef"),
    event: nonEmpty(header.event, "event"),
    ref: nonEmpty(header.ref, "ref"),
    runId: string(header.runId, "runId", RUN_ID_PATTERN),
    runAttempt: positiveInteger(header.runAttempt, "runAttempt"),
    shardIndex: positiveInteger(header.shardIndex, "shardIndex"),
    shardCount: positiveInteger(header.shardCount, "shardCount"),
    runner: {
      image: string(runner.image, "runner.image"),
      imageVersion: string(runner.imageVersion, "runner.imageVersion"),
      release: string(runner.release, "runner.release"),
    },
    tools: {
      node: nonEmpty(tools.node, "tools.node"),
      vitest: nonEmpty(tools.vitest, "tools.vitest"),
      vitePlus: nonEmpty(tools.vitePlus, "tools.vitePlus"),
    },
    identities: Object.fromEntries(
      CAPTURE_IDENTITY_FILES.map((path) => [
        path,
        string(identities[path], `identity of ${path}`, DIGEST_PATTERN),
      ]),
    ),
    fileParallelism: false,
    filters: parseFilters(header.filters),
    weightsInput: checked("malformed", () => parseWeightsInput(header.weightsInput)),
    manifest,
  };
  if (result.shardIndex > result.shardCount) reject("malformed", "shard index exceeds shard count");
  if (result.checkoutSha !== result.githubSha) {
    reject("source-mismatch", "the checkout is not the commit the run was triggered for");
  }
  return result;
}

const MODULE_EVENTS: readonly ModuleEvent[] = ["queued", "collected", "start", "end"];
const DIAGNOSTIC_KEYS = [
  "duration",
  "setupDuration",
  "collectDuration",
  "prepareDuration",
  "environmentSetupDuration",
] as const;
const TERMINAL_REASONS = ["passed", "failed", "interrupted"] as const;
const MODULE_STATES = ["queued", "pending", "passed", "failed", "skipped"];

function parseModule(input: Record<string, unknown>): CaptureModule {
  const keys = ["type", "event", "path", "elapsedMs", "state", "errorCount", "errors"];
  const value = shape(input, input.event === "end" ? [...keys, "diagnostic"] : keys, "module");
  const event = MODULE_EVENTS.find((candidate) => candidate === value.event);
  if (event === undefined) reject("malformed", "unknown module event");
  const state = string(value.state, "module state");
  if (!MODULE_STATES.includes(state)) reject("malformed", "unknown module state");
  const errorCount = count(value.errorCount, "errorCount");
  const errors = strings(value.errors, "module errors");
  if (errors.length > errorCount) reject("malformed", "more module errors than errorCount");
  const result: CaptureModule = {
    type: "module",
    event,
    path: checked("unsafe-path", () => safeRelativePath(value.path)),
    elapsedMs: time(value.elapsedMs, "elapsedMs"),
    state,
    errorCount,
    errors,
  };
  if (event === "end") {
    const diagnostic = shape(value.diagnostic, DIAGNOSTIC_KEYS, "diagnostic");
    result.diagnostic = Object.fromEntries(
      DIAGNOSTIC_KEYS.map((key) => [key, time(diagnostic[key], key)]),
    ) as Record<(typeof DIAGNOSTIC_KEYS)[number], number>;
  }
  return result;
}

function parseTerminal(input: Record<string, unknown>): CaptureTerminal {
  const value = shape(
    input,
    ["type", "reason", "unhandledErrorCount", "completedFiles", "elapsedMs"],
    "terminal",
  );
  const reason = TERMINAL_REASONS.find((item) => item === value.reason);
  if (reason === undefined) reject("malformed", "unknown terminal reason");
  return {
    type: "terminal",
    reason,
    unhandledErrorCount: count(value.unhandledErrorCount, "unhandledErrorCount"),
    completedFiles: paths(value.completedFiles, "completedFiles"),
    elapsedMs: time(value.elapsedMs, "terminal elapsedMs"),
  };
}

/** Parses one shard's capture: exactly one header first and one terminal record last. */
export function parseCapture(text: string): CaptureRecord[] {
  if (text === "") reject("truncated", "empty capture");
  if (!text.endsWith("\n")) reject("truncated", "the final record is incomplete");
  const records = text
    .slice(0, -1)
    .split("\n")
    .map((line, index): CaptureRecord => {
      let value: unknown;
      try {
        value = JSON.parse(line);
      } catch {
        reject("malformed", `line ${index + 1} is not JSON`);
      }
      const parsed = record(value, `line ${index + 1}`);
      if (parsed.type === "header") {
        if (index !== 0) reject("malformed", "header is not the first record");
        return parseHeader(parsed);
      }
      if (index === 0) reject("malformed", "capture does not start with a header");
      if (parsed.type === "module") return parseModule(parsed);
      if (parsed.type === "terminal") return parseTerminal(parsed);
      return reject("malformed", `line ${index + 1} has an unknown record type`);
    });
  const terminals = records.filter((item) => item.type === "terminal");
  if (terminals.length === 0 || records.at(-1)?.type !== "terminal") {
    reject("truncated", "the capture has no final terminal record");
  }
  if (terminals.length > 1) reject("malformed", "more than one terminal record");
  return records;
}

/** Stable JSON with sorted object keys, so two headers compare by content. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map((item) => canonical(item)).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value).toSorted(([a], [b]) => compareStrings(a, b));
    const fields = entries.map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`);
    return `{${fields.join(",")}}`;
  }
  return JSON.stringify(value);
}

/** What every shard of one measurement shares. The runner image may differ between machines. */
function runIdentity(header: CaptureHeader): string {
  return canonical({
    ...header,
    shardIndex: null,
    runner: null,
    manifest: header.manifest.toSorted(compareStrings),
  });
}

/**
 * Combines every shard's capture into one complete wall-time weights envelope.
 *
 * A file's weight is the wall time between the previous file's end and its own
 * end in the same shard; the first file is timed from when it was queued. That
 * charges collection and imports to the file that caused them. Run startup and
 * the tail after the last file are reported separately as overhead.
 *
 * Throws `TimingRejection` unless the captures are exactly `shardCount` passing
 * serial shards of one unfiltered run whose files cover the discovered list once.
 */
export function aggregateServerShardCaptures(
  texts: string[],
  { shardCount }: { shardCount: number },
): { envelope: WeightsEnvelope; overhead: ShardOverhead[] } {
  if (texts.length === 0) reject("no-captures", "no shard captures were supplied");
  let reference: CaptureHeader | undefined;
  let identity: string | undefined;
  const shards = new Set<number>();
  const weights = new Map<string, number>();
  const overhead: ShardOverhead[] = [];

  for (const text of texts) {
    const records = parseCapture(text);
    const header = records[0] as CaptureHeader;
    const terminal = records.at(-1) as CaptureTerminal;
    if (header.shardCount !== shardCount) {
      reject(
        "shard-count-mismatch",
        `capture has ${header.shardCount} shards, expected ${shardCount}`,
      );
    }
    const current = runIdentity(header);
    if (identity !== undefined && identity !== current) {
      reject(
        "mixed-identity",
        "shards disagree on run, source, tools, filters, weights or file list",
      );
    }
    identity = current;
    reference ??= header;
    if (shards.has(header.shardIndex)) {
      reject("duplicate-shard", `shard ${header.shardIndex} appears twice`);
    }
    shards.add(header.shardIndex);
    if (terminal.reason !== "passed" || terminal.unhandledErrorCount !== 0) {
      reject("shard-not-passed", `shard ${header.shardIndex} ended ${terminal.reason}`);
    }

    const manifest = new Set(header.manifest);
    const stages = new Map<string, number>();
    const completed: string[] = [];
    let previousTime = 0;
    let firstQueued: number | undefined;
    let previousEnd: number | undefined;
    let active: string | undefined;
    for (const record of records.slice(1, -1)) {
      if (record.type !== "module") {
        reject("malformed", "unexpected record between header and terminal");
      }
      if (record.elapsedMs < previousTime) reject("event-order", "events are out of time order");
      previousTime = record.elapsedMs;
      if (!manifest.has(record.path)) {
        reject("unlisted-file", `${record.path} is not in the file list`);
      }
      if (record.errorCount !== 0) reject("module-error", `${record.path} reported errors`);
      const stage = MODULE_EVENTS.indexOf(record.event);
      if (stage !== (stages.get(record.path) ?? -1) + 1) {
        reject("event-order", `${record.path} has a missing or repeated ${record.event} event`);
      }
      stages.set(record.path, stage);
      if (record.event === "queued") firstQueued ??= record.elapsedMs;
      if (record.event === "collected") {
        if (active !== undefined) {
          reject("overlapping-modules", `${record.path} overlaps ${active}`);
        }
        active = record.path;
      }
      if ((record.event === "start" || record.event === "end") && active !== record.path) {
        reject("overlapping-modules", `${record.path} ran while another file was active`);
      }
      if (record.event === "end") {
        if (record.state !== "passed" && record.state !== "skipped") {
          reject("module-error", `${record.path} ended ${record.state}`);
        }
        if (weights.has(record.path)) {
          reject("duplicate-file", `${record.path} ran in more than one shard`);
        }
        // Wall time as the reporter saw it, not CPU time; nested import
        // diagnostics are not summed, since collection already includes them.
        const intervalMs = record.elapsedMs - (previousEnd ?? firstQueued ?? record.elapsedMs);
        const seconds = Math.round(intervalMs) / 1000;
        if (seconds > MAX_WEIGHT_SECONDS) reject("out-of-range", `${record.path} took ${seconds}s`);
        weights.set(record.path, seconds);
        previousEnd = record.elapsedMs;
        completed.push(record.path);
        active = undefined;
      }
    }
    if ([...stages.values()].some((stage) => stage !== MODULE_EVENTS.length - 1)) {
      reject("event-order", `shard ${header.shardIndex} has an unfinished file`);
    }
    if (
      canonical(completed.toSorted(compareStrings)) !==
      canonical(terminal.completedFiles.toSorted(compareStrings))
    ) {
      reject(
        "malformed",
        `shard ${header.shardIndex} terminal record disagrees with its file events`,
      );
    }
    if (terminal.elapsedMs < previousTime) reject("event-order", "terminal precedes file events");
    overhead.push({
      shardIndex: header.shardIndex,
      files: completed.length,
      startupMs: Math.round(firstQueued ?? terminal.elapsedMs),
      finalTailMs: Math.round(previousEnd === undefined ? 0 : terminal.elapsedMs - previousEnd),
      elapsedMs: Math.round(terminal.elapsedMs),
    });
  }

  const header = reference as CaptureHeader;
  if (shards.size !== shardCount) {
    reject("missing-shard", `received ${shards.size} of ${shardCount} shards`);
  }
  const covered = [...weights.keys()].toSorted(compareStrings);
  const discovered = header.manifest.toSorted(compareStrings);
  if (canonical(covered) !== canonical(discovered)) {
    reject(
      "incomplete-coverage",
      `shards ran ${covered.length} of ${discovered.length} discovered files`,
    );
  }

  const envelope: WeightsEnvelope = {
    schema: WEIGHTS_SCHEMA,
    semantics: "wall-seconds",
    source: {
      repository: header.repository,
      workflowRef: header.workflowRef,
      runId: header.runId,
      runAttempt: header.runAttempt,
      event: header.event,
      ref: header.ref,
      headSha: header.githubSha,
      shardCount,
      fileCount: discovered.length,
      manifestDigest: manifestDigest(discovered),
      weightsInput: header.weightsInput,
    },
    weights: Object.fromEntries(covered.map((path) => [path, weights.get(path) ?? 0])),
  };
  checked("malformed", () => serializeWeightsEnvelope(envelope));
  return { envelope, overhead: overhead.toSorted((a, b) => a.shardIndex - b.shardIndex) };
}
