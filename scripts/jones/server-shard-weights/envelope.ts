// @effect-diagnostics nodeBuiltinImport:off - The Vitest sequencer and CI scripts read weights synchronously, before any Effect runtime exists.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";

/** A combined weights file produced by CI. Unset or empty keeps the checked-in map. */
export const WEIGHTS_PATH_ENV = "JONES_SERVER_SHARD_WEIGHTS_PATH";
/** SHA-256 of the file at `WEIGHTS_PATH_ENV`; every shard must load the same bytes. */
export const WEIGHTS_DIGEST_ENV = "JONES_SERVER_SHARD_WEIGHTS_DIGEST";
/** Where the CI progress reporter writes this shard's timing capture. */
export const TIMINGS_PATH_ENV = "JONES_SERVER_SHARD_TIMINGS_PATH";
/** The CI run that produced the weights at `WEIGHTS_PATH_ENV`. */
export const WEIGHTS_SOURCE_RUN_ENV = "JONES_SERVER_SHARD_WEIGHTS_SOURCE_RUN_ID";

export const WEIGHTS_SCHEMA = "jones.server-shard-weights/v1";
export const MAX_WEIGHTS_BYTES = 1_000_000;
export const MAX_WEIGHT_ENTRIES = 10_000;
export const MAX_WEIGHT_SECONDS = 600;

// The checked-in map records test time only. CI spends about 0.2s per server
// test file on imports and setup, which those recorded times leave out.
const CHECKED_IN_OVERHEAD_SECONDS = 0.25;
// Runtime weights are measured wall time, imports included, so a file without
// one is charged as a typical fast file.
const UNKNOWN_FILE_SECONDS = 0.6;

export type WeightsInput =
  | { mode: "checked-in" }
  | { mode: "runtime"; sourceRunId: string; digest: string };

export interface WeightsSource {
  repository: string;
  workflowRef: string;
  runId: string;
  runAttempt: number;
  event: string;
  ref: string;
  headSha: string;
  shardCount: number;
  fileCount: number;
  manifestDigest: string;
  /** The weights the measured run itself was sharded with. */
  weightsInput: WeightsInput;
}

export interface WeightsEnvelope {
  schema: typeof WEIGHTS_SCHEMA;
  semantics: "wall-seconds";
  source: WeightsSource;
  /** Server-relative test file path to measured wall seconds, keys sorted. */
  weights: Record<string, number>;
}

function fail(message: string): never {
  throw new Error(`Invalid server shard weights: ${message}`);
}

function object(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail(`${what} is not an object`);
  }
  return value as Record<string, unknown>;
}

/** Only these exact keys, so an unknown field is a format change rather than ignored data. */
export function exactObject(value: unknown, keys: readonly string[], what: string) {
  const record = object(value, what);
  const actual = Object.keys(record);
  if (actual.length !== keys.length || !actual.every((key) => keys.includes(key))) {
    fail(`${what} must have exactly ${keys.join(", ")}`);
  }
  return record;
}

/** A normalized relative path with no empty, `.` or `..` segments. */
export function safeRelativePath(value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 512 ||
    /[\\:]/.test(value) ||
    [...value].some((char) => char.charCodeAt(0) < 0x20 || char.charCodeAt(0) === 0x7f) ||
    value.split("/").some((part) => !part || part === "." || part === "..")
  ) {
    throw new Error(`Unsafe test file path: ${JSON.stringify(value)}`);
  }
  return value;
}

export function compareStrings(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

export function sha256(text: string | Uint8Array): string {
  return NodeCrypto.createHash("sha256").update(text).digest("hex");
}

export function weightsDigest(text: string): string {
  return sha256(text);
}

/** Digest of a discovered file list, independent of the order it was discovered in. */
export function manifestDigest(paths: readonly string[]): string {
  return sha256(paths.toSorted(compareStrings).join("\n") + "\n");
}

function text(value: unknown, what: string, pattern?: RegExp): string {
  if (typeof value !== "string" || value === "" || (pattern && !pattern.test(value))) {
    fail(`invalid ${what}`);
  }
  return value;
}

function integer(value: unknown, what: string, max: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || value > max) {
    fail(`invalid ${what}`);
  }
  return value;
}

export const SHA_PATTERN = /^[0-9a-f]{40}$/;
export const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
export const RUN_ID_PATTERN = /^[1-9][0-9]{0,19}$/;

export function parseWeightsInput(value: unknown): WeightsInput {
  const mode = object(value, "weightsInput").mode;
  if (mode === "checked-in") {
    exactObject(value, ["mode"], "weightsInput");
    return { mode };
  }
  if (mode !== "runtime") fail("unknown weightsInput mode");
  const input = exactObject(value, ["mode", "sourceRunId", "digest"], "weightsInput");
  return {
    mode: "runtime",
    sourceRunId: text(input.sourceRunId, "weightsInput.sourceRunId", RUN_ID_PATTERN),
    digest: text(input.digest, "weightsInput.digest", DIGEST_PATTERN),
  };
}

/** Parses and fully validates a combined weights file. Throws on any problem. */
export function parseWeightsEnvelope(input: string): WeightsEnvelope {
  if (Buffer.byteLength(input, "utf8") > MAX_WEIGHTS_BYTES) fail("file exceeds the size limit");
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch {
    fail("not JSON");
  }
  const envelope = exactObject(parsed, ["schema", "semantics", "source", "weights"], "envelope");
  if (envelope.schema !== WEIGHTS_SCHEMA) fail("unknown schema");
  if (envelope.semantics !== "wall-seconds") fail("unknown semantics");
  const source = exactObject(
    envelope.source,
    [
      "repository",
      "workflowRef",
      "runId",
      "runAttempt",
      "event",
      "ref",
      "headSha",
      "shardCount",
      "fileCount",
      "manifestDigest",
      "weightsInput",
    ],
    "source",
  );
  const entries: [string, number][] = [];
  for (const [key, seconds] of Object.entries(object(envelope.weights, "weights"))) {
    try {
      safeRelativePath(key);
    } catch {
      fail(`unsafe path ${JSON.stringify(key)}`);
    }
    if (
      typeof seconds !== "number" ||
      !Number.isFinite(seconds) ||
      seconds < 0 ||
      seconds > MAX_WEIGHT_SECONDS
    ) {
      fail(`weight for ${key} out of range`);
    }
    entries.push([key, seconds]);
  }
  if (entries.length === 0 || entries.length > MAX_WEIGHT_ENTRIES) {
    fail("weight count out of range");
  }
  const keys = entries.map(([key]) => key);
  const result: WeightsEnvelope = {
    schema: WEIGHTS_SCHEMA,
    semantics: "wall-seconds",
    source: {
      repository: text(source.repository, "repository", /^[\w.-]+\/[\w.-]+$/),
      workflowRef: text(source.workflowRef, "workflowRef"),
      runId: text(source.runId, "runId", RUN_ID_PATTERN),
      runAttempt: integer(source.runAttempt, "runAttempt", 1000),
      event: text(source.event, "event"),
      ref: text(source.ref, "ref"),
      headSha: text(source.headSha, "headSha", SHA_PATTERN),
      shardCount: integer(source.shardCount, "shardCount", 64),
      fileCount: integer(source.fileCount, "fileCount", MAX_WEIGHT_ENTRIES),
      manifestDigest: text(source.manifestDigest, "manifestDigest", DIGEST_PATTERN),
      weightsInput: parseWeightsInput(source.weightsInput),
    },
    weights: Object.fromEntries(entries.toSorted(([a], [b]) => compareStrings(a, b))),
  };
  if (result.source.fileCount !== keys.length) fail("fileCount does not match the weights");
  if (result.source.manifestDigest !== manifestDigest(keys)) {
    fail("weights do not cover exactly the recorded file list");
  }
  return result;
}

/** Same input, same bytes: fixed key order, sorted weights, trailing newline. Validates it. */
export function serializeWeightsEnvelope(envelope: WeightsEnvelope): string {
  const { source } = envelope;
  const weightsInput =
    source.weightsInput.mode === "runtime"
      ? {
          mode: "runtime",
          sourceRunId: source.weightsInput.sourceRunId,
          digest: source.weightsInput.digest,
        }
      : { mode: "checked-in" };
  const output = `${JSON.stringify(
    {
      schema: envelope.schema,
      semantics: envelope.semantics,
      source: {
        repository: source.repository,
        workflowRef: source.workflowRef,
        runId: source.runId,
        runAttempt: source.runAttempt,
        event: source.event,
        ref: source.ref,
        headSha: source.headSha,
        shardCount: source.shardCount,
        fileCount: source.fileCount,
        manifestDigest: source.manifestDigest,
        weightsInput,
      },
      weights: Object.fromEntries(
        Object.entries(envelope.weights).toSorted(([a], [b]) => compareStrings(a, b)),
      ),
    },
    null,
    2,
  )}\n`;
  parseWeightsEnvelope(output);
  return output;
}

/**
 * The per-file cost the server sequencer uses. With no weights file it reads the
 * checked-in map the original way: recorded test time plus fixed overhead. With
 * one it reads measured wall seconds and charges unknown files a typical cost. A
 * file that is named but missing, invalid, or does not match its digest throws,
 * so a shard can never quietly fall back to different weights than its peers.
 */
export function resolveSequencerWeight(
  env: Readonly<Record<string, string | undefined>>,
  checkedIn: Readonly<Record<string, number>>,
): (key: string) => number {
  const path = env[WEIGHTS_PATH_ENV];
  if (!path) {
    return (key) =>
      (Object.hasOwn(checkedIn, key) ? (checkedIn[key] ?? 0) : 0) + CHECKED_IN_OVERHEAD_SECONDS;
  }
  const expectedDigest = env[WEIGHTS_DIGEST_ENV];
  if (!expectedDigest) throw new Error(`${WEIGHTS_DIGEST_ENV} must accompany ${WEIGHTS_PATH_ENV}`);
  if (NodeFS.statSync(path).size > MAX_WEIGHTS_BYTES) fail("file exceeds the size limit");
  const input = NodeFS.readFileSync(path, "utf8");
  if (weightsDigest(input) !== expectedDigest) {
    throw new Error(`${WEIGHTS_PATH_ENV} does not match ${WEIGHTS_DIGEST_ENV}`);
  }
  const { weights } = parseWeightsEnvelope(input);
  return (key) => (Object.hasOwn(weights, key) ? (weights[key] ?? 0) : UNKNOWN_FILE_SECONDS);
}
