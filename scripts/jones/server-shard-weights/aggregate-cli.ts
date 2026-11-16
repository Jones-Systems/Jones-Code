// @effect-diagnostics nodeBuiltinImport:off - The one-shot CI aggregate step reads downloaded captures and writes step outputs before any Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { TimingRejection, aggregateServerShardCaptures, type ShardOverhead } from "./aggregate.ts";
import { serializeWeightsEnvelope, weightsDigest } from "./envelope.ts";

/** One shard's capture file, inside its `server-timings-a<attempt>-s<i>-of-<n>` artifact. */
export const CAPTURE_FILE_NAME = "server-timings.ndjson";
const MAX_CAPTURE_BYTES = 16_000_000;

export function captureArtifactName(
  attempt: number,
  shardIndex: number,
  shardCount: number,
): string {
  return `server-timings-a${attempt}-s${shardIndex}-of-${shardCount}`;
}

export interface AggregateInputs {
  /** `needs.test_server.result`. */
  testResult: string;
  shardCount: number;
  runAttempt: number;
  runId: string;
  repository: string;
  sha: string;
  /** What the picker chose for this run; every shard must have used exactly it. */
  selection: { mode: string; digest: string; sourceRunId: string };
  /** Shard index to capture text, for this attempt's artifacts that exist. */
  captures: ReadonlyMap<number, string>;
}

export type AggregateOutcome =
  | { status: "ineligible"; exitCode: 0; reason: string }
  | { status: "rejected"; exitCode: 1; reason: string }
  | {
      status: "published";
      exitCode: 0;
      reason: string;
      text: string;
      digest: string;
      overhead: ShardOverhead[];
    };

/**
 * Decides whether this run's captures become the next runs' weights. A run whose
 * server tests did not all pass, or a rerun attempt that repeated only some
 * shards, is simply not a measurement. Passing shards whose captures fail the
 * strict checks are a CI defect and fail the job.
 */
export function decideAggregate(inputs: AggregateInputs): AggregateOutcome {
  if (inputs.testResult !== "success") {
    return {
      status: "ineligible",
      exitCode: 0,
      reason: `server tests ended ${inputs.testResult || "unknown"}`,
    };
  }
  if (inputs.captures.size < inputs.shardCount && inputs.runAttempt > 1) {
    return {
      status: "ineligible",
      exitCode: 0,
      reason: `attempt ${inputs.runAttempt} reran ${inputs.captures.size} of ${inputs.shardCount} shards`,
    };
  }
  const rejected = (reason: string) => ({ status: "rejected", exitCode: 1, reason }) as const;
  if (inputs.captures.size < inputs.shardCount) {
    return rejected(
      `missing-shard: ${inputs.captures.size} of ${inputs.shardCount} shards uploaded captures`,
    );
  }
  let result: ReturnType<typeof aggregateServerShardCaptures>;
  try {
    result = aggregateServerShardCaptures(
      [...inputs.captures.entries()].toSorted(([a], [b]) => a - b).map(([, text]) => text),
      { shardCount: inputs.shardCount },
    );
  } catch (error) {
    if (error instanceof TimingRejection) return rejected(error.message);
    throw error;
  }
  const { source } = result.envelope;
  const expectedInput =
    inputs.selection.mode === "runtime"
      ? {
          mode: "runtime",
          sourceRunId: inputs.selection.sourceRunId,
          digest: inputs.selection.digest,
        }
      : { mode: "checked-in" };
  const mismatch = [
    source.runId === inputs.runId ? undefined : "run ID",
    source.runAttempt === inputs.runAttempt ? undefined : "attempt",
    source.repository === inputs.repository ? undefined : "repository",
    source.headSha === inputs.sha ? undefined : "head SHA",
    JSON.stringify(source.weightsInput) === JSON.stringify(expectedInput)
      ? undefined
      : "weights input",
  ].filter((field) => field !== undefined);
  if (mismatch.length > 0) {
    return rejected(`source-mismatch: captures record a different ${mismatch.join(", ")}`);
  }
  const text = serializeWeightsEnvelope(result.envelope);
  return {
    status: "published",
    exitCode: 0,
    reason: `${source.fileCount} files over ${source.shardCount} shards`,
    text,
    digest: weightsDigest(text),
    overhead: result.overhead,
  };
}

/** Reads this attempt's captures as download-artifact extracts them: one directory per artifact. */
export function readCaptures(
  directory: string,
  { attempt, shardCount }: { attempt: number; shardCount: number },
): Map<number, string> {
  const captures = new Map<number, string>();
  for (let shard = 1; shard <= shardCount; shard++) {
    const path = NodePath.join(
      directory,
      captureArtifactName(attempt, shard, shardCount),
      CAPTURE_FILE_NAME,
    );
    if (!NodeFS.existsSync(path)) continue;
    if (NodeFS.statSync(path).size > MAX_CAPTURE_BYTES) {
      throw new TimingRejection("malformed", `shard ${shard} capture exceeds the size limit`);
    }
    captures.set(shard, NodeFS.readFileSync(path, "utf8"));
  }
  return captures;
}

const seconds = (ms: number) => (ms / 1000).toFixed(1);

export function formatSummary(outcome: AggregateOutcome): string {
  const lines = [
    "### Server shard timings",
    "",
    `- Result: \`${outcome.status}\` (${outcome.reason})`,
  ];
  if (outcome.status === "published") {
    lines.push(
      `- Digest: \`${outcome.digest}\``,
      "",
      "| Shard | Files | Startup (s) | Final tail (s) | Elapsed (s) |",
      "| ---: | ---: | ---: | ---: | ---: |",
      ...outcome.overhead.map((shard) => {
        const cells = [
          shard.shardIndex,
          shard.files,
          seconds(shard.startupMs),
          seconds(shard.finalTailMs),
          seconds(shard.elapsedMs),
        ];
        return `| ${cells.join(" | ")} |`;
      }),
    );
  }
  return `${lines.join("\n")}\n`;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

function main(): number {
  const testResult = process.env.TEST_SERVER_RESULT ?? "";
  const shardCount = Number(process.env.SHARD_COUNT);
  const runAttempt = Number(required("GITHUB_RUN_ATTEMPT"));
  if (testResult === "success" && !(Number.isSafeInteger(shardCount) && shardCount > 0)) {
    throw new Error("SHARD_COUNT is not a shard count");
  }
  let outcome: AggregateOutcome;
  try {
    outcome = decideAggregate({
      testResult,
      shardCount,
      runAttempt,
      runId: required("GITHUB_RUN_ID"),
      repository: required("GITHUB_REPOSITORY"),
      sha: required("GITHUB_SHA"),
      selection: {
        mode: process.env.WEIGHTS_MODE ?? "",
        digest: process.env.WEIGHTS_DIGEST ?? "",
        sourceRunId: process.env.WEIGHTS_SOURCE_RUN_ID ?? "",
      },
      // Only read captures when the tests passed; a failed run is not measured.
      captures:
        testResult === "success"
          ? readCaptures(required("CAPTURES_DIR"), { attempt: runAttempt, shardCount })
          : new Map(),
    });
  } catch (error) {
    if (!(error instanceof TimingRejection)) throw error;
    outcome = { status: "rejected", exitCode: 1, reason: error.message };
  }
  if (outcome.status === "published") NodeFS.writeFileSync(required("WEIGHTS_OUT"), outcome.text);
  const digest = outcome.status === "published" ? outcome.digest : "";
  NodeFS.appendFileSync(
    required("GITHUB_OUTPUT"),
    `eligible=${outcome.status === "published"}\ndigest=${digest}\n`,
  );
  const summary = formatSummary(outcome);
  if (process.env.GITHUB_STEP_SUMMARY) {
    NodeFS.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
  }
  (outcome.exitCode === 0 ? process.stdout : process.stderr).write(summary);
  return outcome.exitCode;
}

if (import.meta.main) {
  try {
    process.exitCode = main();
  } catch (error) {
    process.stderr.write(
      `Server shard timing aggregation failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
