import { describe, expect, it } from "vite-plus/test";

import {
  TimingRejection,
  aggregateServerShardCaptures,
  parseCapture,
  type TimingRejectionCode,
} from "./aggregate.ts";
import {
  CAPTURE_IDENTITY_FILES,
  CAPTURE_SCHEMA,
  type CaptureHeader,
  type CaptureModule,
  type CaptureRecord,
  type CaptureTerminal,
} from "./capture.ts";
import { manifestDigest, parseWeightsEnvelope, serializeWeightsEnvelope } from "./envelope.ts";

const SHARDS = 6;
const files = Array.from(
  { length: SHARDS * 2 },
  (_, i) => `src/f${String(i + 1).padStart(2, "0")}.test.ts`,
);

function header(shardIndex: number, overrides: Partial<CaptureHeader> = {}): CaptureHeader {
  return {
    type: "header",
    schema: CAPTURE_SCHEMA,
    checkoutSha: "a".repeat(40),
    githubSha: "a".repeat(40),
    repository: "Jones-Systems/Jones-Code",
    workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/heads/main",
    event: "push",
    ref: "refs/heads/main",
    runId: "4242",
    runAttempt: 1,
    shardIndex,
    shardCount: SHARDS,
    runner: { image: "ubuntu24", imageVersion: `2026101${shardIndex}.1`, release: "6.11" },
    tools: { node: "v24.13.1", vitest: "5.0.1", vitePlus: "1.0.0" },
    identities: Object.fromEntries(CAPTURE_IDENTITY_FILES.map((path) => [path, "b".repeat(64)])),
    fileParallelism: false,
    filters: {
      filenamePatterns: [],
      testNamePattern: null,
      projects: [],
      tagsFilter: [],
      changed: null,
      related: [],
      targetedFiles: 0,
    },
    weightsInput: { mode: "checked-in" },
    manifest: files.toReversed(),
    ...overrides,
  };
}

function events(path: string, queued: number, end: number): CaptureModule[] {
  const base = { type: "module" as const, path, state: "passed", errorCount: 0, errors: [] };
  return [
    { ...base, event: "queued", elapsedMs: queued, state: "queued" },
    { ...base, event: "collected", elapsedMs: queued + 200 },
    { ...base, event: "start", elapsedMs: queued + 201 },
    {
      ...base,
      event: "end",
      elapsedMs: end,
      diagnostic: {
        duration: 500,
        setupDuration: 10,
        collectDuration: 150,
        prepareDuration: 20,
        environmentSetupDuration: 5,
      },
    },
  ];
}

/** Shard `i` runs files 2i-1 and 2i, taking i seconds and then 2.5i seconds. */
function shardRecords(shardIndex: number): CaptureRecord[] {
  const first = files[shardIndex * 2 - 2]!;
  const second = files[shardIndex * 2 - 1]!;
  const firstEnd = 100 + 1000 * shardIndex;
  const secondEnd = firstEnd + 2500 * shardIndex;
  const terminal: CaptureTerminal = {
    type: "terminal",
    reason: "passed",
    unhandledErrorCount: 0,
    completedFiles: [first, second],
    elapsedMs: secondEnd + 200,
  };
  return [
    header(shardIndex),
    ...events(first, 100, firstEnd),
    ...events(second, firstEnd + 50, secondEnd),
    terminal,
  ];
}

const filters = header(1).filters;

const text = (records: readonly unknown[]) =>
  records.map((record) => JSON.stringify(record)).join("\n") + "\n";

function captures(change?: (shardIndex: number, records: CaptureRecord[]) => unknown[]): string[] {
  return Array.from({ length: SHARDS }, (_, i) => {
    const records = shardRecords(i + 1);
    return text(change?.(i + 1, records) ?? records);
  });
}

function rejection(texts: string[], shardCount = SHARDS): TimingRejectionCode {
  try {
    aggregateServerShardCaptures(texts, { shardCount });
  } catch (error) {
    if (error instanceof TimingRejection) return error.code;
    throw error;
  }
  throw new Error("captures were accepted");
}

/** Replaces shard 2's record at `index` (0 is the header). */
const onShard2 =
  (index: number, change: (record: Record<string, unknown>) => unknown) =>
  (shardIndex: number, records: CaptureRecord[]) =>
    shardIndex === 2
      ? records.map((record, i) => (i === index ? change({ ...record }) : record))
      : records;

/** Shard 2 queues and collects its second file before the first one ends. */
function overlapOnShard2(shardIndex: number, records: CaptureRecord[]): unknown[] {
  if (shardIndex !== 2) return records;
  const [head, q1, c1, s1, e1, q2, c2, ...rest] = records;
  return [head, q1, c1, s1, { ...q2, elapsedMs: 302 }, { ...c2, elapsedMs: 303 }, e1, ...rest];
}

/** Shard 2 runs shard 1's first file in place of its own second file. */
function repeatOnShard2(shardIndex: number, records: CaptureRecord[]): unknown[] {
  if (shardIndex !== 2) return records;
  return records.map((record) => {
    if (record.type === "module" && record.path === files[3]) return { ...record, path: files[0] };
    if (record.type === "terminal") return { ...record, completedFiles: [files[2], files[0]] };
    return record;
  });
}

describe("aggregateServerShardCaptures", () => {
  it("turns six complete shards into exact gap-based wall-second weights", () => {
    const { envelope, overhead } = aggregateServerShardCaptures(captures().toReversed(), {
      shardCount: SHARDS,
    });

    expect(envelope.weights).toEqual(
      Object.fromEntries(
        Array.from({ length: SHARDS }, (_, i) => [
          [files[i * 2]!, i + 1],
          [files[i * 2 + 1]!, 2.5 * (i + 1)],
        ]).flat(),
      ),
    );
    expect(Object.keys(envelope.weights)).toEqual(files.toSorted());
    expect(envelope.source).toEqual({
      repository: "Jones-Systems/Jones-Code",
      workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/heads/main",
      runId: "4242",
      runAttempt: 1,
      event: "push",
      ref: "refs/heads/main",
      headSha: "a".repeat(40),
      shardCount: SHARDS,
      fileCount: files.length,
      manifestDigest: manifestDigest(files),
      weightsInput: { mode: "checked-in" },
    });
    expect(overhead).toEqual(
      Array.from({ length: SHARDS }, (_, i) => ({
        shardIndex: i + 1,
        files: 2,
        startupMs: 100,
        finalTailMs: 200,
        elapsedMs: 100 + 3500 * (i + 1) + 200,
      })),
    );
    expect(parseWeightsEnvelope(serializeWeightsEnvelope(envelope))).toEqual(envelope);
  });

  it("rounds weights to milliseconds", () => {
    const texts = captures(
      onShard2(4, (record) => ({ ...record, elapsedMs: (record.elapsedMs as number) + 0.4567 })),
    );
    const { envelope } = aggregateServerShardCaptures(texts, { shardCount: SHARDS });

    expect(envelope.weights[files[2]!]).toBe(2);
    expect(envelope.weights[files[3]!]).toBe(5);
  });

  it("accepts runtime weights when every shard used the same ones", () => {
    const weightsInput = { mode: "runtime", sourceRunId: "4100", digest: "c".repeat(64) } as const;
    const texts = captures((shardIndex, records) => [
      { ...header(shardIndex), weightsInput },
      ...records.slice(1),
    ]);

    const { envelope } = aggregateServerShardCaptures(texts, { shardCount: SHARDS });
    expect(envelope.source.weightsInput).toEqual(weightsInput);
  });

  it.each<[string, () => string[], TimingRejectionCode, number?]>([
    ["no captures", () => [], "no-captures"],
    ["a missing shard", () => captures().slice(1), "missing-shard"],
    [
      "a duplicate shard",
      () => {
        const texts = captures();
        return [...texts.slice(1), texts[1]!];
      },
      "duplicate-shard",
    ],
    ["a different shard count", () => captures(), "shard-count-mismatch", 8],
    ["a missing final newline", () => captures().map((t) => t.slice(0, -1)), "truncated"],
    ["a missing terminal record", () => captures((_, r) => r.slice(0, -1)), "truncated"],
    ["an empty capture", () => ["", ...captures().slice(1)], "truncated"],
    [
      "a line that is not JSON",
      () => captures().map((t, i) => (i === 1 ? t.replace("}\n", "\n") : t)),
      "malformed",
    ],
    [
      "an unknown capture version",
      () => captures(onShard2(0, (r) => ({ ...r, schema: "jones.server-shard-timings/v2" }))),
      "unsupported-version",
    ],
    [
      "an unknown header field",
      () => captures(onShard2(0, (r) => ({ ...r, extra: true }))),
      "malformed",
    ],
    [
      "a missing header field",
      () => captures(onShard2(0, ({ tools: _tools, ...r }) => r)),
      "malformed",
    ],
    [
      "a checkout other than the triggering commit",
      () => captures(onShard2(0, (r) => ({ ...r, checkoutSha: "d".repeat(40) }))),
      "source-mismatch",
    ],
    [
      "parallel execution",
      () => captures(onShard2(0, (r) => ({ ...r, fileParallelism: true }))),
      "parallel-execution",
    ],
    [
      "a test name filter",
      () => captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, testNamePattern: "x" } }))),
      "filtered-invocation",
    ],
    [
      "a related-files filter",
      () => captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, related: ["a"] } }))),
      "filtered-invocation",
    ],
    [
      "a project filter",
      () => captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, projects: ["a"] } }))),
      "filtered-invocation",
    ],
    [
      "a positional filename filter",
      () =>
        captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, filenamePatterns: ["a"] } }))),
      "filtered-invocation",
    ],
    [
      "a file-level test filter",
      () => captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, targetedFiles: 1 } }))),
      "filtered-invocation",
    ],
    [
      "a changed-files run",
      () => captures(onShard2(0, (r) => ({ ...r, filters: { ...filters, changed: "true" } }))),
      "filtered-invocation",
    ],
    [
      "a different run",
      () => captures(onShard2(0, (r) => ({ ...r, runId: "4243" }))),
      "mixed-identity",
    ],
    [
      "a different attempt",
      () => captures(onShard2(0, (r) => ({ ...r, runAttempt: 2 }))),
      "mixed-identity",
    ],
    [
      "different tools",
      () => captures(onShard2(0, (r) => ({ ...r, tools: { ...header(2).tools, node: "v99" } }))),
      "mixed-identity",
    ],
    [
      "a different identity hash",
      () =>
        captures(
          onShard2(0, (r) => ({
            ...r,
            identities: { ...header(2).identities, "pnpm-lock.yaml": "e".repeat(64) },
          })),
        ),
      "mixed-identity",
    ],
    [
      "different weights",
      () =>
        captures(
          onShard2(0, (r) => ({
            ...r,
            weightsInput: { mode: "runtime", sourceRunId: "1", digest: "c".repeat(64) },
          })),
        ),
      "mixed-identity",
    ],
    [
      "a different file list",
      () => captures(onShard2(0, (r) => ({ ...r, manifest: [...files, "src/extra.test.ts"] }))),
      "mixed-identity",
    ],
    [
      "a failed shard",
      () => captures(onShard2(9, (r) => ({ ...r, reason: "failed" }))),
      "shard-not-passed",
    ],
    [
      "an interrupted shard",
      () => captures(onShard2(9, (r) => ({ ...r, reason: "interrupted" }))),
      "shard-not-passed",
    ],
    [
      "unhandled errors",
      () => captures(onShard2(9, (r) => ({ ...r, unhandledErrorCount: 1 }))),
      "shard-not-passed",
    ],
    [
      "module errors",
      () => captures(onShard2(4, (r) => ({ ...r, errorCount: 1, errors: ["boom"] }))),
      "module-error",
    ],
    [
      "a failed module",
      () => captures(onShard2(4, (r) => ({ ...r, state: "failed" }))),
      "module-error",
    ],
    [
      "events out of time order",
      () => captures(onShard2(3, (r) => ({ ...r, elapsedMs: 1 }))),
      "event-order",
    ],
    [
      "a missing module event",
      () => captures((i, r) => (i === 2 ? r.filter((_, index) => index !== 2) : r)),
      "event-order",
    ],
    ["overlapping module execution", () => captures(overlapOnShard2), "overlapping-modules"],
    [
      "an unsafe path",
      () => captures(onShard2(1, (r) => ({ ...r, path: "../escape.test.ts" }))),
      "unsafe-path",
    ],
    [
      "a file outside the file list",
      () => captures(onShard2(1, (r) => ({ ...r, path: "src/other.test.ts" }))),
      "unlisted-file",
    ],
    [
      "a duplicate path in the file list",
      () => captures((i, r) => [{ ...header(i), manifest: [...files, files[0]!] }, ...r.slice(1)]),
      "duplicate-file",
    ],
    ["a file that ran in two shards", () => captures(repeatOnShard2), "duplicate-file"],
    [
      "a discovered file that never ran",
      () =>
        captures((i, r) => [
          { ...header(i), manifest: [...files, "src/skipped.test.ts"] },
          ...r.slice(1),
        ]),
      "incomplete-coverage",
    ],
    [
      "a terminal record that disagrees with the file events",
      () => captures(onShard2(9, (r) => ({ ...r, completedFiles: [files[2]] }))),
      "malformed",
    ],
    [
      "a weight over the limit",
      () => captures(onShard2(8, (r) => ({ ...r, elapsedMs: 700_000 }))),
      "out-of-range",
    ],
  ])("rejects %s", (_name, build, code, shardCount) => {
    expect(rejection(build(), shardCount)).toBe(code);
  });
});

describe("parseCapture", () => {
  it("reads every record of a capture in order", () => {
    const records = parseCapture(captures()[0]!);

    expect(records.map((record) => record.type)).toEqual([
      "header",
      ...Array.from({ length: 8 }, () => "module"),
      "terminal",
    ]);
  });

  it("requires the header to come first", () => {
    const [head, ...rest] = shardRecords(1);
    expect(() => parseCapture(text([...rest.slice(0, -1), head, rest.at(-1)]))).toThrow(
      TimingRejection,
    );
  });
});
