// @effect-diagnostics nodeBuiltinImport:off - Fixtures lay out downloaded artifacts in a per-test temporary directory.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterEach, describe, expect, it } from "vite-plus/test";

import {
  CAPTURE_FILE_NAME,
  captureArtifactName,
  decideAggregate,
  formatSummary,
  readCaptures,
  type AggregateInputs,
} from "./aggregate-cli.ts";
import { CAPTURE_IDENTITY_FILES, CAPTURE_SCHEMA, type CaptureHeader } from "./capture.ts";
import { parseWeightsEnvelope, weightsDigest, type WeightsInput } from "./envelope.ts";

const SHA = "c".repeat(40);
const RUNTIME = { mode: "runtime", sourceRunId: "500", digest: "d".repeat(64) } as const;

interface CaptureOptions {
  shardCount?: number;
  attempt?: number;
  weightsInput?: WeightsInput;
}

/** Shard `i` of `shardCount` runs one file, `src/s<i>.test.ts`, in i seconds after 100 ms. */
function capture(shardIndex: number, options: CaptureOptions = {}): string {
  const { shardCount = 6, attempt = 1, weightsInput = { mode: "checked-in" } } = options;
  const path = `src/s${shardIndex}.test.ts`;
  const header: CaptureHeader = {
    type: "header",
    schema: CAPTURE_SCHEMA,
    checkoutSha: SHA,
    githubSha: SHA,
    repository: "Jones-Systems/Jones-Code",
    workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/heads/main",
    event: "push",
    ref: "refs/heads/main",
    runId: "600",
    runAttempt: attempt,
    shardIndex,
    shardCount,
    runner: { image: "ubuntu24", imageVersion: "20261012.1", release: "6.11" },
    tools: { node: "v24.13.1", vitest: "5.0.1", vitePlus: "1.0.0" },
    identities: Object.fromEntries(CAPTURE_IDENTITY_FILES.map((file) => [file, "e".repeat(64)])),
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
    weightsInput,
    manifest: Array.from({ length: shardCount }, (_, i) => `src/s${i + 1}.test.ts`),
  };
  const base = { type: "module", path, state: "passed", errorCount: 0, errors: [] };
  const end = 100 + 1000 * shardIndex;
  const diagnostic = {
    duration: 1,
    setupDuration: 1,
    collectDuration: 1,
    prepareDuration: 1,
    environmentSetupDuration: 1,
  };
  return [
    header,
    { ...base, event: "queued", elapsedMs: 100, state: "queued" },
    { ...base, event: "collected", elapsedMs: 110 },
    { ...base, event: "start", elapsedMs: 111 },
    { ...base, event: "end", elapsedMs: end, diagnostic },
    {
      type: "terminal",
      reason: "passed",
      unhandledErrorCount: 0,
      completedFiles: [path],
      elapsedMs: end + 30,
    },
  ]
    .map((record) => JSON.stringify(record))
    .join("\n")
    .concat("\n");
}

function shards(indexes: number[], options: CaptureOptions = {}): Map<number, string> {
  return new Map(indexes.map((index) => [index, capture(index, options)]));
}

const ALL = [1, 2, 3, 4, 5, 6];

function inputs(overrides: Partial<AggregateInputs> = {}): AggregateInputs {
  return {
    testResult: "success",
    shardCount: 6,
    runAttempt: 1,
    runId: "600",
    repository: "Jones-Systems/Jones-Code",
    sha: SHA,
    selection: { mode: "checked-in", digest: "", sourceRunId: "" },
    captures: shards(ALL),
    ...overrides,
  };
}

describe("decideAggregate", () => {
  it.each(["failure", "cancelled", "skipped", ""])(
    "treats server tests that ended %j as no measurement",
    (testResult) => {
      expect(decideAggregate(inputs({ testResult, captures: new Map() }))).toMatchObject({
        status: "ineligible",
        exitCode: 0,
      });
    },
  );

  it("treats a rerun of only some shards as no measurement", () => {
    const outcome = decideAggregate(
      inputs({ runAttempt: 2, captures: shards([2, 5], { attempt: 2 }) }),
    );
    expect(outcome).toMatchObject({ status: "ineligible", exitCode: 0 });
  });

  it("publishes a full rerun as that attempt's measurement", () => {
    const rerun = shards(ALL, { attempt: 2 });
    const outcome = decideAggregate(inputs({ runAttempt: 2, captures: rerun }));

    expect(outcome.status).toBe("published");
    if (outcome.status !== "published") return;
    expect(parseWeightsEnvelope(outcome.text).source.runAttempt).toBe(2);
  });

  it("publishes complete passing captures as a combined weights file", () => {
    const outcome = decideAggregate(inputs());

    expect(outcome).toMatchObject({ status: "published", exitCode: 0 });
    if (outcome.status !== "published") return;
    const envelope = parseWeightsEnvelope(outcome.text);
    expect(envelope.weights).toEqual(
      Object.fromEntries(ALL.map((index) => [`src/s${index}.test.ts`, index])),
    );
    expect(outcome.digest).toBe(weightsDigest(outcome.text));
    expect(outcome.overhead.map((shard) => shard.shardIndex)).toEqual(ALL);
  });

  it("publishes captures that used the selected runtime weights", () => {
    const outcome = decideAggregate(
      inputs({
        selection: { mode: "runtime", digest: RUNTIME.digest, sourceRunId: "500" },
        captures: shards(ALL, { weightsInput: RUNTIME }),
      }),
    );
    expect(outcome.status).toBe("published");
  });

  it.each<[string, Partial<AggregateInputs>]>([
    ["a missing first-attempt shard", { captures: shards([1, 2, 3, 4, 5]) }],
    ["a corrupt capture", { captures: new Map([...shards(ALL), [3, "{}"]]) }],
    ["captures from another run", { runId: "601" }],
    ["captures of another commit", { sha: "f".repeat(40) }],
    ["captures from another attempt", { runAttempt: 2, captures: shards(ALL) }],
    [
      "captures that ignored the selected weights",
      { selection: { mode: "runtime", digest: "d".repeat(64), sourceRunId: "500" } },
    ],
    ["captures that used unselected weights", { captures: shards(ALL, { weightsInput: RUNTIME }) }],
  ])("fails the job for %s after passing tests", (_name, overrides) => {
    expect(decideAggregate(inputs(overrides))).toMatchObject({ status: "rejected", exitCode: 1 });
  });
});

describe("readCaptures", () => {
  const roots: string[] = [];
  afterEach(() => {
    for (const root of roots.splice(0)) NodeFS.rmSync(root, { recursive: true, force: true });
  });

  it("reads this attempt's shard artifacts and ignores the rest", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-captures-"));
    roots.push(root);
    const place = (name: string, text: string) => {
      NodeFS.mkdirSync(NodePath.join(root, name));
      NodeFS.writeFileSync(NodePath.join(root, name, CAPTURE_FILE_NAME), text);
    };
    for (const index of [1, 2, 4]) {
      place(captureArtifactName(2, index, 6), `attempt 2 shard ${index}`);
    }
    place(captureArtifactName(1, 3, 6), "attempt 1 shard 3");
    place(captureArtifactName(2, 3, 8), "attempt 2 shard 3 of 8");

    expect(readCaptures(root, { attempt: 2, shardCount: 6 })).toEqual(
      new Map([
        [1, "attempt 2 shard 1"],
        [2, "attempt 2 shard 2"],
        [4, "attempt 2 shard 4"],
      ]),
    );
  });
});

describe("formatSummary", () => {
  it("reports startup and final tail per shard for a published measurement", () => {
    const summary = formatSummary(decideAggregate(inputs()));

    expect(summary).toContain("`published`");
    expect(summary).toContain("| 3 | 1 | 0.1 | 0.0 | 3.1 |");
  });

  it("states why nothing was published", () => {
    expect(formatSummary(decideAggregate(inputs({ testResult: "failure" })))).toContain(
      "server tests ended failure",
    );
  });
});
