// @effect-diagnostics nodeBuiltinImport:off - Fixtures write runtime weight files into a per-test temporary directory.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { TestSpecification, Vitest } from "vite-plus/test/node";

import {
  WEIGHTS_DIGEST_ENV,
  WEIGHTS_PATH_ENV,
  manifestDigest,
  resolveSequencerWeight,
  serializeWeightsEnvelope,
  weightsDigest,
} from "../../../../../scripts/jones/server-shard-weights/envelope.ts";
import shardWeights from "../../testUtils/shardWeights.json" with { type: "json" };
import { WeightedShardSequencer } from "../../testUtils/weightedShardSequencer.ts";

const root = "/repo/apps/server";
const recordedSeconds: Readonly<Record<string, number>> = shardWeights;
const files = [
  ...Object.keys(recordedSeconds),
  ...Array.from({ length: 120 }, (_, i) => `src/fast${i}.test.ts`),
];
const specsFor = (paths: readonly string[]) =>
  paths.map((file) => ({ moduleId: `${root}/${file}` }) as TestSpecification);

async function partition(paths: readonly string[], count: number): Promise<string[][]> {
  return Promise.all(
    Array.from({ length: count }, async (_, i) => {
      const ctx = { config: { root, shard: { index: i + 1, count } } } as unknown as Vitest;
      const shard = await new WeightedShardSequencer(ctx).shard(specsFor(paths));
      return shard.map((spec) => spec.moduleId.slice(root.length + 1));
    }),
  );
}

/** Upstream defaults use recorded wall seconds and 0.6 seconds for unrecorded files. */
function upstreamPartition(paths: readonly string[], count: number): string[][] {
  const weighted = paths
    .map((key) => ({ key, seconds: recordedSeconds[key] ?? 0.6 }))
    .toSorted((a, b) => b.seconds - a.seconds || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const loads = Array.from({ length: count }, () => 0);
  const shards: string[][] = Array.from({ length: count }, () => []);
  for (const file of weighted) {
    const least = Math.min(...loads);
    const lightest = loads.indexOf(least);
    loads[lightest] = least + file.seconds;
    shards[lightest]?.push(file.key);
  }
  return shards;
}

let scratch: string | undefined;

function writeRuntimeWeights(weights: Record<string, number>): { path: string; digest: string } {
  scratch ??= NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-shard-weights-"));
  const keys = Object.keys(weights);
  const text = serializeWeightsEnvelope({
    schema: "jones.server-shard-weights/v1",
    semantics: "wall-seconds",
    source: {
      repository: "Jones-Systems/Jones-Code",
      workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/heads/main",
      runId: "1001",
      runAttempt: 1,
      event: "push",
      ref: "refs/heads/main",
      headSha: "a".repeat(40),
      shardCount: 6,
      fileCount: keys.length,
      manifestDigest: manifestDigest(keys),
      weightsInput: { mode: "checked-in" },
    },
    weights,
  });
  const path = NodePath.join(scratch, `weights-${keys.length}.json`);
  NodeFS.writeFileSync(path, text);
  return { path, digest: weightsDigest(text) };
}

function useRuntimeWeights(weights: Record<string, number>): void {
  const { path, digest } = writeRuntimeWeights(weights);
  vi.stubEnv(WEIGHTS_PATH_ENV, path);
  vi.stubEnv(WEIGHTS_DIGEST_ENV, digest);
}

// A complete wall-time map unrelated to the checked-in times.
const runtimeWeights = Object.fromEntries(
  files.map((file, i) => [file, Math.round((1 + (i % 17) * 0.37) * 1000) / 1000]),
);

beforeEach(() => {
  vi.stubEnv(WEIGHTS_PATH_ENV, undefined);
  vi.stubEnv(WEIGHTS_DIGEST_ENV, undefined);
});

afterEach(() => {
  vi.unstubAllEnvs();
  if (scratch !== undefined) NodeFS.rmSync(scratch, { recursive: true, force: true });
  scratch = undefined;
});

describe("WeightedShardSequencer weight sources", () => {
  it.each([1, 6, 8])("keeps the checked-in partition over %i shards", async (count) => {
    expect(await partition(files, count)).toEqual(upstreamPartition(files, count));
  });

  it("treats an empty weights path as unset", async () => {
    vi.stubEnv(WEIGHTS_PATH_ENV, "");
    expect(await partition(files, 6)).toEqual(upstreamPartition(files, 6));
  });

  it("reads runtime weights as wall seconds and charges unknown files 0.6s", () => {
    useRuntimeWeights({ "src/a.test.ts": 4.5, "src/b.test.ts": 0 });
    const secondsOf = resolveSequencerWeight(process.env, recordedSeconds);

    expect(secondsOf("src/a.test.ts")).toBe(4.5);
    expect(secondsOf("src/b.test.ts")).toBe(0);
    expect(secondsOf("src/unknown.test.ts")).toBe(0.6);
    expect(secondsOf("constructor")).toBe(0.6);
  });

  it("reads the checked-in map as recorded test time plus overhead", () => {
    const [recorded] = Object.keys(recordedSeconds);
    const secondsOf = resolveSequencerWeight({}, recordedSeconds);

    expect(secondsOf(recorded!)).toBe((recordedSeconds[recorded!] ?? 0) + 0.25);
    expect(secondsOf("src/unknown.test.ts")).toBe(0.25);
    expect(secondsOf("constructor")).toBe(0.25);
  });

  it("places files by their runtime weights", async () => {
    // Checked-in weights would charge these two the fast-file minimum.
    useRuntimeWeights({ ...runtimeWeights, "src/fast0.test.ts": 90, "src/fast1.test.ts": 80 });
    const shards = await partition(files, 6);

    // Longest first, each onto the lightest (then lowest-numbered) shard.
    expect(shards[0]?.[0]).toBe("src/fast0.test.ts");
    expect(shards[1]?.[0]).toBe("src/fast1.test.ts");
  });

  it.each([
    ["checked-in", 1],
    ["checked-in", 6],
    ["checked-in", 8],
    ["runtime", 1],
    ["runtime", 6],
    ["runtime", 8],
  ] as const)("runs each file once: %s weights, %i shards", async (mode, count) => {
    if (mode === "runtime") useRuntimeWeights(runtimeWeights);
    const shards = await partition([...files, "src/unweighted.test.ts"], count);

    expect(shards.flat().toSorted()).toEqual([...files, "src/unweighted.test.ts"].toSorted());
  });

  it.each(["checked-in", "runtime"] as const)(
    "splits the same way whatever the input order, with %s weights",
    async (mode) => {
      // Every file weighs the same, so only the tie-break orders them.
      if (mode === "runtime") {
        useRuntimeWeights(Object.fromEntries(files.map((file) => [file, 1])));
      }
      const reversed = files.toReversed();
      const rotated = [...files.slice(37), ...files.slice(0, 37)];

      const expected = await partition(files, 6);
      expect(await partition(reversed, 6)).toEqual(expected);
      expect(await partition(rotated, 6)).toEqual(expected);
    },
  );

  it("throws when the named weights file is missing", async () => {
    vi.stubEnv(WEIGHTS_PATH_ENV, NodePath.join(NodeOS.tmpdir(), "jones-missing", "weights.json"));
    vi.stubEnv(WEIGHTS_DIGEST_ENV, "0".repeat(64));
    await expect(partition(files, 6)).rejects.toThrow();
  });

  it("throws when the weights file is invalid", async () => {
    const { path } = writeRuntimeWeights({ "src/a.test.ts": 1 });
    NodeFS.writeFileSync(path, '{"schema":"jones.server-shard-weights/v1"}\n');
    vi.stubEnv(WEIGHTS_PATH_ENV, path);
    vi.stubEnv(WEIGHTS_DIGEST_ENV, weightsDigest(NodeFS.readFileSync(path, "utf8")));
    await expect(partition(files, 6)).rejects.toThrow(/Invalid server shard weights/);
  });

  it("throws when the weights file does not match its digest", async () => {
    const { path } = writeRuntimeWeights({ "src/a.test.ts": 1 });
    vi.stubEnv(WEIGHTS_PATH_ENV, path);
    vi.stubEnv(WEIGHTS_DIGEST_ENV, "0".repeat(64));
    await expect(partition(files, 6)).rejects.toThrow(/does not match/);
  });

  it("throws when the weights file has no digest", async () => {
    const { path } = writeRuntimeWeights({ "src/a.test.ts": 1 });
    vi.stubEnv(WEIGHTS_PATH_ENV, path);
    await expect(partition(files, 6)).rejects.toThrow(/must accompany/);
  });
});
