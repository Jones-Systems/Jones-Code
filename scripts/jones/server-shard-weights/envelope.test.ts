import { describe, expect, it } from "vite-plus/test";

import {
  MAX_WEIGHTS_BYTES,
  manifestDigest,
  parseWeightsEnvelope,
  serializeWeightsEnvelope,
  weightsDigest,
  type WeightsEnvelope,
} from "./envelope.ts";

function envelope(weights: Record<string, number> = { "src/b.test.ts": 2, "src/a.test.ts": 1.5 }) {
  const keys = Object.keys(weights);
  return {
    schema: "jones.server-shard-weights/v1",
    semantics: "wall-seconds",
    source: {
      repository: "Jones-Systems/Jones-Code",
      workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/heads/main",
      runId: "123456789012",
      runAttempt: 2,
      event: "push",
      ref: "refs/heads/main",
      headSha: "f".repeat(40),
      shardCount: 6,
      fileCount: keys.length,
      manifestDigest: manifestDigest(keys),
      weightsInput: { mode: "runtime", sourceRunId: "123456789000", digest: "0".repeat(64) },
    },
    weights,
  } satisfies WeightsEnvelope;
}

const json = (value: unknown) => `${JSON.stringify(value)}\n`;

describe("weights envelope", () => {
  it("round-trips through serialize and parse", () => {
    const value = envelope();
    expect(parseWeightsEnvelope(serializeWeightsEnvelope(value))).toEqual(value);
  });

  it("serializes the same envelope to the same bytes, sorted, with a trailing newline", () => {
    const forward = serializeWeightsEnvelope(envelope({ "src/a.test.ts": 1, "src/b.test.ts": 2 }));
    const backward = serializeWeightsEnvelope(envelope({ "src/b.test.ts": 2, "src/a.test.ts": 1 }));

    expect(backward).toBe(forward);
    expect(forward.endsWith("}\n")).toBe(true);
    expect(forward.indexOf("src/a.test.ts")).toBeLessThan(forward.indexOf("src/b.test.ts"));
    expect(weightsDigest(forward)).toBe(weightsDigest(backward));
    expect(weightsDigest(forward)).toMatch(/^[0-9a-f]{64}$/);
  });

  it("accepts checked-in weights input without a source run", () => {
    const source = { ...envelope().source, weightsInput: { mode: "checked-in" } };
    const parsed = parseWeightsEnvelope(json({ ...envelope(), source }));
    expect(parsed.source.weightsInput).toEqual({ mode: "checked-in" });
  });

  it("orders the file list digest independently of discovery order", () => {
    expect(manifestDigest(["b", "a"])).toBe(manifestDigest(["a", "b"]));
    expect(manifestDigest(["a"])).not.toBe(manifestDigest(["a", "b"]));
  });

  const withSource = (change: Record<string, unknown>) =>
    json({ ...envelope(), source: { ...envelope().source, ...change } });
  const withWeights = (weights: Record<string, unknown>) =>
    json({ ...envelope(weights as Record<string, number>), weights });
  const runtimeWithoutDigest = { mode: "runtime", sourceRunId: "1" };

  it.each<[string, () => string]>([
    ["text that is not JSON", () => "{"],
    ["an unknown schema", () => json({ ...envelope(), schema: "jones.server-shard-weights/v2" })],
    ["test-duration semantics", () => json({ ...envelope(), semantics: "test-seconds" })],
    ["an unknown top-level field", () => json({ ...envelope(), extra: 1 })],
    ["NaN", () => withWeights({ "src/a.test.ts": Number.NaN })],
    ["a string weight", () => withWeights({ "src/a.test.ts": "1" })],
    ["a negative weight", () => withWeights({ "src/a.test.ts": -0.001 })],
    ["a weight over 600 seconds", () => withWeights({ "src/a.test.ts": 600.001 })],
    ["no weights", () => withWeights({})],
    ["a parent-directory path", () => withWeights({ "../a.test.ts": 1 })],
    ["an absolute path", () => withWeights({ "/src/a.test.ts": 1 })],
    ["a dot segment", () => withWeights({ "src/./a.test.ts": 1 })],
    ["a backslash path", () => withWeights({ "src\\a.test.ts": 1 })],
    ["a drive path", () => withWeights({ "C:/a.test.ts": 1 })],
    ["a missing source field", () => withSource({ headSha: undefined })],
    ["an extra source field", () => withSource({ branch: "main" })],
    ["a short head SHA", () => withSource({ headSha: "f".repeat(39) })],
    ["a numeric run ID", () => withSource({ runId: 1 })],
    ["a zero attempt", () => withSource({ runAttempt: 0 })],
    ["a file count that disagrees", () => withSource({ fileCount: 3 })],
    ["weights that miss the file list", () => withSource({ manifestDigest: "0".repeat(64) })],
    ["runtime input without a digest", () => withSource({ weightsInput: runtimeWithoutDigest })],
    ["an unknown input mode", () => withSource({ weightsInput: { mode: "manual" } })],
    [
      "a file over the size limit",
      () =>
        json({
          ...envelope(),
          source: { ...envelope().source, workflowRef: "x".repeat(MAX_WEIGHTS_BYTES) },
        }),
    ],
  ])("rejects %s", (_name, build) => {
    expect(() => parseWeightsEnvelope(build())).toThrow();
  });

  it("refuses to serialize an envelope it could not parse", () => {
    const invalid = envelope({ "src/a.test.ts": 601 });
    expect(() => serializeWeightsEnvelope(invalid)).toThrow(/out of range/);
  });
});
