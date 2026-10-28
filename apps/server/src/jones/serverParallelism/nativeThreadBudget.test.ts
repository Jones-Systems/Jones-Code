import { expect, it } from "vite-plus/test";
import { evaluateNativeThreadBudget, type ThreadBudgetGeneration } from "./nativeThreadBudget.ts";
import { processResourceSample, type NativePoolObservation } from "./resourceSamples.ts";

type Input = Parameters<typeof evaluateNativeThreadBudget>[0];
function pools(pid: number, B: number, S: number, mac = false): NativePoolObservation {
  return { pid, method: mac ? "macos-sample" : "linux-proc-task-comm", status: "complete", raw: "fixture",
    threads: [
      ...Array.from({ length: B }, (_, index) => ({ tid: `b${index}`, name: `fff-bg-${index}` })),
      ...Array.from({ length: S }, (_, index) => ({ tid: `s${index}`, name: `fff-search-${index}` })),
    ] };
}
function fixture(values: readonly number[] = [4, 4], P?: number): Input {
  const configuredN = values.length as 2 | 3;
  const generations: ThreadBudgetGeneration[] = values.map((A, slot) => ({
    pid: 100 + slot, bootstrap: { version: 1, build: "a".repeat(64), authorityId: "owner",
      slotId: slot, slotGeneration: 1, nonce: `nonce-${slot}` },
    startedAt: 0, exitCapturedAt: 90, journalRetiredAt: 100,
    runtimeMetadata: Object.freeze({ availableParallelism: A, heapLimitBytes: 300 * 1024 * 1024,
      parallelismSource: "node:os.availableParallelism" }),
    nativePools: [pools(100 + slot, Math.max(Math.floor(A / 2), 2), P ?? A, P !== undefined)],
  }));
  return { configuredN, platform: P === undefined ? "linux" : "darwin", parentPid: 1,
    macPerformanceCores: { source: "sysctl:hw.perflevel0.physicalcpu", status: P === undefined ? "unavailable" : "observed",
      value: P ?? null, raw: P === undefined ? "not-macos" : `${P}\n` },
    generations, lifecycleBounds: true, observationComplete: true,
    samples: ["warm-idle", "steady-0", "churn-9", "churn-settling"].map((phase, index) => ({
      at: 10 + index * 20, finishedAt: 11 + index * 20, phase,
      generations: generations.map((generation) => ({ pid: generation.pid!, bootstrap: generation.bootstrap })),
      sample: processResourceSample([1, ...generations.map((generation) => generation.pid!)],
        [{ pid: 1, rssBytes: 1, cpuMs: 0, threads: 100_000 },
          ...generations.map((generation) => ({ pid: generation.pid!, rssBytes: 1, cpuMs: 0, threads: 10 }))]),
    })),
  };
}

it.each([
  { A: [1, 5], expected: 138 },
  { A: [3, 5, 9], expected: 217 },
  { A: [4, 4], expected: 140 },
  { A: [4, 4, 4], expected: 210 },
])("uses each child's odd/minimum B and exact N for $A", ({ A, expected }) => {
  const result = evaluateNativeThreadBudget(fixture(A));
  expect(result.ceiling).toBe(expected);
  expect(result.candidateCeiling).toBe(expected);
  expect(result.R).toBe(4 * A.length);
  expect(result.predicate).toBe(true);
  expect(result.maximumObserved).toBe(10 * A.length); // Parent's 100,000 threads are separate.
});

it("uses observed macOS P with provenance and native names", () => {
  const result = evaluateNativeThreadBudget(fixture([8, 8], 4));
  expect(result.ceiling).toBe(144);
  expect(result.termsByGeneration[0]?.candidateS).toBe(4);
  expect(result.termsByGeneration[0]?.nativeFallback).toBe("unknown");
  expect(result.predicate).toBe(true);
});

it("keeps failed macOS P collection and unproved fallback unknown", () => {
  const input = fixture([8, 8], 4);
  const result = evaluateNativeThreadBudget({ ...input,
    macPerformanceCores: { ...input.macPerformanceCores, status: "unavailable", value: null, raw: "sysctl-failed" } });
  expect(result.predicate).toBe("unknown");
  expect(result.ceiling).toBeNull();
  expect(result.candidateCeiling).toBeNull();
  expect(result.termsByGeneration[0]?.candidateB).toBe(4);
});

it("does not qualify Node A alone or a native pool count differing from it", () => {
  const input = fixture();
  for (const nativePools of [[], [pools(100, 2, 5)]]) {
    const result = evaluateNativeThreadBudget({ ...input,
      generations: input.generations.map((generation, index) => index === 0 ? { ...generation, nativePools } : generation) });
    expect(result.candidateCeiling).toBe(140);
    expect(result.ceiling).toBeNull();
    expect(result.predicate).toBe("unknown");
  }
});

it("keeps missing runtime metadata unknown instead of substituting parent A", () => {
  const input = fixture();
  expect(evaluateNativeThreadBudget({ ...input, generations: input.generations.map((generation) =>
    ({ ...generation, runtimeMetadata: undefined })) }).predicate).toBe("unknown");
});

it.each([0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])(
  "rejects invalid thread count %j", (threads) => {
    const input = fixture();
    expect(evaluateNativeThreadBudget({ ...input, samples: input.samples.map((sample, index) => index ? sample :
      { ...sample, sample: { ...sample.sample, processes: sample.sample.processes.map((row) =>
        row.pid === 100 ? { ...row, threads } : row) } }) }).predicate).toBe("unknown");
  },
);

it("rejects missing/duplicate rows, malformed pool indices, and wrong-PID pool evidence", () => {
  const input = fixture();
  for (const change of ["missing", "duplicate", "thread-missing"] as const) {
    const samples = input.samples.map((sample, index) => index ? sample : { ...sample,
      sample: { ...sample.sample, processes: change === "missing" ? sample.sample.processes.filter((row) => row.pid !== 100) :
        change === "duplicate" ? [...sample.sample.processes, sample.sample.processes[1]!] :
          sample.sample.processes.map((row) => { const { threads: _, ...rest } = row; return row.pid === 100 ? rest : row; }) } });
    expect(evaluateNativeThreadBudget({ ...input, samples }).predicate).toBe("unknown");
  }
  const valid = pools(100, 2, 4);
  for (const bad of [
    { ...valid, pid: 999 },
    { ...valid, threads: [...valid.threads, valid.threads[0]!] },
    { ...valid, threads: valid.threads.map((row, index) => index ? row : { ...row, name: "fff-bg-9" }) },
  ]) {
    expect(evaluateNativeThreadBudget({ ...input, generations: input.generations.map((generation, index) =>
      index ? generation : { ...generation, nativePools: [bad] }) }).predicate).toBe("unknown");
  }
});

it.each([0, 1])("accepts equality and rejects ceiling +%i without counting parent threads", (extra) => {
  const input = fixture();
  const samples = input.samples.map((sample) => ({ ...sample, sample: { ...sample.sample,
    processes: sample.sample.processes.map((row) => row.pid === 1 ? row : { ...row, threads: 70 + (row.pid === 100 ? extra : 0) }) } }));
  expect(evaluateNativeThreadBudget({ ...input, samples }).predicate).toBe(extra ? false : true);
});

it("retains proved excess when another child row is missing", () => {
  const input = fixture();
  const samples = input.samples.map((sample) => ({ ...sample, sample: { ...sample.sample, status: "partial" as const,
    processes: sample.sample.processes.filter((row) => row.pid !== 101).map((row) =>
      row.pid === 100 ? { ...row, threads: 141 } : row), missingPids: [101] } }));
  expect(evaluateNativeThreadBudget({ ...input, samples }).predicate).toBe(false);
});

it.each(["warm-idle", "steady-0", "churn-9", "churn-settling"])("requires phase coverage for %s", (phase) => {
  const input = fixture();
  expect(evaluateNativeThreadBudget({ ...input, samples: input.samples.filter((sample) => sample.phase !== phase) }).predicate)
    .toBe("unknown");
});

it("does not transfer evidence across a reused PID/new generation, including a missing short generation", () => {
  const input = fixture();
  const old = input.generations[0]!;
  const replacement = { ...old, startedAt: 60, exitCapturedAt: 90, journalRetiredAt: 100,
    bootstrap: { ...old.bootstrap, slotGeneration: 2, nonce: "replacement" }, nativePools: [] };
  const generations = [{ ...old, exitCapturedAt: 54, journalRetiredAt: 55 }, input.generations[1]!, replacement];
  const result = evaluateNativeThreadBudget({ ...input, generations });
  expect(result.predicate).toBe("unknown");
  expect(result.coverage.generationCoverage.at(-1)?.complete).toBe(false);
  // A fresh binding AND fresh native evidence are both needed, even though the numeric PID is reused.
  const samples = input.samples.map((sample) => sample.at < 60 ? sample : { ...sample,
    generations: sample.generations.map((ref) => ref.pid === old.pid ? { ...ref, bootstrap: replacement.bootstrap } : ref) });
  expect(evaluateNativeThreadBudget({ ...input, generations: [...generations.slice(0, 2),
    { ...replacement, nativePools: [pools(100, 2, 4)] }], samples }).predicate).toBe(true);
  const short = { ...replacement, pid: 999, startedAt: 56, exitCapturedAt: 57, journalRetiredAt: 58,
    bootstrap: { ...replacement.bootstrap, slotGeneration: 3, nonce: "uncovered" }, nativePools: [pools(999, 2, 4)] };
  expect(evaluateNativeThreadBudget({ ...input, generations: [...generations, short], samples }).predicate).toBe("unknown");
});

it("requires captured exit plus journal retirement and rejects population changes inside a sample", () => {
  const input = fixture();
  for (const patch of [{ exitCapturedAt: undefined }, { journalRetiredAt: undefined }, { exitCapturedAt: 10.5 }]) {
    const generations = input.generations.map((generation, index) => index ? generation : { ...generation, ...patch });
    // Deliberately malformed receipts are outside the typed producer contract.
    expect(evaluateNativeThreadBudget({ ...input, generations: generations as readonly ThreadBudgetGeneration[] }).predicate).toBe("unknown");
  }
  const generations = input.generations.map((generation, index) => index ? generation :
    { ...generation, exitCapturedAt: 5, journalRetiredAt: 100 });
  const samples = input.samples.map((sample) => ({ ...sample,
    generations: sample.generations.filter((ref) => ref.pid !== 100),
    sample: processResourceSample([1, 101], sample.sample.processes.filter((row) => row.pid !== 100)) }));
  const result = evaluateNativeThreadBudget({ ...input, generations, samples });
  expect(result.sampleTotals[0]?.ceiling).toBe(140); // Exited generation remains charged until journal retirement.
  expect(result.sampleTotals[0]?.chargedGenerations).toHaveLength(2);
});

it("keeps lifecycle, observation, duplicate-generation and changed-native-basis failures visible", () => {
  const input = fixture();
  expect(evaluateNativeThreadBudget({ ...input, lifecycleBounds: false }).predicate).toBe(false);
  expect(evaluateNativeThreadBudget({ ...input, observationComplete: false }).predicate).toBe("unknown");
  expect(evaluateNativeThreadBudget({ ...input, generations: [...input.generations, input.generations[0]!] }).predicate).toBe("unknown");
  expect(evaluateNativeThreadBudget({ ...input, generations: input.generations.map((generation, index) => index ? generation :
    { ...generation, nativePools: [pools(100, 2, 4), pools(100, 2, 5)] }) }).predicate).toBe("unknown");
  expect(evaluateNativeThreadBudget({ ...input, generations: input.generations.map((generation, index) => index ? generation :
    { ...generation, nativePools: [pools(100, 2, 4), { ...pools(100, 2, 4), status: "unavailable" }] }) }).predicate).toBe("unknown");
});
