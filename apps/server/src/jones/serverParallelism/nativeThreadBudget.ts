import type { Bootstrap } from "../serverCompute/protocol.ts";
import type { ComputeRuntimeMetadata } from "../serverCompute/processSlot.ts";
import type {
  MacPerformanceCores,
  NativePoolObservation,
  ProcessResourceSample,
  QualificationPredicate,
} from "./resourceSamples.ts";

export interface ThreadBudgetGeneration {
  readonly bootstrap: Bootstrap;
  readonly pid?: number;
  readonly startedAt: number;
  readonly exitCapturedAt?: number;
  readonly journalRetiredAt?: number;
  readonly runtimeMetadata?: ComputeRuntimeMetadata | undefined;
  readonly nativePools: readonly NativePoolObservation[];
}
export interface ThreadBudgetSample {
  readonly at: number;
  readonly finishedAt: number;
  readonly phase: string;
  readonly sample: ProcessResourceSample;
  readonly generations: readonly { readonly pid: number; readonly bootstrap: Bootstrap }[];
}
const positive = (value: unknown): value is number =>
  Number.isSafeInteger(value) && (value as number) > 0;
const finite = (value: number | undefined): value is number =>
  value !== undefined && Number.isFinite(value) && value >= 0;
const safeSum = (values: readonly number[]) => {
  const sum = values.reduce((total, value) => total + value, 0);
  return Number.isSafeInteger(sum) && sum >= 0 ? sum : null;
};
const identity = (boot: Bootstrap) =>
  JSON.stringify([
    boot.version,
    boot.build,
    boot.authorityId,
    boot.slotId,
    boot.slotGeneration,
    boot.nonce,
  ]);
const phaseGroup = (phase: string) =>
  phase === "warm-idle"
    ? "warm"
    : phase.startsWith("steady-")
      ? "steady"
      : phase === "churn-settling"
        ? "settling"
        : phase.startsWith("churn-")
          ? "churn"
          : undefined;

function poolCounts(observation: NativePoolObservation) {
  if (observation.status !== "complete" || observation.threads.length === 0) return undefined;
  if (new Set(observation.threads.map((row) => row.tid)).size !== observation.threads.length)
    return undefined;
  if (observation.threads.some((row) => !row.tid || !row.name || /[\r\n\0]/.test(row.name)))
    return undefined;
  const count = (prefix: string) => {
    const names = observation.threads.filter((row) => row.name.startsWith(prefix));
    const indices = names.map((row) => Number(row.name.slice(prefix.length)));
    if (
      names.some((row) => !new RegExp(`^${prefix}(?:0|[1-9][0-9]*)$`).test(row.name)) ||
      new Set(indices).size !== indices.length ||
      indices.some((index) => !Number.isSafeInteger(index) || index < 0) ||
      indices.some((_, index) => !indices.includes(index))
    )
      return undefined;
    return names.length;
  };
  const B = count("fff-bg-"),
    S = count("fff-search-");
  return B !== undefined && B >= 2 && S !== undefined && S > 0 ? { B, S } : undefined;
}

/** Sampled compute-child budget, including guardians. It grants no CPU control or peak bound. */
export function evaluateNativeThreadBudget(input: {
  readonly configuredN: 2 | 3;
  readonly platform: string;
  readonly parentPid: number;
  readonly macPerformanceCores: MacPerformanceCores;
  readonly generations: readonly ThreadBudgetGeneration[];
  readonly samples: readonly ThreadBudgetSample[];
  readonly lifecycleBounds: QualificationPredicate;
  readonly observationComplete: boolean;
}) {
  const { configuredN: N, generations, samples } = input;
  const termsByGeneration = generations.map((generation) => {
    const metadata = generation.runtimeMetadata;
    const A =
      metadata?.parallelismSource === "node:os.availableParallelism" &&
      positive(metadata.availableParallelism)
        ? metadata.availableParallelism
        : null;
    const B = A === null ? null : Math.max(Math.floor(A / 2), 2);
    const P =
      input.macPerformanceCores.status === "observed" &&
      input.macPerformanceCores.source === "sysctl:hw.perflevel0.physicalcpu" &&
      positive(input.macPerformanceCores.value)
        ? input.macPerformanceCores.value
        : null;
    const S = input.platform === "linux" ? A : input.platform === "darwin" ? P : null;
    const counts = generation.nativePools.map((observation) =>
      observation.pid === generation.pid &&
      observation.method === (input.platform === "linux" ? "linux-proc-task-comm" : "macos-sample")
        ? poolCounts(observation)
        : undefined,
    );
    // Observations begin after actual grep. Missing/malformed or changed pools never raise the ceiling.
    const verified =
      B !== null &&
      S !== null &&
      counts.length > 0 &&
      counts.every((count) => count !== undefined && count.B === B && count.S === S);
    return {
      pid: generation.pid ?? null,
      bootstrap: generation.bootstrap,
      startedAt: generation.startedAt,
      exitCapturedAt: generation.exitCapturedAt ?? null,
      journalRetiredAt: generation.journalRetiredAt ?? null,
      runtimeMetadata: metadata ?? null,
      NodeA: A,
      candidateB: B,
      candidateS: S,
      candidateContribution: B === null || S === null ? null : safeSum([B, S, 64]),
      B: verified ? B : null,
      S: verified ? S : null,
      basis: verified ? "native-pool-names" : "unknown",
      nativePools: generation.nativePools,
      macPerformanceCores: input.macPerformanceCores,
      nativeFallback: "unknown",
    };
  });
  const keys = generations.map((generation) => identity(generation.bootstrap));
  const slotGenerations = generations.map((generation) =>
    JSON.stringify([
      generation.bootstrap.authorityId,
      generation.bootstrap.slotId,
      generation.bootstrap.slotGeneration,
    ]),
  );
  const identitiesValid =
    new Set(keys).size === keys.length &&
    new Set(slotGenerations).size === slotGenerations.length &&
    generations.every(
      (generation) =>
        generation.pid !== input.parentPid &&
        positive(generation.pid) &&
        generation.bootstrap.version === 1 &&
        generation.bootstrap.nonce.length > 0 &&
        generation.bootstrap.authorityId.length > 0 &&
        /^[a-f0-9]{64}$/.test(generation.bootstrap.build) &&
        generation.bootstrap.authorityId === generations[0]?.bootstrap.authorityId &&
        generation.bootstrap.build === generations[0]?.bootstrap.build &&
        Number.isSafeInteger(generation.bootstrap.slotId) &&
        generation.bootstrap.slotId >= 0 &&
        generation.bootstrap.slotId < N &&
        positive(generation.bootstrap.slotGeneration) &&
        finite(generation.startedAt) &&
        finite(generation.exitCapturedAt) &&
        finite(generation.journalRetiredAt) &&
        generation.startedAt <= generation.exitCapturedAt &&
        generation.exitCapturedAt <= generation.journalRetiredAt &&
        !generations.some(
          (other) =>
            other !== generation &&
            (other.pid === generation.pid ||
              other.bootstrap.slotId === generation.bootstrap.slotId) &&
            Math.max(other.startedAt, generation.startedAt) <
              Math.min(other.journalRetiredAt ?? Infinity, generation.journalRetiredAt ?? Infinity),
        ),
    );
  const sampleTotals = samples.map((sample) => {
    // Charging lasts through BOTH captured exit and durable journal retirement.
    const charged = generations.filter(
      (generation) =>
        generation.startedAt <= sample.at &&
        (generation.journalRetiredAt === undefined || generation.journalRetiredAt >= sample.at),
    );
    const live = charged.filter(
      (generation) =>
        generation.exitCapturedAt === undefined || generation.exitCapturedAt >= sample.at,
    );
    const populationStable = !generations.some((generation) =>
      [generation.startedAt, generation.exitCapturedAt, generation.journalRetiredAt].some(
        (at) => at !== undefined && at > sample.at && at < sample.finishedAt,
      ),
    );
    const refs = sample.generations;
    const exactBindings =
      refs.length === live.length &&
      new Set(refs.map((ref) => ref.pid)).size === refs.length &&
      live.every((generation) =>
        refs.some(
          (ref) =>
            ref.pid === generation.pid &&
            identity(ref.bootstrap) === identity(generation.bootstrap),
        ),
      );
    const rows = sample.sample.processes.filter((row) => row.pid !== input.parentPid);
    const exactRows =
      rows.length === live.length &&
      live.every((generation) => rows.filter((row) => row.pid === generation.pid).length === 1) &&
      rows.every((row) => positive(row.threads));
    const valid =
      finite(sample.at) &&
      finite(sample.finishedAt) &&
      sample.at <= sample.finishedAt &&
      populationStable &&
      exactBindings &&
      exactRows &&
      sample.sample.status === "complete" &&
      sample.sample.errors.length === 0 &&
      sample.sample.missingPids.length === 0 &&
      sample.sample.requestedPids.length === live.length + 1 &&
      new Set(sample.sample.requestedPids).size === sample.sample.requestedPids.length &&
      sample.sample.requestedPids.includes(input.parentPid) &&
      live.every((generation) => sample.sample.requestedPids.includes(generation.pid!));
    const terms = charged.map(
      (generation) => termsByGeneration[keys.indexOf(identity(generation.bootstrap))]!,
    );
    const candidateCeiling = terms.every(
      (term) => term.candidateB !== null && term.candidateS !== null,
    )
      ? safeSum([64 * N, ...terms.flatMap((term) => [term.candidateB!, term.candidateS!])])
      : null;
    const ceiling = terms.every((term) => term.B !== null && term.S !== null)
      ? safeSum([64 * N, ...terms.flatMap((term) => [term.B!, term.S!])])
      : null;
    const total = valid ? safeSum(rows.map((row) => row.threads!)) : null;
    // Missing rows cannot hide an independently proved excess. Duplicate/unbound rows never count twice.
    const lowerBound =
      populationStable && exactBindings
        ? safeSum(
            rows
              .filter(
                (row) =>
                  positive(row.threads) &&
                  rows.filter((other) => other.pid === row.pid).length === 1 &&
                  live.some((generation) => generation.pid === row.pid),
              )
              .map((row) => row.threads!),
          )
        : null;
    return {
      at: sample.at,
      finishedAt: sample.finishedAt,
      phase: sample.phase,
      generations: refs,
      chargedGenerations: charged.map((generation) => generation.bootstrap),
      total,
      lowerBound,
      candidateCeiling,
      ceiling,
      complete: valid && total !== null,
    };
  });
  const groups = ["warm", "steady", "churn", "settling"] as const;
  const phases = [
    ...new Set(samples.map((sample) => sample.phase).filter((phase) => phaseGroup(phase))),
  ];
  const phaseCoverage = Object.fromEntries(
    groups.map((group) => [
      group,
      sampleTotals.some(
        (sample) =>
          phaseGroup(sample.phase) === group && sample.complete && sample.generations.length > 0,
      ),
    ]),
  );
  const generationCoverage = generations.map((generation) => {
    const required = phases.filter((phase) => {
      const intervals = samples.filter((sample) => sample.phase === phase);
      return intervals.some(
        (sample) =>
          generation.startedAt <= sample.finishedAt &&
          (generation.exitCapturedAt ?? Infinity) >= sample.at,
      );
    });
    const covered = required.filter((phase) =>
      sampleTotals.some(
        (sample) =>
          sample.phase === phase &&
          sample.complete &&
          sample.generations.some(
            (ref) =>
              ref.pid === generation.pid &&
              identity(ref.bootstrap) === identity(generation.bootstrap),
          ),
      ),
    );
    return {
      bootstrap: generation.bootstrap,
      required,
      covered,
      complete: required.length > 0 && required.length === covered.length,
    };
  });
  const complete =
    [2, 3].includes(N) &&
    generations.length >= N &&
    identitiesValid &&
    input.lifecycleBounds === true &&
    input.observationComplete &&
    termsByGeneration.every((term) => term.basis !== "unknown") &&
    groups.every((group) => phaseCoverage[group]) &&
    generationCoverage.every((row) => row.complete) &&
    sampleTotals.length > 0 &&
    sampleTotals.every((row) => row.complete && row.ceiling !== null);
  const excess = sampleTotals.some(
    (sample) =>
      sample.lowerBound !== null && sample.ceiling !== null && sample.lowerBound > sample.ceiling,
  );
  const predicate: QualificationPredicate =
    input.lifecycleBounds === false || excess ? false : complete ? true : "unknown";
  const maxima = (values: readonly (number | null)[]) => {
    const numbers = values.filter((value) => value !== null);
    return numbers.length ? Math.max(...numbers) : null;
  };
  return {
    configuredN: N,
    R: 4 * N,
    formula: "sum(B_i+S_i)+64*N",
    termsByGeneration,
    ceiling: termsByGeneration.every((term) => term.basis !== "unknown")
      ? maxima(sampleTotals.map((sample) => sample.ceiling))
      : null,
    candidateCeiling: termsByGeneration.every(
      (term) => term.candidateB !== null && term.candidateS !== null,
    )
      ? maxima(sampleTotals.map((sample) => sample.candidateCeiling))
      : null,
    basis:
      termsByGeneration.length > 0 && termsByGeneration.every((term) => term.basis !== "unknown")
        ? "native-pool-names"
        : "unknown",
    sampleTotals,
    maximumObserved: maxima(sampleTotals.map((sample) => sample.total)),
    coverage: { identitiesValid, phaseCoverage, generationCoverage, complete },
    predicate,
    meaning:
      "Sampled compute children including guardians; parent/readers/checkpoint excluded. No peak, CPU, liveness or native-completion guarantee.",
  };
}
