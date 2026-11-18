// Timer callbacks bracket any contiguous period during which the loop could
// not run callbacks. A long interval is only an upper bound, not causal proof.
export function snapshotPhaseLoopBounds(measurement) {
  return measurement.samples.map((sample, index) => {
    const trace = sample.phaseLoopTrace;
    const ticks = trace?.ticksMs;
    const validTicks =
      trace?.overflow === false &&
      Array.isArray(ticks) &&
      ticks.length >= 2 &&
      ticks.length <= 4_096 &&
      ticks.every(
        (tick, tickIndex) =>
          Number.isFinite(tick) && tick >= 0 && (tickIndex === 0 || tick > ticks[tickIndex - 1]),
      );
    const bound = (phase, elapsedMs) => {
      const { startMs, endMs } = phase ?? {};
      if (
        !validTicks ||
        !Number.isFinite(startMs) ||
        !Number.isFinite(endMs) ||
        startMs > endMs ||
        endMs - startMs !== elapsedMs ||
        ticks[0] > startMs ||
        ticks.at(-1) < endMs
      )
        return null;
      let maxOverlapMs = 0;
      for (let tickIndex = 1; tickIndex < ticks.length; tickIndex++) {
        const overlap = Math.min(endMs, ticks[tickIndex]) - Math.max(startMs, ticks[tickIndex - 1]);
        maxOverlapMs = Math.max(maxOverlapMs, overlap);
      }
      return maxOverlapMs;
    };
    return {
      window: index + 1,
      decodeMaxGapMs: bound(trace?.decode, sample.decodeMs),
      schemaProjectionMaxGapMs: bound(trace?.schemaProjection, sample.schemaProjectionMs),
    };
  });
}

// Node's delay histogram records intervals between callbacks. With ticks on both
// sides of the workload, its maximum bounds any contiguous main-loop block.
export function snapshotDecision(measurement) {
  const samples = measurement?.samples;
  const fields = [
    "captureMs",
    "decodeMs",
    "schemaProjectionMs",
    "encodeMs",
    "loopMaxMs",
    "loopP99Ms",
  ];
  const complete =
    Array.isArray(samples) &&
    samples.length === 3 &&
    samples.every(
      (sample) =>
        Number.isSafeInteger(sample?.loopWarmupSampleCount) &&
        sample.loopWarmupSampleCount > 0 &&
        Number.isSafeInteger(sample.loopWorkEndSampleCount) &&
        sample.loopWorkEndSampleCount >= sample.loopWarmupSampleCount &&
        Number.isSafeInteger(sample.loopFinalSampleCount) &&
        sample.loopFinalSampleCount > sample.loopWorkEndSampleCount &&
        sample.loopExceededCount === 0 &&
        sample.loopSampleCount === sample.loopFinalSampleCount - sample.loopWarmupSampleCount &&
        fields.every((field) => Number.isFinite(sample[field]) && sample[field] >= 0),
    );
  if (!complete)
    return { activationDecision: "unknown", activationReason: "incomplete-measurement" };

  const thresholdCrossings = samples.flatMap((sample, index) => {
    const candidates = [
      { field: "encodeMs", limitMs: 50, crossed: sample.encodeMs >= 50 },
      { field: "decodeMs", limitMs: 50, crossed: sample.decodeMs >= 50 },
      { field: "schemaProjectionMs", limitMs: 50, crossed: sample.schemaProjectionMs >= 50 },
      { field: "loopMaxMs", limitMs: 100, crossed: sample.loopMaxMs > 100 },
      { field: "loopP99Ms", limitMs: 50, crossed: sample.loopP99Ms > 50 },
    ];
    return candidates
      .filter(({ crossed }) => crossed)
      .map(({ field, limitMs }) => ({
        window: index + 1,
        field,
        limitMs,
        observedMs: sample[field],
      }));
  });
  const encodeTrigger = thresholdCrossings.some(({ field }) => field === "encodeMs");
  const elapsedCrossings = thresholdCrossings.filter(
    ({ field }) => field === "decodeMs" || field === "schemaProjectionMs",
  );
  const elapsedTrigger = elapsedCrossings.length > 0;
  if (
    measurement.contiguousEncodeTrigger !== encodeTrigger ||
    measurement.decodeProjectionElapsedTrigger !== elapsedTrigger
  )
    return {
      activationDecision: "unknown",
      activationReason: "inconsistent-threshold-flags",
      thresholdCrossings,
    };
  if (encodeTrigger)
    return {
      activationDecision: "activate",
      activationReason: "synchronous-encode",
      thresholdCrossings,
    };
  if (elapsedCrossings.some(({ window, limitMs }) => samples[window - 1].loopMaxMs >= limitMs))
    return {
      activationDecision: "unknown",
      activationReason: "decode-or-projection-attribution",
      thresholdCrossings,
    };
  if (thresholdCrossings.some(({ field }) => field === "loopMaxMs" || field === "loopP99Ms"))
    return {
      activationDecision: "unknown",
      activationReason: "loop-target-exceeded",
      thresholdCrossings,
    };
  return {
    activationDecision: "not-activated",
    activationReason: elapsedTrigger ? "elapsed-phases-bounded" : "within-thresholds",
    thresholdCrossings,
  };
}
