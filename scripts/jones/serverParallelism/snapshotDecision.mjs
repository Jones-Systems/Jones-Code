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
