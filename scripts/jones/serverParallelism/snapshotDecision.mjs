// Elapsed Effect work is an attribution request, not evidence of a contiguous CPU block.
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
        Number.isSafeInteger(sample?.loopSampleCount) &&
        sample.loopSampleCount > 0 &&
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
  const attributionRequired = thresholdCrossings.some(
    ({ field }) => field === "decodeMs" || field === "schemaProjectionMs",
  );
  if (
    measurement.contiguousEncodeTrigger !== encodeTrigger ||
    measurement.decodeProjectionAttributionRequired !== attributionRequired
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
  if (attributionRequired)
    return {
      activationDecision: "unknown",
      activationReason: "decode-or-projection-attribution",
      thresholdCrossings,
    };
  if (thresholdCrossings.length > 0)
    return {
      activationDecision: "unknown",
      activationReason: "loop-target-exceeded",
      thresholdCrossings,
    };
  return {
    activationDecision: "not-activated",
    activationReason: "within-thresholds",
    thresholdCrossings,
  };
}
