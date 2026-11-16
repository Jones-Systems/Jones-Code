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
    samples.every((sample) =>
      fields.every((field) => Number.isFinite(sample?.[field]) && sample[field] >= 0),
    );
  if (!complete) return "unknown";
  const encodeTrigger = samples.some((sample) => sample.encodeMs >= 50);
  const attributionRequired = samples.some(
    (sample) => sample.decodeMs >= 50 || sample.schemaProjectionMs >= 50,
  );
  if (
    measurement.contiguousEncodeTrigger !== encodeTrigger ||
    measurement.decodeProjectionAttributionRequired !== attributionRequired
  )
    return "unknown";
  if (encodeTrigger) return "activate";
  if (
    attributionRequired ||
    samples.some((sample) => sample.loopMaxMs > 100 || sample.loopP99Ms > 50)
  )
    return "unknown";
  return "not-activated";
}
