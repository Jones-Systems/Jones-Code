import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { snapshotDecision, snapshotPhaseLoopBounds } from "./snapshotDecision.mjs";

const measurement = (...windows) => {
  const samples = Array.from({ length: 3 }, (_, index) => ({
    captureMs: 1,
    decodeMs: 2,
    schemaProjectionMs: 3,
    encodeMs: 4,
    loopWarmupSampleCount: 2,
    loopWorkEndSampleCount: 7,
    loopFinalSampleCount: 8,
    loopExceededCount: 0,
    loopSampleCount: 6,
    loopMaxMs: 20,
    loopP99Ms: 15,
    ...windows[index],
  }));
  return {
    samples,
    contiguousEncodeTrigger: samples.some((sample) => sample.encodeMs >= 50),
    decodeProjectionElapsedTrigger: samples.some(
      (sample) => sample.decodeMs >= 50 || sample.schemaProjectionMs >= 50,
    ),
  };
};

const phaseTrace = {
  ticksMs: [0, 85, 97, 109, 121, 133, 145, 157, 240],
  overflow: false,
  decode: { startMs: 90, endMs: 110 },
  schemaProjection: { startMs: 110, endMs: 160 },
};

NodeTest.test("phase bounds use only the portion of each timer gap inside the phase", () => {
  const report = measurement({
    decodeMs: 20,
    schemaProjectionMs: 50,
    loopMaxMs: 85,
    phaseLoopTrace: phaseTrace,
  });
  NodeAssert.deepEqual(snapshotPhaseLoopBounds(report), [
    {
      window: 1,
      captureMaxGapMs: null,
      decodeMaxGapMs: 12,
      captureThroughDecodeMaxGapMs: null,
      schemaProjectionMaxGapMs: 12,
    },
    ...[2, 3].map((window) => ({
      window,
      captureMaxGapMs: null,
      decodeMaxGapMs: null,
      captureThroughDecodeMaxGapMs: null,
      schemaProjectionMaxGapMs: null,
    })),
  ]);
  NodeAssert.equal(snapshotDecision(report).activationDecision, "unknown");
});

NodeTest.test("a timer gap can straddle two individually bounded snapshot phases", () => {
  const report = measurement({
    captureMs: 36,
    decodeMs: 24,
    schemaProjectionMs: 30,
    loopMaxMs: 57,
    loopP99Ms: 57,
    phaseLoopTrace: {
      ticksMs: [0, 5, 62, 72, 82, 92, 102],
      overflow: false,
      capture: { startMs: 10, endMs: 46 },
      decode: { startMs: 46, endMs: 70 },
      schemaProjection: { startMs: 70, endMs: 100 },
    },
  });
  NodeAssert.deepEqual(snapshotPhaseLoopBounds(report)[0], {
    window: 1,
    captureMaxGapMs: 36,
    decodeMaxGapMs: 16,
    captureThroughDecodeMaxGapMs: 52,
    schemaProjectionMaxGapMs: 10,
  });
  NodeAssert.equal(snapshotDecision(report).activationReason, "loop-target-exceeded");
});

NodeTest.test("phase bounds require intact timer coverage and matching measurements", () => {
  for (const trace of [
    { ...phaseTrace, overflow: true },
    { ...phaseTrace, ticksMs: [95, 109, 157, 240] },
    { ...phaseTrace, ticksMs: [0, 160, 160, 240] },
    { ...phaseTrace, ticksMs: [0, 97, 109] },
    { ...phaseTrace, decode: { startMs: 90, endMs: 111 } },
  ]) {
    const [evidence] = snapshotPhaseLoopBounds(
      measurement({ decodeMs: 20, phaseLoopTrace: trace }),
    );
    NodeAssert.deepEqual(evidence, {
      window: 1,
      captureMaxGapMs: null,
      decodeMaxGapMs: null,
      captureThroughDecodeMaxGapMs: null,
      schemaProjectionMaxGapMs: null,
    });
  }
});

NodeTest.test(
  "elapsed phases remain unknown when the covered window cannot exclude a contiguous block",
  () => {
    NodeAssert.deepEqual(
      snapshotDecision(
        measurement({}, { schemaProjectionMs: 80, loopMaxMs: 50 }, { decodeMs: 50 }),
      ),
      {
        activationDecision: "unknown",
        activationReason: "decode-or-projection-attribution",
        thresholdCrossings: [
          { window: 2, field: "schemaProjectionMs", limitMs: 50, observedMs: 80 },
          { window: 3, field: "decodeMs", limitMs: 50, observedMs: 50 },
        ],
      },
    );
  },
);

NodeTest.test(
  "covered low loop maxima rule out long contiguous blocks despite elapsed phase crossings",
  () => {
    NodeAssert.deepEqual(
      snapshotDecision(measurement({ schemaProjectionMs: 80 }, { decodeMs: 50, loopMaxMs: 49.99 })),
      {
        activationDecision: "not-activated",
        activationReason: "elapsed-phases-bounded",
        thresholdCrossings: [
          { window: 1, field: "schemaProjectionMs", limitMs: 50, observedMs: 80 },
          { window: 2, field: "decodeMs", limitMs: 50, observedMs: 50 },
        ],
      },
    );
    NodeAssert.equal(
      snapshotDecision(measurement({ loopWorkEndSampleCount: 2 })).activationDecision,
      "not-activated",
    );
  },
);

NodeTest.test(
  "a demonstrated synchronous encode block activates even while decode needs attribution",
  () => {
    NodeAssert.equal(
      snapshotDecision(measurement({ encodeMs: 50 })).activationDecision,
      "activate",
    );
    NodeAssert.equal(
      snapshotDecision(measurement({ decodeMs: 80, encodeMs: 50 })).activationReason,
      "synchronous-encode",
    );
  },
);

NodeTest.test(
  "complete below-trigger evidence with passing loop targets remains scoped nonactivation",
  () => {
    NodeAssert.equal(snapshotDecision(measurement()).activationDecision, "not-activated");
    NodeAssert.equal(
      snapshotDecision(measurement({ loopP99Ms: 50, loopMaxMs: 100 })).activationDecision,
      "not-activated",
    );
  },
);

NodeTest.test(
  "loop targets identify their own breaches even when every measured phase is under its limit",
  () => {
    NodeAssert.deepEqual(snapshotDecision(measurement({}, { loopP99Ms: 51, loopMaxMs: 101 })), {
      activationDecision: "unknown",
      activationReason: "loop-target-exceeded",
      thresholdCrossings: [
        { window: 2, field: "loopMaxMs", limitMs: 100, observedMs: 101 },
        { window: 2, field: "loopP99Ms", limitMs: 50, observedMs: 51 },
      ],
    });
  },
);

NodeTest.test("empty loop histograms and invalid or incomplete timings remain unknown", () => {
  for (const patch of [
    { loopSampleCount: 0, loopMaxMs: 0, loopP99Ms: 0.000511 },
    { loopSampleCount: undefined },
    { loopWarmupSampleCount: 0, loopSampleCount: 8 },
    { loopWorkEndSampleCount: 1 },
    { schemaProjectionMs: 80, loopWorkEndSampleCount: 8 },
    { loopFinalSampleCount: undefined },
    { loopExceededCount: 1 },
    { loopP99Ms: NaN },
    { loopP99Ms: -1 },
    { encodeMs: Infinity },
  ]) {
    NodeAssert.deepEqual(snapshotDecision(measurement(patch)), {
      activationDecision: "unknown",
      activationReason: "incomplete-measurement",
    });
  }
  NodeAssert.equal(
    snapshotDecision({ ...measurement(), samples: [] }).activationReason,
    "incomplete-measurement",
  );
  NodeAssert.equal(snapshotDecision(undefined).activationReason, "incomplete-measurement");
});

NodeTest.test("threshold flags must agree with their recorded timings", () => {
  NodeAssert.equal(
    snapshotDecision({ ...measurement(), contiguousEncodeTrigger: true }).activationReason,
    "inconsistent-threshold-flags",
  );
  NodeAssert.equal(
    snapshotDecision({
      ...measurement({ decodeMs: 80 }),
      decodeProjectionElapsedTrigger: false,
    }).activationReason,
    "inconsistent-threshold-flags",
  );
});
