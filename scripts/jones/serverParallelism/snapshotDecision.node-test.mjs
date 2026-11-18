import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { snapshotDecision } from "./snapshotDecision.mjs";

const measurement = (...windows) => {
  const samples = Array.from({ length: 3 }, (_, index) => ({
    captureMs: 1,
    decodeMs: 2,
    schemaProjectionMs: 3,
    encodeMs: 4,
    loopSampleCount: 6,
    loopMaxMs: 20,
    loopP99Ms: 15,
    ...windows[index],
  }));
  return {
    samples,
    contiguousEncodeTrigger: samples.some((sample) => sample.encodeMs >= 50),
    decodeProjectionAttributionRequired: samples.some(
      (sample) => sample.decodeMs >= 50 || sample.schemaProjectionMs >= 50,
    ),
  };
};

NodeTest.test(
  "elapsed decode or projection requests attribution without activating offload",
  () => {
    NodeAssert.deepEqual(
      snapshotDecision(measurement({}, { schemaProjectionMs: 80 }, { decodeMs: 50 })),
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
      decodeProjectionAttributionRequired: false,
    }).activationReason,
    "inconsistent-threshold-flags",
  );
});
