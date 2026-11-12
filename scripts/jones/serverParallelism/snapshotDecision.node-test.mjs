import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { snapshotDecision } from "./snapshotDecision.mjs";

const measurement = (patch = {}) => {
  const samples = Array.from({ length: 3 }, () => ({
    captureMs: 1,
    decodeMs: 2,
    schemaProjectionMs: 3,
    encodeMs: 4,
    loopMaxMs: 20,
    loopP99Ms: 15,
    ...patch,
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
    for (const patch of [{ decodeMs: 50 }, { schemaProjectionMs: 80 }])
      NodeAssert.equal(snapshotDecision(measurement(patch)), "unknown");
  },
);

NodeTest.test(
  "a demonstrated synchronous encode block activates even while decode needs attribution",
  () => {
    NodeAssert.equal(snapshotDecision(measurement({ encodeMs: 50 })), "activate");
    NodeAssert.equal(snapshotDecision(measurement({ decodeMs: 80, encodeMs: 50 })), "activate");
  },
);

NodeTest.test(
  "complete below-trigger evidence with passing loop targets remains scoped nonactivation",
  () => {
    NodeAssert.equal(snapshotDecision(measurement()), "not-activated");
    NodeAssert.equal(
      snapshotDecision(measurement({ loopP99Ms: 50, loopMaxMs: 100 })),
      "not-activated",
    );
  },
);

NodeTest.test(
  "unattributed loop failures and incomplete or inconsistent samples remain unknown",
  () => {
    for (const patch of [
      { loopP99Ms: 51 },
      { loopMaxMs: 101 },
      { loopP99Ms: NaN },
      { loopP99Ms: -1 },
      { encodeMs: Infinity },
    ])
      NodeAssert.equal(snapshotDecision(measurement(patch)), "unknown");
    NodeAssert.equal(snapshotDecision({ ...measurement(), samples: [] }), "unknown");
    NodeAssert.equal(
      snapshotDecision({ ...measurement(), contiguousEncodeTrigger: true }),
      "unknown",
    );
    NodeAssert.equal(
      snapshotDecision({
        ...measurement({ decodeMs: 80 }),
        decodeProjectionAttributionRequired: false,
      }),
      "unknown",
    );
    NodeAssert.equal(snapshotDecision(undefined), "unknown");
  },
);
