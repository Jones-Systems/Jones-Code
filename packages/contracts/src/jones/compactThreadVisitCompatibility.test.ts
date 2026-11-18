import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { NonNegativeInt, ThreadId } from "../baseSchemas.ts";
import { OrchestrationV2SubscribeThreadInput } from "../orchestrationV2.ts";

// Frozen receiving aac2f02 contract, blob e32a789ebcb8bba374c3d57cd460d27b066bdf2f.
// Keep this independent of the current schema: old peers must ignore the new acceptance key.
const ReceivingSubscribeThreadInput = Schema.Struct({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(NonNegativeInt),
  requestCompletionMarker: Schema.optionalKey(Schema.Boolean),
  acceptBoundedSnapshot: Schema.optionalKey(Schema.Boolean),
  acceptDeferredWork: Schema.optionalKey(Schema.Boolean),
  acceptCompactTurnItems: Schema.optionalKey(Schema.Boolean),
  acceptDedupedBodies: Schema.optionalKey(Schema.Boolean),
  acceptCompactIds: Schema.optionalKey(Schema.Boolean),
});

const decode = Schema.decodeUnknownSync(OrchestrationV2SubscribeThreadInput);
const decodeJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2SubscribeThreadInput)),
);

const decodeReceiving = Schema.decodeUnknownSync(ReceivingSubscribeThreadInput);
const decodeReceivingJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(ReceivingSubscribeThreadInput)),
);

describe("compact thread visit acceptance", () => {
  it.each([undefined, false, true])(
    "preserves %s acceptance through native and JSON input",
    (accept) => {
      const input = {
        threadId: ThreadId.make("visit-subscriber"),
        afterSequence: 17,
        ...(accept === undefined ? {} : { acceptCompactThreadVisits: accept }),
      };
      expect(decode(input)).toEqual(input);
      expect(decodeJson(JSON.stringify(input))).toEqual(input);
      expect(Object.hasOwn(decode(input), "acceptCompactThreadVisits")).toBe(accept !== undefined);
    },
  );

  it("lets the pinned receiving decoder ignore the new opt-in without losing resume options", () => {
    const legacy = {
      threadId: ThreadId.make("old-subscriber"),
      afterSequence: 41,
      requestCompletionMarker: true,
      acceptBoundedSnapshot: true,
      acceptDeferredWork: true,
      acceptCompactTurnItems: true,
      acceptDedupedBodies: true,
      acceptCompactIds: true,
    };
    const wire = { ...legacy, acceptCompactThreadVisits: true };
    expect(decodeReceiving(wire)).toEqual(legacy);
    expect(decodeReceivingJson(JSON.stringify(wire))).toEqual(legacy);
    expect(() => decodeReceiving({ ...wire, afterSequence: -1 })).toThrow();
    expect(() => decode({ ...wire, acceptCompactThreadVisits: "true" })).toThrow();
  });
});
