import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationEffectRequestV2 as workerRequest } from "../../orchestration-v2/EffectOutbox.ts";
import { OrchestrationEffectRequestV2 as enqueueRequest } from "../../orchestration-v2/EffectOutboxEnqueue.ts";

const readers = [
  { owner: "worker", request: workerRequest },
  { owner: "enqueue", request: enqueueRequest },
] as const;
const ordinaryRequests = [
  { type: "provider-turn.start", runId: "run:interlock" },
  { type: "terminal.cleanup" },
] as const;

it.each(readers.flatMap((reader) => ordinaryRequests.map((ordinary) => ({ ...reader, ordinary }))))(
  "$owner accepts ordinary $ordinary.type but rejects native envelope ambiguity",
  ({ request, ordinary }) => {
    const accepts = Schema.is(request);
    expect(accepts(ordinary)).toBe(true);
    for (const key of ["request", "nativeCreationExecutionReference"]) {
      for (const value of [null, {}, { effectId: "native:effect" }]) {
        expect(accepts({ ...ordinary, [key]: value })).toBe(false);
      }
    }
    expect(accepts({ request: ordinary, nativeCreationExecutionReference: null })).toBe(false);
  },
);

it.each(readers)(
  "$owner retains upstream selection, continuation budget and attachment-cleanup fields",
  ({ request }) => {
    const decode = Schema.decodeUnknownSync(request);
    const accepts = Schema.is(request);
    for (const input of [
      { type: "provider-runtime.continue", sourceRunId: "run:source", processLossCount: 2 },
      { type: "attachment.cleanup", attachmentIds: ["attachment:shared"], skipReferenced: true },
      { type: "provider-session.apply-thread-title", title: "New native title" },
    ]) {
      expect(decode(input)).toEqual(input);
      expect(accepts({ ...input, nativeCreationExecutionReference: null })).toBe(false);
    }
  },
);
