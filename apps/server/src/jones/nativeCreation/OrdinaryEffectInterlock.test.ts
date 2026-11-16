import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationEffectRequestV2 as workerRequest } from "../../orchestration-v2/EffectOutbox.ts";
import { OrchestrationEffectRequestV2 as enqueueRequest } from "../../orchestration-v2/EffectOutboxEnqueue.ts";

// Both queue readers must refuse reserved envelope keys, including explicit null.
for (const [owner, request] of [["worker", workerRequest], ["enqueue", enqueueRequest]] as const) {
  for (const ordinary of [
    { type: "provider-turn.start", runId: "run:interlock" },
    { type: "terminal.cleanup" },
  ]) {
    it(`${owner} accepts ordinary ${ordinary.type} but rejects native envelope ambiguity`, () => {
      const accepts = Schema.is(request);
      expect(accepts(ordinary)).toBe(true);
      for (const key of ["request", "nativeCreationExecutionReference"]) {
        for (const value of [null, {}, { effectId: "native:effect" }]) {
          expect(accepts({ ...ordinary, [key]: value })).toBe(false);
        }
      }
      expect(accepts({ request: ordinary, nativeCreationExecutionReference: null })).toBe(false);
    });
  }
}
