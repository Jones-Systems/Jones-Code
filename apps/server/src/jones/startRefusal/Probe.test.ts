import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { probeStartAdmission } from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Continuity from "../goals/continuity/admission.testkit.ts";
import * as Control from "../orchestration/ControlAdmission.ts";
import * as Fixture from "./integration.testkit.ts";

it.effect.each(["clear", "control_held", "continuity_held"] as const)(
  "read-only probe respects the existing %s fences",
  (gate) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const before = yield* f.orchestrator.getThreadProjection(f.threadId);
      const result = yield* probeStartAdmission(f.threadId).pipe(
        Effect.provide(
          Layer.mergeAll(
            gate === "continuity_held"
              ? Continuity.layerWithRequirement({
                  outcome: "hold",
                  reason: "delivery_unknown",
                  current: null,
                  gate: {
                    outcome: "hold",
                    reason: "delivery_unknown",
                    fenceRevision: "1",
                    deliveryIds: ["old"],
                  },
                })
              : Continuity.layerAbsent,
            gate === "control_held"
              ? Layer.mock(Control.ControlAdmission)({
                  assertOpen: ({ instanceId }) =>
                    Effect.fail(new Control.ControlAdmissionHeld({ instanceId })),
                })
              : Control.layer,
          ),
        ),
      );
      assert.equal(result, gate === "clear");
      assert.deepEqual(yield* f.orchestrator.getThreadProjection(f.threadId), before);
    }).pipe(Effect.provide(Fixture.runtime)),
);
