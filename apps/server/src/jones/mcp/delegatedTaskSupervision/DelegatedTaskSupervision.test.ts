import { assert, describe, it } from "@effect/vitest";
import {
  type OrchestratorMcpDelegateTaskInput,
  type ProviderInteractionMode,
  type RuntimeMode,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { resolveInteractionMode, resolveRuntimeMode } from "../../../mcp/delegatedTaskModes.ts";
import {
  inheritsLinkedDelegationModes,
  refuseUnsolicitedSupervision,
} from "./DelegatedTaskSupervision.ts";

describe("delegated supervision accident guard", () => {
  it.effect("checks every runtime pair, inheritance, plan transition and opt-in", () =>
    Effect.gen(function* () {
      const modes: ReadonlyArray<RuntimeMode> = [
        "approval-required",
        "auto-accept-edits",
        "auto",
        "full-access",
      ];
      const interactions: ReadonlyArray<ProviderInteractionMode> = ["plan", "default"];
      for (const [parentRank, runtimeMode] of modes.entries()) {
        for (const interactionMode of interactions) {
          for (const requestedRuntime of [undefined, "inherit", ...modes] as const) {
            for (const requestedInteraction of [undefined, "inherit", ...interactions] as const) {
              for (const flag of [undefined, false, true]) {
                const request: Pick<
                  OrchestratorMcpDelegateTaskInput,
                  "runtimeMode" | "interactionMode" | "supervisionRequestedByUser"
                > = {
                  runtimeMode: requestedRuntime,
                  interactionMode: requestedInteraction,
                  ...(flag === undefined ? {} : { supervisionRequestedByUser: flag }),
                };
                const narrowerRuntime =
                  requestedRuntime !== undefined &&
                  requestedRuntime !== "inherit" &&
                  modes.indexOf(requestedRuntime) < parentRank;
                const refused =
                  flag !== true &&
                  (narrowerRuntime ||
                    (interactionMode === "default" && requestedInteraction === "plan"));
                const result = yield* refuseUnsolicitedSupervision(
                  { runtimeMode, interactionMode },
                  request,
                ).pipe(Effect.result);
                assert.equal(result._tag, refused ? "Failure" : "Success");
                if (result._tag === "Failure") {
                  assert.equal(result.failure.code, "invalid_request");
                  assert.include(result.failure.message, "Omit runtimeMode and interactionMode");
                  assert.include(
                    result.failure.message,
                    `inherits your modes (${runtimeMode}, ${interactionMode})`,
                  );
                  assert.include(
                    result.failure.message,
                    "A role or read-only task is not a reason",
                  );
                }
              }
            }
          }
        }
      }
    }),
  );

  it.effect("leaves broader requests and provider restrictions to the existing resolvers", () =>
    Effect.gen(function* () {
      const request = {
        runtimeMode: "full-access",
        interactionMode: "default",
        supervisionRequestedByUser: true,
      } as const;
      yield* refuseUnsolicitedSupervision(
        { runtimeMode: "approval-required", interactionMode: "plan" },
        request,
      );
      const runtime = yield* resolveRuntimeMode("approval-required", request.runtimeMode).pipe(
        Effect.result,
      );
      assert.equal(runtime._tag, "Failure");
      if (runtime._tag === "Failure")
        assert.equal(runtime.failure.code, "runtime_mode_escalation_denied");
      const interaction = yield* resolveInteractionMode("plan", request.interactionMode).pipe(
        Effect.result,
      );
      assert.equal(interaction._tag, "Failure");
      if (interaction._tag === "Failure")
        assert.equal(interaction.failure.code, "interaction_mode_escalation_denied");
      const target = {
        modelSelection: { instanceId: ProviderInstanceId.make("restricted"), model: "fixture" },
        supportedRuntimeModes: ["approval-required" as const],
      };
      yield* refuseUnsolicitedSupervision(
        { runtimeMode: "full-access", interactionMode: "default" },
        {},
      );
      assert.equal(
        yield* resolveRuntimeMode("full-access", undefined, target),
        "approval-required",
      );
      const unsupported = yield* resolveRuntimeMode("full-access", "full-access", target).pipe(
        Effect.result,
      );
      assert.equal(unsupported._tag, "Failure");
      if (unsupported._tag === "Failure") assert.equal(unsupported.failure.code, "invalid_request");
    }),
  );

  it("inherits linked modes only with all three authenticated delegation markers", () => {
    for (const delegatedFrom of [undefined, {}])
      for (const linkOrigin of [undefined, {}])
        for (const narrowedTo of [undefined, {}])
          assert.equal(
            inheritsLinkedDelegationModes({ delegatedFrom, linkOrigin, narrowedTo }),
            delegatedFrom !== undefined && linkOrigin !== undefined && narrowedTo !== undefined,
          );
  });
});
