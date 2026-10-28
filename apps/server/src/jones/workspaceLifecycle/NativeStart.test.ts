import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as ContextHandoff from "../../orchestration-v2/ContextHandoffService.ts";
import * as Start from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as RunExecution from "../../orchestration-v2/RunExecutionService.ts";
import * as RuntimePolicy from "../../orchestration-v2/RuntimePolicy.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as Auth from "../../provider/ProviderAuthService.ts";
import * as Instances from "../../provider/ProviderInstanceRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import * as Guard from "../nativeCreation/NativeCreationProviderGuard.ts";
import * as Admission from "../orchestration/ControlAdmission.ts";
import * as Restart from "../orchestration/RestartPreparation.ts";
import * as ContinuityAdmissionTestkit from "../goals/continuity/admission.testkit.ts";
import * as Boundary from "./NativeBoundary.ts";
import { fixture } from "./ManagedConsumer.testkit.ts";
import { nativeStartFixture } from "./NativeStart.testkit.ts";

it.effect.each(["current", "revoked", "retired"] as const)(
  "real startNative uses the original durable native guard and production issuer (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          const native = yield* nativeStartFixture(f);
          const context = yield* native.issue;
          const guard = yield* Guard.issueNativeProviderGuard({
            context,
            resources: native.resources,
            threadId: native.command.threadId,
            effectId: native.reference.effectId,
            commandId: native.command.commandId,
            runId: native.command.runId,
            providerInstanceId: f.instance.instanceId,
          });
          if (mode === "revoked") f.state.afterResume = native.revoke;
          const startLayer = Layer.fresh(Start.layer).pipe(
            Layer.provide(ContinuityAdmissionTestkit.layerAbsent),
            Layer.provide(Admission.layer),
            Layer.provide(IdAllocator.layer),
            Layer.provide(f.configured),
            Layer.provide(
              Layer.mock(Restart.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
            ),
            Layer.provide(Layer.mock(ContextHandoff.ContextHandoffServiceV2)({})),
            Layer.provide(
              Layer.mock(Switch.ProviderSwitchServiceV2)({
                plan: () =>
                  Effect.succeed({
                    instanceChanged: false,
                    modelChanged: false,
                    targetProviderThreadId: null,
                    releaseProviderSessionIds: [],
                    transition: { type: "reuse" },
                  }),
              }),
            ),
            Layer.provide(Layer.mock(GitWorkflow.GitWorkflowService)({})),
            Layer.provide(
              Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone }),
            ),
            Layer.provide(
              Layer.mock(Auth.ProviderAuthService)({
                tryHandlePromptCommand: () => Effect.succeed(false),
              }),
            ),
            Layer.provide(
              Layer.mock(RunExecution.RunExecutionServiceV2)({
                startRootRun: (input) =>
                  input.session
                    .startTurn({
                      nativeCreationGuard: guard,
                      appThread: input.appThread,
                      threadId: input.run.threadId,
                      runId: input.run.id,
                      runOrdinal: input.run.ordinal,
                      attemptId: input.attemptId,
                      rootNodeId: input.rootNode.id,
                      providerTurnOrdinal: input.providerTurnOrdinal,
                      providerThread: input.providerThread,
                      message: input.message,
                      modelSelection: input.modelSelection,
                      runtimePolicy: input.runtimePolicy,
                    })
                    .pipe(
                      Effect.mapError(
                        (cause) =>
                          new RunExecution.RunExecutionStartError({
                            commandId: input.commandId,
                            runId: input.run.id,
                            cause,
                          }),
                      ),
                    ),
              }),
            ),
            Layer.provide(RuntimePolicy.layer),
            Layer.provide(Settings.layerTest()),
            Layer.provide(
              Layer.mock(Instances.ProviderInstanceRegistry)({
                getInstance: () => Effect.succeed(f.instance),
              }),
            ),
          );
          const start = Effect.flatMap(Start.ProviderTurnStartServiceV2, (service) =>
            service.startNative!({
              threadId: native.command.threadId,
              runId: native.command.runId,
              guard,
            }),
          ).pipe(
            (effect) =>
              Boundary.withOriginalTransition(
                `effect:${native.reference.effectId}`,
                {
                  commandId: native.command.commandId,
                  threadId: native.command.threadId,
                  request: { type: "provider-turn.start", runId: native.command.runId },
                },
                effect,
              ),
            Effect.provide(startLayer),
          );
          const result = yield* Effect.exit(start);
          expect(Exit.isSuccess(result)).toBe(mode !== "revoked");
          expect(f.state.resumes).toBe(mode === "revoked" ? 1 : 2);
          expect(f.calls.includes("provider-send")).toBe(mode !== "revoked");
          if (mode !== "revoked") {
            expect(f.calls.indexOf("cas")).toBeLessThan(f.calls.indexOf("provider-open"));
            expect(f.calls[f.calls.indexOf("provider-open") - 1]).toBe("show");
            const acknowledgement = Guard.readNativeProviderAcknowledgement(guard)!;
            yield* Guard.retireNativeProviderGuard(guard, acknowledgement);
            expect(
              Exit.isFailure(
                yield* Effect.exit(
                  Guard.assertNativeProviderStartOperation(
                    guard,
                    native.command.threadId,
                    native.command.runId,
                  ),
                ),
              ),
            ).toBe(true);
            if (mode === "retired") expect(Exit.isFailure(yield* Effect.exit(start))).toBe(true);
          } else
            expect(
              Option.isNone(Option.fromUndefinedOr(Guard.readNativeProviderAcknowledgement(guard))),
            ).toBe(true);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
