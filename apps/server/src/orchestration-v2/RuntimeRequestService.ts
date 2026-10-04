import {
  ProviderApprovalDecision,
  ProviderSessionId,
  ProviderUserInputAnswers,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as EventSink from "./EventSink.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import * as NodeCrypto from "node:crypto";

export class RuntimeRequestResponseExecutionError extends Schema.TaggedError<RuntimeRequestResponseExecutionError>()(
  "RuntimeRequestResponseExecutionError",
  {
    reason: Schema.Literals([
      "request-missing",
      "request-not-ready",
      "request-not-resumable",
      "provider-session-not-active",
      "unexpected-failure",
    ]),
    threadId: ThreadId,
    providerSessionId: ProviderSessionId,
    requestId: RuntimeRequestId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "request-missing":
        return `Runtime request ${this.requestId} no longer exists on thread ${this.threadId}.`;
      case "request-not-ready":
        return `Runtime request ${this.requestId} on thread ${this.threadId} is not ready for response execution.`;
      case "request-not-resumable":
        return `Runtime request ${this.requestId} on thread ${this.threadId} is not resumable on provider session ${this.providerSessionId}.`;
      case "provider-session-not-active":
        return `Provider session ${this.providerSessionId} is not active for runtime request ${this.requestId} on thread ${this.threadId}.`;
      case "unexpected-failure":
        return `Failed to respond to runtime request ${this.requestId} on thread ${this.threadId} via provider session ${this.providerSessionId}.`;
    }
  }
}

const isRuntimeRequestResponseExecutionError = Schema.is(RuntimeRequestResponseExecutionError);

export interface RuntimeRequestServiceV2Shape {
  readonly respond: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly requestId: RuntimeRequestId;
    readonly decision?: ProviderApprovalDecision;
    readonly answers?: ProviderUserInputAnswers;
    readonly ordinaryCheckoutUse?: OrdinaryCheckout.OrdinaryCheckoutUseV1;
    readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  }) => Effect.Effect<void, RuntimeRequestResponseExecutionError>;
}

export class RuntimeRequestServiceV2 extends Context.Service<
  RuntimeRequestServiceV2,
  RuntimeRequestServiceV2Shape
>()("t3/orchestration-v2/RuntimeRequestService/RuntimeRequestServiceV2") {}

export const layer: Layer.Layer<
  RuntimeRequestServiceV2,
  never,
  | ProjectionStore.ProjectionStoreV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | EventSink.EventSinkV2
> = Layer.effect(
  RuntimeRequestServiceV2,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const eventSink = yield* EventSink.EventSinkV2;

    return RuntimeRequestServiceV2.of({
      respond: (input) =>
        Effect.gen(function* () {
          const context = yield* projections.getRuntimeResponseContext(
            input.threadId,
            input.requestId,
          );
          const request = context.request;
          if (request === undefined) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-missing",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          // Dispatch validates the request while it is pending, then persists the
          // resolved projection before this effect is executed.
          if (request.status !== "resolved") {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-ready",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          if (
            request.responseCapability.type !== "live" ||
            request.responseCapability.providerSessionId !== input.providerSessionId
          ) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-resumable",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          if (
            context.node === undefined ||
            context.node.id !== request.nodeId ||
            context.node.threadId !== input.threadId
          ) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-resumable",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
              cause: "The runtime response has no recorded request node.",
            });
          }
          const admission =
            context.node.runId === null
              ? null
              : yield* eventSink.readOrdinaryCheckoutAdmissionForRun({
                  threadId: input.threadId,
                  runId: context.node.runId,
                });
          const execution = input.ordinaryCheckoutExecution;
          const requestDigest = nativeCreationSha256(
            nativeCreationCanonicalJson(
              yield* Schema.encodeEffect(OrchestrationEffectRequestV2)({
                type: "runtime-request.respond",
                providerSessionId: input.providerSessionId,
                requestId: input.requestId,
                ...(input.decision === undefined ? {} : { decision: input.decision }),
                ...(input.answers === undefined ? {} : { answers: input.answers }),
              }).pipe(Effect.orDie),
            ),
          );
          if (
            (input.ordinaryCheckoutUse !== undefined && execution === undefined) ||
            (admission !== null &&
              (execution === undefined ||
                execution.originalUse.admission.admissionId !== admission.admissionId ||
                execution.originalUse.admission.admissionSha256 !==
                  OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission).admissionSha256)) ||
            (execution !== undefined &&
              (admission === null ||
                execution.executor.kind !== "actual_outbox_claim" ||
                execution.originalUse.lease.ownerThreadId !== input.threadId ||
                execution.executor.source.link.threadId !== input.threadId ||
                execution.executor.source.link.requestSha256 !== requestDigest ||
                (input.ordinaryCheckoutUse !== undefined &&
                  nativeCreationCanonicalJson(
                    yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutUseV1)(
                      input.ordinaryCheckoutUse,
                    ).pipe(Effect.orDie),
                  ) !==
                    nativeCreationCanonicalJson(
                      yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutUseV1)(
                        execution.originalUse,
                      ).pipe(Effect.orDie),
                    ))))
          ) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-resumable",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
              cause: "The runtime response has no matching original checkout claim.",
            });
          }
          const revalidate =
            execution === undefined
              ? Effect.void
              : eventSink.revalidateOrdinaryCheckoutExecution(execution).pipe(Effect.asVoid);
          yield* revalidate;
          const session = yield* sessions.get(input.providerSessionId);
          if (Option.isNone(session)) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "provider-session-not-active",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          yield* revalidate;
          yield* session.value.respondToRuntimeRequest({
            nativeOperation: {
              operationId: `runtime-response:${input.requestId}:${NodeCrypto.randomUUID()}`,
              operation: "respond_to_request",
              instanceId: session.value.instanceId,
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              ...(session.value.runtimeGeneration === undefined
                ? {}
                : { runtimeGeneration: session.value.runtimeGeneration }),
            },
            requestId: input.requestId,
            ...(input.decision === undefined ? {} : { decision: input.decision }),
            ...(input.answers === undefined ? {} : { answers: input.answers }),
          });
        }).pipe(
          Effect.mapError((cause) =>
            isRuntimeRequestResponseExecutionError(cause)
              ? cause
              : new RuntimeRequestResponseExecutionError({
                  reason: "unexpected-failure",
                  threadId: input.threadId,
                  providerSessionId: input.providerSessionId,
                  requestId: input.requestId,
                  cause,
                }),
          ),
        ),
    });
  }),
);
