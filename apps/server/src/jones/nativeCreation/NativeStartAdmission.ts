import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Option from "effect/Option";
import {
  ProviderAdapterProtocolError,
  runtimeBinding,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as StartAdmission from "../provider/StartAdmission.ts";
import * as Guard from "./NativeCreationProviderGuard.ts";
import { CurrentEffectClaim } from "../workspaceLifecycle/ActiveEffectClaim.ts";
import { sameOperationRequest } from "../workspaceLifecycle/NativeOperation.ts";
import * as CommandReceipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import * as StartRefusals from "../startRefusal/Store.ts";

// This layer consumes the existing claiming worker and native execution owner.
// It checks authority repeatedly; only ProviderTurnStart crosses the durable prefix.
export const layer = Layer.effect(
  StartAdmission.StartAdmission,
  Effect.gen(function* () {
    const outbox = yield* Outbox.EffectOutboxV2;
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const control = yield* ControlAdmission.ControlAdmission;
    const refusals = yield* StartRefusals.StartRefusals;
    return StartAdmission.StartAdmission.of({
      capture: Effect.fn("NativeStartAdmission.capture")(function* (input) {
        const refused = () =>
          new ProviderAdapterProtocolError({
            driver: input.providerThread.driver,
            detail: "The original recorded start no longer has current execution authority.",
          });
        const claim = yield* Effect.serviceOption(CurrentEffectClaim);
        if (Option.isNone(claim)) return yield* refused();
        const captured = yield* claim.value.use(Effect.succeed).pipe(Effect.mapError(refused));
        const native = yield* Effect.serviceOption(Guard.CurrentNativeGuard);
        if (
          captured.threadId !== input.threadId ||
          (captured.request.type !== "provider-turn.start" &&
            captured.request.type !== "provider-turn.restart") ||
          captured.request.runId !== input.run.id ||
          captured.status !== "running" ||
          captured.leaseOwner === null ||
          captured.payloadIdentity === undefined ||
          (captured.nativeCreationExecutionReference !== undefined && Option.isNone(native)) ||
          (captured.nativeCreationExecutionReference === undefined && Option.isSome(native))
        )
          return yield* refused();
        const nativeCreationGuard = Option.getOrUndefined(native);
        const receipt = yield* receipts
          .getByCommandId(captured.commandId)
          .pipe(Effect.mapError(refused));
        if (
          Option.isNone(receipt) ||
          receipt.value.status !== "accepted" ||
          receipt.value.resultSequence < 1
        )
          return yield* refused();
        const originalReceipt = receipt.value;
        const validateReceiptTarget = Effect.fn("NativeStartAdmission.validateReceiptTarget")(
          function* () {
            if (originalReceipt.threadId === input.threadId) return;
            if (
              originalReceipt.commandType !== "delegated_task.request" ||
              captured.request.type !== "provider-turn.start" ||
              captured.id !== `effect:${captured.commandId}:provider-turn.start:${input.run.id}`
            )
              return yield* refused();
            // Delegation records its receipt on the parent and its start on the child.
            // The original multi-thread commit must prove that exact relationship.
            const rows = yield* sink
              .readByCommandId({ commandId: captured.commandId })
              .pipe(Stream.runCollect, Effect.mapError(refused));
            if (
              rows.at(-1)?.sequence !== originalReceipt.resultSequence ||
              rows.some((row) => row.commandId !== captured.commandId)
            )
              return yield* refused();
            const task = rows.find(
              ({ event }) =>
                event.type === "subagent.updated" &&
                event.threadId === originalReceipt.threadId &&
                event.payload.origin === "app_owned" &&
                event.payload.threadId === originalReceipt.threadId &&
                event.payload.childThreadId === input.threadId,
            )?.event;
            const birth = rows.find(
              ({ event }) =>
                event.type === "thread.created" &&
                event.threadId === input.threadId &&
                event.payload.id === input.threadId,
            )?.event;
            const bornRun = rows.find(
              ({ event }) =>
                event.type === "run.created" &&
                event.threadId === input.threadId &&
                event.payload.id === input.run.id,
            )?.event;
            const bornAttempt = rows.find(
              ({ event }) =>
                event.type === "run-attempt.created" &&
                event.threadId === input.threadId &&
                event.payload.id === input.attemptId,
            )?.event;
            if (
              task?.type !== "subagent.updated" ||
              birth?.type !== "thread.created" ||
              bornRun?.type !== "run.created" ||
              bornAttempt?.type !== "run-attempt.created" ||
              birth.payload.lineage.parentThreadId !== originalReceipt.threadId ||
              birth.payload.lineage.relationshipToParent !== "subagent" ||
              birth.payload.forkedFrom?.type !== "node" ||
              birth.payload.forkedFrom.nodeId !== task.payload.id ||
              bornRun.payload.threadId !== input.threadId ||
              bornRun.payload.userMessageId !== input.run.userMessageId ||
              bornRun.payload.activeAttemptId !== input.attemptId ||
              bornRun.payload.providerThreadId !== input.providerThread.id ||
              bornRun.payload.providerInstanceId !== input.run.providerInstanceId ||
              !sameOperationRequest(bornRun.payload.modelSelection, input.run.modelSelection) ||
              bornAttempt.payload.runId !== input.run.id ||
              bornAttempt.payload.providerThreadId !== input.providerThread.id
            )
              return yield* refused();
          },
        );
        yield* validateReceiptTarget();
        let producerGeneration = input.providerThread.runtimeIdentity?.runtimeGeneration;
        let nativeThreadId = input.providerThread.nativeThreadRef?.nativeId ?? undefined;
        const validateLiveClaim = claim.value.use((current) =>
          sameOperationRequest(current, captured) ? Effect.void : Effect.fail(refused()),
        );
        const readSettledClaim = Effect.fn("NativeStartAdmission.readSettledClaim")(function* () {
          const current = yield* outbox.get(captured.id);
          if (Option.isNone(current) || current.value.status !== "succeeded") return false;
          const settled = current.value;
          if (
            settled.commandId !== captured.commandId ||
            settled.threadId !== input.threadId ||
            settled.payloadIdentity !== captured.payloadIdentity ||
            settled.attemptCount !== captured.attemptCount ||
            settled.completedAt === null
          )
            return yield* refused();
          return true;
        });
        const revalidate = Effect.gen(function* () {
          const continuation = yield* StartAdmission.CurrentCapacityContinuation;
          if (
            continuation !== undefined &&
            (continuation.runId !== input.run.id ||
              continuation.attemptId !== input.attemptId ||
              nativeCreationGuard !== undefined)
          )
            return yield* refused();
          const settledClaim = continuation !== undefined && (yield* readSettledClaim());
          if (!settledClaim) yield* validateLiveClaim;
          const accepted = yield* receipts.getByCommandId(captured.commandId);
          if (Option.isNone(accepted) || !sameOperationRequest(accepted.value, originalReceipt))
            return yield* refused();
          yield* validateReceiptTarget();
          yield* sink.assertRuntimeStopStartAllowed({
            threadId: input.threadId,
            runId: input.run.id,
          });
          const fresh = yield* projections.getTurnStartContext(input.threadId, input.run.id);
          const run = fresh.runs.find((row) => row.id === input.run.id);
          const attempt = fresh.attempts.find((row) => row.id === input.attemptId);
          const provider = fresh.providerThreads.find((row) => row.id === input.providerThread.id);
          if (
            fresh.thread.archivedAt !== null ||
            fresh.thread.deletedAt !== null ||
            run?.activeAttemptId !== input.attemptId ||
            run.threadId !== input.threadId ||
            !["starting", "running"].includes(run.status) ||
            run.providerThreadId !== input.providerThread.id ||
            run.providerInstanceId !== input.run.providerInstanceId ||
            !sameOperationRequest(run.modelSelection, input.run.modelSelection) ||
            attempt?.runId !== input.run.id ||
            attempt.providerThreadId !== input.providerThread.id ||
            provider?.providerInstanceId !== input.providerThread.providerInstanceId ||
            provider.appThreadId !== input.threadId ||
            provider.driver !== input.providerThread.driver ||
            provider.providerSessionId !== input.providerThread.providerSessionId
          )
            return yield* refused();
          const observedGeneration = provider.runtimeIdentity?.runtimeGeneration;
          const observedNativeId = provider.nativeThreadRef?.nativeId ?? undefined;
          if (
            observedNativeId !== undefined &&
            provider.nativeThreadRef?.driver !== provider.driver
          )
            return yield* refused();
          if (nativeThreadId !== undefined && observedNativeId !== nativeThreadId)
            return yield* refused();
          if (
            continuation !== undefined &&
            (run.status !== "running" ||
              observedGeneration === undefined ||
              !sameOperationRequest(
                continuation.binding,
                runtimeBinding(provider, observedGeneration),
              ))
          )
            return yield* refused();
          const publication = yield* StartAdmission.CurrentBindingPublication;
          if (observedGeneration !== producerGeneration || observedNativeId !== nativeThreadId) {
            // Only the manager's retained publication may move this admission to
            // its exact reserved successor. A fresh projection alone is not proof.
            const binding =
              observedGeneration === undefined
                ? undefined
                : runtimeBinding(provider, observedGeneration);
            if (
              publication === undefined ||
              binding === undefined ||
              publication.previousGeneration !== (producerGeneration ?? null) ||
              binding.threadId !== input.threadId ||
              !sameOperationRequest(binding, publication.binding)
            )
              return yield* refused();
          }
          const publishing = yield* ControlAdmission.startupPublicationFor(
            publication?.binding,
            provider,
          );
          yield* control.assertOpen(
            {
              instanceId: provider.providerInstanceId,
              threadId: input.threadId,
              nativeThreadId: provider.nativeThreadRef?.nativeId ?? null,
              producer:
                observedGeneration === undefined || provider.providerSessionId === null
                  ? null
                  : {
                      instanceId: provider.providerInstanceId,
                      providerSessionId: provider.providerSessionId,
                      runtimeGeneration: observedGeneration,
                    },
            },
            publishing,
          );
          if (nativeCreationGuard !== undefined)
            yield* Guard.assertNativeProviderStartOperation(
              nativeCreationGuard,
              input.threadId,
              input.run.id,
            );
          if (!settledClaim) yield* claim.value.check;
          producerGeneration = observedGeneration;
          nativeThreadId = observedNativeId;
        }).pipe(Effect.mapError(refused), Effect.withSpan("NativeStartAdmission.revalidate"));
        yield* revalidate;
        const classifyResume = Effect.gen(function* () {
          yield* revalidate;
          const sourceId = input.run.manualContinuationOfRunId;
          if (sourceId === undefined || captured.nativeCreationExecutionReference !== undefined)
            return "held" as const;
          const records = yield* projections.getThreadRecords(input.threadId, [
            "runs",
            "attempts",
            "providerThreads",
          ]);
          const source = records.runs.find((row) => row.id === sourceId);
          const attempt = records.attempts.find((row) => row.id === source?.activeAttemptId);
          if (
            source === undefined ||
            source.manualContinuationOfRunId !== undefined ||
            attempt === undefined ||
            attempt.runId !== source.id
          )
            return "held" as const;
          const prefix = yield* refusals.readPrefix({
            threadId: input.threadId,
            runId: source.id,
            attemptId: attempt.id,
          });
          // The original refusal certificate is positive no-effect evidence.
          if (prefix.state === "refused") return "unsent" as const;
          // A cancelled original row with a monotonic zero claim count is stronger
          // than pending/retry state. Both current and receiving workers claim before
          // invoking a start; retry/recovery never reset this counter, and the table
          // rebuild preserves it. Pruned/legacy or later recovery rows do not qualify.
          if (
            prefix.state === "unknown" &&
            attempt.reason === "initial" &&
            attempt.attemptOrdinal === 1 &&
            records.attempts.filter((row) => row.runId === source.id).length === 1 &&
            (source.status === "failed" || source.status === "interrupted") &&
            source.startedAt === null
          ) {
            const rows = (yield* outbox.listByThreadId(input.threadId)).filter(
              (row) =>
                (row.request.type === "provider-turn.start" ||
                  row.request.type === "provider-turn.restart") &&
                row.request.runId === source.id,
            );
            const original = rows[0];
            if (
              rows.length === 1 &&
              original !== undefined &&
              original.request.type === "provider-turn.start" &&
              original.nativeCreationExecutionReference === undefined &&
              original.id === `effect:${original.commandId}:provider-turn.start:${source.id}` &&
              original.status === "cancelled" &&
              original.attemptCount === 0 &&
              original.leaseOwner === null &&
              original.payloadIdentity !== undefined
            ) {
              const receipt = yield* receipts.getByCommandId(original.commandId);
              const events = yield* sink
                .readByCommandId({ commandId: original.commandId })
                .pipe(Stream.runCollect);
              const bornRun = events.find(
                ({ event }) => event.type === "run.created" && event.payload.id === source.id,
              );
              const bornAttempt = events.find(
                ({ event }) =>
                  event.type === "run-attempt.created" && event.payload.id === attempt.id,
              );
              const current = yield* outbox.get(original.id);
              if (
                Option.isSome(receipt) &&
                receipt.value.status === "accepted" &&
                receipt.value.threadId === input.threadId &&
                receipt.value.resultSequence === events.at(-1)?.sequence &&
                events.every(
                  (row) =>
                    row.commandId === original.commandId && row.event.threadId === input.threadId,
                ) &&
                bornRun?.event.type === "run.created" &&
                bornRun.event.payload.userMessageId === source.userMessageId &&
                bornRun.event.payload.activeAttemptId === attempt.id &&
                bornRun.event.payload.startedAt === null &&
                bornAttempt?.event.type === "run-attempt.created" &&
                bornAttempt.event.payload.runId === source.id &&
                bornAttempt.event.payload.attemptOrdinal === 1 &&
                bornAttempt.event.payload.startedAt === null &&
                Option.isSome(current) &&
                sameOperationRequest(current.value, original)
              ) {
                yield* revalidate;
                return "unsent" as const;
              }
            }
          }
          const provider = records.providerThreads.find(
            (row) => row.id === source.providerThreadId,
          );
          const settlement = attempt.providerSettlement;
          if (
            provider?.id === input.providerThread.id &&
            provider.providerInstanceId === input.providerThread.providerInstanceId &&
            provider.providerSessionId === input.providerThread.providerSessionId &&
            provider.driver === input.providerThread.driver &&
            provider.nativeThreadRef !== null &&
            provider.nativeThreadRef !== undefined &&
            provider.nativeThreadRef.strength === "strong" &&
            provider.nativeThreadRef.driver === input.providerThread.driver &&
            source.providerInstanceId === input.providerThread.providerInstanceId &&
            attempt.nativeThreadId !== undefined &&
            attempt.nativeThreadId === input.providerThread.nativeThreadRef?.nativeId &&
            provider.nativeThreadRef.nativeId === attempt.nativeThreadId &&
            settlement != null &&
            settlement.runAttemptId === attempt.id &&
            settlement.providerTurnId === attempt.providerTurnId
          ) {
            yield* revalidate;
            return "native_resume" as const;
          }
          return "held" as const;
        }).pipe(Effect.mapError(refused));
        return {
          revalidate,
          classifyResume,
          ...(nativeCreationGuard === undefined ? {} : { nativeCreationGuard }),
        };
      }),
    });
  }),
);
