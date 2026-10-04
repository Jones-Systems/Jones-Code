import {
  NodeId,
  CommandId,
  OrchestrationV2DomainEvent,
  OrchestrationV2StoredEvent,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderTurn,
  type ModelSelection,
  type RuntimeMode,
  type ProviderInteractionMode,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RawEventId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { ProviderAdapterV2Event, type ProviderRuntimeBinding } from "./ProviderAdapter.ts";
import { makeProviderFailureTurnItem } from "./ProviderFailure.ts";
import {
  copyProviderEventOrigin,
  readProviderEventOrigin,
  type ClaudeBufferedSubagentCompletionDerivationV1,
  type ProviderEventOrigin,
  type ProviderEventProducerOrigin,
} from "./ProviderEventOrigin.ts";

export class ProviderEventNormalizeError extends Schema.TaggedError<ProviderEventNormalizeError>()(
  "ProviderEventNormalizeError",
  {
    providerSessionId: ProviderSessionId,
    threadId: ThreadId,
    providerEvent: ProviderAdapterV2Event,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to normalize provider event ${this.providerEvent.type} for thread ${this.threadId}.`;
  }
}

export class ProviderEventPublishError extends Schema.TaggedError<ProviderEventPublishError>()(
  "ProviderEventPublishError",
  {
    providerSessionId: ProviderSessionId,
    eventCount: Schema.Number,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to publish ${this.eventCount} normalized provider event(s).`;
  }
}

export const ProviderEventIngestorV2Error = Schema.Union([
  ProviderEventNormalizeError,
  ProviderEventPublishError,
]);
export type ProviderEventIngestorV2Error = typeof ProviderEventIngestorV2Error.Type;

export interface ProviderTurnAnalyticsContext {
  readonly modelSelection: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
}

export class ProviderTurnAnalytics extends Context.Reference<{
  readonly record: (properties: Readonly<Record<string, unknown>>) => Effect.Effect<void>;
}>("t3/orchestration-v2/ProviderTurnAnalytics", {
  defaultValue: () => ({ record: () => Effect.void }),
}) {}

export const analyticsLive = Layer.effect(
  ProviderTurnAnalytics,
  Effect.gen(function* () {
    const analytics = yield* AnalyticsService.AnalyticsService;
    return {
      record: (properties: Readonly<Record<string, unknown>>) =>
        analytics.record("provider.turn.completed", properties),
    };
  }),
);

function providerTurnAnalyticsProperties(input: {
  readonly driver: ProviderAdapterV2Event["driver"];
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly context?: ProviderTurnAnalyticsContext;
}): Readonly<Record<string, unknown>> {
  const usage = input.providerTurn.turnTokenUsage;
  const modelSelection = input.context?.modelSelection;
  const effort = modelSelection
    ? (getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
      getModelSelectionStringOptionValue(modelSelection, "effort"))
    : undefined;
  return {
    provider: input.driver,
    terminalStatus: input.providerTurn.status,
    usageStatus: usage?.usageStatus ?? "unavailable",
    usageScope: usage?.usageScope ?? "main_agent",
    ...(usage ? { hasSubagents: usage.hasSubagents } : {}),
    ...(usage?.inputTokens === undefined ? {} : { inputTokens: usage.inputTokens }),
    ...(usage?.cachedInputTokens === undefined
      ? {}
      : { cachedInputTokens: usage.cachedInputTokens }),
    ...(usage?.cacheCreationTokens === undefined
      ? {}
      : { cacheCreationTokens: usage.cacheCreationTokens }),
    ...(usage?.outputTokens === undefined ? {} : { outputTokens: usage.outputTokens }),
    ...(usage?.reasoningTokens === undefined ? {} : { reasoningTokens: usage.reasoningTokens }),
    ...(modelSelection ? { model: modelSelection.model, mixedModels: false } : {}),
    ...(effort ? { effort } : {}),
    ...(input.context?.runtimeMode ? { runtimeMode: input.context.runtimeMode } : {}),
    ...(input.context?.interactionMode ? { interactionMode: input.context.interactionMode } : {}),
    ...(input.providerTurn.startedAt && input.providerTurn.completedAt
      ? {
          durationMs: Math.max(
            0,
            DateTime.toEpochMillis(input.providerTurn.completedAt) -
              DateTime.toEpochMillis(input.providerTurn.startedAt),
          ),
        }
      : {}),
  };
}

type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;

function withPlanStepDurations(
  plan: TodoListPlan,
  previous: TodoListPlan | undefined,
  occurredAt: DateTime.Utc,
): TodoListPlan {
  const occurredAtIso = DateTime.formatIso(occurredAt);
  const occurredAtMs = DateTime.toEpochMillis(occurredAt);
  const previousById = new Map(previous?.steps.map((step) => [step.id, step]));
  // Provider step IDs may be positional. Changed text must not inherit another task's timing.
  const previousStep = (step: TodoListPlan["steps"][number]) => {
    const prior = previousById.get(step.id);
    return prior?.text === step.text ? prior : undefined;
  };
  const hasNewCompletion = plan.steps.some((step) => {
    const prior = previousStep(step);
    return step.status === "completed" && prior?.status !== "completed";
  });
  let fallbackCompletionConsumed = false;

  return {
    ...plan,
    steps: plan.steps.map((step) => {
      const prior = previousStep(step);
      const baseStep = { id: step.id, text: step.text, status: step.status };
      if (step.status === "completed") {
        if (prior?.status === "completed") {
          return {
            ...baseStep,
            ...(prior.durationMs === undefined ? {} : { durationMs: prior.durationMs }),
          };
        }
        const durationAnchorAt =
          prior?.status === "running" || !fallbackCompletionConsumed
            ? prior?.durationAnchorAt
            : occurredAtIso;
        fallbackCompletionConsumed = true;
        const anchorMs =
          durationAnchorAt === undefined ? occurredAtMs : Date.parse(durationAnchorAt);
        const durationMs = Number.isFinite(anchorMs) ? Math.max(0, occurredAtMs - anchorMs) : 0;
        return {
          ...baseStep,
          ...(durationMs > 0 ? { durationMs } : {}),
        };
      }
      if (step.status === "running") {
        return {
          ...baseStep,
          durationAnchorAt:
            prior?.status === "running" ? (prior.durationAnchorAt ?? occurredAtIso) : occurredAtIso,
        };
      }
      return {
        ...baseStep,
        durationAnchorAt:
          hasNewCompletion || prior?.status !== "pending"
            ? occurredAtIso
            : (prior.durationAnchorAt ?? occurredAtIso),
      };
    }),
  };
}

export interface ProviderEventIngestInput {
  readonly providerSessionId: ProviderSessionId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly commandId?: CommandId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly nodeId?: NodeId;
  readonly rawEventId?: RawEventId;
  readonly event: ProviderAdapterV2Event;
  readonly analyticsContext?: ProviderTurnAnalyticsContext;
}

export interface ProviderAssistantOutputOwner {
  readonly binding: ProviderRuntimeBinding & { readonly nativeThreadId: string };
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerTurnId: ProviderTurnId;
}

export interface ProviderAssistantOutputFlushInput {
  readonly binding: ProviderAssistantOutputOwner["binding"];
  /** Absence flushes output without attributing a turn or emitting a terminal. */
  readonly providerTurnId?: ProviderTurnId;
  readonly revalidateCurrentOwner: Effect.Effect<void, unknown>;
}

export interface ProviderEventIngestorV2Shape {
  readonly hasBufferedAssistantOutput: (input: {
    readonly binding: ProviderAssistantOutputOwner["binding"];
    readonly providerTurnId?: ProviderTurnId;
  }) => Effect.Effect<boolean>;
  readonly captureAssistantOutput: (
    input: ProviderEventIngestInput & {
      readonly owner: ProviderAssistantOutputOwner;
      readonly revalidateCurrentOwner: Effect.Effect<void, unknown>;
    },
  ) => Effect.Effect<void, ProviderEventIngestorV2Error>;
  readonly flushAssistantOutput: (
    input: ProviderAssistantOutputFlushInput,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, ProviderEventIngestorV2Error>;
  readonly normalize: (
    input: ProviderEventIngestInput,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, ProviderEventIngestorV2Error>;
  readonly ingestNormalized: (
    input: ProviderEventIngestInput & {
      /** Rechecks the emitting resident runtime inside the owner publication transaction. */
      readonly revalidateCurrentOwner?: Effect.Effect<void, unknown>;
      /**
       * Atomically reject mutable provider state emitted by an attempt that
       * lost ownership while the adapter event was in flight.
       */
      readonly writeIfRunCurrent?: {
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedStatus: OrchestrationV2Run["status"];
      };
      /**
       * Atomically reject provider-thread snapshots from an attempt that no
       * longer owns the run or from a run that no longer owns the thread.
       */
      readonly writeIfProviderThreadOwner?: {
        readonly providerThreadId: ProviderThreadId;
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedLastRunOrdinal: number;
      };
    },
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, ProviderEventIngestorV2Error>;
}

export class ProviderEventIngestorV2 extends Context.Service<
  ProviderEventIngestorV2,
  ProviderEventIngestorV2Shape
>()("t3/orchestration-v2/ProviderEventIngestor/ProviderEventIngestorV2") {}

function compactUndefined<T extends Record<string, unknown>>(record: T): T {
  return Object.fromEntries(Object.entries(record).filter(([, value]) => value !== undefined)) as T;
}

const decodeDomainEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEvent);

export const layer: Layer.Layer<
  ProviderEventIngestorV2,
  never,
  EventSink.EventSinkV2 | IdAllocator.IdAllocatorV2 | ProjectionStore.ProjectionStoreV2
> = Layer.effect(
  ProviderEventIngestorV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const analytics = yield* ProviderTurnAnalytics;
    const completedTurnAnalytics = new Set<string>();
    const outputPermit = yield* Semaphore.make(1);
    const bufferedOutput = new Map<
      string,
      {
        readonly owner: ProviderAssistantOutputOwner;
        readonly origin: ProviderEventOrigin;
        readonly events: Map<string, ProviderEventIngestInput>;
      }
    >();
    type DerivationInput = Parameters<ProviderEventIngestorV2Shape["ingestNormalized"]>[0];
    type DerivationBucket = {
      readonly origin: ProviderEventOrigin & {
        readonly derivation: ClaudeBufferedSubagentCompletionDerivationV1;
      };
      readonly events: Map<string, DerivationInput>;
    };
    const bufferedDerivations = new Map<object, Map<NodeId, DerivationBucket>>();
    const outputKey = (owner: ProviderAssistantOutputOwner) =>
      JSON.stringify([
        owner.binding.threadId,
        owner.binding.providerThreadId,
        owner.binding.providerSessionId,
        owner.binding.instanceId,
        owner.binding.runtimeGeneration,
        owner.binding.nativeThreadId,
        owner.runId,
        owner.attemptId,
        owner.providerTurnId,
      ]);
    const matchesOutputBinding = (
      expected: EventSink.ProviderBindingExpectationV2,
      actual: ProviderAssistantOutputOwner["binding"],
      driver: ProviderAdapterV2Event["driver"],
    ) =>
      expected.threadId === actual.threadId &&
      expected.providerThreadId === actual.providerThreadId &&
      expected.providerSessionId === actual.providerSessionId &&
      expected.instanceId === actual.instanceId &&
      expected.driver === driver &&
      expected.runtimeGeneration === actual.runtimeGeneration &&
      expected.nativeThreadId === actual.nativeThreadId;
    const sameOutputBinding = (
      left: ProviderAssistantOutputOwner["binding"],
      right: ProviderAssistantOutputOwner["binding"],
    ) =>
      left.threadId === right.threadId &&
      left.providerThreadId === right.providerThreadId &&
      left.providerSessionId === right.providerSessionId &&
      left.instanceId === right.instanceId &&
      left.runtimeGeneration === right.runtimeGeneration &&
      left.nativeThreadId === right.nativeThreadId;
    const sameRuntimeOwnerBinding = (
      left: EventSink.ProviderBindingExpectationV2,
      right: EventSink.ProviderBindingExpectationV2,
    ) =>
      left.threadId === right.threadId &&
      left.providerThreadId === right.providerThreadId &&
      left.providerSessionId === right.providerSessionId &&
      left.instanceId === right.instanceId &&
      left.driver === right.driver &&
      left.nativeThreadId === right.nativeThreadId &&
      left.runtimeGeneration === right.runtimeGeneration;
    const sameOutputOwner = (
      left: ProviderAssistantOutputOwner,
      right: ProviderAssistantOutputOwner,
    ) =>
      sameOutputBinding(left.binding, right.binding) &&
      left.runId === right.runId &&
      left.attemptId === right.attemptId &&
      left.providerTurnId === right.providerTurnId;
    const sameProducer = (left: ProviderEventProducerOrigin, right: ProviderEventProducerOrigin) =>
      left.token === right.token &&
      left.driver === right.driver &&
      left.instanceId === right.instanceId &&
      left.providerSessionId === right.providerSessionId &&
      left.runtimeGeneration === right.runtimeGeneration &&
      left.revalidateCurrent === right.revalidateCurrent;
    const pendingDerivations = () =>
      Array.from(bufferedDerivations.values()).flatMap((subjects) => Array.from(subjects.values()));
    const matchesPendingDerivation = (
      bucket: ReturnType<typeof pendingDerivations>[number],
      input: Pick<ProviderAssistantOutputFlushInput, "binding" | "providerTurnId">,
    ) =>
      sameOutputBinding(bucket.origin.derivation.executor.binding, input.binding) &&
      (input.providerTurnId === undefined ||
        bucket.origin.derivation.executor.providerTurnId === input.providerTurnId);
    const sameOutputOrigin = (left: ProviderEventOrigin, right: ProviderEventOrigin) =>
      left.producer.token === right.producer.token &&
      left.producer.driver === right.producer.driver &&
      left.producer.instanceId === right.producer.instanceId &&
      left.producer.providerSessionId === right.producer.providerSessionId &&
      left.producer.runtimeGeneration === right.producer.runtimeGeneration &&
      left.producer.revalidateCurrent === right.producer.revalidateCurrent &&
      left.turn !== undefined &&
      right.turn !== undefined &&
      sameOutputOwner(left.turn, right.turn);
    const validateEventOrigin = (input: ProviderEventIngestInput, origin: ProviderEventOrigin) =>
      Effect.gen(function* () {
        const producer = origin.producer;
        if (
          producer.driver !== input.event.driver ||
          producer.instanceId !== input.providerInstanceId ||
          producer.providerSessionId !== input.providerSessionId ||
          (origin.turn !== undefined &&
            (origin.turn.binding.instanceId !== producer.instanceId ||
              origin.turn.binding.providerSessionId !== producer.providerSessionId ||
              (producer.runtimeGeneration !== undefined &&
                producer.runtimeGeneration !== origin.turn.binding.runtimeGeneration)))
        )
          return yield* Effect.fail("Provider event origin does not match its captured source.");
        yield* producer.revalidateCurrent;
      });
    const assistantOutputKey = (event: ProviderAdapterV2Event): string | undefined => {
      if (
        event.type === "node.updated" &&
        (event.node.kind === "assistant_message" || event.node.kind === "reasoning")
      )
        return `node:${event.node.id}`;
      if (event.type === "message.updated" && event.message.role === "assistant")
        return `message:${event.message.id}`;
      if (
        event.type === "turn_item.updated" &&
        (event.turnItem.type === "assistant_message" || event.turnItem.type === "reasoning")
      )
        return `item:${event.turnItem.id}`;
      return undefined;
    };
    const validateOutputOwner = (owner: ProviderAssistantOutputOwner) =>
      Effect.gen(function* () {
        const records = yield* projections.getThreadRecords(owner.binding.threadId, [
          "runs",
          "attempts",
          "providerThreads",
          "providerTurns",
        ]);
        const run = records.runs.find((candidate) => candidate.id === owner.runId);
        const attempt = records.attempts.find((candidate) => candidate.id === owner.attemptId);
        const providerThread = records.providerThreads.find(
          (candidate) => candidate.id === owner.binding.providerThreadId,
        );
        const providerTurn = records.providerTurns.find(
          (candidate) => candidate.id === owner.providerTurnId,
        );
        if (
          records.thread.deletedAt !== null ||
          records.thread.activeProviderThreadId !== owner.binding.providerThreadId ||
          run?.activeAttemptId !== owner.attemptId ||
          attempt?.runId !== owner.runId ||
          attempt.providerThreadId !== owner.binding.providerThreadId ||
          providerThread?.lastRunOrdinal !== run.ordinal ||
          providerTurn?.runAttemptId !== owner.attemptId ||
          providerTurn.providerThreadId !== owner.binding.providerThreadId
        )
          return yield* Effect.fail(
            "Buffered assistant output no longer has its current run, attempt and turn owner.",
          );
      });

    const makeDomainEvent = (
      input: ProviderEventIngestInput,
      payloadInput: {
        readonly type: OrchestrationV2DomainEvent["type"];
        readonly payload: OrchestrationV2DomainEvent["payload"];
        readonly threadId?: ThreadId;
        readonly runId?: RunId | null;
        readonly nodeId?: NodeId | null;
        readonly occurredAt?: DateTime.Utc;
      },
    ) =>
      Effect.gen(function* () {
        const threadId = payloadInput.threadId ?? input.threadId;
        const eventId = yield* idAllocator.allocate.event({
          threadId,
          providerSessionId: input.providerSessionId,
        });
        const occurredAt = payloadInput.occurredAt ?? (yield* DateTime.now);
        return yield* decodeDomainEvent(
          compactUndefined({
            id: eventId,
            type: payloadInput.type,
            threadId,
            runId: payloadInput.runId ?? input.runId,
            nodeId: payloadInput.nodeId ?? input.nodeId,
            driver: input.event.driver,
            providerInstanceId: input.providerInstanceId,
            rawEventId: input.rawEventId,
            occurredAt,
            payload: payloadInput.payload,
          }),
        );
      });

    const dismissNativeUserInputs = Effect.fn("ProviderEventIngestor.dismissNativeUserInputs")(
      function* (
        input: ProviderEventIngestInput,
        providerTurnId: ProviderTurnId,
        threadId = input.threadId,
      ) {
        const pending = yield* projections.getPendingNativeUserInputs(threadId, providerTurnId);
        const now = yield* DateTime.now;
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const request of pending.runtimeRequests) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "runtime-request.updated",
              threadId,
              nodeId: request.nodeId,
              payload: { ...request, status: "cancelled", resolvedAt: now },
            }),
          );
        }
        for (const node of pending.nodes) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "node.updated",
              threadId,
              nodeId: node.id,
              runId: node.runId,
              payload: { ...node, status: "cancelled", completedAt: now },
            }),
          );
        }
        for (const item of pending.turnItems) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "turn-item.updated",
              threadId,
              nodeId: item.nodeId,
              runId: item.runId,
              payload: { ...item, status: "cancelled", completedAt: now, updatedAt: now },
            }),
          );
        }
        return events;
      },
    );

    const publishPrimaryProviderTurn = (
      input: Parameters<ProviderEventIngestorV2Shape["ingestNormalized"]>[0],
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      origin: ProviderEventOrigin | undefined,
    ) =>
      eventSink.withTransaction(
        Effect.gen(function* () {
          const guard = input.writeIfRunCurrent;
          if (
            input.event.type !== "provider_turn.updated" ||
            guard === undefined ||
            origin === undefined ||
            input.revalidateCurrentOwner === undefined ||
            input.runId !== guard.runId ||
            input.nodeId === undefined
          )
            return [];
          const turn = input.event.providerTurn;
          if (
            (input.event.threadId !== undefined && input.event.threadId !== input.threadId) ||
            turn.runAttemptId !== guard.activeAttemptId ||
            turn.nodeId !== input.nodeId
          )
            return [];
          yield* validateEventOrigin(input, origin);
          yield* input.revalidateCurrentOwner;
          const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(input.threadId);
          if (
            evidence === null ||
            evidence.binding.providerThreadId !== turn.providerThreadId ||
            evidence.binding.providerSessionId !== input.providerSessionId ||
            evidence.binding.instanceId !== input.providerInstanceId ||
            evidence.binding.driver !== input.event.driver ||
            evidence.binding.runtimeGeneration !== origin.producer.runtimeGeneration ||
            (origin.turn !== undefined &&
              (origin.turn.runId !== guard.runId ||
                origin.turn.attemptId !== guard.activeAttemptId ||
                origin.turn.providerTurnId !== turn.id ||
                !matchesOutputBinding(evidence.binding, origin.turn.binding, input.event.driver)))
          )
            return [];
          const records = yield* projections.getThreadRecords(input.threadId, [
            "runs",
            "attempts",
            "nodes",
            "providerThreads",
            "providerTurns",
          ]);
          const run = records.runs.find((item) => item.id === guard.runId);
          const attempt = records.attempts.find((item) => item.id === guard.activeAttemptId);
          const root = records.nodes.find((item) => item.id === input.nodeId);
          const provider = records.providerThreads.find(
            (item) => item.id === turn.providerThreadId,
          );
          const previous = records.providerTurns.find((item) => item.id === turn.id);
          if (
            records.thread.deletedAt !== null ||
            records.thread.activeProviderThreadId !== turn.providerThreadId ||
            run === undefined ||
            run.status !== guard.expectedStatus ||
            run.activeAttemptId !== guard.activeAttemptId ||
            run.rootNodeId !== input.nodeId ||
            run.providerThreadId !== turn.providerThreadId ||
            run.providerInstanceId !== input.providerInstanceId ||
            attempt === undefined ||
            attempt.runId !== run.id ||
            attempt.rootNodeId !== input.nodeId ||
            attempt.providerThreadId !== turn.providerThreadId ||
            attempt.providerInstanceId !== input.providerInstanceId ||
            attempt.status !== "running" ||
            (attempt.nativeThreadId !== undefined &&
              attempt.nativeThreadId !== evidence.binding.nativeThreadId) ||
            (attempt.providerTurnId !== null && attempt.providerTurnId !== turn.id) ||
            root === undefined ||
            root.threadId !== input.threadId ||
            root.runId !== run.id ||
            root.kind !== "root_turn" ||
            root.rootNodeId !== root.id ||
            root.parentNodeId !== null ||
            root.providerThreadId !== turn.providerThreadId ||
            (root.providerTurnId !== null && root.providerTurnId !== turn.id) ||
            provider === undefined ||
            provider.appThreadId !== input.threadId ||
            provider.providerSessionId !== input.providerSessionId ||
            provider.providerInstanceId !== input.providerInstanceId ||
            provider.driver !== input.event.driver ||
            provider.lastRunOrdinal !== run.ordinal ||
            (previous !== undefined &&
              (previous.runAttemptId !== turn.runAttemptId ||
                previous.nodeId !== turn.nodeId ||
                previous.providerThreadId !== turn.providerThreadId))
          )
            return [];
          // Bind only the current primary event, without changing execution status or times.
          const boundEvents = [
            ...events,
            yield* makeDomainEvent(input, {
              type: "run-attempt.updated",
              payload: { ...attempt, providerTurnId: turn.id },
            }),
            yield* makeDomainEvent(input, {
              type: "node.updated",
              payload: { ...root, providerTurnId: turn.id },
            }),
          ];
          yield* validateEventOrigin(input, origin);
          yield* input.revalidateCurrentOwner;
          const current = yield* eventSink.readCurrentProviderRuntimeOwner(input.threadId);
          if (
            current === null ||
            current.evidenceRevision !== evidence.evidenceRevision ||
            !sameRuntimeOwnerBinding(current.binding, evidence.binding)
          )
            return [];
          const result = yield* eventSink.writeIfRunCurrent({
            guardPendingUserInputCancellations: true,
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            threadId: input.threadId,
            ...guard,
            events: boundEvents,
          });
          return result.storedEvents;
        }),
      );

    const normalize: ProviderEventIngestorV2Shape["normalize"] = (input) =>
      Effect.gen(function* () {
        switch (input.event.type) {
          case "runtime_identity.observed": {
            const { binding, attestation } = input.event;
            if (
              binding.threadId !== input.threadId ||
              binding.providerSessionId !== input.providerSessionId ||
              binding.instanceId !== input.providerInstanceId ||
              attestation.runtimeGeneration !== binding.runtimeGeneration ||
              attestation.requested.providerInstanceId !== binding.instanceId ||
              attestation.requested.providerDriver !== input.event.driver
            )
              return [];
            const context = yield* projections.getThreadProviderContext(
              input.threadId,
              binding.instanceId,
            );
            const session = context.providerSessions.find(
              (candidate) => candidate.id === binding.providerSessionId,
            );
            const thread = context.providerThreads.find(
              (candidate) => candidate.id === binding.providerThreadId,
            );
            if (
              session === undefined ||
              session.status === "stopped" ||
              session.status === "error" ||
              session.providerInstanceId !== binding.instanceId ||
              session.driver !== input.event.driver ||
              context.thread.activeProviderThreadId !== binding.providerThreadId ||
              thread?.appThreadId !== binding.threadId ||
              thread.providerSessionId !== binding.providerSessionId ||
              thread.providerInstanceId !== binding.instanceId ||
              thread.nativeThreadRef?.nativeId !== binding.nativeThreadId
            )
              return [];
            return [
              yield* makeDomainEvent(input, {
                type: "provider-session.updated",
                payload: {
                  ...session,
                  runtimeIdentity: attestation,
                  updatedAt: yield* DateTime.now,
                },
              }),
            ];
          }
          case "app_thread.created":
            return [
              yield* makeDomainEvent(input, {
                type: "thread.created",
                threadId: input.event.appThread.id,
                payload: input.event.appThread,
              }),
            ];
          case "provider_session.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "provider-session.updated",
                payload: input.event.providerSession,
              }),
            ];
          case "provider_thread.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "provider-thread.updated",
                threadId: input.event.providerThread.appThreadId ?? input.threadId,
                payload: input.event.providerThread,
              }),
            ];
          case "provider_turn.updated":
            return [
              ...(["completed", "interrupted", "failed", "cancelled"].includes(
                input.event.providerTurn.status,
              )
                ? yield* dismissNativeUserInputs(
                    input,
                    input.event.providerTurn.id,
                    input.event.threadId,
                  )
                : []),
              yield* makeDomainEvent(input, {
                type: "provider-turn.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.providerTurn,
                nodeId: input.event.providerTurn.nodeId,
              }),
            ];
          case "node.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "node.updated",
                threadId: input.event.node.threadId,
                payload: input.event.node,
                runId: input.event.node.runId,
                nodeId: input.event.node.id,
              }),
            ];
          case "subagent.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "subagent.updated",
                threadId: input.event.subagent.threadId,
                payload: input.event.subagent,
                runId: input.event.subagent.runId,
                nodeId: input.event.subagent.id,
              }),
            ];
          case "message.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "message.updated",
                threadId: input.event.message.threadId,
                payload: input.event.message,
                runId: input.event.message.runId,
                nodeId: input.event.message.nodeId,
              }),
            ];
          case "turn_item.updated": {
            const item = input.event.turnItem;
            const origin = readProviderEventOrigin(input.event);
            const previous =
              origin?.turn === undefined && origin?.derivation === undefined
                ? undefined
                : (yield* projections.getThreadRecords(item.threadId, ["turnItems"], {
                    turnItemRunIds: [item.runId],
                  })).turnItems.find((candidate) => candidate.id === item.id);
            // SDK ordinals are turn-local; existing items keep their durable
            // position before ownership guards compare immutable fields.
            return [
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                threadId: item.threadId,
                payload: previous === undefined ? item : { ...item, ordinal: previous.ordinal },
                runId: item.runId,
                nodeId: item.nodeId,
              }),
            ];
          }
          case "runtime_request.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "runtime-request.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.runtimeRequest,
                nodeId: input.event.runtimeRequest.nodeId,
              }),
            ];
          case "plan.updated": {
            const occurredAt = yield* DateTime.now;
            const plan = input.event.plan;
            const previous =
              plan.kind === "todo_list"
                ? yield* projections.getPlan(plan.threadId, plan.id)
                : undefined;
            const payload =
              plan.kind === "todo_list"
                ? withPlanStepDurations(
                    plan,
                    previous?.kind === "todo_list" ? previous : undefined,
                    occurredAt,
                  )
                : plan;
            return [
              yield* makeDomainEvent(input, {
                type: "plan.updated",
                threadId: plan.threadId,
                payload,
                runId: plan.runId,
                nodeId: plan.nodeId,
                occurredAt,
              }),
            ];
          }
          case "turn.terminal":
            const dismissed = yield* dismissNativeUserInputs(input, input.event.providerTurnId);
            if (input.event.status !== "failed") {
              return dismissed;
            }
            const occurredAt = yield* DateTime.now;
            return [
              ...dismissed,
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                payload: makeProviderFailureTurnItem({
                  idAllocator,
                  driver: input.event.driver,
                  threadId: input.threadId,
                  runId: input.runId ?? null,
                  nodeId: input.nodeId ?? null,
                  providerThreadId: input.event.providerThreadId,
                  providerTurnId: input.event.providerTurnId,
                  itemOrdinal: input.event.failureItemOrdinal,
                  failure: input.event.failure,
                  ...(input.event.retry === undefined ? {} : { retry: input.event.retry }),
                  ...(input.event.retryStartedAt === undefined
                    ? {}
                    : { retryStartedAt: input.event.retryStartedAt }),
                  occurredAt,
                }),
              }),
            ];
        }
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderEventNormalizeError({
              providerSessionId: input.providerSessionId,
              threadId: input.threadId,
              providerEvent: input.event,
              cause,
            }),
        ),
      );

    const captureAssistantOutput: ProviderEventIngestorV2Shape["captureAssistantOutput"] = (
      input,
    ) =>
      Effect.gen(function* () {
        const key = assistantOutputKey(input.event);
        if (key === undefined) return;
        const origin = readProviderEventOrigin(input.event);
        if (origin?.turn === undefined) return;
        const { owner } = input;
        if (!sameOutputOwner(origin.turn, owner))
          return yield* Effect.fail(
            "Assistant output owner does not match its captured native turn.",
          );
        const output =
          input.event.type === "message.updated"
            ? input.event.message
            : input.event.type === "turn_item.updated"
              ? input.event.turnItem
              : input.event.type === "node.updated"
                ? input.event.node
                : undefined;
        if (
          output === undefined ||
          output.threadId !== owner.binding.threadId ||
          output.runId !== owner.runId ||
          input.threadId !== owner.binding.threadId ||
          input.runId !== owner.runId ||
          input.providerSessionId !== owner.binding.providerSessionId ||
          input.providerInstanceId !== owner.binding.instanceId ||
          (input.event.type === "turn_item.updated" &&
            (input.event.turnItem.providerTurnId !== owner.providerTurnId ||
              input.event.turnItem.providerThreadId !== owner.binding.providerThreadId)) ||
          (input.event.type === "node.updated" &&
            (input.event.node.providerTurnId !== owner.providerTurnId ||
              input.event.node.providerThreadId !== owner.binding.providerThreadId))
        )
          return;
        yield* validateEventOrigin(input, origin);
        yield* input.revalidateCurrentOwner;
        const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(input.threadId);
        if (
          evidence === null ||
          !matchesOutputBinding(evidence.binding, owner.binding, input.event.driver)
        )
          return yield* Effect.fail(
            "Buffered assistant output has no matching resident runtime binding.",
          );
        yield* validateOutputOwner(owner);
        const bucketKey = outputKey(owner);
        const bucket = bufferedOutput.get(bucketKey) ?? {
          owner: origin.turn,
          origin,
          events: new Map(),
        };
        if (!sameOutputOrigin(bucket.origin, origin))
          return yield* Effect.fail(
            "Assistant output cannot be reassociated with a different producer.",
          );
        bucket.events.set(key, input);
        bufferedOutput.set(bucketKey, bucket);
      }).pipe(
        outputPermit.withPermits(1),
        Effect.mapError(
          (cause) =>
            new ProviderEventNormalizeError({
              providerSessionId: input.providerSessionId,
              threadId: input.threadId,
              providerEvent: input.event,
              cause,
            }),
        ),
      );

    const flushAssistantOutput: ProviderEventIngestorV2Shape["flushAssistantOutput"] = (input) =>
      Effect.gen(function* () {
        if (pendingDerivations().some((bucket) => matchesPendingDerivation(bucket, input)))
          return yield* Effect.fail(
            new ProviderEventPublishError({
              providerSessionId: input.binding.providerSessionId,
              eventCount: 0,
              cause:
                "A captured subagent completion family remains held before terminal publication.",
            }),
          );
        const stored: Array<OrchestrationV2StoredEvent> = [];
        for (const [key, bucket] of bufferedOutput) {
          if (
            !sameOutputBinding(bucket.owner.binding, input.binding) ||
            (input.providerTurnId !== undefined &&
              bucket.owner.providerTurnId !== input.providerTurnId)
          )
            continue;
          const originals = Array.from(bucket.events.values()).filter(
            (original) => original.event.type !== "node.updated",
          );
          if (originals.length === 0) continue;
          const referencedNodes = new Set(
            originals.flatMap((original) => {
              const event = original.event;
              const nodeId =
                event.type === "message.updated"
                  ? event.message.nodeId
                  : event.type === "turn_item.updated"
                    ? event.turnItem.nodeId
                    : null;
              return nodeId === null ? [] : [nodeId];
            }),
          );
          const companions = Array.from(bucket.events.values()).filter(
            (original) =>
              original.event.type === "node.updated" && referencedNodes.has(original.event.node.id),
          );
          const mapWriteError = (cause: unknown) =>
            new ProviderEventPublishError({
              providerSessionId: input.binding.providerSessionId,
              eventCount: originals.length,
              cause,
            });
          const committed = yield* eventSink
            .withTransaction(
              Effect.gen(function* () {
                yield* bucket.origin.producer.revalidateCurrent;
                const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(
                  input.binding.threadId,
                );
                if (
                  evidence === null ||
                  !matchesOutputBinding(evidence.binding, input.binding, originals[0]!.event.driver)
                )
                  return yield* Effect.fail(
                    "Buffered assistant output cannot be flushed by a stale runtime binding.",
                  );
                const events: Array<OrchestrationV2DomainEvent> = [];
                for (const original of originals) {
                  // Sealing delivery does not complete a provider turn or change its attribution.
                  const event = original.event;
                  const sealed =
                    event.type === "message.updated"
                      ? { ...event, message: { ...event.message, streaming: false } }
                      : event.type === "turn_item.updated" &&
                          (event.turnItem.type === "assistant_message" ||
                            event.turnItem.type === "reasoning")
                        ? { ...event, turnItem: { ...event.turnItem, streaming: false } }
                        : event;
                  events.push(
                    ...(yield* normalize({
                      ...original,
                      event: copyProviderEventOrigin(event, sealed),
                    })),
                  );
                }
                const companionNodes: Array<
                  Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>
                > = [];
                for (const original of companions) {
                  for (const event of yield* normalize(original)) {
                    if (event.type === "node.updated") companionNodes.push(event);
                  }
                }
                const result = yield* eventSink.writeIfCurrentProviderRuntimeOutputOwner({
                  expectedBinding: evidence.binding,
                  expectedEvidenceRevision: evidence.evidenceRevision,
                  expectedRunId: bucket.owner.runId,
                  expectedRunAttemptId: bucket.owner.attemptId,
                  expectedProviderTurnId: bucket.owner.providerTurnId,
                  revalidateCurrentOwner: input.revalidateCurrentOwner.pipe(
                    Effect.andThen(bucket.origin.producer.revalidateCurrent),
                    Effect.andThen(validateOutputOwner(bucket.owner)),
                  ),
                  events,
                  companionNodes,
                });
                if (!result.committed) return yield* Effect.fail(result.rejection);
                return result.storedEvents;
              }),
            )
            .pipe(Effect.mapError(mapWriteError));
          stored.push(...committed);
          for (const original of [...originals, ...companions]) {
            bucket.events.delete(assistantOutputKey(original.event)!);
          }
          if (bucket.events.size === 0) bufferedOutput.delete(key);
        }
        return stored;
      }).pipe(outputPermit.withPermits(1));

    const publishStreamedAssistantOutput = (
      input: ProviderEventIngestInput,
      origin: ProviderEventOrigin & { readonly turn: ProviderAssistantOutputOwner },
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      revalidateCurrentOwner: Effect.Effect<void, unknown> = Effect.void,
    ) =>
      Effect.gen(function* () {
        const owner = origin.turn;
        const key = outputKey(owner);
        const bucket = bufferedOutput.get(key);
        if (bucket !== undefined && !sameOutputOrigin(bucket.origin, origin))
          return yield* Effect.fail("Streamed assistant output has a different buffered producer.");
        const companions = Array.from(bucket?.events.values() ?? []).filter((candidate) => {
          const event = candidate.event;
          return (
            event.type === "node.updated" &&
            events.some((output) => output.nodeId === event.node.id)
          );
        });
        const companionNodes: Array<
          Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>
        > = [];
        for (const companion of companions) {
          for (const event of yield* normalize(companion)) {
            if (event.type === "node.updated") companionNodes.push(event);
          }
        }
        const stored = yield* eventSink.withTransaction(
          Effect.gen(function* () {
            yield* validateEventOrigin(input, origin);
            const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(
              owner.binding.threadId,
            );
            if (
              evidence === null ||
              !matchesOutputBinding(evidence.binding, owner.binding, origin.producer.driver)
            )
              return yield* Effect.fail(
                "Streamed assistant output cannot be published by a stale runtime binding.",
              );
            const result = yield* eventSink.writeIfCurrentProviderRuntimeOutputOwner({
              expectedBinding: evidence.binding,
              expectedEvidenceRevision: evidence.evidenceRevision,
              expectedRunId: owner.runId,
              expectedRunAttemptId: owner.attemptId,
              expectedProviderTurnId: owner.providerTurnId,
              events,
              companionNodes,
              revalidateCurrentOwner: revalidateCurrentOwner.pipe(
                Effect.andThen(origin.producer.revalidateCurrent),
                Effect.andThen(validateOutputOwner(owner)),
              ),
            });
            if (!result.committed) return yield* Effect.fail(result.rejection);
            return result.storedEvents;
          }),
        );
        for (const companion of companions)
          bucket?.events.delete(assistantOutputKey(companion.event)!);
        if (bucket?.events.size === 0) bufferedOutput.delete(key);
        return stored;
      }).pipe(outputPermit.withPermits(1));

    const publishAssistantOutputNode = (
      input: ProviderEventIngestInput,
      origin: ProviderEventOrigin & { readonly turn: ProviderAssistantOutputOwner },
      events: ReadonlyArray<Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>>,
      revalidateCurrentOwner: Effect.Effect<void, unknown> = Effect.void,
    ) =>
      Effect.gen(function* () {
        const owner = origin.turn;
        yield* captureAssistantOutput({ ...input, owner, revalidateCurrentOwner });
        return yield* eventSink
          .withTransaction(
            Effect.gen(function* () {
              yield* validateEventOrigin(input, origin);
              yield* validateOutputOwner(owner);
              const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(
                owner.binding.threadId,
              );
              if (
                evidence === null ||
                !matchesOutputBinding(evidence.binding, owner.binding, origin.producer.driver)
              )
                return yield* Effect.fail(
                  "Assistant node output cannot be published by a stale runtime binding.",
                );
              const records = yield* projections.getThreadRecords(
                owner.binding.threadId,
                ["nodes", "messages", "turnItems"],
                {
                  messageRunIds: [owner.runId],
                  turnItemRunIds: [owner.runId],
                },
              );
              const hasPublishedOutput = events.every(
                (event) =>
                  records.nodes.some((node) => node.id === event.payload.id) &&
                  (records.messages.some(
                    (message) =>
                      event.payload.kind === "assistant_message" &&
                      message.nodeId === event.payload.id &&
                      message.role === "assistant" &&
                      message.runId === owner.runId,
                  ) ||
                    records.turnItems.some(
                      (item) =>
                        item.nodeId === event.payload.id &&
                        item.type === event.payload.kind &&
                        item.runId === owner.runId &&
                        item.providerThreadId === owner.binding.providerThreadId &&
                        item.providerTurnId === owner.providerTurnId,
                    )),
              );
              // First nodes stay raw until genuine output publishes them as companions.
              if (!hasPublishedOutput) return [];
              const result = yield* eventSink.writeIfCurrentProviderRuntimeOutputNodeOwner({
                expectedBinding: evidence.binding,
                expectedEvidenceRevision: evidence.evidenceRevision,
                expectedRunId: owner.runId,
                expectedRunAttemptId: owner.attemptId,
                expectedProviderTurnId: owner.providerTurnId,
                events,
                revalidateCurrentOwner: revalidateCurrentOwner.pipe(
                  Effect.andThen(origin.producer.revalidateCurrent),
                  Effect.andThen(validateOutputOwner(owner)),
                ),
              });
              if (!result.committed) return yield* Effect.fail(result.rejection);
              return result.storedEvents;
            }),
          )
          .pipe(outputPermit.withPermits(1));
      });

    const validateDerivation = (
      input: ProviderEventIngestInput,
      origin: ProviderEventOrigin & {
        readonly derivation: ClaudeBufferedSubagentCompletionDerivationV1;
      },
    ) =>
      Effect.gen(function* () {
        const d = origin.derivation;
        const result = readProviderEventOrigin(d.result.token);
        const notification = readProviderEventOrigin(d.notification.token);
        if (
          d.kind !== "claude_buffered_subagent_completion" ||
          origin.turn !== undefined ||
          origin.producer.driver !== "claudeAgent" ||
          !sameProducer(origin.producer, d.result.producer) ||
          result === undefined ||
          result.turn !== undefined ||
          result.derivation !== undefined ||
          notification === undefined ||
          notification.turn !== undefined ||
          notification.derivation !== undefined ||
          !sameProducer(result.producer, d.result.producer) ||
          !sameProducer(notification.producer, d.notification.producer) ||
          d.result.nativeThreadId !== d.notification.nativeThreadId ||
          d.result.nativeThreadId !== d.executor.binding.nativeThreadId ||
          d.notification.producer.driver !== origin.producer.driver ||
          d.notification.producer.instanceId !== origin.producer.instanceId ||
          d.notification.producer.providerSessionId !== origin.producer.providerSessionId ||
          d.executor.binding.instanceId !== origin.producer.instanceId ||
          d.executor.binding.providerSessionId !== origin.producer.providerSessionId ||
          d.executor.binding.runtimeGeneration !== origin.producer.runtimeGeneration ||
          d.subject.parentThreadId !== d.executor.binding.threadId ||
          input.threadId !== d.subject.parentThreadId ||
          input.runId !== d.executor.runId ||
          d.subject.nativeTaskRef.driver !== origin.producer.driver ||
          d.subject.nativeTaskRef.nativeId !== d.notification.nativeTaskId
        )
          return yield* Effect.fail(
            "A subagent completion derivation lacks its captured executor, notification or subject association.",
          );
        yield* origin.producer.revalidateCurrent;
        yield* d.revalidateDerivation;
        yield* validateOutputOwner(d.executor);
      });

    const derivationEventKey = (
      event: ProviderAdapterV2Event,
      d: ClaudeBufferedSubagentCompletionDerivationV1,
    ) => {
      if (event.type === "subagent.updated" && event.subagent.id === d.subject.subagentId)
        return "subagent";
      if (event.type === "node.updated") {
        if (event.node.kind === "subagent" && event.node.id === d.subject.subagentId)
          return "task_node";
        if (event.node.kind === "root_turn" && event.node.id === d.subject.childRootNodeId)
          return "child_root";
      }
      if (
        event.type === "turn_item.updated" &&
        event.turnItem.type === "subagent" &&
        event.turnItem.subagentId === d.subject.subagentId
      )
        return "card";
      if (d.childResult !== undefined) {
        if (
          event.type === "message.updated" &&
          event.message.role === "assistant" &&
          event.message.id === d.childResult.messageId
        )
          return "child_message";
        if (
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.id === d.childResult.turnItemId
        )
          return "child_item";
      }
      return undefined;
    };

    const publishSubagentDerivation = (
      input: DerivationInput,
      origin: ProviderEventOrigin & {
        readonly derivation: ClaudeBufferedSubagentCompletionDerivationV1;
      },
    ) =>
      Effect.gen(function* () {
        const d = origin.derivation;
        yield* validateDerivation(input, origin);
        const key = derivationEventKey(input.event, d);
        if (key === undefined)
          return yield* Effect.fail(
            "The event is outside its captured subagent completion family.",
          );
        const subjects =
          bufferedDerivations.get(d.notification.token) ?? new Map<NodeId, DerivationBucket>();
        const bucket = subjects.get(d.subject.subagentId) ?? {
          origin,
          events: new Map<string, DerivationInput>(),
        };
        const first = bucket.events.values().next().value;
        if (first !== undefined)
          yield* Effect.try(() => copyProviderEventOrigin(first.event, input.event));
        const prior = bucket.events.get(key);
        if (prior !== undefined && prior.event !== input.event)
          return yield* Effect.fail(
            "A subagent completion family contains conflicting duplicate artifacts.",
          );
        bucket.events.set(key, input);
        subjects.set(d.subject.subagentId, bucket);
        bufferedDerivations.set(d.notification.token, subjects);
        const keys = [
          "task_node",
          "child_root",
          "subagent",
          "card",
          ...(d.childResult === undefined ? [] : ["child_message", "child_item"]),
        ];
        if (!keys.every((required) => bucket.events.has(required))) return [];
        const originals = keys.map((required) => bucket.events.get(required)!);
        const stored = yield* eventSink.withTransaction(
          Effect.gen(function* () {
            yield* validateDerivation(input, origin);
            const evidence = yield* eventSink.readCurrentProviderRuntimeOwner(
              d.executor.binding.threadId,
            );
            if (
              evidence === null ||
              !matchesOutputBinding(evidence.binding, d.executor.binding, origin.producer.driver)
            )
              return yield* Effect.fail(
                "A subagent completion cannot be published by a replaced executor.",
              );
            const events: Array<OrchestrationV2DomainEvent> = [];
            for (const original of originals) {
              const event = original.event;
              const runId =
                event.type === "subagent.updated"
                  ? event.subagent.runId
                  : event.type === "node.updated"
                    ? event.node.runId
                    : event.type === "message.updated"
                      ? event.message.runId
                      : event.type === "turn_item.updated"
                        ? event.turnItem.runId
                        : null;
              // Historical null attribution must not inherit the current executor's run.
              const { runId: _inheritedRunId, ...unattributed } = original;
              events.push(
                ...(yield* normalize(runId === null ? unattributed : { ...unattributed, runId })),
              );
            }
            const result = yield* eventSink.writeIfCurrentProviderRuntimeSubagentDerivation({
              expectedBinding: evidence.binding,
              expectedEvidenceRevision: evidence.evidenceRevision,
              expectedRunId: d.executor.runId,
              expectedRunAttemptId: d.executor.attemptId,
              expectedExecutorProviderTurnId: d.executor.providerTurnId,
              expectedSubject: d.subject,
              ...(d.childResult === undefined ? {} : { expectedChildResult: d.childResult }),
              expectedSummary: d.notification.summary,
              expectedStatus: d.notification.status,
              revalidateCurrentOwner: Effect.forEach(
                originals,
                (original) => original.revalidateCurrentOwner ?? Effect.void,
                { discard: true },
              ).pipe(
                Effect.andThen(origin.producer.revalidateCurrent),
                Effect.andThen(validateOutputOwner(d.executor)),
              ),
              revalidateDerivation: d.revalidateDerivation,
              events,
            });
            if (!result.committed) return yield* Effect.fail(result.rejection);
            return result.storedEvents;
          }),
        );
        subjects.delete(d.subject.subagentId);
        if (subjects.size === 0) bufferedDerivations.delete(d.notification.token);
        return stored;
      }).pipe(outputPermit.withPermits(1));

    return ProviderEventIngestorV2.of({
      hasBufferedAssistantOutput: (input) =>
        Effect.sync(
          () =>
            Array.from(bufferedOutput.values()).some(
              (bucket) =>
                sameOutputBinding(bucket.owner.binding, input.binding) &&
                (input.providerTurnId === undefined ||
                  bucket.owner.providerTurnId === input.providerTurnId) &&
                Array.from(bucket.events.values()).some(
                  (original) => original.event.type !== "node.updated",
                ),
            ) || pendingDerivations().some((bucket) => matchesPendingDerivation(bucket, input)),
        ).pipe(outputPermit.withPermits(1)),
      captureAssistantOutput,
      flushAssistantOutput,
      normalize,
      ingestNormalized: (input) =>
        Effect.gen(function* () {
          const origin = readProviderEventOrigin(input.event);
          if (origin !== undefined)
            yield* validateEventOrigin(input, origin).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderEventPublishError({
                    providerSessionId: input.providerSessionId,
                    eventCount: 1,
                    cause,
                  }),
              ),
            );
          if (origin?.derivation !== undefined) {
            return yield* publishSubagentDerivation(input, {
              ...origin,
              derivation: origin.derivation,
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderEventPublishError({
                    providerSessionId: input.providerSessionId,
                    eventCount: 1,
                    cause,
                  }),
              ),
            );
          }
          const events = yield* normalize(input);
          if (events.length === 0) {
            return [];
          }
          const mapWriteError = (cause: unknown) =>
            new ProviderEventPublishError({
              providerSessionId: input.providerSessionId,
              eventCount: events.length,
              cause,
            });
          if (
            input.event.type === "provider_turn.updated" &&
            input.writeIfRunCurrent !== undefined &&
            origin !== undefined &&
            input.revalidateCurrentOwner !== undefined &&
            input.event.providerTurn.runAttemptId === input.writeIfRunCurrent.activeAttemptId
          ) {
            return yield* publishPrimaryProviderTurn(input, events, origin).pipe(
              Effect.mapError(mapWriteError),
            );
          }
          const assistantSnapshot =
            (input.event.type === "message.updated" && input.event.message.role === "assistant") ||
            (input.event.type === "turn_item.updated" &&
              (input.event.turnItem.type === "assistant_message" ||
                input.event.turnItem.type === "reasoning"));
          if (assistantSnapshot && origin?.turn !== undefined) {
            return yield* publishStreamedAssistantOutput(
              input,
              { ...origin, turn: origin.turn },
              events,
              input.revalidateCurrentOwner,
            ).pipe(Effect.mapError(mapWriteError));
          }
          if (
            input.event.type === "node.updated" &&
            origin?.turn !== undefined &&
            (input.event.node.kind === "assistant_message" || input.event.node.kind === "reasoning")
          ) {
            return yield* publishAssistantOutputNode(
              input,
              { ...origin, turn: origin.turn },
              events.filter(
                (
                  event,
                ): event is Extract<
                  OrchestrationV2DomainEvent,
                  { readonly type: "node.updated" }
                > => event.type === "node.updated",
              ),
              input.revalidateCurrentOwner,
            ).pipe(Effect.mapError(mapWriteError));
          }
          if (input.event.type === "runtime_identity.observed") {
            if (input.revalidateCurrentOwner === undefined) return [];
            const binding = input.event.binding;
            const evidence = yield* eventSink
              .readProviderRuntimeEvidence(input.threadId)
              .pipe(Effect.mapError(mapWriteError));
            if (
              evidence === null ||
              evidence.binding.providerThreadId !== binding.providerThreadId ||
              evidence.binding.providerSessionId !== binding.providerSessionId ||
              evidence.binding.instanceId !== binding.instanceId ||
              evidence.binding.runtimeGeneration !== binding.runtimeGeneration ||
              evidence.binding.nativeThreadId !== (binding.nativeThreadId ?? null)
            )
              return [];
            const attempt = input.writeIfProviderThreadOwner ?? input.writeIfRunCurrent;
            const result = yield* eventSink
              .writeIfCurrentProviderRuntimeOwner({
                expectedBinding: evidence.binding,
                expectedEvidenceRevision: evidence.evidenceRevision,
                ...(attempt === undefined
                  ? {}
                  : {
                      expectedRunId: attempt.runId,
                      expectedRunAttemptId: attempt.activeAttemptId,
                    }),
                events,
                revalidateCurrentOwner: input.revalidateCurrentOwner,
              })
              .pipe(Effect.mapError(mapWriteError));
            return result.storedEvents;
          }
          if (input.writeIfProviderThreadOwner !== undefined) {
            const ownerResult = yield* eventSink
              .writeIfProviderThreadOwner({
                guardPendingUserInputCancellations: true,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                ...input.writeIfProviderThreadOwner,
                events,
              })
              .pipe(Effect.mapError(mapWriteError));
            return ownerResult.storedEvents;
          }
          if (input.writeIfRunCurrent === undefined) {
            return yield* eventSink
              .write({
                guardPendingUserInputCancellations: true,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                events,
              })
              .pipe(Effect.mapError(mapWriteError));
          }
          const result = yield* eventSink
            .writeIfRunCurrent({
              guardPendingUserInputCancellations: true,
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              threadId: input.threadId,
              ...input.writeIfRunCurrent,
              events,
            })
            .pipe(Effect.mapError(mapWriteError));
          return result.storedEvents;
        }).pipe(
          Effect.tap((storedEvents) =>
            Effect.gen(function* () {
              if (storedEvents.length > 0 && assistantOutputKey(input.event) !== undefined) {
                const output =
                  input.event.type === "message.updated"
                    ? input.event.message
                    : input.event.type === "turn_item.updated" &&
                        (input.event.turnItem.type === "assistant_message" ||
                          input.event.turnItem.type === "reasoning")
                      ? input.event.turnItem
                      : undefined;
                if (input.event.type === "node.updated" || output?.streaming === false) {
                  yield* Effect.sync(() => {
                    for (const [key, bucket] of bufferedOutput) {
                      const outputId = assistantOutputKey(input.event)!;
                      if (bucket.events.get(outputId)?.event !== input.event) continue;
                      bucket.events.delete(outputId);
                      if (bucket.events.size === 0) bufferedOutput.delete(key);
                    }
                  }).pipe(outputPermit.withPermits(1));
                }
              }
              if (storedEvents.length === 0 || input.event.type !== "provider_turn.updated") return;
              const providerTurn = input.event.providerTurn;
              if (
                providerTurn.status !== "completed" &&
                providerTurn.status !== "failed" &&
                providerTurn.status !== "interrupted" &&
                providerTurn.status !== "cancelled"
              )
                return;
              const key = `${input.providerInstanceId}:${providerTurn.id}`;
              if (completedTurnAnalytics.has(key)) return;
              completedTurnAnalytics.add(key);
              if (completedTurnAnalytics.size > 4096) {
                const oldest = completedTurnAnalytics.values().next().value;
                if (oldest !== undefined) completedTurnAnalytics.delete(oldest);
              }
              yield* analytics.record(
                providerTurnAnalyticsProperties({
                  driver: input.event.driver,
                  providerTurn,
                  ...(input.analyticsContext === undefined
                    ? {}
                    : { context: input.analyticsContext }),
                }),
              );
            }),
          ),
        ),
    });
  }),
);
