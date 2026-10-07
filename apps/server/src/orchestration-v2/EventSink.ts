import { readApplicationBirthRecord } from "../jones/importedHistory/ApplicationBirth.ts";
import type { ImportedApplicationAttachmentBirthV1 } from "../jones/importedHistory/ImportedApplicationAttachmentInventory.ts";
import * as NativeCreationRepository from "../jones/nativeCreation/NativeCreationRepository.ts";
import { DispatchGuardRejected } from "./DispatchGuard.ts";
import {
  type RecordedRun as OrchestrationV2Run,
  RecordedLifecycleEvent as OrchestrationV2DomainEvent,
  type RecordedStoredEvent as OrchestrationV2RecordedStoredEvent,
  RecordedStoredLifecycleEvent as OrchestrationV2StoredEvent,
} from "./RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyPayloadHash,
  legacyBootstrapCreateCommandId,
  legacyBootstrapBirth,
  sameLegacyBootstrapPolicy,
} from "./LegacyBootstrap.ts";
import {
  type OrchestrationV2StoredEvent as PublicStoredEvent,
  CommandId,
  type OrchestrationV2ProviderThread,
  type ProviderRuntimeEvidenceCapture,
  type RequestedRuntimeIdentity,
  type OrchestrationV2PrivateEvent,
  OrchestrationV2LegacyPreflightBinding,
  QueueDispatchCommand,
  type MessageId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  NodeId,
  type ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { identityForRequest } from "./ProviderAdapter.ts";
import { replayAndBufferProjectedLiveEvents } from "./LiveStreamBudget.ts";
import type { UnsequencedProjectEvent } from "../persistence/Services/OrchestrationEventStore.ts";
import { isPublicStoredOrchestrationEvent, projectDomainEventForWire } from "./WireProjection.ts";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

/**
 * ERRORS
 */
export class EventSinkWriteError extends Schema.TaggedError<EventSinkWriteError>()(
  "EventSinkWriteError",
  {
    eventCount: Schema.Number,
    commandId: Schema.optional(CommandId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to write ${this.eventCount} orchestration V2 event(s).`;
  }
}

export class EventSinkStreamError extends Schema.TaggedError<EventSinkStreamError>()(
  "EventSinkStreamError",
  {
    threadId: Schema.optional(ThreadId),
    afterSequence: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? "Failed to stream orchestration V2 events."
      : `Failed to stream orchestration V2 events for thread ${this.threadId}.`;
  }
}

export const EventSinkV2Error = Schema.Union([EventSinkWriteError, EventSinkStreamError]);
export type EventSinkV2Error = typeof EventSinkV2Error.Type;

function runtimeEvidenceMatches(
  current: OrchestrationV2ProviderThread | null,
  capture: ProviderRuntimeEvidenceCapture,
): boolean {
  if (
    current === null ||
    current.appThreadId !== capture.threadId ||
    current.id !== capture.providerThreadId ||
    current.providerSessionId !== capture.providerSessionId ||
    current.providerInstanceId !== capture.providerInstanceId ||
    current.driver !== capture.driver ||
    current.nativeThreadRef?.driver !== capture.driver ||
    current.nativeThreadRef.nativeId !== capture.nativeThreadId ||
    current.runtimeIdentity === undefined
  )
    return false;
  const identity = current.runtimeIdentity;
  if (
    capture.runtimeGeneration !== undefined &&
    identity.runtimeGeneration !== capture.runtimeGeneration
  )
    return false;
  if (capture.evidenceRevision !== undefined) {
    const revision = identity.evidenceRevision;
    if (
      revision === undefined ||
      (capture.runtimeGeneration === undefined
        ? revision !== capture.evidenceRevision
        : revision < capture.evidenceRevision)
    )
      return false;
  }
  return true;
}

/**
 * SERVICE DEFINITION
 */
interface EventSinkStreamInput {
  readonly threadId?: ThreadId;
  readonly afterSequence?: number;
  /** Filter before queuing so workers retain only the events they handle. */
  readonly eventType?: OrchestrationV2DomainEvent["type"];
  /** Bounded subscribers receive projected public events. Workers retain recorded values. */
  readonly bounded?: boolean;
}

export interface EventSinkV2Shape {
  readonly readApplicationBirthRecord?: (
    threadId: ThreadId,
  ) => Effect.Effect<ImportedApplicationAttachmentBirthV1 | null, EventSinkV2Error>;

  readonly commitLegacyPreflight: (input: {
    readonly commandId: CommandId;
    readonly event: OrchestrationV2PrivateEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.CommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;

  readonly write: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeWithEffects: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeIfRunCurrent: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedStatus: OrchestrationV2Run["status"];
    readonly effects?: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /**
   * Atomically commit only when the provider thread is still owned by the
   * expected run attempt and ordinal. Used for late post-terminal
   * provider_thread updates so a completed or superseded attempt cannot clobber
   * a newer attempt that already claimed the thread.
   */
  readonly writeIfProviderThreadOwner: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly providerThreadId: ProviderThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedLastRunOrdinal: number;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  readonly commitCommand: (input: {
    readonly nativeCreation?: {
      readonly claimId: string;
      readonly command: import("@t3tools/contracts").OrchestrationV2Command;
    };
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly cancelUnsettledEffects?: {
      readonly effectTypes: ReadonlyArray<EffectOutbox.OrchestrationEffectRequestV2["type"]>;
      readonly reason: string;
    };
  }) => Effect.Effect<
    {
      readonly receipt: CommandReceiptStore.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      readonly committed: boolean;
      readonly cancelledEffectCount: number;
    },
    EventSinkV2Error
  >;
  readonly commitRejectedCommand: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
    readonly legacyGuardRejection?: {
      readonly rejection: DispatchGuardRejected;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    };
  }) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, EventSinkV2Error>;
  /**
   * Append a project event, fold it into its row and record the receipt in one
   * transaction. A reused command id commits nothing and returns its receipt.
   */
  readonly commitProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly event: UnsequencedProjectEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.ProjectCommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;
  /** Record a rejected project command, or return the receipt its command id already has. */
  readonly commitRejectedProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.ProjectCommandReceiptV2, EventSinkV2Error>;
  readonly stream: {
    (
      input: EventSinkStreamInput & { readonly bounded: true },
    ): Stream.Stream<PublicStoredEvent, EventSinkV2Error>;
    (
      input?: EventSinkStreamInput & { readonly bounded?: false },
    ): Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
    (
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error>;
  };
  readonly latestSequence: (input?: {
    readonly threadId?: ThreadId;
  }) => Effect.Effect<number, EventSinkV2Error>;
  readonly canPromoteQueuedAtToolBoundary: (input: {
    readonly threadId: ThreadId;
    readonly queuedRunId: RunId;
    readonly activeRunId: RunId;
    readonly messageId: MessageId;
    readonly boundary: OrchestrationV2StoredEvent;
    readonly runtimeMode: string;
    readonly interactionMode: string;
    readonly births?: Map<
      RunId,
      {
        readonly sequence: number;
        readonly runtimeMode: string;
        readonly interactionMode: string;
        readonly queuedToolBoundaryEligible: boolean;
      } | null
    >;
  }) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly readByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
}

export class EventSinkV2 extends Context.Service<EventSinkV2, EventSinkV2Shape>()(
  "t3/orchestration-v2/EventSink/EventSinkV2",
) {}

/**
 * IMPLEMENTATIONS
 */
const isDispatchGuardRejected = (value: unknown): value is DispatchGuardRejected =>
  Schema.is(DispatchGuardRejected)(value);

const baseLayer: Layer.Layer<
  EventSinkV2,
  never,
  | CommandReceiptStore.CommandReceiptStoreV2
  | EffectOutbox.EffectOutboxV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | TurnItemPositionStore.TurnItemPositionStoreV2
> = Layer.effect(
  EventSinkV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const nativeCreation = yield* Effect.serviceOption(
      NativeCreationRepository.NativeCreationRepository,
    );
    const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
    const liveEventsByType = new Map<
      OrchestrationV2DomainEvent["type"],
      PubSub.PubSub<OrchestrationV2StoredEvent>
    >();
    const publishLiveEvents = (events: ReadonlyArray<OrchestrationV2StoredEvent>) =>
      Effect.gen(function* () {
        yield* PubSub.publishAll(liveEvents, events);
        for (const [type, pubsub] of liveEventsByType) {
          yield* PubSub.publishAll(
            pubsub,
            events.filter((stored) => stored.event.type === type),
          );
        }
      });
    const publishStoredEvents = (events: ReadonlyArray<OrchestrationV2RecordedStoredEvent>) =>
      eventStore
        .publishCommitted(events)
        .pipe(Effect.andThen(publishLiveEvents(events.filter(isPublicStoredOrchestrationEvent))));

    // Transactions commit one at a time, but each writer publishes after its
    // commit. If a writer is descheduled in between, a later commit reaches
    // subscribers first, and clients drop any event at or below the newest
    // sequence they have applied. So a writer takes this lane as the last step
    // of its transaction and holds it until it has published. Publishing never
    // waits, so a writer that holds the transaction while it waits for the
    // lane is not blocked for long.
    const publishLane = yield* Semaphore.make(1);
    const commitThenPublish = <A, E, R>(
      transaction: Effect.Effect<A, E, R>,
      publish: (committed: A) => Effect.Effect<void>,
    ) =>
      Effect.suspend(() => {
        let holdsLane = false;
        const takeLane = publishLane.take(1).pipe(
          Effect.andThen(
            Effect.sync(() => {
              holdsLane = true;
            }),
          ),
          Effect.uninterruptible,
        );
        return sql
          .withTransaction(Effect.tap(transaction, () => takeLane))
          .pipe(
            Effect.tap(publish),
            Effect.ensuring(
              Effect.suspend(() => (holdsLane ? publishLane.release(1) : Effect.void)),
            ),
          );
      });

    // A user can answer after terminal normalization reads the pending request.
    // Recheck inside the write transaction so stale cleanup cannot erase answers.
    const guardUserInputCancellations = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
      Effect.gen(function* () {
        const staleRequests = new Set<RuntimeRequestId>();
        const staleNodes = new Set<NodeId>();
        for (const event of events) {
          if (
            event.type !== "runtime-request.updated" ||
            event.payload.kind !== "user_input" ||
            event.payload.status !== "cancelled"
          )
            continue;
          const current = yield* projectionStore.getRuntimeRequest(
            event.threadId,
            event.payload.id,
          );
          if (
            current?.status !== "pending" ||
            current.kind !== "user_input" ||
            current.providerTurnId !== event.payload.providerTurnId ||
            current.responseCapability.type === "message"
          ) {
            staleRequests.add(event.payload.id);
            staleNodes.add(event.payload.nodeId);
          }
        }
        return events.filter((event) => {
          switch (event.type) {
            case "runtime-request.updated":
              return event.payload.status !== "cancelled" || !staleRequests.has(event.payload.id);
            case "node.updated":
              return event.payload.status !== "cancelled" || !staleNodes.has(event.payload.id);
            case "turn-item.updated":
              return (
                event.payload.type !== "user_input_request" ||
                event.payload.status !== "cancelled" ||
                !staleRequests.has(event.payload.requestId)
              );
            default:
              return true;
          }
        });
      });

    const guardRuntimeIdentity = (
      input: Pick<
        Parameters<EventSinkV2Shape["write"]>[0],
        | "events"
        | "runtimeEvidence"
        | "runtimeIdentityObservation"
        | "runtimeIdentityBoundary"
        | "runtimeIdentityRequest"
        | "runtimeIdentityPreviousRequest"
      >,
    ) =>
      Effect.gen(function* () {
        const capture = input.runtimeEvidence;
        const update = input.events.find((event) => event.type === "provider-thread.updated");
        if (input.runtimeIdentityBoundary !== undefined) {
          if (update?.type !== "provider-thread.updated") return null;
          const current =
            (yield* projectionStore.getThreadRecords(update.threadId, [
              "providerThreads",
            ])).providerThreads.find((thread) => thread.id === update.payload.id) ?? null;
          if (
            (capture !== undefined && !runtimeEvidenceMatches(current, capture)) ||
            (current?.runtimeIdentity?.runtimeGeneration ?? null) !==
              input.runtimeIdentityBoundary.expectedGeneration ||
            (current !== null &&
              (current.appThreadId !== update.payload.appThreadId ||
                current.providerInstanceId !== update.payload.providerInstanceId ||
                current.driver !== update.payload.driver))
          )
            return null;
          return input.events.map((event) =>
            event.type === "provider-thread.updated" && event.payload.id === update.payload.id
              ? {
                  ...event,
                  payload: {
                    ...event.payload,
                    runtimeIdentity:
                      event.payload.runtimeIdentity === undefined
                        ? undefined
                        : {
                            ...event.payload.runtimeIdentity,
                            evidenceRevision: (current?.runtimeIdentity?.evidenceRevision ?? 0) + 1,
                          },
                  },
                }
              : event,
          );
        }
        if (capture === undefined) return input.events;
        const current =
          (yield* projectionStore.getThreadRecords(capture.threadId, [
            "providerThreads",
          ])).providerThreads.find((thread) => thread.id === capture.providerThreadId) ?? null;
        if (!runtimeEvidenceMatches(current, capture) || current === null) return null;
        const identity = current.runtimeIdentity!;
        if (
          input.runtimeIdentityRequest !== undefined &&
          capture.evidenceRevision !== identity.evidenceRevision
        ) {
          const previous = input.runtimeIdentityPreviousRequest;
          if (
            previous === undefined ||
            identity.requested.providerInstanceId !== previous.providerInstanceId ||
            identity.requested.providerDriver !== previous.providerDriver ||
            identity.requested.model !== previous.model ||
            identity.requested.serviceTier !== previous.serviceTier
          )
            return null;
        }
        if (input.runtimeIdentityObservation !== undefined) {
          const requested = input.runtimeIdentityObservation;
          if (
            identity.requested.providerInstanceId !== requested.providerInstanceId ||
            identity.requested.providerDriver !== requested.providerDriver ||
            identity.requested.model !== requested.model ||
            identity.requested.serviceTier !== requested.serviceTier ||
            identity.evidenceRevision !== capture.evidenceRevision
          )
            return null;
        }
        return input.events.map((event) =>
          event.type === "provider-thread.updated" && event.payload.id === current.id
            ? {
                ...event,
                payload: {
                  ...event.payload,
                  runtimeIdentity:
                    input.runtimeIdentityObservation === undefined
                      ? input.runtimeIdentityRequest === undefined
                        ? identity
                        : {
                            ...identityForRequest(input.runtimeIdentityRequest, identity),
                            evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                          }
                      : {
                          ...identity,
                          observed: event.payload.runtimeIdentity!.observed,
                          evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                        },
                },
              }
            : event,
        );
      });

    const normalizeEvents = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      const runOrdinals = new Map(
        events.flatMap((event) =>
          event.type === "run.created" || event.type === "run.updated"
            ? [[event.payload.id, event.payload.ordinal] as const]
            : [],
        ),
      );
      return Effect.forEach(
        events,
        (event): Effect.Effect<OrchestrationV2DomainEvent, unknown> =>
          event.type === "turn-item.updated"
            ? turnItemPositions
                .normalize(
                  event.payload,
                  event.payload.runId === null ? undefined : runOrdinals.get(event.payload.runId),
                )
                .pipe(Effect.map((payload) => ({ ...event, payload })))
            : Effect.succeed(event),
        { concurrency: 1 },
      );
    };

    const applyStoredEvents = (storedEvents: ReadonlyArray<OrchestrationV2RecordedStoredEvent>) =>
      Effect.gen(function* () {
        yield* Effect.forEach(storedEvents, (stored) => projectionStore.apply(stored.event), {
          concurrency: 1,
        });
        const sequence = storedEvents.at(-1)?.sequence;
        if (sequence !== undefined) {
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${sequence},
              ${now}
            )
            ON CONFLICT(projection_name)
            DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }
      });

    const writeEffect = Effect.fn("orchestrationV2.EventSink.write")(function* (
      input: Parameters<EventSinkV2Shape["writeWithEffects"]>[0],
    ) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.thread_id": input.events[0]?.threadId ?? null,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const identityEvents = yield* guardRuntimeIdentity(input);
          if (identityEvents === null) return [];
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const committed = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          yield* applyStoredEvents(committed);
          yield* effectOutbox.enqueue(input.effects);
          return committed;
        }),
        (storedEvents) =>
          Effect.gen(function* () {
            if (input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            yield* publishStoredEvents(storedEvents);
          }),
      );
    });

    const writeIfRunCurrentEffect = Effect.fn("orchestrationV2.EventSink.writeIfRunCurrent")(
      function* (input: Parameters<EventSinkV2Shape["writeIfRunCurrent"]>[0]) {
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.command_id": input.commandId ?? null,
          "orchestration_v2.event_count": input.events.length,
          "orchestration_v2.run_id": input.runId,
          "orchestration_v2.thread_id": input.threadId,
        });

        return yield* commitThenPublish(
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly status: string;
              readonly active_attempt_id: string | null;
            }>`
            SELECT
              status,
              json_extract(payload_json, '$.activeAttemptId') AS active_attempt_id
            FROM orchestration_v2_projection_runs
            WHERE run_id = ${input.runId}
              AND thread_id = ${input.threadId}
            LIMIT 1
          `;
            const current = rows[0];
            if (
              current === undefined ||
              current.status !== input.expectedStatus ||
              current.active_attempt_id !== input.activeAttemptId
            ) {
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            }

            const identityEvents = yield* guardRuntimeIdentity(input);
            if (identityEvents === null)
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            const normalized = yield* normalizeEvents(
              input.guardPendingUserInputCancellations === true
                ? yield* guardUserInputCancellations(identityEvents)
                : identityEvents,
            );
            const storedEvents = yield* eventStore
              .append({
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                events: normalized,
              })
              .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
            yield* applyStoredEvents(storedEvents);
            yield* effectOutbox.enqueue(input.effects ?? []);
            return { committed: true as const, storedEvents };
          }),
          (result) =>
            result.committed
              ? Effect.gen(function* () {
                  if (input.effects !== undefined && input.effects.length > 0) {
                    yield* effectOutbox.notifyAvailable(input.effects.length);
                  }
                  yield* publishStoredEvents(result.storedEvents);
                })
              : Effect.void,
        );
      },
    );

    const writeIfProviderThreadOwnerEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadOwner",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadOwner"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.providerThreadId,
        "orchestration_v2.run_id": input.runId,
        "orchestration_v2.active_attempt_id": input.activeAttemptId,
        "orchestration_v2.expected_last_run_ordinal": input.expectedLastRunOrdinal,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly active_attempt_id: string | null;
            readonly last_run_ordinal: number | null;
          }>`
            SELECT
              json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id,
              p.last_run_ordinal
            FROM orchestration_v2_projection_provider_threads p
            JOIN orchestration_v2_projection_runs r
              ON r.run_id = ${input.runId}
             AND r.thread_id = p.thread_id
            WHERE p.provider_thread_id = ${input.providerThreadId}
            LIMIT 1
          `;
          const current = rows[0];
          if (
            current === undefined ||
            current.active_attempt_id !== input.activeAttemptId ||
            current.last_run_ordinal !== input.expectedLastRunOrdinal
          ) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const identityEvents = yield* guardRuntimeIdentity(input);
          if (identityEvents === null)
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const storedEvents = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents };
        }),
        (result) => (result.committed ? publishStoredEvents(result.storedEvents) : Effect.void),
      );
    });

    const existingCommandResult = (commandId: CommandId) =>
      Effect.gen(function* () {
        const existing = yield* commandReceipts.getByCommandId(commandId);
        if (Option.isNone(existing)) {
          return yield* Effect.die(
            new Error(`Command receipt ${commandId} disappeared during its transaction.`),
          );
        }
        const storedEvents = yield* eventStore.readByCommandId({ commandId }).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.runCollect,
          Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
        );
        return { receipt: existing.value, storedEvents };
      });

    const commitCommandEffect = Effect.fn("orchestrationV2.EventSink.commitCommand")(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          if (
            input.effects.some(
              (effect) =>
                effect.nativeCreationExecutionReference !== undefined &&
                (input.nativeCreation === undefined ||
                  effect.commandId !== input.commandId ||
                  effect.threadId !== input.threadId ||
                  effect.nativeCreationExecutionReference.claimId !==
                    input.nativeCreation.claimId ||
                  effect.nativeCreationExecutionReference.stageCommandId !== input.commandId ||
                  effect.nativeCreationExecutionReference.effectId !== effect.id),
            )
          ) {
            return yield* Effect.fail(
              new NativeCreationRepository.NativeCreationRepositoryError({
                code: "unresolved_claim",
                message: "Native queued effect differs from its acceptance binding",
              }),
            );
          }
          if (
            input.nativeCreation !== undefined &&
            input.effects.some(
              (effect) =>
                effect.request.type === "provider-turn.start" &&
                effect.nativeCreationExecutionReference === undefined,
            )
          ) {
            return yield* Effect.fail(
              new NativeCreationRepository.NativeCreationRepositoryError({
                code: "unresolved_claim",
                message: "Native provider start is missing its execution envelope",
              }),
            );
          }
          const reserved = yield* commandReceipts.insertIfAbsent({
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: 0,
            status: "accepted",
            error: null,
          });
          if (!reserved) {
            const existing = yield* existingCommandResult(input.commandId);
            if (input.nativeCreation !== undefined) {
              const boundary = existing.storedEvents.at(-1);
              if (
                Option.isNone(nativeCreation) ||
                nativeCreation.value.recordExecutionAcceptance === undefined ||
                boundary === undefined
              )
                return yield* Effect.fail(
                  new NativeCreationRepository.NativeCreationRepositoryError({
                    code: "unresolved_claim",
                    message: "Native receipt retry has no native acceptance owner",
                  }),
                );
              yield* nativeCreation.value.recordExecutionAcceptance({
                ...input.nativeCreation,
                eventId: boundary.event.id,
                sequence: boundary.sequence,
              });
            }
            return { ...existing, committed: false as const, cancelledEffectIds: [] };
          }

          const normalized = yield* normalizeEvents(input.events);
          const storedEvents = yield* eventStore
            .append({
              commandId: input.commandId,
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          const sequence = storedEvents.at(-1)?.sequence;
          if (sequence === undefined) {
            return yield* Effect.die(
              new Error(`Command ${input.commandId} produced no orchestration events.`),
            );
          }
          yield* applyStoredEvents(storedEvents);
          yield* effectOutbox.enqueue(input.effects);
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: sequence,
            status: "accepted",
            error: null,
          };
          yield* commandReceipts.upsert(receipt);
          if (input.nativeCreation !== undefined) {
            if (
              Option.isNone(nativeCreation) ||
              nativeCreation.value.recordExecutionAcceptance === undefined ||
              input.nativeCreation.command.commandId !== input.commandId ||
              !("threadId" in input.nativeCreation.command) ||
              input.nativeCreation.command.threadId !== input.threadId ||
              input.nativeCreation.command.type !== input.commandType
            ) {
              return yield* Effect.fail(
                new NativeCreationRepository.NativeCreationRepositoryError({
                  code: "unresolved_claim",
                  message: "Native acceptance owner is unavailable or differs from the receipt",
                }),
              );
            }
            const event = storedEvents.at(-1)!;
            yield* nativeCreation.value.recordExecutionAcceptance({
              ...input.nativeCreation,
              eventId: event.event.id,
              sequence,
            });
          }
          const cancelledEffectIds =
            input.cancelUnsettledEffects === undefined
              ? []
              : yield* effectOutbox.cancelUnsettled({
                  threadId: input.threadId,
                  ...input.cancelUnsettledEffects,
                });
          return { receipt, storedEvents, committed: true as const, cancelledEffectIds };
        }),
        (result) =>
          Effect.gen(function* () {
            yield* effectOutbox.signalCancellations(result.cancelledEffectIds);
            if (result.committed && input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            if (result.committed) yield* publishStoredEvents(result.storedEvents);
          }),
      );
      return {
        receipt: result.receipt,
        storedEvents: result.storedEvents,
        committed: result.committed,
        cancelledEffectCount: result.cancelledEffectIds.length,
      };
    });

    const samePreflightBinding = Schema.toEquivalence(OrchestrationV2LegacyPreflightBinding);
    const readPreflight = (commandId: CommandId) =>
      eventStore.readByCommandId({ commandId }).pipe(
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
      );
    const commitLegacyPreflight = Effect.fn("orchestrationV2.EventSink.commitLegacyPreflight")(
      function* (input: Parameters<EventSinkV2Shape["commitLegacyPreflight"]>[0]) {
        const event = input.event;
        const binding =
          event.type === "legacy-bootstrap.preflight-intent"
            ? event.payload
            : event.payload.binding;
        const policy = binding.policy;
        const intentId = CommandId.make(`${policy.createCommandId}:preflight-intent`);
        const outcomeId = CommandId.make(`${policy.createCommandId}:preflight-outcome`);
        const reject = (detail: string) => new Error(detail);
        const payload = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(QueueDispatchCommand),
        )(binding.canonicalPayload);
        if (
          payload.type !== "thread.turn.start" ||
          payload.bootstrap === undefined ||
          policy.runId !== undefined ||
          policy.createCommandId !==
            legacyBootstrapCreateCommandId(policy.threadId, policy.releaseCommandId) ||
          policy.birthCommandId !== `${policy.createCommandId}:initial-message` ||
          event.threadId !== policy.threadId ||
          binding.fetch.remote !== (binding.fetch.startFromOrigin ? "origin" : null) ||
          canonicalLegacyPayload(policy.dispatchGuard ?? null) !==
            canonicalLegacyPayload(payload.dispatchGuard ?? null) ||
          policy.payloadHash !== legacyPayloadHash(binding.canonicalPayload) ||
          binding.canonicalPayload !== canonicalLegacyPayload(payload) ||
          payload.threadId !== policy.threadId ||
          payload.commandId !== policy.releaseCommandId ||
          payload.message.messageId !== policy.messageId ||
          (payload.bootstrap.createThread !== undefined &&
            payload.bootstrap.createThread.projectId !== policy.projectId) ||
          payload.bootstrap.prepareWorktree?.projectCwd !== binding.fetch.cwd ||
          payload.bootstrap.prepareWorktree.baseBranch !== binding.fetch.baseRef ||
          (payload.bootstrap.prepareWorktree.startFromOrigin === true) !==
            binding.fetch.startFromOrigin ||
          (payload.bootstrap.prepareWorktree.requireWorktree === true) !==
            binding.fetch.requireWorktree ||
          input.commandId !==
            (event.type === "legacy-bootstrap.preflight-intent" ? intentId : outcomeId)
        )
          return yield* Effect.fail(
            reject("Preflight does not match its canonical immutable queue binding."),
          );
        const verify = (
          stored: ReadonlyArray<OrchestrationV2RecordedStoredEvent>,
          type: OrchestrationV2PrivateEvent["type"],
        ) => {
          if (
            stored.length !== 1 ||
            stored[0]?.commandId !== input.commandId ||
            stored[0].event.type !== type ||
            stored[0].event.threadId !== policy.threadId
          )
            return false;
          const recorded = stored[0].event;
          if (recorded.type === "legacy-bootstrap.preflight-intent")
            return samePreflightBinding(recorded.payload, binding);
          if (
            recorded.type !== "legacy-bootstrap.preflight-outcome" ||
            event.type !== recorded.type
          )
            return false;
          return (
            samePreflightBinding(recorded.payload.binding, binding) &&
            canonicalLegacyPayload(recorded.payload) === canonicalLegacyPayload(event.payload)
          );
        };
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            if (Option.isSome(yield* commandReceipts.getProjectByCommandId(input.commandId)))
              return yield* Effect.fail(
                reject("Preflight command ID belongs to a project command."),
              );
            const existing = yield* commandReceipts.getByCommandId(input.commandId);
            if (Option.isSome(existing)) {
              const stored = yield* readPreflight(input.commandId);
              if (
                existing.value.status !== "accepted" ||
                existing.value.commandType !== event.type ||
                existing.value.threadId !== policy.threadId ||
                !verify(stored, event.type) ||
                stored[0]?.sequence !== existing.value.resultSequence
              )
                return yield* Effect.fail(
                  reject("Preflight command ID belongs to another immutable effect binding."),
                );
              return {
                receipt: existing.value,
                committed: false,
                storedEvents: [] as ReadonlyArray<OrchestrationV2RecordedStoredEvent>,
              };
            }
            if (
              Option.isSome(yield* commandReceipts.getByCommandId(policy.releaseCommandId)) ||
              Option.isSome(
                yield* commandReceipts.getProjectByCommandId(policy.releaseCommandId),
              ) ||
              Option.isSome(yield* commandReceipts.getByCommandId(policy.createCommandId)) ||
              Option.isSome(yield* commandReceipts.getProjectByCommandId(policy.createCommandId))
            )
              return yield* Effect.fail(
                reject("Preflight cannot claim an already born or released bootstrap."),
              );
            if (event.type === "legacy-bootstrap.preflight-intent") {
              const history = Array.from(
                yield* eventStore.read({ threadId: policy.threadId }).pipe(Stream.runCollect),
              );
              for (const previous of history) {
                if (previous.event.type !== "legacy-bootstrap.preflight-intent") continue;
                const previousOutcomes = history.filter(
                  (stored) =>
                    stored.event.type === "legacy-bootstrap.preflight-outcome" &&
                    stored.event.payload.intentCommandId === previous.commandId &&
                    stored.event.payload.intentSequence === previous.sequence,
                );
                if (
                  previousOutcomes.length !== 1 ||
                  previousOutcomes[0]?.event.type !== "legacy-bootstrap.preflight-outcome" ||
                  previousOutcomes[0].event.payload.status === "unknown"
                )
                  return yield* Effect.fail(
                    reject(
                      "Target has an unresolved preflight effect; another command cannot repeat or replace it.",
                    ),
                  );
              }
            }
            if (event.type === "legacy-bootstrap.preflight-outcome") {
              const receipt = yield* commandReceipts.getByCommandId(intentId);
              const intents = yield* readPreflight(intentId);
              const intent = intents[0];
              if (
                Option.isNone(receipt) ||
                receipt.value.status !== "accepted" ||
                receipt.value.commandType !== "legacy-bootstrap.preflight-intent" ||
                receipt.value.threadId !== policy.threadId ||
                intents.length !== 1 ||
                intent?.event.type !== "legacy-bootstrap.preflight-intent" ||
                !samePreflightBinding(intent.event.payload, binding) ||
                event.payload.intentCommandId !== intentId ||
                event.payload.intentSequence !== intent.sequence ||
                receipt.value.resultSequence !== intent.sequence
              )
                return yield* Effect.fail(
                  reject("Preflight outcome has no exact accepted intent."),
                );
            }
            const reserved = yield* commandReceipts.insertIfAbsent({
              commandId: input.commandId,
              threadId: policy.threadId,
              commandType: event.type,
              acceptedAt: event.occurredAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            });
            if (!reserved)
              return yield* Effect.fail(
                reject("Preflight reservation changed inside its serialized transaction."),
              );
            const storedEvents = yield* eventStore.append({
              commandId: input.commandId,
              events: [event],
            });
            const sequence = storedEvents[0]?.sequence;
            if (sequence === undefined || !verify(storedEvents, event.type))
              return yield* Effect.fail(
                reject("Preflight journal binding was not committed exactly."),
              );
            yield* applyStoredEvents(storedEvents);
            const receipt: CommandReceiptStore.CommandReceiptV2 = {
              commandId: input.commandId,
              threadId: policy.threadId,
              commandType: event.type,
              acceptedAt: event.occurredAt,
              resultSequence: sequence,
              status: "accepted",
              error: null,
            };
            yield* commandReceipts.upsert(receipt);
            return { receipt, committed: true, storedEvents };
          }),
          (result) => (result.committed ? publishStoredEvents(result.storedEvents) : Effect.void),
        );
        const readback = yield* readPreflight(input.commandId);
        if (
          !verify(readback, event.type) ||
          readback[0]?.sequence !== result.receipt.resultSequence
        )
          return yield* Effect.fail(reject("Preflight readback is unknown."));
        return { receipt: result.receipt, committed: result.committed };
      },
      Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 1, cause })),
    );

    const commitRejectedCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedCommand"]>[0]) {
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          const existing = yield* commandReceipts.getByCommandId(input.commandId);
          if (Option.isSome(existing))
            return {
              receipt: existing.value,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          const legacy = input.legacyGuardRejection;
          let events: ReadonlyArray<OrchestrationV2DomainEvent> = [];
          if (legacy !== undefined) {
            const last = legacy.events.at(-1);
            const decision =
              last?.type === "run.updated" ? last.payload.legacyReleaseDecision : undefined;
            const policy = decision?.policy;
            const invalid = () =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: legacy.events.length,
                cause: "Legacy guard rejection has no exact authenticated birth and current run.",
              });
            if (
              !isDispatchGuardRejected(legacy.rejection) ||
              legacy.rejection.observed === undefined ||
              input.commandType !== "prepared-run.release" ||
              last?.type !== "run.updated" ||
              decision === undefined ||
              policy === undefined ||
              input.commandId !== policy.releaseCommandId ||
              input.threadId !== policy.threadId ||
              last.threadId !== input.threadId ||
              last.runId !== policy.runId ||
              last.payload.id !== policy.runId ||
              last.payload.userMessageId !== policy.messageId ||
              last.id !== decision.evidenceEventId ||
              decision.reason !== legacy.rejection.reason ||
              canonicalLegacyPayload(decision.observed) !==
                canonicalLegacyPayload(legacy.rejection.observed) ||
              canonicalLegacyPayload(decision.guard) !==
                canonicalLegacyPayload(policy.dispatchGuard)
            )
              return yield* invalid();
            const collect = (commandId: CommandId) =>
              eventStore.readByCommandId({ commandId }).pipe(
                Stream.filter(isPublicStoredOrchestrationEvent),
                Stream.runCollect,
                Effect.map((stored) => Array.from(stored)),
              );
            const proof = legacyBootstrapBirth({
              policy,
              claimEvents: yield* collect(policy.createCommandId),
              birthEvents: yield* collect(policy.birthCommandId),
            });
            const claim = yield* commandReceipts.getByCommandId(policy.createCommandId);
            const birth = yield* commandReceipts.getByCommandId(policy.birthCommandId);
            const projection = yield* projectionStore.getThreadRecords(input.threadId, [
              "runs",
              "attempts",
              "nodes",
              "turnItems",
            ]);
            const current = projection.runs.find((run) => run.id === policy.runId);
            if (
              proof.type !== "valid" ||
              proof.claimEventId !== decision.claimEventId ||
              proof.claimSequence !== decision.claimSequence ||
              proof.birthEventId !== decision.birthEventId ||
              proof.sequence !== decision.birthSequence ||
              Option.isNone(claim) ||
              Option.isNone(birth) ||
              claim.value.status !== "accepted" ||
              birth.value.status !== "accepted" ||
              claim.value.threadId !== policy.threadId ||
              birth.value.threadId !== policy.threadId ||
              claim.value.commandType !==
                (policy.ownsNewThread ? "thread.create" : "thread.metadata.update") ||
              birth.value.commandType !== "message.dispatch" ||
              claim.value.resultSequence !== decision.claimReceiptSequence ||
              birth.value.resultSequence !== decision.birthReceiptSequence ||
              current?.status !== "preparing" ||
              current.legacyBootstrap === undefined ||
              !sameLegacyBootstrapPolicy(current.legacyBootstrap, policy) ||
              canonicalLegacyPayload(current) !==
                canonicalLegacyPayload({
                  ...last.payload,
                  legacyReleaseDecision: current.legacyReleaseDecision,
                  ...(policy.ownsNewThread
                    ? { status: current.status, completedAt: current.completedAt }
                    : {}),
                })
            )
              return yield* invalid();
            if (policy.ownsNewThread) {
              const [attemptEvent, nodeEvent, itemEvent] = legacy.events;
              const attempt = projection.attempts.find(
                (record) => record.id === current.activeAttemptId,
              );
              const node = projection.nodes.find((record) => record.id === current.rootNodeId);
              const item =
                itemEvent?.type === "turn-item.updated"
                  ? projection.turnItems.find(
                      (record) =>
                        record.id === itemEvent.payload.id &&
                        record.type === "command_execution" &&
                        record.input === "Preparing workspace" &&
                        record.runId === current.id,
                    )
                  : undefined;
              if (
                legacy.events.length !== 4 ||
                new Set(legacy.events.map((event) => event.id)).size !== 4 ||
                attemptEvent?.type !== "run-attempt.updated" ||
                nodeEvent?.type !== "node.updated" ||
                itemEvent?.type !== "turn-item.updated" ||
                itemEvent.payload.type !== "command_execution" ||
                attempt === undefined ||
                node === undefined ||
                item === undefined ||
                last.payload.status !== "failed" ||
                canonicalLegacyPayload(last.payload.completedAt) !==
                  canonicalLegacyPayload(input.rejectedAt) ||
                canonicalLegacyPayload(attemptEvent.payload) !==
                  canonicalLegacyPayload({
                    ...attempt,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(nodeEvent.payload) !==
                  canonicalLegacyPayload({
                    ...node,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(itemEvent.payload) !==
                  canonicalLegacyPayload({
                    ...item,
                    status: "failed",
                    title: "Dispatch guard rejected",
                    output: legacy.rejection.reason,
                    exitCode: undefined,
                    completedAt: input.rejectedAt,
                    updatedAt: input.rejectedAt,
                  }) ||
                legacy.events.some(
                  (event) =>
                    event.threadId !== policy.threadId ||
                    event.runId !== policy.runId ||
                    event.providerInstanceId !== current.providerInstanceId ||
                    canonicalLegacyPayload(event.occurredAt) !==
                      canonicalLegacyPayload(input.rejectedAt),
                )
              )
                return yield* invalid();
            } else if (legacy.events.length !== 1) return yield* invalid();
            events = legacy.events;
          }
          const reserved: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: 0,
            status: "rejected",
            error: input.error,
          };
          if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
            return {
              receipt: (yield* existingCommandResult(input.commandId)).receipt,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }
          const storedEvents =
            events.length === 0
              ? []
              : (yield* eventStore.append({
                  commandId: input.commandId,
                  events,
                })).filter(isPublicStoredOrchestrationEvent);
          yield* applyStoredEvents(storedEvents);
          const receipt = {
            ...reserved,
            resultSequence:
              storedEvents.at(-1)?.sequence ??
              (yield* eventStore.latestSequence({ threadId: input.threadId })),
          };
          yield* commandReceipts.upsert(receipt);
          return { receipt, storedEvents };
        }),
        (committed) => publishStoredEvents(committed.storedEvents),
      );
      return result.receipt;
    });

    const existingProjectReceipt = (commandId: CommandId) =>
      commandReceipts.getProjectByCommandId(commandId).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(`Command ${commandId} was already used by a thread command.` as const),
            onSome: Effect.succeed,
          }),
        ),
      );

    const commitProjectCommandEffect = Effect.fn("orchestrationV2.EventSink.commitProjectCommand")(
      function* (input: Parameters<EventSinkV2Shape["commitProjectCommand"]>[0]) {
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            const reserved: CommandReceiptStore.ProjectCommandReceiptV2 = {
              commandId: input.commandId,
              projectId: input.projectId,
              commandType: input.commandType,
              acceptedAt: input.acceptedAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            };
            if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
              return { receipt: yield* existingProjectReceipt(input.commandId), event: undefined };
            }
            const event = yield* eventStore.appendProjectEvent(input.event);
            yield* projectStore.apply(event);
            const receipt = { ...reserved, resultSequence: event.sequence };
            yield* commandReceipts.upsert(receipt);
            return { receipt, event };
          }),
          (result) =>
            result.event === undefined ? Effect.void : eventStore.publishCommitted([result.event]),
        );
        return { receipt: result.receipt, committed: result.event !== undefined };
      },
    );

    const commitRejectedProjectCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedProjectCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedProjectCommand"]>[0]) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const receipt: CommandReceiptStore.ProjectCommandReceiptV2 = {
            commandId: input.commandId,
            projectId: input.projectId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: yield* eventStore.latestApplicationSequence,
            status: "rejected",
            error: input.error,
          };
          return (yield* commandReceipts.insertIfAbsent(receipt))
            ? receipt
            : yield* existingProjectReceipt(input.commandId);
        }),
      );
    });

    const catchUp = (input: {
      readonly afterSequence: number;
      readonly throughSequence: number;
      readonly threadId?: ThreadId;
      readonly eventType?: OrchestrationV2DomainEvent["type"];
    }): Stream.Stream<OrchestrationV2StoredEvent, unknown> => {
      const pageSize = 256;
      const loop = (afterSequence: number): Stream.Stream<OrchestrationV2StoredEvent, unknown> =>
        Stream.unwrap(
          eventStore
            .read({
              afterSequence,
              throughSequence: input.throughSequence,
              ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
              ...(input.eventType === undefined ? {} : { eventType: input.eventType }),
              limit: pageSize,
            })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
              Effect.map((events) => {
                if (events.length === 0) {
                  return Stream.empty;
                }
                const current = Stream.fromIterable(
                  events.filter(isPublicStoredOrchestrationEvent),
                );
                const last = events.at(-1)?.sequence ?? input.throughSequence;
                return events.length < pageSize || last >= input.throughSequence
                  ? current
                  : Stream.concat(current, loop(last));
              }),
            ),
        );
      return loop(input.afterSequence);
    };

    const streamEffect = (input?: EventSinkStreamInput) => {
      const afterSequence = input?.afterSequence ?? 0;
      const matches = (stored: OrchestrationV2StoredEvent) =>
        (input?.threadId === undefined || stored.event.threadId === input.threadId) &&
        (input?.eventType === undefined || stored.event.type === input.eventType);
      const replay = (throughSequence: number) =>
        catchUp({
          afterSequence,
          throughSequence,
          ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
          ...(input?.eventType === undefined ? {} : { eventType: input.eventType }),
        }).pipe(Stream.filter(matches));
      return Stream.unwrap(
        Effect.gen(function* () {
          let pubsub = liveEvents;
          if (input?.eventType !== undefined) {
            const existing = liveEventsByType.get(input.eventType);
            if (existing !== undefined) {
              pubsub = existing;
            } else {
              const created = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
              pubsub = liveEventsByType.get(input.eventType) ?? created;
              liveEventsByType.set(input.eventType, pubsub);
            }
          }
          if (input?.bounded === true) {
            return replayAndBufferProjectedLiveEvents({
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: eventStore.latestSequence(),
              afterSequence,
              filter: matches,
              replay,
              project: (stored) => ({ ...stored, event: projectDomainEventForWire(stored.event) }),
            });
          }
          const subscription = yield* PubSub.subscribe(pubsub);
          const highWater = yield* eventStore.latestSequence();
          const live = Stream.fromSubscription(subscription).pipe(
            Stream.filter((stored) => stored.sequence > Math.max(highWater, afterSequence)),
            Stream.filter(matches),
          );
          return Stream.concat(replay(highWater), live);
        }),
      );
    };

    function stream(
      input: EventSinkStreamInput & { readonly bounded: true },
    ): Stream.Stream<PublicStoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput & { readonly bounded?: false },
    ): Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error> {
      return streamEffect(input).pipe(
        Stream.mapError(
          (cause) =>
            new EventSinkStreamError({
              ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
              ...(input?.afterSequence === undefined ? {} : { afterSequence: input.afterSequence }),
              cause,
            }),
        ),
      );
    }

    return EventSinkV2.of({
      readApplicationBirthRecord: (threadId) =>
        sql
          .withTransaction(
            readApplicationBirthRecord(threadId).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          )
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      commitLegacyPreflight,
      write: (input) =>
        writeEffect({ ...input, effects: [] }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeWithEffects: (input) =>
        writeEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfRunCurrent: (input) =>
        writeIfRunCurrentEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadOwner: (input) =>
        writeIfProviderThreadOwnerEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      commitCommand: (input) =>
        commitCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: input.events.length,
                cause,
              }),
          ),
        ),
      commitRejectedCommand: (input) =>
        commitRejectedCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      commitProjectCommand: (input) =>
        commitProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 1, cause }),
          ),
        ),
      commitRejectedProjectCommand: (input) =>
        commitRejectedProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 0, cause }),
          ),
        ),
      stream,
      latestSequence: (input) =>
        eventStore.latestSequence(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkStreamError({
                ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
                cause,
              }),
          ),
        ),
      canPromoteQueuedAtToolBoundary: (input) =>
        Effect.gen(function* () {
          if (input.boundary.event.type !== "node.updated") return false;
          const node = input.boundary.event.payload;
          const births = input.births ?? new Map();
          for (const runId of [input.activeRunId, input.queuedRunId]) {
            if (births.has(runId)) continue;
            const rows = yield* sql<{ sequence: number; eligible: number }>`SELECT sequence,
              CASE WHEN json_type(payload_json, '$.queuedToolBoundaryEligible') = 'true' THEN 1 ELSE 0 END AS eligible FROM orchestration_events
            WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
              AND event_type = 'run.created' AND json_extract(payload_json, '$.id') = ${runId}
            ORDER BY sequence DESC LIMIT 1`;
            if (rows[0] === undefined) {
              births.set(runId, null);
              continue;
            }
            const settings = yield* sql<{ runtime_mode: string; interaction_mode: string }>`SELECT
            json_extract(payload_json, '$.runtimeMode') AS runtime_mode, json_extract(payload_json, '$.interactionMode') AS interaction_mode
            FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
              AND event_type LIKE 'thread.%' AND sequence < ${rows[0].sequence} AND json_type(payload_json, '$.runtimeMode') = 'text'
            ORDER BY sequence DESC LIMIT 1`;
            births.set(
              runId,
              settings[0] === undefined
                ? null
                : {
                    sequence: rows[0].sequence,
                    queuedToolBoundaryEligible: rows[0].eligible === 1,
                    runtimeMode: settings[0].runtime_mode,
                    interactionMode: settings[0].interaction_mode,
                  },
            );
          }
          const queued = births.get(input.queuedRunId);
          const active = births.get(input.activeRunId);
          if (
            queued == null ||
            queued.queuedToolBoundaryEligible !== true ||
            active == null ||
            queued.sequence >= input.boundary.sequence ||
            queued.runtimeMode !== input.runtimeMode ||
            queued.interactionMode !== input.interactionMode ||
            active.runtimeMode !== input.runtimeMode ||
            active.interactionMode !== input.interactionMode
          )
            return false;
          const updates = yield* sql<{ sequence: number }>`SELECT sequence FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
            AND sequence > ${active.sequence} AND event_type IN ('message.created', 'message.updated') AND json_extract(payload_json, '$.id') = ${input.messageId}
          ORDER BY sequence DESC LIMIT 1`;
          if (updates[0] === undefined || updates[0].sequence >= input.boundary.sequence)
            return false;
          const completions = yield* sql<{
            sequence: number;
          }>`SELECT sequence FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
            AND sequence > ${active.sequence} AND event_type = 'node.updated' AND json_extract(payload_json, '$.id') = ${node.id}
            AND json_extract(payload_json, '$.providerTurnId') = ${node.providerTurnId}
            AND json_extract(payload_json, '$.status') = 'completed' ORDER BY sequence ASC LIMIT 1`;
          if (completions[0]?.sequence !== input.boundary.sequence) return false;
          const fences = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
          WHERE thread_id = ${input.threadId} AND run_id = ${input.queuedRunId}
            AND json_extract(payload_json, '$.queueHeld') = 1 LIMIT 1`;
          return fences.length === 0;
        }).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readByCommandId: (input) =>
        eventStore.readByCommandId(input).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                cause,
              }),
          ),
        ),
    } satisfies EventSinkV2Shape);
  }),
);

/**
 * Event sink layer for application compositions that already own the
 * persistence services. Keeping the outbox instance shared with the worker is
 * important because enqueue notifications are in-memory wakeups backed by the
 * durable SQL queue.
 */
export const layerFromStores = baseLayer;

export const layer: Layer.Layer<
  EventSinkV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = baseLayer.pipe(
  Layer.provide(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
    ),
  ),
);
