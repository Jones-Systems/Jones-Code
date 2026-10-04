import * as NativeCreationRepositoryLayer from "../../persistence/Layers/NativeCreationRepository.ts";
import {
  nativeCreationCommandDigest,
  nativeCreationCanonicalJson,
} from "../NativeCreationPreparation.ts";
import type {
  OrchestrationClientOrigin,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { CommandId, OrchestrationCommand } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  isOrchestrationCommandRejection,
  OrchestrationCommandIdConflictError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  WorktreeOwnershipConflictError,
  type OrchestrationDispatchError,
  type OrchestrationProjectorDecodeError,
} from "../Errors.ts";
import { makeDispatchGuard } from "../DispatchGuard.ts";
import { makeCommandObservationQuery } from "../CommandObservation.ts";
import { decideOrchestrationCommand } from "../decider.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ThreadBackgroundLivenessService } from "../ThreadBackgroundLiveness.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
import {
  makeWorktreeOwnershipLeaseStore,
  WORKTREE_OWNERSHIP_LEASE_DURATION_MS,
  WORKTREE_OWNERSHIP_LEASE_RENEW_INTERVAL_MS,
  type WorktreeOwnershipLease,
} from "../WorktreeOwnershipLease.ts";
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isOrchestrationCommandIdConflictError = Schema.is(OrchestrationCommandIdConflictError);

interface CommandEnvelope {
  command: OrchestrationCommand;
  origin: OrchestrationClientOrigin | undefined;
  bootstrapEffect: { readonly claimId: string; readonly effectId: string } | undefined;
  result: Deferred.Deferred<{ sequence: number }, OrchestrationDispatchError>;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const threadBackgroundLiveness = yield* ThreadBackgroundLivenessService;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const worktreeOwnershipLeases = yield* makeWorktreeOwnershipLeaseStore();
  const locallyOwnedWorktrees = new Map<string, WorktreeOwnershipLease>();
  const validateDispatchGuard = yield* makeDispatchGuard();
  const nativeCreationRepository = yield* NativeCreationRepositoryLayer.make;
  const commandObservation = yield* makeCommandObservationQuery();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

  const ownershipTargetForThread = (threadId: ThreadId) => {
    const thread = commandReadModel.threads.find((candidate) => candidate.id === threadId);
    if (thread === undefined) return null;
    const project = commandReadModel.projects.find(
      (candidate) => candidate.id === thread.projectId,
    );
    if (project === undefined) return null;
    return {
      resourcePath: path.resolve(thread.worktreePath ?? project.workspaceRoot),
      ownerThreadId: thread.id,
      branch: thread.branch,
    } as const;
  };

  const prepareOwnershipRecord = Effect.fn("prepareOwnershipRecord")(function* (
    target: NonNullable<ReturnType<typeof ownershipTargetForThread>>,
  ) {
    const resourcePath = yield* fileSystem
      .realPath(target.resourcePath)
      .pipe(Effect.orElseSucceed(() => target.resourcePath));
    const leaseId = yield* crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new OrchestrationCommandInvariantError({
            commandType: "worktree.ownership.acquire",
            detail: "failed to generate a lease identifier",
            cause,
          }),
      ),
    );
    return { ...target, resourcePath, leaseId };
  });

  const acquireOwnershipRecord = Effect.fn("acquireOwnershipRecord")(function* (
    prepared: Effect.Success<ReturnType<typeof prepareOwnershipRecord>>,
  ) {
    const nowMs = yield* Clock.currentTimeMillis;
    // This write must be the transaction's first database access. It selects
    // the authoritative incarnation after reserving the SQLite writer.
    const acquired = yield* worktreeOwnershipLeases.acquire({
      ...prepared,
      nowMs,
      expiresAtMs: nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS,
    });
    if (Option.isSome(acquired)) return acquired.value;

    const ownerIncarnation = yield* worktreeOwnershipLeases.getThreadIncarnation(
      prepared.ownerThreadId,
    );
    if (Option.isNone(ownerIncarnation)) {
      return yield* new OrchestrationCommandInvariantError({
        commandType: "worktree.ownership.acquire",
        detail: `thread '${prepared.ownerThreadId}' has no authoritative creation event`,
      });
    }
    const conflictingLease = (yield* worktreeOwnershipLeases.listAll()).find(
      (lease) => lease.resourcePath === prepared.resourcePath,
    );
    if (conflictingLease === undefined) {
      return yield* new OrchestrationCommandInvariantError({
        commandType: "worktree.ownership.acquire",
        detail: `failed to acquire ownership for '${prepared.resourcePath}'`,
      });
    }
    return yield* new WorktreeOwnershipConflictError({
      resourcePath: conflictingLease.resourcePath,
      ownerThreadId: conflictingLease.ownerThreadId,
      requestingThreadId: prepared.ownerThreadId,
      ownerBranch: conflictingLease.branch,
      expiresAtMs: conflictingLease.expiresAtMs,
    });
  });

  const renewLocallyOwnedWorktrees = Effect.gen(function* () {
    const nowMs = yield* Clock.currentTimeMillis;
    for (const [resourcePath, lease] of locallyOwnedWorktrees) {
      const renewed = yield* worktreeOwnershipLeases
        .renew({
          resourcePath,
          leaseId: lease.leaseId,
          ownerThreadId: lease.ownerThreadId,
          ownerIncarnation: lease.ownerIncarnation,
          nowMs,
          expiresAtMs: nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS,
        })
        .pipe(
          Effect.catch((cause) =>
            Effect.logWarning("failed to renew worktree ownership lease", {
              resourcePath,
              ownerThreadId: lease.ownerThreadId,
              cause,
            }).pipe(Effect.as(true)),
          ),
        );
      if (!renewed) {
        locallyOwnedWorktrees.delete(resourcePath);
        yield* Effect.logWarning("worktree ownership lease was lost", {
          resourcePath,
          ownerThreadId: lease.ownerThreadId,
        });
        yield* Effect.gen(function* () {
          const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
          const createdAt = yield* nowIso;
          yield* Queue.offer(commandQueue, {
            command: {
              type: "thread.session.stop",
              commandId: CommandId.make(`server:worktree-lease-lost:${yield* crypto.randomUUIDv4}`),
              threadId: lease.ownerThreadId,
              createdAt,
            },
            origin: undefined,
            bootstrapEffect: undefined,
            result,
            startedAtMs: nowMs,
          });
          yield* Deferred.await(result);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("failed to stop session after worktree ownership loss", {
              resourcePath,
              ownerThreadId: lease.ownerThreadId,
              cause,
            }),
          ),
        );
      }
    }
  });

  yield* Effect.forkScoped(
    Effect.forever(
      Effect.sleep(Duration.millis(WORKTREE_OWNERSHIP_LEASE_RENEW_INTERVAL_MS)).pipe(
        Effect.andThen(renewLocallyOwnedWorktrees),
      ),
    ),
  );

  const projectEventsOntoReadModel = (
    baseReadModel: OrchestrationReadModel,
    events: ReadonlyArray<OrchestrationEvent>,
  ): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError, never> =>
    Effect.gen(function* () {
      let nextReadModel = baseReadModel;
      for (const event of events) {
        nextReadModel = yield* projectEvent(nextReadModel, event);
      }
      return nextReadModel;
    });

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const dispatchStartSequence = commandReadModel.snapshotSequence;
    // Events this dispatch appended. Reconcile republishes only these: a
    // shared state directory can contain events another server already
    // handled, and republishing them starts a second provider turn.
    const appendedEventIds = new Set<OrchestrationEvent["eventId"]>();
    let processingStartedAtMs = 0;
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      commandReadModel = yield* projectEventsOntoReadModel(commandReadModel, persistedEvents);

      for (const persistedEvent of persistedEvents) {
        if (appendedEventIds.has(persistedEvent.eventId)) {
          yield* PubSub.publish(eventPubSub, persistedEvent);
        }
      }
    });

    return Effect.exit(
      Effect.gen(function* () {
        processingStartedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const identity = yield* nativeCreationRepository
          .getReservedCommandIdentity(envelope.command.commandId)
          .pipe(
            Effect.mapError(
              (cause) =>
                new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "native creation identity lookup unavailable",
                  cause,
                }),
            ),
          );
        if (
          Option.isSome(identity) &&
          (envelope.bootstrapEffect === undefined ||
            identity.value.claimId !== envelope.bootstrapEffect.claimId ||
            identity.value.threadId !== aggregateRef.aggregateId)
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: "native creation command identity context mismatch",
          });
        }
        const reservation = yield* nativeCreationRepository
          .getReservedCommand(envelope.command.commandId)
          .pipe(
            Effect.mapError(
              (cause) =>
                new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "native creation reservation unavailable",
                  cause,
                }),
            ),
          );
        if (Option.isSome(reservation) || envelope.bootstrapEffect !== undefined) {
          if (
            Option.isNone(reservation) ||
            envelope.bootstrapEffect === undefined ||
            reservation.value.claimId !== envelope.bootstrapEffect.claimId ||
            reservation.value.commandDigest !== nativeCreationCommandDigest(envelope.command) ||
            reservation.value.canonicalCommand !== nativeCreationCanonicalJson(envelope.command)
          ) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: "native creation command context mismatch",
            });
          }
        }
        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        if (Option.isSome(existingReceipt)) {
          // A receipt only proves this exact command was handled. Replaying it
          // for a command aimed at another aggregate would report success for
          // work that never happened.
          if (
            existingReceipt.value.aggregateKind !== aggregateRef.aggregateKind ||
            existingReceipt.value.aggregateId !== aggregateRef.aggregateId
          ) {
            return yield* new OrchestrationCommandIdConflictError({
              commandId: envelope.command.commandId,
              receiptAggregateKind: existingReceipt.value.aggregateKind,
              receiptAggregateId: existingReceipt.value.aggregateId,
              commandAggregateKind: aggregateRef.aggregateKind,
              commandAggregateId: aggregateRef.aggregateId,
            });
          }
          if (existingReceipt.value.status === "accepted") {
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        yield* validateDispatchGuard(envelope.command);

        if (
          envelope.command.type === "thread.auto-settle" &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: envelope.command.threadId,
            sequenceExclusive: envelope.command.snapshotSequence,
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} changed before automatic settlement`,
          });
        }

        // The decider compares the lookup inputs. Only recreation needs an
        // event check, since it can reset a thread to the same field values.
        if (
          envelope.command.type === "thread.pull-request.sync" &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: envelope.command.threadId,
            sequenceExclusive: envelope.command.snapshotSequence,
            type: "thread.created",
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} was recreated before pull request discovery`,
          });
        }

        if (
          envelope.command.type === "thread.auto-settle" &&
          threadBackgroundLiveness.getThreadBackgroundLiveness(envelope.command.threadId) !== null
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} has live background work`,
          });
        }

        // New and moved projects do not carry a resolved identity in the event-derived
        // command model. Legacy PR edits need it to identify the link they replace.
        if (
          envelope.command.type === "thread.meta.update" &&
          envelope.command.linkedPullRequest !== undefined
        ) {
          const threadId = envelope.command.threadId;
          const thread = commandReadModel.threads.find((thread) => thread.id === threadId);
          if (thread !== undefined) {
            const project = yield* projectionSnapshotQuery.getProjectShellById(thread.projectId);
            if (Option.isSome(project)) {
              commandReadModel = {
                ...commandReadModel,
                projects: commandReadModel.projects.map((entry) =>
                  entry.id === thread.projectId
                    ? { ...entry, repositoryIdentity: project.value.repositoryIdentity }
                    : entry,
                ),
              };
            }
          }
        }

        // Command snapshots omit activities at startup and cap them while running.
        // Read this request's durable state before deciding how to send the answer.
        const userInputActivity =
          envelope.command.type === "thread.user-input.respond" ||
          envelope.command.type === "thread.user-input.dismiss"
            ? yield* projectionSnapshotQuery.getUserInputActivity(envelope.command)
            : Option.none();
        const ownershipTarget = (() => {
          if (
            envelope.command.type !== "thread.turn.start" &&
            envelope.command.type !== "thread.checkpoint.revert"
          ) {
            return null;
          }
          return ownershipTargetForThread(envelope.command.threadId);
        })();
        const eventBase = yield* decideOrchestrationCommand({
          command: envelope.command,
          readModel: commandReadModel,
          ...(Option.isSome(userInputActivity)
            ? { userInputActivity: userInputActivity.value }
            : {}),
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            isOrchestrationCommandRejection(cause)
              ? cause
              : new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Failed to generate an event identifier.",
                  cause,
                }),
          ),
        );
        const plannedEvents = Array.isArray(eventBase) ? eventBase : [eventBase];
        // Stamp the dispatching client's origin onto every event the command
        // produced. The decider stays pure; attribution is an engine concern.
        const eventBases =
          envelope.origin === undefined
            ? plannedEvents
            : plannedEvents.map((planned) => ({
                ...planned,
                metadata: { ...planned.metadata, origin: envelope.origin },
              }));
        const preparedOwnershipTarget =
          ownershipTarget === null ? null : yield* prepareOwnershipRecord(ownershipTarget);
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const committedEvents: OrchestrationEvent[] = [];
              const attachmentCleanups: Effect.Effect<void>[] = [];
              let nextCommandReadModel = commandReadModel;
              const acquiredLease =
                preparedOwnershipTarget === null
                  ? null
                  : yield* acquireOwnershipRecord(preparedOwnershipTarget);

              // Guarded cleanup names one creation event. Recheck it inside the
              // write transaction so cleanup cannot delete a replacement thread.
              if (
                envelope.bootstrapEffect !== undefined &&
                envelope.command.type === "thread.delete"
              ) {
                const cleanupCommand = envelope.command;
                const history = yield* nativeCreationRepository.readHistoryByClaim(
                  envelope.bootstrapEffect.claimId,
                );
                const commandStart = history.effects.find(
                  (fact) =>
                    fact.effectId === envelope.bootstrapEffect!.effectId &&
                    fact.phase === "started",
                );
                if (
                  commandStart?.kind !== "native_command" ||
                  commandStart.phase !== "started" ||
                  commandStart.commandType !== cleanupCommand.type ||
                  commandStart.commandId !== cleanupCommand.commandId ||
                  commandStart.threadId !== cleanupCommand.threadId ||
                  history.intent.threadId !== cleanupCommand.threadId
                ) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: cleanupCommand.type,
                    detail: "native creation cleanup command context mismatch",
                  });
                }
                const cleanupStart = history.effects
                  .filter(
                    (fact) =>
                      fact.kind === "cleanup" &&
                      fact.phase === "started" &&
                      fact.resource.kind === "thread" &&
                      fact.resource.threadId === cleanupCommand.threadId &&
                      fact.ordinal < commandStart.ordinal &&
                      !history.effects.some(
                        (completion) =>
                          completion.effectId === fact.effectId && completion.phase === "completed",
                      ),
                  )
                  .at(-1);
                if (
                  cleanupStart?.kind !== "cleanup" ||
                  cleanupStart.phase !== "started" ||
                  cleanupStart.resource.kind !== "thread"
                ) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: cleanupCommand.type,
                    detail: "native creation thread cleanup authorization unavailable",
                  });
                }
                const incarnation = cleanupStart.resource.incarnation;
                const created = history.effects.some(
                  (fact) =>
                    fact.kind === "native_command" &&
                    fact.phase === "completed" &&
                    fact.commandType === "thread.create" &&
                    fact.threadId === cleanupCommand.threadId &&
                    fact.eventId === incarnation.eventId &&
                    fact.sequence === incarnation.sequence,
                );
                const currentRows = yield* sql<{ eventId: string; sequence: number }>`
                  SELECT event_id AS "eventId", sequence FROM orchestration_events
                  WHERE aggregate_kind = 'thread' AND stream_id = ${cleanupCommand.threadId} AND event_type = 'thread.created'
                  ORDER BY sequence DESC LIMIT 1
                `;
                const current = currentRows[0];
                if (
                  !created ||
                  current?.eventId !== incarnation.eventId ||
                  current.sequence !== incarnation.sequence
                ) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: cleanupCommand.type,
                    detail: "native creation cleanup thread incarnation changed",
                  });
                }
              }

              for (const nextEvent of eventBases) {
                const savedEvent = yield* eventStore.append(nextEvent);
                appendedEventIds.add(savedEvent.eventId);
                nextCommandReadModel = yield* projectEvent(nextCommandReadModel, savedEvent);
                const cleanup = yield* projectionPipeline.projectEventDeferred(savedEvent);
                attachmentCleanups.push(cleanup);
                committedEvents.push(savedEvent);
              }

              const lastSavedEvent = committedEvents.at(-1) ?? null;
              if (lastSavedEvent === null) {
                return yield* new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Command produced no events.",
                });
              }

              yield* commandReceiptRepository.upsert({
                commandId: envelope.command.commandId,
                aggregateKind: lastSavedEvent.aggregateKind,
                aggregateId: lastSavedEvent.aggregateId,
                acceptedAt: lastSavedEvent.occurredAt,
                resultSequence: lastSavedEvent.sequence,
                status: "accepted",
                error: null,
              });

              if (envelope.bootstrapEffect !== undefined && Option.isSome(reservation)) {
                const history = yield* nativeCreationRepository.readHistoryByClaim(
                  envelope.bootstrapEffect.claimId,
                );
                const start = history.effects.find(
                  (fact) =>
                    fact.effectId === envelope.bootstrapEffect!.effectId &&
                    fact.phase === "started",
                );
                if (
                  start?.kind !== "native_command" ||
                  start.commandId !== envelope.command.commandId ||
                  start.commandDigest !== reservation.value.commandDigest
                ) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: envelope.command.type,
                    detail: "native creation effect context mismatch",
                  });
                }
                yield* nativeCreationRepository.completeEffect(envelope.bootstrapEffect.claimId, {
                  ...start,
                  phase: "completed",
                  timestamp: lastSavedEvent.occurredAt,
                  eventId: lastSavedEvent.eventId,
                  sequence: lastSavedEvent.sequence,
                });
              }
              return {
                committedEvents,
                attachmentCleanups,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
                acquiredLease,
              } as const;
            }),
          )
          .pipe(
            Effect.catchTag("NativeCreationRepositoryError", (cause) =>
              Effect.fail(
                new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "native creation transaction failed",
                  cause,
                }),
              ),
            ),
            Effect.catchTag("SqlError", (sqlError) =>
              Effect.fail(
                toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
              ),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        if (committedCommand.acquiredLease !== null) {
          locallyOwnedWorktrees.set(
            committedCommand.acquiredLease.resourcePath,
            committedCommand.acquiredLease,
          );
        }
        for (const cleanup of committedCommand.attachmentCleanups) {
          yield* cleanup;
        }
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(Effect.withSpan(`orchestration.command.${envelope.command.type}`)),
    ).pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            yield* Deferred.succeed(envelope.result, exit.value);
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (
            !isOrchestrationCommandPreviouslyRejectedError(error) &&
            !isOrchestrationCommandIdConflictError(error)
          ) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: commandReadModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (isOrchestrationCommandRejection(error)) {
              yield* commandReceiptRepository
                .upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: yield* nowIso,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.ignore);
            }
          }

          yield* Deferred.fail(envelope.result, error);
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();

  const worker = Effect.forever(Queue.take(commandQueue).pipe(Effect.flatMap(processEnvelope)));
  yield* Effect.forkScoped(worker);
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: commandReadModel.snapshotSequence }),
  );

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive, limit) =>
    eventStore.readFromSequence(fromSequenceExclusive, limit);

  const readThreadEvents: OrchestrationEngineShape["readThreadEvents"] = ({ threadId, ...range }) =>
    eventStore.readAggregateRange({ ...range, aggregateKind: "thread", aggregateId: threadId });

  const getThreadReplayStats: OrchestrationEngineShape["getThreadReplayStats"] = ({
    threadId,
    ...range
  }) =>
    eventStore.getAggregateReplayStats({
      ...range,
      aggregateKind: "thread",
      aggregateId: threadId,
    });

  const dispatch: OrchestrationEngineShape["dispatch"] = (command, options) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      yield* Queue.offer(commandQueue, {
        command,
        origin: options?.origin,
        bootstrapEffect: options?.bootstrapEffect,
        result,
        startedAtMs: yield* Clock.currentTimeMillis,
      });
      return yield* Deferred.await(result);
    });

  const acquireWorktreeOwnership: OrchestrationEngineShape["acquireWorktreeOwnership"] = (
    threadId,
    requestedPath,
  ) =>
    Effect.gen(function* () {
      const target = ownershipTargetForThread(threadId);
      if (target === null) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: "worktree.ownership.acquire",
          detail: `thread '${threadId}' has no projected checkout`,
        });
      }
      const resourcePath = yield* fileSystem
        .realPath(target.resourcePath)
        .pipe(Effect.orElseSucceed(() => target.resourcePath));
      if (requestedPath !== undefined) {
        const resolvedRequestedPath = path.resolve(requestedPath);
        const canonicalRequestedPath = yield* fileSystem
          .realPath(resolvedRequestedPath)
          .pipe(Effect.orElseSucceed(() => resolvedRequestedPath));
        const relativeRequestedPath = path.relative(resourcePath, canonicalRequestedPath);
        if (
          relativeRequestedPath === ".." ||
          relativeRequestedPath.startsWith(`..${path.sep}`) ||
          path.isAbsolute(relativeRequestedPath)
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: "worktree.ownership.acquire",
            detail: `requested mutation path '${canonicalRequestedPath}' is outside thread '${threadId}' checkout '${resourcePath}'`,
          });
        }
      }
      const prepared = yield* prepareOwnershipRecord({ ...target, resourcePath });
      const lease = yield* sql.withTransaction(acquireOwnershipRecord(prepared));
      locallyOwnedWorktrees.set(lease.resourcePath, lease);
      return lease;
    }).pipe(
      Effect.catchTag("SqlError", (sqlError) =>
        Effect.fail(
          toPersistenceSqlError("OrchestrationEngine.acquireWorktreeOwnership:transaction")(
            sqlError,
          ),
        ),
      ),
    );

  const releaseWorktreeOwnership: OrchestrationEngineShape["releaseWorktreeOwnership"] = (lease) =>
    Effect.gen(function* () {
      yield* sql.withTransaction(worktreeOwnershipLeases.release(lease));
      if (locallyOwnedWorktrees.get(lease.resourcePath)?.leaseId === lease.leaseId) {
        locallyOwnedWorktrees.delete(lease.resourcePath);
      }
    }).pipe(
      Effect.catchTag("SqlError", (sqlError) =>
        Effect.fail(
          toPersistenceSqlError("OrchestrationEngine.releaseWorktreeOwnership:transaction")(
            sqlError,
          ),
        ),
      ),
    );

  return {
    readEvents,
    readThreadEvents,
    getThreadReplayStats,
    dispatch,
    acquireWorktreeOwnership,
    releaseWorktreeOwnership,
    getThreadOwnershipIncarnation: worktreeOwnershipLeases.getThreadIncarnation,
    observeCommand: commandObservation.observe,
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
    // The command read model's snapshotSequence tracks the latest committed
    // event sequence (updated on the worker fiber). A plain property read is a
    // consistent, committed value — reassignment of `commandReadModel` is
    // atomic on the single-threaded event loop.
    latestSequence: Effect.sync(() => commandReadModel.snapshotSequence),
    listWorktreeOwnershipLeases: worktreeOwnershipLeases.listAll(),
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
