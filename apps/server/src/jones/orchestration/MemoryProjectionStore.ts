import * as Schema from "effect/Schema";
import {
  idleCompactionOf,
  OrchestrationV2DomainEvent,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  RunId,
} from "@t3tools/contracts";
import {
  RecordedAppThread,
  RecordedRun,
  RecordedLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as LegacyIntegrity from "../legacyBootstrap/Integrity.ts";
import * as LegacyMetadata from "../legacyBootstrap/RecordedMetadata.ts";
import {
  LegacyStorageError,
  type ThreadMetadata,
  type RunMetadata,
} from "../legacyBootstrap/StorageSchema.ts";

export interface LegacyMemoryRecords {
  readonly thread: (
    thread: OrchestrationV2AppThread,
  ) => Effect.Effect<RecordedAppThread, LegacyStorageError>;
  readonly run: (run: OrchestrationV2Run) => Effect.Effect<RecordedRun, LegacyStorageError>;
}
import type {
  ProjectionStoreV2Shape,
  ProjectionOperatingCountsCandidate,
  ProjectionRecoveryKind,
  ProjectionThreadPullRequests,
  ProjectionRecords,
  ProjectionCheckpointRollbackContext,
  ProjectionStoreApplyEventError as ApplyError,
  ProjectionStoreThreadNotFoundError as MissingThreadError,
} from "../../orchestration-v2/ProjectionStore.ts";
import { projectTurnItemForWire } from "../../orchestration-v2/WireProjection.ts";
import * as Stream from "effect/Stream";
import { searchableMessageSegments, searchablePlanSegments } from "@t3tools/shared/threadFindText";
import { countThreadSearchOccurrences } from "@t3tools/shared/threadSearch";
import { htmlRenderFromToolItem, mcpAppFromToolItem } from "@t3tools/shared/toolOutput";
import {
  type OrchestrationV2DomainEvent as OrchestrationV2RecordedEvent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2SearchThreadInput,
  type OrchestrationV2SearchThreadResult,
} from "@t3tools/contracts";
import { latestUnheldRun } from "@t3tools/shared/orchestrationV2ThreadError";
import {
  ThreadId,
  ProjectId,
  type ProviderSessionId,
  type ProviderThreadId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import { threadHtmlRenderAttachmentIds } from "../../attachmentStore.ts";
import { threadPullRequestsOf } from "../../sourceControl/threadPullRequests.ts";
import {
  isConversationHistoryItem,
  missingInterruptRequests,
  selectHistoryPageFromCursor,
  deferWorkHistoryRows,
  selectDeferredWorkPage,
  THREAD_HISTORY_PAGE_POLICY,
  isThreadHistoryUserTurn,
  THREAD_HISTORY_MAX_RAW_TURNS,
  THREAD_HISTORY_TURN_SNAPSHOT_ROW_LIMIT,
} from "../../orchestration-v2/threadHistoryPaging.ts";

const decodeRecordedLifecycleEvent = Schema.decodeSync(RecordedLifecycleEvent);
const decodePublicDomainEvent = Schema.decodeSync(OrchestrationV2DomainEvent);
const decodeRecordedThread = Schema.decodeEffect(RecordedAppThread);
const decodeRecordedRun = Schema.decodeEffect(RecordedRun);

interface FindDocument {
  readonly entryId: string;
  readonly runId: OrchestrationV2ProjectedTurnItem["item"]["runId"];
  readonly count: number;
}

function selectMatch(
  documents: readonly FindDocument[],
  input: Pick<OrchestrationV2SearchThreadInput, "index" | "start" | "offset">,
) {
  const totalMatches = documents.reduce((sum, doc) => sum + doc.count, 0);
  let requestedIndex = input.index ?? 0;
  if (input.index === undefined && input.start) {
    const startIndex = documents.findIndex((doc) => doc.entryId === input.start?.entryId);
    if (startIndex >= 0) {
      requestedIndex =
        documents.slice(0, startIndex).reduce((sum, doc) => sum + doc.count, 0) +
        Math.min(input.start.occurrence, documents[startIndex]!.count);
      if (requestedIndex >= totalMatches) requestedIndex = 0;
    }
  }
  const activeIndex =
    input.index === undefined && totalMatches > 0
      ? (((requestedIndex + (input.offset ?? 0)) % totalMatches) + totalMatches) % totalMatches
      : Math.min(requestedIndex, Math.max(0, totalMatches - 1));
  let occurrence = activeIndex;
  for (const document of documents) {
    if (occurrence < document.count) return { totalMatches, activeIndex, document, occurrence };
    occurrence -= document.count;
  }
  return { totalMatches, activeIndex, document: null, occurrence: 0 };
}

function findProjectedThreadItems(
  items: readonly OrchestrationV2ProjectedTurnItem[],
  input: OrchestrationV2SearchThreadInput,
  snapshotSequence: number,
  cwd?: string,
): OrchestrationV2SearchThreadResult {
  const foldedAnswers = new Map<string, FindDocument["runId"]>(
    items.flatMap(({ item }) =>
      item.type === "user_input_request" && item.questionAnswer
        ? [[`async-answer:${item.questionAnswer.requestId}`, item.runId] as const]
        : [],
    ),
  );
  const embeddedResultRunIds = new Set(
    items.flatMap(({ item }) =>
      item.type === "dynamic_tool" &&
      ((item.status === "completed" && htmlRenderFromToolItem(item)) || mcpAppFromToolItem(item))
        ? [item.runId]
        : [],
    ),
  );
  const docs = items
    .filter(
      ({ item }) =>
        ["user_message", "assistant_message", "proposed_plan"].includes(item.type) &&
        !(item.type === "user_message" && foldedAnswers.get(item.messageId) === item.runId),
    )
    .map((row) => {
      const item = row.item;
      const segments =
        item.type === "proposed_plan"
          ? searchablePlanSegments(item.markdown, cwd)
          : item.type === "user_message" || item.type === "assistant_message"
            ? item.type === "assistant_message" &&
              item.text === "" &&
              embeddedResultRunIds.has(item.runId)
              ? []
              : (searchableMessageSegments(
                  {
                    role: item.type === "user_message" ? "user" : "assistant",
                    text: item.text,
                    streaming: item.type === "assistant_message" && item.streaming,
                    context: item.type === "user_message" ? item.context : undefined,
                  },
                  cwd,
                  input.skills ?? [],
                ) ?? [])
            : [];
      return {
        entryId:
          item.type === "user_message" || item.type === "assistant_message"
            ? item.messageId
            : item.id,
        runId: item.runId,
        count: segments.reduce(
          (sum, text) => sum + countThreadSearchOccurrences(text, input.query),
          0,
        ),
      };
    });
  return resultForSelection(selectMatch(docs, input), docs, snapshotSequence);
}

function resultForSelection(
  selected: ReturnType<typeof selectMatch>,
  documents: readonly FindDocument[],
  snapshotSequence: number,
): OrchestrationV2SearchThreadResult {
  let startIndex = 0;
  const entries = documents.flatMap(({ entryId, runId, count }) => {
    const entry = { entryId, runId, count, startIndex };
    startIndex += count;
    return count > 0 ? [entry] : [];
  });
  const selectedEntry = entries.findIndex((entry) => entry.entryId === selected.document?.entryId);
  const navigation =
    entries.length <= 17
      ? entries
      : Array.from(
          { length: 17 },
          (_, i) => entries[(selectedEntry + i - 8 + entries.length) % entries.length]!,
        );
  return {
    snapshotSequence,
    totalMatches: selected.totalMatches,
    activeIndex: selected.activeIndex,
    navigation,
    match:
      selected.document === null
        ? null
        : {
            entryId: selected.document.entryId,
            runId: selected.document.runId,
            occurrence: selected.occurrence,
          },
  };
}

type ProjectionNode = OrchestrationV2ThreadProjection["nodes"][number];
type ProjectionRun = OrchestrationV2ThreadProjection["runs"][number];
interface ProjectionReplayState {
  readonly projections: Map<ThreadId, OrchestrationV2ThreadProjection>;
  readonly legacyThreads: Map<
    ThreadId,
    { readonly projectId: ProjectId; readonly createdAt: string; readonly metadata: ThreadMetadata }
  >;
  readonly legacyRuns: Map<
    RunId,
    {
      readonly threadId: ThreadId;
      readonly userMessageId: string;
      readonly requestedAt: string;
      readonly metadata: RunMetadata;
    }
  >;

  readonly providerSessionThreadIds: Map<ProviderSessionId, ReadonlySet<ThreadId>>;
  readonly providerThreadInstanceIds: Map<ProviderThreadId, ProviderInstanceId>;
}

export const makeMemoryLayer = (
  internals: typeof import("../../orchestration-v2/ProjectionStore.ts").memoryProjectionInternals,
) => {
  const {
    ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
    ProjectionStoreApplyEventError,
    ProjectionStoreReadError,
    ProjectionStoreThreadNotFoundError,
    ProjectionStoreV2,
    applyToProjection,
    buildVisibleTurnItems,
    emptyProjection,
    inheritsForkConversation,
    isActivityRunForShell,
    threadShellFromProjection,
    upsertById,
    withLocalVisibleTurnItems,
  } = internals;
  const activeRunStatuses: ReadonlySet<ProjectionRun["status"]> = new Set([
    "preparing",
    "starting",
    "running",
    "waiting",
  ]);
  const settledLeafNodeKinds: ReadonlySet<ProjectionNode["kind"]> = new Set([
    "tool_call",
    "assistant_message",
    "reasoning",
  ]);

  const settledNodeStatuses: ReadonlySet<ProjectionNode["status"]> = new Set([
    "completed",
    "interrupted",
    "failed",
    "cancelled",
    "rolled_back",
  ]);

  const isSettledLeafNode = (node: ProjectionNode, runs: ReadonlyArray<ProjectionRun>) =>
    settledLeafNodeKinds.has(node.kind) &&
    settledNodeStatuses.has(node.status) &&
    node.runId !== null &&
    !runs.some((run) => run.id === node.runId && activeRunStatuses.has(run.status));

  function needsRecovery(
    projection: OrchestrationV2ThreadProjection,
    kind: ProjectionRecoveryKind,
    parent?: Pick<OrchestrationV2ThreadProjection, "subagents" | "contextTransfers">,
  ): boolean {
    if (projection.thread.deletedAt !== null) return false;
    switch (kind) {
      case "self-settlement":
        return projection.thread.selfSettlement != null;
      case "queued-runs":
        return (
          projection.thread.archivedAt === null &&
          projection.runs.some((run) => run.status === "queued") &&
          !projection.runs.some((run) =>
            ["preparing", "starting", "running", "waiting"].includes(run.status),
          )
        );
      case "planned-update-queued-runs":
        return (
          projection.thread.archivedAt === null &&
          projection.runs.some((run) => run.status === "queued")
        );
      case "delegated-completions":
        return projection.runs.some((run) => run.delegatedCompletion?.delivery != null);
      case "unread-steers":
        return projection.turnItems.some(
          (item) =>
            item.type === "user_message" &&
            item.status === "pending" &&
            (item.inputIntent === "steer" || item.inputIntent === "promoted_queued_to_steer") &&
            projection.runs.some(
              (run) =>
                run.id === item.runId &&
                ["completed", "interrupted", "failed", "cancelled"].includes(run.status),
            ),
        );
      case "restart-continuations":
        return (
          projection.providerSessions.some(
            (session) => session.status !== "stopped" && session.status !== "error",
          ) ||
          projection.runs.some(
            (run) => run.status === "starting" && run.restartContinuationOfRunId !== undefined,
          )
        );
      case "subagent-results": {
        const parentThreadId = projection.thread.lineage.parentThreadId;
        return (
          projection.thread.lineage.relationshipToParent === "subagent" &&
          parentThreadId !== null &&
          projection.thread.forkedFrom?.type === "node" &&
          ((parent?.subagents ?? []).some(
            (task) =>
              task.origin === "app_owned" &&
              task.result === null &&
              task.childThreadId === projection.thread.id &&
              projection.runs.some(
                (run) =>
                  run.delegatedTaskId === task.id &&
                  ["completed", "interrupted", "failed", "cancelled", "rolled_back"].includes(
                    run.status,
                  ),
              ),
          ) ||
            (["completed", "interrupted", "failed", "cancelled", "rolled_back"].includes(
              latestUnheldRun(projection.runs)?.status ?? "idle",
            ) &&
              ![...projection.contextTransfers, ...(parent?.contextTransfers ?? [])].some(
                (transfer) =>
                  transfer.type === "subagent_result" &&
                  transfer.sourceThreadId === projection.thread.id &&
                  transfer.targetThreadId === parentThreadId,
              )))
        );
      }
      case "runtime":
        return (
          projection.runs.some(
            (run) =>
              ["preparing", "starting", "running", "waiting"].includes(run.status) ||
              (run.status === "queued" && run.queueHeld !== true),
          ) ||
          projection.runtimeRequests.some((request) => request.status === "pending") ||
          projection.subagents.some(
            (subagent) =>
              subagent.origin === "provider_native" &&
              ["pending", "running", "waiting"].includes(subagent.status),
          ) ||
          projection.providerSessions.some(
            (session) => session.status !== "stopped" && session.status !== "error",
          ) ||
          projection.providerThreads.some(
            (thread) =>
              thread.status === "active" || (thread.pendingBackgroundTasks?.length ?? 0) > 0,
          ) ||
          projection.turnItems.some(
            (item) =>
              ["command_execution", "dynamic_tool", "subagent"].includes(item.type) &&
              ["pending", "running", "waiting"].includes(item.status) &&
              !projection.runs.some((run) => run.id === item.runId && run.status === "rolled_back"),
          )
        );
    }
  }

  function makeProjectionReplayState(): ProjectionReplayState {
    return {
      projections: new Map(),
      legacyThreads: new Map(),
      legacyRuns: new Map(),
      providerSessionThreadIds: new Map(),
      providerThreadInstanceIds: new Map(),
    };
  }

  function applyToProjectionReplayState(
    state: ProjectionReplayState,
    event: OrchestrationV2RecordedEvent,
  ): boolean {
    const recorded = decodeRecordedLifecycleEvent(event);
    if (
      recorded.type === "thread.created" ||
      recorded.type === "thread.metadata-updated" ||
      recorded.type === "thread.deleted"
    ) {
      const metadata = LegacyMetadata.threadMetadata(recorded.payload);
      if (LegacyMetadata.hasMetadata(metadata))
        state.legacyThreads.set(recorded.threadId, {
          projectId: recorded.payload.projectId,
          createdAt: DateTime.formatIso(recorded.payload.createdAt),
          metadata,
        });
    }
    if (recorded.type === "run.created" || recorded.type === "run.updated") {
      if (LegacyMetadata.hasMetadata(LegacyMetadata.runMetadata(recorded.payload))) {
        const previous = state.legacyRuns.get(recorded.payload.id);
        const publicRun = state.projections
          .get(recorded.threadId)
          ?.runs.find((run) => run.id === recorded.payload.id);
        // Receiving's merge and provenance checks, against the run recorded before this event.
        const metadata = LegacyIntegrity.retainRunMetadata(
          publicRun === undefined ? undefined : { ...publicRun, ...previous?.metadata },
          recorded.payload,
          recorded,
        );
        state.legacyRuns.set(recorded.payload.id, {
          threadId: recorded.threadId,
          userMessageId: recorded.payload.userMessageId,
          requestedAt: DateTime.formatIso(recorded.payload.requestedAt),
          metadata,
        });
      }
    }
    event = decodePublicDomainEvent(event);
    if (event.type === "thread.created" && !state.projections.has(event.threadId)) {
      state.projections.set(event.threadId, emptyProjection(event));
      return true;
    }

    const current = state.projections.get(event.threadId);
    if (current === undefined) {
      return false;
    }

    let next = applyToProjection(current, event);
    if (event.type === "provider-session.updated") {
      const boundThreadIds = state.providerSessionThreadIds.get(event.payload.id);
      if (boundThreadIds?.has(event.threadId) !== true) {
        next = { ...next, providerSessions: current.providerSessions };
      }
    }
    state.projections.set(event.threadId, next);

    switch (event.type) {
      case "provider-thread.updated": {
        state.providerThreadInstanceIds.set(event.payload.id, event.payload.providerInstanceId);
        // SQL's upsert moves the provider-thread row and its roster to appThreadId.
        for (const [threadId, projection] of state.projections) {
          if (threadId === event.payload.appThreadId) {
            if (threadId !== event.threadId) {
              state.projections.set(threadId, applyToProjection(projection, event));
            }
            continue;
          }
          const providerThreads = projection.providerThreads.filter(
            (thread) => thread.id !== event.payload.id,
          );
          if (providerThreads.length !== projection.providerThreads.length) {
            state.projections.set(threadId, { ...projection, providerThreads });
          }
        }
        break;
      }
      case "provider-session.attached": {
        const boundThreadIds = new Set(state.providerSessionThreadIds.get(event.payload.id) ?? []);
        boundThreadIds.add(event.threadId);
        state.providerSessionThreadIds.set(event.payload.id, boundThreadIds);
        for (const threadId of boundThreadIds) {
          if (threadId === event.threadId) continue;
          const projection = state.projections.get(threadId);
          if (projection === undefined) continue;
          state.projections.set(threadId, {
            ...projection,
            providerSessions: upsertById(projection.providerSessions, event.payload),
          });
        }
        break;
      }
      case "provider-session.updated": {
        const boundThreadIds = state.providerSessionThreadIds.get(event.payload.id) ?? [];
        for (const threadId of boundThreadIds) {
          if (threadId === event.threadId) continue;
          const projection = state.projections.get(threadId);
          if (projection === undefined) continue;
          state.projections.set(threadId, {
            ...projection,
            providerSessions: upsertById(projection.providerSessions, event.payload),
          });
        }
        break;
      }
      case "provider-session.detached": {
        const boundThreadIds = new Set(
          state.providerSessionThreadIds.get(event.payload.providerSessionId) ?? [],
        );
        boundThreadIds.delete(event.threadId);
        if (boundThreadIds.size === 0) {
          state.providerSessionThreadIds.delete(event.payload.providerSessionId);
        } else {
          state.providerSessionThreadIds.set(event.payload.providerSessionId, boundThreadIds);
        }
        break;
      }
    }

    return true;
  }
  return Layer.effect(
    ProjectionStoreV2,
    Effect.gen(function* () {
      const replayState = yield* Ref.make(makeProjectionReplayState());
      const sequence = yield* Ref.make(0);
      const censusPermit = yield* Semaphore.make(1);

      const legacyRecords: LegacyMemoryRecords = {
        thread: (thread) =>
          Ref.get(replayState).pipe(
            Effect.flatMap((state) => {
              const retained = state.legacyThreads.get(thread.id);
              if (
                retained !== undefined &&
                (retained.projectId !== thread.projectId ||
                  retained.createdAt !== DateTime.formatIso(thread.createdAt))
              )
                return Effect.fail(
                  new LegacyStorageError({ reason: "memory_thread_incarnation_changed" }),
                );
              return decodeRecordedThread({
                ...thread,
                ...retained?.metadata,
              }).pipe(
                Effect.mapError(
                  (cause) => new LegacyStorageError({ reason: "memory_thread_decode", cause }),
                ),
              );
            }),
          ),
        run: (run) =>
          Ref.get(replayState).pipe(
            Effect.flatMap((state) => {
              const retained = state.legacyRuns.get(run.id);
              if (
                retained !== undefined &&
                (retained.threadId !== run.threadId ||
                  retained.userMessageId !== run.userMessageId ||
                  retained.requestedAt !== DateTime.formatIso(run.requestedAt))
              )
                return Effect.fail(
                  new LegacyStorageError({ reason: "memory_run_incarnation_changed" }),
                );
              return decodeRecordedRun({ ...run, ...retained?.metadata }).pipe(
                Effect.mapError(
                  (cause) => new LegacyStorageError({ reason: "memory_run_decode", cause }),
                ),
              );
            }),
          ),
      };
      const service: ProjectionStoreV2Shape = {
        legacyRecords,
        listActiveThreads: (projectId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()]
                .map(({ thread }) => thread)
                .filter(
                  (thread) =>
                    thread.deletedAt === null &&
                    thread.archivedAt === null &&
                    (projectId === undefined || thread.projectId === projectId),
                )
                .toSorted(
                  (a, b) =>
                    DateTime.toEpochMillis(a.updatedAt) - DateTime.toEpochMillis(b.updatedAt) ||
                    a.id.localeCompare(b.id),
                )
                .map((thread) => ({
                  id: thread.id,
                  projectId: thread.projectId,
                  title: thread.title,
                  lineage: thread.lineage,
                  ...(thread.delegatedFrom === undefined
                    ? {}
                    : { delegatedFrom: thread.delegatedFrom }),
                  settledOverride: thread.settledOverride,
                  settledAt: thread.settledAt,
                  snoozedUntil: thread.snoozedUntil ?? null,
                  updatedAt: thread.updatedAt,
                })),
            ),
          ),
        hasPendingDelegatedCompletion: (childThreadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()].some((projection) =>
                projection.subagents.some((task) => {
                  const owner = projection.runs.find((run) => run.id === task.runId);
                  return (
                    task.childThreadId === childThreadId &&
                    task.origin === "app_owned" &&
                    (task.result == null ||
                      (task.completionDelivery?.state === "claimed" &&
                        owner?.delegatedCompletion?.disposition === "open" &&
                        owner.delegatedCompletion.delivery != null))
                  );
                }),
              ),
            ),
          ),
        getTranscriptByteFloor: (threadId, limit) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => {
              const projection = state.projections.get(threadId);
              if (projection === undefined) return 0;
              const stringBytes = (value: unknown): number => {
                if (typeof value === "string") return Buffer.byteLength(value);
                if (value === null || typeof value !== "object") return 0;
                return Object.values(value).reduce<number>(
                  (sum, child) => sum + stringBytes(child),
                  0,
                );
              };
              const bytes = projection.turnItems.reduce(
                (sum, item) =>
                  (item.type === "command_execution" || item.type === "dynamic_tool") &&
                  !projection.runs.some(
                    (run) => run.id === item.runId && run.status === "rolled_back",
                  )
                    ? sum + stringBytes(item.output)
                    : sum,
                0,
              );
              return bytes <= limit ? 0 : bytes;
            }),
          ),
        getSubagentCount: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => state.projections.get(threadId)?.subagents.length ?? 0),
          ),
        listAppOwnedChildThreadIds: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => [
              ...new Set(
                (state.projections.get(threadId)?.subagents ?? [])
                  .filter((task) => task.origin === "app_owned" && task.childThreadId !== null)
                  .toSorted(
                    (a, b) =>
                      (a.startedAt == null ? "" : DateTime.formatIso(a.startedAt)).localeCompare(
                        b.startedAt == null ? "" : DateTime.formatIso(b.startedAt),
                      ) || a.id.localeCompare(b.id),
                  )
                  .flatMap((task) => (task.childThreadId === null ? [] : [task.childThreadId])),
              ),
            ]),
          ),
        countThreads: () =>
          Ref.get(replayState).pipe(
            Effect.map(
              (state) =>
                [...state.projections.values()].filter(({ thread }) => thread.deletedAt === null)
                  .length,
            ),
          ),
        listActiveShellIndex: () =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()]
                .filter(({ thread }) => thread.deletedAt === null && thread.archivedAt === null)
                .toSorted(
                  (a, b) =>
                    DateTime.toEpochMillis(a.thread.updatedAt) -
                      DateTime.toEpochMillis(b.thread.updatedAt) ||
                    a.thread.id.localeCompare(b.thread.id),
                )
                .map((projection) => {
                  const { thread } = projection;
                  return {
                    id: thread.id,
                    projectId: thread.projectId,
                    lineage: thread.lineage,
                    settledOverride: thread.settledOverride,
                    settledAt: thread.settledAt,
                    pinnedAt: thread.pinnedAt ?? null,
                    pinOrderKey: thread.pinOrderKey ?? null,
                    snoozedUntil: thread.snoozedUntil ?? null,
                    hasRemoteTasks: projection.subagents.some(
                      (task) =>
                        task.origin === "app_owned" &&
                        task.childThreadId === null &&
                        task.remoteChild != null,
                    ),
                    mayBeLive:
                      projection.runs.some((run) =>
                        ["queued", "preparing", "starting", "running", "waiting"].includes(
                          run.status,
                        ),
                      ) ||
                      projection.runtimeRequests.some((request) => request.status === "pending") ||
                      projection.turnItems.some(
                        (item) =>
                          (["command_execution", "dynamic_tool", "subagent"].includes(item.type) &&
                            !["completed", "interrupted", "failed", "cancelled"].includes(
                              item.status,
                            )) ||
                          (item.type === "link_request" && item.status === "waiting"),
                      ) ||
                      projection.providerThreads.some(
                        (provider) => (provider.pendingBackgroundTasks?.length ?? 0) > 0,
                      ) ||
                      (thread.guestShare?.messages.some(
                        (message) => message.status === "pending",
                      ) ??
                        false) ||
                      (thread.pullRequests?.some((link) => link.watch != null) ?? false),
                  };
                }),
            ),
          ),
        getLimitRecoveryWakeAt: (now) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => {
              const nowMs = DateTime.toEpochMillis(now);
              const wakes = [...state.projections.values()]
                .flatMap(({ thread }) =>
                  thread.deletedAt !== null || thread.limitRecovery?.autoResume !== true
                    ? []
                    : [
                        Date.parse(thread.limitRecovery.resetAt),
                        thread.snoozedUntil == null
                          ? NaN
                          : DateTime.toEpochMillis(thread.snoozedUntil),
                      ],
                )
                .filter((time) => Number.isFinite(time) && time > nowMs);
              return wakes.length === 0 ? null : Math.round(Math.min(...wakes));
            }),
          ),
        getHeldQueuedRuns: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              (state.projections.get(threadId)?.runs ?? [])
                .filter((run) => run.status === "queued" && run.queueHeld === true)
                .toSorted((a, b) => a.ordinal - b.ordinal),
            ),
          ),

        getOperatingCountsCandidates: Effect.fn("ProjectionStore.getOperatingCountsCandidates")(
          (input) =>
            Effect.gen(function* () {
              const projections = (yield* Ref.get(replayState)).projections;
              const threads = [...projections.values()]
                .filter(
                  ({ thread }) =>
                    thread.archivedAt === null &&
                    thread.deletedAt === null &&
                    (input?.projectId === undefined || thread.projectId === input.projectId),
                )
                .map((projection): ProjectionOperatingCountsCandidate => {
                  const pending = projection.runtimeRequests
                    .filter((request) => request.status === "pending")
                    .toSorted(
                      (left, right) =>
                        DateTime.toEpochMillis(right.createdAt) -
                          DateTime.toEpochMillis(left.createdAt) ||
                        String(right.id).localeCompare(String(left.id)),
                    )[0];
                  const active = projection.runs
                    .filter(isActivityRunForShell)
                    .toSorted(
                      (left, right) =>
                        right.ordinal - left.ordinal ||
                        String(right.id).localeCompare(String(left.id)),
                    )[0];
                  return {
                    id: projection.thread.id,
                    projectId: projection.thread.projectId,
                    activeProviderThreadId: projection.thread.activeProviderThreadId,
                    archivedAt: null,
                    deletedAt: null,
                    interactionMode: projection.thread.interactionMode,
                    activityRunStatus: active?.status ?? null,
                    pendingRuntimeRequest: pending === undefined ? null : { kind: pending.kind },
                    hasActionableProposedPlan: projection.plans.some(
                      (plan) => plan.kind === "proposed_plan" && plan.status === "active",
                    ),
                    latestRunCompletedAt: latestUnheldRun(projection.runs)?.completedAt ?? null,
                  };
                })
                .toSorted((left, right) => String(left.id).localeCompare(String(right.id)));
              return { threads, snapshotSequence: yield* Ref.get(sequence) };
            }).pipe(censusPermit.withPermits(1)),
        ),

        apply:
          /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
          Effect.fnUntraced(
            function* (event) {
              const result = yield* Ref.modify(
                replayState,
                (
                  existing,
                ): readonly [
                  ApplyError | MissingThreadError | undefined,
                  ProjectionReplayState,
                ] => {
                  const next: ProjectionReplayState = {
                    projections: new Map(existing.projections),
                    legacyThreads: new Map(existing.legacyThreads),
                    legacyRuns: new Map(existing.legacyRuns),
                    providerSessionThreadIds: new Map(existing.providerSessionThreadIds),
                    providerThreadInstanceIds: new Map(existing.providerThreadInstanceIds),
                  };

                  try {
                    if (!applyToProjectionReplayState(next, event)) {
                      return [
                        new ProjectionStoreThreadNotFoundError({ threadId: event.threadId }),
                        existing,
                      ] as const;
                    }
                  } catch (cause) {
                    return [
                      new ProjectionStoreApplyEventError({ eventType: event.type, cause }),
                      existing,
                    ] as const;
                  }
                  return [undefined, next] as const;
                },
              );

              if (result) {
                return yield* result;
              }
              yield* Ref.update(sequence, (current) => current + 1);
            },
            Effect.uninterruptible,
            censusPermit.withPermits(1),
          ),
        getShellSnapshot: Effect.fn("ProjectionStore.getShellSnapshot")(function* (options) {
          const existing = (yield* Ref.get(replayState)).projections;
          const selectedThreadIds = [...existing.entries()]
            .filter(([, projection]) => {
              if (
                options?.projectId !== undefined &&
                projection.thread.projectId !== options.projectId
              ) {
                return false;
              }
              if (
                options?.unsettledOnly &&
                (projection.thread.settledAt !== null ||
                  projection.thread.settledOverride === "settled")
              ) {
                return false;
              }
              if (options?.familyOf !== undefined) {
                const member = existing.get(options.familyOf)?.thread;
                if (
                  member === undefined ||
                  projection.thread.lineage.rootThreadId !== member.lineage.rootThreadId
                ) {
                  return false;
                }
              }
              if (
                options?.threadIds !== undefined &&
                !options.threadIds.includes(projection.thread.id)
              ) {
                return false;
              }
              if (
                options?.worktreePath !== undefined &&
                projection.thread.worktreePath !== options.worktreePath
              ) {
                return false;
              }
              if (options?.location === "active") return projection.thread.archivedAt === null;
              if (options?.location === "archive") return projection.thread.archivedAt !== null;
              return true;
            })
            .map(([threadId]) => threadId);
          const shells = yield* Effect.forEach(
            selectedThreadIds.toSorted((left, right) => String(left).localeCompare(String(right))),
            (threadId) =>
              service
                .getThreadProjection(threadId)
                .pipe(Effect.map((projection) => threadShellFromProjection(projection))),
          );
          const visible = shells.filter((thread) => thread.deletedAt === null);
          return {
            schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
            snapshotSequence: yield* Ref.get(sequence),
            threads: visible.filter((thread) => thread.archivedAt === null),
            archivedThreads: visible.filter((thread) => thread.archivedAt !== null),
          };
        }),
        listActiveSubagentThreadIds: Effect.fn("ProjectionStore.listActiveSubagentThreadIds")(
          function* () {
            const existing = (yield* Ref.get(replayState)).projections;
            return [...existing.values()]
              .filter(
                ({ thread }) =>
                  thread.deletedAt === null &&
                  thread.archivedAt === null &&
                  thread.lineage.relationshipToParent === "subagent",
              )
              .map(({ thread }) => thread.id);
          },
        ),
        listOlderSettledThreads: Effect.fn("ProjectionStore.listOlderSettledThreads")(function* ({
          before,
          projectIds,
          limit,
        }) {
          const older = [...(yield* Ref.get(replayState)).projections.values()].flatMap(
            ({ thread }) => {
              if (
                thread.deletedAt !== null ||
                thread.archivedAt !== null ||
                thread.lineage.relationshipToParent === "subagent" ||
                thread.lineage.sideChat === true ||
                thread.settledOverride !== "settled" ||
                thread.settledAt === null ||
                (projectIds !== undefined && !projectIds.includes(thread.projectId))
              ) {
                return [];
              }
              const settledAt = DateTime.formatIso(thread.settledAt);
              const below =
                settledAt < before.settledAt ||
                (before.threadId !== undefined &&
                  settledAt === before.settledAt &&
                  thread.id > before.threadId);
              return below ? [{ thread, settledAt }] : [];
            },
          );
          const counts = new Map<ProjectId, number>();
          for (const { thread } of limit === 0 ? older : []) {
            counts.set(thread.projectId, (counts.get(thread.projectId) ?? 0) + 1);
          }
          return {
            threadIds: older
              .toSorted((left, right) =>
                left.settledAt !== right.settledAt
                  ? left.settledAt > right.settledAt
                    ? -1
                    : 1
                  : left.thread.id < right.thread.id
                    ? -1
                    : 1,
              )
              .slice(0, limit)
              .map(({ thread }) => thread.id),
            counts: Array.from(counts, ([projectId, count]) => ({ projectId, count })),
          };
        }),
        readShellSnapshot: (options) =>
          service.getShellSnapshot(options).pipe(Effect.map(Effect.succeed)),
        getThreadShell: Effect.fn("ProjectionStore.getThreadShell")(function* (threadId) {
          const existing = (yield* Ref.get(replayState)).projections;
          if (!existing.has(threadId)) {
            return null;
          }
          const shell = yield* service
            .getThreadProjection(threadId)
            .pipe(Effect.map((projection) => threadShellFromProjection(projection)));
          return shell.deletedAt === null ? shell : null;
        }),
        findGuestShareThreadId: Effect.fn("ProjectionStore.findGuestShareThreadId")(
          function* (shareId) {
            const existing = (yield* Ref.get(replayState)).projections;
            for (const { thread } of existing.values()) {
              if (thread.guestShare?.id === shareId) return thread.id;
            }
            return null;
          },
        ),
        getThread: Effect.fn("ProjectionStore.getThread")(function* (threadId) {
          const projection = (yield* Ref.get(replayState)).projections.get(threadId);
          if (projection === undefined) {
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          }
          return projection.thread;
        }),
        getSettlementCandidates: Effect.fn("ProjectionStore.getSettlementCandidates")(
          function* (threadId) {
            const projections = (yield* Ref.get(replayState)).projections;
            return [...projections.values()]
              .filter(
                ({ thread, runs, runtimeRequests }) =>
                  (threadId === undefined || thread.id === threadId) &&
                  thread.deletedAt === null &&
                  thread.archivedAt === null &&
                  thread.settledOverride === null &&
                  thread.pinnedAt == null &&
                  thread.autoSettleDisabledAt == null &&
                  !runs.some(isActivityRunForShell) &&
                  !runtimeRequests.some((request) => request.status === "pending"),
              )
              .map((projection) => {
                const shell = threadShellFromProjection(projection);
                const latestResumingMessage = projection.messages
                  .filter(
                    (message) =>
                      message.role === "user" &&
                      idleCompactionOf(message) === undefined &&
                      (message.createdBy === "user" ||
                        (message.createdBy === "agent" && message.creationSource === "mcp")),
                  )
                  .toSorted(
                    (left, right) =>
                      DateTime.toEpochMillis(right.updatedAt) -
                      DateTime.toEpochMillis(left.updatedAt),
                  )[0];
                return {
                  ...shell,
                  lastSnoozeWakeAt: projection.thread.lastSnoozeWakeAt ?? null,
                  latestUserAuthoredMessageAt: shell.latestUserAuthoredMessageAt ?? null,
                  latestResumingMessageAt: latestResumingMessage?.updatedAt ?? null,
                };
              })
              .toSorted(
                (left, right) =>
                  DateTime.toEpochMillis(left.updatedAt) -
                    DateTime.toEpochMillis(right.updatedAt) || left.id.localeCompare(right.id),
              );
          },
        ),
        getThreadWorktreePaths: (options) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()].flatMap(({ thread }) =>
                thread.deletedAt === null &&
                (options?.includeArchived === true || thread.archivedAt === null) &&
                thread.worktreePath !== null
                  ? [{ threadId: thread.id, worktreePath: thread.worktreePath }]
                  : [],
              ),
            ),
          ),
        getOpenRemoteDelegatedTasks: Ref.get(replayState).pipe(
          Effect.map((state) =>
            [...state.projections.values()].flatMap((projection) =>
              projection.subagents
                .filter(
                  (task) =>
                    task.origin === "app_owned" &&
                    task.childThreadId === null &&
                    task.remoteChild != null &&
                    (task.result == null ||
                      ((task.status === "cancelled" || task.status === "interrupted") &&
                        task.remoteChild.stoppedThere !== true) ||
                      task.remoteChild.stoppedThere === false),
                )
                .map((task) => ({ parentThreadId: projection.thread.id, taskId: task.id })),
            ),
          ),
        ),
        getThreadsWithPullRequests: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()]
                .map(({ thread }) => thread)
                .filter(
                  (thread) =>
                    (threadId === undefined || thread.id === threadId) &&
                    thread.deletedAt === null &&
                    thread.archivedAt === null &&
                    (thread.pullRequests ?? []).length > 0,
                )
                .toSorted(
                  (left, right) =>
                    DateTime.toEpochMillis(left.updatedAt) -
                      DateTime.toEpochMillis(right.updatedAt) || left.id.localeCompare(right.id),
                )
                .map((thread): ProjectionThreadPullRequests => ({
                  id: thread.id,
                  projectId: thread.projectId,
                  lineage: thread.lineage,
                  settledOverride: thread.settledOverride,
                  settledAt: thread.settledAt,
                  pullRequests: thread.pullRequests ?? [],
                })),
            ),
          ),
        getThreadLinkMetadata: (threadIds) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              Object.fromEntries(
                threadIds.flatMap((threadId) => {
                  const thread = state.projections.get(threadId)?.thread;
                  return thread === undefined || thread.deletedAt !== null
                    ? []
                    : [[threadId, { title: thread.title, projectId: thread.projectId }]];
                }),
              ),
            ),
          ),
        getThreadTitles: (threadIds) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              Object.fromEntries(
                threadIds.flatMap((threadId) => {
                  const thread = state.projections.get(threadId)?.thread;
                  return thread === undefined || thread.deletedAt !== null
                    ? []
                    : [[threadId, thread.title]];
                }),
              ),
            ),
          ),
        getMessagesById: (threadId, messageIds) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => {
              const requested = new Set<string>(messageIds);
              return new Map(
                (state.projections.get(threadId)?.messages ?? [])
                  .filter((message) => requested.has(message.id))
                  .map((message) => [message.id, message] as const),
              );
            }),
          ),
        getPullRequestSnapshot: ({ threadId, unsettledOnly = false }) =>
          Effect.all([Ref.get(replayState), Ref.get(sequence)]).pipe(
            Effect.map(([state, snapshotSequence]) => ({
              snapshotSequence,
              threads: [...state.projections.values()]
                .map(({ thread }) => thread)
                .filter(
                  (thread) =>
                    thread.deletedAt === null &&
                    (threadId === undefined
                      ? thread.archivedAt === null
                      : thread.id === threadId) &&
                    (!unsettledOnly ||
                      (thread.settledAt === null && thread.settledOverride !== "settled")),
                )
                .toSorted(
                  (left, right) =>
                    DateTime.toEpochMillis(left.updatedAt) -
                      DateTime.toEpochMillis(right.updatedAt) || left.id.localeCompare(right.id),
                )
                .map((thread) => ({
                  id: thread.id,
                  projectId: thread.projectId,
                  lineage: thread.lineage,
                  branch: thread.branch,
                  worktreePath: thread.worktreePath,
                  archivedAt: thread.archivedAt,
                  createdAt: thread.createdAt,
                  updatedAt: thread.updatedAt,
                  settledOverride: thread.settledOverride,
                  settledAt: thread.settledAt,
                  ...(thread.linkedPullRequest === undefined
                    ? {}
                    : { linkedPullRequest: thread.linkedPullRequest }),
                  ...(thread.branchPullRequest === undefined
                    ? {}
                    : { branchPullRequest: thread.branchPullRequest }),
                  pullRequests: threadPullRequestsOf(thread),
                })),
            })),
          ),
        getAgentAwarenessSnapshot: (threadId) =>
          Effect.all([Ref.get(replayState), Ref.get(sequence)]).pipe(
            Effect.map(([state, snapshotSequence]) => ({
              snapshotSequence,
              threads: [...state.projections.values()]
                .filter(({ thread }) =>
                  threadId === undefined
                    ? thread.deletedAt === null &&
                      thread.archivedAt === null &&
                      thread.lineage.relationshipToParent !== "subagent" &&
                      thread.lineage.sideChat !== true
                    : thread.deletedAt === null && thread.id === threadId,
                )
                .map((projection) => threadShellFromProjection(projection))
                .toSorted(
                  (left, right) =>
                    DateTime.toEpochMillis(left.updatedAt) -
                      DateTime.toEpochMillis(right.updatedAt) || left.id.localeCompare(right.id),
                )
                .map((shell) => ({
                  id: shell.id,
                  projectId: shell.projectId,
                  creationSource: shell.creationSource,
                  archivedAt: shell.archivedAt,
                  activeRunId: shell.activeRunId,
                  latestRunId: shell.latestRunId,
                  hasActionableProposedPlan: shell.hasActionableProposedPlan,
                  title: shell.title,
                  lineage: shell.lineage,
                  modelSelection: shell.modelSelection,
                  settledOverride: shell.settledOverride,
                  updatedAt: shell.updatedAt,
                  status: shell.status,
                  activityRunStatus: shell.activityRunStatus,
                  latestRunCompletedAt: shell.latestRunCompletedAt,
                  latestRunStopRequested: shell.latestRunStopRequested,
                  pendingRuntimeRequest: shell.pendingRuntimeRequest,
                  pendingGuestMessageCount: shell.pendingGuestMessageCount,
                  snoozedAt: shell.snoozedAt,
                  snoozedUntil: shell.snoozedUntil,
                  pendingBackgroundTasks: shell.pendingBackgroundTasks,
                })),
            })),
          ),
        getLimitRecoveryCandidates: (options) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              [...state.projections.values()]
                .filter(
                  ({ thread }) =>
                    thread.deletedAt === null &&
                    thread.archivedAt === null &&
                    thread.settledOverride !== "settled",
                )
                .map((projection) => threadShellFromProjection(projection))
                .filter(
                  (thread) =>
                    thread.status === "failed" &&
                    thread.lastErrorClass === "usage_limit" &&
                    thread.usageLimitResetAt !== null &&
                    thread.pendingRuntimeRequest === null,
                )
                .filter((thread) => {
                  const resetMs = Date.parse(thread.usageLimitResetAt!);
                  const nowMs = DateTime.toEpochMillis(options.now);
                  if (
                    !Number.isFinite(resetMs) ||
                    resetMs <=
                      DateTime.toEpochMillis(thread.latestRunCompletedAt ?? thread.updatedAt)
                  )
                    return false;
                  if (
                    thread.limitRecovery?.runId !== thread.latestRunId ||
                    thread.limitRecovery.resetAt !== thread.usageLimitResetAt
                  ) {
                    return options.autoResume || (options.snooze && resetMs > nowMs);
                  }
                  return (
                    thread.limitRecovery.autoResume &&
                    resetMs <= nowMs &&
                    (thread.snoozedUntil == null ||
                      DateTime.toEpochMillis(thread.snoozedUntil) <= nowMs)
                  );
                })
                .toSorted((left, right) => left.id.localeCompare(right.id)),
            ),
          ),
        getRecoveryThreadIds: Effect.fn("ProjectionStore.getRecoveryThreadIds")(
          function* (kind, projects) {
            const projections = (yield* Ref.get(replayState)).projections;
            return [...projections.values()]
              .filter(
                (projection) =>
                  (projects?.include === undefined ||
                    projects.include.includes(projection.thread.projectId)) &&
                  (projects?.exclude === undefined ||
                    !projects.exclude.includes(projection.thread.projectId)) &&
                  needsRecovery(
                    projection,
                    kind,
                    projection.thread.lineage.parentThreadId === null
                      ? undefined
                      : projections.get(projection.thread.lineage.parentThreadId),
                  ),
              )
              .toSorted(
                (left, right) =>
                  DateTime.toEpochMillis(left.thread.updatedAt) -
                    DateTime.toEpochMillis(right.thread.updatedAt) ||
                  left.thread.id.localeCompare(right.thread.id),
              )
              .map((projection) => projection.thread.id);
          },
        ),
        getUnreadableThreadIds: () => Effect.succeed([]),
        getPendingNativeUserInputs: Effect.fn("ProjectionStore.getPendingNativeUserInputs")(
          function* (threadId, providerTurnId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined) return { runtimeRequests: [], nodes: [], turnItems: [] };
            const runtimeRequests = projection.runtimeRequests.filter(
              (request) =>
                request.providerTurnId === providerTurnId &&
                request.kind === "user_input" &&
                request.status === "pending" &&
                request.responseCapability.type !== "message",
            );
            const requestIds = new Set(runtimeRequests.map((request) => request.id));
            const nodeIds = new Set(runtimeRequests.map((request) => request.nodeId));
            return {
              runtimeRequests,
              nodes: projection.nodes.filter((node) => nodeIds.has(node.id)),
              turnItems: projection.turnItems.filter(
                (item) => item.type === "user_input_request" && requestIds.has(item.requestId),
              ),
            };
          },
        ),
        getPlan: Effect.fn("ProjectionStore.getPlan")(function* (threadId, planId) {
          const projection = (yield* Ref.get(replayState)).projections.get(threadId);
          if (projection === undefined)
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          return projection.plans.find((plan) => plan.id === planId);
        }),
        hasUnpairedRunInterruptRequest: Effect.fn("ProjectionStore.hasUnpairedRunInterruptRequest")(
          function* (threadId, requestId, resultId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            return (
              projection !== undefined &&
              projection.turnItems.some((item) => item.id === requestId) &&
              !projection.turnItems.some((item) => item.id === resultId)
            );
          },
        ),
        hasRunInterruptRequest: (threadId, runId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              (state.projections.get(threadId)?.turnItems ?? []).some(
                (item) => item.type === "run_interrupt_request" && item.runId === runId,
              ),
            ),
          ),
        getRun: (threadId, runId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) =>
              state.projections.get(threadId)?.runs.find((run) => run.id === runId),
            ),
          ),
        getRunProviderRecords: (threadId, runId, nodeId) =>
          service.getThreadProjection(threadId).pipe(
            Effect.map((projection) => {
              const runs = projection.runs.filter((run) => run.id === runId);
              const run = runs[0];
              const attempts = projection.attempts.filter(
                (attempt) => attempt.id === run?.activeAttemptId,
              );
              const attemptTurns =
                run?.activeAttemptId == null
                  ? []
                  : projection.providerTurns.filter(
                      (turn) => turn.runAttemptId === run.activeAttemptId,
                    );
              return {
                thread: projection.thread,
                runs,
                attempts,
                nodes: projection.nodes.filter((node) => node.id === nodeId),
                providerThreads: projection.providerThreads.filter(
                  (thread) => thread.id === run?.providerThreadId,
                ),
                providerTurns:
                  attemptTurns.length > 0
                    ? attemptTurns
                    : projection.providerTurns.filter(
                        (turn) => turn.id === attempts[0]?.providerTurnId,
                      ),
              };
            }),
          ),
        isNodeStored: (node) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => {
              const stored = state.projections
                .get(node.threadId)
                ?.nodes.find((candidate) => candidate.id === node.id);
              return stored !== undefined && Equal.equals(stored, node);
            }),
          ),
        getMessageCount: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map((state) => state.projections.get(threadId)?.messages.length ?? 0),
          ),
        getNextTurnItemOrdinal: (threadId) =>
          Ref.get(replayState).pipe(
            Effect.map(
              (state) =>
                (state.projections
                  .get(threadId)
                  ?.turnItems.reduce((max, item) => Math.max(max, item.ordinal), 0) ?? 0) + 1,
            ),
          ),
        getTurnItem: ({ threadId, itemId }) =>
          Ref.get(replayState).pipe(
            Effect.map(
              (state) =>
                state.projections.get(threadId)?.turnItems.find((item) => item.id === itemId) ??
                null,
            ),
          ),
        getThreadAttachmentIds: (threadId) =>
          service.getThreadProjection(threadId).pipe(
            Effect.map((projection) => [
              ...new Set([
                ...projection.messages.flatMap((message) =>
                  message.attachments.map((attachment) => attachment.id),
                ),
                ...(projection.thread.guestShare?.messages ?? []).flatMap((message) =>
                  message.status === "pending"
                    ? (message.attachments ?? []).map((attachment) => attachment.id)
                    : [],
                ),
                ...threadHtmlRenderAttachmentIds(
                  threadId,
                  projection.turnItems.flatMap((item) =>
                    item.type === "dynamic_tool" ? [item] : [],
                  ),
                ),
              ]),
            ]),
          ),
        getThreadRecords: Effect.fn("ProjectionStore.getThreadRecords")(
          function* (threadId, fields, filter) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const selected = {
              ...projection,
              messages: projection.messages.filter(
                (row) =>
                  (filter?.messageRunIds === undefined ||
                    (row.runId !== null && filter.messageRunIds.includes(row.runId))) &&
                  (filter?.messageIds === undefined || filter.messageIds.includes(row.id)) &&
                  (filter?.messageRoles === undefined || filter.messageRoles.includes(row.role)),
              ),
              runs:
                filter?.runIds === undefined
                  ? projection.runs
                  : projection.runs.filter((row) => filter.runIds!.includes(row.id)),
              turnItems: projection.turnItems.filter(
                (row) =>
                  (filter?.turnItemRunIds === undefined ||
                    filter.turnItemRunIds.includes(row.runId)) &&
                  (filter?.turnItemTypes === undefined ||
                    filter.turnItemTypes.includes(row.type)) &&
                  (filter?.turnItemStatuses === undefined ||
                    filter.turnItemStatuses.includes(row.status)) &&
                  (filter?.turnItemRunId === undefined || filter.turnItemRunId === row.runId),
              ),
              nodes:
                filter?.omitSettledLeafNodes === true
                  ? projection.nodes.filter((node) => !isSettledLeafNode(node, projection.runs))
                  : projection.nodes,
            };
            return Object.fromEntries([
              ["thread", projection.thread],
              ...fields.map((field) => [field, selected[field]]),
            ]) as ProjectionRecords<(typeof fields)[number]>;
          },
        ),
        getRuntimeRequest: Effect.fn("ProjectionStore.getRuntimeRequest")(
          function* (threadId, requestId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            return projection.runtimeRequests.find((request) => request.id === requestId);
          },
        ),
        getProviderControlContext: Effect.fn("ProjectionStore.getProviderControlContext")(
          function* (threadId, target) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const providerTurn = projection.providerTurns.find(
              (turn) => turn.id === target.providerTurnId,
            );
            return {
              providerThread: projection.providerThreads.find(
                (thread) => thread.id === target.providerThreadId,
              ),
              providerTurn,
              attempt:
                target.attemptId === undefined
                  ? undefined
                  : projection.attempts.find((attempt) => attempt.id === target.attemptId),
              message:
                target.messageId === undefined
                  ? undefined
                  : projection.messages.find((message) => message.id === target.messageId),
              run:
                target.messageId === undefined || providerTurn === undefined
                  ? undefined
                  : projection.runs.find(
                      (run) => run.activeAttemptId === providerTurn.runAttemptId,
                    ),
            };
          },
        ),
        getRunningTurnContext: Effect.fn("ProjectionStore.getRunningTurnContext")(
          function* (threadId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const run = projection.runs.find((run) => run.status === "running");
            return {
              run,
              providerThread: projection.providerThreads.find(
                (thread) => thread.id === run?.providerThreadId,
              ),
              providerTurn: projection.providerTurns.find(
                (turn) =>
                  turn.providerThreadId === run?.providerThreadId &&
                  turn.runAttemptId === run?.activeAttemptId &&
                  turn.nodeId === run?.rootNodeId &&
                  turn.status === "running",
              ),
            };
          },
        ),
        getThreadProviderContext: Effect.fn("ProjectionStore.getThreadProviderContext")(
          function* (threadId, targetInstanceId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const target = projection.providerThreads
              .filter(
                (thread) =>
                  thread.appThreadId === threadId &&
                  thread.ownerNodeId === null &&
                  thread.providerInstanceId === targetInstanceId,
              )
              .toSorted(
                (a, b) =>
                  DateTime.toEpochMillis(b.updatedAt) - DateTime.toEpochMillis(a.updatedAt) ||
                  a.id.localeCompare(b.id),
              )[0];
            return {
              thread: projection.thread,
              providerSessions: projection.providerSessions,
              providerThreads:
                targetInstanceId === undefined
                  ? []
                  : projection.providerThreads.filter(
                      (thread) =>
                        thread.id === projection.thread.activeProviderThreadId ||
                        thread.id === target?.id,
                    ),
            };
          },
        ),
        getRuntimeResponseContext: Effect.fn("ProjectionStore.getRuntimeResponseContext")(
          function* (threadId, requestId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const request = projection.runtimeRequests.find((request) => request.id === requestId);
            return {
              request,
              node: projection.nodes.find((node) => node.id === request?.nodeId),
              item: projection.turnItems.find(
                (item) =>
                  item.nodeId === request?.nodeId &&
                  (item.type === "approval_request" || item.type === "user_input_request") &&
                  item.requestId === requestId,
              ),
              session: projection.providerSessions.find(
                (session) =>
                  request?.responseCapability.type === "live" &&
                  session.id === request.responseCapability.providerSessionId,
              ),
            };
          },
        ),
        getCheckpointContext: Effect.fn("ProjectionStore.getCheckpointContext")(
          function* (threadId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined) {
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            }
            return {
              runs: projection.runs.map(({ id, ordinal, status }) => ({ id, ordinal, status })),
              checkpointScopes: projection.checkpointScopes.map(({ id, runId, kind, cwd }) => ({
                id,
                runId,
                kind,
                cwd,
              })),
              checkpoints: projection.checkpoints.map(
                ({ scopeId, runId, ordinalWithinScope, appRunOrdinal, status, ref }) => ({
                  scopeId,
                  runId,
                  ordinalWithinScope,
                  appRunOrdinal,
                  status,
                  ref,
                }),
              ),
            };
          },
        ),
        getCheckpointCaptureContext: Effect.fn("ProjectionStore.getCheckpointCaptureContext")(
          function* (threadId, target) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const run = projection.runs.find((candidate) => candidate.id === target.runId);
            const repositoryScopes = projection.checkpointScopes.filter(
              (candidate) => candidate.kind === "manual",
            );
            const preservesCheckpoint = (
              checkpoint: OrchestrationV2ThreadProjection["checkpoints"][number],
            ) =>
              checkpoint.status === "ready" ||
              ((checkpoint.status === "missing" || checkpoint.status === "error") &&
                checkpoint.runId !== null);
            return {
              sideChat: projection.thread.lineage.sideChat === true,
              thread: {
                projectId: projection.thread.projectId,
                worktreePath: projection.thread.worktreePath,
              },
              run,
              rootNode: projection.nodes.find((candidate) => candidate.id === run?.rootNodeId),
              scope: projection.checkpointScopes.find(
                (candidate) => candidate.id === target.scopeId,
              ),
              providerThread: projection.providerThreads.find(
                (candidate) => candidate.id === run?.providerThreadId,
              ),
              preservedCheckpointOrdinals: projection.checkpoints
                .filter(
                  (candidate) =>
                    candidate.scopeId === target.scopeId && preservesCheckpoint(candidate),
                )
                .map((candidate) => candidate.ordinalWithinScope),
              repositoryScopes,
              repositoryScopesWithBaseline: projection.checkpoints
                .filter(
                  (candidate) =>
                    candidate.ordinalWithinScope === Math.max(0, (run?.ordinal ?? 0) - 1) &&
                    repositoryScopes.some((scope) => scope.id === candidate.scopeId) &&
                    preservesCheckpoint(candidate),
                )
                .map((candidate) => candidate.scopeId),
            };
          },
        ),
        getCheckpointRollbackContext: Effect.fn("ProjectionStore.getCheckpointRollbackContext")(
          function* (threadId, target) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const { thread } = projection;
            const providerThread = projection.providerThreads.find(
              (candidate) => candidate.id === target.providerThreadId,
            );
            const checkpoint = projection.checkpoints.find(
              (candidate) => candidate.id === target.checkpointId,
            );
            const scope = projection.checkpointScopes.find(
              (candidate) => candidate.id === target.scopeId,
            );
            const context: ProjectionCheckpointRollbackContext = {
              thread,
              providerThread,
              checkpoint,
              scope,
              providerSession: undefined,
              targetAttempt: undefined,
              providerTurns: [],
              rolledBackAttemptIds: [],
              runsToRollback: [],
              rootNodes: [],
              staleCheckpoints: [],
            };
            if (
              providerThread === undefined ||
              providerThread.providerSessionId === null ||
              checkpoint === undefined ||
              scope === undefined ||
              checkpoint.scopeId !== scope.id ||
              checkpoint.status !== "ready" ||
              providerThread.id !== thread.activeProviderThreadId ||
              providerThread.providerInstanceId !== thread.modelSelection.instanceId
            )
              return context;

            const targetOrdinal = checkpoint.appRunOrdinal ?? 0;
            const targetRun = projection.runs.find((run) => run.ordinal === targetOrdinal);
            const attempt = projection.attempts.find(
              (attempt) => attempt.id === targetRun?.activeAttemptId,
            );
            const runsToRollback = projection.runs.filter(
              (run) =>
                run.ordinal > targetOrdinal &&
                (run.status === "completed" ||
                  run.status === "interrupted" ||
                  run.status === "failed" ||
                  run.status === "cancelled"),
            );
            const rootNodeIds = new Set(runsToRollback.map((run) => run.rootNodeId));
            const rolledBackRunIds = new Set(
              projection.runs.filter((run) => run.status === "rolled_back").map((run) => run.id),
            );
            return {
              ...context,
              targetAttempt: attempt,
              runsToRollback,
              rootNodes: projection.nodes.filter((node) => rootNodeIds.has(node.id)),
              rolledBackAttemptIds: projection.attempts
                .filter((attempt) => rolledBackRunIds.has(attempt.runId))
                .map((attempt) => attempt.id),
              providerSession: projection.providerSessions.find(
                (session) => session.id === providerThread.providerSessionId,
              ),
              providerTurns: projection.providerTurns.filter(
                (turn) =>
                  turn.providerThreadId === providerThread.id ||
                  turn.id === attempt?.providerTurnId ||
                  (attempt !== undefined && turn.runAttemptId === attempt.id),
              ),
              staleCheckpoints: projection.checkpoints.filter(
                (candidate) =>
                  candidate.scopeId === scope.id &&
                  candidate.appRunOrdinal !== null &&
                  candidate.appRunOrdinal > targetOrdinal &&
                  candidate.status === "ready",
              ),
            };
          },
        ),
        getRunMessage: Effect.fn("ProjectionStore.getRunMessage")(function* (threadId, runId) {
          const projection = (yield* Ref.get(replayState)).projections.get(threadId);
          if (projection === undefined)
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          const run = projection.runs.find((candidate) => candidate.id === runId);
          return projection.messages.find((message) => message.id === run?.userMessageId);
        }),
        hasQueuedToolBoundaryWork: Effect.fn("ProjectionStore.hasQueuedToolBoundaryWork")(
          function* (threadId, runId) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            return (
              projection !== undefined &&
              projection.thread.archivedAt === null &&
              projection.thread.deletedAt === null &&
              projection.runs.some((run) => run.id === runId && run.status === "running") &&
              projection.runs.some((run) => run.status === "queued" && run.queueHeld !== true) &&
              !projection.runtimeRequests.some((request) => request.status === "pending")
            );
          },
        ),
        getQueuedToolBoundaryContext: Effect.fn("ProjectionStore.getQueuedToolBoundaryContext")(
          function* (threadId, node) {
            const projection = (yield* Ref.get(replayState)).projections.get(threadId);
            if (projection === undefined)
              return yield* new ProjectionStoreThreadNotFoundError({ threadId });
            const runs = projection.runs.filter(
              (run) => run.status === "queued" || run.id === node.runId,
            );
            const ids = new Set(runs.map((run) => run.id));
            const nodeIds = new Set([
              node.id,
              ...runs.flatMap((run) => (run.rootNodeId === null ? [] : [run.rootNodeId])),
              ...projection.nodes
                .filter(
                  (candidate) =>
                    candidate.runId === node.runId &&
                    candidate.kind === "tool_call" &&
                    ["pending", "running", "waiting"].includes(candidate.status),
                )
                .map((candidate) => candidate.id),
            ]);
            for (const id of nodeIds) {
              const parent = projection.nodes.find(
                (candidate) => candidate.id === id,
              )?.parentNodeId;
              if (parent !== null && parent !== undefined) nodeIds.add(parent);
            }
            return {
              thread: projection.thread,
              runs,
              nodes: projection.nodes.filter((candidate) => nodeIds.has(candidate.id)),
              attempts: projection.attempts.filter((attempt) =>
                runs.some((run) => run.activeAttemptId === attempt.id),
              ),
              messages: projection.messages.filter(
                (message) =>
                  message.runId !== null && ids.has(message.runId) && message.role === "user",
              ),
              providerThreads: projection.providerThreads.filter((provider) =>
                runs.some((run) => run.providerThreadId === provider.id),
              ),
              providerTurns: projection.providerTurns.filter(
                (turn) => turn.id === node.providerTurnId,
              ),
              providerSessions: projection.providerSessions.filter((session) =>
                projection.providerThreads.some(
                  (provider) =>
                    provider.id === node.providerThreadId &&
                    provider.providerSessionId === session.id,
                ),
              ),
              runtimeRequests: projection.runtimeRequests.filter(
                (request) => request.status === "pending",
              ),
              subagents: [],
              turnItems: [],
            };
          },
        ),
        canStartQueuedRun: Effect.fn("ProjectionStore.canStartQueuedRun")(function* (threadId) {
          const projection = (yield* Ref.get(replayState)).projections.get(threadId);
          if (projection === undefined)
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          return (
            projection.thread.archivedAt === null &&
            projection.thread.deletedAt === null &&
            projection.runs.some((run) => run.status === "queued" && run.queueHeld !== true) &&
            !projection.runs.some(
              (run) =>
                run.status === "preparing" ||
                run.status === "starting" ||
                run.status === "running" ||
                run.status === "waiting",
            )
          );
        }),
        getThreadProjection: Effect.fn("ProjectionStore.getThreadProjection")(function* (threadId) {
          const { projections: existing, providerThreadInstanceIds } = yield* Ref.get(replayState);
          const readProjection = (
            targetThreadId: ThreadId,
            seenThreadIds: ReadonlySet<ThreadId>,
          ): OrchestrationV2ThreadProjection | null => {
            const projection = existing.get(targetThreadId);
            if (!projection) {
              return null;
            }
            const forkedFrom = projection.thread.forkedFrom;
            if (forkedFrom?.type !== "run" || seenThreadIds.has(forkedFrom.threadId)) {
              return withLocalVisibleTurnItems(projection);
            }
            const sourceProjection = readProjection(
              forkedFrom.threadId,
              new Set([...seenThreadIds, targetThreadId]),
            );
            return {
              ...projection,
              visibleTurnItems: buildVisibleTurnItems({
                projection,
                sourceProjection,
              }),
            };
          };
          const projection = readProjection(threadId, new Set());
          if (!projection) {
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          }
          // Match the SQL shell join, including provider threads stored on child app threads.
          return {
            ...projection,
            turnItems: projection.turnItems.map((item) => {
              const owner =
                item.type !== "subagent" || item.providerThreadId === null
                  ? undefined
                  : providerThreadInstanceIds.get(item.providerThreadId);
              return owner === undefined ? item : { ...item, owningProviderInstanceId: owner };
            }),
          };
        }),
        getTurnStartContext: (threadId, runId) =>
          service.getThreadProjection(threadId).pipe(
            Effect.map((projection) => ({
              ...projection,
              hasConversation:
                projection.messages.some(
                  (message) =>
                    message.role === "user" &&
                    (message.text.trim().toLowerCase() !== "/compact" ||
                      message.attachments.length > 0),
                ) || inheritsForkConversation(projection),
              turnItems: projection.turnItems.filter((item) => item.runId === runId),
            })),
          ),
        getTurnStartHistory: (threadId, runIds) =>
          service
            .getThreadProjection(threadId)
            .pipe(
              Effect.map((projection) =>
                projection.turnItems.filter(
                  (item) =>
                    [
                      "user_message",
                      "assistant_message",
                      "command_execution",
                      "error",
                      "run_interrupt_result",
                      "file_change",
                      "proposed_plan",
                    ].includes(item.type) &&
                    (runIds === undefined || (item.runId !== null && runIds.includes(item.runId))),
                ),
              ),
            ),
        getRuntimeRecoveryProjection: (threadId) => service.getThreadProjection(threadId),
        getThreadSnapshot: (threadId) =>
          service.getThreadProjection(threadId).pipe(
            Effect.flatMap((projection) =>
              Ref.get(sequence).pipe(
                Effect.map((snapshotSequence) => ({
                  schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
                  snapshotSequence,
                  projection,
                })),
              ),
            ),
          ),
        searchThreadStream: (input) => Stream.fromEffect(service.searchThread(input)),
        searchThread: (input) =>
          service
            .getThreadSnapshot(input.threadId)
            .pipe(
              Effect.flatMap(({ projection, snapshotSequence }) =>
                projection.thread.deletedAt !== null
                  ? Effect.fail(
                      new ProjectionStoreThreadNotFoundError({ threadId: input.threadId }),
                    )
                  : Effect.succeed(
                      findProjectedThreadItems(
                        projection.visibleTurnItems,
                        input,
                        snapshotSequence,
                        projection.thread.worktreePath ?? undefined,
                      ),
                    ),
              ),
            ),
        getThreadHistoryPage: (threadId, cursor, throughEntryId, conversationOnly, workMode) =>
          service.getThreadSnapshot(threadId).pipe(
            Effect.flatMap(({ projection, snapshotSequence }) =>
              Effect.gen(function* () {
                if (projection.thread.deletedAt !== null)
                  return yield* new ProjectionStoreThreadNotFoundError({ threadId });
                return yield* Effect.try({
                  try: () => {
                    const visible = projection.visibleTurnItems.map((row) => ({
                      ...row,
                      item: projectTurnItemForWire(row.item),
                    }));
                    const page =
                      workMode === "work"
                        ? selectDeferredWorkPage({
                            items: visible,
                            cursor,
                            throughEntryId,
                            snapshotSequence,
                          })
                        : selectHistoryPageFromCursor({
                            items:
                              workMode === "deferred" ? deferWorkHistoryRows(visible) : visible,
                            cursor,
                            throughEntryId,
                            snapshotSequence,
                          });
                    const items = [
                      ...(workMode === "work" ? [] : missingInterruptRequests(visible, page.items)),
                      ...page.items,
                    ].map((row, position) => ({ ...row, position }));
                    return {
                      ...page,
                      snapshotSequence,
                      items: conversationOnly
                        ? items.filter((row) => isConversationHistoryItem(row.item))
                        : items,
                    };
                  },
                  catch: (cause) => new ProjectionStoreReadError({ threadId, cause }),
                });
              }),
            ),
          ),
        getTimelinePage: (threadId, options) =>
          service.getTimelinePages(threadId, [options]).pipe(Effect.map((pages) => pages[0]!)),
        getTimelinePages: (threadId, selections) =>
          service.getThreadSnapshot(threadId).pipe(
            Effect.map(({ projection, snapshotSequence }) =>
              selections.map((options) => {
                const visible = projection.visibleTurnItems.map((row, position) => ({
                  ...row,
                  position,
                }));
                const inView = visible.filter(
                  (row) =>
                    (options.itemId === undefined
                      ? options.view === "activity" ||
                        ["user_message", "assistant_message", "proposed_plan"].includes(
                          row.item.type,
                        )
                      : row.sourceItemId === options.itemId) &&
                    (options.attachmentId === undefined ||
                      (row.item.type === "user_message" &&
                        row.item.attachments.some(
                          (attachment) => attachment.id === options.attachmentId,
                        ))),
                );
                const before = options.beforePosition ?? Number.POSITIVE_INFINITY;
                const backward = options.tail === true || options.beforePosition !== undefined;
                const matching =
                  options.itemId === undefined
                    ? inView.filter(
                        (row) =>
                          row.position > (options.afterPosition ?? -1) && row.position < before,
                      )
                    : inView;
                const items = !backward
                  ? matching.slice(0, options.limit)
                  : matching.slice(Math.max(0, matching.length - options.limit));
                return {
                  items,
                  totalItems: visible.length,
                  ...(options.tail === true
                    ? {
                        snapshotSequence,
                        inProgress: projection.runs.some((run) =>
                          activeRunStatuses.has(run.status),
                        ),
                      }
                    : {}),
                  hasMore:
                    options.tail === true
                      ? matching.length > items.length
                      : options.beforePosition === undefined
                        ? matching.length > items.length
                        : (inView.at(-1)?.position ?? -1) >= before,
                  hasEarlier:
                    (inView[0]?.position ?? Number.POSITIVE_INFINITY) <
                    (items[0]?.position ?? (options.afterPosition ?? -1) + 1),
                };
              }),
            ),
          ),
        getThreadSnapshotWindow: (threadId, options) =>
          service.getThreadSnapshot(threadId).pipe(
            Effect.map((snapshot) => {
              const anchorIndex =
                options.anchorItemId === undefined
                  ? snapshot.projection.visibleTurnItems.length
                  : snapshot.projection.visibleTurnItems.findIndex(
                      (row) =>
                        row.sourceItemId === options.anchorItemId &&
                        (options.anchorThreadId === undefined ||
                          row.sourceThreadId === options.anchorThreadId),
                    ) + 1;
              const candidates = snapshot.projection.visibleTurnItems.slice(0, anchorIndex);
              const startedRuns = new Set<string>();
              const turnStarts = candidates.map(({ item }) => {
                if (item.type !== "user_message" && item.type !== "notification") return false;
                const firstInput = item.runId != null && !startedRuns.has(item.runId);
                if (item.runId != null) startedRuns.add(item.runId);
                return item.type === "notification"
                  ? firstInput
                  : item.inputIntent === "turn_start" || item.inputIntent === "queued_turn";
              });
              const turnAnchors =
                options.userTurnLimit === undefined
                  ? []
                  : candidates.flatMap((_row, index) => (turnStarts[index] ? [index] : []));
              const rawStart = turnAnchors.at(-(THREAD_HISTORY_MAX_RAW_TURNS + 2)) ?? 0;
              const anchors = turnAnchors.filter(
                (index) => index >= rawStart && isThreadHistoryUserTurn(candidates[index]!.item),
              );
              const anchorLimit = (options.userTurnLimit ?? 0) + 2;
              const start =
                anchors.length > 0
                  ? anchors.length < anchorLimit
                    ? rawStart
                    : anchors.at(-anchorLimit)!
                  : Math.max(0, anchorIndex - options.rowLimit);
              const latestTurnRows = anchors.length > 0 ? anchorIndex - anchors.at(-1)! : 0;
              const rowLimit =
                options.rowLimit > 0 &&
                latestTurnRows > 0 &&
                latestTurnRows <= THREAD_HISTORY_PAGE_POLICY.maxTurnItems
                  ? THREAD_HISTORY_TURN_SNAPSHOT_ROW_LIMIT
                  : options.rowLimit;
              const visibleTurnItems = candidates.slice(
                Math.max(start, anchorIndex - Math.max(0, rowLimit)),
              );
              return {
                ...snapshot,
                projection: {
                  ...snapshot.projection,
                  visibleTurnItems: options.deferWork
                    ? deferWorkHistoryRows(visibleTurnItems)
                    : visibleTurnItems,
                },
              };
            }),
          ),
      };

      return service;
    }),
  );
};
