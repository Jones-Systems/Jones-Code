import {
  type RunId,
  type ThreadId,
  type OrchestrationV2LegacyBootstrapPolicy,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  RecordedAppThread,
  RecordedRun,
  type RecordedThreadProjection,
  type RecordedLifecycleEvent,
  type RecordedServerCommand,
  type LegacyGuardRejectionDeleteCommand,
  type LegacyFailureDeleteCommand,
  type LegacyPreparationUpdate,
} from "../../orchestration-v2/RecordedTypes.ts";
import type * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import {
  ProjectionStoreReadError,
  ProjectionStoreV2Error,
} from "../../orchestration-v2/ProjectionStore.ts";
import { legacyBootstrapBirth } from "../../orchestration-v2/LegacyBootstrap.ts";
import * as Storage from "./Storage.ts";
import * as M from "./RecordedMetadata.ts";
import { LegacyStorageError, type CommandBinding } from "./StorageSchema.ts";

const decodeRecordedThread = Schema.decodeEffect(RecordedAppThread);
const decodeRecordedRun = Schema.decodeEffect(RecordedRun);
const decodeRecordedRuns = Schema.decodeUnknownEffect(Schema.Array(RecordedRun));
const isProjectionStoreError = Schema.is(Schema.suspend(() => ProjectionStoreV2Error));

export type LegacyCommand =
  | Exclude<RecordedServerCommand, { readonly type: "legacy-bootstrap.failure-delete" }>
  | LegacyGuardRejectionDeleteCommand
  | LegacyFailureDeleteCommand;
export type LegacyMutation =
  | { readonly kind: "claim"; readonly policy: OrchestrationV2LegacyBootstrapPolicy }
  | { readonly kind: "birth"; readonly policy: OrchestrationV2LegacyBootstrapPolicy }
  | { readonly kind: "release"; readonly policy: OrchestrationV2LegacyBootstrapPolicy }
  | { readonly kind: "progress"; readonly runId: RunId; readonly update: LegacyPreparationUpdate }
  | { readonly kind: "failure"; readonly runId: RunId }
  | {
      readonly kind: "delete";
      readonly command: LegacyGuardRejectionDeleteCommand | LegacyFailureDeleteCommand;
    };
export interface LegacyCommitContext {
  readonly mutation: LegacyMutation;
  readonly binding: CommandBinding;
  readonly expectedThreadRevision: number | null;
  readonly expectedRunRevision: number | null;
  readonly expectedThreadMetadata: string;
  readonly expectedRunMetadata: string;
  readonly expectedThread: string;
  readonly expectedRun: string;
  readonly deletionTarget?: string;
}
type Store = ReturnType<typeof Storage.make>;
const refused = (reason: string) => Effect.fail(new LegacyStorageError({ reason }));

export const capture = Effect.fn("LegacyDecision.capture")(function* (
  storage: Store,
  command: LegacyCommand,
  events: readonly RecordedLifecycleEvent[],
  deletionTarget?: string,
) {
  const policy = "legacyBootstrap" in command ? command.legacyBootstrap : undefined;
  const mutation: LegacyMutation | undefined =
    command.type === "legacy-bootstrap.guard-rejection-delete" ||
    command.type === "legacy-bootstrap.failure-delete"
      ? { kind: "delete", command }
      : command.type === "prepared-run.progress" && command.legacyPreparationUpdate !== undefined
        ? { kind: "progress", runId: command.runId, update: command.legacyPreparationUpdate }
        : command.type === "prepared-run.fail" &&
            events.some(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === command.runId &&
                event.payload.status === "failed" &&
                // Cancellation readback needs the preparation as it stood at failure,
                // even when the failure supplies no authority to delete the thread.
                (event.payload.legacyPreparationFailureDecision !== undefined ||
                  (event.payload.legacyPreparation !== undefined &&
                    event.payload.legacyBootstrap !== undefined)),
            )
          ? { kind: "failure", runId: command.runId }
          : policy === undefined
            ? undefined
            : command.type === "thread.create" || command.type === "thread.metadata.update"
              ? { kind: "claim", policy }
              : command.type === "message.dispatch"
                ? { kind: "birth", policy }
                : command.type === "prepared-run.release"
                  ? { kind: "release", policy }
                  : undefined;
  if (mutation === undefined) return undefined;
  if (!("threadId" in command)) return yield* refused("legacy_thread_required");
  const thread = yield* storage.readThread(command.threadId);
  const runId = "runId" in command ? command.runId : undefined;
  const run = runId === undefined ? undefined : yield* storage.readRun(runId);
  const projectId =
    policy?.projectId ?? run?.run.legacyBootstrap?.projectId ?? thread?.thread.projectId;
  const created = events.find((event) => event.type === "thread.created");
  const createdAt =
    thread?.thread.createdAt ??
    (created?.type === "thread.created" ? created.payload.createdAt : undefined);
  if (projectId === undefined || createdAt === undefined)
    return yield* refused("legacy_incarnation_unavailable");
  const binding: CommandBinding = {
    commandId: command.commandId,
    threadId: command.threadId,
    projectId,
    threadCreatedAt: DateTime.formatIso(createdAt),
    kind: mutation.kind === "delete" ? "delete" : "lifecycle",
    bindingJson: M.canonical(command),
  };
  return {
    mutation,
    binding,
    expectedThreadRevision: thread?.revision ?? null,
    expectedRunRevision: run?.revision ?? null,
    expectedThreadMetadata: M.canonical(
      thread === undefined ? {} : M.threadMetadata(thread.thread),
    ),
    expectedRunMetadata: M.canonical(run === undefined ? {} : M.runMetadata(run.run)),
    expectedThread: M.canonical(thread?.thread ?? null),
    expectedRun: M.canonical(run?.run ?? null),
    ...(deletionTarget === undefined ? {} : { deletionTarget }),
  } satisfies LegacyCommitContext;
});

export const assertDeleteReplay = Effect.fn("LegacyDecision.assertDeleteReplay")(function* (
  storage: Store,
  command: LegacyGuardRejectionDeleteCommand | LegacyFailureDeleteCommand,
) {
  const record = yield* storage.findCommand(command.commandId);
  const receipt = yield* storage.readLegacyReceipt(command.commandId);
  const events = yield* storage.readLifecycleByCommandId(command.commandId);
  // Historical private receipts have no sparse command entry; their exact provenance is still required.
  const evidence = events[0];
  const tombstone = events[1];
  if (
    receipt?.domain !== "legacy-delete" ||
    receipt.receipt.status !== "accepted" ||
    receipt.receipt.commandType !== command.type ||
    receipt.receipt.threadId !== command.threadId ||
    events.at(-1)?.sequence !== receipt.receipt.resultSequence ||
    evidence?.event.type !== "run.updated" ||
    evidence.event.payload.id !== command.runId ||
    tombstone?.event.type !== "thread.deleted" ||
    tombstone.event.threadId !== command.threadId ||
    tombstone.sequence <= evidence.sequence ||
    !command.legacyBootstrap.ownsNewThread ||
    (record !== undefined && record.binding.bindingJson !== M.canonical(command))
  )
    return yield* refused("delete_replay_proof_unavailable");
  const deletion =
    command.type === "legacy-bootstrap.guard-rejection-delete"
      ? evidence.event.payload.legacyReleaseDecision?.deletion
      : evidence.event.payload.legacyPreparationFailureDecision?.deletion?.provenance;
  if (
    deletion === undefined ||
    deletion.commandId !== command.commandId ||
    deletion.evidenceEventId !== evidence.event.id ||
    deletion.runId !== command.runId ||
    M.canonical(deletion.policy) !== M.canonical(command.legacyBootstrap) ||
    M.canonical(deletion.control) !==
      M.canonical(
        command.legacyNoControl ??
          ("legacyOwnedControl" in command ? command.legacyOwnedControl : undefined),
      )
  )
    return yield* refused("delete_replay_binding_conflict");
  const policy = command.legacyBootstrap;
  const proof = legacyBootstrapBirth({
    policy,
    claimEvents: yield* storage.readLifecycleByCommandId(policy.createCommandId),
    birthEvents: yield* storage.readLifecycleByCommandId(policy.birthCommandId),
  });
  const claim = yield* storage.readLegacyReceipt(policy.createCommandId);
  const birth = yield* storage.readLegacyReceipt(policy.birthCommandId);
  const original =
    command.type === "legacy-bootstrap.guard-rejection-delete"
      ? evidence.event.payload.legacyReleaseDecision
      : evidence.event.payload.legacyPreparationFailureDecision;
  const preparation = evidence.event.payload.legacyPreparation;
  if (
    proof.type !== "valid" ||
    claim?.domain !== "lifecycle" ||
    birth?.domain !== "lifecycle" ||
    claim.receipt.status !== "accepted" ||
    claim.receipt.commandType !== "thread.create" ||
    birth.receipt.status !== "accepted" ||
    birth.receipt.commandType !== "message.dispatch" ||
    original === undefined ||
    preparation === undefined ||
    deletion.claimEventId !== proof.claimEventId ||
    deletion.claimSequence !== proof.claimSequence ||
    deletion.claimReceiptSequence !== claim.receipt.resultSequence ||
    deletion.birthEventId !== proof.birthEventId ||
    deletion.birthSequence !== proof.sequence ||
    deletion.birthReceiptSequence !== birth.receipt.resultSequence ||
    deletion.preparationGeneration !== preparation.generation ||
    M.canonical((({ runId: _runId, ...claimPolicy }) => claimPolicy)(policy)) !==
      M.canonical(tombstone.event.payload.legacyBootstrapClaim)
  )
    return yield* refused("delete_replay_anchors_unavailable");
  const originalCommandId =
    command.type === "legacy-bootstrap.guard-rejection-delete"
      ? policy.releaseCommandId
      : evidence.event.payload.legacyPreparationFailureDecision?.failureCommandId;
  if (originalCommandId === undefined) return yield* refused("delete_original_decision_missing");
  const originalReceipt = yield* storage.readLegacyReceipt(originalCommandId);
  const originalEvent = (yield* storage.readLifecycleByCommandId(originalCommandId)).at(-1);
  const { deletion: _deletion, ...beforeDelete } = original;
  if (
    originalReceipt?.domain !== "lifecycle" ||
    originalReceipt.receipt.threadId !== command.threadId ||
    originalReceipt.receipt.status !==
      (command.type === "legacy-bootstrap.guard-rejection-delete" ? "rejected" : "accepted") ||
    originalReceipt.receipt.commandType !==
      (command.type === "legacy-bootstrap.guard-rejection-delete"
        ? "prepared-run.release"
        : "prepared-run.fail") ||
    originalEvent?.event.type !== "run.updated" ||
    originalEvent.event.payload.id !== command.runId ||
    originalEvent.event.id !== original.evidenceEventId ||
    originalEvent.sequence !== originalReceipt.receipt.resultSequence ||
    originalEvent.sequence >= evidence.sequence ||
    M.canonical(beforeDelete) !==
      M.canonical(
        command.type === "legacy-bootstrap.guard-rejection-delete"
          ? originalEvent.event.payload.legacyReleaseDecision
          : originalEvent.event.payload.legacyPreparationFailureDecision,
      )
  )
    return yield* refused("delete_original_decision_unavailable");
  if (deletion.type === "bound_control") {
    const cleanup = yield* storage.readTerminalCleanup(
      `effect:${command.commandId}:terminal.cleanup`,
    );
    if (cleanup === undefined || M.canonical(cleanup.control) !== M.canonical(deletion.control))
      return yield* refused("delete_cleanup_unavailable");
  }
});

// Internal Core adapter only. Public readers and wire projections retain their upstream shapes.
export const withRetainedRecords = (
  publicStore: Projections.ProjectionStoreV2["Service"],
  storage: Store,
) => {
  const thread = Effect.fn("LegacyDecision.readThread")(function* (threadId: ThreadId) {
    const publicThread = yield* publicStore.getThread(threadId);
    const retained = yield* storage.readThread(threadId);
    return (
      retained?.thread ??
      (publicStore.legacyRecords === undefined
        ? yield* decodeRecordedThread(publicThread)
        : yield* publicStore.legacyRecords.thread(publicThread))
    );
  });
  const run = Effect.fn("LegacyDecision.readRun")(function* (value: RecordedRun) {
    return (
      (yield* storage.readRun(value.id))?.run ??
      (publicStore.legacyRecords === undefined
        ? yield* decodeRecordedRun(value)
        : yield* publicStore.legacyRecords.run(value))
    );
  });
  const mapError = (threadId: ThreadId) =>
    Effect.mapError((cause: unknown) =>
      isProjectionStoreError(cause) ? cause : new ProjectionStoreReadError({ threadId, cause }),
    );
  const readThreadRecords = Effect.fn("LegacyDecision.readThreadRecords")(function* <
    K extends Projections.ProjectionRecordField,
  >(
    threadId: ThreadId,
    fields: readonly K[],
    filter?: Projections.ProjectionRecordFilter,
  ): Effect.fn.Return<
    Pick<RecordedThreadProjection, "thread" | K>,
    Projections.ProjectionStoreV2Error | LegacyStorageError | Schema.SchemaError
  > {
    const records = yield* publicStore.getThreadRecords(threadId, fields, filter);
    const retainedThread = yield* thread(threadId);
    // Preserve the generic selected-field shape without adding fields the caller did not request.
    const runs =
      "runs" in records
        ? yield* Effect.forEach(yield* decodeRecordedRuns(records.runs), run)
        : undefined;
    // Every added private value above was decoded/composed; this assertion only relates the selected keys to K.
    return {
      ...records,
      thread: retainedThread,
      ...(runs === undefined ? {} : { runs }),
    } as Pick<RecordedThreadProjection, "thread" | K>;
  });
  return {
    ...publicStore,
    getThread: (threadId: ThreadId) => thread(threadId).pipe(mapError(threadId)),
    getThreadRecords: Effect.fn("LegacyDecision.getThreadRecords")(function* <
      K extends Projections.ProjectionRecordField,
    >(
      threadId: ThreadId,
      fields: readonly K[],
      filter?: Projections.ProjectionRecordFilter,
    ): Effect.fn.Return<
      Pick<RecordedThreadProjection, "thread" | K>,
      Projections.ProjectionStoreV2Error
    > {
      return yield* readThreadRecords(threadId, fields, filter).pipe(mapError(threadId));
    }),
    getThreadProjection: Effect.fn("LegacyDecision.getThreadProjection")(
      function* (threadId: ThreadId) {
        const projection = yield* publicStore.getThreadProjection(threadId);
        return {
          ...projection,
          thread: yield* thread(threadId),
          runs: yield* Effect.forEach(projection.runs, run),
        };
      },
      (effect, threadId) => effect.pipe(mapError(threadId)),
    ),
  };
};
