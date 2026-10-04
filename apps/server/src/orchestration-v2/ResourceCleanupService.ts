import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  parseAttachmentIdFromRelativePath,
  parseThreadSegmentFromAttachmentId,
  resolveAttachmentPathById,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";
import { resolveAttachmentRelativePath } from "../attachmentPaths.ts";
import * as ServerConfig from "../config.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as EventSink from "./EventSink.ts";
import type {
  DeletionWorktreeCleanupInputV1,
  DeletionWorktreeCleanupResultV1,
} from "./ThreadDeletion.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";

export const terminalOwnerObservationLive = Layer.effect(
  TerminalManager.TerminalOwnerObservation,
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    return {
      observeCurrentBirth: (threadId: string) =>
        sink.readApplicationBirthRecord(ThreadId.make(threadId)),
    };
  }),
);

export class ResourceCleanupError extends Schema.TaggedError<ResourceCleanupError>()(
  "ResourceCleanupError",
  {
    operation: Schema.Literals(["terminal", "attachment"]),
    threadId: Schema.optional(Schema.String),
    attachmentId: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {}

export type AttachmentNamespaceScanResultV1 =
  | {
      readonly status: "completed";
      readonly matchingPaths: ReadonlyArray<string>;
      readonly removedPaths: ReadonlyArray<string>;
      readonly retainedPaths: ReadonlyArray<string>;
      readonly rootAbsent: boolean;
    }
  | {
      readonly status: "retryable_failure";
      readonly removedPaths: ReadonlyArray<string>;
      readonly remainingPaths: ReadonlyArray<string>;
      readonly reason: string;
    }
  | {
      readonly status: "unknown";
      readonly removedPaths: ReadonlyArray<string>;
      readonly reason: string;
    };

export const makeAttachmentNamespaceScan =
  (fileSystem: Pick<FileSystem.FileSystem, "readDirectory" | "remove">) =>
  (input: {
    readonly configuredRoot: string;
    readonly namespaceSegment: string;
    readonly retainedRelativePaths: ReadonlyArray<string>;
    readonly beforeRemove?: Effect.Effect<void, EventSink.EventSinkV2Error | string>;
  }) =>
    Effect.gen(function* () {
      const removedPaths: string[] = [];
      const listed = yield* fileSystem
        .readDirectory(input.configuredRoot, { recursive: false })
        .pipe(Effect.result);
      if (listed._tag === "Failure") {
        return listed.failure.reason._tag === "NotFound"
          ? ({
              status: "completed",
              matchingPaths: [],
              removedPaths: [],
              retainedPaths: [],
              rootAbsent: true,
            } as const)
          : ({
              status: "unknown",
              removedPaths,
              reason: "attachment_root_inventory_unavailable",
            } as const);
      }
      if (
        !Array.isArray(listed.success) ||
        listed.success.some((entry) => typeof entry !== "string")
      )
        return {
          status: "unknown",
          removedPaths,
          reason: "attachment_root_inventory_invalid",
        } as const;
      const matchingPaths = listed.success
        .filter((entry) => {
          if (entry.length === 0 || entry === "." || entry === ".." || /[\\/\0]/.test(entry))
            return false;
          const id = parseAttachmentIdFromRelativePath(entry);
          return id !== null && parseThreadSegmentFromAttachmentId(id) === input.namespaceSegment;
        })
        .sort();
      const keep = new Set(input.retainedRelativePaths);
      const retainedPaths = matchingPaths.filter((entry) => keep.has(entry));
      const candidates = matchingPaths.filter((entry) => !keep.has(entry));
      const remainingPaths: string[] = [];
      for (const entry of candidates) {
        if (input.beforeRemove !== undefined) {
          const qualified = yield* input.beforeRemove.pipe(Effect.result);
          if (qualified._tag === "Failure")
            return {
              status: "unknown",
              removedPaths,
              reason: "attachment_namespace_basis_changed",
            } as const;
        }
        const path = resolveAttachmentRelativePath({
          attachmentsDir: input.configuredRoot,
          relativePath: entry,
        });
        if (path === null)
          return {
            status: "unknown",
            removedPaths,
            reason: "attachment_path_not_contained",
          } as const;
        const removed = yield* fileSystem
          .remove(path, { recursive: false, force: false })
          .pipe(Effect.result);
        if (removed._tag === "Success" || removed.failure.reason._tag === "NotFound")
          removedPaths.push(entry);
        else remainingPaths.push(entry);
      }
      return remainingPaths.length > 0
        ? ({
            status: "retryable_failure",
            removedPaths,
            remainingPaths,
            reason: "attachment_remove_failed",
          } as const)
        : ({
            status: "completed",
            matchingPaths,
            removedPaths,
            retainedPaths,
            rootAbsent: false,
          } as const);
    });

export interface AttachmentNamespaceCleanupInvocationResultV1 {
  readonly status: "completed" | "retryable" | "unknown" | "stale" | "unavailable";
  readonly basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1;
  readonly observation: EventSink.AttachmentNamespaceCleanupObservationV1 | null;
  readonly record: EventSink.AttachmentNamespaceCleanupRecordResultV1 | null;
  readonly anchorOrdinal: number | null;
}

export const makeAttachmentNamespaceCleanup = (input: {
  readonly sink: Pick<
    EventSink.EventSinkV2["Service"],
    | "readAttachmentNamespaceCleanupTask"
    | "readAttachmentNamespaceCleanupBasis"
    | "readAttachmentNamespaceCleanupObservation"
    | "recordAttachmentNamespaceCleanupObservation"
  >;
  readonly executor: ThreadCommandExecutor.ThreadCommandExecutor["Service"];
  readonly fileSystem: Pick<FileSystem.FileSystem, "readDirectory" | "remove">;
  readonly configuredRoot: string;
  readonly path: Pick<Path.Path, "resolve">;
}) => {
  const configuredRoot = input.path.resolve(input.configuredRoot);
  const scan = makeAttachmentNamespaceScan(input.fileSystem);
  const observe = (
    basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1,
    outcome: EventSink.AttachmentNamespaceCleanupObservationV1["outcome"],
  ) =>
    DateTime.now.pipe(
      Effect.map((now): EventSink.AttachmentNamespaceCleanupObservationV1 => ({
        version: 1,
        producer: "attachment_namespace",
        effectId: basis.task.effectId,
        bindingSha256: basis.task.bindingSha256,
        workerId: basis.claim.workerId,
        expectedAttempt: basis.claim.expectedAttempt,
        basisEventSequence: basis.basisEventSequence,
        configuredRoot,
        namespaceSegment: toSafeThreadAttachmentSegment(basis.task.threadId),
        observedAt: DateTime.formatIso(now),
        outcome,
      })),
    );
  return (offered: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1) =>
    input.executor.withLock(
      offered.task.threadId,
      Effect.gen(function* () {
        const unavailable = (): AttachmentNamespaceCleanupInvocationResultV1 => ({
          status: "unavailable",
          basis: offered,
          observation: null,
          record: null,
          anchorOrdinal: null,
        });
        const task = yield* input.sink
          .readAttachmentNamespaceCleanupTask(offered.task.effectId)
          .pipe(Effect.result);
        if (
          task._tag === "Failure" ||
          task.success === null ||
          nativeCreationCanonicalJson(task.success) !== nativeCreationCanonicalJson(offered.task)
        )
          return unavailable();
        const read = yield* input.sink
          .readAttachmentNamespaceCleanupBasis({
            effectId: offered.task.effectId,
            workerId: offered.claim.workerId,
            expectedAttempt: offered.claim.expectedAttempt,
          })
          .pipe(Effect.result);
        if (
          read._tag === "Failure" ||
          read.success.status === "unavailable" ||
          nativeCreationCanonicalJson(read.success.task) !==
            nativeCreationCanonicalJson(offered.task) ||
          read.success.claim.workerId !== offered.claim.workerId ||
          read.success.claim.expectedAttempt !== offered.claim.expectedAttempt
        )
          return unavailable();
        const basis = read.success;
        if (basis.status !== "ready") {
          const observation = yield* observe(
            basis,
            basis.status === "superseded"
              ? { status: "superseded", replacementBirth: basis.replacementBirth }
              : { status: "unsafe_namespace" },
          );
          const record = yield* input.sink
            .recordAttachmentNamespaceCleanupObservation({ basis, observation })
            .pipe(Effect.result);
          return {
            status: record._tag === "Success" ? record.success.status : "unknown",
            basis,
            observation,
            record: record._tag === "Success" ? record.success : null,
            anchorOrdinal: null,
          } satisfies AttachmentNamespaceCleanupInvocationResultV1;
        }
        if (
          basis.task.reference.mode === "prune_thread" &&
          (offered.status !== "ready" ||
            basis.retentionSourceEvidence === undefined ||
            offered.retentionSourceEvidence === undefined ||
            nativeCreationCanonicalJson(basis.retentionSourceEvidence) !==
              nativeCreationCanonicalJson(offered.retentionSourceEvidence) ||
            nativeCreationCanonicalJson([...basis.retainedRelativePaths].sort()) !==
              nativeCreationCanonicalJson([...offered.retainedRelativePaths].sort()))
        )
          return unavailable();
        const pending = yield* observe(basis, {
          status: "unknown",
          removedPaths: [],
          reason: "attachment_namespace_invocation_pending",
        });
        const anchored = yield* input.sink
          .recordAttachmentNamespaceCleanupObservation({ basis, observation: pending })
          .pipe(Effect.result);
        if (
          anchored._tag === "Failure" ||
          anchored.success.status !== "unknown" ||
          anchored.success.ordinal === null ||
          anchored.success.effectId !== basis.task.effectId
        )
          return {
            status: "unknown",
            basis,
            observation: pending,
            record: anchored._tag === "Success" ? anchored.success : null,
            anchorOrdinal: null,
          } satisfies AttachmentNamespaceCleanupInvocationResultV1;
        const anchorOrdinal = anchored.success.ordinal;
        const anchor = yield* input.sink
          .readAttachmentNamespaceCleanupObservation(basis.task.effectId)
          .pipe(Effect.result);
        if (
          anchor._tag === "Failure" ||
          anchor.success === null ||
          anchor.success.ordinal !== anchorOrdinal ||
          anchor.success.status !== "unknown" ||
          nativeCreationCanonicalJson(anchor.success.task) !==
            nativeCreationCanonicalJson(basis.task) ||
          nativeCreationCanonicalJson(anchor.success.basis) !==
            nativeCreationCanonicalJson(basis) ||
          nativeCreationCanonicalJson(anchor.success.observation) !==
            nativeCreationCanonicalJson(pending)
        )
          return {
            status: "unknown",
            basis,
            observation: pending,
            record: anchored.success,
            anchorOrdinal,
          } satisfies AttachmentNamespaceCleanupInvocationResultV1;
        const requalify = Effect.gen(function* () {
          const current = yield* input.sink.readAttachmentNamespaceCleanupBasis({
            effectId: basis.task.effectId,
            workerId: basis.claim.workerId,
            expectedAttempt: basis.claim.expectedAttempt,
          });
          if (
            current.status !== "ready" ||
            nativeCreationCanonicalJson(current.task) !== nativeCreationCanonicalJson(basis.task) ||
            current.claim.workerId !== basis.claim.workerId ||
            current.claim.expectedAttempt !== basis.claim.expectedAttempt ||
            nativeCreationCanonicalJson([...current.retainedRelativePaths].sort()) !==
              nativeCreationCanonicalJson([...basis.retainedRelativePaths].sort()) ||
            (basis.task.reference.mode === "prune_thread" &&
              (current.retentionSourceEvidence === undefined ||
                nativeCreationCanonicalJson(current.retentionSourceEvidence) !==
                  nativeCreationCanonicalJson(basis.retentionSourceEvidence)))
          )
            return yield* Effect.fail("attachment_namespace_basis_changed");
        });
        const qualified = yield* requalify.pipe(Effect.result);
        const namespaceSegment = toSafeThreadAttachmentSegment(basis.task.threadId);
        const result: AttachmentNamespaceScanResultV1 =
          qualified._tag === "Failure" || namespaceSegment === null
            ? { status: "unknown", removedPaths: [], reason: "attachment_namespace_basis_changed" }
            : yield* scan({
                configuredRoot,
                namespaceSegment,
                retainedRelativePaths: basis.retainedRelativePaths,
                beforeRemove: requalify,
              });
        const observation = yield* observe(basis, result);
        const committed = yield* input.sink
          .recordAttachmentNamespaceCleanupObservation({ basis, observation })
          .pipe(Effect.result);
        if (committed._tag === "Success" && committed.success.status !== "stale")
          return {
            status: committed.success.status,
            basis,
            observation,
            record: committed.success,
            anchorOrdinal,
          } satisfies AttachmentNamespaceCleanupInvocationResultV1;
        const unknown = yield* observe(basis, {
          status: "unknown",
          removedPaths: result.removedPaths,
          reason: "attachment_namespace_observation_not_committed",
        });
        const retained = yield* input.sink
          .recordAttachmentNamespaceCleanupObservation({ basis, observation: unknown })
          .pipe(Effect.result);
        return {
          status: "unknown",
          basis,
          observation: unknown,
          record: retained._tag === "Success" ? retained.success : null,
          anchorOrdinal,
        } satisfies AttachmentNamespaceCleanupInvocationResultV1;
      }),
    );
};

export interface OwnedResourceCleanupResultV2 {
  readonly outcome: EventSink.LeaseCleanupTaskOutcomeV2;
  readonly evidence: Readonly<Record<string, unknown>>;
  readonly observation?: EventSink.ManagedTerminalDeletionObservationV1;
}

export interface OwnedResourceCleanupCorrelationV1 {
  readonly workerId: string;
  readonly expectedAttempt: number;
}

export const makeOwnedResourceCleanup = (input: {
  readonly sink: Pick<EventSink.EventSinkV2["Service"], "readDeletionCleanupTask">;
  readonly terminals: Pick<TerminalManager.TerminalManager["Service"], "closeOwnedTargets"> &
    Partial<Pick<TerminalManager.TerminalManager["Service"], "captureOwnedTargets">>;
}) => {
  const unknown = (
    taskId: string,
    evidence: Readonly<Record<string, unknown>>,
  ): OwnedResourceCleanupResultV2 => ({
    outcome: { taskId, result: null, effect: "unknown" },
    evidence,
  });
  const rejected = (taskId: string, reason: string): OwnedResourceCleanupResultV2 => ({
    outcome: { taskId, result: "failed", effect: "no_effect" },
    evidence: { reason },
  });
  const readCurrentTask = (binding: EventSink.DeletionCleanupTaskBindingV1) =>
    input.sink
      .readDeletionCleanupTask(binding.effectId)
      .pipe(
        Effect.map((current) =>
          current !== null &&
          current.bindingSha256 === binding.bindingSha256 &&
          current.recordedAt === binding.recordedAt &&
          current.threadId === binding.threadId &&
          current.version === binding.version &&
          current.effectId === binding.effectId
            ? current
            : null,
        ),
      );
  const cleanupOwnedTerminals = (
    binding: EventSink.DeletionCleanupTaskBindingV1,
    correlation: OwnedResourceCleanupCorrelationV1,
  ) =>
    Effect.gen(function* () {
      if (
        correlation.workerId.length === 0 ||
        !Number.isSafeInteger(correlation.expectedAttempt) ||
        correlation.expectedAttempt <= 0
      )
        return rejected(binding.effectId, "terminal_claim_correlation_invalid");
      const task = yield* readCurrentTask(binding);
      if (task === null || task.task.kind !== "terminal")
        return rejected(binding.effectId, "terminal_task_binding_changed");
      const closed = yield* input.terminals.closeOwnedTargets(task.task.capture);
      const now = yield* DateTime.now;
      const observation: EventSink.ManagedTerminalDeletionObservationV1 = {
        version: 1,
        kind: "managed_terminal",
        effectId: task.effectId,
        bindingSha256: task.bindingSha256,
        workerId: correlation.workerId,
        expectedAttempt: correlation.expectedAttempt,
        capture: task.task.capture,
        result: closed,
        observedAt: DateTime.formatIso(now),
      };
      return {
        ...unknown(binding.effectId, {
          terminalStatus: closed.status,
          managedTargetsOnly: closed.managedTargetsOnly,
          processExitObserved: closed.processExitObserved,
          descendantsQuiescence: closed.descendantsQuiescence,
          futureWakeClosure: closed.futureWakeClosure,
        }),
        observation,
      } satisfies OwnedResourceCleanupResultV2;
    }).pipe(
      Effect.catch(() =>
        Effect.succeed(unknown(binding.effectId, { reason: "terminal_cleanup_unavailable" })),
      ),
    );
  const cleanupOwnedAttachments = (
    binding: EventSink.DeletionCleanupTaskBindingV1,
    correlation: OwnedResourceCleanupCorrelationV1,
  ) =>
    Effect.gen(function* () {
      if (
        correlation.workerId.length === 0 ||
        !Number.isSafeInteger(correlation.expectedAttempt) ||
        correlation.expectedAttempt <= 0
      )
        return rejected(binding.effectId, "attachment_claim_correlation_invalid");
      const task = yield* readCurrentTask(binding);
      if (task === null || task.task.kind !== "attachment")
        return rejected(binding.effectId, "attachment_task_binding_changed");
      return unknown(binding.effectId, {
        reason: "attachment_generation_and_shared_reference_proof_unavailable",
        attachmentIds: task.task.attachmentIds,
      });
    }).pipe(
      Effect.catch(() =>
        Effect.succeed(unknown(binding.effectId, { reason: "attachment_cleanup_unavailable" })),
      ),
    );
  const captureOwnedTargets = input.terminals.captureOwnedTargets;
  const captureOwnedTerminalTargets =
    captureOwnedTargets === undefined
      ? undefined
      : (ownerBirth: TerminalManager.TerminalOwnerBirth) =>
          captureOwnedTargets({
            threadId: ownerBirth.threadId,
            ownerBirth,
          });
  return {
    cleanupOwnedTerminals,
    cleanupOwnedAttachments,
    ...(captureOwnedTerminalTargets === undefined ? {} : { captureOwnedTerminalTargets }),
  };
};

export class ResourceCleanupService extends Context.Reference<{
  readonly cleanupTerminals: (threadId: string) => Effect.Effect<void, ResourceCleanupError>;
  readonly cleanupAttachments: (
    attachmentIds: ReadonlyArray<string>,
  ) => Effect.Effect<void, ResourceCleanupError>;
  readonly cleanupOwnedTerminals?: (
    binding: EventSink.DeletionCleanupTaskBindingV1,
    correlation: OwnedResourceCleanupCorrelationV1,
  ) => Effect.Effect<OwnedResourceCleanupResultV2, ResourceCleanupError>;
  readonly cleanupOwnedAttachments?: (
    binding: EventSink.DeletionCleanupTaskBindingV1,
    correlation: OwnedResourceCleanupCorrelationV1,
  ) => Effect.Effect<OwnedResourceCleanupResultV2, ResourceCleanupError>;
  readonly cleanupAttachmentNamespace?: (
    basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1,
  ) => Effect.Effect<AttachmentNamespaceCleanupInvocationResultV1>;
  readonly cleanupOwnedWorktree?: (
    input: DeletionWorktreeCleanupInputV1,
  ) => Effect.Effect<DeletionWorktreeCleanupResultV1>;
  readonly captureOwnedTerminalTargets?: (
    ownerBirth: TerminalManager.TerminalOwnerBirth,
  ) => Effect.Effect<TerminalManager.TerminalOwnedTargetCapture>;
}>("t3/orchestration-v2/ResourceCleanupService", {
  defaultValue: () => ({
    cleanupTerminals: () => Effect.void,
    cleanupAttachments: () => Effect.void,
  }),
}) {}

export const live = Layer.effect(
  ResourceCleanupService,
  Effect.gen(function* () {
    const terminals = yield* TerminalManager.TerminalManager;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    const sink = yield* EventSink.EventSinkV2;
    const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    return {
      ...makeOwnedResourceCleanup({ sink, terminals }),
      cleanupAttachmentNamespace: makeAttachmentNamespaceCleanup({
        sink,
        executor,
        fileSystem,
        configuredRoot: config.attachmentsDir,
        path,
      }),
      cleanupTerminals: (threadId: string) =>
        terminals
          .close({ threadId, deleteHistory: true })
          .pipe(
            Effect.mapError(
              (cause) => new ResourceCleanupError({ operation: "terminal", threadId, cause }),
            ),
          ),
      cleanupAttachments: (attachmentIds: ReadonlyArray<string>) =>
        Effect.forEach(
          attachmentIds,
          (attachmentId) => {
            const path = resolveAttachmentPathById({
              attachmentsDir: config.attachmentsDir,
              attachmentId,
            });
            return path === null
              ? Effect.void
              : fileSystem
                  .remove(path, { force: true })
                  .pipe(
                    Effect.mapError(
                      (cause) =>
                        new ResourceCleanupError({ operation: "attachment", attachmentId, cause }),
                    ),
                  );
          },
          { discard: true, concurrency: 4 },
        ),
    };
  }),
);
