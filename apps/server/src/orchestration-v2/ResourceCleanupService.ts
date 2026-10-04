import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as EventSink from "./EventSink.ts";

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

export interface OwnedResourceCleanupResultV2 {
  readonly outcome: EventSink.LeaseCleanupTaskOutcomeV2;
  readonly evidence: Readonly<Record<string, unknown>>;
}

export const makeOwnedResourceCleanup = (input: {
  readonly sink: Pick<EventSink.EventSinkV2["Service"], "readLeaseCleanupTask">;
  readonly terminals: Pick<TerminalManager.TerminalManager["Service"], "closeOwnedTargets"> &
    Partial<Pick<TerminalManager.TerminalManager["Service"], "captureOwnedTargets">>;
}) => {
  const unknown = (taskId: string, evidence: Readonly<Record<string, unknown>>): OwnedResourceCleanupResultV2 => ({
    outcome: { taskId, result: null, effect: "unknown" }, evidence,
  });
  const rejected = (taskId: string, reason: string): OwnedResourceCleanupResultV2 => ({
    outcome: { taskId, result: "failed", effect: "no_effect" }, evidence: { reason },
  });
  const readCurrentTask = (binding: EventSink.LeaseCleanupTaskBindingV2) =>
    input.sink.readLeaseCleanupTask(binding.effectId).pipe(Effect.map((current) =>
      current !== null && current.bindingSha256 === binding.bindingSha256 &&
      current.recordedAt === binding.recordedAt && current.threadId === binding.threadId &&
      current.lease.ownerIncarnation === binding.lease.ownerIncarnation
        ? current : null,
    ));
  const cleanupOwnedTerminals = (binding: EventSink.LeaseCleanupTaskBindingV2) => Effect.gen(function* () {
    const task = yield* readCurrentTask(binding);
    if (task === null || task.task.kind !== "terminal")
      return rejected(binding.effectId, "terminal_task_binding_changed");
    const closed = yield* input.terminals.closeOwnedTargets(task.task.capture);
    return unknown(binding.effectId, {
      terminalStatus: closed.status,
      managedTargetsOnly: closed.managedTargetsOnly,
      processExitObserved: closed.processExitObserved,
      descendantsQuiescence: closed.descendantsQuiescence,
      futureWakeClosure: closed.futureWakeClosure,
    });
  }).pipe(Effect.catch(() => Effect.succeed(unknown(binding.effectId, { reason: "terminal_cleanup_unavailable" }))));
  const cleanupOwnedAttachments = (binding: EventSink.LeaseCleanupTaskBindingV2) => Effect.gen(function* () {
    const task = yield* readCurrentTask(binding);
    if (task === null || task.task.kind !== "attachment")
      return rejected(binding.effectId, "attachment_task_binding_changed");
    return unknown(binding.effectId, {
      reason: "attachment_generation_and_shared_reference_proof_unavailable",
      attachmentIds: task.task.attachmentIds,
    });
  }).pipe(Effect.catch(() => Effect.succeed(unknown(binding.effectId, { reason: "attachment_cleanup_unavailable" }))));
  const captureOwnedTargets = input.terminals.captureOwnedTargets;
  const captureOwnedTerminalTargets = captureOwnedTargets === undefined ? undefined
    : (ownerBirth: TerminalManager.TerminalOwnerBirth) => captureOwnedTargets({
      threadId: ownerBirth.threadId, ownerBirth,
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
    binding: EventSink.LeaseCleanupTaskBindingV2,
  ) => Effect.Effect<OwnedResourceCleanupResultV2, ResourceCleanupError>;
  readonly cleanupOwnedAttachments?: (
    binding: EventSink.LeaseCleanupTaskBindingV2,
  ) => Effect.Effect<OwnedResourceCleanupResultV2, ResourceCleanupError>;
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
    const config = yield* ServerConfig.ServerConfig;
    const sink = yield* EventSink.EventSinkV2;
    return {
      ...makeOwnedResourceCleanup({ sink, terminals }),
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
