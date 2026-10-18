import * as EffectOutbox from "./EffectOutbox.ts";
import { awaitThreadCreationCleanup } from "./ThreadDeletion.ts";
import {
  canonicalLegacyPayload,
  legacyBootstrapCreateCommandId,
  legacyPayloadHash,
} from "./LegacyBootstrap.ts";
import {
  ChatAttachmentId,
  CommandId,
  OrchestrationDispatchCommandError,
  QueueDispatchCommand,
  RunId,
  type ChatAttachment,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import { remapComposerContextAttachments } from "@t3tools/shared/composerContextReferences";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Config from "../config.ts";
import {
  createDeterministicAttachmentId,
  planAttachmentClaim,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import { parseBase64DataUrl } from "../imageMime.ts";
import * as ProjectClone from "../project/ProjectCloneTracker.ts";
import * as Projects from "../project/ProjectService.ts";
import * as EventSink from "./EventSink.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Launch from "./ThreadLaunchService.ts";
import * as Intake from "./ThreadMessageIntake.ts";
import * as Claims from "./AttachmentClaims.ts";

export class QueueCompatibilityError extends Schema.TaggedError<QueueCompatibilityError>()(
  "QueueCompatibilityError",
  {
    reason: Schema.Literals([
      "invalid_command",
      "dispatch_guard_bootstrap_unsupported",
      "dispatch_guard_rejected",
      "orchestration_dispatch_failed",
    ]),
    cause: Schema.Defect(),
  },
) {}
export class QueueCompatibility extends Context.Service<
  QueueCompatibility,
  {
    readonly dispatch: (
      payload: unknown,
      transport?: "http" | "legacy_websocket",
    ) => Effect.Effect<{ sequence: number }, QueueCompatibilityError>;
  }
>()("t3/orchestration-v2/QueueCompatibility") {}

const make = Effect.gen(function* () {
  const threads = yield* Threads.ThreadManagementService;
  const launch = yield* Launch.ThreadLaunchService;
  const clones = yield* ProjectClone.ProjectCloneTracker;
  const projects = yield* Projects.ProjectService;
  const events = yield* EventSink.EventSinkV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const context = yield* Effect.context<
    | Effect.Services<ReturnType<typeof Intake.dispatchCommand>>
    | FileSystem.FileSystem
    | Path.Path
    | Config.ServerConfig
  >();
  const dispatch = (payload: unknown, transport: "http" | "legacy_websocket" = "http") =>
    Effect.gen(function* () {
      const invalid = (
        cause: unknown,
        reason: QueueCompatibilityError["reason"] = "invalid_command",
      ) => new QueueCompatibilityError({ reason, cause });
      const input = yield* Schema.decodeUnknownEffect(QueueDispatchCommand, {
        onExcessProperty: "error",
      })(payload).pipe(Effect.mapError((cause) => invalid(cause)));
      if (
        input.type === "thread.turn.start" &&
        input.bootstrap !== undefined &&
        input.dispatchGuard !== undefined &&
        transport !== "legacy_websocket"
      )
        return yield* invalid(
          "Guarded bootstrap is not supported",
          "dispatch_guard_bootstrap_unsupported",
        );
      yield* ProjectClone.rejectCommandsDuringClone(clones, input).pipe(
        Effect.mapError((cause) => invalid(cause, "orchestration_dispatch_failed")),
      );
      if (
        input.type === "project.create" ||
        input.type === "project.update" ||
        input.type === "project.delete"
      ) {
        yield* (
          input.type === "project.create"
            ? projects.create(input)
            : input.type === "project.update"
              ? projects.update(input)
              : projects.delete({
                  commandId: input.commandId,
                  projectId: input.projectId,
                  ...(input.force === undefined ? {} : { force: input.force }),
                })
        ).pipe(Effect.mapError((cause) => invalid(cause, "orchestration_dispatch_failed")));
        if (input.type === "project.delete") yield* clones.discard(input.projectId);
        return { sequence: yield* events.latestSequence() };
      }
      if (input.type === "thread.turn.interrupt") {
        const shell = yield* threads.getThreadShell(input.threadId);
        if (shell === null) return yield* invalid("Thread not found");
        const result = yield* threads
          .interruptThread({
            projectId: shell.projectId,
            commandId: input.commandId,
            threadId: input.threadId,
            ...(input.turnId === undefined ? {} : { runId: RunId.make(input.turnId) }),
          })
          .pipe(Effect.mapError((cause) => invalid(cause, "orchestration_dispatch_failed")));
        return result.type === "interrupt_requested"
          ? { sequence: result.dispatch.sequence }
          : { sequence: yield* events.latestSequence() };
      }
      if (input.type === "thread.create") {
        yield* awaitThreadCreationCleanup(outbox, input.threadId);
        const { createdAt: _createdAt, ...command } = input;
        const result = yield* threads
          .dispatch({ ...command, createdBy: "user", creationSource: "web" })
          .pipe(Effect.mapError((cause) => invalid(cause, "orchestration_dispatch_failed")));
        return { sequence: result.sequence };
      }
      const uploadedPaths: string[] = [];
      const attachments = yield* Effect.gen(function* () {
        const config = yield* Config.ServerConfig;
        const fs = yield* FileSystem.FileSystem;
        const prepared: ChatAttachment[] = [];
        const clientIds = input.message.attachments.flatMap((attachment) =>
          attachment.id === undefined ? [] : [attachment.id],
        );
        if (new Set(clientIds).size !== clientIds.length)
          return yield* invalid("Duplicate attachment identifier");
        for (const [index, attachment] of input.message.attachments.entries()) {
          if (!("dataUrl" in attachment)) {
            const claim = planAttachmentClaim({
              attachmentsDir: config.attachmentsDir,
              threadId: input.threadId,
              attachmentId: attachment.id,
            });
            if (!claim.ok) return yield* invalid(claim.reason);
            const stat = yield* fs.stat(claim.currentPath);
            if (Number(stat.size) !== attachment.sizeBytes)
              return yield* invalid("Stored attachment size changed");
            const stored = {
              ...attachment,
              id: ChatAttachmentId.make(claim.finalId),
              mimeType: attachment.mimeType.toLowerCase(),
            };
            if (
              resolveAttachmentPath({
                attachmentsDir: config.attachmentsDir,
                attachment: stored,
              }) !== claim.finalPath
            )
              return yield* invalid("Attachment type changed");
            if (claim.currentPath !== claim.finalPath) {
              yield* fs
                .copyFile(claim.currentPath, claim.finalPath)
                .pipe(
                  Effect.andThen(Effect.sync(() => uploadedPaths.push(claim.finalPath))),
                  Effect.uninterruptible,
                );
            }
            prepared.push(stored);
            continue;
          }
          const parsed = parseBase64DataUrl(attachment.dataUrl);
          if (parsed === null || parsed.mimeType !== attachment.mimeType.toLowerCase())
            return yield* invalid("Invalid image data URL");
          const bytes = yield* Effect.fromResult(Base64.decode(parsed.base64)).pipe(
            Effect.mapError((cause) => invalid(cause)),
          );
          if (bytes.byteLength === 0) return yield* invalid("Empty image payload");
          const id = createDeterministicAttachmentId(
            input.threadId,
            `${input.message.messageId}:${index}`,
          );
          if (id === null) return yield* invalid("Invalid attachment identifier");
          const stored = {
            type: "image" as const,
            id: ChatAttachmentId.make(id),
            name: attachment.name,
            mimeType: parsed.mimeType.toLowerCase(),
            sizeBytes: bytes.byteLength,
            ...(attachment.source === undefined ? {} : { source: attachment.source }),
          };
          const file = resolveAttachmentPath({
            attachmentsDir: config.attachmentsDir,
            attachment: stored,
          });
          if (file === null) return yield* invalid("Invalid attachment path");
          if (yield* fs.exists(file)) {
            const existing = yield* fs.readFile(file);
            if (
              existing.length !== bytes.length ||
              existing.some((value, index) => value !== bytes[index])
            )
              return yield* invalid("Attachment retry payload changed");
          } else {
            yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
            uploadedPaths.push(file);
            yield* fs.writeFile(file, bytes);
          }
          prepared.push(stored);
          yield* Claims.validateAttachmentLimits(prepared);
        }
        yield* Claims.validateAttachmentLimits(prepared);
        return prepared;
      }).pipe(
        Effect.provide(context),
        Effect.tapError(() =>
          Claims.releaseClaimedAttachments(uploadedPaths).pipe(Effect.provide(context)),
        ),
        Effect.mapError((cause) => invalid(cause)),
      );
      const contextRecords =
        input.message.context === undefined
          ? undefined
          : remapComposerContextAttachments(
              input.message.context,
              input.message.attachments,
              attachments,
            );
      const command: OrchestrationV2Command = {
        type: "message.dispatch",
        commandId: input.commandId,
        threadId: input.threadId,
        messageId: input.message.messageId,
        createdBy: "user",
        creationSource: "web",
        text: input.message.text,
        attachments,
        context: contextRecords,
        modelSelection: input.modelSelection,
        titleSeed: input.titleSeed,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        sourcePlanRef: input.sourceProposedPlan,
        dispatchGuard: input.dispatchGuard,
        dispatchMode: { type: "start_immediately" },
      };
      const send = Effect.gen(function* () {
        if (input.bootstrap !== undefined) {
          const existing = yield* threads.getThreadShell(input.threadId);
          const create = input.bootstrap.createThread ?? existing;
          if (create == null) return yield* invalid("Bootstrap requires a thread binding");
          const prepare = input.bootstrap.prepareWorktree;
          if (prepare !== undefined) {
            const project = yield* projects.getById(create.projectId);
            const path = yield* Path.Path.pipe(Effect.provide(context));
            if (
              Option.isNone(project) ||
              path.resolve(prepare.projectCwd) !== path.resolve(project.value.workspaceRoot)
            )
              return yield* invalid("Bootstrap workspace does not match its project");
          }
          let workspaceStrategy: Launch.ThreadLaunchWorkspaceStrategy =
            prepare === undefined
              ? create.worktreePath === null
                ? { type: "root", branch: create.branch ?? undefined }
                : {
                    type: "existing_worktree",
                    worktreePath: create.worktreePath,
                    branch: create.branch ?? undefined,
                  }
              : {
                  type: "worktree",
                  baseRef: prepare.baseBranch,
                  ...(prepare.branch === undefined ? {} : { branch: prepare.branch }),
                  ...(prepare.startFromOrigin === undefined
                    ? {}
                    : { startFromOrigin: prepare.startFromOrigin }),
                };
          const createCommandId = legacyBootstrapCreateCommandId(input.threadId, input.commandId);
          const canonicalPayload = canonicalLegacyPayload(input);
          const previousClaim =
            existing === null
              ? undefined
              : (yield* threads.getThreadRecords(input.threadId, ["runs"])).thread
                  .legacyBootstrapClaim;
          const ownsNewThread =
            previousClaim?.createCommandId === createCommandId &&
            previousClaim.releaseCommandId === input.commandId
              ? previousClaim.ownsNewThread
              : existing === null || existing.deletedAt !== null;
          const policy = {
            version: 1 as const,
            createCommandId,
            birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
            releaseCommandId: input.commandId,
            threadId: input.threadId,
            projectId: create.projectId,
            messageId: input.message.messageId,
            ownsNewThread,
            payloadHash: legacyPayloadHash(canonicalPayload),
            ...(input.dispatchGuard === undefined ? {} : { dispatchGuard: input.dispatchGuard }),
          };
          if (prepare !== undefined) {
            const preflight = yield* launch.preflightLegacyBootstrap({
              policy,
              canonicalPayload,
              fetch: {
                cwd: prepare.projectCwd,
                baseRef: prepare.baseBranch,
                startFromOrigin: prepare.startFromOrigin === true,
                requireWorktree: prepare.requireWorktree === true,
                remote: prepare.startFromOrigin === true ? "origin" : null,
              },
            });
            if (preflight.status !== "ready" || preflight.workspaceStrategy === undefined)
              return yield* invalid(
                new OrchestrationDispatchCommandError({
                  message: preflight.detail ?? "Workspace preparation outcome is unknown.",
                  ...(preflight.status === "known_failed" &&
                  policy.ownsNewThread &&
                  prepare.requireWorktree === true
                    ? { bootstrapThreadDisposition: "not-created" as const }
                    : {}),
                }),
                "orchestration_dispatch_failed",
              );
            workspaceStrategy = preflight.workspaceStrategy;
          }
          const launched = yield* launch.launch({
            commandId: createCommandId,
            preparationReleaseCommandId: input.commandId,
            legacyBootstrap: policy,
            threadId: input.threadId,
            projectId: create.projectId,
            title: create.title,
            modelSelection: input.modelSelection ?? create.modelSelection,
            runtimeMode: input.runtimeMode,
            interactionMode: input.interactionMode,
            workspaceStrategy,
            reuseExistingThread: !ownsNewThread,
            runSetupScript: input.bootstrap.runSetupScript === true,
            createdBy: "user",
            creationSource: "web",
            initialMessage: {
              messageId: input.message.messageId,
              text: input.message.text,
              attachments,
              context: contextRecords,
              ...(input.titleSeed === undefined ? {} : { titleSeed: input.titleSeed }),
              ...(input.sourceProposedPlan === undefined
                ? {}
                : { sourcePlanRef: input.sourceProposedPlan }),
            },
          });
          if (launched.legacyReleaseSequence === undefined)
            return yield* invalid(
              "Bootstrap has no exact accepted release receipt",
              "orchestration_dispatch_failed",
            );
          return { sequence: launched.legacyReleaseSequence };
        }
        const result = yield* Intake.dispatchCommand(command).pipe(Effect.provide(context));
        return { sequence: result.sequence };
      });
      return yield* send.pipe(
        Effect.tapError((cause) => {
          // A generic dispatch error may follow a commit. Retain those files for reconciliation.
          const tag =
            typeof cause === "object" && cause !== null && "_tag" in cause ? cause._tag : null;
          return tag === "OrchestratorCommandRejectedError" ||
            tag === "OrchestratorCommandPreviouslyRejectedError" ||
            tag === "QueueCompatibilityError"
            ? Claims.releaseClaimedAttachments(uploadedPaths).pipe(Effect.provide(context))
            : Effect.void;
        }),
        Effect.mapError((cause) => {
          const dispatchCause =
            Schema.is(Launch.ThreadLaunchError)(cause) && cause.operation === "release-run"
              ? cause.cause
              : cause;
          const guarded =
            input.dispatchGuard !== undefined &&
            typeof dispatchCause === "object" &&
            dispatchCause !== null &&
            "_tag" in dispatchCause &&
            (dispatchCause._tag === "OrchestratorCommandPreviouslyRejectedError" ||
              (dispatchCause._tag === "OrchestratorDispatchError" &&
                "cause" in dispatchCause &&
                typeof dispatchCause.cause === "object" &&
                dispatchCause.cause !== null &&
                "_tag" in dispatchCause.cause &&
                dispatchCause.cause._tag === "DispatchGuardRejected"));
          const launchFailure = Schema.is(Launch.ThreadLaunchError)(cause) ? cause : undefined;
          const websocketBootstrapDetail =
            transport === "legacy_websocket" && input.bootstrap !== undefined && launchFailure
              ? launchFailure.cause instanceof Error
                ? launchFailure.cause.message
                : typeof launchFailure.cause === "string"
                  ? launchFailure.cause
                  : undefined
              : undefined;
          const disposition =
            websocketBootstrapDetail !== undefined ||
            launchFailure?.bootstrapThreadDisposition === "deleted"
              ? new OrchestrationDispatchCommandError({
                  message: guarded
                    ? "Dispatch guard rejected."
                    : (websocketBootstrapDetail ?? "Failed to dispatch orchestration command."),
                  ...(launchFailure?.bootstrapThreadDisposition === "deleted"
                    ? { bootstrapThreadDisposition: "deleted" as const }
                    : {}),
                  cause,
                })
              : cause;
          return Schema.is(QueueCompatibilityError)(cause)
            ? cause
            : invalid(
                disposition,
                guarded ? "dispatch_guard_rejected" : "orchestration_dispatch_failed",
              );
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        Schema.is(QueueCompatibilityError)(cause)
          ? cause
          : new QueueCompatibilityError({ reason: "orchestration_dispatch_failed", cause }),
      ),
    );
  return QueueCompatibility.of({ dispatch });
});
export const layer = Layer.effect(QueueCompatibility, make);
