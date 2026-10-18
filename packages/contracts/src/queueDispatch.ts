import * as Schema from "effect/Schema";
import * as SchemaParser from "effect/SchemaParser";
import {
  CommandId,
  MessageId,
  ThreadId,
  TurnId,
  ProjectId,
  PlanId,
  IsoDateTime,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ChatAttachment, UploadChatAttachment } from "./chatAttachment.ts";
import { OrchestrationMessageContext } from "./composerContext.ts";
import { ModelSelection } from "./modelSelection.ts";
import { RuntimeMode, ProviderInteractionMode } from "./providerPolicy.ts";
import { ThreadTurnDispatchGuard } from "./providerQueue.ts";
import { ProjectMutation } from "./project.ts";

const QueueBootstrapThread = Schema.Struct({
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: IsoDateTime,
});
const QueueBootstrap = Schema.Struct({
  createThread: Schema.optional(QueueBootstrapThread),
  prepareWorktree: Schema.optional(
    Schema.Struct({
      projectCwd: TrimmedNonEmptyString,
      baseBranch: TrimmedNonEmptyString,
      branch: Schema.optional(TrimmedNonEmptyString),
      startFromOrigin: Schema.optional(Schema.Boolean),
      requireWorktree: Schema.optional(Schema.Boolean),
    }),
  ),
  runSetupScript: Schema.optional(Schema.Boolean),
});

// The queue bridge names supported legacy commands explicitly; unsupported V1 fields fail closed.
const QueueDispatchCommandShape = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("thread.turn.start"),
    commandId: CommandId,
    threadId: ThreadId,
    message: Schema.Struct({
      messageId: MessageId,
      role: Schema.Literal("user"),
      text: Schema.String,
      attachments: Schema.Array(Schema.Union([ChatAttachment, UploadChatAttachment])),
      context: Schema.optional(OrchestrationMessageContext),
    }),
    modelSelection: Schema.optional(ModelSelection),
    titleSeed: Schema.optional(TrimmedNonEmptyString),
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
    bootstrap: Schema.optional(QueueBootstrap),
    dispatchGuard: Schema.optional(ThreadTurnDispatchGuard),
    sourceProposedPlan: Schema.optional(Schema.Struct({ threadId: ThreadId, planId: PlanId })),
    createdAt: IsoDateTime,
  }),
  Schema.Struct({
    type: Schema.Literal("thread.turn.interrupt"),
    commandId: CommandId,
    threadId: ThreadId,
    turnId: Schema.optional(TurnId),
    createdAt: IsoDateTime,
  }),
  Schema.Struct({
    type: Schema.Literal("thread.create"),
    commandId: CommandId,
    threadId: ThreadId,
    projectId: ProjectId,
    title: TrimmedNonEmptyString,
    modelSelection: ModelSelection,
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
    branch: Schema.NullOr(TrimmedNonEmptyString),
    worktreePath: Schema.NullOr(TrimmedNonEmptyString),
    createdAt: IsoDateTime,
  }),
  ProjectMutation,
]);

export const QueueDispatchCommand = Schema.declareConstructor<
  typeof QueueDispatchCommandShape.Type,
  typeof QueueDispatchCommandShape.Encoded
>()(
  [QueueDispatchCommandShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type QueueDispatchCommand = typeof QueueDispatchCommand.Type;
