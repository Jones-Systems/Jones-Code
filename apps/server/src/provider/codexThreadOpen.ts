import type { RuntimeMode, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexRpc from "effect-codex-app-server/rpc";
import type * as EffectCodexSchema from "effect-codex-app-server/schema";

const RECOVERABLE_THREAD_RESUME_ERROR_SNIPPETS = [
  "not found",
  "missing thread",
  "no such thread",
  "unknown thread",
  "does not exist",
  "no rollout found",
];

export const CodexResumeCursorSchema = Schema.Struct({
  threadId: Schema.String,
});

type CodexServiceTier = NonNullable<EffectCodexSchema.V2ThreadStartParams["serviceTier"]>;

function runtimeModeToThreadConfig(input: RuntimeMode): {
  readonly approvalPolicy: EffectCodexSchema.V2ThreadStartParams__AskForApproval;
  readonly sandbox: EffectCodexSchema.V2ThreadStartParams__SandboxMode;
  // Always explicit: omitting the field on resume keeps the thread's previous
  // reviewer, which would leave auto_review sticky after switching modes.
  readonly approvalsReviewer: EffectCodexSchema.V2ThreadStartParams__ApprovalsReviewer;
} {
  switch (input) {
    case "approval-required":
      return {
        approvalPolicy: "untrusted",
        sandbox: "read-only",
        approvalsReviewer: "user",
      };
    case "auto-accept-edits":
      return {
        approvalPolicy: "on-request",
        sandbox: "workspace-write",
        approvalsReviewer: "user",
      };
    case "auto":
      return {
        approvalPolicy: "on-request",
        sandbox: "workspace-write",
        approvalsReviewer: "auto_review",
      };
    case "full-access":
    default:
      return {
        approvalPolicy: "never",
        sandbox: "danger-full-access",
        approvalsReviewer: "user",
      };
  }
}

function buildThreadStartParams(input: {
  readonly cwd: string;
  readonly runtimeMode: RuntimeMode;
  readonly model: string | undefined;
  readonly serviceTier: CodexServiceTier | undefined;
}): EffectCodexSchema.V2ThreadStartParams {
  const config = runtimeModeToThreadConfig(input.runtimeMode);
  return {
    cwd: input.cwd,
    approvalPolicy: config.approvalPolicy,
    sandbox: config.sandbox,
    approvalsReviewer: config.approvalsReviewer,
    ...(input.model ? { model: input.model } : {}),
    ...(input.serviceTier ? { serviceTier: input.serviceTier } : {}),
  };
}

function isRecoverableThreadResumeError(error: unknown): boolean {
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  if (!message.includes("thread")) {
    return false;
  }
  return RECOVERABLE_THREAD_RESUME_ERROR_SNIPPETS.some((snippet) => message.includes(snippet));
}

const CodexThreadResumeMetadata = Schema.Struct({
  cwd: Schema.String,
  model: Schema.String,
  modelProvider: Schema.String,
  serviceTier: Schema.optional(Schema.NullOr(Schema.String)),
  thread: Schema.Struct({ id: Schema.String }),
});
const decodeCodexThreadResumeMetadata = Schema.decodeUnknownEffect(CodexThreadResumeMetadata);

interface CodexThreadOpenClient {
  readonly raw: {
    readonly request: (
      method: "thread/resume",
      payload: CodexRpc.ClientRequestParamsByMethod["thread/resume"] & {
        readonly excludeTurns?: boolean;
      },
    ) => Effect.Effect<unknown, CodexErrors.CodexAppServerError>;
  };
  readonly request: (
    method: "thread/start",
    payload: CodexRpc.ClientRequestParamsByMethod["thread/start"],
  ) => Effect.Effect<
    CodexRpc.ClientRequestResponsesByMethod["thread/start"],
    CodexErrors.CodexAppServerError
  >;
}

export const openCodexThread = (input: {
  readonly client: CodexThreadOpenClient;
  readonly threadId: ThreadId;
  readonly runtimeMode: RuntimeMode;
  readonly cwd: string;
  readonly requestedModel: string | undefined;
  readonly serviceTier: CodexServiceTier | undefined;
  readonly resumeThreadId: string | undefined;
  readonly requireResume?: boolean | undefined;
}): Effect.Effect<typeof CodexThreadResumeMetadata.Type, CodexErrors.CodexAppServerError> => {
  const resumeThreadId = input.resumeThreadId;
  const startParams = buildThreadStartParams({
    cwd: input.cwd,
    runtimeMode: input.runtimeMode,
    model: input.requestedModel,
    serviceTier: input.serviceTier,
  });

  if (resumeThreadId === undefined) {
    return input.client.request("thread/start", startParams);
  }

  // Older providers may still return history despite excludeTurns. Only the
  // session metadata is needed here, so unrelated historical items cannot
  // prevent resuming a valid provider thread.
  return input.client.raw
    .request("thread/resume", {
      threadId: resumeThreadId,
      ...startParams,
      excludeTurns: true,
    })
    .pipe(
      Effect.flatMap((response) =>
        decodeCodexThreadResumeMetadata(response).pipe(
          Effect.mapError((error) =>
            CodexErrors.CodexAppServerRequestError.invalidPayload(
              "thread/resume",
              "decode-payload",
              error,
            ),
          ),
        ),
      ),
      Effect.catchIf(
        (error) => !input.requireResume && isRecoverableThreadResumeError(error),
        (error) =>
          Effect.logWarning("codex app-server thread resume fell back to fresh start", {
            threadId: input.threadId,
            requestedRuntimeMode: input.runtimeMode,
            resumeThreadId,
            recoverable: true,
            cause: error,
          }).pipe(Effect.andThen(input.client.request("thread/start", startParams))),
      ),
    );
};
