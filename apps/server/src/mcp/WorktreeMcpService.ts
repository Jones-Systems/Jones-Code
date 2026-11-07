import * as RetirementOutcome from "../jones/workspaceRetirement/Outcome.ts";
import * as WorkspaceDuty from "../jones/workspaceRetirement/Duty.ts";
import * as ConversationPolicy from "../jones/conversationWorkspace/ConversationPolicy.ts";
import { resolveSource } from "../jones/conversationWorkspace/SourceBroker.ts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Stable legacy request identity; explicit clientRequestId remains preferred.
import * as NodeCrypto from "node:crypto";
import * as WorkspaceProvenance from "../jones/workspaceLifecycle/Provenance.ts";
import {
  CommandId,
  ChatAttachment,
  MessageId,
  ProjectId,
  ThreadId,
  WorktreeMcpFailure,
  type WorktreeMcpContinuationStatus,
  type WorktreeMcpHandoffInput,
  type WorktreeMcpHandoffResult,
  type WorktreeMcpSetupScriptStatus,
  type WorktreeMcpStatusResult,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "./McpInvocationContext.ts";

export class WorktreeMcpService extends Context.Service<
  WorktreeMcpService,
  {
    readonly handoff: (
      scope: McpInvocationScope,
      input: WorktreeMcpHandoffInput,
      beforeWrite?: Effect.Effect<void, WorktreeMcpFailure>,
    ) => Effect.Effect<WorktreeMcpHandoffResult, WorktreeMcpFailure>;
    readonly attachForTurn: (
      threadId: ThreadId,
      runId: string,
      text: string,
      attachments: ReadonlyArray<ChatAttachment>,
    ) => Effect.Effect<void, WorktreeMcpFailure>;
    readonly attachForTerminal: (
      threadId: ThreadId,
    ) => Effect.Effect<string | null, WorktreeMcpFailure>;
    readonly status: (
      scope: McpInvocationScope,
    ) => Effect.Effect<WorktreeMcpStatusResult, WorktreeMcpFailure>;
  }
>()("t3/mcp/WorktreeMcpService") {}

function failure(code: WorktreeMcpFailure["code"], message: string): WorktreeMcpFailure {
  return new WorktreeMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
}

const asOperationFailed = (prefix: string) =>
  Effect.mapError((error: unknown) =>
    failure("operation_failed", `${prefix}: ${errorMessage(error)}`),
  );

const encodeHandoffRequest = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      callerThreadId: Schema.String,
      branch: Schema.String,
      baseRef: Schema.NullOr(Schema.String),
      startFromOrigin: Schema.NullOr(Schema.Boolean),
      runSetupScript: Schema.Boolean,
      continuationPrompt: Schema.NullOr(Schema.String),
      continuationAttachments: Schema.Array(ChatAttachment),
    }),
  ),
);

export const make = Effect.gen(function* () {
  const random = yield* SecureRandom.SecureRandom;
  const retirementOutcome = yield* RetirementOutcome.lookup;
  const captureProvenance = yield* WorkspaceProvenance.optionalCapture;
  const conversationPolicy = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
  const lookupProvenance = yield* WorkspaceProvenance.optionalLookup;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;

  // Serialize handoffs per thread to avoid creating two worktrees and orphaning one.
  const handoffThreadsInFlight = new Set<string>();

  const requireThreadScope = (scope: McpInvocationScope) =>
    scope.thread === undefined
      ? Effect.fail(
          failure(
            "thread_credential_required",
            "Worktree handoff and status act as the calling T3 thread, so they need an agent running inside T3 Code.",
          ),
        )
      : Effect.succeed(scope as McpThreadInvocationScope);

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("worktree")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP credential does not grant worktree capabilities."),
        );

  const loadThread = (scope: { readonly thread: { readonly threadId: ThreadId } }) =>
    threadManagement.getThreadRecords(scope.thread.threadId, []).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorProjectionError"
          ? failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`)
          : failure(
              "operation_failed",
              `Unable to read thread ${scope.thread.threadId}: ${errorMessage(error)}`,
            ),
      ),
      Effect.filterOrFail(
        (projection) => projection.thread.deletedAt === null,
        () => failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`),
      ),
    );

  const loadProject = (
    scope: { readonly thread: { readonly threadId: ThreadId } },
    projectId: ProjectId,
  ) =>
    projects.getById(projectId).pipe(
      asOperationFailed(`Unable to read project ${projectId}`),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              failure(
                "project_not_found",
                `Project '${projectId}' was not found for thread '${scope.thread.threadId}'.`,
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const readDefaultStartFromOrigin = serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.newWorktreesStartFromOrigin),
    asOperationFailed("Unable to read server settings"),
  );

  const handoffIds = (scope: McpThreadInvocationScope) =>
    random.uuidV4().pipe(
      Effect.map((uuid) => {
        const part = (kind: string, operation: string) =>
          [kind, "mcp", encodeURIComponent(scope.thread.providerSessionId), operation, uuid].join(
            ":",
          );
        return {
          commandId: CommandId.make(part("command", "worktree-handoff")),
          continuationCommandId: CommandId.make(part("command", "worktree-continuation")),
          continuationMessageId: MessageId.make(part("message", "worktree-continuation")),
        };
      }),
      Effect.orDie,
    );

  const performHandoff = Effect.fn("WorktreeMcpService.performHandoff")(function* (
    scope: McpThreadInvocationScope,
    input: WorktreeMcpHandoffInput,
  ) {
    const alreadyInWorktree = (worktreePath: string) =>
      failure(
        "already_in_worktree",
        `Thread '${scope.thread.threadId}' is already attached to worktree '${worktreePath}'.`,
      );

    const projection = yield* loadThread(scope);
    if (projection.thread.worktreePath !== null) {
      return yield* alreadyInWorktree(projection.thread.worktreePath);
    }
    if (projection.thread.archivedAt !== null) {
      return yield* failure(
        "invalid_request",
        `Thread '${scope.thread.threadId}' is archived and cannot be handed off to a worktree.`,
      );
    }

    const project = yield* loadProject(scope, projection.thread.projectId);
    const projectCwd = project.workspaceRoot;

    const localStatus = yield* gitWorkflow
      .localStatus({ cwd: projectCwd })
      .pipe(asOperationFailed("Unable to read git status"));
    if (!localStatus.isRepo) {
      return yield* failure(
        "invalid_request",
        `Project workspace '${projectCwd}' is not a git repository.`,
      );
    }

    const localBranchNames = yield* gitWorkflow
      .listLocalBranchNames(projectCwd)
      .pipe(asOperationFailed("Unable to list branches"));
    if (localBranchNames.includes(input.branch)) {
      const existingRef = yield* gitWorkflow
        .listRefs({ cwd: projectCwd, query: input.branch, refKind: "local" })
        .pipe(
          Effect.map((result) =>
            result.refs.find((ref) => ref.name === input.branch && ref.isRemote !== true),
          ),
          Effect.orElseSucceed(() => undefined),
        );
      const checkoutPath = existingRef?.worktreePath ?? null;
      return yield* failure(
        "invalid_request",
        `Branch '${input.branch}' already exists${
          checkoutPath === null ? "" : ` and is checked out at '${checkoutPath}'`
        }. Choose a different branch name, or delete the existing branch${
          checkoutPath === null ? "" : " and its worktree"
        } first.`,
      );
    }

    let baseRef = input.baseRef;
    if (baseRef === undefined) {
      if (localStatus.refName === null) {
        return yield* failure(
          "invalid_request",
          "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
        );
      }
      baseRef = localStatus.refName;
    }

    const startFromOrigin = input.startFromOrigin ?? (yield* readDefaultStartFromOrigin);

    let worktreeBaseRef = baseRef;
    let baseRefName = baseRef;
    let startedFromOrigin = false;
    if (startFromOrigin) {
      const resolvedRemoteBase = yield* gitWorkflow
        .fetchWorktreeBaseCommit({ cwd: projectCwd, refName: baseRef })
        .pipe(asOperationFailed(`Unable to fetch the remote base of '${baseRef}'`));
      if (resolvedRemoteBase !== null) {
        worktreeBaseRef = resolvedRemoteBase.commitSha;
        baseRefName = `refs/remotes/${resolvedRemoteBase.remoteRefName}`;
        startedFromOrigin = true;
      }
    }

    yield* captureProvenance({
      threadId: scope.thread.threadId,
      reason: "reference",
      createdBy: "agent",
    });
    const ids = yield* handoffIds(scope);

    // Mask interruption after worktree creation through setup; binding can detach and interrupt the caller.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const worktree = yield* restore(
          gitWorkflow
            .createWorktree({
              cwd: projectCwd,
              refName: worktreeBaseRef,
              newRefName: input.branch,
              baseRefName,
              path: null,
            })
            .pipe(asOperationFailed("Unable to create the worktree")),
        );
        const worktreePath = worktree.worktree.path;

        const reportFailed = (logMessage: string) =>
          Effect.catchCause((cause: Cause.Cause<unknown>) => {
            const detail = errorMessage(Cause.squash(cause));
            return Effect.logWarning(logMessage, {
              threadId: scope.thread.threadId,
              worktreePath,
              detail,
            }).pipe(Effect.as({ status: "failed", detail } as const));
          });

        const removeCreatedWorktree = Effect.suspend(() =>
          gitWorkflow.removeWorktree({ cwd: projectCwd, path: worktreePath, force: true }).pipe(
            Effect.andThen(
              Effect.suspend(() =>
                gitWorkflow.deleteLocalBranch({
                  cwd: projectCwd,
                  refName: worktree.worktree.refName,
                  force: true,
                }),
              ),
            ),
          ),
        ).pipe(Effect.ignoreCause({ log: true }));

        const recheckAndBind = Effect.gen(function* () {
          // A concurrent UI binding may have landed during git work; do not orphan a second worktree.
          const recheck = yield* loadThread(scope);
          if (recheck.thread.worktreePath !== null) {
            return yield* alreadyInWorktree(recheck.thread.worktreePath);
          }
          if (recheck.thread.archivedAt !== null) {
            return yield* failure(
              "invalid_request",
              `Thread '${scope.thread.threadId}' was archived while the worktree was being created; the handoff was rolled back.`,
            );
          }
          yield* threadManagement
            .dispatch({
              type: "thread.metadata.update",
              commandId: ids.commandId,
              threadId: scope.thread.threadId,
              branch: worktree.worktree.refName,
              worktreePath,
              expectedWorktreePath: null,
            })
            .pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause as Cause.Cause<never>)
                  : Effect.fail(
                      failure(
                        "operation_failed",
                        `Unable to re-point the thread at the worktree: ${errorMessage(Cause.squash(cause))}`,
                      ),
                    ),
              ),
            );
        }).pipe(
          // Do not remove on interruption alone; the thread binding may already have committed.
          Effect.onError((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.void : removeCreatedWorktree,
          ),
        );

        // Queue continuation immediately after binding; detachment terminates the caller, so relaunch needs a durable message.
        const queueContinuation: Effect.Effect<WorktreeMcpContinuationStatus, WorktreeMcpFailure> =
          Effect.suspend(() =>
            input.continuationPrompt === undefined
              ? Effect.succeed<WorktreeMcpContinuationStatus>({ status: "skipped" })
              : threadManagement
                  .sendToThread({
                    projectId: projection.thread.projectId,
                    commandId: ids.continuationCommandId,
                    threadId: scope.thread.threadId,
                    messageId: ids.continuationMessageId,
                    text: input.continuationPrompt,
                    attachments: [],
                    mode: "queue",
                    createdBy: "agent",
                    creationSource: "mcp",
                  })
                  .pipe(
                    Effect.map((sendResult): WorktreeMcpContinuationStatus => ({
                      status: "scheduled",
                      delivery: sendResult.delivery,
                    })),
                    reportFailed("worktree handoff continuation failed to queue"),
                  ),
          );

        const duty = yield* WorkspaceDuty.hooks;
        const releaseDuty = yield* duty
          .hold(worktreePath, "handoff", scope.thread.threadId)
          .pipe(asOperationFailed("Workspace handoff duty refused"));
        const continuation = yield* recheckAndBind.pipe(
          Effect.andThen(
            captureProvenance({
              threadId: scope.thread.threadId,
              reason: "handoff",
              createdBy: "agent",
              baseRef: baseRefName,
              baseIsCurrentHead: true,
            }),
          ),
          Effect.andThen(queueContinuation),
          Effect.ensuring(releaseDuty.pipe(Effect.orDie)),
        );

        yield* vcsStatusBroadcaster
          .refreshStatus(worktreePath)
          .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach);

        let setupScript: WorktreeMcpSetupScriptStatus = { status: "skipped" };
        if (input.runSetupScript ?? true) {
          setupScript = yield* setupScriptRunner
            .runForThread({
              threadId: scope.thread.threadId,
              projectId: projection.thread.projectId,
              projectCwd,
              worktreePath,
              project: {
                id: project.id,
                workspaceRoot: project.workspaceRoot,
                scripts: project.scripts,
              },
            })
            .pipe(
              Effect.map((result): WorktreeMcpSetupScriptStatus =>
                result.status === "started"
                  ? {
                      status: "started",
                      scriptName: result.scriptName,
                      terminalId: result.terminalId,
                    }
                  : result.status === "skipped-for-link"
                    ? { status: "skipped" }
                    : { status: "no-script" },
              ),
              reportFailed("worktree handoff setup script failed"),
            );
        }

        const result: WorktreeMcpHandoffResult = {
          worktreePath,
          branch: worktree.worktree.refName,
          baseRef,
          startedFromOrigin,
          setupScript,
          continuation,
          note:
            continuation.status === "scheduled"
              ? "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the queued continuation prompt then starts the next turn inside the worktree with the conversation preserved. The worktree is not removed automatically when the thread is deleted."
              : "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the conversation continues inside the worktree when the thread receives its next message. Pass continuationPrompt to resume automatically. The worktree is not removed automatically when the thread is deleted.",
        };
        return result;
      }),
    );
  });

  const durableHandoff = Effect.fn("WorktreeMcpService.durableHandoff")(function* (
    threadId: ThreadId,
    input: WorktreeMcpHandoffInput,
    beforeWrite: Effect.Effect<void, WorktreeMcpFailure> = Effect.void,
    continuationAttachments: ReadonlyArray<ChatAttachment> = [],
  ) {
    if (Option.isNone(conversationPolicy))
      return yield* failure("operation_failed", "Conversation handoff service unavailable.");
    const policy = conversationPolicy.value;
    const scope = { thread: { threadId } };
    const projection = yield* loadThread(scope);
    const project = yield* loadProject(scope, projection.thread.projectId);
    const binding = yield* policy
      .lookup(threadId)
      .pipe(asOperationFailed("Unable to read conversation binding"));
    const journalOwner = binding?.ownerThreadId ?? threadId;
    const request = yield* encodeHandoffRequest({
      callerThreadId: threadId,
      branch: input.branch,
      baseRef: input.baseRef ?? null,
      startFromOrigin: input.startFromOrigin ?? null,
      runSetupScript: input.runSetupScript ?? true,
      continuationPrompt: input.continuationPrompt ?? null,
      continuationAttachments,
    }).pipe(asOperationFailed("Unable to encode handoff request"));
    const requestId =
      input.clientRequestId ??
      `legacy:${NodeCrypto.createHash("sha256").update(request).digest("hex")}`;
    // The journal is read before the already-attached check so exact retries
    // return the original receipt after binding or provider detachment.
    const previous = yield* policy
      .readHandoff(journalOwner)
      .pipe(asOperationFailed("Unable to read handoff journal"));
    if (previous !== null && (previous.requestId !== requestId || previous.request !== request))
      return yield* failure(
        "invalid_request",
        "Handoff request conflicts with the persisted original request.",
      );
    if (previous?.phase === "done")
      return previous.result ?? (yield* failure("operation_failed", "Handoff result is missing."));
    yield* beforeWrite;
    let commit = previous?.commit;
    let baseRef = previous?.baseRef;
    let startedFromOrigin = previous?.startedFromOrigin ?? false;
    if (commit === undefined || baseRef === undefined) {
      if (binding !== null) {
        const recorded = yield* policy
          .snapshot(threadId)
          .pipe(asOperationFailed("Recorded source is unavailable"));
        commit = recorded.commit;
        baseRef = recorded.baseRef;
        if (input.baseRef !== undefined && input.baseRef !== baseRef && input.baseRef !== commit)
          return yield* failure(
            "invalid_request",
            "Conversation handoff must use its recorded source commit.",
          );
      } else {
        const local = yield* gitWorkflow
          .localStatus({ cwd: project.workspaceRoot })
          .pipe(asOperationFailed("Unable to resolve handoff base"));
        baseRef = input.baseRef ?? local.refName ?? undefined;
        if (baseRef === undefined)
          return yield* failure("invalid_request", "Select a base ref for the handoff.");
        let revision = baseRef;
        if (input.startFromOrigin ?? (yield* readDefaultStartFromOrigin)) {
          const remote = yield* gitWorkflow
            .fetchWorktreeBaseCommit({ cwd: project.workspaceRoot, refName: baseRef })
            .pipe(asOperationFailed("Unable to fetch handoff base"));
          if (remote !== null) {
            revision = remote.commitSha;
            baseRef = `refs/remotes/${remote.remoteRefName}`;
            startedFromOrigin = true;
          }
        }
        commit = (yield* Effect.tryPromise({
          try: () => resolveSource(project.workspaceRoot, revision),
          catch: () => failure("invalid_request", "Unable to resolve handoff commit."),
        })).commit;
      }
    }
    let record =
      previous ??
      (yield* policy
        .reserveHandoff({
          threadId: journalOwner,
          requestId,
          request,
          commit,
          baseRef,
          startedFromOrigin,
          members: yield* policy
            .sharing(threadId)
            .pipe(asOperationFailed("Unable to resolve shared conversation bindings")),
        })
        .pipe(asOperationFailed("Unable to reserve handoff")));
    if (projection.thread.archivedAt !== null)
      return yield* failure("invalid_request", "Archived threads cannot attach a checkout.");
    const step = (patch: Parameters<typeof policy.updateHandoff>[1]) =>
      policy
        .updateHandoff(record, patch)
        .pipe(asOperationFailed("Unable to record handoff progress"));
    if (record.phase === "reserved") {
      if (projection.thread.worktreePath !== null)
        return yield* failure("already_in_worktree", "This thread already has a checkout.");
      const existing = yield* gitWorkflow
        .listRefs({ cwd: project.workspaceRoot, query: input.branch, refKind: "local" })
        .pipe(asOperationFailed("Unable to check the new branch"));
      if (existing.refs.some((ref) => ref.name === input.branch && ref.isRemote !== true))
        return yield* failure(
          "invalid_request",
          "The requested branch already exists; it is not an issued checkout from this handoff.",
        );
      const currentSource = yield* Effect.tryPromise({
        try: () => resolveSource(project.workspaceRoot, record.commit),
        catch: () =>
          failure("invalid_request", "The project no longer contains the recorded source commit."),
      });
      if (binding !== null && currentSource.commonDirectory !== binding.commonDirectory)
        return yield* failure(
          "invalid_request",
          "The project repository changed since the conversation source was recorded.",
        );
      record = yield* step({ phase: "checkout_issued" });
      // Intentionally no fetch: the checkout must match the already recorded source commit.
      const created = yield* gitWorkflow
        .createWorktree({
          cwd: project.workspaceRoot,
          refName: record.commit,
          newRefName: input.branch,
          baseRefName: record.baseRef,
          path: record.path,
        })
        .pipe(asOperationFailed("Checkout outcome must be reconciled before retry"));
      if (created.worktree.path !== record.path)
        return yield* failure(
          "operation_failed",
          "handoff_unknown: checkout result does not match the persisted allocation path.",
        );
      record = yield* step({ phase: "checkout_created", path: created.worktree.path });
    } else if (record.phase === "checkout_issued") {
      const refs = yield* gitWorkflow
        .listRefs({ cwd: project.workspaceRoot, query: input.branch, refKind: "local" })
        .pipe(asOperationFailed("Unable to reconcile checkout"));
      const found = refs.refs.find((ref) => ref.name === input.branch && ref.isRemote !== true);
      if (found?.worktreePath == null || found.worktreePath !== record.path)
        return yield* failure(
          "handoff_in_progress",
          "handoff_unknown: checkout issuance has no proved result; preserve the request and reconcile its original operation.",
        );
      const observed = yield* Effect.tryPromise({
        try: () => resolveSource(found.worktreePath!, "HEAD"),
        catch: () => failure("operation_failed", "Unable to verify issued checkout."),
      });
      if (
        observed.commit !== record.commit ||
        (binding !== null && observed.commonDirectory !== binding.commonDirectory)
      )
        return yield* failure(
          "operation_failed",
          "handoff_unknown: issued checkout does not match the recorded source commit.",
        );
      record = yield* step({ phase: "checkout_created", path: found.worktreePath });
    }
    const worktreePath = record.path;
    if (worktreePath === null)
      return yield* failure("operation_failed", "Handoff journal has no checkout path.");
    const commandKey = `conversation-handoff:${encodeURIComponent(threadId)}:${encodeURIComponent(requestId)}`;
    const duty = yield* WorkspaceDuty.hooks;
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const releaseDuty = yield* duty
          .hold(worktreePath, "handoff", threadId)
          .pipe(asOperationFailed("Workspace handoff duty refused"));
        return yield* Effect.gen(function* () {
          if (record.phase === "checkout_created") {
            for (const member of record.members) {
              const memberId = ThreadId.make(member);
              const fresh = yield* loadThread({ thread: { threadId: memberId } });
              if (
                fresh.thread.projectId !== project.id ||
                (fresh.thread.worktreePath !== null && fresh.thread.worktreePath !== worktreePath)
              )
                return yield* failure(
                  "already_in_worktree",
                  "A shared thread binding changed during handoff.",
                );
              yield* threadManagement
                .dispatch({
                  type: "thread.metadata.update",
                  commandId: CommandId.make(`${commandKey}:bind:${encodeURIComponent(member)}`),
                  threadId: memberId,
                  branch: input.branch,
                  worktreePath,
                })
                .pipe(asOperationFailed("Unable to bind shared handoff checkout"));
              yield* policy
                .markAttached(member)
                .pipe(asOperationFailed("Unable to record shared checkout binding"));
            }
            for (const member of record.members)
              yield* policy
                .dispose(member, true)
                .pipe(asOperationFailed("Unable to dispose the attached conversation directory"));
            yield* captureProvenance({
              threadId,
              reason: "handoff",
              createdBy: "agent",
              baseRef: record.baseRef,
              baseIsCurrentHead: true,
            });
            record = yield* step({ phase: "bound" });
          }
          if (record.phase === "bound") {
            // Persist issuance before setup. An interrupted setup is never blindly rerun.
            record = yield* step({ phase: "setup_issued" });
            const setup: WorktreeMcpSetupScriptStatus =
              input.runSetupScript === false
                ? { status: "skipped" }
                : yield* setupScriptRunner
                    .runForThread({
                      threadId,
                      projectId: project.id,
                      projectCwd: project.workspaceRoot,
                      worktreePath,
                      preferredTerminalId: `conversation-setup-${NodeCrypto.createHash("sha256").update(commandKey).digest("hex").slice(0, 24)}`,
                      project: {
                        id: project.id,
                        workspaceRoot: project.workspaceRoot,
                        scripts: project.scripts,
                      },
                    })
                    .pipe(
                      Effect.flatMap((result) =>
                        result.status === "started"
                          ? result.writeCompletion.pipe(
                              Effect.as<WorktreeMcpSetupScriptStatus>({
                                status: "started",
                                scriptName: result.scriptName,
                                terminalId: result.terminalId,
                              }),
                            )
                          : Effect.succeed<WorktreeMcpSetupScriptStatus>(
                              result.status === "skipped-for-link"
                                ? { status: "skipped" }
                                : { status: "no-script" },
                            ),
                      ),
                      asOperationFailed("Setup outcome must be reconciled before retry"),
                    );
            record = yield* step({ phase: "setup_done", setup });
          } else if (record.phase === "setup_issued")
            return yield* failure(
              "handoff_in_progress",
              "handoff_unknown: setup was issued; retain its checkout and reconcile the original setup before continuing.",
            );
          if (record.phase === "setup_done") record = yield* step({ phase: "continuation_issued" });
          const continuation: WorktreeMcpContinuationStatus =
            input.continuationPrompt === undefined
              ? { status: "skipped" }
              : yield* threadManagement
                  .sendToThread({
                    projectId: project.id,
                    commandId: CommandId.make(`${commandKey}:continue`),
                    threadId,
                    messageId: MessageId.make(`${commandKey}:message`),
                    text: input.continuationPrompt,
                    attachments: continuationAttachments,
                    mode: "queue",
                    createdBy: "agent",
                    creationSource: "mcp",
                  })
                  .pipe(
                    Effect.map((sent): WorktreeMcpContinuationStatus => ({
                      status: "scheduled",
                      delivery: sent.delivery,
                    })),
                    asOperationFailed("Unable to queue handoff continuation"),
                  );
          const directory = yield* policy
            .dispose(threadId, true)
            .pipe(asOperationFailed("Unable to finalize conversation directory ownership"));
          const result: WorktreeMcpHandoffResult = {
            worktreePath,
            branch: input.branch,
            baseRef: record.baseRef,
            startedFromOrigin: record.startedFromOrigin,
            setupScript: record.setup ?? { status: "skipped" },
            continuation,
            note: `Handoff recorded at source commit ${record.commit}. The current provider turn ends after the workspace changes; continuation is ${continuation.status}. Conversation directory: ${directory ?? "none"}. Nonempty content is retained with its original owner obligation.`,
          };
          record = yield* step({ phase: "done", result });
          return result;
        }).pipe(Effect.ensuring(releaseDuty.pipe(Effect.orDie)));
      }),
    );
  });

  const handoff: WorktreeMcpService["Service"]["handoff"] = Effect.fn("WorktreeMcpService.handoff")(
    function* (callerScope, input, beforeWrite = Effect.void) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      // Acquire and register cleanup atomically so cancellation cannot strand the guard until restart.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          if (handoffThreadsInFlight.has(scope.thread.threadId)) {
            return Effect.fail(
              failure(
                "handoff_in_progress",
                `A worktree handoff is already in progress for thread '${scope.thread.threadId}'.`,
              ),
            );
          }
          handoffThreadsInFlight.add(scope.thread.threadId);
          return restore(
            Option.isSome(conversationPolicy)
              ? durableHandoff(scope.thread.threadId, input, beforeWrite)
              : beforeWrite.pipe(Effect.andThen(performHandoff(scope, input))),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => handoffThreadsInFlight.delete(scope.thread.threadId)),
            ),
          );
        }),
      );
    },
  );

  const status: WorktreeMcpService["Service"]["status"] = Effect.fn("WorktreeMcpService.status")(
    function* (callerScope) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      const projection = yield* loadThread(scope);
      const project = yield* loadProject(scope, projection.thread.projectId);

      const defaultStartFromOrigin = yield* readDefaultStartFromOrigin;

      const provenance = yield* lookupProvenance({ threadId: scope.thread.threadId });
      const binding = Option.isSome(conversationPolicy)
        ? yield* conversationPolicy.value
            .lookup(scope.thread.threadId)
            .pipe(asOperationFailed("Unable to read conversation status"))
        : null;
      const recorded =
        binding === null || Option.isNone(conversationPolicy)
          ? null
          : yield* conversationPolicy.value
              .snapshot(scope.thread.threadId)
              .pipe(asOperationFailed("Unable to read recorded source"));
      const handoff =
        binding === null || Option.isNone(conversationPolicy)
          ? null
          : yield* conversationPolicy.value
              .readHandoff(binding.ownerThreadId)
              .pipe(asOperationFailed("Unable to read handoff status"));
      const result: WorktreeMcpStatusResult = {
        ...(yield* retirementOutcome(scope.thread.threadId)),
        ...(binding === null || recorded === null
          ? {}
          : {
              conversation: {
                state: binding.state,
                directory: binding.path,
                ownerThreadId: binding.ownerThreadId,
                recordedCommit: recorded.commit,
                baseRef: recorded.baseRef,
                sourcePolicy: "broker-only" as const,
                handoffPhase: handoff?.phase ?? null,
              },
            }),
        attached: projection.thread.worktreePath !== null,
        worktreePath: projection.thread.worktreePath,
        branch: projection.thread.branch,
        projectWorkspaceRoot: project.workspaceRoot,
        defaultStartFromOrigin,
        ...(provenance === undefined ? {} : { provenance }),
      };
      return result;
    },
  );

  const shareAttachedCheckout = Effect.fn("WorktreeMcpService.shareAttachedCheckout")(function* (
    threadId: ThreadId,
    binding: ConversationPolicy.Binding,
    key: string,
  ) {
    if (Option.isNone(conversationPolicy))
      return yield* failure("operation_failed", "Conversation binding service unavailable.");
    const original = yield* conversationPolicy.value
      .readHandoff(binding.ownerThreadId)
      .pipe(asOperationFailed("Unable to read shared handoff"));
    if (original?.phase !== "done" || original.result === null)
      return yield* failure("handoff_in_progress", "The original shared handoff is not complete.");
    const current = yield* loadThread({ thread: { threadId } });
    if (
      current.thread.projectId !== binding.projectId ||
      (current.thread.worktreePath !== null &&
        current.thread.worktreePath !== original.result.worktreePath)
    )
      return yield* failure("invalid_request", "The shared checkout binding changed.");
    yield* threadManagement
      .dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make(`${key}:bind`),
        threadId,
        branch: original.result.branch,
        worktreePath: original.result.worktreePath,
      })
      .pipe(asOperationFailed("Unable to share the parent's checkout"));
    return original.result.worktreePath;
  });
  const attachForTerminal = Effect.fn("WorktreeMcpService.attachForTerminal")(function* (
    threadId: ThreadId,
  ) {
    if (Option.isNone(conversationPolicy)) return null;
    const binding = yield* conversationPolicy.value
      .lookup(threadId)
      .pipe(asOperationFailed("Unable to read terminal binding"));
    if (binding === null) return null;
    if (binding.state === "attached") {
      return binding.ownerThreadId === threadId
        ? null
        : yield* shareAttachedCheckout(
            threadId,
            binding,
            `conversation-terminal-share:${encodeURIComponent(threadId)}`,
          );
    }
    const result = yield* durableHandoff(threadId, {
      branch: `work/conversation-${NodeCrypto.createHash("sha256").update(threadId).digest("hex").slice(0, 16)}`,
      clientRequestId: `terminal:${threadId}`,
      runSetupScript: true,
    });
    return result.worktreePath;
  });
  const attachForTurn = Effect.fn("WorktreeMcpService.attachForTurn")(function* (
    threadId: ThreadId,
    runId: string,
    text: string,
    attachments: ReadonlyArray<ChatAttachment>,
  ) {
    if (Option.isSome(conversationPolicy)) {
      const binding = yield* conversationPolicy.value
        .lookup(threadId)
        .pipe(asOperationFailed("Unable to read shared checkout binding"));
      if (binding !== null && binding.ownerThreadId !== threadId && binding.state === "attached") {
        const key = `conversation-share:${encodeURIComponent(threadId)}:${encodeURIComponent(runId)}`;
        yield* shareAttachedCheckout(threadId, binding, key);
        yield* threadManagement
          .sendToThread({
            projectId: ProjectId.make(binding.projectId),
            commandId: CommandId.make(`${key}:continue`),
            threadId,
            messageId: MessageId.make(`${key}:message`),
            text: text || "Continue the previous request in the shared checkout.",
            attachments,
            mode: "queue",
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(asOperationFailed("Unable to continue in the shared checkout"));
        return;
      }
    }
    yield* durableHandoff(
      threadId,
      {
        branch: `work/conversation-${NodeCrypto.createHash("sha256").update(threadId).digest("hex").slice(0, 16)}`,
        clientRequestId: `adapter-fallback:${runId}`,
        continuationPrompt: text || "Continue the previous request in the attached checkout.",
        runSetupScript: true,
      },
      Effect.void,
      attachments,
    );
  });
  if (Option.isSome(conversationPolicy))
    yield* conversationPolicy.value.registerTurnAttachment((threadId, runId, text, attachments) =>
      attachForTurn(threadId, runId, text, attachments).pipe(
        Effect.mapError(
          (cause) =>
            new ConversationPolicy.ConversationWorkspaceError({
              threadId,
              reason: "handoff_unknown",
              cause,
            }),
        ),
      ),
    );
  return WorktreeMcpService.of({ handoff, status, attachForTerminal, attachForTurn });
});

export const layer: Layer.Layer<
  WorktreeMcpService,
  never,
  | SecureRandom.SecureRandom
  | ThreadManagementService.ThreadManagementService
  | ProjectService.ProjectService
  | ServerSettings.ServerSettingsService
  | GitWorkflowService.GitWorkflowService
  | ProjectSetupScriptRunner.ProjectSetupScriptRunner
  | VcsStatusBroadcaster.VcsStatusBroadcaster
> = Layer.effect(WorktreeMcpService, make);
