import { CommandId, OrchestratorMcpFailure, type ClientLaunchLookup } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import type { Caller } from "../../../mcp/threadAccess.ts";
import { unavailable } from "../../../mcp/threadAccess.ts";
import type { StartedModes, ThreadStartGate } from "../../../mcp/McpToolAccess.ts";
import {
  clientLaunchIds,
  clientLaunchDigest,
  type ClientLaunchRequest,
} from "./ClientLaunchIdentity.ts";

interface Claim {
  readonly request_digest: string;
  readonly message_requested: number;
}
const isMcpFailure = Schema.is(OrchestratorMcpFailure);
const fenceReason = "jones-client-launch:v1:fenced";
const readClaim = (commandId: CommandId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return (yield* sql<Claim>`SELECT request_digest, message_requested FROM jones_mcp_client_launch_requests WHERE command_id = ${commandId}`)[0];
  }).pipe(Effect.mapError(unavailable));
const readReceipt = (commandId: CommandId) =>
  Receipts.CommandReceiptStoreV2.pipe(
    Effect.flatMap((receipts) => receipts.getByCommandId(commandId)),
    Effect.mapError(unavailable),
    Effect.map(Option.getOrUndefined),
  );

export const lookup = Effect.fn("jones.clientLaunch.lookup")(function* (
  namespace: string,
  clientRequestId: string,
): Effect.fn.Return<
  ClientLaunchLookup,
  OrchestratorMcpFailure,
  | SqlClient.SqlClient
  | Receipts.CommandReceiptStoreV2
  | ThreadLaunch.ThreadLaunchService
  | ThreadManagement.ThreadManagementService
> {
  const ids = clientLaunchIds(namespace, clientRequestId);
  const receipt = yield* readReceipt(ids.commandId);
  const claim = yield* readClaim(ids.commandId);
  const base: ClientLaunchLookup = {
    clientRequestId,
    state: "absent",
    basis: "none",
    threadId: null,
    requestDigest: claim?.request_digest ?? null,
    claimed: claim !== undefined,
    threadDeleted: false,
    initialMessage:
      claim === undefined ? "unknown" : claim.message_requested === 0 ? "not_requested" : "pending",
    runId: null,
    runStatus: null,
    preparation: "not_applicable",
    workspace: null,
    reason: null,
  };
  if (receipt === undefined) return base;
  if (receipt.status === "rejected")
    return {
      ...base,
      state: "no_effect",
      basis: receipt.error === fenceReason ? "fence" : "rejection",
      reason: receipt.error,
    };
  const threads = yield* ThreadManagement.ThreadManagementService;
  const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
  const messageReceipt =
    claim?.message_requested === 1
      ? yield* readReceipt(CommandId.make(`${ids.commandId}:initial-message`))
      : undefined;
  const initialMessage: ClientLaunchLookup["initialMessage"] =
    claim === undefined
      ? "unknown"
      : claim.message_requested === 0
        ? "not_requested"
        : (messageReceipt?.status ?? "pending");
  const launched = {
    ...base,
    state: "launched" as const,
    basis: "receipt" as const,
    threadId: ids.threadId,
    initialMessage,
    reason: messageReceipt?.status === "rejected" ? messageReceipt.error : null,
  };
  if (shell === null || shell.deletedAt !== null) return { ...launched, threadDeleted: true };
  const projection = yield* threads
    .getThreadProjection(ids.threadId)
    .pipe(Effect.mapError(unavailable));
  const run = projection.runs.find((entry) => entry.userMessageId === ids.messageId);
  let preparation: ClientLaunchLookup["preparation"] = "not_applicable";
  if (run !== undefined) {
    if (run.status !== "preparing") preparation = "left_preparation";
    else {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      if (yield* launches.preparationActive(ids.commandId)) preparation = "in_progress";
      else {
        const worktree = yield* readReceipt(CommandId.make(`${ids.commandId}:progress:worktree`));
        const setup = yield* readReceipt(CommandId.make(`${ids.commandId}:progress:setup`));
        preparation =
          worktree?.status === "accepted" || setup?.status === "accepted"
            ? "unknown_effect"
            : "unscheduled";
      }
    }
  }
  return {
    ...launched,
    runId: run?.id ?? null,
    runStatus: run?.status ?? null,
    preparation,
    workspace: {
      type: shell.worktreePath === null ? "root" : "existing_worktree",
      branch: shell.branch,
      worktreePath: shell.worktreePath,
    },
  };
});

export const fence = Effect.fn("jones.clientLaunch.fence")(function* (
  namespace: string,
  clientRequestId: string,
) {
  const ids = clientLaunchIds(namespace, clientRequestId);
  if ((yield* readReceipt(ids.commandId)) === undefined) {
    const sink = yield* EventSink.EventSinkV2;
    yield* sink
      .commitRejectedCommand({
        commandId: ids.commandId,
        threadId: ids.threadId,
        commandType: "thread.create",
        rejectedAt: yield* DateTime.now,
        error: fenceReason,
      })
      .pipe(Effect.mapError(unavailable));
  }
  const result = yield* lookup(namespace, clientRequestId);
  if (result.state === "absent") return yield* unavailable();
  return { ...result, state: result.state };
});

export const launch = Effect.fn("jones.clientLaunch.launch")(function* (
  input: ClientLaunchRequest,
  caller: Caller,
  modes: StartedModes,
  gate: ThreadStartGate,
) {
  if (
    input.scratch !== undefined ||
    (input.attachments?.length ?? 0) > 0 ||
    (input.workspaceStrategy?.type === "worktree" && !input.message?.trim()) ||
    (caller.caller === undefined && input.projectId === undefined)
  ) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Keyed launches require a project, no scratch or attachments, and a message for a new worktree.",
    });
  }
  const projectId = input.projectId ?? caller.caller!.projectId;
  const ids = clientLaunchIds(caller.scope.requestNamespace, input.clientRequestId);
  const digest = yield* Effect.try({
    try: () => clientLaunchDigest(input, projectId),
    catch: () =>
      new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "The launch request is not canonical JSON.",
      }),
  });
  const receipt = yield* readReceipt(ids.commandId);
  if (receipt?.status === "rejected")
    return yield* new OrchestratorMcpFailure({
      code: "launch_fenced",
      message: "This launch key has an authoritative rejection.",
    });
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const workspaceStrategy = input.workspaceStrategy ?? { type: "root" as const };
  const initialMessage =
    input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: ids.messageId,
            text: input.message,
            attachments: [],
            ...(caller.caller === undefined ? {} : { senderThreadId: caller.caller.id }),
          },
        };
  const common = {
    commandId: ids.commandId,
    threadId: ids.threadId,
    projectId,
    title: input.title.trim(),
    workspaceStrategy,
    ...initialMessage,
    createdBy: "agent" as const,
    creationSource: "mcp" as const,
  };
  const invoke = (request: ThreadLaunch.ThreadLaunchInput) =>
    launches.launch(request).pipe(Effect.mapError(unavailable));
  let replayed = receipt?.status === "accepted";
  const continueAccepted = () =>
    Effect.gen(function* () {
      const claim = yield* readClaim(ids.commandId);
      if (claim?.request_digest !== digest)
        return yield* new OrchestratorMcpFailure({
          code: "request_conflict",
          message: "The key belongs to a different launch request.",
        });
      const state = yield* lookup(caller.scope.requestNamespace, input.clientRequestId);
      if (state.threadDeleted)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The recorded launch thread was deleted; this key cannot relaunch it.",
        });
      const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
      if (shell === null) return yield* unavailable();
      if (
        state.initialMessage === "pending" ||
        (state.initialMessage === "accepted" && state.preparation === "unscheduled")
      ) {
        yield* gate.continueWithin(
          shell,
          invoke({
            ...common,
            modelSelection: shell.modelSelection,
            runtimeMode: shell.runtimeMode,
            interactionMode: shell.interactionMode,
          }),
        );
      }
    });
  if (replayed) {
    yield* continueAccepted();
  } else {
    const projects = yield* Project.ProjectService;
    const project = yield* projects
      .getById(projectId)
      .pipe(Effect.mapError(unavailable), Effect.map(Option.getOrUndefined));
    if (project === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "The project was not found.",
      });
    if (workspaceStrategy.type === "existing_worktree") {
      const fs = yield* FileSystem.FileSystem;
      const git = yield* Git.GitVcsDriver;
      const real = (path: string) => fs.realPath(path).pipe(Effect.orElseSucceed(() => path));
      const paths = yield* git.listWorktreePaths(project.workspaceRoot).pipe(
        Effect.flatMap((paths) => Effect.forEach(paths, real)),
        Effect.orElseSucceed((): ReadonlyArray<string> => []),
      );
      if (!paths.includes(yield* real(workspaceStrategy.worktreePath)))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "worktreePath must be one of the project's git worktrees.",
        });
    }
    if (workspaceStrategy.type === "worktree")
      yield* launches
        .checkWorktreeBase({ projectId, workspaceStrategy })
        .pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "invalid_request",
                message:
                  "The requested worktree base could not be resolved. No thread was created.",
              }),
          ),
        );
    const modelSelection =
      input.modelSelection ??
      caller.caller?.modelSelection ??
      project.defaultModelSelection ??
      undefined;
    if (modelSelection === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Pass modelSelection: the project has no default model.",
      });
    const sql = yield* SqlClient.SqlClient;
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const currentReceipt = yield* readReceipt(ids.commandId);
          if (currentReceipt?.status === "rejected")
            return yield* new OrchestratorMcpFailure({
              code: "launch_fenced",
              message: "This launch key has an authoritative rejection.",
            });
          const claim = yield* readClaim(ids.commandId);
          if (claim !== undefined) {
            if (claim.request_digest !== digest)
              return yield* new OrchestratorMcpFailure({
                code: "request_conflict",
                message: "The key belongs to a different launch request.",
              });
          } else {
            if (currentReceipt !== undefined)
              return yield* new OrchestratorMcpFailure({
                code: "request_conflict",
                message: "The existing receipt has no matching claim.",
              });
            yield* sql`INSERT INTO jones_mcp_client_launch_requests (command_id, thread_id, request_digest, request_version, message_requested, recorded_at) VALUES (${ids.commandId}, ${ids.threadId}, ${digest}, 1, ${input.message === undefined ? 0 : 1}, ${DateTime.formatIso(yield* DateTime.now)})`.pipe(
              Effect.mapError(unavailable),
            );
          }
        }),
      )
      .pipe(Effect.mapError((error) => (isMcpFailure(error) ? error : unavailable())));
    // Another identical caller may have accepted while this call was in preflight.
    // Reconcile that receipt with recorded modes before attempting continuation.
    if ((yield* readReceipt(ids.commandId))?.status === "accepted") {
      replayed = true;
      yield* continueAccepted();
    } else {
      const result = yield* gate
        .continueWithin(modes, invoke({ ...common, modelSelection, ...modes }))
        .pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              if ((yield* readReceipt(ids.commandId))?.status === "rejected")
                return yield* new OrchestratorMcpFailure({
                  code: "launch_fenced",
                  message: "This launch key has an authoritative rejection.",
                });
              return yield* error;
            }),
          ),
        );
      replayed = result.resumed;
    }
  }
  const state = yield* lookup(caller.scope.requestNamespace, input.clientRequestId);
  const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
  if (shell === null) return yield* unavailable();
  return {
    threadId: ids.threadId,
    projectId: shell.projectId,
    modelSelection: shell.modelSelection,
    runId: state.runId,
    status: state.runStatus,
    replayed,
    requestDigest: digest,
    initialMessage: state.initialMessage,
    preparation: state.preparation,
  };
});
