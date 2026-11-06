import * as ConversationPolicy from "../../../jones/conversationWorkspace/ConversationPolicy.ts";
import type { SourceRequest } from "../../../jones/conversationWorkspace/SourceBroker.ts";
import * as WorkspaceProvenance from "../../../jones/workspaceLifecycle/Provenance.ts";
import { OrchestratorMcpFailure, WorktreeMcpFailure } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as GitWorkflow from "../../../git/GitWorkflowService.ts";
import * as Project from "../../../project/ProjectService.ts";
import { readThread, unavailable } from "../../threadAccess.ts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as WorktreeMcpService from "../../WorktreeMcpService.ts";
import { WorktreeToolkit } from "./tools.ts";

const source = Effect.fn("WorktreeHandlers.source")(function* (request: SourceRequest) {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  if (scope.thread === undefined || !scope.capabilities.has("worktree"))
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Source tools require this conversation's worktree capability.",
    });
  const policy = yield* ConversationPolicy.ConversationPolicy;
  return yield* policy.source(scope.thread.threadId, request).pipe(
    Effect.mapError(
      () =>
        new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "Source request failed or exceeded its bounds; use this turn's recorded commit and a smaller request.",
        }),
    ),
  );
});
const handlers = {
  t3_source_read: McpToolAccess.readsAsCaller((input) => source({ operation: "read", ...input })),
  t3_source_list: McpToolAccess.readsAsCaller((input) => source({ operation: "list", ...input })),
  t3_source_search: McpToolAccess.readsAsCaller((input) =>
    source({ operation: "search", ...input }),
  ),
  t3_source_history: McpToolAccess.readsAsCaller((input) =>
    input.operation === "diff"
      ? input.from === undefined
        ? Effect.fail(
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Diff requires an exact from commit.",
            }),
          )
        : source({ operation: "diff", from: input.from })
      : source({ operation: input.operation }),
  ),
  t3_worktree_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const context = yield* McpInvocationContext.McpInvocationContext;
      if (!context.capabilities.has("worktree"))
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This credential cannot inspect worktrees.",
        });
      const { threadId, provenancePath, ...refs } = input;
      const {
        projection: { thread },
      } = yield* readThread(threadId);
      const projects = yield* Project.ProjectService;
      const project = yield* projects.getById(thread.projectId).pipe(Effect.mapError(unavailable));
      if (Option.isNone(project))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The project was not found.",
        });
      const git = yield* GitWorkflow.GitWorkflowService;
      const result = yield* git
        .listRefs({ ...refs, cwd: thread.worktreePath ?? project.value.workspaceRoot })
        .pipe(Effect.mapError(unavailable));
      const lookup = yield* WorkspaceProvenance.optionalLookup;
      const provenance = yield* lookup(
        provenancePath === undefined ? { threadId: thread.id } : { path: provenancePath },
      );
      return {
        ...result,
        ...(provenance === undefined
          ? {}
          : { provenance: provenance.filter((p) => p.projectId === thread.projectId) }),
      };
    }),
  ),
  t3_worktree_handoff: McpToolAccess.actsAsCallerWithReceipt((input, beforeWrite) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      return yield* service.handoff(
        scope,
        input,
        beforeWrite.pipe(
          Effect.mapError(
            (cause) =>
              new WorktreeMcpFailure({ code: "capability_denied", message: cause.message }),
          ),
        ),
      );
    }),
  ),
  t3_worktree_status: McpToolAccess.readsAsCaller(() =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      return yield* service.status(scope);
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof WorktreeToolkit.tools>;

export const layer = McpToolAccess.toLayer(WorktreeToolkit, handlers);
