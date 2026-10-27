import { OrchestratorMcpFailure, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { assertSameLink } from "../../mcp/linkOrigin.ts";
import { assertLiveCaller, assertTargetWithinLimits, readCaller, unavailable } from "../../mcp/threadAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../../project/ProjectService.ts";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";

export class DocumentRepositoryAccess extends Context.Service<
  DocumentRepositoryAccess,
  { readonly resolve: (target?: ThreadId) => Effect.Effect<
    AuthorizedRepository, OrchestratorMcpFailure,
    Invocation.McpInvocationContext | Threads.ThreadManagementService
  > }
>()("jones/documentSections/DocumentRepositoryAccess") {}

const make = Effect.gen(function* () {
  const projects = yield* Projects.ProjectService;
  const resolve: DocumentRepositoryAccess["Service"]["resolve"] = Effect.fn(
    "DocumentRepositoryAccess.resolve",
  )(function* (target) {
    const caller = yield* readCaller();
    // Thread credentials always take this branch, even if a client is also present.
    // A malformed or stale agent never falls back to owner-client authority.
    if (caller.scope.thread !== undefined) {
      yield* assertLiveCaller(caller);
      if (target !== undefined && target !== caller.scope.thread.threadId)
        return yield* new OrchestratorMcpFailure({ code: "capability_denied",
          message: "Document extraction is limited to the calling thread's workspace." });
    } else if (caller.scope.client === undefined) {
      return yield* new OrchestratorMcpFailure({ code: "capability_denied",
        message: "Document extraction requires an authenticated caller." });
    }
    const threadId = caller.scope.thread?.threadId ?? target;
    if (threadId === undefined)
      return yield* new OrchestratorMcpFailure({ code: "target_required",
        message: "Pass threadId to select an authorized workspace." });
    const shell = yield* caller.threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
    if (shell === null || shell.deletedAt !== null)
      return yield* new OrchestratorMcpFailure({ code: "thread_not_found",
        message: "The authorized workspace is unavailable." });
    yield* assertTargetWithinLimits(caller.limits, shell);
    yield* assertSameLink(caller, shell);
    const project = yield* projects.getById(shell.projectId).pipe(Effect.mapError(unavailable));
    if (Option.isNone(project))
      return yield* new OrchestratorMcpFailure({ code: "project_not_found",
        message: "The authorized workspace is unavailable." });
    return AuthorizedRepository.fromAuthorizedWorkspace(shell.worktreePath ?? project.value.workspaceRoot);
  });
  return DocumentRepositoryAccess.of({ resolve });
});

export const layer = Layer.effect(DocumentRepositoryAccess, make);
