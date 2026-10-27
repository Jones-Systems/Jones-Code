import { makeCommit } from "./Transactions.ts";
import {
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  ThreadRequestFulfillInput,
  ThreadRequestFulfillResult,
  ThreadDependencyReleaseInput,
  ThreadDependencyReleaseResult,
  ThreadRequestObserveFields,
  ThreadRequestObservation,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { Tool } from "effect/ai";
import * as Access from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import { readCaller, unavailable } from "../../mcp/threadAccess.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as Changes from "./DependencyChanges.ts";
import * as Watch from "./RequestWatch.ts";

export const ObserveInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  ...ThreadRequestObserveFields,
});
const dependencies = [
  Invocation.McpInvocationContext,
  Threads.ThreadManagementService,
  SqlClient.SqlClient,
  Changes.DependencyChanges,
];
export const ThreadRequestFulfillTool = Tool.make("t3_thread_request_fulfill", {
  description:
    "Answer or decline a Result requested message by its exact requestId. Only this receiving thread can answer. Supply the bounded requested result or a decline reason. Ordinary replies and ending a run do not answer. The first explicit answer is immutable; retry the identical answer safely.",
  parameters: ThreadRequestFulfillInput,
  success: ThreadRequestFulfillResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);
export const ThreadDependencyReleaseTool = Tool.make("t3_thread_dependency_release", {
  description:
    "Release this thread's result dependency without waking it or stopping the target. Stop releases all outstanding dependencies. A released dependency cannot be reopened by a late answer.",
  parameters: ThreadDependencyReleaseInput,
  success: ThreadDependencyReleaseResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);
export const ThreadRequestObserveTool = Tool.make("t3_thread_request_observe", {
  description:
    "Server transport observation of one authenticated originating environment's exact result operation. Subscribe then read, up to 50000 ms. A fence reconciles a lost dispatch without replaying it. Observation cannot fulfill a request or cancel remote work. Agents should use awaitResult on send and end their turn rather than poll this tool.",
  parameters: ObserveInput,
  success: ThreadRequestObservation,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    Invocation.McpInvocationContext,
    Threads.ThreadManagementService,
    Watch.RequestWatch,
  ],
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const update = <A, E, R>(
  run: (
    store: ReturnType<typeof makeDependencyStore>,
    threadId: import("@t3tools/contracts").ThreadId,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const { scope } = yield* readCaller();
    const bound = yield* Invocation.requireThreadScope(scope, "Result requests");
    const sql = yield* SqlClient.SqlClient;
    const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
    const changes = yield* Changes.DependencyChanges;
    return yield* makeCommit(sql, store, changes)(run(store, bound.thread.threadId));
  }).pipe(
    Effect.mapError((error) =>
      Schema.is(OrchestratorMcpFailure)(error) ? error : unavailable(),
    ),
  );

export const threadRequestFulfillHandler = Access.actsAsCaller(
  (input: typeof ThreadRequestFulfillInput.Type) =>
    update((store, threadId) =>
      Effect.gen(function* () {
        const { threads } = yield* readCaller();
        const records = yield* threads.getThreadRecords(threadId, ["runs"]);
        const run = records.runs
          .filter((run) => ["starting", "running", "waiting"].includes(run.status))
          .toSorted((a, b) => b.ordinal - a.ordinal)[0];
        return yield* store.answer(
          threadId,
          input.requestId,
          input.outcome,
          input.result,
          run?.id ?? null,
        );
      }),
    ),
);
export const threadDependencyReleaseHandler = Access.actsAsCaller(
  (input: typeof ThreadDependencyReleaseInput.Type) =>
    update((store, threadId) =>
      store.release(threadId, input.dependencyId).pipe(Effect.map((released) => ({ released }))),
    ),
);
export const threadRequestObserveHandler = Access.reads((input: typeof ObserveInput.Type) =>
  Effect.gen(function* () {
    const scope = yield* Invocation.McpInvocationContext;
    const origin = scope.client?.remoteOrigin;
    if (
      scope.client?.linked !== true ||
      origin === undefined ||
      origin.linkSessionId !== scope.client.sessionId
    )
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "Result observation requires an authenticated linked origin.",
      });
    const watch = yield* Watch.RequestWatch;
    return yield* watch.observe({
      ...input,
      originEnvironmentId: origin.environmentId,
      timeoutMs: input.timeoutMs ?? 50_000,
    });
  }),
);
