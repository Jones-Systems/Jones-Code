import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSettings from "../../../serverSettings.ts";
import * as ServerConfig from "../../../config.ts";
import * as Launch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Receipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import { DispatchModeLimit } from "../../../orchestration-v2/DispatchModeLimit.ts";
import * as Projects from "../../../project/ProjectService.ts";
import * as ManagedFolders from "../../../project/ManagedProjectFolders.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import * as Handlers from "../../../mcp/toolkits/project/handlers.ts";
import { ProjectToolkit } from "../../../mcp/toolkits/project/tools.ts";
import migration from "../../persistence/Migrations/112_JonesClientLaunchRequests.ts";
import { clientLaunchDigest, clientLaunchIds } from "./ClientLaunchIdentity.ts";

const projectId = ProjectId.make("synthetic-project");
const modelSelection = {
  instanceId: ProviderInstanceId.make("synthetic-provider"),
  model: "synthetic-model",
};
const request = { clientRequestId: "key", projectId, title: "New", message: "Task" };
const ids = clientLaunchIds("synthetic-handlers", "key");
const full = { runtimeMode: "full-access", interactionMode: "default" } as const;
const baseScope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("synthetic"),
  issuedAt: 0,
  requestNamespace: "synthetic-handlers",
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: { sessionId: "synthetic", label: "Test", access: "full-access" },
};
const shell = {
  id: ids.threadId,
  projectId,
  modelSelection,
  ...full,
  branch: null,
  worktreePath: null,
  deletedAt: null,
} as OrchestrationV2ThreadShell;

function harness(
  options: {
    scope?: Invocation.McpInvocationScope;
    accepted?: boolean;
    pending?: boolean;
    midDispatch?: boolean;
    callerShell?: OrchestrationV2ThreadShell | null;
  } = {},
) {
  const state = { reads: 0, dispatches: 0, baseChecks: 0 };
  const scope = options.scope ?? baseScope;
  const receipts = new Map<CommandId, Receipts.CommandReceiptV2>();
  const receipt = (
    commandId: CommandId,
    status: "accepted" | "rejected",
    error: string | null = null,
  ): Receipts.CommandReceiptV2 => ({
    commandId,
    threadId: ids.threadId,
    commandType: "thread.create",
    acceptedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
    resultSequence: 0,
    status,
    error,
  });
  if (options.accepted) {
    receipts.set(ids.commandId, receipt(ids.commandId, "accepted"));
    if (!options.pending)
      receipts.set(
        CommandId.make(`${ids.commandId}:initial-message`),
        receipt(CommandId.make(`${ids.commandId}:initial-message`), "accepted"),
      );
  }
  const database = Layer.effectDiscard(
    Effect.gen(function* () {
      yield* migration;
      if (options.accepted) {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO jones_mcp_client_launch_requests VALUES (${ids.commandId}, ${ids.threadId}, ${clientLaunchDigest(request, projectId)}, 1, 1, 'synthetic')`;
      }
    }),
  ).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
  const dependencies = Layer.mergeAll(
    database,
    NodeCrypto.layer,
    NodeServices.layer,
    ServerSettings.layerTest(),
    ServerConfig.layerTest(process.cwd(), { prefix: "jones-client-launch-" }).pipe(
      Layer.provide(NodeServices.layer),
    ),
    Layer.succeed(Invocation.McpInvocationContext, scope),
    Layer.mock(ManagedFolders.ManagedProjectFolders)({ namedProjectsRoot: "/synthetic" }),
    Layer.mock(Git.GitVcsDriver)({}),
    Layer.mock(Receipts.CommandReceiptStoreV2)({
      getByCommandId: (command) =>
        Effect.sync(() => {
          state.reads++;
          return Option.fromNullishOr(receipts.get(command));
        }),
    }),
    Layer.mock(EventSink.EventSinkV2)({
      commitRejectedCommand: (input) =>
        Effect.sync(() => {
          const existing = receipts.get(input.commandId);
          if (existing) return existing;
          const value = receipt(input.commandId, "rejected", input.error);
          receipts.set(input.commandId, value);
          return value;
        }),
    }),
    Layer.mock(Projects.ProjectService)({
      getById: () =>
        Effect.succeed(
          Option.some({
            id: projectId,
            title: "Project",
            workspaceRoot: "/synthetic",
            repositoryIdentity: null,
            faviconPath: null,
            defaultModelSelection: modelSelection,
            defaultThreadEnvMode: null,
            scripts: [],
            createdAt: "2026-01-01T00:00:00Z",
            updatedAt: "2026-01-01T00:00:00Z",
            deletedAt: null,
          }),
        ),
    }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.sync(() => {
          if (id === scope.thread?.threadId) return options.callerShell ?? null;
          state.reads++;
          return options.accepted ? shell : null;
        }),
      getThreadProjection: () =>
        Effect.succeed({
          thread: shell,
          runs: options.pending
            ? []
            : [{ id: RunId.make("bound"), userMessageId: ids.messageId, status: "starting" }],
        } as unknown as Launch.ThreadLaunchResult["projection"]),
    }),
    Layer.mock(Launch.ThreadLaunchService)({
      preparationActive: () => Effect.succeed(false),
      checkWorktreeBase: (input) =>
        Effect.gen(function* () {
          state.baseChecks++;
          return yield* new Launch.ThreadLaunchBaseRefError({
            projectId: input.projectId,
            projectTitle: "Synthetic",
            workspaceRoot: "/synthetic",
            baseRef: "missing",
          });
        }),
      launch: (input) =>
        Effect.gen(function* () {
          if (options.midDispatch) {
            const guard = yield* DispatchModeLimit;
            if (guard?.refused === undefined)
              return yield* Effect.die("Continuation dispatch guard missing");
            yield* Ref.set(guard.refused, { mode: "runtime", threadId: ids.threadId, ...full });
            return yield* new Launch.ThreadLaunchError({
              operation: "dispatch-message",
              commandId: input.commandId,
              projectId,
              cause: "synthetic dispatch refusal",
            });
          }
          state.dispatches++;
          return {
            threadId: input.threadId!,
            projection: { thread: { ...shell, id: input.threadId! }, runs: [] },
            resumed: false,
          } as unknown as Launch.ThreadLaunchResult;
        }),
    }),
  );
  return { state, dependencies };
}

const execute = <A, E, R>(h: ReturnType<typeof harness>, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(h.dependencies));
const tool = Effect.gen(function* () {
  return yield* ProjectToolkit.pipe(Effect.provide(Access.HandlersLayer.layer(Handlers.layer)));
});
const code = (result: ReadonlyArray<{ readonly result: unknown }>) =>
  (result.at(-1)!.result as { code?: string }).code;

it.effect.each(["t3_thread_launch", "t3_thread_launch_lookup", "t3_thread_launch_fence"] as const)(
  "T14: %s checks orchestration before launch state",
  (name) => {
    const h = harness({
      scope: {
        ...baseScope,
        capabilities: new Set(),
        client: { ...baseScope.client!, access: "approval-required" },
      },
    });
    return execute(
      h,
      Effect.gen(function* () {
        const toolkit = yield* tool;
        const result =
          name === "t3_thread_launch"
            ? yield* toolkit
                .handle(name, { ...request, runtimeMode: "full-access" })
                .pipe(Stream.unwrap, Stream.runCollect)
            : yield* toolkit
                .handle(name, { clientRequestId: "key" })
                .pipe(Stream.unwrap, Stream.runCollect);
        assert.equal(code(result), "capability_denied");
        assert.equal(h.state.reads, 0);
        assert.equal(h.state.dispatches, 0);
      }),
    );
  },
);
it.effect.each(["t3_thread_launch", "t3_thread_launch_fence"] as const)(
  "T14: read-only client cannot call %s",
  (name) => {
    const h = harness({
      scope: { ...baseScope, client: { ...baseScope.client!, access: "read-only" } },
    });
    return execute(
      h,
      Effect.gen(function* () {
        const toolkit = yield* tool;
        const result =
          name === "t3_thread_launch"
            ? yield* toolkit.handle(name, request).pipe(Stream.unwrap, Stream.runCollect)
            : yield* toolkit
                .handle(name, { clientRequestId: "key" })
                .pipe(Stream.unwrap, Stream.runCollect);
        assert.equal(code(result), "capability_denied");
        assert.equal(h.state.reads, 0);
        const lookup = yield* toolkit
          .handle("t3_thread_launch_lookup", { clientRequestId: "key" })
          .pipe(Stream.unwrap, Stream.runCollect);
        assert.equal((lookup.at(-1)!.result as { state: string }).state, "absent");
      }),
    );
  },
);
it.effect.each([
  { scratch: false },
  {
    attachments: [
      {
        type: "file",
        id: "pending_synthetic",
        name: "synthetic.txt",
        mimeType: "text/plain",
        sizeBytes: 1,
      },
    ],
  },
  { workspaceStrategy: { type: "worktree" as const }, message: "" },
  { projectId: undefined },
])("T3: refuses keyed unsupported input %j before claiming", (change) => {
  const h = harness();
  return execute(
    h,
    Effect.gen(function* () {
      const toolkit = yield* tool;
      const result = yield* toolkit
        .handle("t3_thread_launch", { ...request, ...change } as typeof request)
        .pipe(Stream.unwrap, Stream.runCollect);
      assert.equal(code(result), "invalid_request");
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT * FROM jones_mcp_client_launch_requests`, []);
      assert.equal(h.state.dispatches, 0);
    }),
  );
});
it.effect("T11: a narrower caller cannot continue, but may read a completed replay", () =>
  Effect.gen(function* () {
    for (const pending of [true, false]) {
      const h = harness({
        accepted: true,
        pending,
        scope: { ...baseScope, client: { ...baseScope.client!, access: "approval-required" } },
      });
      yield* execute(
        h,
        Effect.gen(function* () {
          const toolkit = yield* tool;
          const result = yield* toolkit
            .handle("t3_thread_launch", request)
            .pipe(Stream.unwrap, Stream.runCollect);
          if (pending) assert.equal(code(result), "runtime_mode_escalation_denied");
          else assert.isTrue((result.at(-1)!.result as { replayed: boolean }).replayed);
          assert.equal(h.state.dispatches, 0);
        }),
      );
    }
  }),
);
it.effect("T11: a mid-dispatch refusal uses the continuation guard and commits nothing", () => {
  const h = harness({ accepted: true, pending: true, midDispatch: true });
  return execute(
    h,
    Effect.gen(function* () {
      const toolkit = yield* tool;
      const result = yield* toolkit
        .handle("t3_thread_launch", request)
        .pipe(Stream.unwrap, Stream.runCollect);
      assert.equal(code(result), "runtime_mode_escalation_denied");
      assert.equal(h.state.dispatches, 0);
    }),
  );
});
it.effect("T10: unkeyed mode refusal retains precedence over missing orchestration", () => {
  const h = harness({
    scope: {
      ...baseScope,
      capabilities: new Set(),
      client: { ...baseScope.client!, access: "approval-required" },
    },
  });
  return execute(
    h,
    Effect.gen(function* () {
      const toolkit = yield* tool;
      const result = yield* toolkit
        .handle("t3_thread_launch", { title: "Unkeyed", projectId, runtimeMode: "full-access" })
        .pipe(Stream.unwrap, Stream.runCollect);
      assert.equal(code(result), "runtime_mode_escalation_denied");
      assert.equal(h.state.reads, 0);
    }),
  );
});

it.effect("T3: base preflight and mode refusals leave no claim", () =>
  Effect.gen(function* () {
    for (const ceiling of ["full-access", "approval-required"] as const) {
      const h = harness({
        scope: { ...baseScope, client: { ...baseScope.client!, access: ceiling } },
      });
      yield* execute(
        h,
        Effect.gen(function* () {
          const toolkit = yield* tool;
          const result = yield* toolkit
            .handle("t3_thread_launch", {
              ...request,
              runtimeMode: "full-access",
              workspaceStrategy: { type: "worktree", baseRef: "missing" },
            })
            .pipe(Stream.unwrap, Stream.runCollect);
          assert.equal(
            code(result),
            ceiling === "full-access" ? "invalid_request" : "runtime_mode_escalation_denied",
          );
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`SELECT * FROM jones_mcp_client_launch_requests`, []);
          assert.equal(h.state.dispatches, 0);
        }),
      );
    }
  }),
);
it.effect.each([
  { access: "read-only" as const, callerShell: null, expected: "capability_denied" },
  { access: "full-access" as const, callerShell: null, expected: "thread_not_found" },
  {
    access: "full-access" as const,
    callerShell: { ...shell, activeRunId: null, archivedAt: null } as OrchestrationV2ThreadShell,
    expected: "parent_not_active",
  },
])(
  "T10: unkeyed wrapper failures keep their original precedence",
  ({ access, callerShell, expected }) => {
    const h = harness({
      callerShell,
      scope: {
        ...baseScope,
        capabilities: new Set(),
        client: { ...baseScope.client!, access },
        thread: {
          threadId: ThreadId.make("synthetic-caller"),
          providerSessionId: "synthetic",
          providerInstanceId: modelSelection.instanceId,
        },
      },
    });
    return execute(
      h,
      Effect.gen(function* () {
        const toolkit = yield* tool;
        const result = yield* toolkit
          .handle("t3_thread_launch", { title: "Unkeyed", projectId, runtimeMode: "full-access" })
          .pipe(Stream.unwrap, Stream.runCollect);
        assert.equal(code(result), expected);
        assert.equal(h.state.reads, 0);
        assert.equal(h.state.dispatches, 0);
      }),
    );
  },
);
