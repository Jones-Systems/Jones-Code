// @effect-diagnostics nodeBuiltinImport:off -- Synthetic Git repositories and exact fixture teardown; no provider executable or installed state.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeChildProcess from "node:child_process";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import {
  EnvironmentId,
  GitCommandError,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import { makeAdapterRuntimeProbe } from "./AdapterRuntime.testkit.ts";
vi.mock("@anthropic-ai/claude-agent-sdk", async (original) => ({
  ...(await original<typeof import("@anthropic-ai/claude-agent-sdk")>()),
  resolveSettings: vi.fn(async () => ({ effective: {}, provenance: {}, sources: [] })),
}));
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Setup from "../../project/ProjectSetupScriptRunner.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as VcsStatus from "../../vcs/VcsStatusBroadcaster.ts";
import * as Worktree from "../../mcp/WorktreeMcpService.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as ToolAccess from "../../mcp/McpToolAccess.ts";
import * as Handlers from "../../mcp/toolkits/worktree/handlers.ts";
import { WorktreeToolkit } from "../../mcp/toolkits/worktree/tools.ts";
import RetirementMigration from "../persistence/Migrations/138_JonesWorkspaceRetirement.ts";
import Migration from "../persistence/Migrations/139_JonesConversationWorkspace.ts";
import * as Policy from "./ConversationPolicy.ts";

const git = (cwd: string, ...args: string[]) =>
  NodeChildProcess.execFileSync(
    "git",
    [
      "-c",
      "init.defaultBranch=main",
      "-c",
      "user.name=C9 Fixture",
      "-c",
      "user.email=c9@example.invalid",
      ...args,
    ],
    {
      cwd,
      encoding: "utf8",
      timeout: 3000,
      maxBuffer: 1024 * 1024,
      env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" },
    },
  ).trim();

it.effect.each([
  { driver: "codex", scenario: "handoff" },
  { driver: "claudeAgent", scenario: "handoff" },
  { driver: "codex", scenario: "registered-callback" },
  { driver: "claudeAgent", scenario: "registered-callback" },
  { driver: "codex", scenario: "checkout-interruption" },
  { driver: "claudeAgent", scenario: "setup-unknown" },
  { driver: "codex", scenario: "terminal" },
  { driver: "claudeAgent", scenario: "nonempty" },
  { driver: "codex", scenario: "shared" },
] as const)("$driver adapter runtime: $scenario", ({ driver, scenario }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const root = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const cache = NodePath.join(NodeOS.homedir(), "Library", "Caches");
          await NodeFSP.mkdir(cache, { recursive: true });
          return NodeFSP.mkdtemp(NodePath.join(cache, "jones-c9-adapter-"));
        }),
        (root) => Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
      );
      const repo = NodePath.join(root, "repo");
      const state = NodePath.join(root, "state");
      let checkout = NodePath.join(root, "checkout");
      yield* Effect.promise(async () => {
        await NodeFSP.mkdir(repo);
        await NodeFSP.mkdir(state);
        git(repo, "init", "--template=");
        await NodeFSP.writeFile(NodePath.join(repo, "AGENTS.md"), "Read the recorded source.\n");
        git(repo, "add", ".");
        git(repo, "commit", "-qm", "source");
      });
      const threadId = ThreadId.make(`thread:adapter:${driver}`);
      const projectId = ProjectId.make("project:c9");
      const thread = {
        id: threadId,
        projectId,
        worktreePath: null as string | null,
        branch: null as string | null,
        deletedAt: null,
        archivedAt: null,
      };
      const members = new Map([[threadId, thread]]);
      let created = 0;
      let setupCount = 0;
      const sent = new Map<string, string>();
      const database = NodeSqlite.layer({ filename: ":memory:" });
      const setupLayer = Layer.unwrap(
        Effect.gen(function* () {
          // Capture the same memoized database as Policy and the fixture body.
          const sql = yield* SqlClient.SqlClient;
          return Layer.mock(Setup.ProjectSetupScriptRunner)({
            runForThread: (input) =>
              Effect.gen(function* () {
                assert.equal(input.worktreePath, checkout);
                assert.equal(input.projectCwd, repo);
                assert.lengthOf(
                  yield* sql`SELECT claim_id FROM jones_workspace_duty_claims
                    WHERE worktree=${checkout} AND kind='handoff' AND owner=${threadId} AND released_at IS NULL`.pipe(
                    Effect.orDie,
                  ),
                  1,
                );
                setupCount++;
                if (scenario === "setup-unknown")
                  return yield* new Setup.ProjectSetupScriptOperationError({
                    threadId,
                    worktreePath: checkout,
                    operation: "writeCommand",
                    cause: "Synthetic setup acknowledgement lost",
                  });
                return { status: "no-script" as const };
              }),
          });
        }),
      ).pipe(Layer.provide(database));
      const external = Layer.mergeAll(
        Layer.mock(Threads.ThreadManagementService)({
          getThreadRecords: (id) => Effect.succeed({ thread: members.get(id)! } as never),
          dispatch: (command) =>
            Effect.sync(() => {
              if (command.type === "thread.metadata.update") {
                const target = members.get(command.threadId)!;
                target.worktreePath = command.worktreePath ?? target.worktreePath;
                target.branch = command.branch ?? target.branch;
              }
              return { sequence: 1, storedEvents: [] } as never;
            }),
          sendToThread: (input) =>
            Effect.sync(() => {
              sent.set(input.commandId, input.text);
              return { delivery: "queued" as const } as never;
            }),
        }),
        Layer.mock(Projects.ProjectService)({
          getById: () =>
            Effect.succeedSome({ id: projectId, workspaceRoot: repo, scripts: [] } as never),
        }),
        Layer.mock(GitWorkflow.GitWorkflowService)({
          createWorktree: (input) =>
            Effect.gen(function* () {
              const result = yield* Effect.sync(() => {
                created++;
                checkout = input.path!;
                git(repo, "worktree", "add", "-b", input.newRefName!, checkout, input.refName);
                return {
                  worktree: {
                    path: checkout,
                    refName: input.newRefName,
                    headSha: git(checkout, "rev-parse", "HEAD"),
                  },
                } as never;
              });
              if (scenario === "checkout-interruption" && created === 1)
                return yield* new GitCommandError({
                  operation: "createWorktree",
                  command: "git",
                  cwd: repo,
                  detail: "Synthetic reply lost after checkout creation",
                });
              return result;
            }),
          listRefs: () =>
            Effect.succeed({
              refs:
                created === 0
                  ? []
                  : [{ name: "work/c9-proof", worktreePath: checkout, isRemote: false }],
              isRepo: true,
            } as never),
        }),
        setupLayer,
        Layer.mock(VcsStatus.VcsStatusBroadcaster)({ refreshStatus: () => Effect.void as never }),
        ServerSettings.layerTest({ newWorktreesStartFromOrigin: true }),
      );
      const config = ServerConfig.layerTest(root, state).pipe(
        Layer.provide(NodeServices.layer),
        Layer.orDie,
      );
      const policyLayer = Policy.layer.pipe(Layer.provideMerge(database), Layer.provide(config));
      let policyBeforeWorktree: Policy.ConversationPolicy["Service"] | undefined;
      // The orchestrator/turn-start side receives Policy before the outer MCP
      // layer exists. Worktree construction must register on that same instance.
      const beforeWorktree = Layer.effectDiscard(
        Effect.gen(function* () {
          const policy = yield* Policy.ConversationPolicy;
          policyBeforeWorktree = policy;
          const refusal = yield* policy
            .attachForTurn(threadId, "run:before-registration", "Continue", [])
            .pipe(Effect.flip);
          assert.equal(refusal.reason, "attach_required");
          assert.equal(created, 0);
          assert.equal(setupCount, 0);
        }),
      ).pipe(Layer.provide(policyLayer));
      const worktreeLayer = Worktree.layer.pipe(
        Layer.provide(external),
        Layer.provide(policyLayer),
        Layer.provide(beforeWorktree),
      );
      const services = Layer.mergeAll(
        external,
        policyLayer,
        worktreeLayer,
        config,
        IdAllocator.layer,
        McpSessions.layer,
        ControlAdmission.layer,
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* RetirementMigration;
        yield* Migration;
        yield* sql`CREATE TABLE projection_projects(workspace_root TEXT, deleted_at TEXT)`;
        yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT, deleted_at TEXT)`;
        yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${threadId},${JSON.stringify({ id: threadId, projectId })},NULL)`;
        const policy = yield* Policy.ConversationPolicy;
        const worktree = yield* Worktree.WorktreeMcpService;
        assert.equal(policy, policyBeforeWorktree);
        const binding = yield* policy.allocate({
          threadId,
          projectId,
          projectRoot: repo,
          baseRef: "main",
        });
        const childId = ThreadId.make("thread:shared:c9");
        if (scenario === "shared") {
          members.set(childId, { ...thread, id: childId });
          yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${childId},${JSON.stringify({ id: childId, projectId })},NULL)`;
          yield* policy.inherit(childId, threadId);
        }
        const turn = yield* policy.startTurn(threadId, "run:proof");
        assert.isNotNull(turn);
        assert.equal(created, 0);
        assert.equal(setupCount, 0);
        const scope: Invocation.McpThreadInvocationScope = {
          environmentId: EnvironmentId.make("environment:c9"),
          requestNamespace: "c9",
          issuedAt: Date.UTC(2026, 0, 1),
          client: undefined,
          capabilities: new Set(["worktree"]),
          thread: {
            threadId,
            providerInstanceId: ProviderInstanceId.make(driver),
            providerSessionId: `session:${driver}`,
          },
        };
        const mcpSession = {
          environmentId: scope.environmentId,
          threadId,
          providerInstanceId: scope.thread.providerInstanceId,
          providerSessionId: scope.thread.providerSessionId,
          endpoint: "http://127.0.0.1:1/mcp",
          authorizationHeader: "Bearer synthetic",
          browserToolsAvailable: false,
        };
        const probe = yield* makeAdapterRuntimeProbe(driver, threadId, mcpSession);
        const providerThread = yield* probe.run(binding.path);
        assert.isTrue(probe.observed.some((call) => call.cwd === binding.path && call.mcp));
        const toolkit = yield* WorktreeToolkit.pipe(
          Effect.provide(
            ToolAccess.HandlersLayer.layer(Handlers.layer).pipe(Layer.provide(services)),
          ),
        );
        const source = yield* toolkit
          .handle("t3_source_read", { path: "AGENTS.md" })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provideService(Invocation.McpInvocationContext, scope),
          );
        assert.include(JSON.stringify(source), "Read the recorded source.");
        assert.include(JSON.stringify(source), turn!.binding.commit);
        const input = {
          clientRequestId: "proof:attach",
          branch: "work/c9-proof",
          continuationPrompt: "Continue the original conversation in the checkout.",
        };
        if (scenario === "nonempty")
          yield* Effect.promise(() =>
            NodeFSP.writeFile(NodePath.join(binding.path, "owner-note"), "retain"),
          );
        if (scenario === "checkout-interruption") {
          assert.equal((yield* worktree.handoff(scope, input).pipe(Effect.result))._tag, "Failure");
          assert.equal((yield* policy.readHandoff(threadId))?.phase, "checkout_issued");
        }
        if (scenario === "setup-unknown") {
          assert.equal((yield* worktree.handoff(scope, input).pipe(Effect.result))._tag, "Failure");
          const retry = yield* worktree.handoff(scope, input).pipe(Effect.flip);
          assert.include(retry.message, "handoff_unknown");
          assert.equal(setupCount, 1);
          assert.equal(created, 1);
          assert.equal(sent.size, 0);
          assert.equal((yield* policy.readHandoff(threadId))?.phase, "setup_issued");
          assert.deepEqual(
            yield* sql`SELECT claim_id FROM jones_workspace_duty_claims WHERE released_at IS NULL`,
            [],
          );
          return;
        }
        if (scenario === "terminal") {
          assert.equal(yield* worktree.attachForTerminal(threadId), checkout);
          assert.equal(created, 1);
          assert.equal(setupCount, 1);
          assert.equal(sent.size, 0);
          assert.isFalse(yield* policy.acceptsCwd(threadId, binding.path));
          assert.deepEqual(
            yield* sql`SELECT claim_id FROM jones_workspace_duty_claims WHERE released_at IS NULL`,
            [],
          );
          return;
        }
        const result = yield* Effect.gen(function* () {
          if (scenario !== "registered-callback") return yield* worktree.handoff(scope, input);
          yield* policy.attachForTurn(threadId, "run:registered", input.continuationPrompt, []);
          const journal = yield* policy.readHandoff(threadId);
          assert.equal(journal?.phase, "done");
          assert.equal(journal?.requestId, "adapter-fallback:run:registered");
          assert.equal(journal?.commit, turn!.binding.commit);
          assert.isNotNull(journal?.result);
          // Re-enter the registered boundary after attachment: the durable receipt
          // must replay without issuing checkout/setup/continuation again.
          yield* policy.attachForTurn(threadId, "run:registered", input.continuationPrompt, []);
          assert.deepEqual(yield* policy.readHandoff(threadId), journal);
          return journal!.result!;
        });
        if (scenario !== "registered-callback")
          assert.deepEqual(
            yield* worktree.handoff(
              scope,
              input,
              Effect.die("Replay must not reacquire live execution"),
            ),
            result,
          );
        assert.equal(created, 1);
        assert.equal(setupCount, 1);
        assert.equal(sent.size, 1);
        assert.equal(result.continuation.status, "scheduled");
        assert.deepEqual(
          yield* sql`SELECT claim_id FROM jones_workspace_duty_claims WHERE released_at IS NULL`,
          [],
        );
        assert.equal(result.startedFromOrigin, false);
        assert.equal(git(checkout, "rev-parse", "HEAD"), turn!.binding.commit);
        if (scenario === "nonempty") {
          assert.equal(
            yield* Effect.promise(() =>
              NodeFSP.readFile(NodePath.join(binding.path, "owner-note"), "utf8"),
            ),
            "retain",
          );
          assert.lengthOf(
            yield* sql`SELECT 1 FROM jones_conversation_obligations WHERE path=${binding.path} AND thread_id=${threadId}`,
            1,
          );
        } else
          assert.equal(
            yield* Effect.promise(() => NodeFSP.stat(binding.path).catch(() => null)),
            null,
          );
        if (scenario === "shared") {
          assert.equal(members.get(childId)!.worktreePath, checkout);
          const lateId = ThreadId.make("thread:shared:late:c9");
          members.set(lateId, { ...thread, id: lateId, worktreePath: null });
          yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${lateId},${JSON.stringify({ id: lateId, projectId })},NULL)`;
          yield* policy.inherit(lateId, threadId);
          assert.equal(yield* worktree.attachForTerminal(lateId), checkout);
          assert.equal(members.get(lateId)!.worktreePath, checkout);
          yield* policy.attachForTurn(lateId, "run:late", "Continue shared request", []);
          assert.equal(created, 1);
          assert.equal(setupCount, 1);
          assert.equal(sent.size, 2);
        }
        const continued = yield* probe.run(result.worktreePath, providerThread);
        assert.equal(continued.nativeThreadRef?.nativeId, providerThread.nativeThreadRef?.nativeId);
        assert.isTrue(
          probe.observed.some(
            (call) => call.cwd === result.worktreePath && call.resume && call.mcp,
          ),
        );
        assert.isFalse(yield* policy.acceptsCwd(threadId, binding.path));
      }).pipe(Effect.provide(services));
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
