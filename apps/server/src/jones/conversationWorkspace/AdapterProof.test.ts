// @effect-diagnostics nodeBuiltinImport:off -- Synthetic Git repositories and exact fixture teardown; no provider executable or installed state.
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { homedir } from "node:os";
import { execFileSync } from "node:child_process";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import { vi } from "vitest";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import { makeAdapterRuntimeProbe } from "./AdapterRuntime.testkit.ts";
vi.mock("@anthropic-ai/claude-agent-sdk", async (original) => ({ ...(await original<typeof import("@anthropic-ai/claude-agent-sdk")>()), resolveSettings: vi.fn(async () => ({ effective: {}, provenance: {}, sources: [] })) }));
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
import Migration from "../persistence/Migrations/138_JonesConversationWorkspace.ts";
import * as Policy from "./ConversationPolicy.ts";

const git = (cwd: string, ...args: string[]) => execFileSync("git", ["-c", "init.defaultBranch=main", "-c", "user.name=C9 Fixture", "-c", "user.email=c9@example.invalid", ...args], { cwd, encoding: "utf8", timeout: 3000, maxBuffer: 1024 * 1024, env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" } }).trim();

it.effect.each(["codex", "claudeAgent"] as const)("%s adapter seam preserves conversation cwd, source tools and durable attach continuation", (driver) => Effect.scoped(Effect.gen(function* () {
  const root = yield* Effect.acquireRelease(Effect.promise(async () => {
    const cache = path.join(homedir(), "Library", "Caches"); await fs.mkdir(cache, { recursive: true });
    return fs.mkdtemp(path.join(cache, "jones-c9-adapter-"));
  }), (root) => Effect.promise(() => fs.rm(root, { recursive: true, force: true })));
  const repo = path.join(root, "repo"); const state = path.join(root, "state"); const checkout = path.join(root, "checkout");
  yield* Effect.promise(async () => { await fs.mkdir(repo); await fs.mkdir(state); git(repo, "init", "--template="); await fs.writeFile(path.join(repo, "AGENTS.md"), "Read the recorded source.\n"); git(repo, "add", "."); git(repo, "commit", "-qm", "source"); });
  const threadId = ThreadId.make(`thread:adapter:${driver}`); const projectId = ProjectId.make("project:c9");
  const thread = { id: threadId, projectId, worktreePath: null as string | null, branch: null as string | null, deletedAt: null, archivedAt: null };
  let created = 0; let setupCount = 0; const sent = new Map<string, string>();
  const external = Layer.mergeAll(
    Layer.mock(Threads.ThreadManagementService)({
      getThreadRecords: () => Effect.succeed({ thread } as never),
      dispatch: (command) => Effect.sync(() => { if (command.type === "thread.metadata.update") { thread.worktreePath = command.worktreePath ?? thread.worktreePath; thread.branch = command.branch ?? thread.branch; } return { sequence: 1, storedEvents: [] } as never; }),
      sendToThread: (input) => Effect.sync(() => { sent.set(input.commandId, input.text); return { delivery: "queued" as const } as never; }),
    }),
    Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeed(Option.some({ id: projectId, workspaceRoot: repo, scripts: [] } as never)) }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree: (input) => Effect.sync(() => { created++; git(repo, "worktree", "add", "-b", input.newRefName!, checkout, input.refName); return { worktree: { path: checkout, refName: input.newRefName, headSha: git(checkout, "rev-parse", "HEAD") } } as never; }),
      listRefs: () => Effect.succeed({ refs: [{ name: "work/c9-proof", worktreePath: checkout, isRemote: false }], isRepo: true } as never),
    }),
    Layer.mock(Setup.ProjectSetupScriptRunner)({ runForThread: (input) => Effect.sync(() => { assert.equal(input.worktreePath, checkout); assert.equal(input.projectCwd, repo); setupCount++; return { status: "no-script" as const }; }) }),
    Layer.mock(VcsStatus.VcsStatusBroadcaster)({ refreshStatus: () => Effect.void as never }),
    ServerSettings.layerTest({ newWorktreesStartFromOrigin: true }),
  );
  const database = NodeSqlite.layer({ filename: ":memory:" });
  const config = ServerConfig.layerTest(root, state).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
  const policyLayer = Policy.layer.pipe(Layer.provideMerge(database), Layer.provide(config));
  const worktreeLayer = Worktree.layer.pipe(Layer.provide(external), Layer.provide(policyLayer));
  const services = Layer.mergeAll(external, policyLayer, worktreeLayer, config, IdAllocator.layer, McpSessions.layer, ControlAdmission.layer);
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient; yield* Migration;
    yield* sql`CREATE TABLE projection_projects(workspace_root TEXT, deleted_at TEXT)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT, deleted_at TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${threadId},${JSON.stringify({ id: threadId, projectId })},NULL)`;
    const policy = yield* Policy.ConversationPolicy; const worktree = yield* Worktree.WorktreeMcpService;
    const binding = yield* policy.allocate({ threadId, projectId, projectRoot: repo, baseRef: "main" });
    const turn = yield* policy.startTurn(threadId, "run:proof"); assert.isNotNull(turn);
    assert.equal(created, 0); assert.equal(setupCount, 0);
    const scope: Invocation.McpThreadInvocationScope = { environmentId: EnvironmentId.make("environment:c9"), requestNamespace: "c9", capabilities: new Set(["worktree"]), thread: { threadId, providerInstanceId: ProviderInstanceId.make(driver), providerSessionId: `session:${driver}` } };
    const mcpSession = { environmentId: scope.environmentId, threadId, providerInstanceId: scope.thread.providerInstanceId, providerSessionId: scope.thread.providerSessionId, endpoint: "http://127.0.0.1:1/mcp", authorizationHeader: "Bearer synthetic", browserToolsAvailable: false };
    const probe = yield* makeAdapterRuntimeProbe(driver, threadId, mcpSession);
    const providerThread = yield* probe.run(binding.path);
    assert.isTrue(probe.observed.some((call) => call.cwd === binding.path && call.mcp));
    const toolkit = yield* WorktreeToolkit.pipe(Effect.provide(ToolAccess.HandlersLayer.layer(Handlers.layer).pipe(Layer.provide(services))));
    const source = yield* toolkit.handle("t3_source_read", { path: "AGENTS.md" }).pipe(Stream.unwrap, Stream.runCollect, Effect.provideService(Invocation.McpInvocationContext, scope));
    assert.include(JSON.stringify(source), "Read the recorded source."); assert.include(JSON.stringify(source), turn!.binding.commit);
    const input = { clientRequestId: "proof:attach", branch: "work/c9-proof", continuationPrompt: "Continue the original conversation in the checkout." };
    const result = yield* worktree.handoff(scope, input);
    assert.deepEqual(yield* worktree.handoff(scope, input), result);
    assert.equal(created, 1); assert.equal(setupCount, 1); assert.equal(sent.size, 1);
    assert.equal(result.continuation.status, "scheduled"); assert.equal(result.startedFromOrigin, false);
    assert.equal(git(checkout, "rev-parse", "HEAD"), turn!.binding.commit);
    assert.equal(yield* Effect.promise(() => fs.stat(binding.path).catch(() => null)), null);
    const continued = yield* probe.run(result.worktreePath, providerThread);
    assert.equal(continued.nativeThreadRef?.nativeId, providerThread.nativeThreadRef?.nativeId);
    assert.isTrue(probe.observed.some((call) => call.cwd === result.worktreePath && call.resume && call.mcp));
    assert.isFalse(yield* policy.acceptsCwd(threadId, binding.path));
  }).pipe(Effect.provide(services));
})).pipe(Effect.provide(NodeServices.layer)));
