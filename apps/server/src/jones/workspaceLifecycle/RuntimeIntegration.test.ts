import {
  CommandId,
  ThreadId,
  EnvironmentId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../config.ts";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Runner from "../../processRunner.ts";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { jonesMigrationEntries } from "../persistence/JonesMigrations.ts";
import * as Boundary from "./NativeBoundary.ts";
import * as Lifecycle from "./WorkspaceLifecycle.ts";
import { type Binding, type InventoryEntry } from "./Protocol.ts";
import * as Runtime from "./RuntimeIntegration.ts";
import type { Configuration } from "./Configuration.ts";

const beforeStart = Boundary.beforeUse({
  threadId: "thread",
  boundary: "start",
  fallback: { id: "original-effect", request: { threadId: "thread" } },
});
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-composition-" });
  const repo = path.join(root, "repo");
  const linked = path.join(root, "linked");
  const templates = path.join(root, "templates");
  yield* fs.makeDirectory(repo);
  yield* fs.makeDirectory(templates);
  const config = yield* ServerConfig.ServerConfig.pipe(
    Effect.provide(ServerConfig.layerTest(root, path.join(root, "state"))),
  );
  const gitLayer = Git.layer.pipe(Layer.provide(ServerConfig.layer(config)));
  const git = yield* Git.GitVcsDriver.pipe(Effect.provide(gitLayer));
  const run = (args: readonly string[]) =>
    git.execute({
      operation: "WorkspaceLifecycle.compositionTest",
      cwd: repo,
      args: [
        "-c",
        "user.name=Synthetic",
        "-c",
        "user.email=synthetic@example.invalid",
        "-c",
        `core.hooksPath=${templates}`,
        ...args,
      ],
      env: { GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
    });
  yield* run(["init", "--template", templates, "-b", "main"]);
  yield* run(["commit", "--allow-empty", "-m", "synthetic"]);
  yield* run(["worktree", "add", "-b", "work", linked]);
  yield* sql`CREATE TABLE projection_projects(project_id TEXT PRIMARY KEY, workspace_root TEXT, updated_at TEXT, deleted_at TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_events(event_id TEXT, sequence INTEGER, payload_json TEXT, application_event_version INTEGER, aggregate_kind TEXT, stream_id TEXT, event_type TEXT)`;
  const thread = {
    id: "thread",
    projectId: "project",
    createdAt: "2026-10-24T00:00:00.000Z",
    updatedAt: "revision",
    deletedAt: null,
    worktreePath: linked,
    branch: "work",
  };
  yield* sql`INSERT INTO projection_projects VALUES('project',${repo},'revision',NULL)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES('thread',${canonical(thread)})`;
  yield* sql`INSERT INTO orchestration_events VALUES('birth',1,${canonical(thread)},2,'thread','thread','thread.created')`;
  yield* jonesMigrationEntries.find(([id]) => id === 123)![2];
  const settings: Configuration = {
    environmentId: "environment",
    projects: [{ projectId: "project", repository: "Jones-Systems/Synthetic", projectRoot: repo }],
    registry: {
      executable: "/qualified/v3",
      arguments: [],
      database: path.join(root, "existing.sqlite"),
      cwd: root,
    },
  };
  const calls: Runner.ProcessRunInput[] = [];
  let view: InventoryEntry = {
    worktree: linked,
    generation: null,
    binding: null,
    assessment: null,
    disposition: "unknown",
    assessment_status: "absent",
    stored_readiness: null,
    physical_readiness: "unknown",
    binding_evidence: "caller_asserted",
    authority_effect: "none",
  };
  const runner = Runner.ProcessRunner.of({
    run: (input) =>
      Effect.sync(() => {
        calls.push(input);
        const resume = input.args.includes("resume");
        if (resume)
          view = {
            ...view,
            binding: JSON.parse(input.stdin!) as Binding,
            generation: `cas:${calls.length}`,
            assessment_status: "stale",
          };
        return {
          code: 0 as Runner.ProcessRunOutput["code"],
          stdout: canonical(
            resume
              ? {
                  schema: "workspace-lifecycle-cli/v1",
                  command: "resume",
                  result: {
                    schema: "workspace-write/v1",
                    outcome: "resumed",
                    generation: view.generation,
                    authority_effect: "none",
                  },
                }
              : {
                  schema: "workspace-lifecycle-cli/v1",
                  command: "show",
                  result: {
                    schema: "workspace-inventory/v1",
                    outcome: "unknown",
                    results: [view],
                    next_after: null,
                    evidence: "stored_only",
                    authority_effect: "none",
                  },
                },
          ),
          stderr: "",
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const configured = (workspaceLifecycle: Configuration | null = settings) =>
    Runtime.layer.pipe(
      Layer.provide(
        ServerConfig.layer({ ...config, workspaceLifecycle: workspaceLifecycle ?? undefined }),
      ),
      Layer.provide(
        Layer.succeed(ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
        }),
      ),
      Layer.provide(Layer.succeed(Git.GitVcsDriver, git)),
      Layer.provide(Layer.succeed(Runner.ProcessRunner, runner)),
    );
  return { sql, settings, configured, calls, linked, thread, readView: () => view };
});

it.effect(
  "composes the production command issuer, source and CLI with SQL-only commit validation and restart identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const composed = () => f.configured();
        const command = {
          type: "thread.unsettle" as const,
          commandId: CommandId.make("original-command"),
          threadId: ThreadId.make("thread"),
          reason: "user" as const,
        };
        const events = [
          { type: "thread.unsettled", threadId: command.threadId },
        ] as unknown as readonly OrchestrationV2DomainEvent[];
        const beforeCommit = Boundary.commitCommand(
          command,
          events,
          f.sql.withTransaction(Boundary.validateCommand(command, events)),
        );
        yield* beforeCommit.pipe(Effect.provide(composed()));
        const first = f.readView().binding!;
        expect(first.git_common_dir).not.toBe(first.git_dir);
        expect(first.source_generation).not.toBe(f.readView().generation);
        yield* beforeCommit.pipe(Effect.provide(composed()));
        expect(f.calls.filter((call) => call.args.includes("resume"))).toHaveLength(1);
        expect(f.readView().binding).toEqual(first);
        expect(yield* f.sql`SELECT state FROM jones_workspace_lifecycle_transitions`).toEqual([
          { state: "confirmed" },
        ]);
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              Boundary.beforeCleanup({
                threadId: "thread",
                boundary: "settle-script",
                id: "settle",
              }).pipe(Effect.provide(composed())),
            ),
          ),
        ).toBe(true);
        expect(
          f.calls.every((call) => call.args.includes("show") || call.args.includes("resume")),
        ).toBe(true);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.projectId','unmanaged','$.worktreePath',NULL)`;
        expect(
          Exit.isFailure(yield* Effect.exit(beforeCommit.pipe(Effect.provide(composed())))),
        ).toBe(true);
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              Boundary.beforeCleanupPath(f.linked).pipe(Effect.provide(composed())),
            ),
          ),
        ).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.merge(memory, NodeServices.layer))),
);

it.effect(
  "disabled composition leaves existing use alone; managed use without a qualified issuer holds before CLI I/O",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* beforeStart.pipe(Effect.provide(f.configured(null)));
        expect(
          Exit.isFailure(yield* Effect.exit(beforeStart.pipe(Effect.provide(f.configured())))),
        ).toBe(true);
        expect(f.calls).toEqual([]);
        expect(yield* f.sql`SELECT * FROM jones_workspace_lifecycle_transitions`).toEqual([]);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.projectId','unmanaged')`;
        yield* beforeStart.pipe(Effect.provide(f.configured()));
        yield* Boundary.beforeCleanupPath(f.linked).pipe(Effect.provide(f.configured()));
        expect(f.calls).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.merge(memory, NodeServices.layer))),
);

it.effect(
  "rejects foreign environments and duplicate projects without running registry commands",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        for (const settings of [
          { ...f.settings, environmentId: "foreign" },
          { ...f.settings, projects: [...f.settings.projects, ...f.settings.projects] },
        ])
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                Effect.serviceOption(Lifecycle.WorkspaceLifecycle).pipe(
                  Effect.provide(f.configured(settings)),
                ),
              ),
            ),
          ).toBe(true);
        expect(f.calls).toEqual([]);
        expect(Option.isNone(yield* Effect.serviceOption(Lifecycle.WorkspaceLifecycle))).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.merge(memory, NodeServices.layer))),
);
