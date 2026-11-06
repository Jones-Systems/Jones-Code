// @effect-diagnostics nodeBuiltinImport:off -- Small synthetic repositories and exact cleanup belong to this fixture.
import * as NodeFs from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOs from "node:os";
import * as NodeChildProcess from "node:child_process";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../config.ts";
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

it.effect(
  "records launch, immutable turn commits, shared delegation, replay/unknown phases and exact directory obligations",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const root = yield* Effect.acquireRelease(
          Effect.promise(async () => {
            const parent = NodePath.join(NodeOs.homedir(), "Library", "Caches");
            await NodeFs.mkdir(parent, { recursive: true });
            return NodeFs.mkdtemp(NodePath.join(parent, "jones-c9-policy-"));
          }),
          (root) => Effect.promise(() => NodeFs.rm(root, { recursive: true, force: true })),
        );
        const repo = NodePath.join(root, "repo");
        const state = NodePath.join(root, "state");
        yield* Effect.promise(async () => {
          await NodeFs.mkdir(repo);
          await NodeFs.mkdir(state);
          git(repo, "init", "--template=");
          await NodeFs.writeFile(NodePath.join(repo, "AGENTS.md"), "initial instructions\n");
          git(repo, "add", ".");
          git(repo, "commit", "-qm", "first");
        });
        const database = NodeSqlite.layer({ filename: ":memory:" });
        const config = ServerConfig.layerTest(root, state).pipe(
          Layer.provide(NodeServices.layer),
          Layer.orDie,
        );
        const policyLayer = Policy.layer.pipe(Layer.provideMerge(database), Layer.provide(config));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* Migration;
          // Only the narrow upstream read columns used by allocation; no upstream mutation by production code.
          yield* sql`CREATE TABLE projection_projects(workspace_root TEXT, deleted_at TEXT)`;
          yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT, deleted_at TEXT)`;
          const service = yield* Policy.ConversationPolicy;
          const input = {
            threadId: "conversation",
            projectId: "project",
            projectRoot: repo,
            baseRef: "main",
          };
          yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES('conversation','{"id":"conversation","projectId":"project"}',NULL),('delegate','{"id":"delegate","projectId":"project"}',NULL)`;
          const binding = yield* service.allocate(input);
          assert.deepEqual(yield* service.allocate(input), binding);
          assert.deepEqual(yield* Effect.promise(() => NodeFs.readdir(binding.path)), []);
          const first = yield* service.startTurn(input.threadId, "turn:first");
          assert.isNotNull(first);
          assert.include(first!.context, "AGENTS.md");
          yield* Effect.promise(async () => {
            await NodeFs.writeFile(NodePath.join(repo, "AGENTS.md"), "second instructions\n");
            git(repo, "add", ".");
            git(repo, "commit", "-qm", "second");
          });
          const replay = yield* service.startTurn(input.threadId, "turn:first");
          assert.equal(replay!.binding.commit, first!.binding.commit);
          const next = yield* service.startTurn(input.threadId, "turn:second");
          assert.notEqual(next!.binding.commit, first!.binding.commit);
          assert.equal(next!.binding.path, binding.path);
          assert.equal(
            (yield* service.source(input.threadId, { operation: "read", path: "AGENTS.md" }))
              .content,
            "second instructions\n",
          );
          assert.isTrue(yield* service.acceptsCwd(input.threadId, binding.path));
          assert.isFalse(yield* service.acceptsCwd("unrelated", binding.path));
          assert.isFalse(yield* service.acceptsCwd(input.threadId, repo));
          assert.equal(yield* service.dispose(input.threadId, false), "disposed");
          const resumed = yield* service.startTurn(input.threadId, "turn:resumed");
          assert.equal(resumed!.binding.path, binding.path);
          assert.equal(resumed!.binding.commit, next!.binding.commit);
          assert.isTrue(yield* service.acceptsCwd(input.threadId, binding.path));
          yield* service.inherit("delegate", input.threadId);
          assert.equal((yield* service.lookup("delegate"))!.path, binding.path);
          assert.equal(yield* service.dispose("delegate", false), "shared");
          const handoffInput = {
            threadId: input.threadId,
            requestId: "attach:1",
            request: "original",
            commit: next!.binding.commit,
            baseRef: "main",
            startedFromOrigin: false,
          };
          const record = yield* service.reserveHandoff(handoffInput);
          assert.deepEqual(yield* service.reserveHandoff(handoffInput), record);
          assert.equal(
            (yield* service
              .reserveHandoff({ ...handoffInput, request: "different" })
              .pipe(Effect.flip)).reason,
            "handoff_conflict",
          );
          const issued = yield* service.updateHandoff(record, { phase: "checkout_issued" });
          assert.deepEqual(yield* service.readHandoff(input.threadId), issued);
          assert.equal(
            (yield* service.updateHandoff(record, { phase: "checkout_issued" }).pipe(Effect.flip))
              .reason,
            "handoff_conflict",
          );
          yield* Effect.promise(() =>
            NodeFs.writeFile(NodePath.join(binding.path, "owner-note"), "retained"),
          );
          assert.isFalse(yield* service.acceptsCwd(input.threadId, binding.path));
          assert.equal(yield* service.dispose(input.threadId, true), "conversation_dir_nonempty");
          assert.isFalse(yield* service.acceptsCwd(input.threadId, binding.path));
          const obligations = yield* sql<{
            path: string;
            thread_id: string;
          }>`SELECT path,thread_id FROM jones_conversation_obligations`;
          assert.deepEqual(obligations, [{ path: binding.path, thread_id: input.threadId }]);
          assert.equal(
            yield* Effect.promise(() =>
              NodeFs.readFile(NodePath.join(binding.path, "owner-note"), "utf8"),
            ),
            "retained",
          );
          yield* service.fallback("other-adapter", "cursor", "adapter proof unavailable; eager");
          assert.lengthOf(
            yield* sql`SELECT 1 FROM jones_conversation_fallbacks WHERE thread_id='other-adapter'`,
            1,
          );
        }).pipe(Effect.provide(policyLayer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
