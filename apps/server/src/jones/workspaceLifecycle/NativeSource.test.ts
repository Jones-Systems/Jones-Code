import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Sources from "./CheckoutSource.ts";
import * as Native from "./NativeSource.ts";
import * as Lifecycle from "./WorkspaceLifecycle.ts";
import * as Registry from "./RegistryCli.ts";
import { LifecycleError, type InventoryEntry } from "./Protocol.ts";
import * as Store from "./TransitionStore.ts";
import migration from "../persistence/Migrations/123_JonesWorkspaceLifecycle.ts";

const native = {
  transitionId: "effect:original",
  threadId: "thread",
  requestDigest: "original-command-and-request",
  boundary: "start" as const,
};
const createdAt = "2026-10-24T00:00:00.000Z";
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE projection_projects(project_id TEXT PRIMARY KEY, workspace_root TEXT, updated_at TEXT, deleted_at TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_events(event_id TEXT, sequence INTEGER, payload_json TEXT, application_event_version INTEGER, aggregate_kind TEXT, stream_id TEXT, event_type TEXT)`;
  const thread = {
    id: "thread",
    projectId: "project",
    createdAt,
    updatedAt: createdAt,
    deletedAt: null,
    worktreePath: "/linked",
    branch: "work",
  };
  yield* sql`INSERT INTO projection_projects VALUES('project','/repo',${createdAt},NULL)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES('thread',${canonical(thread)})`;
  yield* sql`INSERT INTO orchestration_events VALUES('birth',1,${canonical(thread)},2,'thread','thread','thread.created')`;
  yield* migration;
});

const fixture = (qualifiedOwner = true) => {
  const state = { exists: true, inode: "1", head: "a".repeat(40), writes: 0 };
  let view: InventoryEntry = {
    worktree: "/linked",
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
  const common = { version: 1 as const, device: "1", inode: "common", birth: "1" };
  const observations = Sources.CheckoutSourceObservation.of({
    capture: (input) =>
      Effect.succeed({
        target: {
          ...input,
          version: 1,
          canonicalProjectRoot: "/repo",
          cwd: "/linked",
          checkoutKey: "/linked",
          directoryIdentity: { version: 1, device: "1", inode: state.inode, birth: state.inode },
          gitCommonDirectory: "/repo/.git",
          gitDirectory: "/repo/.git/worktrees/linked",
          gitCommonIdentity: common,
          gitDirectoryIdentity: {
            version: 1,
            device: "1",
            inode: `git-${state.inode}`,
            birth: state.inode,
          },
          branch: "refs/heads/work",
        },
        head: state.head,
      }),
    revalidate: () => Effect.void,
  });
  const source = Native.layer.pipe(
    Layer.provide(
      qualifiedOwner
        ? Layer.succeed(Native.NativeCheckoutOwner, {
            verify: () =>
              Effect.succeed({
                id: "synthetic-existing-native-owner",
                generation: "owner-generation",
              }),
            validate: () => Effect.void,
          })
        : Layer.empty,
    ),
    Layer.provideMerge(Store.layer),
    Layer.provide(Layer.succeed(Sources.CheckoutSourceObservation, observations)),
    Layer.provide(
      Layer.succeed(Native.LifecycleConfiguration, {
        environmentId: "environment",
        projects: new Map([
          ["project", { repository: "Jones-Systems/Synthetic", projectRoot: "/repo" }],
        ]),
      }),
    ),
    Layer.provide(FileSystem.layerNoop({ exists: () => Effect.succeed(state.exists) })),
    Layer.provide(
      Layer.mock(Git.GitVcsDriver)({
        execute: () =>
          Effect.succeed({
            exitCode: 0 as never,
            stdout: `${state.head}\n`,
            stderr: "",
            stdoutTruncated: false,
            stderrTruncated: false,
          }),
      }),
    ),
    Layer.provide(
      Layer.mock(Targets.CheckoutTargetObservation)({
        git: () =>
          Effect.succeed({
            root: "/repo",
            commonDirectory: "/repo/.git",
            gitDirectory: "/repo/.git",
            commonIdentity: common,
            directoryIdentity: common,
            branch: "refs/heads/main",
            head: state.head,
            registeredWorktrees: ["/repo", "/linked"],
          }),
      }),
    ),
    Layer.provide(Path.layer),
  );
  const registry = Layer.succeed(Registry.LifecycleRegistry, {
    show: () => Effect.succeed(view),
    resume: (binding, expected) =>
      Effect.gen(function* () {
        if (view.generation !== expected) return yield* new LifecycleError({ reason: "conflict" });
        const generation = `cas:${++state.writes}`;
        view = { ...view, generation, binding };
        return generation;
      }),
  });

  return {
    state,
    source,
    layer: Lifecycle.layer.pipe(Layer.provideMerge(source), Layer.provide(registry)),
  };
};

it.effect(
  "authenticates birth and assigned checkout, invalidates before recreation, then observes a different physical incarnation",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const f = fixture();
      yield* Effect.gen(function* () {
        const source = yield* Lifecycle.NativeLifecycleSource;
        const lifecycle = yield* Lifecycle.WorkspaceLifecycle;
        const before = yield* source.capture(native);
        const initial = yield* lifecycle.resume(native);
        f.state.exists = false;
        const recreated = yield* lifecycle.resume({
          ...native,
          transitionId: "effect:recreation",
          boundary: "recreate",
        });
        expect(recreated.binding.incarnation_id).toBe(initial.binding.incarnation_id);
        expect(recreated.binding.source_generation).not.toBe(initial.binding.source_generation);
        f.state.exists = true;
        f.state.inode = "2";
        const after = yield* lifecycle.resume({
          ...native,
          transitionId: "effect:after-recreation",
        });
        expect(after.binding.incarnation_id).not.toBe(before.binding.incarnation_id);
        expect(Exit.isFailure(yield* Effect.exit(source.revalidate(native, before)))).toBe(true);
        expect(f.state.writes).toBe(3);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "SQL transaction validation rejects changed assignment or replacement birth without invoking physical observations",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const f = fixture();
      const sql = yield* SqlClient.SqlClient;
      yield* Effect.gen(function* () {
        const source = yield* Lifecycle.NativeLifecycleSource;
        const before = yield* source.capture(native);
        yield* sql.withTransaction(source.validateOwnership(native, before));
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath','/foreign')`;
        expect(
          Exit.isFailure(
            yield* Effect.exit(sql.withTransaction(source.validateOwnership(native, before))),
          ),
        ).toBe(true);
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath','/linked')`;
        const rows = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id='thread'`;
        yield* sql`INSERT INTO orchestration_events VALUES('replacement-birth',2,${rows[0]!.payload_json},2,'thread','thread','thread.created')`;
        expect(
          Exit.isFailure(
            yield* Effect.exit(sql.withTransaction(source.validateOwnership(native, before))),
          ),
        ).toBe(true);
        expect((yield* source.capture(native)).binding.incarnation_id).not.toBe(
          before.binding.incarnation_id,
        );
      }).pipe(Effect.provide(f.source));
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "a valid native assignment and physical snapshot cannot replace the missing native owner issuer",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const f = fixture(false);
      const result = yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (lifecycle) =>
        lifecycle.resume(native),
      ).pipe(Effect.provide(f.layer), Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      expect(f.state.writes).toBe(0);
      const sql = yield* SqlClient.SqlClient;
      expect(yield* sql`SELECT * FROM jones_workspace_lifecycle_transitions`).toEqual([]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
