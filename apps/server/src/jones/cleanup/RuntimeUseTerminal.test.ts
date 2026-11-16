// @effect-diagnostics nodeBuiltinImport:off -- Task-owned physical owner/SQLite fixtures only; no installed terminal or provider.
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EventId, ThreadId, ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Ownership from "../../serverOwnership.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as Manager from "../../terminal/Manager.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import { HeldRuntimeOwner, invalidateCheckedServerOwnership } from "./CheckedRuntimeOwner.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";

const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const threadId = ThreadId.make("runtime-use-terminal-thread");
const projectId = ProjectId.make("runtime-use-terminal-project");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic-only" };
const fixture = <A, E>(
  test: (input: {
    readonly manager: Manager.TerminalManager["Service"];
    readonly calls: () => number;
    readonly awaitExit: Effect.Effect<void>;
    readonly reloadedOpen: ReturnType<Manager.TerminalManager["Service"]["open"]>;
    readonly setBeforeWrap: (effect: Effect.Effect<void>) => void;
    readonly open: ReturnType<Manager.TerminalManager["Service"]["open"]>;
    readonly workspace: string;
    readonly loseOwner: Effect.Effect<void>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
  spawnSucceeds = false,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "runtime-use-terminal-" }),
      );
      const stateDir = NodePath.join(root, "userdata"),
        workspace = NodePath.join(root, "workspace");
      yield* fs.makeDirectory(stateDir);
      yield* fs.makeDirectory(workspace);
      return yield* Effect.gen(function* () {
        const owner = yield* Ownership.acquireServerOwnership(
          NodePath.join(stateDir, "server-runtime.json"),
        );
        const persistence = Layer.effectDiscard(runMigrations()).pipe(
          Layer.provideMerge(
            NodeSqliteClient.layer({ filename: NodePath.join(stateDir, "statev2.sqlite") }),
          ),
        );
        const stores = Layer.mergeAll(
          EventStore.layer,
          ProjectStore.layer,
          ProjectionStore.layer,
        ).pipe(Layer.provideMerge(persistence));
        const sink = EventSink.layer.pipe(Layer.provideMerge(stores));
        return yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const eventSink = yield* EventSink.EventSinkV2;
          const now = yield* DateTime.now;
          const iso = DateTime.formatIso(now);
          yield* sql.withTransaction(
            projects.apply({
              sequence: 1,
              eventId: EventId.make("runtime-use-project-event"),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: iso,
              commandId: null,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: "Synthetic runtime use",
                workspaceRoot: workspace,
                defaultModelSelection: null,
                scripts: [],
                createdAt: iso,
                updatedAt: iso,
              },
            }),
          );
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("runtime-use-thread-event"),
                type: "thread.created",
                threadId,
                occurredAt: now,
                payload: {
                  id: threadId,
                  projectId,
                  title: "Synthetic runtime use",
                  createdBy: "user",
                  creationSource: "web",
                  providerInstanceId: instanceId,
                  modelSelection,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  worktreePath: workspace,
                  branch: null,
                  activeProviderThreadId: null,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: threadId,
                  },
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                  archivedAt: null,
                  settledOverride: null,
                  settledAt: null,
                  lastVisitedAt: null,
                  deletedAt: null,
                },
              },
            ],
          });

          let calls = 0;
          let beforeWrap: Effect.Effect<void> = Effect.void;
          const createManager = Manager.makeWithOptions({
            logsDir: NodePath.join(root, "terminal-logs"),
            shellResolver: () => "/synthetic-shell",
            env: {},
            processTable: Effect.succeed([]),
            ptyAdapter: {
              spawn: () =>
                Effect.gen(function* () {
                  const pending = yield* (yield* makeRuntimeUseStore).pending();
                  assert.isTrue(pending.length > 0);
                  assert.isTrue(
                    pending.every(
                      (use) =>
                        use.threadId === threadId &&
                        use.actor.kind === "terminal_spawn" &&
                        use.canonicalPath === workspace,
                    ),
                  );
                  calls++;
                  if (spawnSucceeds)
                    return {
                      pid: 123456789,
                      write: () => {},
                      resize: () => {},
                      kill: () => {},
                      onData: () => () => {},
                      onExit: (callback: (event: PtyAdapter.PtyExitEvent) => void) => {
                        callback({ exitCode: 0, signal: 0 });
                        return () => {};
                      },
                    } satisfies PtyAdapter.PtyProcess;
                  return yield* new PtyAdapter.PtySpawnError({
                    adapter: "synthetic-boundary",
                    cause: "No native terminal was spawned",
                  });
                }).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                  Effect.mapError(
                    (cause) =>
                      new PtyAdapter.PtySpawnError({ adapter: "synthetic-boundary", cause }),
                  ),
                ),
            },
          }).pipe(
            Effect.provideService(AgentScope, {
              wrap: ({ command, args }) => beforeWrap.pipe(Effect.as({ command, args })),
              oomKilled: () => Effect.succeed(false),
              stop: () => Effect.void,
              kill: () => Effect.succeed(false),
              clear: () => Effect.void,
            }),
          );
          const recreateContext = yield* Effect.context<Effect.Services<typeof createManager>>();
          const manager = yield* createManager;
          const exits = yield* Queue.unbounded<void>();
          const unsubscribe = yield* manager.subscribe((event) =>
            event.type === "exited"
              ? Queue.offer(exits, undefined).pipe(Effect.asVoid)
              : Effect.void,
          );
          yield* Effect.addFinalizer(() => Effect.sync(unsubscribe));
          return yield* test({
            manager,
            reloadedOpen: createManager.pipe(
              Effect.provide(recreateContext),
              Effect.flatMap((next) =>
                next.open({
                  threadId,
                  terminalId: "terminal",
                  cwd: workspace,
                  worktreePath: workspace,
                  cols: 80,
                  rows: 24,
                }),
              ),
            ),
            awaitExit: Queue.take(exits),
            calls: () => calls,
            setBeforeWrap: (effect) => {
              beforeWrap = effect;
            },
            workspace,
            open: manager.open({
              threadId,
              terminalId: "terminal",
              cwd: workspace,
              worktreePath: workspace,
              cols: 80,
              rows: 24,
            }),
            loseOwner: Effect.sync(() => invalidateCheckedServerOwnership(owner.heldRuntimeOwner)),
          });
        }).pipe(
          Effect.provide(sink),
          Effect.provideService(HeldRuntimeOwner, { token: owner.heldRuntimeOwner }),
        );
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, root),
        Effect.provideService(HostProcess.Environment, {}),
      );
    }).pipe(Effect.provide(platform)),
  );

it.effect(
  "actual terminal start commits pending before the PTY boundary and a repeated failed open cannot replay",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.open;
        assert.strictEqual(input.calls(), 1);
        const store = yield* makeRuntimeUseStore;
        const first = yield* store.pending();
        assert.strictEqual(first.length, 1);
        yield* input.open;
        assert.strictEqual(input.calls(), 1);
        assert.deepStrictEqual(yield* store.pending(), first);
        yield* input.manager.close({ threadId, terminalId: "terminal" });
        assert.deepStrictEqual(
          yield* store.pending(),
          first,
          "generic terminal close is not physical writer retirement",
        );
      }),
    ),
);
it.effect(
  "explicit restart has independent durable identity and preserves unknown old starts",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.open;
        yield* input.manager.restart({
          threadId,
          terminalId: "terminal",
          cwd: input.workspace,
          worktreePath: input.workspace,
          cols: 80,
          rows: 24,
        });
        assert.strictEqual(input.calls(), 2);
        const uses = yield* (yield* makeRuntimeUseStore).pending();
        assert.strictEqual(uses.length, 2);
        assert.notStrictEqual(uses[0]?.useId, uses[1]?.useId);
      }),
    ),
);
it.effect("owner loss after preparation at the actual wrapper boundary refuses PTY entry", () =>
  fixture((input) =>
    Effect.gen(function* () {
      input.setBeforeWrap(input.loseOwner);
      yield* input.open;
      assert.strictEqual(input.calls(), 0);
      assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 1);
    }),
  ),
);
it.effect(
  "a direct deletion reservation prevents terminal entry before any pending-use write",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO jones_deletion_worktree_admissions(effect_id,worktree_path,binding_json,start_json,state)
 VALUES('synthetic-deletion',${input.workspace},'{}','{}','started')`;
        yield* input.open;
        assert.strictEqual(input.calls(), 0);
        assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
      }),
    ),
);
it.effect("terminal pending use reciprocally rejects a direct conflicting deletion writer", () =>
  fixture((input) =>
    Effect.gen(function* () {
      yield* input.open;
      const sql = yield* SqlClient.SqlClient;
      const result =
        yield* Effect.exit(sql`INSERT INTO jones_deletion_worktree_admissions(effect_id,worktree_path,binding_json,start_json,state)
 VALUES('synthetic-deletion',${input.workspace},'{}','{}','started')`);
      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 1);
    }),
  ),
);

it.effect(
  "an actually observed PTY exit permits a fresh explicit open but retires neither writer journal",
  () =>
    fixture(
      (input) =>
        Effect.gen(function* () {
          yield* input.open;
          yield* input.awaitExit;
          assert.strictEqual(input.calls(), 1);
          const store = yield* makeRuntimeUseStore;
          const first = yield* store.pending();
          assert.strictEqual(first.length, 1);
          yield* input.open;
          yield* input.awaitExit;
          assert.strictEqual(input.calls(), 2);
          const second = yield* store.pending();
          assert.strictEqual(second.length, 2);
          assert.isTrue(second.some((use) => use.useId === first[0]?.useId));
        }),
      true,
    ),
);
it.effect(
  "an explicit changed environment starts independently while retaining the old unknown start",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.open;
        yield* input.manager.open({
          threadId,
          terminalId: "terminal",
          cwd: input.workspace,
          worktreePath: input.workspace,
          cols: 80,
          rows: 24,
          env: { JONES_SYNTHETIC_CHANGED: "1" },
        });
        assert.strictEqual(input.calls(), 2);
        assert.strictEqual((yield* (yield* makeRuntimeUseStore).pending()).length, 2);
      }),
    ),
);

it.effect(
  "a reconstructed manager cannot allocate a new identity to replay an unknown initial raw start",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.open;
        assert.strictEqual(input.calls(), 1);
        const store = yield* makeRuntimeUseStore;
        const original = yield* store.pending();
        yield* input.reloadedOpen;
        assert.strictEqual(input.calls(), 1);
        assert.deepStrictEqual(yield* store.pending(), original);
      }),
    ),
);
