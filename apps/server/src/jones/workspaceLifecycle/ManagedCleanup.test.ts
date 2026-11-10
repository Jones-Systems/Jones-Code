import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { NativeObservations } from "../workspaceRetirement/NativeObservations.ts";
import { LifecycleRegistry } from "./RegistryCli.ts";
import * as RetirementGate from "../workspaceRetirement/RemovalGate.ts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EventId,
  WorktreeCleanupRules,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Stream from "effect/Stream";
import * as GitManager from "../../git/GitManager.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Settlement from "../../orchestration-v2/ThreadSettlementService.ts";
import * as ProjectScripts from "../../project/ProjectSetupScriptRunner.ts";
import * as PullRequests from "../../pullRequest/PullRequestService.ts";
import * as Settings from "../../serverSettings.ts";
import * as Storage from "../../storageCleanup.ts";
import * as Terminals from "../../terminal/Manager.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Removal from "../cleanup/DeletionWorktreeRemoval.ts";
import { fixture, projectId, threadId } from "./ManagedConsumer.testkit.ts";

const recordLineage = Effect.fnUntraced(function* (f: Effect.Success<typeof fixture>) {
  yield* f.seed;
  yield* f.dispatch({
    type: "thread.settle",
    commandId: CommandId.make("initial-settle"),
    threadId,
  });
  yield* f.dispatch({
    type: "thread.unsettle",
    commandId: CommandId.make("record-lineage"),
    reason: "user",
    threadId,
  });
  yield* f.dispatch({
    type: "thread.settle",
    commandId: CommandId.make("cleanup-settle"),
    threadId,
  });
});

// No shared-host service/process scan is performed by these isolated native-policy fixtures.
const fixtureProbe = () => ({
  observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
  status: "complete" as const,
  paths: [],
  limits: ["isolated synthetic cleanup fixture"],
});

const decodeCleanupRules = Schema.decodeUnknownEffect(WorktreeCleanupRules);

it.effect.each([
  "matching",
  "missing",
  "head-changed",
  "successor",
  "native-changed",
  "startup",
  "metadata-failure",
] as const)(
  "real settle-script owner checks preservation (%s), without resume or a live provider",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* recordLineage(f);
          if (mode !== "missing") f.state.assess();
          if (mode === "head-changed")
            yield* f.runGit(["commit", "--allow-empty", "-m", "changed-head"], f.linked);
          if (mode === "successor") {
            yield* f.runGit(["commit", "--allow-empty", "-m", "successor-head"], f.linked);
            yield* f.dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make("successor"),
              reason: "user",
              threadId,
            });
            yield* f.dispatch({
              type: "thread.settle",
              commandId: CommandId.make("successor-settle"),
              threadId,
            });
            f.state.assess();
          }
          const sql = yield* SqlClient.SqlClient;
          if (mode === "metadata-failure")
            yield* sql`CREATE TEMP TRIGGER reject_settle_outcome BEFORE UPDATE OF outcome_json ON jones_workspace_retirement_intents BEGIN SELECT RAISE(FAIL,'synthetic metadata failure'); END`;
          if (mode === "native-changed")
            f.state.beforeRegistryRead =
              sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.settledOverride',NULL) WHERE thread_id=${threadId}`.pipe(
                Effect.asVoid,
                Effect.orDie,
              );
          const engine = yield* Orchestrator.OrchestratorV2;
          const thread = (yield* engine.getThreadProjection(threadId)).thread;
          const finished = yield* Deferred.make<void>();
          const scriptStarted = yield* Deferred.make<void>();
          const scripts: string[] = [];
          const startup = mode === "startup" || mode === "metadata-failure";
          const stream = (
            startup
              ? Stream.empty
              : Stream.succeed({
                  type: "thread.settled" as const,
                  id: EventId.make("settled-observation"),
                  threadId,
                  payload: thread,
                  occurredAt: yield* DateTime.now,
                })
          ).pipe(Stream.ensuring(Deferred.succeed(finished, undefined)));
          yield* Effect.gen(function* () {
            const settlement = yield* Settlement.make;
            yield* settlement.start();
            yield* Deferred.await(startup ? scriptStarted : finished);
            yield* settlement.drain;
          }).pipe(
            Effect.provideService(Orchestrator.OrchestratorV2, {
              ...engine,
              acquireDomainEvents: Effect.succeed(stream),
              streamDomainEvents: stream,
            }),
            Effect.provide(
              Layer.mergeAll(
                Settings.layerTest(),
                Layer.mock(GitManager.GitManager)({}),
                Layer.mock(PullRequests.PullRequestService)({
                  subscribeMerges: Effect.succeed(Stream.empty),
                }),
                Layer.mock(Terminals.TerminalManager)({ closeIdle: () => Effect.void }),
                Layer.mock(ProjectScripts.ProjectSetupScriptRunner)({
                  runForThread: (input) =>
                    Effect.sync(() => {
                      scripts.push(input.worktreePath!);
                      return { status: "no-script" as const };
                    }).pipe(Effect.tap(() => Deferred.succeed(scriptStarted, undefined))),
                }),
              ),
            ),
          );
          expect(scripts).toEqual(
            ["matching", "successor", "startup", "metadata-failure"].includes(mode)
              ? [f.linked]
              : [],
          );
          if (startup) {
            const rows = yield* sql<{
              script_state: string;
            }>`SELECT script_state FROM jones_workspace_retirement_intents ORDER BY rowid DESC LIMIT 1`;
            expect(rows[0]!.script_state).toBe("completed");
          }
          expect(f.state.resumes).toBe(mode === "successor" ? 2 : 1);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.each(["matching", "missing", "head-changed", "native-policy"] as const)(
  "real qualified deletion checks its original native admission and preservation (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* recordLineage(f);
          // This cleanup fixture launches no settle script. Supply its explicit
          // synthetic no-script acknowledgement; completion admission itself is
          // exercised by the real settlement owner above and the retirement suite.
          const fixtureSql = yield* SqlClient.SqlClient;
          yield* fixtureSql`UPDATE jones_workspace_retirement_intents SET script_state='completed',script_completed_at=settled_at,script_exit_code=0 WHERE worktree=${f.linked}`;
          yield* fixtureSql`UPDATE jones_workspace_duty_claims SET released_at=acquired_at WHERE kind='settlement-intent' AND worktree=${f.linked}`;
          if (mode !== "missing") f.state.assess();
          if (mode === "head-changed")
            yield* f.runGit(["commit", "--allow-empty", "-m", "changed-head"], f.linked);
          yield* f.dispatch({
            type: "thread.delete",
            commandId: CommandId.make("delete-native"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          // Synthetic native cleanup receipts: no provider or pending native effect remains.
          yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded' WHERE thread_id=${threadId}`;
          const rules = yield* decodeCleanupRules({
            ...resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, null),
            worktreeOnDelete: mode !== "native-policy",
          });
          const sink = yield* EventSink.EventSinkV2;
          const result = yield* Effect.gen(function* () {
            const admission = yield* sink.startDeletionWorktreeRemoval!({
              threadId,
              target: {
                projectId,
                projectRoot: f.repo,
                path: f.linked,
                branch: "work",
                force: false,
              },
              rules,
              currentRules: Effect.succeed(rules),
              currentLive: Effect.succeed(true),
            });
            expect(admission.status).toBe("start_now");
            const removal = yield* Removal.QualifiedDeletionWorktreeRemoval;
            yield* removal.execute(
              admission.start.effectId,
              Effect.succeed(rules),
              Effect.succeed(true),
            );
          }).pipe(
            Effect.provide(
              Removal.layer.pipe(
                Layer.provide(
                  Removal.producerLayer.pipe(
                    Layer.provide(
                      RetirementGate.layer.pipe(
                        Layer.provide(f.configured),
                        Layer.provide(Settings.layerTest()),
                      ),
                    ),
                    Layer.provide(
                      Layer.succeed(Removal.DeletionWorktreeGit, { execute: f.git.execute }),
                    ),
                  ),
                ),
              ),
            ),
            Effect.exit,
          );
          expect(Exit.isSuccess(result)).toBe(mode === "matching");
          expect(yield* f.fs.exists(f.linked)).toBe(mode !== "matching");
          expect(f.state.resumes).toBe(1);
        }).pipe(
          Effect.provideService(NativeObservations, {
            services: async () => fixtureProbe(),
            processes: async () => fixtureProbe(),
            workloads: () => Effect.sync(fixtureProbe),
            temporaryRoots: [],
          }),
          Effect.provideService(LifecycleRegistry, {
            resume: () => Effect.die("Cleanup fixtures must not resume or register a workspace"),
            show: () => Effect.sync(f.state.view),
            outputs: () => Effect.succeed([]),
          }),
          Effect.provide(f.layer),
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.each(["matching", "missing", "head-changed", "native-policy"] as const)(
  "real ordinary storage cleanup checks preservation and retains native policy (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* recordLineage(f);
          // This cleanup fixture launches no settle script. Supply its explicit
          // synthetic no-script acknowledgement; completion admission itself is
          // exercised by the real settlement owner above and the retirement suite.
          const fixtureSql = yield* SqlClient.SqlClient;
          yield* fixtureSql`UPDATE jones_workspace_retirement_intents SET script_state='completed',script_completed_at=settled_at,script_exit_code=0 WHERE worktree=${f.linked}`;
          yield* fixtureSql`UPDATE jones_workspace_duty_claims SET released_at=acquired_at WHERE kind='settlement-intent' AND worktree=${f.linked}`;
          if (mode !== "missing") f.state.assess();
          if (mode === "head-changed")
            yield* f.runGit(["commit", "--allow-empty", "-m", "changed-head"], f.linked);
          yield* TestClock.adjust("2 minutes");
          const cleanupSettings = {
            ...DEFAULT_SERVER_SETTINGS,
            storageCleanup: { ...DEFAULT_SERVER_SETTINGS.storageCleanup, worktreeAfterDays: null },
            worktreeCleanup: {
              mode: "custom" as const,
              rules: {
                ...resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, null),
                worktreeAfterDays: null,
                worktreeSettledAfterDays: mode === "native-policy" ? null : 0,
                worktreeOnMerge: false,
                worktreeUnchanged: false,
                worktreeOnDelete: false,
              },
            },
          };
          yield* Effect.gen(function* () {
            const storage = yield* Storage.StorageCleanup;
            yield* storage.runNow;
          }).pipe(
            Effect.provide(
              Storage.layer.pipe(
                Layer.provide(f.configured),
                Layer.provide(Layer.succeed(Git.GitVcsDriver, f.git)),
                Layer.provide(Settings.layerTest(cleanupSettings)),
                Layer.provide(
                  Layer.mock(GitManager.GitManager)({ invalidateStatus: () => Effect.void }),
                ),
                Layer.provide(
                  Layer.mock(Terminals.TerminalManager)({
                    subscribeMetadata: () => Effect.succeed(() => {}),
                  }),
                ),
              ),
            ),
          );
          expect(yield* f.fs.exists(f.linked)).toBe(mode !== "matching");
          expect(f.state.resumes).toBe(1);
        }).pipe(
          Effect.provideService(NativeObservations, {
            services: async () => fixtureProbe(),
            processes: async () => fixtureProbe(),
            workloads: () => Effect.sync(fixtureProbe),
            temporaryRoots: [],
          }),
          Effect.provideService(LifecycleRegistry, {
            resume: () => Effect.die("Cleanup fixtures must not resume or register a workspace"),
            show: () => Effect.sync(f.state.view),
            outputs: () => Effect.succeed([]),
          }),
          Effect.provide(f.layer),
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
