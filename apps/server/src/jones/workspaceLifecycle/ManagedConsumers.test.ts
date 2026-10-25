import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Restore from "../settlementRestore/SettlementRestore.ts";
import * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Boundary from "./NativeBoundary.ts";
import { fixture, threadId, selection } from "./ManagedConsumer.testkit.ts";

it.effect(
  "real command precommit issues present and removed unsettle/restore attestations; registry I/O stays outside SQL transactions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const sql = yield* SqlClient.SqlClient;
          f.state.beforeRegistryRead = Effect.gen(function* () {
            expect(Option.isNone(yield* Effect.serviceOption(sql.transactionService))).toBe(true);
          });
          const settle = (id: string) =>
            f.dispatch({ type: "thread.settle", commandId: CommandId.make(id), threadId });
          const unsettle = (id: string) =>
            f.dispatch({
              type: "thread.unsettle",
              reason: "user",
              commandId: CommandId.make(id),
              threadId,
            });
          yield* settle("settle-present");
          yield* unsettle("unsettle-present");
          const original = f.state.view().binding!;
          expect(original).not.toBeNull();
          yield* settle("settle-restore-present");
          yield* (yield* Restore.SettlementRestore).restore({
            threadId,
            settleCommandId: CommandId.make("settle-restore-present"),
            commandId: CommandId.make("restore-present"),
          });
          yield* settle("settle-removed");
          yield* f.runGit(["worktree", "remove", f.linked]);
          yield* unsettle("unsettle-removed");
          expect(f.state.view().binding!.incarnation_id).toBe(original.incarnation_id);
          const rows = yield* sql<{
            source_json: string;
          }>`SELECT source_json FROM jones_workspace_lifecycle_transitions WHERE transition_id='command:unsettle-removed:unsettle'`;
          expect(rows[0]!.source_json).toContain('\\"missing\\":true');
          yield* settle("settle-restore");
          yield* (yield* Restore.SettlementRestore).restore({
            threadId,
            settleCommandId: CommandId.make("settle-restore"),
            commandId: CommandId.make("restore-removed"),
          });
          expect(f.state.view().binding!.incarnation_id).toBe(original.incarnation_id);
          expect(f.state.resumes).toBe(4);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
          expect(yield* sql`SELECT state FROM jones_workspace_lifecycle_transitions`).toEqual([
            { state: "confirmed" },
            { state: "confirmed" },
            { state: "confirmed" },
            { state: "confirmed" },
          ]);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "real worker/start/session consumers read CAS back before provider effects and check a loaded-session resume again",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("start-work"),
            text: "Synthetic work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* worker.drain(20);
          expect(f.calls).toContain("provider-open");
          expect(
            yield* (yield* SqlClient.SqlClient)`SELECT last_error FROM orchestration_v2_effect_outbox WHERE last_error IS NOT NULL`,
          ).toEqual([]);
          expect(f.calls).toContain("provider-send");
          expect(f.calls.indexOf("cas")).toBeLessThan(f.calls.indexOf("provider-open"));
          expect(f.calls[f.calls.indexOf("provider-open") - 1]).toBe("show");
          const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(
            threadId,
          );
          const providerThread = projection.providerThreads[0]!;
          const live = yield* (yield* Sessions.ProviderSessionManagerV2).get(
            providerThread.providerSessionId!,
          );
          expect(Option.isSome(live)).toBe(true);
          if (Option.isNone(live)) return;
          const beforeEnsure = f.state.resumes;
          yield* live.value.ensureThread({
            threadId,
            existingProviderThread: providerThread,
            modelSelection: selection,
            runtimePolicy: Adapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: f.linked,
            }),
          });
          // The adapter's nested native admission reuses this operation's original transition.
          expect(f.state.resumes).toBe(beforeEnsure + 1);
          const count = f.state.resumes;
          const externalResumes = f.calls.filter((call) => call === "provider-resume").length;
          const resume = () =>
            live.value.resumeThread({
              threadId,
              providerThread,
              modelSelection: selection,
              runtimePolicy: Adapter.ProviderAdapterV2RuntimePolicy.make({
                runtimeMode: "full-access",
                interactionMode: "default",
                cwd: f.linked,
              }),
            });
          yield* resume();
          expect(f.state.resumes).toBe(count + 1);
          expect(f.calls.filter((call) => call === "provider-resume")).toHaveLength(
            externalResumes,
          );
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch','changed') WHERE thread_id=${threadId}`;
          expect(Exit.isFailure(yield* Effect.exit(resume()))).toBe(true);
          expect(f.calls.filter((call) => call === "provider-resume")).toHaveLength(
            externalResumes,
          );
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["present", "removed"] as const)(
  "implicit new work invalidates the exact %s checkout before command commit",
  (presence) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-before-lineage"),
            threadId,
          });
          yield* f.dispatch({
            type: "thread.unsettle",
            reason: "user",
            commandId: CommandId.make("record-lineage"),
            threadId,
          });
          const original = f.state.view().binding!;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-before-work"),
            threadId,
          });
          if (presence === "removed") yield* f.runGit(["worktree", "remove", f.linked]);
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("implicit-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("implicit-work"),
            text: "Synthetic queued work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          expect(f.state.view().binding!.incarnation_id).toBe(original.incarnation_id);
          expect(f.state.resumes).toBe(2);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["attempt", "assignment", "birth", "head", "directory", "cancellation"] as const)(
  "fresh production attestation holds a changed %s before the first provider effect",
  (change) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const sql = yield* SqlClient.SqlClient;
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("start-work"),
            text: "Synthetic work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          f.state.afterResume = Effect.gen(function* () {
            f.state.afterResume = Effect.void;
            if (change === "assignment")
              yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch','changed') WHERE thread_id=${threadId}`;
            if (change === "attempt")
              yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.activeAttemptId','changed') WHERE thread_id=${threadId}`;
            if (change === "birth")
              yield* sql`UPDATE orchestration_events SET event_id='replaced-birth' WHERE stream_id=${threadId} AND event_type='thread.created'`;
            if (change === "head")
              yield* f
                .runGit(["commit", "--allow-empty", "-m", "successor"], f.linked)
                .pipe(Effect.orDie);
            if (change === "directory") {
              yield* f.fs.rename(f.linked, `${f.linked}-old`).pipe(Effect.orDie);
              yield* f.fs.makeDirectory(f.linked).pipe(Effect.orDie);
              yield* f.fs.copyFile(`${f.linked}-old/.git`, `${f.linked}/.git`).pipe(Effect.orDie);
            }
            if (change === "cancellation")
              yield* sql`UPDATE orchestration_v2_effect_outbox SET status='cancelled' WHERE thread_id=${threadId} AND status='running'`;
          }).pipe(Effect.orDie);
          yield* (yield* Worker.OrchestrationEffectWorkerV2).drain(20).pipe(Effect.exit);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
          expect(f.state.resumes).toBe(1);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["assignment", "birth", "head", "directory"] as const)(
  "nested activation keeps the original operation and refuses a changed %s after opening the session",
  (change) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const sql = yield* SqlClient.SqlClient;
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("nested-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("nested-work"),
            text: "Synthetic nested work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          f.state.beforeRegistryRead = Effect.gen(function* () {
            if (!f.calls.includes("provider-open")) return;
            f.state.beforeRegistryRead = Effect.void;
            if (change === "assignment")
              yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch','changed') WHERE thread_id=${threadId}`;
            if (change === "birth")
              yield* sql`UPDATE orchestration_events SET event_id='replaced-birth' WHERE stream_id=${threadId} AND event_type='thread.created'`;
            if (change === "head")
              yield* f
                .runGit(["commit", "--allow-empty", "-m", "nested successor"], f.linked)
                .pipe(Effect.orDie);
            if (change === "directory") {
              yield* f.fs.rename(f.linked, `${f.linked}-old`).pipe(Effect.orDie);
              yield* f.fs.makeDirectory(f.linked).pipe(Effect.orDie);
              yield* f.fs.copyFile(`${f.linked}-old/.git`, `${f.linked}/.git`).pipe(Effect.orDie);
            }
          }).pipe(Effect.orDie);
          yield* (yield* Worker.OrchestrationEffectWorkerV2).drain(20).pipe(Effect.exit);
          expect(f.calls).toContain("provider-open");
          expect(f.calls).not.toContain("provider-ensure");
          expect(f.calls).not.toContain("provider-send");
          const originals = yield* sql<{
            transition_id: string;
          }>`SELECT transition_id FROM jones_workspace_lifecycle_transitions`;
          expect(originals).toHaveLength(2);
          expect(
            new Set(originals.map((row) => row.transition_id.replace(/:(start|resume)$/, ""))).size,
          ).toBe(1);
          expect(f.state.resumes).toBe(2);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "lost CAS response reconciles the stored original after restart without reissuing native execution authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const sql = yield* SqlClient.SqlClient;
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("start-work"),
            text: "Synthetic work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          f.state.loseResponse = true;
          yield* (yield* Worker.OrchestrationEffectWorkerV2).drain(20).pipe(Effect.exit);
          const rows = yield* sql<{
            transition_id: string;
            source_json: string;
            expected_generation: string | null;
          }>`SELECT transition_id,source_json,expected_generation FROM jones_workspace_lifecycle_transitions WHERE state='invoking'`;
          expect(rows).toHaveLength(1);
          const { input } = yield* Schema.decodeEffect(
            Schema.fromJsonString(
              Schema.Struct({
                input: Schema.Struct({ transitionId: Schema.String, requestDigest: Schema.String }),
              }),
            ),
          )(rows[0]!.source_json);
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                Boundary.withOriginalTransition(
                  input.transitionId.replace(/:start$/, ""),
                  null,
                  Boundary.beforeUse({ threadId, boundary: "start" }).pipe(
                    Effect.provideService(Boundary.OriginalNativeTransition, {
                      id: input.transitionId.replace(/:start$/, ""),
                      requestDigest: input.requestDigest,
                    }),
                  ),
                ).pipe(Effect.provide(Layer.fresh(f.restartLifecycle))),
              ),
            ),
          ).toBe(true);
          expect(
            yield* sql`SELECT state,expected_generation FROM jones_workspace_lifecycle_transitions`,
          ).toEqual([{ state: "confirmed", expected_generation: rows[0]!.expected_generation }]);
          expect(f.state.resumes).toBe(1);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "real recreation invalidates the absent prior lineage then records a new physical incarnation before provider use",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("initial-settle"),
            threadId,
          });
          yield* f.dispatch({
            type: "thread.unsettle",
            commandId: CommandId.make("lineage"),
            threadId,
            reason: "user",
          });
          const prior = f.state.view().binding!;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-before-removal"),
            threadId,
          });
          yield* f.runGit(["worktree", "remove", f.linked]);
          yield* f.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("recreate-work"),
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("recreate-work"),
            text: "Synthetic recreation",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          expect(f.state.view().binding!.incarnation_id).toBe(prior.incarnation_id);
          yield* (yield* Worker.OrchestrationEffectWorkerV2).drain(20);
          expect(f.calls).toContain("provider-send");
          expect(f.calls.indexOf("git-create")).toBeLessThan(f.calls.indexOf("provider-open"));
          expect(f.calls[f.calls.indexOf("git-create") - 1]).toBe("show");
          expect(f.state.view().binding!.incarnation_id).not.toBe(prior.incarnation_id);
          expect(f.state.view().binding!.head).toBe(prior.head);
          expect(yield* f.fs.exists(f.linked)).toBe(true);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["lineage", "branch", "head"] as const)(
  "missing checkout refuses a missing prior %s before recreation",
  (missing) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const sql = yield* SqlClient.SqlClient;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("first-settle"),
            threadId,
          });
          if (missing !== "lineage") {
            yield* f.dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make("lineage"),
              threadId,
              reason: "user",
            });
            yield* f.dispatch({
              type: "thread.settle",
              commandId: CommandId.make("second-settle"),
              threadId,
            });
          }
          yield* f.runGit(["worktree", "remove", f.linked]);
          if (missing === "branch")
            yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch',NULL) WHERE thread_id=${threadId}`;
          if (missing === "head") yield* f.runGit(["update-ref", "-d", "refs/heads/work"]);
          const result = yield* Effect.exit(
            f.dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make("missing-work"),
              threadId,
              reason: "user",
            }),
          );
          expect(Exit.isFailure(result)).toBe(true);
          expect(f.state.resumes).toBe(missing === "lineage" ? 0 : 1);
          expect(f.calls).not.toContain("git-create");
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
