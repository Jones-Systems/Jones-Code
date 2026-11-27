import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import {
  fixture,
  threadId,
  projectId,
  selection,
} from "../workspaceLifecycle/ManagedConsumer.testkit.ts";
import { command } from "./Conformance.testkit.ts";
import { ClaimAbsence } from "./ClaimReconciliation.ts";
import { Receipts, layer } from "./Receipts.ts";
import * as Recovery from "./SettlementRecovery.ts";
import { TargetObservation } from "./TargetPresence.ts";

it.effect("deleting a thread cancels its never-issued settlement script", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* f.seed;
        yield* f.dispatch({ type: "thread.settle", commandId: CommandId.make("settle"), threadId });
        const store = yield* Receipts.pipe(Effect.provide(layer));
        const [intent] = yield* store.recoveryBatch(0);
        expect(intent?.script_state).toBe("pending");
        yield* f.dispatch({ type: "thread.delete", commandId: CommandId.make("delete"), threadId });
        expect(yield* store.startScript(intent!.event_id)).toBe(false);
        const sql = yield* SqlClient.SqlClient;
        expect(
          yield* sql`SELECT script_state FROM jones_workspace_retirement_intents WHERE event_id=${intent!.event_id}`,
        ).toEqual([{ script_state: "cancelled" }]);
        expect((yield* store.claims(f.linked)).map((claim) => claim.claim_id)).not.toContain(
          `settlement-intent:${intent!.event_id}`,
        );
      }).pipe(Effect.provide(f.layer));
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.each(["pending", "completed", "failed", "cancelled", "running"] as const)(
  "absence reconciles %s truthfully and idempotently without replay or retirement receipts",
  (state) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("absent"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [intent] = yield* store.recoveryBatch(0);
          if (state !== "pending") {
            expect(yield* store.startScript(intent!.event_id)).toBe(true);
            if (state !== "running")
              yield* store.finishScript(intent!.event_id, state, state === "completed" ? 0 : null);
          }
          yield* Effect.promise(() => command(f.repo, ["worktree", "remove", f.linked]));
          let issued = 0;
          const recover = yield* Recovery.make(
            store,
            () =>
              Effect.sync(() => {
                issued++;
              }),
            () =>
              Effect.sync(() => {
                issued++;
              }),
          ).pipe(
            Effect.provideService(ClaimAbsence, {
              observe: (claim) => Effect.succeed({ holder: claim.holder_json, status: "unknown" }),
            }),
          );
          yield* recover();
          const before = yield* sql`SELECT * FROM jones_workspace_retirement_intents`;
          yield* recover();
          expect(yield* sql`SELECT * FROM jones_workspace_retirement_intents`).toEqual(before);
          const rows = yield* sql<{
            script_state: string;
            script_exit_code: number | null;
            outcome_json: string;
          }>`SELECT * FROM jones_workspace_retirement_intents`;
          const outcome = JSON.parse(rows[0]!.outcome_json);
          expect(rows[0]!.script_state).toBe(state === "pending" ? "cancelled" : state);
          expect(rows[0]!.script_exit_code).toBe(state === "completed" ? 0 : null);
          expect(outcome.reasonCodes).toEqual([
            state === "running" ? "settle_script_holder_unverified" : "worktree_already_absent",
          ]);
          expect(outcome.receiptId).toBeNull();
          expect(yield* sql`SELECT * FROM jones_workspace_retirement_receipts`).toEqual([]);
          expect(yield* sql`SELECT * FROM jones_workspace_retirement_state`).toEqual([]);
          expect((yield* store.claims(f.linked)).length).toBe(state === "running" ? 1 : 0);
          expect(issued).toBe(0);
          if (state !== "running") expect(yield* store.recoveryBatch(0)).toEqual([]);
          expect(
            yield* Effect.promise(() => command(f.repo, ["branch", "--list", "work"])),
          ).toContain("work");
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.each(["unsettle", "recreate", "issued"] as const)(
  "absence revalidation refuses a concurrent %s before cancelling or reporting terminal absence",
  (race) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({ type: "thread.settle", commandId: CommandId.make("race"), threadId });
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [intent] = yield* store.recoveryBatch(0);
          const engine = yield* Orchestrator.OrchestratorV2;
          let observations = 0;
          // Guarded store operations are also exercised directly with real transactions below.
          const guarded = {
            ...store,
            cancelPendingScript: (
              event: string,
              guard?: Effect.Effect<void, import("./Receipts.ts").RetirementError>,
              result?: unknown,
            ) =>
              Effect.gen(function* () {
                if (race === "unsettle")
                  yield* engine
                    .dispatch({
                      type: "thread.unsettle",
                      commandId: CommandId.make("resume-race"),
                      threadId,
                      reason: "user",
                    })
                    .pipe(Effect.orDie);
                if (race === "issued") yield* store.startScript(intent!.event_id);
                return yield* store.cancelPendingScript(event, guard, result);
              }),
          };
          const run = yield* Recovery.make(
            guarded,
            () => Effect.void,
            () => Effect.void,
          ).pipe(
            Effect.provideService(TargetObservation, async () =>
              ++observations > 1 && race === "recreate"
                ? { status: "present" }
                : { status: "absent_unregistered" },
            ),
          );
          yield* run();
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{
            script_state: string;
            outcome_json: string | null;
          }>`SELECT * FROM jones_workspace_retirement_intents`;
          expect(rows[0]!.script_state).toBe(
            race === "issued" ? "running" : race === "unsettle" ? "cancelled" : "pending",
          );
          expect(rows[0]!.outcome_json).toBeNull();
          expect((yield* store.claims(f.linked)).length).toBe(race === "unsettle" ? 0 : 1);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect(
  "completed observation holds retry in a small cooled-down batch and update their outcomes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("retry"),
            threadId,
          });
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [first] = yield* store.recoveryBatch(0);
          yield* store.startScript(first!.event_id);
          yield* store.finishScript(first!.event_id, "completed", 0);
          for (let i = 0; i < 7; i++) {
            const extraId = ThreadId.make(`retry-${i}`);
            yield* f.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`birth-${i}`),
              threadId: extraId,
              projectId,
              createdBy: "user",
              creationSource: "web",
              title: "Retry fixture",
              modelSelection: selection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: "work",
              worktreePath: f.linked,
            });
            yield* f.dispatch({
              type: "thread.settle",
              commandId: CommandId.make(`settle-${i}`),
              threadId: extraId,
            });
            const extra = (yield* store.recoveryBatch(0)).find((row) => row.thread_id === extraId)!;
            yield* store.startScript(extra.event_id);
            yield* store.finishScript(extra.event_id, "completed", 0);
            yield* store.outcome(
              extra.event_id,
              Recovery.held(extra, "observation_failed", "failed probe", "retry"),
            );
          }
          yield* store.outcome(
            first!.event_id,
            Recovery.held(first!, "observation_failed", "failed probe", "retry"),
          );
          let proposed = 0;
          const recover = yield* Recovery.make(
            store,
            () => Effect.die("completed script replayed"),
            () =>
              Effect.sync(() => {
                proposed++;
              }),
          );
          yield* recover();
          expect(proposed).toBe(3);
          yield* recover();
          expect(proposed).toBe(6);
          yield* recover();
          expect(proposed).toBe(8);
          yield* recover();
          expect(proposed).toBe(8);
          // The current exact intent is the only one allowed to persist the recovered proposal.
          const latest = (yield* store.settlement(threadId, f.linked, first!.settled_at))!;
          for (const old of (yield* store.recoveryBatch(0)).filter(
            (i) => i.event_id !== latest.event_id,
          ))
            yield* store.outcome(
              old.event_id,
              Recovery.held(old, "active_consumer", "real holder", "wait"),
            );
          const recovered = yield* Recovery.make(
            store,
            () => Effect.die("replayed"),
            () =>
              store.outcome(latest.event_id, {
                ...Recovery.held(latest, "", "fresh observation", "none"),
                state: "proposed",
                reasonCodes: [],
              }),
          );
          yield* recovered();
          yield* recovered();
          expect((yield* store.recoveryBatch(0)).some((i) => i.event_id === latest.event_id)).toBe(
            false,
          );
          yield* store.outcome(first!.event_id, {
            ...Recovery.held(first!, "active_consumer", "real holder", "wait"),
            reasonCodes: ["observation_failed", "active_consumer"],
          });
          expect((yield* store.recoveryBatch(0)).some((i) => i.event_id === first!.event_id)).toBe(
            false,
          );
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("a completed proposal cannot persist a stale outcome after the thread resumes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* f.seed;
        yield* f.dispatch({
          type: "thread.settle",
          commandId: CommandId.make("completed-race"),
          threadId,
        });
        const store = yield* Receipts.pipe(Effect.provide(layer));
        const [intent] = yield* store.recoveryBatch(0);
        yield* store.startScript(intent!.event_id);
        yield* store.finishScript(intent!.event_id, "completed", 0);
        const old = Recovery.held(intent!, "observation_failed", "previous observation", "retry");
        yield* store.outcome(intent!.event_id, old);
        const engine = yield* Orchestrator.OrchestratorV2;
        const recover = yield* Recovery.make(
          store,
          () => Effect.die("replayed"),
          () =>
            Effect.gen(function* () {
              yield* engine
                .dispatch({
                  type: "thread.unsettle",
                  commandId: CommandId.make("completed-resume"),
                  threadId,
                  reason: "user",
                })
                .pipe(Effect.orDie);
              yield* store.outcome(intent!.event_id, {
                ...old,
                state: "proposed",
                reasonCodes: [],
              });
            }),
        );
        yield* recover();
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          outcome_json: string;
        }>`SELECT outcome_json FROM jones_workspace_retirement_intents`;
        expect(JSON.parse(rows[0]!.outcome_json)).toEqual(old);
      }).pipe(Effect.provide(f.layer));
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect(
  "an older never-issued intent cannot keep ownership when a newer intent has the same binding and timestamp",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("superseded-event"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [intent] = yield* store.recoveryBatch(0);
          yield* sql`INSERT INTO jones_workspace_retirement_intents(event_id,thread_id,worktree,settled_at) VALUES('newer-intent',${threadId},${f.linked},${intent!.settled_at})`;
          const recover = yield* Recovery.make(
            store,
            () => Effect.die("superseded intent replayed"),
            () => Effect.void,
          );
          yield* recover();
          const older = yield* sql<{
            script_state: string;
            outcome_json: string;
          }>`SELECT * FROM jones_workspace_retirement_intents WHERE event_id=${intent!.event_id}`;
          expect(older[0]!.script_state).toBe("cancelled");
          expect(JSON.parse(older[0]!.outcome_json).reasonCodes).toEqual(["settlement_superseded"]);
          expect(yield* store.claims(f.linked)).toEqual([]);
          expect(
            (yield* store.settlement(threadId, f.linked, intent!.settled_at))!.script_state,
          ).toBe("pending");
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
