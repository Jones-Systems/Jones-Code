import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { userFacingDispatchErrorMessage } from "../../orchestration-v2/UserFacingErrors.ts";
import { fixture, threadId } from "../workspaceLifecycle/ManagedConsumer.testkit.ts";
import { ClaimAbsence } from "./ClaimReconciliation.ts";
import { Receipts, layer, settlementClaimId, type SettlementIntent } from "./Receipts.ts";
import * as Recovery from "./SettlementRecovery.ts";

it.effect(
  "restart resumes a never-issued settlement once; redelivery and a second recovery do not replay it",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const command = {
            type: "thread.settle" as const,
            commandId: CommandId.make("settle-restart"),
            threadId,
          };
          yield* f.dispatch(command);
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const before = yield* store.recoveryBatch(0);
          expect(before).toHaveLength(1);
          expect(before[0]!.script_state).toBe("pending");
          expect(before[0]!.identity_json).toContain('"status":"bound"');
          yield* f.dispatch(command);
          expect(yield* store.recoveryBatch(0)).toEqual(before);
          let issued = 0;
          const resume = (intent: SettlementIntent) =>
            Effect.gen(function* () {
              if (!(yield* store.startScript(intent.event_id, true))) return;
              issued++;
              yield* store.finishScript(intent.event_id, "completed", 0);
              yield* store.outcome(intent.event_id, {
                ...Recovery.held(intent, "fixture", "Synthetic completion.", "None."),
                state: "proposed",
              });
            }).pipe(Effect.orDie);
          const recover = yield* Recovery.make(store, resume, () => Effect.void);
          yield* recover();
          const restarted = yield* Recovery.make(
            store,
            () =>
              Effect.sync(() => {
                issued++;
              }),
            () => Effect.void,
          );
          yield* restarted();
          expect(issued).toBe(1);
          expect(yield* sql`SELECT script_state FROM jones_workspace_retirement_intents`).toEqual([
            { script_state: "completed" },
          ]);
          expect(
            yield* sql`SELECT claim_id FROM jones_workspace_duty_claims WHERE released_at IS NULL`,
          ).toEqual([]);
          for (let index = 0; index < 101; index++) {
            yield* sql`INSERT INTO jones_workspace_retirement_intents(event_id,thread_id,worktree,settled_at) VALUES(${`old-${index}`},${threadId},${f.linked},'2000-01-01T00:00:00.000Z')`;
          }
          const bounded = yield* Recovery.make(
            store,
            () => Effect.die("obsolete intent issued"),
            () => Effect.void,
          );
          yield* bounded();
          expect(
            yield* sql`SELECT COUNT(*) AS count FROM jones_workspace_retirement_intents WHERE script_state='pending'`,
          ).toEqual([{ count: 1 }]);
          yield* bounded();
          expect(
            yield* sql`SELECT COUNT(*) AS count FROM jones_workspace_retirement_intents WHERE script_state='pending'`,
          ).toEqual([{ count: 0 }]);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["unsettled", "time", "branch", "replacement", "legacy"] as const)(
  "restart revalidates the still-settled binding and original incarnation (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-revalidate"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          if (mode === "unsettled")
            yield* f.dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make("unsettle"),
              reason: "user",
              threadId,
            });
          if (mode === "time")
            yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.settledAt','2026-11-09T00:00:00.000Z') WHERE thread_id=${threadId}`;
          if (mode === "branch")
            yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch','other') WHERE thread_id=${threadId}`;
          if (mode === "replacement") {
            yield* f.fs.rename(f.linked, `${f.linked}-original`);
            yield* f.fs.makeDirectory(f.linked);
          }
          if (mode === "legacy")
            yield* sql`DELETE FROM jones_workspace_duty_identity WHERE claim_id LIKE 'settlement-intent:%'`;
          let issued = 0;
          const recover = yield* Recovery.make(
            store,
            () =>
              Effect.sync(() => {
                issued++;
              }),
            () => Effect.void,
          );
          yield* recover();
          expect(issued).toBe(0);
          const [intent] = yield* sql<{
            script_state: string;
          }>`SELECT script_state FROM jones_workspace_retirement_intents`;
          expect(intent!.script_state).toBe(
            ["unsettled", "time"].includes(mode) ? "cancelled" : "pending",
          );
          if (!["unsettled", "time"].includes(mode)) {
            const rows = yield* sql<{
              outcome_json: string;
            }>`SELECT outcome_json FROM jones_workspace_retirement_intents`;
            expect(JSON.parse(rows[0]!.outcome_json).reasonCodes).toEqual([
              "settlement_incarnation_unverified",
            ]);
          }
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "retiring atomically refuses manual unsettle and exposes the checkout hold through the client error mapper",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-conflict"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const engine = yield* Orchestrator.OrchestratorV2;
          const before = (yield* engine.getThreadProjection(threadId)).thread;
          const intents = yield* sql`SELECT * FROM jones_workspace_retirement_intents`;
          yield* sql`INSERT INTO jones_workspace_retirement_state VALUES(${f.linked},'synthetic-incarnation','retiring','synthetic-receipt')`;
          const result = yield* f
            .dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make("unsettle-conflict"),
              threadId,
              reason: "user",
            })
            .pipe(Effect.exit);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") {
            const error = Cause.squash(result.cause);
            expect(userFacingDispatchErrorMessage(error)).toContain("retiring");
          }
          expect((yield* engine.getThreadProjection(threadId)).thread).toEqual(before);
          expect(yield* sql`SELECT * FROM jones_workspace_retirement_intents`).toEqual(intents);
          expect(f.calls.some((call) => call.startsWith("provider-"))).toBe(false);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["present", "unknown", "absent", "changed-holder", "completed-race"] as const)(
  "issued scripts never replay; only exact verified absence cancels (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.gen(function* () {
          yield* f.seed;
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-issued"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [intent] = yield* store.recoveryBatch(0);
          expect(yield* store.startScript(intent!.event_id)).toBe(true);
          let issued = 0;
          const recover = yield* Recovery.make(
            store,
            () =>
              Effect.sync(() => {
                issued++;
              }),
            () => Effect.void,
          ).pipe(
            Effect.provideService(ClaimAbsence, {
              observe: (claim) =>
                Effect.gen(function* () {
                  if (mode === "changed-holder")
                    yield* sql`UPDATE jones_workspace_duty_claims SET holder_json='"successor"' WHERE claim_id=${claim.claim_id}`;
                  if (mode === "completed-race") {
                    yield* store.finishScript(intent!.event_id, "completed", 0);
                    yield* store.outcome(intent!.event_id, {
                      ...Recovery.held(intent!, "fixture", "Synthetic completion.", "None."),
                      state: "proposed",
                    });
                  }
                  return {
                    holder: claim.holder_json,
                    status:
                      mode === "present"
                        ? ("present" as const)
                        : mode === "unknown"
                          ? ("unknown" as const)
                          : ("verified_absent" as const),
                  };
                }).pipe(Effect.orDie),
            }),
          );
          yield* recover();
          yield* recover();
          expect(issued).toBe(0);
          const [after] = yield* sql<{
            script_state: string;
          }>`SELECT script_state FROM jones_workspace_retirement_intents`;
          expect(after!.script_state).toBe(
            mode === "absent" ? "cancelled" : mode === "completed-race" ? "completed" : "running",
          );
          const claims = yield* store.claims(f.linked);
          expect(
            claims.some((claim) => claim.claim_id === settlementClaimId(intent!.event_id)),
          ).toBe(!["absent", "completed-race"].includes(mode));
          const rows = yield* sql<{
            outcome_json: string;
          }>`SELECT outcome_json FROM jones_workspace_retirement_intents`;
          const outcome = JSON.parse(rows[0]!.outcome_json);
          if (mode === "completed-race") expect(outcome.state).toBe("proposed");
          else
            expect(outcome.reasonCodes).toEqual([
              mode === "absent"
                ? "settle_script_cancelled"
                : mode === "present"
                  ? "settle_script_running"
                  : "settle_script_holder_unverified",
            ]);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
