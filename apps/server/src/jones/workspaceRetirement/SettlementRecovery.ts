import { ThreadId, type ProjectId, type WorkspaceRetirementOutcome } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import { ClaimAbsence } from "./ClaimReconciliation.ts";
import { DutyIdentityJson, SettlementHolderJson } from "./DurableCodecs.ts";
import { dutyIdentity } from "./DutyIdentity.ts";
import { settlementClaimId, type Receipts, type SettlementIntent } from "./Receipts.ts";

export const held = (
  intent: SettlementIntent,
  reason: string,
  fact: string,
  nextAction: string,
): WorkspaceRetirementOutcome => ({
  state: "held",
  reasonCodes: [reason],
  receiptId: null,
  fingerprint: null,
  paths: [],
  details: [
    {
      reason,
      target: intent.worktree,
      root: intent.worktree,
      fact,
      missingFact: reason,
      lineage: [{ threadId: intent.thread_id, settlementEventId: intent.event_id }],
      nextAction,
    },
  ],
  outputObligations: [],
});

export const make = Effect.fn("WorkspaceRetirement.makeSettlementRecovery")(function* (
  store: Receipts["Service"],
  resume: (intent: SettlementIntent) => Effect.Effect<void>,
  propose: (
    thread: { id: ThreadId; projectId: ProjectId },
    worktree: string,
  ) => Effect.Effect<void, unknown>,
) {
  const projections = yield* Projections.ProjectionStoreV2;
  const absence = yield* ClaimAbsence;
  let cursor = 0;
  return Effect.fn("WorkspaceRetirement.recoverSettlements")(function* () {
    const batch = yield* store.recoveryBatch(cursor);
    cursor = batch.length === 100 ? batch[batch.length - 1]!.cursor : 0;
    for (const intent of batch) {
      yield* Effect.gen(function* () {
        const holder =
          intent.holder_json === null
            ? null
            : yield* Schema.decodeEffect(SettlementHolderJson)(intent.holder_json).pipe(
                Effect.orElseSucceed(() => null),
              );
        if (intent.script_state === "running") {
          let status: "present" | "unknown" | "verified_absent" = "unknown";
          if (holder?.eventId === intent.event_id && intent.holder_json !== null) {
            const observed = yield* absence.observe({
              claim_id: settlementClaimId(intent.event_id),
              worktree: intent.worktree,
              kind: "settlement-intent",
              owner: intent.thread_id,
              holder_json: intent.holder_json,
              native_owner_json: intent.native_owner_json,
            });
            status = observed.status;
            if (
              observed.status === "verified_absent" &&
              observed.holder === intent.holder_json &&
              (yield* store.cancelAbsentScript(intent.event_id, observed.holder))
            ) {
              yield* store.recoveryOutcome(
                intent.event_id,
                "cancelled",
                held(
                  intent,
                  "settle_script_cancelled",
                  "The exact recorded script holder and its possible descendants are verified absent; completion remains unproved.",
                  "Review the script's actual effects before explicitly settling again. Retirement remains held.",
                ),
              );
              return;
            }
          }
          // Neither restart, PID disappearance nor missing completion proves child drain.
          yield* store.recoveryOutcome(
            intent.event_id,
            "running",
            held(
              intent,
              status === "present" ? "settle_script_running" : "settle_script_holder_unverified",
              status === "present"
                ? "The original script holder is observed present. Completion is not recorded; its durable duty remains live."
                : "The settle script was issued, but its completion or exact holder absence is unproved. Its durable duty is retained.",
              "Have the native script/terminal owner record actual completion or verify its exact isolated session and descendants absent; never rerun this intent.",
            ),
          );
          return;
        }
        const thread = yield* projections.getThread(ThreadId.make(intent.thread_id));
        const current =
          thread.settledOverride === "settled" &&
          thread.worktreePath === intent.worktree &&
          thread.settledAt !== null &&
          DateTime.formatIso(thread.settledAt) === intent.settled_at;
        if (intent.script_state === "pending") {
          if (!current) {
            yield* store.cancelPendingScript(intent.event_id);
            yield* store.recoveryOutcome(
              intent.event_id,
              "cancelled",
              held(
                intent,
                "settlement_superseded",
                "The thread is no longer settled at this intent's original binding and time; its never-issued script was cancelled.",
                "Use the thread's current settlement state. This old intent cannot authorize cleanup.",
              ),
            );
            return;
          }
          const prior =
            intent.identity_json === null
              ? null
              : yield* Schema.decodeEffect(DutyIdentityJson)(intent.identity_json).pipe(
                  Effect.orElseSucceed(() => null),
                );
          const native = yield* Effect.promise(() => dutyIdentity(intent.worktree));
          if (
            holder?.eventId !== intent.event_id ||
            holder.projectId !== thread.projectId ||
            holder.branch !== thread.branch ||
            prior?.status !== "bound" ||
            native.status !== "bound" ||
            native.incarnation !== prior.incarnation
          ) {
            yield* store.recoveryOutcome(
              intent.event_id,
              "pending",
              held(
                intent,
                "settlement_incarnation_unverified",
                "The pending settlement cannot be bound to the original physical checkout.",
                "Observe the original checkout identity through its owner; do not run the old script on a replacement. Unsettle and settle explicitly to create a new intent.",
              ),
            );
            return;
          }
          yield* resume(intent);
          const after = yield* store.settlement(
            intent.thread_id,
            intent.worktree,
            intent.settled_at,
          );
          if (after?.script_state === "pending")
            yield* store.recoveryOutcome(
              intent.event_id,
              "pending",
              held(
                intent,
                "settle_script_recovery_pending",
                "The original intent remains never-issued after current settlement guards were rechecked.",
                "Resolve this checkout's current preservation or lifecycle hold, then reevaluate. Retirement remains held.",
              ),
            );
          return;
        }
        if (intent.needs_outcome) {
          if (current && intent.script_state === "completed")
            yield* propose(thread, intent.worktree);
          else
            yield* store.recoveryOutcome(
              intent.event_id,
              intent.script_state,
              held(
                intent,
                current ? `settle_script_${intent.script_state}` : "settlement_superseded",
                "This intent has a terminal script state or no longer matches the current settlement.",
                "Review the recorded script result and current thread state before a new explicit settlement; retirement remains held.",
              ),
            );
        }
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Settlement recovery held", { eventId: intent.event_id, cause }).pipe(
            Effect.andThen(
              store.recoveryOutcome(
                intent.event_id,
                intent.script_state,
                held(
                  intent,
                  "settlement_recovery_unavailable",
                  "Current settlement or holder observation is unavailable; no script replay or claim release is authorized.",
                  "Restore the current projection and native holder observation, then reevaluate this intent. Retain its real duty meanwhile.",
                ),
              ),
            ),
            Effect.catch(() => Effect.void),
          ),
        ),
      );
    }
  });
});
