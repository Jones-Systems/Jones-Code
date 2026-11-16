import { ThreadId, type ProjectId, type WorkspaceRetirementOutcome } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Option from "effect/Option";
import { TargetObservation, presenceOutcome } from "./TargetPresence.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import { ClaimAbsence } from "./ClaimReconciliation.ts";
import { DutyIdentityJson, SettlementHolderJson } from "./DurableCodecs.ts";
import { dutyIdentity } from "./DutyIdentity.ts";
import {
  settlementClaimId,
  SettlementRecoveryGuard,
  type Receipts,
  RetirementError,
  type SettlementIntent,
} from "./Receipts.ts";

const isRetirementError = Schema.is(RetirementError);
const decodeSettlementHolder = Schema.decodeEffect(SettlementHolderJson);
const decodeDutyIdentity = Schema.decodeEffect(DutyIdentityJson);

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
  resume: (intent: SettlementIntent) => Effect.Effect<void, RetirementError>,
  propose: (
    thread: { id: ThreadId; projectId: ProjectId },
    worktree: string,
  ) => Effect.Effect<void, RetirementError>,
) {
  const projections = yield* Projections.ProjectionStoreV2;
  const absence = yield* ClaimAbsence;
  const projects = yield* Projects.ProjectStoreV2;
  const observe = yield* TargetObservation;
  const retryAfter = new Map<string, number>();
  let cursor = 0;
  return Effect.fn("WorkspaceRetirement.recoverSettlements")(function* () {
    const batch = yield* store.recoveryBatch(cursor);
    cursor = batch.length === 100 ? batch[batch.length - 1]!.cursor : 0;
    let completedPasses = 0;
    const now = DateTime.toEpochMillis(DateTime.nowUnsafe());
    for (const intent of batch) {
      if (intent.script_state === "completed") {
        if (completedPasses >= 3 || (retryAfter.get(intent.event_id) ?? 0) > now) continue;
        completedPasses++;
        // Bounded memory and a five-minute cooldown; no durable scheduler or script retry.
        if (retryAfter.size >= 300) retryAfter.delete(retryAfter.keys().next().value!);
        retryAfter.set(intent.event_id, now + 5 * 60 * 1000);
      }
      yield* Effect.gen(function* () {
        const holder =
          intent.holder_json === null
            ? null
            : yield* decodeSettlementHolder(intent.holder_json).pipe(
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
        const latestIntent = yield* store.settlement(
          intent.thread_id,
          intent.worktree,
          intent.settled_at,
        );
        const current =
          latestIntent?.event_id === intent.event_id &&
          thread.settledOverride === "settled" &&
          thread.worktreePath === intent.worktree &&
          thread.settledAt !== null &&
          DateTime.formatIso(thread.settledAt) === intent.settled_at;
        if (!current) {
          if (intent.script_state === "pending") yield* store.cancelPendingScript(intent.event_id);
          yield* store.recoveryOutcome(
            intent.event_id,
            intent.script_state === "pending" ? "cancelled" : intent.script_state,
            held(
              intent,
              "settlement_superseded",
              "The thread no longer matches this settlement. Its never-issued script cannot authorize cleanup.",
              "Use the current thread settlement and binding.",
            ),
          );
          return;
        }
        const project = yield* projects.get(thread.projectId);
        if (Option.isNone(project))
          return yield* new RetirementError({
            target: intent.worktree,
            reason: "project_metadata_missing",
          });
        const prior =
          intent.identity_json === null
            ? null
            : yield* decodeDutyIdentity(intent.identity_json).pipe(
                Effect.orElseSucceed(() => null),
              );
        const presence = yield* Effect.promise(() =>
          observe(intent.worktree, project.value.workspaceRoot, prior),
        );
        const revalidate = Effect.gen(function* () {
          const fresh = yield* projections.getThread(thread.id);
          if (
            fresh.settledOverride !== "settled" ||
            fresh.worktreePath !== intent.worktree ||
            fresh.settledAt === null ||
            DateTime.formatIso(fresh.settledAt) !== intent.settled_at ||
            fresh.branch !== thread.branch ||
            fresh.projectId !== thread.projectId ||
            DateTime.toEpochMillis(fresh.updatedAt) !== DateTime.toEpochMillis(thread.updatedAt)
          )
            return yield* new RetirementError({
              target: intent.worktree,
              reason: "settlement_superseded",
            });
          const freshProject = yield* projects.get(thread.projectId);
          if (
            Option.isNone(freshProject) ||
            freshProject.value.workspaceRoot !== project.value.workspaceRoot
          )
            return yield* new RetirementError({
              target: intent.worktree,
              reason: "target_presence_changed",
            });
          const latest = yield* store.settlement(
            intent.thread_id,
            intent.worktree,
            intent.settled_at,
          );
          if (latest?.event_id !== intent.event_id)
            return yield* new RetirementError({
              target: intent.worktree,
              reason: "settlement_superseded",
            });
          const again = yield* Effect.promise(() =>
            observe(intent.worktree, project.value.workspaceRoot, prior),
          );
          if (
            again.status !== presence.status ||
            ("reason" in again && "reason" in presence && again.reason !== presence.reason)
          )
            return yield* new RetirementError({
              target: intent.worktree,
              reason: "target_presence_changed",
            });
        }).pipe(
          Effect.mapError((cause) =>
            isRetirementError(cause)
              ? cause
              : new RetirementError({
                  target: intent.worktree,
                  reason: "settlement_recovery_unavailable",
                  cause,
                }),
          ),
        );
        if (presence.status !== "present") {
          const outcome = presenceOutcome(intent.worktree, presence);
          if (presence.status === "absent_unregistered" && intent.script_state === "pending") {
            // State CAS and claim release share a transaction; an issued script cannot be cancelled here.
            yield* store.cancelPendingScript(intent.event_id, revalidate, outcome);
          } else
            yield* store.recoveryOutcome(intent.event_id, intent.script_state, outcome, revalidate);
          return;
        }
        if (intent.script_state === "pending") {
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
          yield* revalidate;
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
            yield* propose(thread, intent.worktree).pipe(
              Effect.provideService(SettlementRecoveryGuard, revalidate),
            );
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
          isRetirementError(cause) &&
          ["settlement_superseded", "target_presence_changed"].includes(cause.reason)
            ? Effect.void
            : Effect.logWarning("Settlement recovery held", {
                eventId: intent.event_id,
                cause,
              }).pipe(
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
                Effect.ignore,
              ),
        ),
      );
    }
  });
});
