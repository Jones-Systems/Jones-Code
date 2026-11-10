import type { ReservationRecord } from "./Reservation.ts";
import {
  DutyIdentityJson,
  NativeDutyOwnerJson,
  RetirementReceiptJson,
  SettlementHolderJson,
} from "./DurableCodecs.ts";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { Binding } from "./OutputAttestation.ts";
import type { QualificationResult } from "./Qualification.ts";
import type { SettlementScriptState } from "./SettlementAdmission.ts";
import { dutyIdentity, type DutyIdentity } from "./DutyIdentity.ts";
import { inProcessOwner, unresolvedOwner, type NativeDutyOwner } from "./NativeDutyOwner.ts";

// Internal observation scoped only to the executor's actual acquired reservation.
// Presence alone is never permission: readbacks compare current durable ownership.
export class RetirementReservationOwner extends Context.Reference<{
  readonly worktree: string;
  readonly record: ReservationRecord;
} | null>("t3/jones/workspaceRetirement/Receipts/RetirementReservationOwner", {
  defaultValue: () => null,
}) {}

export class RetirementError extends Schema.TaggedError<RetirementError>()("RetirementError", {
  reason: Schema.String,
  target: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Checkout retirement held (${this.reason}): ${this.target}.`;
  }
}
export interface RetirementReceipt {
  schema: "worktree-retirement/v1";
  id: string;
  cause: "settlement" | "sweep" | "deletion" | "owner_action";
  state: "intent" | "retired" | "not_retired" | "partial" | "unknown";
  binding: Binding;
  incarnation: string;
  qualification: QualificationResult;
  coverage: unknown;
  preservationRefs: { ref: string; head: string }[];
  journal: string;
  disposed: string[];
  reasons: string[];
  limits: readonly string[];
  removalDiagnostic?: string;
  bytes: { logical: number | null; allocated: number | null; statvfs_delta: number | null };
}
export interface SettlementIntent {
  event_id: string;
  thread_id: string;
  worktree: string;
  settled_at: string;
  script_state: SettlementScriptState;
  script_started_at: string | null;
  script_completed_at: string | null;
  script_exit_code: number | null;
}
export const settlementClaimId = (event: string) => `settlement-intent:${event}`;
export interface RecoverableSettlement extends SettlementIntent {
  cursor: number;
  identity_json: string | null;
  native_owner_json: string | null;
  holder_json: string | null;
  needs_outcome: number | null;
}
export interface JournalEntry {
  path: string;
  kind: "untracked" | "ignored";
  identity: string;
  state: "planned" | "invoking" | "completed" | "removed_with_checkout";
}
const mapError = (target: string) =>
  Effect.mapError((cause: unknown) =>
    Schema.is(RetirementError)(cause)
      ? cause
      : new RetirementError({ reason: "storage_unavailable", target, cause }),
  );
export class Receipts extends Context.Service<
  Receipts,
  {
    readonly writable: (worktree: string) => Effect.Effect<void, RetirementError>;
    readonly claims: (worktree: string) => Effect.Effect<
      readonly {
        claim_id: string;
        kind: string;
        owner: string;
        incarnation_id: string | null;
        identity_json: string | null;
        native_owner_json: string | null;
      }[],
      RetirementError
    >;
    readonly claim: (input: {
      id: string;
      worktree: string;
      kind: string;
      owner: string;
      holder: string;
    }) => Effect.Effect<DutyIdentity, RetirementError>;
    readonly releaseClaim: (id: string) => Effect.Effect<void, RetirementError>;
    readonly bindNativeOwner: (
      id: string,
      holder: string,
      owner: NativeDutyOwner,
    ) => Effect.Effect<void, RetirementError>;
    readonly reconcileClaim: (
      id: string,
      holder: string,
      verifiedAbsent: boolean,
    ) => Effect.Effect<boolean, RetirementError>;
    readonly settlement: (
      thread: string,
      worktree: string,
      settledAt: string,
    ) => Effect.Effect<SettlementIntent | null, RetirementError>;
    readonly startScript: (
      event: string,
      recovery?: boolean,
    ) => Effect.Effect<boolean, RetirementError>;
    readonly recoveryBatch: (
      after: number,
    ) => Effect.Effect<readonly RecoverableSettlement[], RetirementError>;
    readonly cancelAbsentScript: (
      event: string,
      holder: string,
    ) => Effect.Effect<boolean, RetirementError>;
    readonly cancelPendingScript: (event: string) => Effect.Effect<void, RetirementError>;
    readonly finishScript: (
      event: string,
      state: Exclude<SettlementScriptState, "pending" | "running">,
      exit: number | null,
    ) => Effect.Effect<void, RetirementError>;
    readonly outcome: (event: string, result: unknown) => Effect.Effect<void, RetirementError>;
    readonly recoveryOutcome: (
      event: string,
      state: SettlementScriptState,
      result: unknown,
    ) => Effect.Effect<void, RetirementError>;
    readonly begin: (receipt: RetirementReceipt) => Effect.Effect<void, RetirementError>;
    readonly finish: (receipt: RetirementReceipt) => Effect.Effect<void, RetirementError>;
    readonly journal: (
      receipt: string,
      entries: readonly JournalEntry[],
    ) => Effect.Effect<void, RetirementError>;
    readonly completePath: (
      receipt: string,
      ordinal: number,
      state: JournalEntry["state"],
    ) => Effect.Effect<void, RetirementError>;
    readonly read: (receipt: string) => Effect.Effect<RetirementReceipt | null, RetirementError>;
  }
>()("t3/jones/workspaceRetirement/Receipts") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const writable = Effect.fn("WorkspaceRetirement.writable")(function* (worktree: string) {
    const rows = yield* sql<{
      state: string;
    }>`SELECT state FROM jones_workspace_retirement_state WHERE worktree=${worktree} AND state='retiring'`;
    if (rows[0]) return yield* new RetirementError({ reason: rows[0].state, target: worktree });
  }, mapError("workspace"));
  const claims = Effect.fn("WorkspaceRetirement.claims")(function* (worktree: string) {
    return yield* sql<{
      claim_id: string;
      kind: string;
      owner: string;
      incarnation_id: string | null;
      identity_json: string | null;
      native_owner_json: string | null;
    }>`SELECT c.claim_id,c.kind,c.owner,c.incarnation_id,i.identity_json,i.native_owner_json FROM jones_workspace_duty_claims c LEFT JOIN jones_workspace_duty_identity i ON i.claim_id=c.claim_id WHERE c.worktree=${worktree} AND c.released_at IS NULL`;
  }, mapError("workspace"));
  return Receipts.of({
    writable,
    claims,
    claim: Effect.fn("WorkspaceRetirement.claim")(function* (input) {
      const identity = yield* Effect.promise(() => dutyIdentity(input.worktree));
      const nativeOwner = ["session", "terminal"].includes(input.kind)
        ? unresolvedOwner(`${input.kind}_native_launch_capture_pending`)
        : yield* Effect.promise(() => inProcessOwner(input.id));
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writable(input.worktree);
          const prior = yield* sql<{
            worktree: string;
            holder_json: string;
            released_at: string | null;
            incarnation_id: string | null;
          }>`SELECT worktree,holder_json,released_at,incarnation_id FROM jones_workspace_duty_claims WHERE claim_id=${input.id}`;
          if (prior[0]) {
            if (
              prior[0].worktree !== input.worktree ||
              prior[0].holder_json !== input.holder ||
              prior[0].released_at !== null ||
              prior[0].incarnation_id !==
                (identity.status === "bound" ? identity.incarnation : null)
            )
              return yield* new RetirementError({
                reason: "claim_identity_changed",
                target: input.worktree,
              });
            return;
          }
          yield* sql`INSERT INTO jones_workspace_duty_claims VALUES (${input.id},${input.worktree},${identity.status === "bound" ? identity.incarnation : null},${input.kind},${input.owner},${input.holder},${DateTime.formatIso(DateTime.nowUnsafe())},NULL)`;
          yield* sql`INSERT INTO jones_workspace_duty_identity VALUES (${input.id},${yield* Schema.encodeEffect(DutyIdentityJson)(identity)},${yield* Schema.encodeEffect(NativeDutyOwnerJson)(nativeOwner)})`;
        }),
      );
      return identity;
    }, mapError("workspace")),
    bindNativeOwner: Effect.fn("WorkspaceRetirement.bindNativeOwner")(function* (
      id,
      holder,
      owner,
    ) {
      yield* sql`UPDATE jones_workspace_duty_identity SET native_owner_json=${yield* Schema.encodeEffect(NativeDutyOwnerJson)(owner)} WHERE claim_id=${id} AND EXISTS (SELECT 1 FROM jones_workspace_duty_claims c WHERE c.claim_id=${id} AND c.holder_json=${holder} AND c.released_at IS NULL)`;
    }, mapError("workspace")),
    releaseClaim: Effect.fn("WorkspaceRetirement.releaseClaim")(function* (id) {
      yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${id} AND released_at IS NULL`;
    }, mapError("workspace")),
    reconcileClaim: Effect.fn("WorkspaceRetirement.reconcileClaim")(function* (
      id,
      holder,
      verifiedAbsent,
    ) {
      if (!verifiedAbsent)
        return yield* new RetirementError({
          reason: "claim_holder_absence_unverified",
          target: id,
        });
      const released =
        yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${id} AND holder_json=${holder} AND released_at IS NULL RETURNING claim_id`;
      return released.length === 1;
    }, mapError("workspace")),
    settlement: Effect.fn("WorkspaceRetirement.settlement")(function* (
      thread,
      worktree,
      settledAt,
    ) {
      const rows =
        yield* sql<SettlementIntent>`SELECT * FROM jones_workspace_retirement_intents WHERE thread_id=${thread} AND worktree=${worktree} AND settled_at=${settledAt} ORDER BY rowid DESC LIMIT 1`;
      return rows[0] ?? null;
    }, mapError("workspace")),
    recoveryBatch: Effect.fn("WorkspaceRetirement.recoveryBatch")(function* (after) {
      return yield* sql<RecoverableSettlement>`SELECT i.*,i.rowid AS cursor,d.identity_json,d.native_owner_json,c.holder_json,
        (i.outcome_json IS NULL OR json_extract(i.outcome_json,'$.reasonCodes[0]') IN ('settle_script_pending','settle_script_recovery_pending')) AS needs_outcome
        FROM jones_workspace_retirement_intents i
        LEFT JOIN jones_workspace_duty_claims c ON c.claim_id='settlement-intent:'||i.event_id AND c.worktree=i.worktree AND c.owner=i.thread_id AND c.kind='settlement-intent'
        LEFT JOIN jones_workspace_duty_identity d ON d.claim_id=c.claim_id
        WHERE i.rowid>${after} AND (i.script_state IN ('pending','running') OR i.outcome_json IS NULL OR json_extract(i.outcome_json,'$.reasonCodes[0]') IN ('settle_script_pending','settle_script_recovery_pending')) ORDER BY i.rowid LIMIT 100`;
    }, mapError("workspace")),
    cancelAbsentScript: Effect.fn("WorkspaceRetirement.cancelAbsentScript")(function* (
      event,
      holder,
    ) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows =
            yield* sql`UPDATE jones_workspace_retirement_intents SET script_state='cancelled',script_completed_at=${DateTime.formatIso(DateTime.nowUnsafe())}
          WHERE event_id=${event} AND script_state='running' AND EXISTS
          (SELECT 1 FROM jones_workspace_duty_claims WHERE claim_id=${settlementClaimId(event)} AND holder_json=${holder} AND released_at IS NULL) RETURNING event_id`;
          if (!rows.length) return false;
          yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${settlementClaimId(event)} AND holder_json=${holder} AND released_at IS NULL`;
          return true;
        }),
      );
    }, mapError("workspace")),
    cancelPendingScript: Effect.fn("WorkspaceRetirement.cancelPendingScript")(function* (event) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows =
            yield* sql`UPDATE jones_workspace_retirement_intents SET script_state='cancelled',script_completed_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE event_id=${event} AND script_state='pending' RETURNING event_id`;
          if (rows.length)
            yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${settlementClaimId(event)} AND released_at IS NULL`;
        }),
      );
    }, mapError("workspace")),
    startScript: Effect.fn("WorkspaceRetirement.startScript")(function* (event, recovery = false) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            script_state: string;
            worktree: string;
            thread_id: string;
            settled_at: string;
          }>`SELECT script_state,worktree,thread_id,settled_at FROM jones_workspace_retirement_intents WHERE event_id=${event}`;
          const intent = rows[0];
          if (!intent)
            return yield* new RetirementError({
              reason: "settlement_intent_missing",
              target: event,
            });
          if (intent.script_state !== "pending") return false;
          yield* writable(intent.worktree);
          const captured = yield* sql<{
            identity_json: string;
            holder_json: string;
          }>`SELECT i.identity_json,c.holder_json FROM jones_workspace_duty_identity i JOIN jones_workspace_duty_claims c ON c.claim_id=i.claim_id WHERE c.claim_id=${settlementClaimId(event)} AND c.released_at IS NULL`;
          if (recovery && !captured.length)
            return yield* new RetirementError({
              reason: "settlement_incarnation_unverified",
              target: intent.worktree,
            });
          if (captured[0]) {
            const holder = yield* Schema.decodeEffect(SettlementHolderJson)(
              captured[0].holder_json,
            );
            const currentThread =
              yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id=${intent.thread_id}
              AND json_extract(payload_json,'$.settledOverride')='settled' AND json_extract(payload_json,'$.settledAt')=${intent.settled_at}
              AND json_extract(payload_json,'$.worktreePath')=${intent.worktree} AND json_extract(payload_json,'$.projectId')=${holder.projectId}
              AND json_extract(payload_json,'$.branch') IS ${holder.branch}`;
            if (holder.eventId !== event || !currentThread.length) return false;
            const prior = yield* Schema.decodeEffect(DutyIdentityJson)(captured[0].identity_json);
            const current = yield* Effect.promise(() => dutyIdentity(intent.worktree));
            if (
              (recovery && (prior.status !== "bound" || current.status !== "bound")) ||
              (prior.status === "bound" &&
                current.status === "bound" &&
                prior.incarnation !== current.incarnation)
            )
              return yield* new RetirementError({
                reason: "settlement_incarnation_changed",
                target: intent.worktree,
              });
            const native = yield* Effect.promise(() => inProcessOwner(event));
            yield* sql`UPDATE jones_workspace_duty_identity SET native_owner_json=${yield* Schema.encodeEffect(NativeDutyOwnerJson)(native)} WHERE claim_id=${settlementClaimId(event)}`;
          }
          yield* sql`UPDATE jones_workspace_retirement_intents SET script_state='running',script_started_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE event_id=${event}`;
          return true;
        }),
      );
    }, mapError("workspace")),
    finishScript: Effect.fn("WorkspaceRetirement.finishScript")(function* (event, state, exit) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows =
            yield* sql`UPDATE jones_workspace_retirement_intents SET script_state=${state},script_exit_code=${exit},script_completed_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE event_id=${event} AND script_state='running' RETURNING event_id`;
          if (rows.length)
            yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${settlementClaimId(event)} AND released_at IS NULL`;
        }),
      );
    }, mapError("workspace")),
    outcome: Effect.fn("WorkspaceRetirement.outcome")(function* (event, result) {
      yield* sql`UPDATE jones_workspace_retirement_intents SET outcome_json=${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(result)} WHERE event_id=${event}`;
    }, mapError("workspace")),
    recoveryOutcome: Effect.fn("WorkspaceRetirement.recoveryOutcome")(function* (
      event,
      state,
      result,
    ) {
      yield* sql`UPDATE jones_workspace_retirement_intents SET outcome_json=${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(result)} WHERE event_id=${event} AND script_state=${state}`;
    }, mapError("workspace")),
    begin: Effect.fn("WorkspaceRetirement.begin")(function* (receipt) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writable(receipt.binding.path);
          if ((yield* claims(receipt.binding.path)).length)
            return yield* new RetirementError({
              reason: "live_duty",
              target: receipt.binding.path,
            });
          const previous = yield* sql<{
            incarnation_id: string;
            state: string;
          }>`SELECT incarnation_id,state FROM jones_workspace_retirement_state WHERE worktree=${receipt.binding.path}`;
          if (previous[0]?.incarnation_id === receipt.incarnation)
            return yield* new RetirementError({
              reason: "retired_incarnation_reused",
              target: receipt.binding.path,
            });
          yield* sql`INSERT INTO jones_workspace_retirement_state VALUES (${receipt.binding.path},${receipt.incarnation},'retiring',${receipt.id})
          ON CONFLICT(worktree) DO UPDATE SET incarnation_id=excluded.incarnation_id,state='retiring',receipt_id=excluded.receipt_id WHERE jones_workspace_retirement_state.state='retired'`;
          yield* sql`INSERT INTO jones_workspace_retirement_receipts VALUES (${receipt.id},${receipt.incarnation},${receipt.binding.path},'intent',${yield* Schema.encodeEffect(RetirementReceiptJson)(receipt)})`;
        }),
      );
    }, mapError("workspace")),
    finish: Effect.fn("WorkspaceRetirement.finish")(function* (receipt) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE jones_workspace_retirement_receipts SET state=${receipt.state},receipt_json=${yield* Schema.encodeEffect(RetirementReceiptJson)(receipt)} WHERE receipt_id=${receipt.id} AND (state='intent' OR state=${receipt.state})`;
          if (receipt.state === "retired") {
            yield* sql`UPDATE jones_workspace_retirement_state SET state='retired' WHERE receipt_id=${receipt.id} AND state='retiring'`;
            yield* sql`UPDATE jones_workspace_disposal_journal SET state='removed_with_checkout' WHERE receipt_id=${receipt.id} AND kind='ignored'`;
          } else if (receipt.state === "not_retired") {
            yield* sql`DELETE FROM jones_workspace_retirement_state WHERE receipt_id=${receipt.id} AND state='retiring'`;
          }
          // Partial/unknown effects keep the durable exclusion until explicit reconciliation.
        }),
      );
    }, mapError("workspace")),
    journal: Effect.fn("WorkspaceRetirement.journal")(function* (receipt, entries) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          for (const [ordinal, entry] of entries.entries())
            yield* sql`INSERT INTO jones_workspace_disposal_journal VALUES (${receipt},${ordinal},${entry.path},${entry.kind},${entry.identity},'planned')`;
        }),
      );
    }, mapError("workspace")),
    completePath: Effect.fn("WorkspaceRetirement.completePath")(function* (
      receipt,
      ordinal,
      state,
    ) {
      yield* sql`UPDATE jones_workspace_disposal_journal SET state=${state} WHERE receipt_id=${receipt} AND ordinal=${ordinal}`;
    }, mapError("workspace")),
    read: Effect.fn("WorkspaceRetirement.read")(function* (id) {
      const rows = yield* sql<{
        receipt_json: string;
      }>`SELECT receipt_json FROM jones_workspace_retirement_receipts WHERE receipt_id=${id}`;
      return rows[0]
        ? yield* Schema.decodeEffect(RetirementReceiptJson)(rows[0].receipt_json)
        : null;
    }, mapError("workspace")),
  });
});
export const layer = Layer.effect(Receipts, make);
