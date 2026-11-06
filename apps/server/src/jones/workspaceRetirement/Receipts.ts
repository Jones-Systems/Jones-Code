import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { Binding } from "./OutputAttestation.ts";
import type { QualificationResult } from "./Qualification.ts";
import type { SettlementScriptState } from "./SettlementAdmission.ts";

export class RetirementError extends Schema.TaggedError<RetirementError>()("RetirementError", {
  reason: Schema.String, target: Schema.String, cause: Schema.optional(Schema.Defect()),
}) {
  override get message():string {return `Checkout retirement held (${this.reason}): ${this.target}.`;}
}
export interface RetirementReceipt {
  schema: "worktree-retirement/v1"; id: string; cause: "settlement" | "sweep" | "deletion" | "owner_action";
  state: "intent" | "retired" | "not_retired" | "partial" | "unknown";
  binding: Binding; incarnation: string; qualification: QualificationResult;
  coverage: unknown; preservationRefs: { ref: string; head: string }[];
  journal: string; disposed: string[]; reasons: string[]; limits: readonly string[];
  bytes: { logical: number | null; allocated: number | null; statvfs_delta: number | null };
}
export interface SettlementIntent {
  event_id: string; thread_id: string; worktree: string; settled_at: string;
  script_state: SettlementScriptState; script_started_at: string | null;
  script_completed_at: string | null; script_exit_code: number | null;
}
export interface JournalEntry {
  path: string; kind: "untracked" | "ignored"; identity: string;
  state: "planned" | "invoking" | "completed" | "removed_with_checkout";
}
const mapError = (target: string) => Effect.mapError((cause: unknown) => Schema.is(RetirementError)(cause) ? cause : new RetirementError({ reason: "storage_unavailable", target, cause }));
export class Receipts extends Context.Service<Receipts, {
  readonly writable: (worktree: string) => Effect.Effect<void, RetirementError>;
  readonly claims: (worktree: string) => Effect.Effect<readonly { claim_id: string; kind: string; owner: string }[], RetirementError>;
  readonly claim: (input: { id: string; worktree: string; kind: string; owner: string; holder: string }) => Effect.Effect<void, RetirementError>;
  readonly releaseClaim: (id: string) => Effect.Effect<void, RetirementError>;
  readonly reconcileClaim: (id: string, holder: string, verifiedAbsent: boolean) => Effect.Effect<void, RetirementError>;
  readonly settlement: (thread: string, worktree: string, settledAt: string) => Effect.Effect<SettlementIntent | null, RetirementError>;
  readonly startScript: (event: string) => Effect.Effect<boolean, RetirementError>;
  readonly finishScript: (event: string, state: Exclude<SettlementScriptState, "pending" | "running">, exit: number | null) => Effect.Effect<void, RetirementError>;
  readonly outcome: (event: string, result: unknown) => Effect.Effect<void, RetirementError>;
  readonly begin: (receipt: RetirementReceipt) => Effect.Effect<void, RetirementError>;
  readonly finish: (receipt: RetirementReceipt) => Effect.Effect<void, RetirementError>;
  readonly journal: (receipt: string, entries: readonly JournalEntry[]) => Effect.Effect<void, RetirementError>;
  readonly completePath: (receipt: string, ordinal: number, state: JournalEntry["state"]) => Effect.Effect<void, RetirementError>;
  readonly read: (receipt: string) => Effect.Effect<RetirementReceipt | null, RetirementError>;
}>()("jones/workspaceRetirement/Receipts") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const writable = Effect.fn("WorkspaceRetirement.writable")(function* (worktree: string) {
    const rows = yield* sql<{state:string}>`SELECT state FROM jones_workspace_retirement_state WHERE worktree=${worktree}`;
    if (rows[0]) return yield* new RetirementError({ reason: rows[0].state, target: worktree });
  }, mapError("workspace"));
  const claims = Effect.fn("WorkspaceRetirement.claims")(function* (worktree: string) {
    return yield* sql<{ claim_id: string; kind: string; owner: string }>`SELECT claim_id,kind,owner FROM jones_workspace_duty_claims WHERE worktree=${worktree} AND released_at IS NULL`;
  }, mapError("workspace"));
  return Receipts.of({
    writable, claims,
    claim: Effect.fn("WorkspaceRetirement.claim")(function* (input) {
      yield* sql.withTransaction(Effect.gen(function* () {
        yield* writable(input.worktree);
        const prior = yield* sql<{ worktree: string; holder_json: string; released_at: string | null }>`SELECT worktree,holder_json,released_at FROM jones_workspace_duty_claims WHERE claim_id=${input.id}`;
        if (prior[0]) {
          if (prior[0].worktree !== input.worktree || prior[0].holder_json !== input.holder || prior[0].released_at !== null)
            return yield* new RetirementError({ reason: "claim_identity_changed", target: input.worktree });
          return;
        }
        const known = yield* sql<{ incarnation_id: string }>`SELECT incarnation_id FROM jones_workspace_provenance WHERE worktree=${input.worktree} ORDER BY rowid DESC LIMIT 1`;
        yield* sql`INSERT INTO jones_workspace_duty_claims VALUES (${input.id},${input.worktree},${known[0]?.incarnation_id ?? null},${input.kind},${input.owner},${input.holder},${DateTime.formatIso(DateTime.nowUnsafe())},NULL)`;
      }));
    }, mapError("workspace")),
    releaseClaim: Effect.fn("WorkspaceRetirement.releaseClaim")(function* (id) {
      yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${id} AND released_at IS NULL`;
    }, mapError("workspace")),
    reconcileClaim: Effect.fn("WorkspaceRetirement.reconcileClaim")(function* (id, holder, verifiedAbsent) {
      if (!verifiedAbsent) return yield* new RetirementError({ reason: "claim_holder_absence_unverified", target: id });
      yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE claim_id=${id} AND holder_json=${holder} AND released_at IS NULL`;
    }, mapError("workspace")),
    settlement: Effect.fn("WorkspaceRetirement.settlement")(function* (thread, worktree, settledAt) {
      const rows = yield* sql<SettlementIntent>`SELECT * FROM jones_workspace_retirement_intents WHERE thread_id=${thread} AND worktree=${worktree} AND settled_at=${settledAt} ORDER BY rowid DESC LIMIT 1`;
      return rows[0] ?? null;
    }, mapError("workspace")),
    startScript: Effect.fn("WorkspaceRetirement.startScript")(function* (event) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const rows = yield* sql<{ script_state: string; worktree: string }>`SELECT script_state,worktree FROM jones_workspace_retirement_intents WHERE event_id=${event}`;
        if (!rows[0]) return yield* new RetirementError({ reason: "settlement_intent_missing", target: event });
        if (rows[0].script_state !== "pending") return false;
        yield* writable(rows[0].worktree);
        yield* sql`UPDATE jones_workspace_retirement_intents SET script_state='running',script_started_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE event_id=${event}`;
        return true;
      }));
    }, mapError("workspace")),
    finishScript: Effect.fn("WorkspaceRetirement.finishScript")(function* (event, state, exit) {
      yield* sql`UPDATE jones_workspace_retirement_intents SET script_state=${state},script_exit_code=${exit},script_completed_at=${DateTime.formatIso(DateTime.nowUnsafe())} WHERE event_id=${event} AND script_state='running'`;
    }, mapError("workspace")),
    outcome: Effect.fn("WorkspaceRetirement.outcome")(function* (event, result) {
      yield* sql`UPDATE jones_workspace_retirement_intents SET outcome_json=${JSON.stringify(result)} WHERE event_id=${event}`;
    }, mapError("workspace")),
    begin: Effect.fn("WorkspaceRetirement.begin")(function* (receipt) {
      yield* sql.withTransaction(Effect.gen(function* () {
        yield* writable(receipt.binding.path);
        if ((yield* claims(receipt.binding.path)).length) return yield* new RetirementError({ reason: "live_duty", target: receipt.binding.path });
        yield* sql`INSERT INTO jones_workspace_retirement_state VALUES (${receipt.binding.path},${receipt.incarnation},'retiring',${receipt.id})`;
        yield* sql`INSERT INTO jones_workspace_retirement_receipts VALUES (${receipt.id},${receipt.incarnation},${receipt.binding.path},'intent',${JSON.stringify(receipt)})`;
      }));
    }, mapError("workspace")),
    finish: Effect.fn("WorkspaceRetirement.finish")(function* (receipt) {
      yield* sql.withTransaction(Effect.gen(function* () {
        yield* sql`UPDATE jones_workspace_retirement_receipts SET state=${receipt.state},receipt_json=${JSON.stringify(receipt)} WHERE receipt_id=${receipt.id} AND (state='intent' OR state=${receipt.state})`;
        if (receipt.state === "retired") {
          yield* sql`UPDATE jones_workspace_retirement_state SET state='retired' WHERE receipt_id=${receipt.id} AND state='retiring'`;
          yield* sql`UPDATE jones_workspace_disposal_journal SET state='removed_with_checkout' WHERE receipt_id=${receipt.id} AND kind='ignored'`;
        } else if (receipt.state === "not_retired") {
          yield* sql`DELETE FROM jones_workspace_retirement_state WHERE receipt_id=${receipt.id} AND state='retiring'`;
        }
        // Partial/unknown effects keep the durable exclusion until explicit reconciliation.
      }));
    }, mapError("workspace")),
    journal: Effect.fn("WorkspaceRetirement.journal")(function* (receipt, entries) {
      yield* sql.withTransaction(Effect.gen(function* () {
        for (const [ordinal, entry] of entries.entries())
          yield* sql`INSERT INTO jones_workspace_disposal_journal VALUES (${receipt},${ordinal},${entry.path},${entry.kind},${entry.identity},'planned')`;
      }));
    }, mapError("workspace")),
    completePath: Effect.fn("WorkspaceRetirement.completePath")(function* (receipt, ordinal, state) {
      yield* sql`UPDATE jones_workspace_disposal_journal SET state=${state} WHERE receipt_id=${receipt} AND ordinal=${ordinal}`;
    }, mapError("workspace")),
    read: Effect.fn("WorkspaceRetirement.read")(function* (id) {
      const rows = yield* sql<{ receipt_json: string }>`SELECT receipt_json FROM jones_workspace_retirement_receipts WHERE receipt_id=${id}`;
      return rows[0] ? JSON.parse(rows[0].receipt_json) as RetirementReceipt : null;
    }, mapError("workspace")),
  });
});
export const layer = Layer.effect(Receipts, make);
