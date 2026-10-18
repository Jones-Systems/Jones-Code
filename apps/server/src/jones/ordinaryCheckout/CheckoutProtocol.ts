// @effect-diagnostics nodeBuiltinImport:off -- Durable identities use SHA-256 of canonical, decoded JSON.
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Schema from "effect/Schema";

export class CheckoutError extends Schema.TaggedError<CheckoutError>()("CheckoutError", {
  reason: Schema.Literals([
    "unavailable",
    "identity",
    "conflict",
    "replay",
    "stale",
    "unknown",
    "storage",
  ]),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Ordinary checkout refused: ${this.reason}.`;
  }
}

export const canonical = importedHistoryCanonicalJson;
export const digest = (value: unknown): string =>
  NodeCrypto.createHash("sha256").update(canonical(value)).digest("hex");
const id = Schema.NonEmptyString;
const ordinal = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const Utc = Schema.String.check(
  Schema.makeFilter((s) => {
    const time = DateTime.make(s);
    return Option.isSome(time) && DateTime.formatIso(time.value) === s;
  }),
);
export const DirectoryIdentity = Schema.Struct({
  version: Schema.Literal(1),
  device: id,
  inode: id,
  birth: id,
});
export const Target = Schema.Struct({
  version: Schema.Literal(1),
  projectId: id,
  threadId: id,
  projectRoot: id,
  worktreePath: Schema.NullOr(id),
  threadBranch: Schema.NullOr(id),
  projectRevision: id,
  threadRevision: id,
  canonicalProjectRoot: id,
  cwd: id,
  checkoutKey: id,
  directoryIdentity: DirectoryIdentity,
  gitCommonDirectory: Schema.NullOr(id),
  gitDirectory: Schema.NullOr(id),
  gitCommonIdentity: Schema.NullOr(DirectoryIdentity),
  gitDirectoryIdentity: Schema.NullOr(DirectoryIdentity),
  branch: Schema.NullOr(id),
});
export type Target = typeof Target.Type;
export const Birth = Schema.Struct({ eventId: id, sequence: ordinal });
export type Birth = typeof Birth.Type;
export const Origin = Schema.Struct({
  kind: Schema.Literals(["command", "queue", "delegated", "scheduled", "prepared"]),
  parentThreadId: Schema.NullOr(id),
  requestId: Schema.NullOr(id),
});
export const Relation = Schema.Struct({
  runId: Schema.NullOr(id),
  attemptId: Schema.NullOr(id),
  nodeId: Schema.NullOr(id),
  messageId: Schema.NullOr(id),
  checkpointScopeId: Schema.NullOr(id),
});
export type Relation = typeof Relation.Type;
export const Source = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("outbox"),
    effectId: id,
    workerId: id,
    attempt: ordinal,
    claimExpiresAt: Utc,
  }),
  Schema.Struct({ kind: Schema.Literals(["prepared_run", "prepared_launch"]), producerId: id }),
]);
export type Source = typeof Source.Type;
export const Binding = Schema.Struct({
  version: Schema.Literal(1),
  operationId: id,
  admissionId: id,
  executor: Schema.Literals([
    "outbox",
    "prepared",
    "managed_run",
    "background",
    "checkpoint",
    "transition",
  ]),
  issuerId: id,
  source: Source,
  relation: Relation,
  provider: Schema.NullOr(
    Schema.Struct({
      instanceId: id,
      sessionId: id,
      threadId: id,
      driver: id,
      generation: id,
      evidenceRevision: ordinal,
    }),
  ),
});
export type Binding = typeof Binding.Type;
export const Evidence = Schema.Struct({
  version: Schema.Literal(1),
  issuerId: id,
  reference: id,
  outcome: Schema.Literals(["active", "quiescent", "not_invoked", "unknown"]),
  coverage: Schema.Literals(["complete", "partial", "unknown"]),
  observedAt: Utc,
  transition: Schema.optionalKey(
    Schema.Struct({
      beforeEpoch: ordinal,
      afterEpoch: ordinal,
      beforeLease: id,
      afterLease: id,
      beforeThread: id,
      afterThread: id,
      beforeBirth: id,
      afterBirth: id,
      beforeBranch: Schema.NullOr(id),
      afterBranch: Schema.NullOr(id),
    }),
  ),
});
export type Evidence = typeof Evidence.Type;
export const operationId = (admissionId: string, source: Source): string =>
  `checkout-use:${digest({
    admissionId,
    source:
      source.kind === "outbox"
        ? { kind: source.kind, effectId: source.effectId, attempt: source.attempt }
        : source,
  })}`;
export const associationId = (binding: Binding): string => digest(binding);
export type UseState = "reserved" | "started" | "draining" | "completed" | "no_effect" | "unknown";
export type EventKind =
  | "bind"
  | "activate"
  | "join"
  | "renew"
  | "retire"
  | "unknown"
  | "target_transition"
  | "release"
  | "handoff";
export const terminal = (state: UseState): boolean =>
  state === "completed" || state === "no_effect";
export interface Owner {
  readonly checkout_key: string;
  readonly project_root: string;
  readonly directory_identity_json: string;
  readonly git_common_directory: string | null;
  readonly lease_id: string;
  readonly owner_thread_id: string;
  readonly birth_event_id: string;
  readonly birth_sequence: number;
  readonly branch: string | null;
  readonly acquired_at: string;
  readonly renewed_at: string;
  readonly expires_at: string;
  readonly owner_epoch: number;
  readonly target_revision: number;
  readonly revision: number;
  readonly state: "owned" | "unknown" | "released";
}
export interface Admission {
  readonly admission_id: string;
  readonly command_id: string;
  readonly receipt_thread_id: string;
  readonly subject_thread_id: string;
  readonly command_type: string;
  readonly command_digest: string;
  readonly origin_kind: string;
  readonly origin_parent_thread_id: string | null;
  readonly request_id: string | null;
  readonly checkout_key: string;
  readonly lease_id: string;
  readonly owner_epoch: number;
  readonly target_revision: number;
  readonly birth_event_id: string;
  readonly birth_sequence: number;
  readonly receipt_sequence: number;
  readonly basis_event_id: string;
  readonly basis_sequence: number;
  readonly run_id: string | null;
  readonly attempt_id: string | null;
  readonly node_id: string | null;
  readonly message_id: string | null;
  readonly checkpoint_scope_id: string | null;
  readonly recorded_at: string;
}
export interface Use {
  readonly operation_id: string;
  readonly admission_id: string;
  readonly checkout_key: string;
  readonly lease_id: string;
  readonly owner_epoch: number;
  readonly source_kind: Source["kind"];
  readonly effect_id: string | null;
  readonly worker_id: string | null;
  readonly attempt_count: number | null;
  readonly claim_expires_at: string | null;
  readonly producer_id: string | null;
  readonly state: UseState;
  readonly revision: number;
  readonly last_event_ordinal: number;
  readonly started_at: string | null;
  readonly settled_at: string | null;
}
export interface LifecycleEvent {
  readonly operation_id: string;
  readonly ordinal: number;
  readonly predecessor_ordinal: number;
  readonly association_id: string;
  readonly event_kind: EventKind;
  readonly binding_json: string;
  readonly evidence_json: string;
  readonly recorded_at: string;
}
