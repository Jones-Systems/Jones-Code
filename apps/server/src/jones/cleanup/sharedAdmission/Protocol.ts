import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical, digest, DirectoryIdentity } from "../../ordinaryCheckout/CheckoutProtocol.ts";

export { canonical, digest, DirectoryIdentity };
const id = Schema.NonEmptyString;
const ordinal = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export class AdmissionError extends Schema.TaggedError<AdmissionError>()("SharedAdmissionError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
export const failure = (reason: string) => Effect.fail(new AdmissionError({ reason }));
const isAdmissionError = Schema.is(AdmissionError);
export const storageError = (cause: unknown): AdmissionError =>
  isAdmissionError(cause) ? cause : new AdmissionError({ reason: "storage", cause });

export const PhysicalScope = Schema.Struct({
  requestedPath: id,
  canonicalPath: id,
  // Nearest existing directory first, then every physical ancestor through root.
  ancestors: Schema.NonEmptyArray(Schema.Struct({ path: id, identity: DirectoryIdentity })),
  unresolved: Schema.Array(id),
});
export type PhysicalScope = typeof PhysicalScope.Type;
export const ParticipantIdentity = Schema.Struct({
  storeId: id,
  database: DirectoryIdentity,
  nonce: id,
  coverage: id,
});
export type ParticipantIdentity = typeof ParticipantIdentity.Type;
export const OperationIdentity = Schema.Struct({
  storeId: id,
  originalOperationId: id,
  role: id,
  generation: ordinal,
});
export type OperationIdentity = typeof OperationIdentity.Type;
export const operationKey = (identity: OperationIdentity) => canonical(identity);
export const OwnerReference = Schema.Struct({
  storeId: id,
  threadId: id,
  threadBirth: id,
  projectId: id,
  ownerEpoch: id,
  targetRevision: ordinal,
});
export type OwnerReference = typeof OwnerReference.Type;
export const Slot = Schema.Struct({
  slotId: id,
  rootId: id,
  root: PhysicalScope,
  scope: PhysicalScope,
  owner: OwnerReference,
  revision: ordinal,
  phase: Schema.Literals([
    "owned",
    "removal_reserved",
    "removal_invoking",
    "removed",
    "recreation_reserved",
    "recreation_invoking",
    "materialized",
  ]),
  operation: Schema.NullOr(id),
  removalReceipt: Schema.NullOr(hash),
  successor: Schema.NullOr(PhysicalScope),
  bindingCertificate: Schema.NullOr(hash),
});
export type Slot = typeof Slot.Type;
export const SlotRequest = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("remove"),
    slotId: id,
    revision: ordinal,
    owner: OwnerReference,
    beforeImageSha256: hash,
  }),
  Schema.Struct({
    action: Schema.Literal("recreate"),
    slotId: id,
    revision: ordinal,
    owner: OwnerReference,
    removalReceipt: hash,
  }),
]);
export type SlotRequest = typeof SlotRequest.Type;
export const ReservationBinding = Schema.Struct({
  operation: OperationIdentity,
  participant: ParticipantIdentity,
  scopes: Schema.NonEmptyArray(PhysicalScope),
  kind: Schema.Literals([
    "acceptance",
    "reference",
    "runtime",
    "mutation",
    "removal",
    "recreation",
  ]),
  localReference: id,
  owner: Schema.NullOr(OwnerReference),
  slot: Schema.NullOr(SlotRequest),
});
export type ReservationBinding = typeof ReservationBinding.Type;
export const LocalCertificate = Schema.Struct({
  journalId: id,
  operationKey: id,
  bindingSha256: hash,
  receiptId: id,
  authorityRevision: id,
});
export type LocalCertificate = typeof LocalCertificate.Type;
export const TerminalEvidence = Schema.Union([
  // Retirement authority is checked by the local custody adapter, not a status string.
  Schema.Struct({
    outcome: Schema.Literal("retired"),
    certificate: LocalCertificate,
    evidenceSha256: hash,
  }),
  Schema.Struct({
    outcome: Schema.Literal("removed"),
    certificate: LocalCertificate,
    evidenceSha256: hash,
    absent: PhysicalScope,
  }),
  Schema.Struct({
    outcome: Schema.Literal("materialized"),
    certificate: LocalCertificate,
    evidenceSha256: hash,
    successor: PhysicalScope,
  }),
]);
export type TerminalEvidence = typeof TerminalEvidence.Type;
export const RecreationBindingCertificate = Schema.Struct({
  journalId: id,
  operationKey: id,
  bindingSha256: hash,
  receiptId: id,
  authorityRevision: id,
  slotId: id,
  slotRevision: ordinal,
  ownerBefore: OwnerReference,
  oldScope: PhysicalScope,
  successor: PhysicalScope,
});
export type RecreationBindingCertificate = typeof RecreationBindingCertificate.Type;
export const Reservation = Schema.Struct({
  binding: ReservationBinding,
  bindingSha256: hash,
  phase: Schema.Literals(["pending", "accepted", "entered", "unknown", "terminal"]),
  revision: ordinal,
  certificate: Schema.NullOr(LocalCertificate),
  terminal: Schema.NullOr(TerminalEvidence),
});
export type Reservation = typeof Reservation.Type;

const decodeBinding = Schema.decodeUnknownEffect(Schema.fromJsonString(ReservationBinding));

// Production must bind the actual retained checked owner and actual local SqlClient.
// There is deliberately no permissive layer and no coverage boolean.
export class Participant extends Context.Service<
  Participant,
  {
    readonly identity: ParticipantIdentity;
    readonly sql: SqlClient.SqlClient;
    readonly checkOutside: Effect.Effect<void, AdmissionError>;
    readonly validateLocal: Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Protocol/Participant") {}
export class ScopeObservation extends Context.Service<
  ScopeObservation,
  {
    readonly capture: (path: string) => Effect.Effect<PhysicalScope, AdmissionError>;
    readonly revalidate: (scope: PhysicalScope) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Protocol/ScopeObservation") {}

export class CommonJournal extends Context.Service<
  CommonJournal,
  {
    readonly id: string;
    readonly sql: SqlClient.SqlClient;
    readonly enroll: (identity: ParticipantIdentity) => Effect.Effect<void, AdmissionError>;
    readonly enrollRoot: (root: PhysicalScope) => Effect.Effect<string, AdmissionError>;
    readonly enrollSlot: (slot: Slot) => Effect.Effect<void, AdmissionError>;
    readonly readSlot: (slotId: string) => Effect.Effect<Slot | null, AdmissionError>;
    readonly read: (key: string) => Effect.Effect<Reservation | null, AdmissionError>;
    readonly reserveScopes: (
      binding: ReservationBinding,
    ) => Effect.Effect<Reservation, AdmissionError>;
    readonly commitLocalAcceptance: (
      record: Reservation,
      certificate: LocalCertificate,
    ) => Effect.Effect<Reservation, AdmissionError>;
    readonly enterOnce: (record: Reservation) => Effect.Effect<Reservation, AdmissionError>;
    readonly markUnknown: (record: Reservation) => Effect.Effect<void, AdmissionError>;
    readonly recordTerminalEvidence: (
      record: Reservation,
      evidence: TerminalEvidence,
    ) => Effect.Effect<Reservation, AdmissionError>;
    readonly bindRecreation: (
      certificate: RecreationBindingCertificate,
    ) => Effect.Effect<Slot, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Protocol/CommonJournal") {}

export class LocalBridge extends Context.Service<
  LocalBridge,
  {
    readonly validateInstallation: Effect.Effect<void, AdmissionError>;
    readonly accept: <A, E, R>(
      certificate: LocalCertificate,
      write: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AdmissionError, R>;
    readonly validate: (certificate: LocalCertificate) => Effect.Effect<void, AdmissionError>;
    readonly recordTerminal: (evidence: TerminalEvidence) => Effect.Effect<void, AdmissionError>;
    readonly commitRecreationBinding: <A, E, R>(
      certificate: RecreationBindingCertificate,
      write: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AdmissionError, R>;
    readonly validateRecreationBinding: (
      certificate: RecreationBindingCertificate,
    ) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Protocol/LocalBridge") {}

const liveBrand: unique symbol = Symbol("live-common-admission");
export type LiveAdmission = { readonly [liveBrand]: true };
interface LiveState {
  readonly fiberId: number;
  readonly localSql: SqlClient.SqlClient;
  readonly participant: Participant["Service"];
  readonly journal: CommonJournal["Service"];
  readonly bridge: LocalBridge["Service"];
  readonly observations: ScopeObservation["Service"];
  record: Reservation;
  active: boolean;
  stage: "reserved" | "accepting" | "accepted" | "checking" | "entered" | "spent";
}
const lives = new WeakMap<LiveAdmission, LiveState>();
const current = Effect.fn("SharedAdmission.current")(function* (token: LiveAdmission) {
  const state = lives.get(token);
  if (state === undefined || !state.active || state.fiberId !== (yield* Effect.fiberId))
    return yield* failure("capability_not_live_on_this_fiber");
  return state;
});
const outside = Effect.fn("SharedAdmission.outside")(function* (
  state: Pick<LiveState, "localSql" | "journal">,
) {
  for (const sql of [state.localSql, state.journal.sql])
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* failure("cross_store_or_physical_work_inside_transaction");
});
const revalidate = Effect.fn("SharedAdmission.revalidate")(function* (state: LiveState) {
  yield* outside(state);
  yield* state.participant.checkOutside;
  for (const scope of state.record.binding.scopes) yield* state.observations.revalidate(scope);
});

/** Raw local repositories call this in their existing transaction immediately
 * before the authority-creating write. Inherited child contexts confer no right. */
export const requireLocalAdmission = Effect.fn("SharedAdmission.requireLocalAdmission")(function* (
  token: LiveAdmission,
  expected: { readonly operationKey: string; readonly bindingSha256: string },
) {
  const state = yield* current(token);
  const sql = yield* SqlClient.SqlClient;
  if (
    sql !== state.localSql ||
    state.stage !== "accepting" ||
    Option.isNone(yield* Effect.serviceOption(sql.transactionService)) ||
    expected.operationKey !== operationKey(state.record.binding.operation) ||
    expected.bindingSha256 !== state.record.bindingSha256
  )
    return yield* failure("local_admission_binding_or_phase");
  yield* state.participant.validateLocal;
});

/** @public No production layer: composition must supply every checked port. */
export const reserveScopes = Effect.fn("SharedAdmission.reserveScopes")(function* (
  input: Omit<ReservationBinding, "participant">,
): Effect.fn.Return<
  LiveAdmission,
  AdmissionError,
  Participant | CommonJournal | LocalBridge | ScopeObservation | Scope.Scope
> {
  const participant = yield* Participant;
  const journal = yield* CommonJournal;
  const bridge = yield* LocalBridge;
  const observations = yield* ScopeObservation;
  // Snapshot caller-owned arrays/objects before retaining any live authority.
  const binding = yield* decodeBinding(
    canonical({ ...input, participant: participant.identity }),
  ).pipe(Effect.mapError(storageError));
  if (binding.operation.storeId !== participant.identity.storeId)
    return yield* failure("foreign_store_operation");
  yield* outside({ localSql: participant.sql, journal });
  yield* participant.checkOutside;
  yield* bridge.validateInstallation;
  for (const scope of binding.scopes) yield* observations.revalidate(scope);
  const record = yield* journal.reserveScopes(binding);
  const token: LiveAdmission = Object.freeze({ [liveBrand]: true });
  const state: LiveState = {
    fiberId: yield* Effect.fiberId,
    localSql: participant.sql,
    participant,
    journal,
    bridge,
    observations,
    record,
    active: true,
    stage: "reserved",
  };
  lives.set(token, state);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.active = false;
    }),
  );
  return token;
});

/** Shared commit precedes this local transaction; shared acknowledgement follows
 * known local commit. An error at either boundary leaves the durable fence. */
export const commitLocalAcceptance = Effect.fn("SharedAdmission.commitLocalAcceptance")(function* <
  A,
  E,
  R,
>(
  token: LiveAdmission,
  local: { readonly receiptId: string; readonly authorityRevision: string },
  write: Effect.Effect<A, E, R>,
): Effect.fn.Return<
  { readonly value: A; readonly certificate: LocalCertificate },
  E | AdmissionError,
  R
> {
  const state = yield* current(token);
  if (state.stage !== "reserved") return yield* failure("acceptance_already_attempted");
  yield* revalidate(state);
  const certificate: LocalCertificate = {
    ...local,
    journalId: state.journal.id,
    operationKey: operationKey(state.record.binding.operation),
    bindingSha256: state.record.bindingSha256,
  };
  state.stage = "accepting";
  const value = yield* state.bridge.accept(certificate, write).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        // A lost local COMMIT reply never authorizes cancellation or replay.
        state.stage = "spent";
      }),
    ),
  );
  state.record = yield* state.journal.commitLocalAcceptance(state.record, certificate);
  state.stage = "accepted";
  return { value, certificate };
});

/** CAS commits before callback entry. No SQL transaction spans the callback.
 * Callback failure/interruption stays entered/unknown until exact custody proof. */
export const enterOnce = <A, E, R>(
  token: LiveAdmission,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | AdmissionError, R> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const state = yield* current(token);
      if (state.stage !== "accepted" || state.record.certificate === null)
        return yield* failure("invocation_already_attempted_or_unaccepted");
      state.stage = "checking";
      yield* restore(revalidate(state));
      yield* restore(state.bridge.validate(state.record.certificate));
      state.record = yield* state.journal.enterOnce(state.record);
      state.stage = "entered";
      yield* restore(revalidate(state));
      // No SQL handle is retained across the caller's physical effect.
      return yield* restore(effect).pipe(
        Effect.onExit((exit) =>
          exit._tag === "Failure" ? state.journal.markUnknown(state.record) : Effect.void,
        ),
      );
    }),
  );

export const recordTerminalEvidence = Effect.fn("SharedAdmission.recordTerminalEvidence")(
  function* (token: LiveAdmission, evidence: TerminalEvidence) {
    const state = yield* current(token);
    if (state.stage !== "entered") return yield* failure("terminal_outside_original_invocation");
    yield* outside(state);
    yield* state.participant.checkOutside;
    // The local custody adapter must validate exact evidence inside this transaction.
    yield* state.bridge.recordTerminal(evidence);
    state.record = yield* state.journal.recordTerminalEvidence(state.record, evidence);
    state.stage = "spent";
    return state.record;
  },
);

/** Restart-safe publication of an already consumed local owner-rebind
 * certificate. This never returns a physical invocation capability. */
export const publishRecreationBinding = Effect.fn("SharedAdmission.publishRecreationBinding")(
  function* (certificate: RecreationBindingCertificate) {
    const participant = yield* Participant;
    const journal = yield* CommonJournal;
    const bridge = yield* LocalBridge;
    const observations = yield* ScopeObservation;
    yield* outside({ localSql: participant.sql, journal });
    yield* participant.checkOutside;
    yield* bridge.validateInstallation;
    yield* observations.revalidate(certificate.successor);
    yield* bridge.validateRecreationBinding(certificate);
    return yield* journal.bindRecreation(certificate);
  },
);

/** Reconcile retained completion evidence under a fresh checked incarnation.
 * Local custody must prove it again. This cannot mint a permit or repeat work. */
export const reconcileTerminalEvidence = Effect.fn("SharedAdmission.reconcileTerminalEvidence")(
  function* (record: Reservation, evidence: TerminalEvidence) {
    const participant = yield* Participant;
    const journal = yield* CommonJournal;
    const bridge = yield* LocalBridge;
    yield* outside({ localSql: participant.sql, journal });
    yield* participant.checkOutside;
    yield* bridge.validateInstallation;
    if (
      record.binding.participant.storeId !== participant.identity.storeId ||
      canonical(record.binding.participant.database) !== canonical(participant.identity.database)
    )
      return yield* failure("foreign_store_reconciliation");
    yield* bridge.recordTerminal(evidence);
    return yield* journal.recordTerminalEvidence(record, evidence);
  },
);
