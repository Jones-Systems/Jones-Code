// @effect-diagnostics nodeBuiltinImport:off -- Exact retained command and forwarding bytes define the dispatch identity.
import * as NodeCrypto from "node:crypto";
import type * as Wire from "@t3tools/contracts";
import type * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import type { WorkstreamsPeerSessionPinV1 } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import type { ExternalIntent } from "../domain/state.ts";
import { RegistryError } from "../registry/session.ts";
import * as Envelope from "../runtime/peerEffectEnvelope.ts";
import * as Origin from "../runtime/peerEffectOrigin.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Boundary from "./peerWriteAdmission.ts";
import * as Qualification from "./sourceForwardQualification.ts";

const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const refused = () => new RegistryError({ code: "not_granted" });
declare const ticketBrand: unique symbol;
/** Only the newly committed acceptance callback can hold this single-use ticket. */
export interface InitialPeerTicket {
  readonly [ticketBrand]: true;
  readonly command_id: string;
}
export class InitialPeerExecution extends Context.Service<
  InitialPeerExecution,
  {
    readonly ready: () => Effect.Effect<void, RegistryError>;
    readonly run: (
      ticket: InitialPeerTicket,
    ) => Effect.Effect<Wire.WorkstreamReceipt | null, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/peerEffectAdmission/InitialPeerExecution") {}
interface Acceptance {
  active: boolean;
  readonly sql: SqlClient.SqlClient;
  readonly request: Forward.WorkstreamsForwardRequest;
  readonly wire: string;
  readonly admission: Boundary.HomeWriteAdmission;
  readonly pin: WorkstreamsPeerSessionPinV1;
  readonly source: Effect.Success<ReturnType<typeof Qualification.capturePeerEffectSource>>;
  readonly check: () => Effect.Effect<void, RegistryError>;
  readonly checkOriginal: () => Effect.Effect<void, RegistryError>;
  reserved: Envelope.StoredPeerEffectV2 | null;
}
interface TicketFacts {
  readonly acceptance: Acceptance;
  readonly envelope: Envelope.StoredPeerEffectV2;
  started: boolean;
  used: boolean;
}
const acceptances = new WeakMap<object, Acceptance>();
const tickets = new WeakMap<InitialPeerTicket, TicketFacts>();
const AcceptanceScope = Context.Reference<object | undefined>(
  "jones/workstreams/native/PeerEffectAcceptanceScope",
  { defaultValue: () => undefined },
);
const current = Effect.fn("PeerEffectAdmission.current")(function* () {
  const token = yield* AcceptanceScope;
  const value = token ? acceptances.get(token) : undefined;
  const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (!value?.active || Option.isNone(selected) || value.sql !== selected.value)
    return yield* refused();
  yield* value.check();
  return value;
});
export const assertPeerEffectAcceptance = current;

/** Called only by Registry's new external reservation branch inside the outer acceptance writer. */
export const wrapPeerEffectIfAdmitted = Effect.fn("PeerEffectAdmission.wrap")(function* (
  intent: ExternalIntent,
  caller: AdmittedCaller,
  body: string,
) {
  if ((yield* AcceptanceScope) === undefined) return null;
  const a = yield* current();
  const request = a.request;
  if (
    request.kind !== "command" ||
    request.request.evidence ||
    request.request.body !== body ||
    a.admission.caller !== caller ||
    request.identity.command_ids.length !== 1 ||
    request.identity.command_ids[0] !== intent.command.command_id ||
    request.identity.request_sha256 !== intent.request_sha256 ||
    hash(body) !== intent.request_sha256 ||
    intent.actor.principal_id !== request.identity.principal_id ||
    (intent.kind !== "verify_reference" &&
      intent.kind !== "native_t3_settlement" &&
      intent.kind !== "refresh_linked_pr")
  )
    return yield* refused();
  return yield* Envelope.wrapPeerEffectIntent(
    canonical(intent),
    body,
    intent.request_sha256,
    {
      schema: "jones-workstreams-peer-effect-recovery/2",
      kind: "source-recorded-forward",
      incoming_peer_pin_id: a.pin.pin_id,
      incoming_peer_pin_revision: a.pin.revision,
      source_readmission: {
        contract: "workstreams-source-readmission/2.0.0",
        accepted_forward: request.identity,
        command_id: intent.command.command_id,
        command_request_sha256: intent.request_sha256,
      },
      original_ceiling: a.source.ceiling,
    },
    hash(a.wire),
    a.source.generation,
  ).pipe(Effect.mapError(() => refused()));
});
export const markPeerEffectReserved = Effect.fn("PeerEffectAdmission.reserved")(function* (
  envelope: Envelope.StoredPeerEffectV2,
) {
  const a = yield* current();
  if (a.reserved !== null) return yield* refused();
  a.reserved = envelope;
});
export const checkPeerEffectReserved = Effect.fn("PeerEffectAdmission.checkReserved")(function* (
  commandId: string,
) {
  const a = yield* current();
  if (!a.reserved || a.reserved.recovery.source_readmission.command_id !== commandId)
    return yield* refused();
});

/** Keeps the original authenticator registration and private admission live across commit and dispatch.
 * An ambient writer refuses before any lookup. Replays never mark a new reservation or receive a ticket.
 */
export const withPeerEffectAcceptance = Effect.fn("PeerEffectAdmission.withAcceptance")(function* <
  A,
  E,
  R,
  B,
  E2,
  R2,
>(
  request: Forward.WorkstreamsForwardRequest,
  wire: string,
  admission: Boundary.HomeWriteAdmission,
  pin: WorkstreamsPeerSessionPinV1,
  qualified: {
    readonly forward_request_sha256: string;
    readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
  },
  accept: Effect.Effect<A, E, R>,
  use: (result: A, ticket: InitialPeerTicket | null) => Effect.Effect<B, E2, R2>,
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new RegistryError({ code: "preparation_in_transaction" });
  const invocation = yield* Mcp.McpInvocationContext;
  const native = yield* Pins.NativePeerSessions;
  const registration = yield* native.current(invocation).pipe(Effect.mapError(() => refused()));
  const boundary = yield* Boundary.PeerWriteBoundary;
  const source = yield* Qualification.capturePeerEffectSource(qualified, request, pin);
  const checkOriginal = Effect.fn("PeerEffectAdmission.checkOriginal")(function* () {
    const selected = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (Option.isNone(selected) || selected.value !== invocation) return yield* refused();
    yield* registration.checkCurrent().pipe(Effect.mapError(() => refused()));
    yield* source.checkCurrent();
  });
  const check = Effect.fn("PeerEffectAdmission.checkCurrent")(function* () {
    yield* checkOriginal();
    yield* boundary.checkCurrent(admission);
  });
  yield* check();
  const a: Acceptance = {
    active: true,
    sql,
    request: structuredClone(request),
    wire,
    admission,
    pin: structuredClone(pin),
    source,
    check,
    checkOriginal,
    reserved: null,
  };
  const token = Object.freeze({});
  acceptances.set(token, a);
  return yield* Effect.gen(function* () {
    const result = yield* accept;
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) return yield* refused();
    yield* check();
    if (!a.reserved) return yield* use(result, null);
    const ticket = Object.freeze({
      command_id: a.reserved.recovery.source_readmission.command_id,
    }) as InitialPeerTicket;
    tickets.set(ticket, { acceptance: a, envelope: a.reserved, started: false, used: false });
    return yield* use(result, ticket);
  }).pipe(
    Effect.provideService(AcceptanceScope, token),
    Effect.ensuring(
      Effect.sync(() => {
        a.active = false;
      }),
    ),
  );
});

/** Consume immediately before the original fixed provider call. No persisted value can mint this proof. */
const validateTicket = Effect.fn("PeerEffectAdmission.validateTicket")(function* (
  ticket: InitialPeerTicket,
  origin: Origin.ValidatedPeerReservation,
  writerAllowed = false,
) {
  const f = tickets.get(ticket);
  const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(selected)) return yield* refused();
  const sql = selected.value;
  if (
    !f ||
    !f.acceptance.active ||
    f.acceptance.sql !== sql ||
    (!writerAllowed && Option.isSome(yield* Effect.serviceOption(sql.transactionService))) ||
    ticket.command_id !== origin.command_id ||
    canonical(f.envelope) !== canonical(origin.envelope) ||
    origin.envelope.forward_request_sha256 !== hash(f.acceptance.wire)
  )
    return yield* refused();
  yield* Origin.checkOrigin(origin).pipe(Effect.mapError(() => refused()));
  // Origin and the source proof own fresh receiving SQL/owner authorization here.
  // The original live registration remains mandatory; do not reuse its outer owner-scope token
  // inside the separately prepared origin scope or substitute a new authenticator registration.
  yield* f.acceptance.checkOriginal();
  return f;
});
export const beginInitialPeerTicket = Effect.fn("PeerEffectAdmission.begin")(function* (
  ticket: InitialPeerTicket,
  origin: Origin.ValidatedPeerReservation,
) {
  const f = yield* validateTicket(ticket, origin);
  if (f.started) return yield* refused();
  yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
  f.started = true;
});
/** This extra live request fence joins fresh retained receiver/source authorization at completion. */
export const checkInitialPeerCurrent = Effect.fn("PeerEffectAdmission.checkInitialCurrent")(
  function* (ticket: InitialPeerTicket, origin: Origin.ValidatedPeerReservation) {
    const f = yield* validateTicket(ticket, origin, true);
    if (!f.started) return yield* refused();
  },
);
export const checkInitialPeerTicket = Effect.fn("PeerEffectAdmission.checkTicket")(function* (
  ticket: InitialPeerTicket,
  origin: Origin.ValidatedPeerReservation,
) {
  const f = yield* validateTicket(ticket, origin);
  if (!f.started || f.used) return yield* refused();
  yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
});
export const claimInitialPeerTicket = Effect.fn("PeerEffectAdmission.claim")(function* (
  ticket: InitialPeerTicket,
  origin: Origin.ValidatedPeerReservation,
) {
  yield* checkInitialPeerTicket(ticket, origin);
  tickets.get(ticket)!.used = true;
});
