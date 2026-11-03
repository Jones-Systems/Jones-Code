// @effect-diagnostics nodeBuiltinImport:off -- Bind provider requests to exact retained intent bytes.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Carrier from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import type { WorkstreamsTransportCeiling as Ceiling } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Origin from "../runtime/peerEffectOrigin.ts";
import * as Receiving from "../runtime/retainedPeerSession.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import { RegistryError } from "../registry/session.ts";
import { validateAttestation } from "../providers/localAttestation.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";

const decodeEnvironment = Schema.decodeUnknownEffect(Wire.EnvironmentId);
const decodeParent = Schema.decodeUnknownEffect(
  Parent.WorkstreamsSourceParentReadmissionResponseV3,
);
const decodeCarrier = Schema.decodeUnknownEffect(Carrier.WorkstreamsRetainedSourceEffectRequestV1);
const decodeResult = Schema.decodeUnknownEffect(Carrier.WorkstreamsRetainedSourceEffectResultV1);
const decodeIntentJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const deepFreeze = <A>(value: A): A => {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
};
const isRegistryError = Schema.is(RegistryError);
const decodeNativeIdentity = Schema.decodeUnknownEffect(Wire.WorkstreamsNativeIdentity);
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
const lower = <A extends keyof typeof rank>(a: A, b: A): A => (rank[a] <= rank[b] ? a : b);
const narrow = (a: Ceiling, b: Ceiling): Ceiling => ({
  access:
    a.access === "read-only" || b.access === "read-only" ? "read-only" : lower(a.access, b.access),
  runtime_mode: lower(a.runtime_mode, b.runtime_mode),
  interaction_mode:
    a.interaction_mode === "plan" || b.interaction_mode === "plan" ? "plan" : "default",
  scopes: a.scopes.filter((s) => b.scopes.includes(s)),
  permissions: a.permissions.filter((p) => b.permissions.includes(p)),
});
const refused = (code = "not_granted") => new RegistryError({ code });
declare const sourceBrand: unique symbol;
/** A callback-scoped observation, never persisted as authorization or decoded from tool input. */
export interface PreparedPeerSource {
  readonly [sourceBrand]: true;
  readonly ceiling: Ceiling;
}
declare const resultBrand: unique symbol;
export interface AuthenticatedPeerResult {
  readonly [resultBrand]: true;
  readonly response: Carrier.WorkstreamsRetainedSourceEffectResultV1;
}
export type PeerSourceLookup =
  | { readonly kind: "retained"; readonly evidence: AuthenticatedPeerResult }
  | {
      readonly kind: "observation";
      readonly response: Carrier.WorkstreamsRetainedSourceEffectResultV1;
    };
interface ResultFacts {
  readonly source: PreparedPeerSource;
  readonly origin: Origin.ValidatedPeerReservation;
}
const resultFacts = new WeakMap<AuthenticatedPeerResult, ResultFacts>();
/** Allows owner-fenced encrypted retention, never projection authorization or client release. */
export const checkAuthenticatedPeerResult = Effect.fn("PeerEffectSource.checkResult")(function* (
  evidence: AuthenticatedPeerResult,
  origin: Origin.ValidatedPeerReservation,
) {
  const r = resultFacts.get(evidence);
  const f = r ? facts.get(r.source) : undefined;
  if (!r || !f?.active || r.origin !== origin || (yield* SourceScope) !== f.token)
    return yield* refused();
  yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
});
interface Facts {
  readonly origin: Origin.ValidatedPeerReservation;
  readonly receiving: Receiving.CurrentRetainedPeer;
  active: boolean;
  used: boolean;
  readonly token: object;
  readonly check: () => Effect.Effect<void, RegistryError>;
  readonly lookup: () => Effect.Effect<PeerSourceLookup, RegistryError>;
}
const facts = new WeakMap<PreparedPeerSource, Facts>();
const SourceScope = Context.Reference<object | undefined>(
  "jones/workstreams/native/PeerEffectSourceScope",
  {
    defaultValue: () => undefined,
  },
);
export const checkPeerSource = Effect.fn("PeerEffectSource.check")(function* (
  proof: PreparedPeerSource,
) {
  const f = facts.get(proof);
  if (!f?.active || (yield* SourceScope) !== f.token) return yield* refused();
  yield* f.check();
});
/** Verify the exact authoritative command and original receiver associated with this observation. */
export const checkPeerSourceOrigin = Effect.fn("PeerEffectSource.checkOrigin")(function* (
  proof: PreparedPeerSource,
  origin: Origin.ValidatedPeerReservation,
  receiving: Receiving.CurrentRetainedPeer,
) {
  yield* checkPeerSource(proof);
  const f = facts.get(proof)!;
  if (f.origin !== origin || f.receiving !== receiving) return yield* refused();
});
/** Only fixed attest/settle_lookup calls; this API has no dispatch operation or arbitrary request input. */
export const lookupPeerSourceEffect = Effect.fn("PeerEffectSource.lookup")(function* (
  proof: PreparedPeerSource,
) {
  yield* checkPeerSource(proof);
  const f = facts.get(proof)!;
  if (f.used) return yield* refused("source_observation_consumed");
  f.used = true;
  return yield* f.lookup();
});
export class PeerEffectSource extends Context.Service<
  PeerEffectSource,
  {
    readonly withReadmission: <A, E, R>(
      origin: Origin.ValidatedPeerReservation,
      receiving: Receiving.CurrentRetainedPeer,
      use: (source: PreparedPeerSource) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | RegistryError, R>;
  }
>()("t3/jones/workstreams/native/forwarding/peerEffectSource") {}
export const layer = Layer.effect(
  PeerEffectSource,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const peer = yield* Peer.PeerMcpClient;
    const links = yield* Links.PeerLinks;
    return PeerEffectSource.of({
      withReadmission: Effect.fn("PeerEffectSource.withReadmission")(function* <A, E, R>(
        origin: Origin.ValidatedPeerReservation,
        receiving: Receiving.CurrentRetainedPeer,
        use: (source: PreparedPeerSource) => Effect.Effect<A, E, R>,
      ) {
        const outcome = yield* Effect.gen(function* () {
          if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
            return yield* refused("preparation_in_transaction");
          yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
          yield* Receiving.checkRetainedPeer(receiving).pipe(Effect.mapError(() => refused()));
          const retained = origin.envelope;
          const recovery = retained.recovery;
          const identity = recovery.source_readmission.accepted_forward;
          if (
            receiving.pin.pin_id !== recovery.incoming_peer_pin_id ||
            receiving.pin.revision !== recovery.incoming_peer_pin_revision ||
            !same(receiving.pin.expected_home, identity.expected_home) ||
            !same(receiving.pin.source_authority, identity.source_authority)
          )
            return yield* refused();
          const environmentId = yield* decodeEnvironment(
            identity.source_authority.environment_id,
          ).pipe(Effect.mapError(() => refused()));
          const generation = retained.source_link_generation;
          const checkLocal = Effect.fn("PeerEffectSource.checkLocal")(function* () {
            yield* Origin.checkOrigin(origin).pipe(Effect.mapError(() => refused()));
            yield* Receiving.checkRetainedPeer(receiving).pipe(Effect.mapError(() => refused()));
            const current = yield* links
              .currentGeneration(environmentId)
              .pipe(Effect.mapError(() => refused("unavailable")));
            if (Option.isNone(current) || current.value !== generation) return yield* refused();
          });
          yield* checkLocal();
          const request: Parent.WorkstreamsSourceParentReadmissionRequestV3 = {
            contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
            accepted_forward: structuredClone(identity),
            receiving_home_pin_id: receiving.pin.pin_id,
            receiving_home_pin_revision: receiving.pin.revision,
          };
          const sent = canonical(request);
          const response = yield* peer
            .call({
              environmentId,
              expectedGeneration: generation,
              tool: "jones_workstreams_source_readmission",
              arguments: request,
              limits: {
                runtimeMode: receiving.ceiling.runtime_mode,
                interactionMode: receiving.ceiling.interaction_mode,
              },
              success: Parent.WorkstreamsSourceParentReadmissionResponseV3,
            })
            .pipe(Effect.mapError(() => refused("unavailable")));
          const result = yield* decodeParent(response, { onExcessProperty: "error" }).pipe(
            Effect.mapError(() => refused("invalid_response")),
          );
          yield* checkLocal();
          yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
          if (canonical(result.request) !== sent) return yield* refused("invalid_response");
          if (result.state !== "authorized")
            return yield* refused(result.state === "unavailable" ? "unavailable" : "not_granted");
          if (result.forward_request_sha256 !== retained.forward_request_sha256)
            return yield* refused("invalid_response");
          const ceiling = narrow(
            narrow(receiving.ceiling, result.original_ceiling),
            result.current_ceiling,
          );
          if (!Pins.ceilingAllows(ceiling, "workstreams:write")) return yield* refused();
          const lookup = Effect.fn("PeerEffectSource.lookupFixed")(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              return yield* refused("preparation_in_transaction");
            yield* checkLocal();
            yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
            const intent = origin.intent;
            const reference = intent.reference;
            const authority = identity.source_authority;
            if (
              reference.identity.provider !== "t3" ||
              reference.identity.source_instance_id !== authority.environment_id ||
              intent.source_binding.authority_namespace !== authority.authority_namespace ||
              intent.source_binding.store_generation !== authority.store_generation
            )
              return yield* refused("provider_mismatch");
            const nativeIdentity = yield* decodeNativeIdentity(reference.identity).pipe(
              Effect.mapError(() => refused("provider_mismatch")),
            );
            const base = {
              contract:
                Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1 as typeof Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
              source_readmission: recovery.source_readmission,
              receiving_home_pin_id: receiving.pin.pin_id,
              receiving_home_pin_revision: receiving.pin.revision,
            };
            let input: Carrier.WorkstreamsRetainedSourceEffectRequestV1;
            if (intent.kind === "verify_reference") {
              input = {
                ...base,
                kind: "attest",
                request: {
                  owner_id: intent.owner_id,
                  principal_id: intent.actor.principal_id,
                  identity: nativeIdentity,
                  expected_authority_namespace: authority.authority_namespace,
                  expected_store_generation: authority.store_generation,
                },
              };
            } else if (
              intent.kind === "native_t3_settlement" &&
              intent.command.action.operation === "request_native_t3_settlement"
            ) {
              // Hash the exact retained domain JSON value, including optional cache fields.
              const rawIntent = yield* decodeIntentJson(retained.intent_body).pipe(
                Effect.mapError(() => refused("invalid_response")),
              );
              input = {
                ...base,
                kind: "settle_lookup",
                request: {
                  owner_id: intent.owner_id,
                  principal_id: intent.actor.principal_id,
                  command_id: intent.command.command_id,
                  native_reference_id: reference.native_reference_id,
                  native_action: intent.command.action.native_action,
                  request_sha256: intent.request_sha256,
                  reservation_sha256: NodeCrypto.createHash("sha256")
                    .update(canonical(rawIntent))
                    .digest("hex"),
                  identity: nativeIdentity,
                  expected_authority_namespace: authority.authority_namespace,
                  expected_store_generation: authority.store_generation,
                },
              };
            } else return yield* refused("provider_unconfigured");
            const decoded = yield* decodeCarrier(input, { onExcessProperty: "error" }).pipe(
              Effect.mapError(() => refused("provider_mismatch")),
            );
            const expected = canonical(decoded);
            const reply = yield* peer
              .call({
                environmentId,
                expectedGeneration: generation,
                tool: "jones_workstreams_source_retained_effect",
                arguments: decoded,
                limits: {
                  runtimeMode: ceiling.runtime_mode,
                  interactionMode: ceiling.interaction_mode,
                },
                success: Carrier.WorkstreamsRetainedSourceEffectResultV1,
              })
              .pipe(Effect.mapError(() => refused("unavailable")));
            const checked = yield* decodeResult(reply, { onExcessProperty: "error" }).pipe(
              Effect.mapError(() => refused("invalid_response")),
            );
            yield* Origin.checkOriginSnapshot(origin).pipe(Effect.mapError(() => refused()));
            if (canonical(checked.request) !== expected) return yield* refused("invalid_response");
            if (
              checked.state === "result" &&
              (checked.result.state === "terminal" || checked.result.state === "attested")
            ) {
              if (checked.request.kind === "attest") {
                yield* validateAttestation(checked.request.request, checked.result).pipe(
                  Effect.mapError(() => refused("invalid_response")),
                );
              } else if (
                checked.result.state !== "terminal" ||
                !same(
                  checked.result.result,
                  nativeSettlementResult(intent, checked.result.result.native_outcome),
                )
              )
                return yield* refused("invalid_response");
              // The authenticated original transport may finish after receiver/source permission changes.
              // Retain successful evidence under the actual owner; completion must freshly readmit.
              const evidence = Object.freeze({
                response: deepFreeze(structuredClone(checked)),
              }) as AuthenticatedPeerResult;
              resultFacts.set(evidence, { source: proof, origin });
              return { kind: "retained" as const, evidence };
            }
            yield* checkLocal();
            return { kind: "observation" as const, response: checked };
          });
          const proof = Object.freeze({
            ceiling: Object.freeze({
              ...ceiling,
              scopes: Object.freeze([...ceiling.scopes]),
              permissions: Object.freeze([...ceiling.permissions]),
            }),
          }) as PreparedPeerSource;
          const f: Facts = {
            origin,
            receiving,
            active: true,
            used: false,
            token: Object.freeze({}),
            check: () => checkLocal(),
            lookup: () => lookup(),
          };
          facts.set(proof, f);
          return yield* use(proof).pipe(
            Effect.provideService(SourceScope, f.token),
            Effect.ensuring(
              Effect.sync(() => {
                f.active = false;
              }),
            ),
            Effect.exit,
          );
        }).pipe(
          Effect.mapError((error) => (isRegistryError(error) ? error : refused("unavailable"))),
        );
        return yield* Exit.isFailure(outcome)
          ? Effect.failCause(outcome.cause)
          : Effect.succeed(outcome.value);
      }),
    });
  }),
);
