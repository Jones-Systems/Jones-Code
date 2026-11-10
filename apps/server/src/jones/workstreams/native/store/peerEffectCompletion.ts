import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Origin from "../runtime/peerEffectOrigin.ts";
import * as Receiving from "../runtime/retainedPeerSession.ts";
import * as Source from "../forwarding/peerEffectSource.ts";
import * as Registry from "../registry/service.ts";
import { RegistryError } from "../registry/session.ts";
import type { RegistryExternalResult } from "../domain/state.ts";
import * as Effects from "./effectStore.ts";
import * as Receipts from "./receiptStore.ts";
import * as Store from "./registryStore.ts";
import * as Db from "./sql.ts";

const isRegistryError = Schema.is(RegistryError);
const decode = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardResult);
type CommandResult = Extract<Forward.WorkstreamsForwardResult, { kind: "command" }>;
/** Only an authoritative encrypted PEER origin and fresh private admission can complete its parent. */
export class PeerEffectCompletion extends Context.Service<
  PeerEffectCompletion,
  {
    readonly complete: (
      origin: Origin.ValidatedPeerReservation,
      receiving: Receiving.CurrentRetainedPeer,
      source: Source.PreparedPeerSource,
    ) => Effect.Effect<CommandResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/store/peerEffectCompletion") {}

export const layer = Layer.effect(
  PeerEffectCompletion,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const db = yield* Db.NativeSql;
    const home = yield* NativeHome;
    const registry = yield* Registry.Registry;
    const receipts = yield* Receipts.ReceiptStore;
    const effects = yield* Effects.EffectStore;
    const store = yield* Store.RegistryStore;
    return PeerEffectCompletion.of({
      complete: Effect.fn("PeerEffectCompletion.complete")(
        function* (origin, receiving, source) {
          const check = Effect.gen(function* () {
            yield* Origin.checkOrigin(origin);
            yield* Receiving.checkRetainedPeer(receiving);
            yield* Source.checkPeerSourceOrigin(source, origin, receiving);
          });
          return yield* db.transaction(
            home,
            Effect.gen(function* () {
              yield* check;
              yield* Origin.checkOriginSnapshot(origin);
              if (origin.parent_result) return origin.parent_result;
              const retained = yield* effects.result(home, origin.command_id);
              if (retained === null) return yield* new RegistryError({ code: "unknown_effect" });
              // Registry owns the pure decision and authoritative command receipt. It does not mint a caller.
              const receipt = yield* registry.complete(
                origin.intent,
                retained as RegistryExternalResult,
                origin.intent.kind === "native_t3_settlement",
              );
              const saved = yield* receipts.read(home, origin.command_id);
              const context = yield* store.context(home);
              const identity = origin.parent.identity;
              if (
                !saved ||
                saved.origin !== "native" ||
                saved.request_body !== origin.envelope.request_body ||
                canonical(saved.receipt) !== canonical(receipt) ||
                !["committed", "rejected"].includes(receipt.state) ||
                !context ||
                context.state !== "active" ||
                context.registry_instance_id !== identity.expected_home.registry_instance_id ||
                context.owner_id !== identity.expected_home.owner_id ||
                context.home_environment_id !== identity.expected_home.home_environment_id ||
                context.server_generation !== identity.expected_home.server_generation ||
                context.canonical_origin !== identity.canonical_origin
              )
                return yield* new RegistryError({ code: "invalid_receipt" });
              const result = yield* decode({
                contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                kind: "command",
                identity,
                result: receipt,
                home: {
                  registry: {
                    ...identity.expected_home,
                    canonical_origin: context.canonical_origin,
                    registry_version: context.registry_version,
                  },
                  principal_id: identity.principal_id,
                  authorization_revision: receiving.grant.grant_version,
                  source_binding_version: identity.source_binding_version,
                  incoming_peer_pin_id: receiving.pin.pin_id,
                  incoming_peer_pin_revision: receiving.pin.revision,
                },
              }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_receipt" })));
              if (result.kind !== "command")
                return yield* new RegistryError({ code: "invalid_receipt" });
              const envelope = yield* db.sealJson(
                {
                  ...home,
                  purpose: "forward_result",
                  record_id: canonical([
                    identity.source_authority.environment_id,
                    identity.forward_id,
                  ]),
                  revision: 1,
                },
                canonical(result),
              );
              const changed = yield* sql`UPDATE jones_workstreams_forward_receipts
              SET state='completed',result_envelope=${envelope}
              WHERE registry_instance_id=${home.registry_instance_id} AND owner_id=${home.owner_id}
                AND source_environment_id=${identity.source_authority.environment_id}
                AND forward_id=${identity.forward_id} AND state='accepted'
                AND wire_sha256=${origin.envelope.forward_request_sha256}
              RETURNING forward_id`;
              if (changed.length !== 1)
                return yield* new RegistryError({ code: "idempotency_conflict" });
              // Own receipt/reservation/parent advances invalidate the pre-write snapshot.
              // Current owner, original receiver and fresh source fences must still hold.
              yield* check;
              return result;
            }),
          );
        },
        (effect) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
          ),
      ),
    });
  }),
);
