import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Signals from "./OriginSignals.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as OriginPeerBindings from "./OriginPeerBindings.ts";
import {
  encodeIntent,
  encodeReturnBinding,
  makeOriginStore,
  originConflict,
  type ObservationRow,
  type ReturnBinding,
} from "./OriginStore.ts";

const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);

export class OriginDelivery extends Context.Service<
  OriginDelivery,
  {
    readonly prepare: (input: {
      readonly environmentId: import("@t3tools/contracts").EnvironmentId;
      readonly clientRequestId: string;
      readonly binding: ReturnBinding;
      readonly fingerprint: string;
      readonly limits?: import("../../peer/PeerMcpClient.ts").PeerCallLimits;
    }) => Effect.Effect<
      ObservationRow & { readonly transmit: boolean },
      OrchestratorMcpFailure,
      import("effect/Scope").Scope
    >;
    readonly isSending: (key: string) => boolean;
    readonly pending: (
      key: string,
      phase: "held" | "sent_unknown",
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly release: (key: string) => Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/jones/remoteAdmissionOrigin/OriginDelivery") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const peers = yield* OriginPeerBindings.OriginPeerBindings;
  const store = makeOriginStore(sql);
  const sending = new Set<string>();
  const signals = yield* Effect.serviceOption(Signals.OriginSignals);
  const changed = Option.isSome(signals) ? signals.value.changed : Effect.void;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const prepare = Effect.fn("OriginDelivery.prepare")(
    function* (input: Parameters<OriginDelivery["Service"]["prepare"]>[0]) {
      if (sending.has(input.binding.rootObservationKey)) return yield* originConflict();
      yield* Effect.acquireRelease(
        Effect.sync(() => sending.add(input.binding.rootObservationKey)),
        () =>
          Effect.sync(() => {
            sending.delete(input.binding.rootObservationKey);
          }).pipe(Effect.andThen(changed)),
      );
      const peer = yield* peers.current(input.environmentId);
      if (!peer || !(yield* store.available)) return yield* originConflict();
      const timestamp = yield* now;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const old = yield* store.get(input.binding.rootObservationKey);
          const row = yield* store.prepareInTransaction({
            observation_key: input.binding.rootObservationKey,
            peer_environment_id: input.environmentId,
            link_generation: peer.generation,
            session_id: peer.receiverSessionId,
            client_request_id: input.clientRequestId,
            receiver_namespace: `client:${peer.receiverSessionId}`,
            kind: "launch",
            root_observation_key: input.binding.rootObservationKey,
            predecessor_observation_key: null,
            admission_id: null,
            attempt_id: null,
            recovered_revision: null,
            return_binding_json: encodeReturnBinding(input.binding),
            intent_json: encodeIntent({
              phase: "prepared",
              fingerprint: input.fingerprint,
              registered: true,
              acknowledged: false,
              limits: input.limits ?? peer.limits,
            }),
            result_json: null,
            state: "intent",
            created_at: timestamp,
            updated_at: timestamp,
            terminal_at: null,
          });
          return { ...row, transmit: old === undefined };
        }),
      );
    },
    Effect.mapError((error) => (isOrchestratorMcpFailure(error) ? error : originConflict())),
  );
  const pending = Effect.fn("OriginDelivery.pending")(
    function* (key: string, phase: "held" | "sent_unknown") {
      yield* sql.withTransaction(store.pending(key, phase, yield* now));
      yield* changed;
    },
    Effect.mapError((error) => (isOrchestratorMcpFailure(error) ? error : originConflict())),
  );
  const release = Effect.fn("OriginDelivery.release")(
    function* (key: string) {
      yield* sql.withTransaction(store.release(key, yield* now));
      yield* changed;
    },
    Effect.mapError((error) => (isOrchestratorMcpFailure(error) ? error : originConflict())),
  );
  return OriginDelivery.of({ prepare, pending, release, isSending: (key) => sending.has(key) });
});
export const layer = Layer.effect(OriginDelivery, make);
