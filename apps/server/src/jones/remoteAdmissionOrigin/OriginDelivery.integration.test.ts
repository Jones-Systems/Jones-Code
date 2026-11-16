import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Delivery from "./OriginDelivery.ts";
import * as Bindings from "./OriginPeerBindings.ts";
import { intent, makeOriginStore } from "./OriginStore.ts";

const environmentId = EnvironmentId.make("receiver");
const input = { environmentId, clientRequestId:"fwd:stable",fingerprint:"digest",
  limits:{runtimeMode:"auto-accept-edits" as const,interactionMode:"default" as const},
  binding:{rootObservationKey:"root",originThreadId:"waiter",returnRouteKey:"return",waiterGeneration:0,waiterRunId:null} };
const memory = Layer.effectDiscard(Effect.gen(function* () {
  yield* migration;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_thread_result_fences (origin_environment_id TEXT,operation_id TEXT,generation INTEGER)`;
})).pipe(Layer.provideMerge(NodeSqliteClient.layer({filename:":memory:"})));
const unavailable = () => Effect.fail(new OrchestratorMcpFailure({code:"capability_denied",message:"Unavailable"}));
const peers = (offline = false) => Layer.succeed(Bindings.OriginPeerBindings, {
  current: () => Effect.succeed(offline ? null : {environmentId,generation:"generation",receiverSessionId:"receiver-session",peerLongPoll:false,
    limits:{runtimeMode:"full-access" as const,interactionMode:"default" as const}}),
  relevant: () => Effect.succeed([]), observeRetry: unavailable, ackRetry: unavailable,
});
const layer = Delivery.layer.pipe(Layer.provideMerge(peers()),Layer.provideMerge(memory));

it.effect("durable intent exists before transmission permission and reload never grants another transmission", () => Effect.gen(function* () {
  const delivery = yield* Delivery.OriginDelivery; const sql = yield* SqlClient.SqlClient;
  const store = makeOriginStore(sql);
  yield* Effect.scoped(Effect.gen(function* () {
    const prepared = yield* delivery.prepare(input);
    expect(prepared.transmit).toBe(true); expect(delivery.isSending("root")).toBe(true);
    expect((yield* store.get("root"))?.receiver_namespace).toBe("client:receiver-session");
    expect(intent(prepared).limits).toEqual(input.limits);
    yield* delivery.pending("root","sent_unknown");
  }));
  expect(delivery.isSending("root")).toBe(false);
  yield* Effect.scoped(Effect.gen(function* () {
    expect((yield* delivery.prepare(input)).transmit).toBe(false);
  }));
  expect((yield* store.list()).length).toBe(1);
}).pipe(Effect.provide(layer)));

it.effect("offline or missing authenticated session cannot promise a bound wake or write intent", () => Effect.gen(function* () {
  const delivery = yield* Delivery.OriginDelivery; const sql = yield* SqlClient.SqlClient;
  const rejected = yield* Effect.scoped(delivery.prepare(input)).pipe(Effect.flip());
  expect(rejected._tag).toBe("OrchestratorMcpFailure");
  expect((yield* makeOriginStore(sql).list()).length).toBe(0);
  expect(delivery.isSending("root")).toBe(false);
}).pipe(Effect.provide(Delivery.layer.pipe(Layer.provideMerge(peers(true)),Layer.provideMerge(memory)))));
