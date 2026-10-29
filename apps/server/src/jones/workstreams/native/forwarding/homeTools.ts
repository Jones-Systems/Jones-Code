import { HomeTools } from "./homeForwardPort.ts";
export { HomeTools } from "./homeForwardPort.ts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Crypto from "effect/Crypto";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Index from "../crypto/identityIndex.ts";
import * as Database from "../store/sql.ts";
import * as Store from "../store/registryStore.ts";
import * as PinsStore from "../store/peerSessionPinStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as History from "../store/historyStore.ts";
import * as Effects from "../store/effectStore.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Session from "../registry/session.ts";
import * as Registry from "../registry/service.ts";
import * as Appearance from "../registry/appearance.ts";
import * as Operations from "../operations/forwardedHomeOperation.ts";
import * as Ledger from "../store/forwardReceiptStore.ts";
import * as Boundary from "./peerWriteAdmission.ts";
import * as Supplied from "./suppliedEvidence.ts";
import * as Guard from "./guard.ts";

const decodeForward = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);
const decodeReadback = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardReadbackRequest);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const home = yield* NativeHome,
    keys = yield* Keys.WorkstreamsKeyProvider,
    sql = yield* SqlClient.SqlClient;
  const fences = yield* Boundary.ReceivingAuthorityFence,
    source = yield* Supplied.SourceForwardQualification;
  // This captures application-owned native authentication. Never reconstruct its registration map.
  const native = yield* Pins.NativePeerSessions;
  const localSession = yield* Session.Session;
  const prepare = Effect.fn("HomeTools.prepare")(function* (invocation: Mcp.McpInvocationScope) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new Session.RegistryError({ code: "preparation_in_transaction" });
    yield* native.current(invocation).pipe(Effect.mapError(Boundary.mapFailure));
    const fence = yield* fences.prepare(invocation);
    yield* fence.checkCurrent();
    // External key retrieval and owner qualification finish before constructing any writer service.
    const rows = yield* sql<{
      key_id: string;
      scope: string;
    }>`SELECT key_id,scope FROM jones_workstreams_keys WHERE registry_instance_id=${home.registry_instance_id}`;
    const activeHome = yield* keys.activeKeyId(home);
    const activeLocal = yield* keys.activeKeyId({ ...home, key_scope: "local" });
    const content = new Map<string, Uint8Array>();
    for (const id of new Set([...rows.map((r) => r.key_id), activeHome, activeLocal]))
      content.set(id, Uint8Array.from(yield* keys.get(id)));
    const index = Uint8Array.from(yield* keys.indexKey(home.registry_instance_id));
    const missing = () => new Keys.KeyUnavailable({ reason: "missing" });
    const cached = Keys.WorkstreamsKeyProvider.of({
      activeKeyId: Effect.fn("HomeTools.preparedActiveKeyId")(function* (scope: Keys.KeyScope) {
        if (
          scope.registry_instance_id !== home.registry_instance_id ||
          scope.owner_id !== home.owner_id ||
          (scope.key_scope !== undefined &&
            scope.key_scope !== "home" &&
            scope.key_scope !== "local")
        )
          return yield* missing();
        const keyScope = scope.key_scope ?? "home";
        const preparedId = keyScope === "local" ? activeLocal : activeHome;
        // Select on the same application client; prepared bytes never authorize a replacement ID.
        const current = yield* sql<{ key_id: string }>`
          SELECT k.key_id FROM jones_workstreams_keys k
          JOIN jones_workstreams_registries r ON r.registry_instance_id=k.registry_instance_id
          WHERE k.registry_instance_id=${scope.registry_instance_id} AND r.owner_id=${scope.owner_id}
            AND k.scope=${keyScope} AND k.state='active'
            AND (${keyScope}='local' OR r.active_key_id=k.key_id)
        `.pipe(Effect.mapError(() => new Keys.KeyUnavailable({ reason: "store_unavailable" })));
        if (current.length !== 1 || current[0]?.key_id !== preparedId)
          return yield* new Keys.KeyUnavailable({ reason: "active_key_missing" });
        return preparedId;
      }),
      get: (id) =>
        Effect.suspend(() =>
          content.has(id)
            ? Effect.succeed(Uint8Array.from(content.get(id)!))
            : Effect.fail(missing()),
        ),
      indexKey: (id) =>
        id === home.registry_instance_id
          ? Effect.succeed(Uint8Array.from(index))
          : Effect.fail(missing()),
    });
    const db = Database.layer.pipe(
      Layer.provideMerge(
        Envelope.layer.pipe(Layer.provideMerge(Layer.succeed(Keys.WorkstreamsKeyProvider, cached))),
      ),
    );
    const receipts = Receipts.layer.pipe(Layer.provideMerge(Archive.layer));
    const stores = Layer.mergeAll(
      Store.layer,
      PinsStore.layer,
      Index.layer,
      Bindings.layer,
      Effects.layer.pipe(Layer.provideMerge(Layer.mergeAll(receipts, History.layer))),
    ).pipe(Layer.provideMerge(db));
    const authority = Layer.succeed(Authority.NativeStoreAuthority, {
      readCurrent: fence.checkCurrent().pipe(
        Effect.mapError(
          (cause) =>
            new NativeStoreAuthorityPersistenceError(
              "fenced",
              "Home authority fence refused.",
              cause,
            ),
        ),
        Effect.as(fence.authority),
      ),
      trustProvider: {
        readTrustSnapshot: () => ({
          readiness: "trust-provider-required",
          trustedEnvironments: [],
        }),
      },
    });
    const guard = Guard.layer.pipe(
      Layer.provideMerge(Pins.layer.pipe(Layer.provideMerge(authority))),
      Layer.provideMerge(stores),
      Layer.provideMerge(Layer.succeed(Pins.NativePeerSessions, native)),
    );
    const boundary = Boundary.layer.pipe(Layer.provideMerge(guard));
    const session = Boundary.sessionLayer.pipe(
      Layer.provideMerge(boundary),
      Layer.provide(Layer.succeed(Session.Session, localSession)),
    );
    const registry = Registry.layer.pipe(Layer.provideMerge(session));
    const operations = Operations.layer.pipe(
      Layer.provideMerge(Supplied.layer),
      Layer.provideMerge(registry),
    );
    const graph = Ledger.layer.pipe(
      Layer.provideMerge(operations),
      Layer.provideMerge(Appearance.layer.pipe(Layer.provideMerge(registry))),
      Layer.provideMerge(Layer.succeed(NativeHome, home)),
      Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
      Layer.provideMerge(Layer.succeed(Crypto.Crypto, crypto)),
    );
    return { graph, fence };
  });
  const forward = Effect.fn("HomeTools.forward")(
    function* (input: unknown, invocation: Mcp.McpInvocationScope) {
      const request = yield* decodeForward(input).pipe(
        Effect.mapError(() => new Session.RegistryError({ code: "unsupported_protocol" })),
      );
      const { graph, fence } = yield* prepare(invocation);
      return yield* Effect.gen(function* () {
        const boundary = yield* Boundary.PeerWriteBoundary;
        const admission = yield* boundary.current(request);
        const receiving = yield* (yield* Guard.Guard).authorize(
          "workstreams:write",
          request.request.relay,
        );
        const qualified = yield* source.prepare(request, receiving.pin);
        yield* qualified.checkCurrent();
        yield* fence.checkCurrent();
        return yield* (yield* Ledger.ForwardReceiptStore)
          .execute(request, encodeJson(request), admission)
          .pipe(Effect.provideService(Boundary.CurrentSourceForwardFence, qualified));
      }).pipe(
        Effect.provide(graph.pipe(Layer.fresh)),
        Effect.provideService(Mcp.McpInvocationContext, invocation),
      );
    },
    (effect) => effect.pipe(Effect.mapError(Boundary.mapFailure)),
  );
  const readback = Effect.fn("HomeTools.readback")(
    function* (input: unknown, invocation: Mcp.McpInvocationScope) {
      const request = yield* decodeReadback(input).pipe(
        Effect.mapError(() => new Session.RegistryError({ code: "unsupported_protocol" })),
      );
      const { graph, fence } = yield* prepare(invocation);
      return yield* Effect.gen(function* () {
        const admission = yield* (yield* Boundary.PeerWriteBoundary).current(request, true);
        // Readback still requires original-principal association and current source authority.
        const pin = (yield* (yield* Guard.Guard).authorize("workstreams:read")).pin;
        const qualified = yield* source.prepareReadback(request, pin);
        if (qualified.state === "unknown") {
          yield* (yield* Boundary.PeerWriteBoundary).checkCurrent(admission);
          yield* fence.checkCurrent();
          return {
            contract:
              Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT as typeof Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
            identity: request.identity,
            state: "unknown" as const,
            reason: qualified.reason,
          };
        }
        yield* qualified.checkCurrent();
        yield* fence.checkCurrent();
        return yield* (yield* Ledger.ForwardReceiptStore)
          .readback(request.identity, admission)
          .pipe(Effect.provideService(Boundary.CurrentSourceForwardFence, qualified));
      }).pipe(
        Effect.provide(graph.pipe(Layer.fresh)),
        Effect.provideService(Mcp.McpInvocationContext, invocation),
      );
    },
    (effect) => effect.pipe(Effect.mapError(Boundary.mapFailure)),
  );
  return HomeTools.of({ forward, readback });
});
/** @public Mount these fixed handlers only in native-home with both current qualification ports. */
export const layer = Layer.effect(HomeTools, make);
