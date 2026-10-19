import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  SETTLEMENT_ORIGINS_CAPABILITY,
  type ServerConfig,
  type SettlementOriginQuery,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import {
  BearerConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import type { RpcSession } from "../../rpc/session.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import {
  fetchSettlementOrigins,
  resolveEnvironmentSettlementOrigins,
} from "./fetchSettlementOrigins.ts";
import { env, otherEnv, entry } from "./testFixtures.ts";

function connection(environmentId = env): PreparedConnection {
  return {
    environmentId,
    label: environmentId,
    target: new RelayConnectionTarget({ environmentId, label: environmentId }),
    httpBaseUrl: `https://${environmentId}.test`,
    socketUrl: `wss://${environmentId}.test/ws`,
    httpAuthorization: { _tag: "Bearer", token: `synthetic-${environmentId}` },
  };
}
const signer = Option.none<ManagedRelayDpopSigner["Service"]>();
const items = (length: number): SettlementOriginQuery[] =>
  Array.from({ length }, (_, i) => ({
    threadId: ThreadId.make(`thread-${i}`),
    settledAtMs: i + 100,
  }));

it.effect("batches at 100, keeps identity order and selects each environment's authorization", () =>
  Effect.gen(function* () {
    const calls: Array<{ url: string; init: RequestInit; items: SettlementOriginQuery[] }> = [];
    const layer = layerRemoteHttpClient(async (url, init) => {
      const body = JSON.parse(
        init?.body instanceof Uint8Array ? new TextDecoder().decode(init.body) : String(init?.body),
      ) as { items: SettlementOriginQuery[] };
      calls.push({ url: String(url), init: init ?? {}, items: body.items });
      return Response.json({
        items: body.items.map((item) => ({
          ...item,
          origin: item.settledAtMs % 2 ? "other" : "client",
        })),
      });
    });
    for (const environmentId of [env, otherEnv]) {
      const result = yield* fetchSettlementOrigins({
        prepared: connection(environmentId),
        signer,
        items: items(201),
      }).pipe(Effect.provide(layer));
      expect(result).toEqual(items(201).map((item) => (item.settledAtMs % 2 ? "other" : "client")));
    }
    expect(calls.map((call) => call.items.length)).toEqual([100, 100, 1, 100, 100, 1]);
    expect(calls.map((call) => call.url)).toEqual(
      [env, env, env, otherEnv, otherEnv, otherEnv].map(
        (id) => `https://${id}.test/api/jones/settlement-origins`,
      ),
    );
    expect(calls.map((call) => new Headers(call.init.headers).get("authorization"))).toEqual(
      [env, env, env, otherEnv, otherEnv, otherEnv].map((id) => `Bearer synthetic-${id}`),
    );
    expect(calls.every((call) => call.init.method === "POST")).toBe(true);
  }),
);

it.effect("rejects wrong-length, reordered, foreign-time and malformed responses", () =>
  Effect.gen(function* () {
    const requested = items(2);
    const valid = requested.map((item) => ({ ...item, origin: "client" }));
    for (const response of [
      { items: valid.slice(0, 1) },
      { items: [...valid].reverse() },
      { items: [{ ...valid[0], settledAtMs: 999 }, valid[1]] },
      { items: [{ ...valid[0], threadId: "unrequested" }, valid[1]] },
      { items: [{ ...valid[0], origin: "invalid" }, valid[1]] },
    ]) {
      const result = yield* fetchSettlementOrigins({
        prepared: connection(),
        signer,
        items: requested,
      }).pipe(
        Effect.result,
        Effect.provide(layerRemoteHttpClient(async () => Response.json(response))),
      );
      expect(result._tag).toBe("Failure");
    }
  }),
);

it.effect("keeps cookie credentials and leaves unsupported or failed transport retryable", () =>
  Effect.gen(function* () {
    const prepared = { ...connection(), httpAuthorization: null };
    const result = yield* fetchSettlementOrigins({ prepared, signer, items: items(1) }).pipe(
      Effect.provide(
        layerRemoteHttpClient(async (_url, init) => {
          expect(init?.credentials).toBe("include");
          return Response.json({ items: [{ ...items(1)[0], origin: "unknown" }] });
        }),
      ),
    );
    expect(result).toEqual(["unknown"]);
    for (const reply of [
      async () => new Response("Not found", { status: 404 }),
      async () => {
        throw new TypeError("network unavailable");
      },
    ]) {
      const failure = yield* fetchSettlementOrigins({ prepared, signer, items: items(1) }).pipe(
        Effect.result,
        Effect.provide(layerRemoteHttpClient(reply)),
      );
      expect(failure._tag).toBe("Failure");
    }
  }),
);

it.effect(
  "renews relay authorization while preserving learned direct routing and signed URL identity",
  () =>
    Effect.gen(function* () {
      for (const direct of [false, true]) {
        const base = connection();
        const prepared: PreparedConnection = {
          ...base,
          target: direct
            ? new BearerConnectionTarget({ environmentId: env, label: env, connectionId: "direct" })
            : base.target,
          httpAuthorization: { _tag: "Dpop", accessToken: "old", expiresAtEpochMs: 0 },
        };
        const expectedBase = direct ? prepared.httpBaseUrl : "https://renewed-relay.test";
        const authorization = RemoteEnvironmentAuthorization.of({
          authorizeBearer: () => Effect.die("Unexpected socket authorization"),
          authorizeDpop: () => Effect.die("Unexpected socket authorization"),
          authorizeDpopHttp: ({ expectedEnvironmentId }) => {
            expect(expectedEnvironmentId).toBe(env);
            return Effect.succeed({
              environmentId: env,
              label: env,
              httpBaseUrl: "https://renewed-relay.test",
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "current",
                expiresAtEpochMs: 99_999,
              },
            });
          },
        });
        const signing = ManagedRelayDpopSigner.of({
          thumbprint: Effect.succeed("synthetic"),
          createProof: (input) => {
            expect(input).toMatchObject({
              method: "POST",
              url: `${expectedBase}/api/jones/settlement-origins`,
              accessToken: "current",
            });
            return Effect.succeed("synthetic-proof");
          },
        });
        const result = yield* fetchSettlementOrigins({
          prepared,
          signer: Option.some(signing),
          remoteAuthorization: Option.some(authorization),
          items: items(1),
        }).pipe(
          Effect.provide(
            layerRemoteHttpClient(async (url, init) => {
              expect(String(url)).toBe(`${expectedBase}/api/jones/settlement-origins`);
              expect(new Headers(init?.headers).get("authorization")).toBe("DPoP current");
              expect(new Headers(init?.headers).get("dpop")).toBe("synthetic-proof");
              return Response.json({ items: [{ ...items(1)[0], origin: "client" }] });
            }),
          ),
        );
        expect(result).toEqual(["client"]);
      }
    }),
);

it.effect(
  "uses only the current environment config, distinguishes absent capability from missing config, and re-reads on retry",
  () =>
    Effect.gen(function* () {
      const prepared = yield* SubscriptionRef.make(Option.some(connection()));
      const makeSession = (environmentId: EnvironmentId, supported: boolean) =>
        ({
          initialConfig: Effect.succeed({
            environment: {
              environmentId,
              capabilities: supported ? { settlementOrigins: SETTLEMENT_ORIGINS_CAPABILITY } : {},
            },
          } as ServerConfig),
        }) as unknown as RpcSession;
      const session = yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none());
      // Only these two supervisor ports are exercised by the resolver.
      const supervisor = {
        prepared,
        session,
      } as EnvironmentSupervisor.EnvironmentSupervisor["Service"];
      let requests = 0;
      const query = resolveEnvironmentSettlementOrigins(env, [entry()]).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provide(
          layerRemoteHttpClient(async () => {
            requests++;
            return Response.json({
              items: [
                { threadId: entry().threadId, settledAtMs: entry().settledAtMs, origin: "client" },
              ],
            });
          }),
        ),
      );
      expect((yield* query.pipe(Effect.result))._tag).toBe("Failure");
      yield* SubscriptionRef.set(session, Option.some(makeSession(env, false)));
      expect(yield* query).toBeNull();
      expect(requests).toBe(0);
      yield* SubscriptionRef.set(session, Option.some(makeSession(otherEnv, true)));
      expect((yield* query.pipe(Effect.result))._tag).toBe("Failure");
      yield* SubscriptionRef.set(session, Option.some(makeSession(env, true)));
      expect(yield* query).toEqual(["client"]);
      expect(requests).toBe(1);
      yield* SubscriptionRef.set(prepared, Option.some(connection(otherEnv)));
      expect((yield* query.pipe(Effect.result))._tag).toBe("Failure");
      expect(requests).toBe(1);
    }),
);
