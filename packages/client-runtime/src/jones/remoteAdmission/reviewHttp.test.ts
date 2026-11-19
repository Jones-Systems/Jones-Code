import { EnvironmentId } from "@t3tools/contracts";
import {
  REMOTE_ADMISSION_REVIEW_BASE,
  REMOTE_ADMISSION_REVIEW_VERSION,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import type { RpcSession } from "../../rpc/session.ts";
import { reviewHttp } from "./reviewHttp.ts";

const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("receiver"),
  label: "Synthetic receiver",
  httpBaseUrl: "https://receiver.example.test",
  wsBaseUrl: "wss://receiver.example.test/ws",
});
const prepared: PreparedConnection = {
  environmentId: target.environmentId,
  label: target.label,
  target,
  httpBaseUrl: target.httpBaseUrl,
  socketUrl: target.wsBaseUrl,
  httpAuthorization: { _tag: "Bearer", token: "synthetic-token" },
};
const supervisor = Layer.effect(
  EnvironmentSupervisor,
  Effect.gen(function* () {
    return EnvironmentSupervisor.of({
      target,
      prepared: yield* SubscriptionRef.make(Option.some(prepared)),
      session: yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none()),
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      connect: Effect.void,
      disconnect: Effect.void,
      retryNow: Effect.void,
    });
  }),
);
const capabilities = {
  version: REMOTE_ADMISSION_REVIEW_VERSION,
  features: { reviewV2: true, grants: true, conflicts: true, changeFeed: true },
  principal: { canDecide: false },
};

describe("authenticated review transport", () => {
  it.effect("refuses a route prepared for another receiver before sending an owner mutation", () =>
    Effect.gen(function* () {
      const { prepared: active } = yield* EnvironmentSupervisor;
      yield* SubscriptionRef.set(
        active,
        Option.some({ ...prepared, environmentId: EnvironmentId.make("wrong-receiver") }),
      );
      let sent = false;
      const http = HttpClient.make(() =>
        Effect.sync(() => {
          sent = true;
          throw new Error("Wrong receiver must never be contacted.");
        }),
      );
      const result = yield* reviewHttp
        .revokeGrant({
          grantId: "grant",
          expectedGeneration: 1,
        })
        .pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "configuration",
        });
      }
      expect(sent).toBe(false);
    }).pipe(Effect.provide(supervisor)),
  );

  it.effect("preserves a declared conflict and sends the mutation exactly once", () =>
    Effect.gen(function* () {
      const calls: string[] = [];
      const conflict = {
        code: "conflict",
        reason: "generation_changed",
        message: "Refresh the current grant.",
        current: { admission: null, grant: null, chainHead: null, attempt: null },
      };
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          calls.push(`${request.method} ${request.url}`);
          expect(request.headers.authorization).toBe("Bearer synthetic-token");
          return HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(conflict), {
              status: 409,
              headers: { "content-type": "application/json" },
            }),
          );
        }),
      );
      const result = yield* reviewHttp
        .revokeGrant({ grantId: "grant", expectedGeneration: 1 })
        .pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(result.failure).toEqual(conflict);
      expect(calls).toEqual([
        `POST https://receiver.example.test${REMOTE_ADMISSION_REVIEW_BASE}/grants/revoke`,
      ]);
    }).pipe(Effect.provide(supervisor)),
  );

  it.effect("separates capability support from decision permission", () =>
    Effect.gen(function* () {
      const http = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(capabilities), {
              headers: { "content-type": "application/json" },
            }),
          ),
        ),
      );
      expect(
        yield* reviewHttp.capabilities().pipe(Effect.provideService(HttpClient.HttpClient, http)),
      ).toEqual(capabilities);
    }).pipe(Effect.provide(supervisor)),
  );

  it.effect("follows the supervisor on failover without replaying an uncertain mutation", () =>
    Effect.gen(function* () {
      const { prepared: active } = yield* EnvironmentSupervisor;
      const requests: { method: string; url: string; authorization: string | undefined }[] = [];
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push({
            method: request.method,
            url: request.url,
            authorization: request.headers.authorization,
          });
          return HttpClientResponse.fromWeb(
            request,
            request.method === "POST"
              ? new Response(null, { status: 502 })
              : new Response(JSON.stringify(capabilities), {
                  headers: { "content-type": "application/json" },
                }),
          );
        }),
      );
      yield* SubscriptionRef.set(
        active,
        Option.some({
          ...prepared,
          httpBaseUrl: "https://active-route.example.test",
          httpAuthorization: { _tag: "Bearer", token: "active-token" },
        }),
      );
      const uncertain = yield* reviewHttp
        .revokeGrant({ grantId: "grant", expectedGeneration: 1 })
        .pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
      expect(Result.isFailure(uncertain)).toBe(true);
      yield* SubscriptionRef.set(active, Option.none());
      const disconnected = yield* reviewHttp
        .capabilities()
        .pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
      expect(Result.isFailure(disconnected)).toBe(true);
      yield* SubscriptionRef.set(active, Option.some(prepared));
      yield* reviewHttp.capabilities().pipe(Effect.provideService(HttpClient.HttpClient, http));
      expect(requests).toEqual([
        {
          method: "POST",
          url: `https://active-route.example.test${REMOTE_ADMISSION_REVIEW_BASE}/grants/revoke`,
          authorization: "Bearer active-token",
        },
        {
          method: "GET",
          url: `https://receiver.example.test${REMOTE_ADMISSION_REVIEW_BASE}/capabilities`,
          authorization: "Bearer synthetic-token",
        },
      ]);
    }).pipe(Effect.provide(supervisor)),
  );
});
