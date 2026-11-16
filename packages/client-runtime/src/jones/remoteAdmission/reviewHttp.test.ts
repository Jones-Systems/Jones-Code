import { EnvironmentId } from "@t3tools/contracts";
import { REMOTE_ADMISSION_REVIEW_BASE, REMOTE_ADMISSION_REVIEW_VERSION } from "@t3tools/contracts/jones/remoteAdmissionReview";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { PrimaryConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { reviewHttp } from "./reviewHttp.ts";

const target = new PrimaryConnectionTarget({ environmentId: EnvironmentId.make("receiver"), label: "Synthetic receiver", httpBaseUrl: "https://receiver.example.test", wsBaseUrl: "wss://receiver.example.test/ws" });
const entry: ConnectionCatalogEntry = { target, profile: Option.none(), enabled: true };
const prepared: PreparedConnection = { environmentId: target.environmentId, label: target.label, target, httpBaseUrl: target.httpBaseUrl, socketUrl: target.wsBaseUrl, httpAuthorization: { _tag: "Bearer", token: "synthetic-token" } };
const resolver = Layer.succeed(ConnectionResolver, ConnectionResolver.of({ prepare: () => Effect.succeed(prepared), prepareForUpdate: () => Effect.die("Review cannot use the update-only compatibility bypass.") }));

describe("authenticated review transport", () => {
  it.effect("preserves a declared conflict and sends the mutation exactly once", () => Effect.gen(function* () {
    const calls: string[] = [];
    const conflict = { code: "conflict", reason: "generation_changed", message: "Refresh the current grant.", current: { admission: null, grant: null, chainHead: null, attempt: null } };
    const http = HttpClient.make((request) => Effect.sync(() => {
      calls.push(`${request.method} ${request.url}`);
      expect(request.headers.authorization).toBe("Bearer synthetic-token");
      return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(conflict), { status: 409, headers: { "content-type": "application/json" } }));
    }));
    const result = yield* reviewHttp.revokeGrant(entry, { grantId: "grant", expectedGeneration: 1 }).pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) expect(result.failure).toEqual(conflict);
    expect(calls).toEqual([`POST https://receiver.example.test${REMOTE_ADMISSION_REVIEW_BASE}/grants/revoke`]);
  }).pipe(Effect.provide(resolver)));

  it.effect("separates capability support from decision permission", () => Effect.gen(function* () {
    const capabilities = { version: REMOTE_ADMISSION_REVIEW_VERSION, features: { reviewV2: true, grants: true, conflicts: true, changeFeed: true }, principal: { canDecide: false } };
    const http = HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, new Response(JSON.stringify(capabilities), { headers: { "content-type": "application/json" } }))));
    expect(yield* reviewHttp.capabilities(entry).pipe(Effect.provideService(HttpClient.HttpClient, http))).toEqual(capabilities);
  }).pipe(Effect.provide(resolver)));
});
