import { EnvironmentId, type ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { PrimaryConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { requestJonesUpdate } from "./updateBridge.ts";

const target = new PrimaryConnectionTarget({ environmentId: EnvironmentId.make("remote"), label: "Remote", httpBaseUrl: "https://remote.example.test", wsBaseUrl: "wss://remote.example.test/ws" });
const entry: ConnectionCatalogEntry = { target, profile: Option.none(), enabled: false, serverUpdateRequired: true };
const descriptor: ExecutionEnvironmentDescriptor = { environmentId: target.environmentId, label: target.label, platform: { os: "linux", arch: "x64" }, serverVersion: "0.0.45-preview.20261010.123.1", orchestrationProtocolVersion: 1, capabilities: { repositoryIdentity: true } };
const prepared: PreparedConnection = { environmentId: target.environmentId, label: target.label, httpBaseUrl: target.httpBaseUrl, socketUrl: target.wsBaseUrl, httpAuthorization: { _tag: "Bearer", token: "synthetic-test-token" }, target };
const state = { source: "jones-actions", channel: "jones-main", phase: "staged", capability: { check: true, download: true, install: true }, stagedHandle: "fixed", environmentId: target.environmentId, currentVersion: descriptor.serverVersion };

function resolverLayer(observed = descriptor) {
  return Layer.succeed(ConnectionResolver, ConnectionResolver.of({
    prepare: () => Effect.die("The update bridge must not prepare ordinary orchestration."),
    prepareForUpdate: () => Effect.succeed({ prepared, descriptor: observed }),
  }));
}

describe("qualified update-only connection", () => {
  it.effect("reads and installs on an incompatible host with existing bearer authorization", () => Effect.gen(function* () {
    const calls: Array<{ method: string; url: string; authorization: string | undefined; protocol: string | undefined }> = [];
    const http = HttpClient.make((request) => Effect.sync(() => {
      calls.push({ method: request.method, url: request.url, authorization: request.headers.authorization, protocol: request.headers["x-t3-orchestration-protocol"] });
      return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(state), { headers: { "content-type": "application/json" } }));
    }));
    yield* requestJonesUpdate(entry, { action: "state" }).pipe(Effect.provideService(HttpClient.HttpClient, http));
    yield* requestJonesUpdate(entry, { action: "install", input: { stagedHandle: "fixed", environmentId: target.environmentId, currentVersion: descriptor.serverVersion } }).pipe(Effect.provideService(HttpClient.HttpClient, http));
    expect(calls).toEqual([
      { method: "GET", url: "https://remote.example.test/api/jones-updates", authorization: "Bearer synthetic-test-token", protocol: undefined },
      { method: "POST", url: "https://remote.example.test/api/jones-updates/install", authorization: "Bearer synthetic-test-token", protocol: undefined },
    ]);
  }).pipe(Effect.provide(resolverLayer())));

  it.effect("rejects a descriptor for another environment before HTTP dispatch", () => Effect.gen(function* () {
    const result = yield* requestJonesUpdate(entry, { action: "state" }).pipe(Effect.result);
    expect(Result.isFailure(result) && result.failure._tag).toBe("JonesUpdateBindingError");
  }).pipe(Effect.provide(resolverLayer({ ...descriptor, environmentId: EnvironmentId.make("other") })), Effect.provideService(HttpClient.HttpClient, HttpClient.make(() => Effect.die("No request permitted")))));

  it.effect("rejects stale install version before sending the mutation", () => Effect.gen(function* () {
    const result = yield* requestJonesUpdate(entry, { action: "install", input: { stagedHandle: "fixed", environmentId: target.environmentId, currentVersion: "old" } }).pipe(Effect.result);
    expect(Result.isFailure(result) && result.failure._tag).toBe("JonesUpdateBindingError");
  }).pipe(Effect.provide(resolverLayer()), Effect.provideService(HttpClient.HttpClient, HttpClient.make(() => Effect.die("No mutation permitted")))));

  it.effect("does not retry an ambiguous install response", () => Effect.gen(function* () {
    let calls = 0;
    const http = HttpClient.make((request) => Effect.sync(() => {
      calls++;
      return HttpClientResponse.fromWeb(request, new Response("unavailable", { status: 503 }));
    }));
    const result = yield* requestJonesUpdate(entry, { action: "install", input: { stagedHandle: "fixed", environmentId: target.environmentId, currentVersion: descriptor.serverVersion } }).pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
    expect(Result.isFailure(result)).toBe(true);
    expect(calls).toBe(1);
  }).pipe(Effect.provide(resolverLayer())));
});
