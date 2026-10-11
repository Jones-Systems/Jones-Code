import { EnvironmentId, type ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import { normalizeDpopHtu } from "@t3tools/shared/dpopCommon";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import {
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import {
  ManagedRelayDpopSigner,
  type ManagedRelayDpopProofInput,
} from "../../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import { requestJonesUpdate } from "./updateBridge.ts";

const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("remote"),
  label: "Remote",
  httpBaseUrl: "https://remote.example.test",
  wsBaseUrl: "wss://remote.example.test/ws",
});
const entry: ConnectionCatalogEntry = {
  target,
  profile: Option.none(),
  enabled: false,
  serverUpdateRequired: true,
};
const descriptor: ExecutionEnvironmentDescriptor = {
  environmentId: target.environmentId,
  label: target.label,
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.45-preview.20261010.123.1",
  orchestrationProtocolVersion: 1,
  capabilities: { repositoryIdentity: true },
};
const prepared: PreparedConnection = {
  environmentId: target.environmentId,
  label: target.label,
  httpBaseUrl: target.httpBaseUrl,
  socketUrl: target.wsBaseUrl,
  httpAuthorization: { _tag: "Bearer", token: "synthetic-test-token" },
  target,
};
const state = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "staged",
  capability: { check: true, download: true, install: true },
  stagedHandle: "fixed",
  environmentId: target.environmentId,
  currentVersion: descriptor.serverVersion,
};

function resolverLayer(observed = descriptor) {
  return Layer.succeed(
    ConnectionResolver,
    ConnectionResolver.of({
      prepare: () => Effect.die("The update bridge must not prepare ordinary orchestration."),
      prepareForUpdate: () => Effect.succeed({ prepared, descriptor: observed }),
    }),
  );
}

describe("qualified update-only connection", () => {
  it.effect("reads and installs on an incompatible host with existing bearer authorization", () =>
    Effect.gen(function* () {
      const calls: Array<{
        method: string;
        url: string;
        authorization: string | undefined;
        protocol: string | undefined;
      }> = [];
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          calls.push({
            method: request.method,
            url: request.url,
            authorization: request.headers.authorization,
            protocol: request.headers["x-t3-orchestration-protocol"],
          });
          return HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(state), {
              headers: { "content-type": "application/json" },
            }),
          );
        }),
      );
      yield* requestJonesUpdate(entry, { action: "state" }).pipe(
        Effect.provideService(HttpClient.HttpClient, http),
      );
      yield* requestJonesUpdate(entry, {
        action: "install",
        input: {
          stagedHandle: "fixed",
          environmentId: target.environmentId,
          currentVersion: descriptor.serverVersion,
        },
      }).pipe(Effect.provideService(HttpClient.HttpClient, http));
      expect(calls).toEqual([
        {
          method: "GET",
          url: "https://remote.example.test/api/jones-updates",
          authorization: "Bearer synthetic-test-token",
          protocol: undefined,
        },
        {
          method: "POST",
          url: "https://remote.example.test/api/jones-updates/install",
          authorization: "Bearer synthetic-test-token",
          protocol: undefined,
        },
      ]);
    }).pipe(Effect.provide(resolverLayer())),
  );

  it.effect("authenticates relay state reads with initial, zero and later cursors", () =>
    Effect.gen(function* () {
      const relayTarget = new RelayConnectionTarget({
        environmentId: target.environmentId,
        label: "Synthetic relay",
      });
      const origin = "https://updates.example.test";
      const relayPrepared: PreparedConnection = {
        ...prepared,
        target: relayTarget,
        httpBaseUrl: origin,
        socketUrl: "wss://updates.example.test/ws",
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "synthetic-relay-token",
          expiresAtEpochMs: 3_600_000,
        },
      };
      const authorizations: Array<{
        expectedEnvironmentId: EnvironmentId;
        rejectedAccessToken?: string;
      }> = [];
      const authorization = RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("Unexpected bearer authorization"),
        authorizeDpop: () => Effect.die("Update reads must not replace the socket"),
        authorizeDpopHttp: (input) =>
          Effect.sync(() => {
            authorizations.push(input);
            return {
              environmentId: relayTarget.environmentId,
              label: relayTarget.label,
              httpBaseUrl: origin,
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "synthetic-relay-token",
                expiresAtEpochMs: 3_600_000,
              },
            };
          }),
      });
      const proofs: Array<ManagedRelayDpopProofInput> = [];
      const signer = ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("synthetic-thumbprint"),
        createProof: (input) =>
          Effect.sync(() => {
            proofs.push(input);
            return JSON.stringify({ method: input.method, htu: normalizeDpopHtu(input.url) });
          }),
      });
      const calls: Array<{ url: string; method: string; authorization: string | null }> = [];
      const fetchFn: typeof fetch = async (request, init) => {
        const url = String(request);
        const headers = new Headers(init?.headers);
        const proof = JSON.parse(headers.get("dpop")!);
        const method = init?.method ?? "GET";
        calls.push({ url, method, authorization: headers.get("authorization") });
        if (proof.htu !== normalizeDpopHtu(url) || proof.method !== method)
          return Response.json(
            {
              _tag: "EnvironmentAuthInvalidError",
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "synthetic-rejection",
            },
            { status: 401 },
          );
        return Response.json(state);
      };
      const resolver = ConnectionResolver.of({
        prepare: () => Effect.die("Update reads must not prepare ordinary orchestration"),
        prepareForUpdate: () => Effect.succeed({ prepared: relayPrepared, descriptor }),
      });
      yield* Effect.forEach([undefined, 0, 7], (after) =>
        requestJonesUpdate(
          { ...entry, target: relayTarget },
          { action: "state", ...(after === undefined ? {} : { after }) },
        ).pipe(Effect.tap((result) => Effect.sync(() => expect(result).toEqual(state)))),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(ConnectionResolver, resolver),
            Layer.succeed(RemoteEnvironmentAuthorization, authorization),
            Layer.succeed(ManagedRelayDpopSigner, signer),
            layerRemoteHttpClient(fetchFn),
          ),
        ),
      );
      expect(calls).toEqual(
        ["", "?after=0", "?after=7"].map((query) => ({
          url: `https://updates.example.test/api/jones-updates${query}`,
          method: "GET",
          authorization: "DPoP synthetic-relay-token",
        })),
      );
      expect(proofs).toEqual(
        Array(3).fill({
          method: "GET",
          url: "https://updates.example.test/api/jones-updates",
          accessToken: "synthetic-relay-token",
        }),
      );
      expect(authorizations).toEqual(
        Array(3).fill({ expectedEnvironmentId: target.environmentId }),
      );
    }),
  );

  it.effect("rejects a descriptor for another environment before HTTP dispatch", () =>
    Effect.gen(function* () {
      const result = yield* requestJonesUpdate(entry, { action: "state" }).pipe(Effect.result);
      expect(Result.isFailure(result) && result.failure._tag).toBe("JonesUpdateBindingError");
    }).pipe(
      Effect.provide(resolverLayer({ ...descriptor, environmentId: EnvironmentId.make("other") })),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("No request permitted")),
      ),
    ),
  );

  it.effect("rejects stale install version before sending the mutation", () =>
    Effect.gen(function* () {
      const result = yield* requestJonesUpdate(entry, {
        action: "install",
        input: {
          stagedHandle: "fixed",
          environmentId: target.environmentId,
          currentVersion: "old",
        },
      }).pipe(Effect.result);
      expect(Result.isFailure(result) && result.failure._tag).toBe("JonesUpdateBindingError");
    }).pipe(
      Effect.provide(resolverLayer()),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("No mutation permitted")),
      ),
    ),
  );

  it.effect("does not retry an ambiguous install response", () =>
    Effect.gen(function* () {
      let calls = 0;
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          calls++;
          return HttpClientResponse.fromWeb(request, new Response("unavailable", { status: 503 }));
        }),
      );
      const result = yield* requestJonesUpdate(entry, {
        action: "install",
        input: {
          stagedHandle: "fixed",
          environmentId: target.environmentId,
          currentVersion: descriptor.serverVersion,
        },
      }).pipe(Effect.provideService(HttpClient.HttpClient, http), Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      expect(calls).toBe(1);
    }).pipe(Effect.provide(resolverLayer())),
  );
});
