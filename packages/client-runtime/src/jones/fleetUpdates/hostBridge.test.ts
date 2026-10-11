import { EnvironmentId, type ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import { expect, it } from "@effect/vitest";
import { normalizeDpopHtu } from "@t3tools/shared/dpopCommon";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { RelayConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import {
  ManagedRelayDpopSigner,
  type ManagedRelayDpopProofInput,
} from "../../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import { requestFleetHost } from "./hostBridge.ts";

it.effect("authenticates relay status reads with and without an operation ID", () =>
  Effect.gen(function* () {
    const target = new RelayConnectionTarget({
      environmentId: EnvironmentId.make("fleet-status-relay"),
      label: "Synthetic relay",
    });
    const origin = "https://fleet.example.test";
    const authorization = {
      _tag: "Dpop" as const,
      accessToken: "synthetic-relay-token",
      expiresAtEpochMs: 3_600_000,
    };
    const prepared: PreparedConnection = {
      environmentId: target.environmentId,
      label: target.label,
      httpBaseUrl: origin,
      socketUrl: "wss://fleet.example.test/ws",
      httpAuthorization: authorization,
      target,
    };
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
      serverVersion: "0.0.45-preview.20261010.1.1",
      orchestrationProtocolVersion: 1,
      capabilities: { repositoryIdentity: true },
    };
    const status: FleetHostStatus = {
      operationProtocol: 1,
      environmentId: target.environmentId,
      enrollment: null,
      operation: null,
      update: null,
    };
    const authorizations: Array<{
      expectedEnvironmentId: EnvironmentId;
      rejectedAccessToken?: string;
    }> = [];
    const remote = RemoteEnvironmentAuthorization.of({
      authorizeBearer: () => Effect.die("Unexpected bearer authorization"),
      authorizeDpop: () => Effect.die("Status reads must not replace the socket"),
      authorizeDpopHttp: (input) =>
        Effect.sync(() => {
          authorizations.push(input);
          return {
            environmentId: target.environmentId,
            label: target.label,
            httpBaseUrl: origin,
            httpAuthorization: authorization,
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
      return Response.json(status);
    };
    const resolver = ConnectionResolver.of({
      prepare: () => Effect.die("Status reads must not prepare ordinary orchestration"),
      prepareForUpdate: () => Effect.succeed({ prepared, descriptor }),
    });
    const operationId = "22222222-2222-4222-8222-222222222222";
    yield* Effect.forEach([undefined, operationId], (id) =>
      requestFleetHost(entry, {
        action: "status",
        ...(id === undefined ? {} : { operationId: id }),
      }).pipe(Effect.tap((result) => Effect.sync(() => expect(result).toEqual(status)))),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(ConnectionResolver, resolver),
          Layer.succeed(RemoteEnvironmentAuthorization, remote),
          Layer.succeed(ManagedRelayDpopSigner, signer),
          layerRemoteHttpClient(fetchFn),
        ),
      ),
    );
    expect(calls).toEqual([
      {
        url: "https://fleet.example.test/api/jones-fleet-updates",
        method: "GET",
        authorization: "DPoP synthetic-relay-token",
      },
      {
        url: "https://fleet.example.test/api/jones-fleet-updates?operationId=22222222-2222-4222-8222-222222222222",
        method: "GET",
        authorization: "DPoP synthetic-relay-token",
      },
    ]);
    expect(proofs).toEqual(
      Array(2).fill({
        method: "GET",
        url: "https://fleet.example.test/api/jones-fleet-updates",
        accessToken: "synthetic-relay-token",
      }),
    );
    expect(authorizations).toEqual(Array(2).fill({ expectedEnvironmentId: target.environmentId }));
  }),
);
