import { unusedAuthorization } from "../connection/unusedAuthorization.ts";
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

const target = new RelayConnectionTarget({
  environmentId: EnvironmentId.make("fleet-status-relay"),
  label: "Synthetic relay",
});
const origin = "https://fleet.example.test";
const entry: ConnectionCatalogEntry = {
  target,
  profile: Option.none(),
  enabled: false,
  serverUpdateRequired: true,
};
const status: FleetHostStatus = {
  operationProtocol: 1,
  environmentId: target.environmentId,
  enrollment: null,
  operation: null,
  update: null,
};
const directive = {
  directiveId: "33333333-3333-4333-8333-333333333333",
  enrollmentId: "44444444-4444-4444-8444-444444444444",
  environmentId: target.environmentId,
  repository: "Jones-Systems/Jones-Code" as const,
  targetSource: "a".repeat(40),
  targetTree: "b".repeat(40),
  expectedDirectiveId: null,
};
const directiveActions = ["planDirective", "fenceDirective", "releaseDirective"] as const;

function relayTransport(fetchFn: typeof fetch, events: string[] = []) {
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
  const descriptor: ExecutionEnvironmentDescriptor = {
    environmentId: target.environmentId,
    label: target.label,
    platform: { os: "linux", arch: "x64" },
    serverVersion: "0.0.45-preview.20261010.1.1",
    orchestrationProtocolVersion: 1,
    capabilities: { repositoryIdentity: true },
  };
  const authorizations: Array<{
    expectedEnvironmentId: EnvironmentId;
    rejectedAccessToken?: string;
  }> = [];
  const remote = RemoteEnvironmentAuthorization.of({
    ...unusedAuthorization,
    renewDpopBeforeExpiry: () => Effect.die("Unexpected socket renewal in HTTP fixture"),
    rememberCheckedDescriptor: () => Effect.void,
    authorizeBearer: () => Effect.die("Unexpected bearer authorization"),
    authorizeDpop: () => Effect.die("Status reads must not replace the socket"),
    authorizeDpopHttp: (input) =>
      Effect.sync(() => {
        events.push("authorize");
        authorizations.push(input);
        return {
          environmentId: target.environmentId,
          label: target.label,
          httpBaseUrl: origin,
          httpAuthorization:
            input.rejectedAccessToken === undefined
              ? authorization
              : { ...authorization, accessToken: "synthetic-renewed-token" },
        };
      }),
  });
  const proofs: Array<ManagedRelayDpopProofInput> = [];
  const signer = ManagedRelayDpopSigner.of({
    thumbprint: Effect.succeed("synthetic-thumbprint"),
    createProof: (input) =>
      Effect.sync(() => {
        events.push("proof");
        proofs.push(input);
        return JSON.stringify({ method: input.method, htu: normalizeDpopHtu(input.url) });
      }),
  });
  const resolver = ConnectionResolver.of({
    prepare: () => Effect.die("Fleet requests must not prepare ordinary orchestration"),
    prepareForUpdate: () =>
      Effect.sync(() => {
        events.push("prepare");
        return { prepared, descriptor };
      }),
  });
  return {
    authorizations,
    proofs,
    layer: Layer.mergeAll(
      Layer.succeed(ConnectionResolver, resolver),
      Layer.succeed(RemoteEnvironmentAuthorization, remote),
      Layer.succeed(ManagedRelayDpopSigner, signer),
      layerRemoteHttpClient(fetchFn),
    ),
  };
}

const invalidCredential = () =>
  Response.json(
    {
      _tag: "EnvironmentAuthInvalidError",
      code: "auth_invalid",
      reason: "invalid_credential",
      traceId: "synthetic-rejection",
    },
    { status: 401 },
  );

it.effect("authenticates relay status reads and each exact directive endpoint", () =>
  Effect.gen(function* () {
    const calls: Array<{ url: string; method: string; authorization: string | null }> = [];
    const fetchFn: typeof fetch = async (request, init) => {
      const url = String(request);
      const headers = new Headers(init?.headers);
      const proof = JSON.parse(headers.get("dpop")!);
      const method = init?.method ?? "GET";
      calls.push({ url, method, authorization: headers.get("authorization") });
      if (method === "PUT" || method === "POST")
        expect(JSON.parse(String(init?.body))).toEqual(directive);
      if (proof.htu !== normalizeDpopHtu(url) || proof.method !== method)
        return invalidCredential();
      return Response.json(
        url.endsWith("/fence")
          ? {
              ...status,
              fencePlanDirective: 1,
              directiveFence: { protocol: 1, outcome: "fenced", input: directive },
            }
          : status,
      );
    };
    const { authorizations, proofs, layer } = relayTransport(fetchFn);
    const operationId = "22222222-2222-4222-8222-222222222222";
    yield* Effect.forEach([undefined, operationId], (id) =>
      requestFleetHost(entry, {
        action: "status",
        ...(id === undefined ? {} : { operationId: id }),
      }).pipe(Effect.tap((result) => Effect.sync(() => expect(result).toEqual(status)))),
    ).pipe(Effect.provide(layer));
    yield* Effect.forEach(directiveActions, (action) =>
      requestFleetHost(entry, { action, input: directive, beforeSend: () => true }).pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            if (action === "fenceDirective")
              expect(result.directiveFence).toEqual({
                protocol: 1,
                outcome: "fenced",
                input: directive,
              });
          }),
        ),
      ),
    ).pipe(Effect.provide(layer));
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
      {
        url: "https://fleet.example.test/api/jones-fleet-updates/plan-directive",
        method: "PUT",
        authorization: "DPoP synthetic-relay-token",
      },
      {
        url: "https://fleet.example.test/api/jones-fleet-updates/plan-directive/fence",
        method: "POST",
        authorization: "DPoP synthetic-relay-token",
      },
      {
        url: "https://fleet.example.test/api/jones-fleet-updates/plan-directive/release",
        method: "POST",
        authorization: "DPoP synthetic-relay-token",
      },
    ]);
    // The signer receives each exact request URL; its proof claim omits the operation query.
    expect(proofs).toEqual(
      calls.map(({ url, method }) => ({ method, url, accessToken: "synthetic-relay-token" })),
    );
    expect(proofs.map((proof) => normalizeDpopHtu(proof.url))).toEqual([
      "https://fleet.example.test/api/jones-fleet-updates",
      "https://fleet.example.test/api/jones-fleet-updates",
      "https://fleet.example.test/api/jones-fleet-updates/plan-directive",
      "https://fleet.example.test/api/jones-fleet-updates/plan-directive/fence",
      "https://fleet.example.test/api/jones-fleet-updates/plan-directive/release",
    ]);
    expect(authorizations).toEqual(
      calls.map(() => ({ expectedEnvironmentId: target.environmentId })),
    );
  }),
);

it.effect.each(directiveActions)(
  "honors a veto for %s after route and proof preparation",
  (action) =>
    Effect.gen(function* () {
      const events: string[] = [];
      const { layer } = relayTransport(async () => {
        events.push("fetch");
        return Response.json(status);
      }, events);
      const result = yield* requestFleetHost(entry, {
        action,
        input: directive,
        beforeSend: () => {
          events.push("guard");
          return false;
        },
      }).pipe(Effect.provide(layer), Effect.result);
      expect(events).toEqual(["prepare", "authorize", "proof", "guard"]);
      expect(result._tag).toBe("Failure");
    }),
);

it.effect("rechecks the directive guard after renewing a rejected relay credential", () =>
  Effect.gen(function* () {
    const events: string[] = [];
    const { authorizations, proofs, layer } = relayTransport(async () => {
      events.push("fetch");
      return invalidCredential();
    }, events);
    let attempts = 0;
    const result = yield* requestFleetHost(entry, {
      action: "planDirective",
      input: directive,
      beforeSend: async () => {
        events.push("guard");
        return ++attempts === 1;
      },
    }).pipe(Effect.provide(layer), Effect.result);
    expect(events).toEqual([
      "prepare",
      "authorize",
      "proof",
      "guard",
      "fetch",
      "authorize",
      "proof",
      "guard",
    ]);
    expect(authorizations).toEqual([
      { expectedEnvironmentId: target.environmentId },
      {
        expectedEnvironmentId: target.environmentId,
        rejectedAccessToken: "synthetic-relay-token",
      },
    ]);
    expect(proofs.map((proof) => proof.accessToken)).toEqual([
      "synthetic-relay-token",
      "synthetic-renewed-token",
    ]);
    expect(result._tag).toBe("Failure");
  }),
);
