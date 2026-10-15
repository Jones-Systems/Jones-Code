import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import { RelayConnectionTarget, type PreparedConnection } from "@t3tools/client-runtime/connection";
import { RemoteEnvironmentAuthorization } from "@t3tools/client-runtime/authorization";
import { ManagedRelay } from "@t3tools/client-runtime/relay";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import * as Effect from "effect/Effect";
import { workstreamPageUrl, workstreamRequest } from "./gateway";

it.effect("signs the same encoded endpoint and pagination URL that the mobile client sends", () =>
  Effect.gen(function* () {
    const environmentId = EnvironmentId.make("fixture");
    const prepared: PreparedConnection = {
      environmentId,
      label: "Fixture",
      target: new RelayConnectionTarget({ environmentId, label: "Fixture" }),
      httpBaseUrl: "https://old.test",
      socketUrl: "wss://old.test/ws",
      httpAuthorization: { _tag: "Dpop", accessToken: "expired", expiresAtEpochMs: 0 },
    };
    const proofs: ManagedRelay.ManagedRelayDpopProofInput[] = [];
    const calls: Array<{ url: string; method: string }> = [];
    const signer = ManagedRelay.ManagedRelayDpopSigner.of({
      thumbprint: Effect.succeed("fixture-thumbprint"),
      createProof: (input) =>
        Effect.sync(() => {
          proofs.push(input);
          return "fixture-proof";
        }),
    });
    const authorization = RemoteEnvironmentAuthorization.of({
      authorizeBearer: () => Effect.die("unexpected"),
      authorizeDpop: () => Effect.die("unexpected"),
      authorizeDpopHttp: () =>
        Effect.succeed({
          environmentId,
          label: "Fixture",
          httpBaseUrl: "https://relay.test",
          httpAuthorization: {
            _tag: "Dpop",
            accessToken: "fresh-token",
            expiresAtEpochMs: 3600000,
          },
        }),
    });
    const http = layerRemoteHttpClient(async (url, init) => {
      calls.push({ url: String(url), method: init?.method ?? "GET" });
      expect(new Headers(init?.headers).get("authorization")).toBe("DPoP fresh-token");
      expect(new Headers(init?.headers).get("dpop")).toBe("fixture-proof");
      return new Response("Missing", { status: 404 });
    });
    const missing = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.provide(http),
        Effect.provideService(ManagedRelay.ManagedRelayDpopSigner, signer),
        Effect.provideService(RemoteEnvironmentAuthorization, authorization),
        Effect.flip,
        Effect.tap((error) =>
          Effect.sync(() =>
            expect(error).toMatchObject({
              _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
              status: 404,
            }),
          ),
        ),
      );
    for (const cursor of [undefined, "next_page-2"]) {
      const payload = { limit: 17, ...(cursor === undefined ? {} : { cursor }) };
      yield* missing(
        workstreamRequest(
          prepared,
          "GET",
          (urls) => workstreamPageUrl(urls.list(), payload),
          (client, headers) => client.list({ headers, payload }),
        ),
      );
    }
    const params = { workstreamId: "workstream:alpha" };
    const payload = { limit: 19, cursor: "history_page-3" };
    yield* missing(
      workstreamRequest(
        prepared,
        "GET",
        (urls) => workstreamPageUrl(urls.history({ params }), payload),
        (client, headers) => client.history({ headers, params, payload }),
      ),
    );
    const commandParams = { commandId: "command:fixture:0001" };
    yield* missing(
      workstreamRequest(
        prepared,
        "GET",
        (urls) => urls.command({ params: commandParams }),
        (client, headers) => client.command({ headers, params: commandParams }),
      ),
    );
    yield* missing(
      workstreamRequest(
        prepared,
        "POST",
        (urls) => urls.appearanceRead(),
        (client, headers) => client.appearanceRead({ headers, payload: { workstream_ids: [] } }),
      ),
    );
    expect(calls).toEqual([
      { method: "GET", url: "https://relay.test/api/workstreams?limit=17" },
      { method: "GET", url: "https://relay.test/api/workstreams?limit=17&cursor=next_page-2" },
      {
        method: "GET",
        url: "https://relay.test/api/workstreams/workstream%3Aalpha/history?limit=19&cursor=history_page-3",
      },
      {
        method: "GET",
        url: "https://relay.test/api/workstreams/commands/command%3Afixture%3A0001",
      },
      { method: "POST", url: "https://relay.test/api/workstreams/appearance/read" },
    ]);
    expect(proofs).toEqual(
      calls.map(({ url, method }) => ({
        method,
        url,
        accessToken: "fresh-token",
      })),
    );
  }),
);

it("preserves existing query keys and omits absent pagination fields", () => {
  const endpoint = "https://fixture.test/api/workstreams?existing=kept";
  expect(workstreamPageUrl(endpoint, {})).toBe(endpoint);
  expect(workstreamPageUrl(endpoint, { limit: 7, cursor: "next_page" })).toBe(
    `${endpoint}&limit=7&cursor=next_page`,
  );
});
