// @effect-diagnostics nodeBuiltinImport:off -- Unconnected sockets model the native request source; no listeners are opened.
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import { AuthAccessWriteScope } from "@t3tools/contracts";
import * as HttpRequest from "effect/http/HttpServerRequest";
import * as Stream from "effect/Stream";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as Cli from "./peerSetupCli.ts";
const input: W.WorkstreamsPeerSetupOwnerRequest = {
  operation: "status",
  setup_id: "setup:test",
  source_environment_id: "member:test",
  expected_home: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  },
  request_sha256: "a".repeat(64),
};
it.effect("CLI passes only a closed selected-home owner operation and prints no credentials", () =>
  Effect.gen(function* () {
    let calls = 0;
    const endpoint = Layer.succeed(Cli.PeerSetupCliEndpoint, {
      run: (home, request) =>
        Effect.sync(() => {
          calls++;
          expect(home).toBe("/synthetic/home");
          expect(request).toEqual(input);
          return {
            contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
            ...input,
            state: "unknown" as const,
            enrollment_id: "enrollment:test",
            bind_id: "bind:test",
          };
        }),
    });
    // Return only the closed result; injected fields must be refused by the endpoint decoder.
    const valid = Layer.succeed(Cli.PeerSetupCliEndpoint, {
      run: () =>
        Effect.succeed({
          contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
          setup_id: input.setup_id,
          source_environment_id: input.source_environment_id,
          expected_home: input.expected_home,
          request_sha256: input.request_sha256,
          state: "unknown" as const,
          enrollment_id: "enrollment:test",
          bind_id: "bind:test",
        }),
    });
    expect(
      (yield* Cli.runPeerSetupCli("/synthetic/home", input).pipe(Effect.provide(valid))).state,
    ).toBe("unknown");
    expect(
      (yield* Cli.runPeerSetupCli("relative", input).pipe(Effect.provide(valid), Effect.result))
        ._tag,
    ).toBe("Failure");
    expect(
      (yield* Cli.runPeerSetupCli("/synthetic/home", {
        ...input,
        url: "https://forged",
      } as W.WorkstreamsPeerSetupOwnerRequest).pipe(Effect.provide(valid), Effect.result))._tag,
    ).toBe("Failure");
    expect(
      (yield* Cli.runPeerSetupCli("/synthetic/home", input).pipe(
        Effect.provide(endpoint),
        Effect.result,
      ))._tag,
    ).toBe("Failure");
    expect(calls).toBe(1);
  }).pipe(Effect.provide(NodeServices.layer)),
);

const localRequest = (
  url = Cli.PEER_SETUP_OWNER_PATH,
  headers: Record<string, string> = {},
  remote = "127.0.0.1",
) => {
  const socket = new NodeNet.Socket();
  Object.defineProperty(socket, "remoteAddress", { value: remote });
  const source = new NodeHttp.IncomingMessage(socket);
  source.method = "POST";
  source.url = url;
  source.headers = headers;
  const request = HttpRequest.fromWeb(
    new Request("http://127.0.0.1:1111" + url, { method: "POST", headers }),
  );
  Object.defineProperty(request, "source", { value: source });
  return { request, close: () => socket.destroy() };
};
it("separate local-owner guard refuses web/cookie/proxy/remote/old-route authority without a listener", () => {
  const principal = { method: "bearer-access-token", scopes: new Set([AuthAccessWriteScope]) };
  const allowed = localRequest();
  expect(() => Cli.assertLocalPeerSetupOwnerRequest(allowed.request, principal)).not.toThrow();
  allowed.close();
  for (const headers of [
    { origin: "http://127.0.0.1:1111" },
    { cookie: "session=sentinel" },
    { forwarded: "for=127.0.0.1" },
    { "x-forwarded-for": "127.0.0.1" },
    { "tailscale-user-login": "owner" },
  ]) {
    const f = localRequest(undefined, headers);
    expect(() => Cli.assertLocalPeerSetupOwnerRequest(f.request, principal)).toThrow();
    f.close();
  }
  for (const url of [
    Cli.PEER_SETUP_OWNER_PATH + "?changed=1",
    "/api/workstreams/native/owner/v1/operations",
  ]) {
    const f = localRequest(url);
    expect(() => Cli.assertLocalPeerSetupOwnerRequest(f.request, principal)).toThrow();
    f.close();
  }
  const remote = localRequest(undefined, {}, "192.0.2.1");
  expect(() => Cli.assertLocalPeerSetupOwnerRequest(remote.request, principal)).toThrow();
  remote.close();
  const f = localRequest();
  expect(() =>
    Cli.assertLocalPeerSetupOwnerRequest(f.request, {
      ...principal,
      method: "browser-session-cookie",
    }),
  ).toThrow();
  expect(() =>
    Cli.assertLocalPeerSetupOwnerRequest(f.request, { ...principal, scopes: new Set() }),
  ).toThrow();
  f.close();
  expect(() =>
    Cli.assertLocalPeerSetupOwnerRequest(
      HttpRequest.fromWeb(
        new Request("http://127.0.0.1:1111" + Cli.PEER_SETUP_OWNER_PATH, { method: "POST" }),
      ),
      principal,
    ),
  ).toThrow();
});
it.effect("owner body is closed and bounded before effects", () =>
  Effect.gen(function* () {
    const bytes = (value: unknown) => Stream.make(new TextEncoder().encode(JSON.stringify(value)));
    expect(yield* Cli.readPeerSetupOwnerRequest(bytes(input))).toEqual(input);
    expect(
      (yield* Cli.readPeerSetupOwnerRequest(bytes({ ...input, callback: "forged" })).pipe(
        Effect.result,
      ))._tag,
    ).toBe("Failure");
    expect(
      (yield* Cli.readPeerSetupOwnerRequest(Stream.make(new Uint8Array(8193))).pipe(Effect.result))
        ._tag,
    ).toBe("Failure");
  }),
);
