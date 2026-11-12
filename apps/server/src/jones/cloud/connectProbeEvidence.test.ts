import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NetAddress from "effect/net/NetAddress";
import { HttpClient, HttpClientResponse, HttpServer } from "effect/http";

import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import {
  connectHttpEvidence,
  probeConnectLoopback,
  unexpectedConnectResponse,
} from "./connectProbeEvidence.ts";

const environmentId = EnvironmentId.make("synthetic-connect-environment");
const descriptor = {
  environmentId,
  label: "Synthetic server",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
};

describe("Connect host probe evidence", () => {
  it("keeps only bounded allowlisted response metadata, without attributing the refusal", () => {
    const evidence = connectHttpEvidence(403, {
      "cf-ray": "0123456789abcdef-IAD",
      "content-type": "text/html; charset=utf-8",
      "set-cookie": "private",
      location: "https://example.test/?token=private",
      server: "private",
    });
    expect(evidence).toEqual({
      status: 403,
      contentType: "text/html",
      rayId: "0123456789abcdef-IAD",
    });
    expect(
      connectHttpEvidence(403, { "cf-ray": "private\nheader", "content-type": "x".repeat(1000) }),
    ).toEqual({ status: 403 });
    const explanation = unexpectedConnectResponse(evidence, "matching_environment");
    expect(explanation.reason).toContain("expected environment identity");
    expect(explanation.reason).toContain("signed-in device is unverified");
    expect(explanation.hint).toContain("does not identify the cause");
    expect(explanation.fix).not.toContain("Restart");
  });

  it.effect("checks the actual listener and requires a fully decoded matching descriptor", () =>
    Effect.gen(function* () {
      for (const [body, status, expected] of [
        [JSON.stringify(descriptor), 200, "matching_environment"],
        [
          JSON.stringify({ ...descriptor, environmentId: "another-environment" }),
          200,
          "different_environment",
        ],
        [JSON.stringify({ environmentId }), 200, "unverified"],
        [JSON.stringify(descriptor), 403, "unverified"],
        [JSON.stringify(descriptor), 302, "unverified"],
        ["x".repeat(64 * 1024 + 1), 200, "unverified"],
      ] as const) {
        const requests: string[] = [];
        const client = HttpClient.make((request) => {
          requests.push(request.url);
          return Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response(body, { status })),
          );
        });
        const observed = yield* probeConnectLoopback().pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide(Layer.mock(HttpServer.HttpServer)({
            address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43129),
          })),
          Effect.provideService(ServerEnvironment.ServerEnvironment, {
            getEnvironmentId: Effect.succeed(environmentId),
            getDescriptor: Effect.die("No direct descriptor shortcut in probe"),
          }),
        );
        expect(requests).toEqual(["http://127.0.0.1:43129/.well-known/t3/environment"]);
        expect(observed).toBe(expected);
      }
    }),
  );
});
