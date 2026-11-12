// @effect-diagnostics nodeBuiltinImport:off - reads the user guide to check its anchors.
import * as NodeFS from "node:fs";

import { EnvironmentId, T3_CONNECT_TROUBLESHOOTING_SECTIONS } from "@t3tools/contracts";
import { describe, expect, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as NetAddress from "effect/net/NetAddress";
import { HttpClient, HttpClientResponse, HttpServer } from "effect/http";
import { ChildProcessSpawner } from "effect/process";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ConnectReachability from "./ConnectReachability.ts";
import * as ManagedEndpointRuntime from "./ManagedEndpointRuntime.ts";
import {
  classifyReachability,
  formatReachabilityFailure,
  type ReachabilityFacts,
} from "./ConnectReachability.ts";

vi.mock("node:net", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:net")>();
  const { EventEmitter } = await import("node:events");
  return {
    ...original,
    connect: () => {
      const socket = Object.assign(new EventEmitter(), {
        destroy: () => undefined,
        setTimeout: () => undefined,
      });
      queueMicrotask(() => socket.emit("connect"));
      return socket;
    },
  };
});

vi.mock("@t3tools/tailscale", async (importOriginal) => {
  const original = await importOriginal<typeof import("@t3tools/tailscale")>();
  const Effect = await import("effect/Effect");
  return { ...original, readTailscaleExitNode: Effect.succeed(null) };
});

const checkedAt = "2026-10-19T00:00:00.000Z";
const connected = { readyConnections: 4, hostname: "env.example.test", protocol: "quic" };
const notConnected = { readyConnections: 0, hostname: null, protocol: null };
const facts = (overrides: Partial<ReachabilityFacts>): ReachabilityFacts => ({
  managedTunnelConfigured: true,
  connector: connected,
  endpoint: null,
  edgeReachable: null,
  exitNode: null,
  ...overrides,
});

it.effect("owns loopback probe dependencies after service construction", () =>
  Effect.gen(function* () {
    const environmentId = EnvironmentId.make("synthetic-probe-owner");
    const requests: string[] = [];
    const client = HttpClient.make((request) => {
      requests.push(request.url);
      const local = request.url.startsWith("http://127.0.0.1:43129/");
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          local
            ? Response.json({
                environmentId,
                label: "Synthetic server",
                platform: { os: "darwin", arch: "arm64" },
                serverVersion: "0.0.0-test",
                capabilities: { repositoryIdentity: true },
              })
            : new Response("", { status: 403 }),
        ),
      );
    });
    const context = yield* Layer.build(
      ConnectReachability.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(HttpClient.HttpClient, client),
            Layer.mock(HttpServer.HttpServer)({
              address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43129),
            }),
            Layer.succeed(ServerEnvironment.ServerEnvironment, {
              getEnvironmentId: Effect.succeed(environmentId),
              getDescriptor: Effect.die("Probe must use the listener"),
            }),
            Layer.mock(ServerSecretStore.ServerSecretStore)({
              get: () => Effect.succeedSome(new Uint8Array()),
            }),
            Layer.mock(ManagedEndpointRuntime.CloudManagedEndpointRuntime)({
              inspectConnector: Effect.succeed(connected),
              readinessChanged: Stream.never,
            }),
            Layer.mock(ChildProcessSpawner.ChildProcessSpawner)({}),
          ),
        ),
      ),
    );
    const service = yield* ConnectReachability.ConnectReachability.pipe(Effect.provide(context));
    // Caller has none of the construction services. The 403 takes the actual
    // loopback branch, rather than only testing the public 200 short circuit.
    const result = yield* service.check;
    expect(result).toMatchObject({ status: "unreachable", cause: "unexpected_response" });
    expect(result.status === "unreachable" && result.reason).toContain("server answers locally");
    expect(requests).toEqual([
      "https://env.example.test/.well-known/t3/environment",
      "http://127.0.0.1:43129/.well-known/t3/environment",
    ]);
    expect(Option.getOrThrow(yield* service.latest)).toEqual(result);
  }),
);

describe("classifyReachability", () => {
  it("keeps an anonymous 403 separate from local health and signed-in reachability", () => {
    const result = classifyReachability(
      facts({
        endpoint: { kind: "status", status: 403, rayId: "0123456789abcdef-IAD" },
        local: "matching_environment",
        edgeReachable: true,
      }),
      checkedAt,
    );
    expect(result).toMatchObject({ status: "unreachable", cause: "unexpected_response" });
    if (result.status !== "unreachable") throw new Error("Expected host probe failure");
    const text = formatReachabilityFailure(result);
    expect(text).toContain("could not be confirmed from this host");
    expect(text).toContain("HTTP 403");
    expect(text).toContain("server answers locally");
    expect(text).toContain("signed-in device is unverified");
    expect(text).not.toContain("NOT reachable");
    expect(text).not.toContain("Restart");
  });

  it("does not claim local health for an absent or mismatched descriptor", () => {
    for (const local of [undefined, "unverified", "different_environment"] as const) {
      const result = classifyReachability(
        facts({
          endpoint: { kind: "status", status: 403 },
          ...(local === undefined ? {} : { local }),
        }),
        checkedAt,
      );
      expect(result.status === "unreachable" && result.reason).not.toContain(
        "server answers locally",
      );
    }
  });

  it.each([
    [{ kind: "status", status: 502 }, "origin_unreachable"],
    [{ kind: "status", status: 530 }, "no_connector"],
    [{ kind: "network_error" }, "timeout"],
  ] as const)("maps endpoint result %o to %s", (endpoint, cause) => {
    expect(classifyReachability(facts({ endpoint, edgeReachable: true }), checkedAt)).toMatchObject(
      { status: "unreachable", cause },
    );
  });

  it("keeps the endpoint cause when the TCP edge test cannot explain it", () => {
    // QUIC (UDP) can connect while TCP 7844 is blocked.
    expect(
      classifyReachability(
        facts({ endpoint: { kind: "status", status: 502 }, edgeReachable: false }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "origin_unreachable" });
    expect(
      classifyReachability(
        facts({
          connector: { readyConnections: null, hostname: "env.example.test", protocol: null },
          endpoint: { kind: "status", status: 502 },
          edgeReachable: false,
        }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "origin_unreachable" });
    // Failed DNS also fails the edge lookups.
    expect(
      classifyReachability(
        facts({
          connector: { readyConnections: null, hostname: "env.example.test", protocol: null },
          endpoint: { kind: "dns_failed" },
          edgeReachable: false,
        }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "dns_failed" });
  });

  it("waits on a connector that has not connected while the edge answers", () => {
    expect(
      classifyReachability(facts({ connector: notConnected, edgeReachable: true }), checkedAt),
    ).toMatchObject({ cause: "connector_not_ready" });
  });
});

describe("T3 Connect troubleshooting links", () => {
  it("each section names a heading in the user guide", () => {
    const guide = NodeFS.readFileSync(
      new URL("../../../../docs/user/t3-connect-troubleshooting.md", import.meta.url),
      "utf8",
    );
    // GitHub's heading anchors: lowercase, drop punctuation, spaces become hyphens.
    const anchors = new Set(
      [...guide.matchAll(/^#{2,3} (.+)$/gm)].map(([, heading]) =>
        heading!
          .toLowerCase()
          .replace(/[^\p{L}\p{N} _-]/gu, "")
          .replace(/ /g, "-"),
      ),
    );
    for (const section of T3_CONNECT_TROUBLESHOOTING_SECTIONS) {
      expect(anchors, section).toContain(section);
    }
  });
});
