import * as NodeNet from "node:net";

import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NetAddress from "effect/net/NetAddress";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse, HttpServer } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import { parseServiceLauncherArgs, runServiceLauncherCli } from "../../cli/serviceLauncher.ts";
import { isJonesBootServiceCgroup } from "../../cli/update.ts";
import * as ConnectReachability from "../../cloud/ConnectReachability.ts";
import * as ManagedEndpointRuntime from "../../cloud/ManagedEndpointRuntime.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as ServiceLauncher from "../../serviceLauncher.ts";
import { QualifiedCommitFailure } from "../cloud/qualifiedCommitAttempt.ts";

vi.mock("node:net", { spy: true });
vi.mock("../../serviceLauncher.ts", () => ({ main: vi.fn() }));

it("keeps Windows launcher arguments and nested classified refusals at the CLI boundary", async () => {
  const options = parseServiceLauncherArgs([
    "--base-dir",
    "/synthetic/home",
    "--log-file",
    "/synthetic/log",
    "--service-unit",
    "JonesTask",
  ]);
  expect(options).toEqual({
    baseDir: "/synthetic/home",
    logFile: "/synthetic/log",
    serviceUnit: "JonesTask",
  });
  expect(parseServiceLauncherArgs(["--unknown", "value"])).toBeUndefined();
  expect(parseServiceLauncherArgs(["--base-dir"])).toBeUndefined();
  const originalExitCode = process.exitCode;
  const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
  try {
    const failure = new QualifiedCommitFailure(
      { updateId: "synthetic-update", step: "pointer-write" },
      Object.assign(new Error("private cause details"), { code: "ENOSPC" }),
    );
    vi.mocked(ServiceLauncher.main).mockRejectedValueOnce(
      new Error("Launcher shutdown evidence could not be reconciled.", { cause: failure }),
    );
    await runServiceLauncherCli(options);
    expect(ServiceLauncher.main).toHaveBeenLastCalledWith(options);
    expect(process.exitCode).toBe(1);
    const diagnostic = String(write.mock.calls[0]?.[0]);
    expect(diagnostic).toContain("Launcher shutdown evidence could not be reconciled.");
    expect(diagnostic).toContain("pointer-write (update synthetic-update)");
    expect(diagnostic).toContain("ENOSPC");
    expect(diagnostic).toContain("paired state is retained for reconciliation");
    expect(diagnostic).not.toContain("private cause details");

    write.mockClear();
    vi.mocked(ServiceLauncher.main).mockRejectedValueOnce(
      new Error("Launcher refused", {
        cause: Object.assign(new Error("secret"), { code: "EACCES" }),
      }),
    );
    await runServiceLauncherCli();
    expect(String(write.mock.calls[0]?.[0])).toContain("Launcher refused <- caused by: EACCES");
    expect(String(write.mock.calls[0]?.[0])).not.toContain("secret");
  } finally {
    process.exitCode = originalExitCode;
    write.mockRestore();
    vi.mocked(ServiceLauncher.main).mockReset();
  }
});

it.each([
  [
    "0::/user.slice/user-1000.slice/user@1000.service/app.slice/jones-code.service/child\n",
    undefined,
    true,
  ],
  ["0::/user.slice/app.slice/jones-code.service-other\n", undefined, false],
  ["0::/system.slice/t3code.service\n", undefined, false],
  ["0::/system.slice/host-tooling-t3.service/child\n", "host-tooling-t3.service", true],
  ["0::/system.slice/other.service\n", "host-tooling-t3.service", false],
  ["0::/system.slice/jones-code.service\n", "host-tooling-t3.service", false],
  ["0::/user.slice/user@1000.service/host-tooling-t3.service\n", "host-tooling-t3.service", false],
  ["0::/system.slice/a.service\n0::/system.slice/b.service\n", "a.service", false],
  ["", "host-tooling-t3.service", false],
] as const)(
  "classifies exact service cgroup %s with binding %s",
  (contents, boundUnit, expected) => {
    expect(isJonesBootServiceCgroup(contents, boundUnit)).toBe(expected);
  },
);

const environmentId = EnvironmentId.make("synthetic-connect-composition");
const descriptor = {
  environmentId,
  label: "Synthetic server",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
};

const localCases = [
  [JSON.stringify(descriptor), 200, "expected environment identity"],
  [
    JSON.stringify({ ...descriptor, environmentId: "foreign-environment" }),
    200,
    "different environment",
  ],
  [JSON.stringify({ environmentId }), 200, "reachability is unverified"],
  ["x".repeat(64 * 1024 + 1), 200, "reachability is unverified"],
  [JSON.stringify(descriptor), 403, "reachability is unverified"],
] as const;

describe("Connect caller composition", () => {
  it.each(["matching_environment", "different_environment", "unverified", undefined] as const)(
    "does not promote missing public evidence from local evidence %s",
    (local) => {
      const result = ConnectReachability.classifyReachability(
        {
          managedTunnelConfigured: true,
          connector: {
            pid: 7,
            readyConnections: 4,
            hostname: "env.example.test",
            origin: null,
            protocol: "quic",
          },
          endpoint: null,
          // The undefined case omits local evidence, as callers without it do.
          ...(local === undefined ? {} : { local }),
          edgeReachable: true,
          exitNode: null,
        },
        "2026-11-17T00:00:00.000Z",
      );
      expect(result).toMatchObject({ status: "unreachable", cause: "connector_not_ready" });
    },
  );

  it.effect(
    "keeps anonymous HTTP evidence and bounded local identity without inventing authenticated reachability",
    () =>
      Effect.gen(function* () {
        using _connect = vi.spyOn(NodeNet, "connect").mockImplementation(() => {
          const socket = new NodeNet.Socket();
          queueMicrotask(() => socket.emit("connect"));
          return socket;
        });
        for (const [body, status, localReason] of localCases) {
          const requests: string[] = [];
          let restarts = 0;
          const layer = ConnectReachability.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.mock(ManagedEndpointRuntime.CloudManagedEndpointRuntime)({
                  inspectConnector: Effect.succeed({
                    pid: 7,
                    readyConnections: 4,
                    hostname: "env.example.test",
                    origin: null,
                    protocol: "quic",
                  }),
                  readinessChanged: Stream.never,
                  restartConnector: () =>
                    Effect.sync(() => {
                      restarts++;
                      return false;
                    }),
                }),
                Layer.mock(ServerSecretStore.ServerSecretStore)({
                  get: () => Effect.succeedSome(new Uint8Array()),
                }),
                Layer.mock(HttpServer.HttpServer)({
                  address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43129),
                }),
                Layer.succeed(ServerEnvironment.ServerEnvironment, {
                  getEnvironmentId: Effect.succeed(environmentId),
                  getDescriptor: Effect.die("The actual loopback HTTP response must be decoded"),
                }),
                Layer.succeed(
                  HttpClient.HttpClient,
                  HttpClient.make((request) => {
                    requests.push(request.url);
                    const local = request.url.startsWith("http://127.0.0.1:43129/");
                    return Effect.succeed(
                      HttpClientResponse.fromWeb(
                        request,
                        new Response(local ? body : "private response body", {
                          status: local ? status : 403,
                          headers: local
                            ? {}
                            : {
                                "content-type": "text/html; charset=utf-8",
                                "cf-ray": "0123456789abcdef-IAD",
                                "set-cookie": "private-cookie",
                                location: "https://private.example/?token=private",
                              },
                        }),
                      ),
                    );
                  }),
                ),
                Layer.succeed(
                  ChildProcessSpawner.ChildProcessSpawner,
                  ChildProcessSpawner.make(() => Effect.die("no tailscale")),
                ),
              ),
            ),
          );
          yield* Effect.gen(function* () {
            const service = yield* ConnectReachability.ConnectReachability;
            const result = yield* service.check;
            expect(result).toMatchObject({ status: "unreachable", cause: "unexpected_response" });
            if (result.status !== "unreachable") throw new Error("Unexpected reachability success");
            expect(result.reason).toContain("HTTP 403");
            expect(result.reason).toContain(localReason);
            expect(result.reason).toContain("signed-in device is unverified");
            expect(result.hint).toContain("Content type: text/html");
            expect(result.hint).toContain("0123456789abcdef-IAD");
            expect(result.hint).toContain("does not identify the cause");
            expect(result.fix).toContain("Copy diagnostics");
            expect(result.fix).not.toContain("Restart");
            expect(JSON.stringify(result)).not.toContain("private");
            expect(ConnectReachability.formatReachabilityFailure(result)).toContain(
              "could not be confirmed from this host",
            );
            expect(requests).toEqual([
              "https://env.example.test/.well-known/t3/environment",
              "http://127.0.0.1:43129/.well-known/t3/environment",
            ]);
            expect(restarts).toBe(0);
            expect(yield* service.latest).toMatchObject({ value: result });
          }).pipe(Effect.provide(layer));
        }
      }),
  );
});
