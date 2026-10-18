// @effect-diagnostics nodeBuiltinImport:off -- Real authority state lives in a task-owned temporary directory.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ServiceLauncherHostProcess } from "../../cloud/serviceLauncherClient.ts";
import {
  LEGACY_SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../../cloud/serviceProtocol.ts";
import * as ServerConfig from "../../config.ts";
import type { T3PlacementTrustProvider } from "../../environment/NativePlacementTrust.ts";
import * as NativeStoreAuthority from "../../environment/NativeStoreAuthority.ts";
import {
  fenceNativeStoreAuthority,
  initializeNativeStoreAuthority,
} from "../../environment/nativeStoreAuthorityPersistence.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import {
  makeNativeStoreAuthorityConfigReader,
  nativeStoreAuthorityConfig,
} from "./serverConfigNativeStoreAuthority.ts";

const authorityLayer = (baseDir: string, authorityStateDir: string, environmentId: string) =>
  Layer.mergeAll(
    Layer.succeed(HostProcess.Environment, {
      [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        childVersion: "1.0.0",
      }),
    }),
    Layer.succeed(ServiceLauncherHostProcess, {
      connected: true,
      send: () => true,
      on: () => {},
      off: () => {},
    }),
    Layer.succeed(ServerConfig.ServerConfig, {
      baseDir,
      authorityStateDir,
    } as ServerConfig.ServerConfig["Service"]),
    Layer.succeed(
      ServerEnvironment.ServerEnvironmentIdentity,
      ServerEnvironment.ServerEnvironmentIdentity.of({
        getEnvironmentId: Effect.succeed(EnvironmentId.make(environmentId)),
      }),
    ),
  );

const writeServiceState = (root: string, protocol: number) =>
  NodeFS.writeFileSync(
    NodePath.join(root, "runtime", "service-state.json"),
    JSON.stringify({ protocol, activeVersion: "1.0.0" }),
    { mode: 0o600 },
  );

const withAuthorityRoot = <A, E, R>(use: (root: string) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const root = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-server-config-authority-test-"),
      );
      NodeFS.mkdirSync(NodePath.join(root, "runtime"), { recursive: true });
      writeServiceState(root, SERVICE_LAUNCHER_PROTOCOL);
      return root;
    }),
    use,
    (root) => Effect.sync(() => NodeFS.rmSync(root, { recursive: true, force: true })),
  );

const provider = (
  snapshot: ReturnType<T3PlacementTrustProvider["readTrustSnapshot"]>,
): T3PlacementTrustProvider => ({ readTrustSnapshot: () => snapshot });

const descriptor = {
  environmentId: "environment-1",
  authorityNamespace: "t3-native:namespace-1",
  storeGeneration: 2,
};

it.effect("advertises this server's descriptor only while its native authority is active", () =>
  withAuthorityRoot((root) =>
    Effect.gen(function* () {
      const authorityStateDir = NodePath.join(root, "authority");
      const environmentId = "environment-config-test";
      const initial = initializeNativeStoreAuthority(authorityStateDir, environmentId);
      const authority = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, authorityStateDir, environmentId)),
      );
      const readConfig = yield* makeNativeStoreAuthorityConfigReader.pipe(
        Effect.provideService(NativeStoreAuthority.NativeStoreAuthority, authority),
      );

      expect(yield* readConfig(environmentId)).toEqual({
        nativeStoreAuthority: {
          environmentId,
          authorityNamespace: initial.authority_namespace,
          storeGeneration: initial.store_generation,
        },
      });
      // A descriptor for a different environment is never relabelled as this one.
      expect(yield* readConfig("environment-other")).toEqual({});

      writeServiceState(root, LEGACY_SERVICE_LAUNCHER_PROTOCOL);
      expect(yield* readConfig(environmentId)).toEqual({});
      writeServiceState(root, SERVICE_LAUNCHER_PROTOCOL);
      expect((yield* readConfig(environmentId)).nativeStoreAuthority).toBeDefined();

      fenceNativeStoreAuthority(authorityStateDir, environmentId);
      expect(yield* readConfig(environmentId)).toEqual({});
    }),
  ),
);

it.effect("omits the descriptor when the authority is missing or belongs to another store", () =>
  withAuthorityRoot((root) =>
    Effect.gen(function* () {
      const missing = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, NodePath.join(root, "missing"), "environment-a")),
      );
      expect(nativeStoreAuthorityConfig(missing.trustProvider, "environment-a")).toEqual({});

      const authorityStateDir = NodePath.join(root, "authority");
      initializeNativeStoreAuthority(authorityStateDir, "environment-a");
      const mismatched = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, authorityStateDir, "environment-b")),
      );
      expect(nativeStoreAuthorityConfig(mismatched.trustProvider, "environment-b")).toEqual({});
    }),
  ),
);

it("advertises nothing but one ready, well-formed descriptor for this environment", () => {
  expect(
    nativeStoreAuthorityConfig(
      provider({ trustedEnvironments: [descriptor], readiness: "ready" }),
      "environment-1",
    ),
  ).toEqual({ nativeStoreAuthority: descriptor });
  expect(
    nativeStoreAuthorityConfig(
      provider({
        trustedEnvironments: [{ ...descriptor, extra: "dropped" } as typeof descriptor],
        readiness: "ready",
      }),
      "environment-1",
    ),
  ).toEqual({ nativeStoreAuthority: descriptor });

  for (const snapshot of [
    { trustedEnvironments: [descriptor], readiness: "trust-provider-required" as const },
    { trustedEnvironments: [], readiness: "ready" as const },
    { trustedEnvironments: [descriptor, descriptor], readiness: "ready" as const },
    { trustedEnvironments: [{ ...descriptor, storeGeneration: 0 }], readiness: "ready" as const },
  ]) {
    expect(nativeStoreAuthorityConfig(provider(snapshot), "environment-1")).toEqual({});
  }
  expect(
    nativeStoreAuthorityConfig(
      {
        readTrustSnapshot: () => {
          throw new Error("unavailable");
        },
      },
      "environment-1",
    ),
  ).toEqual({});
});
