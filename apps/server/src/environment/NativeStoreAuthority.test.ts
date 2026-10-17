// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { ServiceLauncherHostProcess } from "../cloud/serviceLauncherClient.ts";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_RESTART_PENDING_FILE,
  LEGACY_SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../cloud/serviceProtocol.ts";
import * as ServerEnvironment from "./ServerEnvironment.ts";
import {
  fenceNativeStoreAuthority,
  initializeNativeStoreAuthority,
} from "./nativeStoreAuthorityPersistence.ts";
import * as NativeStoreAuthority from "./NativeStoreAuthority.ts";
import * as DesktopNativeOwner from "../jones/workstreams/DesktopNativeOwner.ts";

const authorityLayer = (
  baseDir: string,
  authorityStateDir: string,
  environmentId: string,
  context: string | null = JSON.stringify({
    protocol: SERVICE_LAUNCHER_PROTOCOL,
    childVersion: "1.0.0",
  }),
  connected = true,
) =>
  Layer.mergeAll(
    Layer.succeed(
      HostProcess.Environment,
      context === null ? {} : { [SERVICE_LAUNCHER_CONTEXT_ENV]: context },
    ),
    Layer.succeed(ServiceLauncherHostProcess, {
      connected,
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

it.effect("publishes only the current T3-owned tuple and fails closed when fenced", () =>
  Effect.gen(function* () {
    const root = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-native-authority-layer-test-"),
    );
    try {
      const authorityStateDir = NodePath.join(root, "authority");
      const environmentId = "environment-layer-test";
      NodeFS.mkdirSync(NodePath.join(root, "runtime"), { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(root, "runtime", "service-state.json"),
        JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" }),
        { mode: 0o600 },
      );
      const initial = initializeNativeStoreAuthority(authorityStateDir, environmentId);

      const authority = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, authorityStateDir, environmentId)),
      );
      expect(yield* authority.readCurrent).toEqual({
        environmentId,
        authorityNamespace: initial.authority_namespace,
        storeGeneration: initial.store_generation,
      });
      expect(authority.trustProvider.readTrustSnapshot()).toEqual({
        trustedEnvironments: [
          {
            environmentId,
            authorityNamespace: initial.authority_namespace,
            storeGeneration: initial.store_generation,
          },
        ],
        readiness: "ready",
      });

      NodeFS.writeFileSync(
        NodePath.join(root, "runtime", "service-state.json"),
        JSON.stringify({ protocol: LEGACY_SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" }),
        { mode: 0o600 },
      );
      expect(authority.trustProvider.readTrustSnapshot()).toEqual({
        trustedEnvironments: [],
        readiness: "trust-provider-required",
      });
      NodeFS.writeFileSync(
        NodePath.join(root, "runtime", "service-state.json"),
        JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" }),
        { mode: 0o600 },
      );

      const marker = NodePath.join(root, "runtime", SERVICE_RESTART_PENDING_FILE);
      NodeFS.writeFileSync(marker, "1.0.0");
      expect(authority.trustProvider.readTrustSnapshot().readiness).toBe("trust-provider-required");
      NodeFS.unlinkSync(marker);
      for (const context of [
        null,
        "",
        `{"protocol":${LEGACY_SERVICE_LAUNCHER_PROTOCOL},"childVersion":"1.0.0"}`,
        `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"childVersion":"2.0.0"}`,
      ]) {
        const incompatible = yield* NativeStoreAuthority.make().pipe(
          Effect.provide(authorityLayer(root, authorityStateDir, environmentId, context)),
        );
        expect(incompatible.trustProvider.readTrustSnapshot().readiness).toBe(
          "trust-provider-required",
        );
      }
      const disconnected = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, authorityStateDir, environmentId, undefined, false)),
      );
      expect(disconnected.trustProvider.readTrustSnapshot().readiness).toBe(
        "trust-provider-required",
      );
      fenceNativeStoreAuthority(authorityStateDir, environmentId);
      expect((yield* Effect.result(authority.readCurrent))._tag).toBe("Failure");
      expect(authority.trustProvider.readTrustSnapshot()).toEqual({
        trustedEnvironments: [],
        readiness: "trust-provider-required",
      });
      expect(initial.authority_namespace).toMatch(/^t3-native:/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  }),
);

it.effect("keeps live placement fail-closed when the native authority is missing or unusable", () =>
  Effect.gen(function* () {
    const root = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-native-authority-layer-test-"),
    );
    try {
      const missingAuthority = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(
          authorityLayer(root, NodePath.join(root, "missing-authority"), "environment-missing"),
        ),
      );
      expect(missingAuthority.trustProvider.readTrustSnapshot()).toEqual({
        trustedEnvironments: [],
        readiness: "trust-provider-required",
      });

      const authorityStatePath = NodePath.join(root, "authority-state");
      NodeFS.writeFileSync(authorityStatePath, "not a directory");
      const authority = yield* NativeStoreAuthority.make().pipe(
        Effect.provide(authorityLayer(root, authorityStatePath, "environment-unavailable")),
      );
      expect(authority.trustProvider.readTrustSnapshot()).toEqual({
        trustedEnvironments: [],
        readiness: "trust-provider-required",
      });
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  }),
);

it.effect("keeps desktop enrollment explicit and preserves the tuple across owner restart", () =>
  Effect.gen(function* () {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "desktop-authority-test-"));
    try {
      const authorityStateDir = NodePath.join(root, "native-store-authority");
      const dbPath = NodePath.join(root, "userdata", "statev2.sqlite");
      const environmentId = "desktop-environment";
      let live = true;
      const assertCurrent = () => {
        if (!live) throw new Error("owner disconnected");
      };
      const layer = Layer.mergeAll(
        authorityLayer(root, authorityStateDir, environmentId, null, false),
        Layer.succeed(ServerConfig.ServerConfig, {
          mode: "desktop",
          baseDir: root,
          authorityStateDir,
          dbPath,
        } as ServerConfig.ServerConfig["Service"]),
        Layer.succeed(DesktopNativeOwner.DesktopNativeOwner, {
          available: true,
          binding: {
            home: root,
            databasePath: dbPath,
            profile: NodePath.join(root, "profile"),
            environmentId,
            version: "fixture",
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            manifestSha256: "c".repeat(64),
          },
          assertCurrent,
          refresh: Effect.sync(assertCurrent),
        }),
      );
      const authority = yield* NativeStoreAuthority.make().pipe(Effect.provide(layer));
      expect(authority.trustProvider.readTrustSnapshot().readiness).toBe("trust-provider-required");
      expect(
        NodeFS.existsSync(NodePath.join(authorityStateDir, "native-store-authority-v1.json")),
      ).toBe(false);
      const enrolled = initializeNativeStoreAuthority(authorityStateDir, environmentId);
      const first = yield* authority.readCurrent;
      const restarted = yield* NativeStoreAuthority.make().pipe(Effect.provide(layer));
      expect(yield* restarted.readCurrent).toEqual(first);
      expect(first.authorityNamespace).toBe(enrolled.authority_namespace);
      expect(first.storeGeneration).toBe(1);
      expect(NodeFS.existsSync(NodePath.join(root, "runtime", "service-state.json"))).toBe(false);
      live = false;
      expect(authority.trustProvider.readTrustSnapshot().readiness).toBe("trust-provider-required");
      expect(restarted.trustProvider.readTrustSnapshot().readiness).toBe("trust-provider-required");
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  }),
);
