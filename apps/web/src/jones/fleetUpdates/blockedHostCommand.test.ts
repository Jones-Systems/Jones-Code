import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import {
  EnvironmentRegistry,
  PrimaryConnectionTarget,
  type ConnectionCatalogEntry,
  type NetworkStatus,
} from "@t3tools/client-runtime/connection";
import { makeBlockedHostUpdate } from "./blockedHostCommand";

vi.mock("../../branding", () => ({ APP_SOURCE_SHA: undefined }));

describe("blocked-host update command", () => {
  it.effect(
    "keeps an owner-disabled connection disabled when a status read finds a compatible host",
    () =>
      Effect.gen(function* () {
        const id = EnvironmentId.make("fleet-disabled-host");
        const entry: ConnectionCatalogEntry = {
          target: new PrimaryConnectionTarget({
            environmentId: id,
            label: "Synthetic host",
            httpBaseUrl: "https://synthetic.example.test",
            wsBaseUrl: "wss://synthetic.example.test/ws",
          }),
          profile: Option.none(),
          enabled: false,
          serverUpdateRequired: true,
        };
        const setEnabled = vi.fn(() => Effect.void);
        const setCompatibility = vi.fn(() => Effect.void);
        const request = vi.fn(() =>
          Effect.succeed({
            state: null,
            descriptor: {
              environmentId: id,
              label: "Synthetic host",
              platform: { os: "linux" as const, arch: "x64" as const },
              serverVersion: "0.0.45-preview.20261010.1.1",
              capabilities: { repositoryIdentity: true },
              orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
            },
          }),
        );
        const entries = yield* SubscriptionRef.make<
          ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>
        >(new Map([[id, entry]]));
        const networkStatus = yield* SubscriptionRef.make<NetworkStatus>("online");
        const execute = makeBlockedHostUpdate(request);
        yield* execute({ environmentId: id, request: { action: "state" } }).pipe(
          Effect.provide(
            Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({
              entries,
              networkStatus,
              setEnabled,
              setCompatibility,
            }),
          ),
        );
        expect(request).toHaveBeenCalledWith(entry, { action: "state" });
        expect(setEnabled).not.toHaveBeenCalled();
        expect(setCompatibility).not.toHaveBeenCalled();
        expect((yield* SubscriptionRef.get(entries)).get(id)?.enabled).toBe(false);
      }),
  );
});
