import { describe, expect, it, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { requestJonesUpdateWithDescriptor } from "@t3tools/client-runtime/jones/fleet-updates";
import { executeBlockedHostUpdate } from "./blockedHostCommand";

vi.mock("../../branding", () => ({ APP_SOURCE_SHA: undefined }));
vi.mock("@t3tools/client-runtime/jones/fleet-updates", () => ({
  requestJonesUpdateWithDescriptor: vi.fn(),
}));

describe("blocked-host update command", () => {
  it("keeps an owner-disabled connection disabled when a status read finds a compatible host", async () => {
    const id = EnvironmentId.make("fleet-disabled-host");
    const entry = { target: { environmentId: id }, enabled: false, serverUpdateRequired: true };
    const setEnabled = vi.fn();
    const setCompatibility = vi.fn();
    vi.mocked(requestJonesUpdateWithDescriptor).mockReturnValue(
      Effect.succeed({
        state: null,
        descriptor: {
          environmentId: id,
          label: "Synthetic host",
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.0.45-preview.20261010.1.1",
          capabilities: { repositoryIdentity: true },
          orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        },
      }) as ReturnType<typeof requestJonesUpdateWithDescriptor>,
    );
    const entries = await Effect.runPromise(SubscriptionRef.make(new Map([[id, entry]])));
    // The bridge mock supplies a context-free effect; no transport services execute here.
    await Effect.runPromise(
      executeBlockedHostUpdate({ environmentId: id, request: { action: "state" } }).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, {
          entries,
          setEnabled,
          setCompatibility,
        } as unknown as EnvironmentRegistry.EnvironmentRegistry["Service"]),
      ) as Effect.Effect<unknown, unknown>,
    );
    expect(requestJonesUpdateWithDescriptor).toHaveBeenCalledWith(entry, { action: "state" });
    expect(setEnabled).not.toHaveBeenCalled();
    expect(setCompatibility).not.toHaveBeenCalled();
    expect((await Effect.runPromise(SubscriptionRef.get(entries))).get(id)?.enabled).toBe(false);
  });
});
