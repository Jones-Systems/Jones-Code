import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { NativeRecoveryPreflights, type NativeRecoveryMeasurement } from "./recoveryPreflight.ts";

const now = 1_790_000_000_000;
const id = "11111111-1111-4111-8111-111111111111";
const binding = { input: { environmentId: EnvironmentId.make("laptop"), stagedHandle: "stage", currentVersion: "old", expectedInstalledSource: "a".repeat(40) }, nativeIdentity: "manifest-generation-selection-sha256" };
const measurement = (availableBytes = 1000, status: "proved" | "unsupported" | "unknown" = "proved"): NativeRecoveryMeasurement => ({
  protocol: 1, firstInstall: false, stateBytes: 500, candidateBytes: 100,
  volumes: [{ role: "state", availableBytes, copyRequiredBytes: 900, cloneRequiredBytes: 400 }], clone: { status },
});
describe("native recovery policy authority", () => {
  it("admits ordinary copying while keeping the exact opaque proof and five minute expiry", () => {
    const cache = new NativeRecoveryPreflights();
    const value = cache.record(binding, measurement(), now, id);
    expect(value.binding).toEqual(binding.input);
    expect(Date.parse(value.expiresAt) - Date.parse(value.measuredAt)).toBe(300_000);
    expect(cache.require({ preflightId: id, policy: "clone-or-copy" }, binding, now + 299_999)).toBe("clone-or-copy");
  });
  it("offers Clone or Stop only with proved strict cloning and enough candidate capacity", () => {
    const cache = new NativeRecoveryPreflights();
    const value = cache.record(binding, measurement(500), now, id);
    expect(value.policies.cloneOrCopy.admissible).toBe(false);
    expect(value.policies.cloneRequired.admissible).toBe(true);
    expect(cache.require({ preflightId: id, policy: "clone-required" }, binding, now)).toBe("clone-required");
    expect(() => cache.require({ preflightId: id, policy: "clone-or-copy" }, binding, now)).toThrow(/full backup copy/);
    expect(cache.record(binding, measurement(399), now, id).policies.cloneRequired.admissible).toBe(false);
  });
  it.each(["unsupported", "unknown"] as const)("offers Stop only when clone proof is %s and a copy does not fit", (status) => {
    const cache = new NativeRecoveryPreflights();
    const value = cache.record(binding, measurement(500, status), now, id);
    expect(value.policies.cloneOrCopy.admissible).toBe(false);
    expect(value.policies.cloneRequired.admissible).toBe(false);
    expect(() => cache.require({ preflightId: id, policy: "clone-required" }, binding, now)).toThrow(/Stop/);
  });
  it("first install claims no backup and offers no clone choice", () => {
    const cache = new NativeRecoveryPreflights();
    const value = cache.record(binding, { ...measurement(), stateBytes: 0, firstInstall: true, clone: { status: "unsupported" } }, now, id);
    expect(value.firstInstall).toBe(true);
    expect(value.policies.cloneRequired).toMatchObject({ admissible: false, reason: expect.stringContaining("No existing state") });
  });
  it.each([
    { ...binding, nativeIdentity: "different-selection" },
    { ...binding, nativeIdentity: "different-manifest-generation" },
    { ...binding, input: { ...binding.input, environmentId: EnvironmentId.make("other") } },
    { ...binding, input: { ...binding.input, currentVersion: "changed" } },
    { ...binding, input: { ...binding.input, expectedInstalledSource: "b".repeat(40) } },
    { ...binding, input: { ...binding.input, stagedHandle: "other" } },
  ])("refuses changed native binding %#", (changed) => {
    const cache = new NativeRecoveryPreflights(); cache.record(binding, measurement(), now, id);
    expect(() => cache.require({ preflightId: id, policy: "clone-required" }, changed, now)).toThrow(/changed/);
  });
  it("rejects expiry, future proof, arbitrary IDs and replaced proofs", () => {
    const cache = new NativeRecoveryPreflights(); cache.record(binding, measurement(), now, id);
    for (const clock of [now - 1, now + 300_000]) expect(() => cache.require({ preflightId: id, policy: "clone-required" }, binding, clock)).toThrow(/expired/);
    expect(() => cache.require({ preflightId: "22222222-2222-4222-8222-222222222222", policy: "clone-required" }, binding, now)).toThrow();
    cache.record(binding, measurement(), now, "22222222-2222-4222-8222-222222222222");
    expect(() => cache.require({ preflightId: id, policy: "clone-required" }, binding, now)).toThrow();
  });
  it("returned measurements cannot overwrite the native cached authority", () => {
    const cache = new NativeRecoveryPreflights();
    const value = cache.record(binding, measurement(500, "unknown"), now, id);
    Object.assign(value.policies.cloneRequired, { admissible: true });
    Object.assign(value.clone, { status: "proved" });
    expect(() => cache.require({ preflightId: id, policy: "clone-required" }, binding, now)).toThrow(/Stop/);
  });
});
