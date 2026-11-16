import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type { QualifiedBackupInspection } from "../../cloud/qualifiedBackup.ts";
import { createRecoveryPreflights } from "./recoveryPreflight.ts";

const binding = {
  environmentId: EnvironmentId.make("synthetic-target"),
  stagedHandle: "stage",
  currentVersion: "1.0.0",
  expectedInstalledSource: "a".repeat(40),
};
function fixture() {
  let now = Date.parse("2026-10-27T04:00:00Z");
  let supported = true;
  let state: JonesUpdateState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "staged",
    ...binding,
    installedSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: binding.expectedInstalledSource,
      tree: "b".repeat(40),
    },
    capability: { check: true, download: true, install: true, operations: 1, recoveryPolicy: 1 },
  };
  let measurement: QualifiedBackupInspection = {
    bytes: 1000,
    firstInstall: false,
    sameVolume: true,
    availableBytes: 100,
    copyRequiredBytes: 200,
    cloneRequiredBytes: 10,
    clone: { status: "proved" },
  };
  let duringMeasurement: (() => void) | undefined;
  const preflights = createRecoveryPreflights({
    now: () => now,
    snapshot: () => state,
    nativeSupported: () => supported,
    measure: async () => {
      duringMeasurement?.();
      return measurement;
    },
  });
  return {
    preflights,
    advance: (ms: number) => {
      now += ms;
    },
    change: (patch: Partial<JonesUpdateState>) => {
      state = { ...state, ...patch };
    },
    nativeUnsupported: () => {
      supported = false;
    },
    duringMeasurement: (run: () => void) => {
      duringMeasurement = run;
    },
    measurement: (patch: Partial<QualifiedBackupInspection>) => {
      measurement = { ...measurement, ...patch };
    },
  };
}
describe("target recovery preflight", () => {
  it("allows only strict clone with low copy space; unsupported or unknown proof offers no policy", async () => {
    const f = fixture();
    const p = await f.preflights.preflight(binding);
    expect(p.policies.cloneOrCopy.admissible).toBe(false);
    expect(p.policies.cloneRequired.admissible).toBe(true);
    expect(Date.parse(p.expiresAt) - Date.parse(p.measuredAt)).toBe(300_000);
    for (const status of ["unknown", "unsupported"] as const) {
      f.measurement({ clone: { status } });
      const refused = await f.preflights.preflight(binding);
      expect(refused.policies.cloneRequired.admissible).toBe(false);
      expect(refused.policies.cloneOrCopy.admissible).toBe(false);
    }
    f.measurement({ availableBytes: 1000, clone: { status: "unknown" } });
    expect((await f.preflights.preflight(binding)).policies.cloneOrCopy.admissible).toBe(false);
    f.measurement({ clone: { status: "unsupported" } });
    expect((await f.preflights.preflight(binding)).policies.cloneOrCopy.admissible).toBe(true);
  });
  it("refuses expiry, forged IDs, changed exact bindings and lost native capability", async () => {
    const f = fixture();
    const p = await f.preflights.preflight(binding);
    const r = { policy: "clone-required" as const, preflightId: p.preflightId };
    expect(f.preflights.validate(r, binding)).toBeUndefined();
    for (const patch of [
      { stagedHandle: "other" },
      { currentVersion: "2.0.0" },
      { environmentId: EnvironmentId.make("other") },
      { expectedInstalledSource: "c".repeat(40) },
    ])
      expect(f.preflights.validate(r, { ...binding, ...patch })).toBe("recovery-stale-binding");
    expect(f.preflights.validate({ ...r, preflightId: "forged" }, binding)).toBe(
      "recovery-preflight-expired",
    );
    // A returned object cannot rewrite the target-owned cache.
    Object.assign(p.policies.cloneOrCopy, { admissible: true });
    expect(f.preflights.validate({ ...r, policy: "clone-or-copy" }, binding)).toBe(
      "recovery-policy-refused",
    );
    f.advance(300_000);
    expect(f.preflights.validate(r, binding)).toBe("recovery-preflight-expired");
    const next = await f.preflights.preflight(binding);
    f.nativeUnsupported();
    expect(f.preflights.validate({ ...r, preflightId: next.preflightId }, binding)).toBe(
      "recovery-stale-binding",
    );
    await expect(f.preflights.preflight(binding)).rejects.toThrow("recovery-stale-binding");
  });
  it("keeps cross-volume state refused and first install free of a backup claim", async () => {
    const f = fixture();
    f.measurement({ sameVolume: false, availableBytes: 1000 });
    const cross = await f.preflights.preflight(binding);
    expect(cross.policies.cloneOrCopy.admissible).toBe(false);
    expect(cross.policies.cloneRequired.admissible).toBe(false);
    f.measurement({ sameVolume: true, firstInstall: true, bytes: 0 });
    const first = await f.preflights.preflight(binding);
    expect(first.firstInstall).toBe(true);
    expect(first.stateBytes).toBe(0);
    expect(first.policies.cloneRequired.admissible).toBe(false);
    expect(first.policies.cloneOrCopy.admissible).toBe(false);
  });
  it("rechecks host binding after an asynchronous measurement", async () => {
    const f = fixture();
    f.duringMeasurement(() => f.change({ stagedHandle: "replaced" }));
    await expect(f.preflights.preflight(binding)).rejects.toThrow("recovery-stale-binding");
  });
});
