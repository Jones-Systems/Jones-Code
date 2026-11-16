// @effect-diagnostics nodeBuiltinImport:off globalDate:off -- Promise adapter shares the existing target updater.
import * as NodeCrypto from "node:crypto";
import type {
  JonesRecoveryPreflight,
  JonesRecoveryPreflightInput,
  JonesRecoveryPolicy,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { QualifiedBackupInspection } from "../../cloud/qualifiedBackup.ts";

const TTL = 5 * 60_000;
const sameBinding = (a: JonesRecoveryPreflightInput, b: JonesRecoveryPreflightInput) =>
  a.environmentId === b.environmentId &&
  a.stagedHandle === b.stagedHandle &&
  a.currentVersion === b.currentVersion &&
  a.expectedInstalledSource === b.expectedInstalledSource;

/** Target-owned, bounded evidence; callers cannot supply measurements or extend expiry. */
export function createRecoveryPreflights(options: {
  readonly snapshot: () => JonesUpdateState;
  readonly nativeSupported: () => boolean;
  readonly measure: (binding: JonesRecoveryPreflightInput) => Promise<QualifiedBackupInspection>;
  readonly now?: () => number;
}) {
  const now = options.now ?? Date.now;
  const cache = new Map<string, JonesRecoveryPreflight>();
  const current = (binding: JonesRecoveryPreflightInput) => {
    const s = options.snapshot();
    return (
      s.capability.recoveryPolicy === 1 &&
      options.nativeSupported() &&
      s.environmentId === binding.environmentId &&
      s.stagedHandle === binding.stagedHandle &&
      s.currentVersion === binding.currentVersion &&
      s.installedSource?.sha === binding.expectedInstalledSource
    );
  };
  const expire = () => {
    const time = now();
    for (const [id, p] of cache) if (time >= Date.parse(p.expiresAt)) cache.delete(id);
  };
  return {
    preflight: async (binding: JonesRecoveryPreflightInput): Promise<JonesRecoveryPreflight> => {
      expire();
      if (!current(binding))
        throw new Error(
          "recovery-stale-binding: Refresh the staged host before reviewing recovery.",
        );
      const measuredAt = now();
      const m = await options.measure(binding);
      if (!current(binding) || now() - measuredAt >= TTL)
        throw new Error("recovery-stale-binding: The host changed during recovery inspection.");
      const sameVolume = m.sameVolume;
      const copy =
        sameVolume && m.clone.status !== "unknown" && m.availableBytes >= m.copyRequiredBytes;
      const clone =
        sameVolume && m.clone.status === "proved" && m.availableBytes >= m.cloneRequiredBytes;
      const copyReason = m.firstInstall
        ? "No existing state requires a recovery snapshot."
        : !sameVolume
          ? "State and recovery are on different filesystems."
          : m.clone.status === "unknown"
            ? (m.clone.reason ?? "The native recovery clone result is unknown.")
            : !copy
              ? "Not enough free space for a full recovery copy."
              : undefined;
      const cloneReason = m.firstInstall
        ? "No existing state requires a recovery snapshot."
        : !sameVolume
          ? "State and recovery are on different filesystems."
          : m.clone.status !== "proved"
            ? (m.clone.reason ?? "A strict native recovery clone has not been proved.")
            : !clone
              ? "Not enough free space for recovery clone metadata."
              : undefined;
      const p: JonesRecoveryPreflight = {
        protocol: 1,
        preflightId: NodeCrypto.randomUUID(),
        measuredAt: new Date(measuredAt).toISOString(),
        expiresAt: new Date(measuredAt + TTL).toISOString(),
        binding: { ...binding },
        firstInstall: m.firstInstall,
        stateBytes: m.bytes,
        volumes: [
          {
            role: "state",
            availableBytes: m.availableBytes,
            copyRequiredBytes: m.copyRequiredBytes,
            cloneRequiredBytes: m.cloneRequiredBytes,
          },
        ],
        clone: { ...m.clone },
        policies: {
          cloneOrCopy: {
            admissible: !m.firstInstall && copy,
            ...(copyReason === undefined ? {} : { reason: copyReason }),
          },
          cloneRequired: {
            admissible: !m.firstInstall && clone,
            ...(cloneReason === undefined ? {} : { reason: cloneReason }),
          },
        },
      };
      if (cache.size >= 32) cache.delete(cache.keys().next().value!);
      cache.set(p.preflightId, p);
      return structuredClone(p);
    },
    validate: (
      recovery: { readonly policy: JonesRecoveryPolicy; readonly preflightId: string },
      binding: JonesRecoveryPreflightInput,
    ): string | undefined => {
      expire();
      if (!current(binding)) return "recovery-stale-binding";
      const p = cache.get(recovery.preflightId);
      if (p === undefined || now() < Date.parse(p.measuredAt)) return "recovery-preflight-expired";
      if (!sameBinding(p.binding, binding)) return "recovery-stale-binding";
      const policy =
        recovery.policy === "clone-required" ? p.policies.cloneRequired : p.policies.cloneOrCopy;
      if (!policy.admissible) return "recovery-policy-refused";
      return undefined;
    },
  };
}
