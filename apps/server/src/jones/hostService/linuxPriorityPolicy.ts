export const LINUX_PRIORITY_DROPIN = "90-jones-priority.conf";
export const LINUX_PRIORITY_POLICY = "[Service]\nNice=-15\n";
const policyBytes = new TextEncoder().encode(LINUX_PRIORITY_POLICY);

export interface LinuxPriorityFile {
  readonly uid: number;
  readonly gid: number;
  readonly mode: number;
  readonly bytes: Uint8Array;
}

export function assertLinuxPriorityPolicy(file: LinuxPriorityFile, effectiveNice: string): void {
  if (
    file.uid !== 0 ||
    file.gid !== 0 ||
    file.mode !== 0o100644 ||
    file.bytes.length !== policyBytes.length ||
    file.bytes.some((byte, index) => byte !== policyBytes[index]) ||
    effectiveNice !== "-15"
  ) {
    throw new Error(
      "The system service priority policy must be a root:root 0644 regular file containing exactly [Service] and Nice=-15, with effective Nice=-15. Reconcile it through the external administrator before retrying.",
    );
  }
}

export function assertNoLaterLinuxPriorityOverride(name: string, bytes: Uint8Array): void {
  if (name > LINUX_PRIORITY_DROPIN && /^\s*Nice\s*=/m.test(new TextDecoder().decode(bytes))) {
    throw new Error(
      "A later drop-in overrides the recognized system service priority policy. Reconcile it through the external administrator.",
    );
  }
}
