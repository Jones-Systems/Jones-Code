import type { EnvironmentMachineKind } from "../environment.ts";
import * as Schema from "effect/Schema";

/** Cosmetic only: never use this field for routing, shell assertions or filters. */
export const DeviceDisplayName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(64),
  Schema.isPattern(/^[^\p{Cc}\p{Cf}]*$/u),
);

export function resolveMachineIdentity(
  label: string | null | undefined,
  kind?: EnvironmentMachineKind | null,
) {
  const fullLabel = label?.trim() || "Unknown environment";
  const normalized = fullLabel.toLowerCase().replace(/[-_\s]+/g, " ");
  const aliases: Readonly<Record<string, "vps" | "test" | "laptop" | "mini">> = {
    vps: "vps",
    "test server": "test",
    tsv: "test",
    laptop: "laptop",
    home: "laptop",
    lap: "laptop",
    mini: "mini",
    "mac mini": "mini",
  };
  const known = Object.hasOwn(aliases, normalized) ? aliases[normalized] : undefined;
  const identity =
    known ?? (kind === "laptop" ? "laptop" : kind === "mac-mini" ? "mini" : "unknown");
  const shortLabel = {
    vps: "VPS",
    test: "Test Server",
    laptop: "Laptop",
    mini: "Mini",
    unknown: fullLabel,
  }[identity];
  return { identity, label: fullLabel, shortLabel, kind: kind ?? "server" } as const;
}

export function deviceDisplayLabel(
  label: string,
  kind?: EnvironmentMachineKind | null,
  override?: string | null,
): string {
  return override?.trim() || resolveMachineIdentity(label, kind).shortLabel;
}
