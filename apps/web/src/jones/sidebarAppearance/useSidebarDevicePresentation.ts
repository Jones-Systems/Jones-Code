import { resolveMachineIdentity } from "@t3tools/contracts/jones/devicePresentation";
import type { EnvironmentId } from "@t3tools/contracts";
import { useSidebarAppearancePresentation } from "./SidebarAppearanceContext";
import { useEnvironmentBadgeAppearance } from "./environmentBadgeAppearance";
import { deviceLabelHidden } from "./model";

export function useSidebarDevicePresentation(input: {
  environmentId: EnvironmentId;
  label: string | null | undefined;
  visibleLabel: string;
  iconOverridden: boolean;
}) {
  const appearance = useSidebarAppearancePresentation();
  const { appearance: badge } = useEnvironmentBadgeAppearance(input.environmentId);
  // Do not use the kind fallback to hide an ambiguous or custom machine name.
  const identity = resolveMachineIdentity(input.label);
  const nameHidden = badge?.display
    ? badge.display === "icon-only"
    : deviceLabelHidden({
        mode: appearance?.deviceLabel ?? "label",
        identity: identity.identity,
        shortLabel: identity.shortLabel,
        visibleLabel: input.visibleLabel,
        iconOverridden: input.iconOverridden,
      });
  return {
    nameHidden,
    iconHidden: badge?.display === "name-only",
    symbolFill: badge?.symbolFill,
    accessibleSuffix: nameHidden ? `, ${input.visibleLabel}` : "",
  };
}
