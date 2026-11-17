import { resolveMachineIdentity } from "@t3tools/contracts/jones/devicePresentation";
import type { EnvironmentMachineKind } from "@t3tools/contracts";
import { useSidebarAppearancePresentation } from "./SidebarAppearanceContext";
import { deviceLabelHidden } from "./model";

export function useSidebarDevicePresentation(input: {
  label: string | null | undefined;
  kind: EnvironmentMachineKind;
  visibleLabel: string;
  iconOverridden: boolean;
}) {
  const appearance = useSidebarAppearancePresentation();
  // Do not use the kind fallback to hide an ambiguous or custom machine name.
  const identity = resolveMachineIdentity(input.label);
  const hidden = deviceLabelHidden({
    mode: appearance?.deviceLabel ?? "label",
    identity: identity.identity,
    shortLabel: identity.shortLabel,
    visibleLabel: input.visibleLabel,
    iconOverridden: input.iconOverridden,
  });
  return { hidden, accessibleSuffix: hidden ? `, ${input.visibleLabel}` : "" };
}
