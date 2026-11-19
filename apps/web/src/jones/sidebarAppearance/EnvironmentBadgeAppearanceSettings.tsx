import type { EnvironmentMachineKind } from "@t3tools/contracts";
import { useState } from "react";
import { EnvironmentMachineIcon } from "../../components/EnvironmentMachineIcon";
import { SettingsSection } from "../../components/settings/settingsLayout";
import { settingsScopeEnvironmentLabel } from "../../components/settings/settingsScopeAxis";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";
import {
  useEnvironmentMachines,
  useEnvironments,
  type EnvironmentPresentation,
} from "../../state/environments";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import { useEnvironmentBadgeAppearance } from "./environmentBadgeAppearance";
import { useSidebarAppearanceStore } from "./useSidebarAppearanceStore";

const displayOptions = [
  { value: "default", label: "Use default" },
  { value: "icon-and-name", label: "Icon and name" },
  { value: "icon-only", label: "Icon only" },
  { value: "name-only", label: "Name only" },
] as const;
const fillOptions = [
  { value: "default", label: "Use default" },
  { value: "filled", label: "Filled" },
  { value: "outline", label: "Outline" },
] as const;

function BadgeControl<Value extends string>({
  label,
  name,
  options,
  value,
  onChange,
}: {
  label: string;
  name: string;
  options: ReadonlyArray<{ value: Value; label: string }>;
  value: Value;
  onChange: (value: Value) => void;
}) {
  return (
    <div className="min-w-0 space-y-1">
      <span className="block text-xs text-muted-foreground">{label}</span>
      <Select items={options} value={value} onValueChange={(next) => next && onChange(next)}>
        <SelectTrigger size="sm" aria-label={`${label} for ${name}`}>
          <SelectValue />
        </SelectTrigger>
        <SelectPopup>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
    </div>
  );
}

function EnvironmentBadgeSettingsRow({
  environment,
  name,
  kind,
}: {
  environment: EnvironmentPresentation;
  name: string;
  kind: EnvironmentMachineKind;
}) {
  const { appearance, update } = useEnvironmentBadgeAppearance(environment.environmentId);
  const [saveFailed, setSaveFailed] = useState(false);

  return (
    <div className="space-y-2 px-3 py-2.5 sm:px-4">
      <div className="flex min-w-0 items-center gap-2">
        <EnvironmentMachineIcon
          aria-hidden
          className="size-4 shrink-0 text-muted-foreground"
          kind={kind}
          label={environment.label}
          override={environment.serverConfig?.settings.environmentIcon != null}
          symbolFill={appearance?.symbolFill}
        />
        <span className="min-w-0 wrap-anywhere text-sm font-medium">{name}</span>
      </div>
      <div className="flex flex-wrap gap-2">
        <BadgeControl
          label="Badge display"
          name={name}
          options={displayOptions}
          value={appearance?.display ?? "default"}
          onChange={(value) =>
            setSaveFailed(!update("display", value === "default" ? undefined : value))
          }
        />
        <BadgeControl
          label="Icon style"
          name={name}
          options={fillOptions}
          value={appearance?.symbolFill ?? "default"}
          onChange={(value) =>
            setSaveFailed(!update("symbolFill", value === "default" ? undefined : value))
          }
        />
      </div>
      {saveFailed ? (
        <p role="status" className="text-xs text-muted-foreground">
          Couldn’t save badge appearance.
        </p>
      ) : null}
    </div>
  );
}

export function EnvironmentBadgeAppearanceSettings() {
  const { environments } = useEnvironments();
  const machineById = useEnvironmentMachines();
  const { stored: sidebar } = useSidebarAppearanceStore();
  const names = environments.map((environment) => ({
    ...environment,
    label: environment.displayLabel,
  }));

  return (
    <SidebarAppearanceOverride value={sidebar}>
      <SettingsSection title="Thread environment badges" id="thread-board-environment-badges">
        {environments.length > 0 ? (
          environments.map((environment, index) => (
            <EnvironmentBadgeSettingsRow
              key={environment.environmentId}
              environment={environment}
              name={settingsScopeEnvironmentLabel(names[index]!, names)}
              kind={machineById.get(environment.environmentId) ?? "server"}
            />
          ))
        ) : (
          <p className="px-3 py-2.5 text-sm text-muted-foreground sm:px-4">
            No environments available.
          </p>
        )}
      </SettingsSection>
    </SidebarAppearanceOverride>
  );
}
