import { LayersIcon } from "lucide-react";
import { useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import { ComposerContextLabel } from "../../components/ComposerContextLabel";
import { ComposerControl } from "../../components/chat/ComposerControl";
import {
  Menu,
  MenuPopup,
  MenuRadioGroup,
  MenuRadioItem,
  MenuTrigger,
} from "../../components/ui/menu";
import { Separator } from "../../components/ui/separator";
import { usePersonalWorkstreams } from "../../state/workstreamsPersonal";
import { availableComposerWorkstreams } from "./newThreadWorkstream";

const NO_WORKSTREAM = "";

export function NewThreadWorkstreamPicker({
  environmentId,
  value,
  onChange,
  disabled,
  showContextDivider = false,
}: {
  environmentId: EnvironmentId;
  value: string | null;
  onChange: (value: string | null) => void;
  disabled: boolean;
  showContextDivider?: boolean;
}) {
  const { state } = usePersonalWorkstreams();
  const [menuOpenedAt, setMenuOpenedAt] = useState(Date.now);
  const environment = state.environments.find((entry) => entry.environmentId === environmentId);
  const canAssign = environment?.confirmed === true && environment.status === "ready";
  const options = canAssign ? availableComposerWorkstreams(state.workstreams, menuOpenedAt) : [];
  if (!canAssign && value === null) return null;

  const selected = state.workstreams.find(({ id }) => id === value);
  const label = selected?.name ?? (value === null ? "Workstream" : "Unavailable Workstream");
  return (
    <div className="flex min-w-0 max-w-[38%] flex-initial items-center gap-1">
      {showContextDivider ? (
        <Separator orientation="vertical" className="mx-0.5 h-3.5!" data-composer-context-control />
      ) : null}
      <Menu
        onOpenChange={(open) => {
          if (open) setMenuOpenedAt(Date.now());
        }}
      >
        <MenuTrigger
          render={<ComposerControl size="xs" />}
          className="min-w-0 justify-start"
          aria-label={value === null ? "Workstream for new thread" : `Workstream: ${label}`}
          disabled={disabled}
          data-composer-context-control
        >
          <LayersIcon aria-hidden />
          <ComposerContextLabel>{label}</ComposerContextLabel>
        </MenuTrigger>
        <MenuPopup align="start">
          <MenuRadioGroup
            value={value ?? NO_WORKSTREAM}
            onValueChange={(id) => onChange(id || null)}
          >
            <MenuRadioItem value={NO_WORKSTREAM} closeOnClick>
              No Workstream
            </MenuRadioItem>
            {options.map(({ id, name }) => (
              <MenuRadioItem key={id} value={id} closeOnClick>
                {name}
              </MenuRadioItem>
            ))}
          </MenuRadioGroup>
        </MenuPopup>
      </Menu>
    </div>
  );
}
