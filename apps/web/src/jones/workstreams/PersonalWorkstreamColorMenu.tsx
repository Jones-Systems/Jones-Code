import { PaletteIcon } from "lucide-react";
import {
  MenuItemDetail,
  MenuRadioGroup,
  MenuRadioItem,
  MenuRadioItemIndicator,
  MenuSeparator,
  MenuSub,
  MenuSubPopup,
  MenuSubTrigger,
} from "../../components/ui/menu";
import { PERSONAL_WORKSTREAM_PALETTE } from "./personalRail";

function ColorSwatch({ color }: { color: string }) {
  return (
    <span
      aria-hidden
      className="block size-3 shrink-0 rounded-full ring-1 ring-black/10 ring-inset dark:ring-white/15 forced-colors:border"
      style={{ backgroundColor: color }}
    />
  );
}

function ColorOption({
  value,
  label,
  color,
}: {
  value: string | null;
  label: string;
  color: string;
}) {
  return (
    <MenuRadioItem value={value} label={label} closeOnClick>
      <span className="flex items-center gap-2">
        <ColorSwatch color={color} />
        <span className="flex-1">{label}</span>
        <MenuRadioItemIndicator />
      </span>
    </MenuRadioItem>
  );
}

export function PersonalWorkstreamColorMenu({
  color,
  automaticColor,
  disabled,
  onSelect,
}: {
  color: string | null;
  automaticColor: string;
  disabled: boolean;
  onSelect: (color: string | null) => void;
}) {
  const selected = color?.toUpperCase() ?? null;
  const custom =
    selected !== null && !PERSONAL_WORKSTREAM_PALETTE.some((preset) => preset.color === selected);

  return (
    <MenuSub>
      <MenuSubTrigger disabled={disabled}>
        <PaletteIcon aria-hidden />
        <span className="min-w-0 flex-1">Change color</span>
        <MenuItemDetail aria-hidden>
          <ColorSwatch color={color ?? automaticColor} />
        </MenuItemDetail>
      </MenuSubTrigger>
      <MenuSubPopup aria-label="Change color" align="center">
        <MenuRadioGroup
          aria-label="Workstream color"
          disabled={disabled}
          value={selected}
          onValueChange={(value: string | null) => {
            if (value !== selected) onSelect(value);
          }}
        >
          <ColorOption value={null} label="Automatic" color={automaticColor} />
          <MenuSeparator />
          {PERSONAL_WORKSTREAM_PALETTE.map((preset) => (
            <ColorOption
              key={preset.color}
              value={preset.color}
              label={preset.label}
              color={preset.color}
            />
          ))}
          {custom ? (
            <>
              <MenuSeparator />
              <ColorOption value={selected} label={`Custom (${selected})`} color={selected} />
            </>
          ) : null}
        </MenuRadioGroup>
      </MenuSubPopup>
    </MenuSub>
  );
}
