import { BrainIcon, HouseIcon } from "lucide-react";
import { PersonalMachineSymbol } from "../devicePresentation/PersonalMachineSymbol";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  SIDEBAR_APPEARANCE_STORAGE_KEY,
  SIDEBAR_APPEARANCE_OPTIONS,
  SIDEBAR_APPEARANCE_PRESETS,
  sidebarAppearancePresetName,
  type SidebarAppearance,
} from "./model";
import { ThreadTitleBrightnessSlider } from "./ThreadTitleBrightnessSlider";
import { useSidebarAppearanceHostContext } from "./useSidebarAppearanceHost";
import { useState } from "react";

const labels = {
  cardScale: "Card size",
  workstreamSpacing: "Spacing",
  workstreamTitleWeight: "Title weight",
  workstreamTitleBrightness: "Workstream title brightness",
  pillBackgroundOpacity: "Pill background",
  countStyle: "Thread count",
  deviceLabel: "Device name",
  laptopSymbol: "Laptop symbol",
  vpsSymbol: "VPS symbol",
};
type LookField = keyof typeof SIDEBAR_APPEARANCE_OPTIONS;
function optionLabel(field: LookField, value: string | number) {
  if (field === "cardScale") return `${Math.round(Number(value) * 100)}%`;
  if (field === "pillBackgroundOpacity") return value === 0 ? "None" : `${value}%`;
  if (value === "default") return "Original";
  if (value === "muted") return "88%";
  if (value === "verbose") return "10 threads";
  if (value === "numeric-right") return "Footnote “10”";
  if (value === "icon") return "Icon only";
  return String(value).replace(/^./, (s) => s.toUpperCase());
}

export function SidebarAppearanceControls() {
  const host = useSidebarAppearanceHostContext();
  const [message, setMessage] = useState("");
  const preset = sidebarAppearancePresetName(host.presented);
  const differences = (
    Object.keys(DEFAULT_SIDEBAR_APPEARANCE) as (keyof SidebarAppearance)[]
  ).filter((key) => host.stored[key] !== DEFAULT_SIDEBAR_APPEARANCE[key]).length;
  let hasRecord = false;
  try {
    hasRecord = window.localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY) !== null;
  } catch {
    /* Storage can be unavailable in this profile. */
  }
  function field(key: LookField) {
    return (
      <fieldset className="min-w-0 space-y-1" key={key}>
        <legend className="text-xs text-muted-foreground">{labels[key]}</legend>
        <div className="flex flex-wrap gap-1">
          {SIDEBAR_APPEARANCE_OPTIONS[key].map((value) => (
            <label
              key={String(value)}
              className="inline-flex cursor-pointer items-center gap-1 rounded border border-border px-1.5 py-1 text-xs has-checked:border-primary"
            >
              <input
                type="radio"
                name={key}
                checked={host.presented[key] === value}
                onChange={() => {
                  host.update({ [key]: value });
                  setMessage("");
                }}
              />
              {key === "laptopSymbol" || key === "vpsSymbol" ? (
                <SidebarAppearanceOverride value={{ ...host.presented, [key]: value }}>
                  <PersonalMachineSymbol
                    label={key === "laptopSymbol" ? "Laptop" : "VPS"}
                    kind={key === "laptopSymbol" ? "laptop" : "server"}
                    fallback={key === "laptopSymbol" ? HouseIcon : BrainIcon}
                    aria-hidden
                    className="size-3"
                  />
                </SidebarAppearanceOverride>
              ) : null}
              {optionLabel(key, value)}
              {SIDEBAR_APPEARANCE_PRESETS.selected[key] === value ? (
                <>
                  <span aria-hidden>★</span>
                  <span className="sr-only">(selected default)</span>
                </>
              ) : null}
              {SIDEBAR_APPEARANCE_PRESETS.original[key] === value ? (
                <>
                  <span aria-hidden>○</span>
                  <span className="sr-only">(Original)</span>
                </>
              ) : null}
            </label>
          ))}
        </div>
      </fieldset>
    );
  }
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap gap-2 text-xs">
        <button
          type="button"
          className="cursor-pointer rounded border border-border px-2 py-1"
          aria-pressed={host.comparingPreset === "original"}
          onClick={() =>
            host.setComparingPreset(host.comparingPreset === "original" ? null : "original")
          }
        >
          {host.comparingPreset === "original" ? "Showing Original" : "Compare with Original"}
        </button>
        <button
          type="button"
          className="cursor-pointer rounded border border-border px-2 py-1"
          onClick={() => {
            host.reset();
            setMessage("Reset: Selected look at Current brightness. Stored record removed.");
          }}
        >
          Reset all
        </button>
      </div>
      <fieldset>
        <legend className="text-xs text-muted-foreground">Look</legend>
        <div className="flex gap-2 text-xs">
          {(["selected", "original"] as const).map((name) => (
            <label key={name} className="cursor-pointer">
              <input
                type="radio"
                name="sidebar-look"
                checked={preset === name}
                onChange={() => undefined}
                onClick={() => {
                  host.applyPreset(name);
                  setMessage(
                    `${name === "original" ? "Original" : "Selected"} look applied; title brightness kept.`,
                  );
                }}
              />{" "}
              {name === "selected" ? "Selected ★" : "Original ○"}
            </label>
          ))}
          {preset === null ? (
            <span className="rounded border border-dashed px-1">Custom</span>
          ) : null}
        </div>
      </fieldset>
      <section className="space-y-3">
        <h3 className="text-sm font-medium">Thread cards</h3>
        {field("cardScale")}
        <ThreadTitleBrightnessSlider
          value={host.presented.threadTitleBrightnessBoost}
          onChange={(threadTitleBrightnessBoost) => {
            host.update({ threadTitleBrightnessBoost });
            setMessage("");
          }}
        />
      </section>
      <section className="space-y-3">
        <h3 className="text-sm font-medium">Workstreams</h3>
        {field("workstreamSpacing")}
        {field("workstreamTitleWeight")}
        {field("workstreamTitleBrightness")}
        {field("pillBackgroundOpacity")}
        {field("countStyle")}
      </section>
      <section className="space-y-3">
        <h3 className="text-sm font-medium">Devices</h3>
        {field("deviceLabel")}
        {field("laptopSymbol")}
        {field("vpsSymbol")}
      </section>
      <p className="text-xs text-muted-foreground">
        {!hasRecord
          ? "Stored record: none."
          : `Stored record: written (${differences} fields differ from the default). Reset all removes it.`}
      </p>
      <p className="text-xs text-muted-foreground">
        Saved in this browser profile only. Presets change the nine look fields and keep title
        brightness. Card size changes thread cards only; chat text, interface font and zoom are
        untouched.
      </p>
      <p role="status" className="text-xs">
        {host.saveFailed ? "Couldn't save; showing the saved appearance." : message}
      </p>
    </div>
  );
}
