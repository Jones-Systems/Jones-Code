import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const SIDEBAR_APPEARANCE_STORAGE_KEY = "jones:sidebar-appearance:v1";
export const SIDEBAR_APPEARANCE_OPTIONS = {
  cardScale: [1, 0.93, 0.87],
  workstreamSpacing: ["default", "roomy", "tight"],
  workstreamTitleWeight: ["default", "medium", "regular"],
  workstreamTitleBrightness: ["default", "muted"],
  pillBackgroundOpacity: [30, 15, 4, 0],
  countStyle: ["verbose", "numeric-right"],
  deviceLabel: ["label", "icon"],
  laptopSymbol: ["outline", "filled"],
  vpsSymbol: ["outline", "left", "right", "filled"],
} as const;
export const THREAD_TITLE_BRIGHTNESS_BOOST = { min: 0, max: 100, default: 0 } as const;

export const SidebarAppearance = Schema.Struct({
  cardScale: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.cardScale).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(0.93 as const)),
  ),
  workstreamSpacing: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.workstreamSpacing).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("roomy" as const)),
  ),
  workstreamTitleWeight: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.workstreamTitleWeight).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("medium" as const)),
  ),
  workstreamTitleBrightness: Schema.Literals(
    SIDEBAR_APPEARANCE_OPTIONS.workstreamTitleBrightness,
  ).pipe(Schema.withDecodingDefaultKey(Effect.succeed("muted" as const))),
  pillBackgroundOpacity: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.pillBackgroundOpacity).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(4 as const)),
  ),
  countStyle: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.countStyle).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("numeric-right" as const)),
  ),
  deviceLabel: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.deviceLabel).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("icon" as const)),
  ),
  laptopSymbol: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.laptopSymbol).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("filled" as const)),
  ),
  vpsSymbol: Schema.Literals(SIDEBAR_APPEARANCE_OPTIONS.vpsSymbol).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("filled" as const)),
  ),
  threadTitleBrightnessBoost: Schema.Int.check(
    Schema.isBetween({
      minimum: THREAD_TITLE_BRIGHTNESS_BOOST.min,
      maximum: THREAD_TITLE_BRIGHTNESS_BOOST.max,
    }),
  ).pipe(Schema.withDecodingDefaultKey(Effect.succeed(THREAD_TITLE_BRIGHTNESS_BOOST.default))),
});
export type SidebarAppearance = typeof SidebarAppearance.Type;
const decodeSidebarAppearance = Schema.decodeSync(SidebarAppearance);
export const StoredSidebarAppearance = Schema.NullOr(SidebarAppearance);
export type SidebarAppearancePresetName = "selected" | "original";
export const SIDEBAR_APPEARANCE_PRESETS: Readonly<
  Record<SidebarAppearancePresetName, Omit<SidebarAppearance, "threadTitleBrightnessBoost">>
> = {
  selected: {
    cardScale: 0.93,
    workstreamSpacing: "roomy",
    workstreamTitleWeight: "medium",
    workstreamTitleBrightness: "muted",
    pillBackgroundOpacity: 4,
    countStyle: "numeric-right",
    deviceLabel: "icon",
    laptopSymbol: "filled",
    vpsSymbol: "filled",
  },
  original: {
    cardScale: 1,
    workstreamSpacing: "default",
    workstreamTitleWeight: "default",
    workstreamTitleBrightness: "default",
    pillBackgroundOpacity: 30,
    countStyle: "verbose",
    deviceLabel: "label",
    laptopSymbol: "outline",
    vpsSymbol: "outline",
  },
};
export const DEFAULT_SIDEBAR_APPEARANCE: SidebarAppearance = {
  ...SIDEBAR_APPEARANCE_PRESETS.selected,
  threadTitleBrightnessBoost: 0,
};
export const ORIGINAL_SIDEBAR_APPEARANCE: SidebarAppearance = {
  ...SIDEBAR_APPEARANCE_PRESETS.original,
  threadTitleBrightnessBoost: 0,
};

export function resolveSidebarAppearance(stored: SidebarAppearance | null): SidebarAppearance {
  return stored ?? DEFAULT_SIDEBAR_APPEARANCE;
}

export function patchSidebarAppearance(
  stored: SidebarAppearance | null,
  patch: Partial<SidebarAppearance>,
): SidebarAppearance | null {
  // Decode again to strip excess keys and reject invalid runtime patches atomically.
  const next = decodeSidebarAppearance({ ...resolveSidebarAppearance(stored), ...patch });
  const isDefault = Object.keys(DEFAULT_SIDEBAR_APPEARANCE).every(
    (key) =>
      next[key as keyof SidebarAppearance] ===
      DEFAULT_SIDEBAR_APPEARANCE[key as keyof SidebarAppearance],
  );
  return isDefault ? null : next;
}

export function sidebarAppearancePresetName(
  value: SidebarAppearance,
): SidebarAppearancePresetName | null {
  const keys = Object.keys(
    SIDEBAR_APPEARANCE_OPTIONS,
  ) as (keyof typeof SIDEBAR_APPEARANCE_OPTIONS)[];
  for (const name of ["selected", "original"] as const) {
    if (keys.every((key) => value[key] === SIDEBAR_APPEARANCE_PRESETS[name][key])) return name;
  }
  return null;
}

export const SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES: readonly string[] = [
  "data-jones-sidebar-appearance",
  "data-jones-card-scale",
  "data-jones-ws-spacing",
  "data-jones-ws-title-weight",
  "data-jones-ws-title-brightness",
  "data-jones-pill-bg",
  "data-jones-thread-title-boost",
];
export const SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES: readonly string[] = [
  "--jones-thread-title-boost",
];

export function sidebarAppearanceScope(value: SidebarAppearance): {
  readonly attributes: Readonly<Record<string, string>>;
  readonly variables: Readonly<Record<string, string>>;
} {
  const attributes: Record<string, string> = {
    "data-jones-sidebar-appearance": "",
    "data-jones-card-scale": String(value.cardScale),
    "data-jones-ws-spacing": value.workstreamSpacing,
    "data-jones-ws-title-weight": value.workstreamTitleWeight,
    "data-jones-ws-title-brightness": value.workstreamTitleBrightness,
    "data-jones-pill-bg": String(value.pillBackgroundOpacity),
  };
  const variables: Record<string, string> = {};
  const boost = value.threadTitleBrightnessBoost;
  if (Number.isInteger(boost) && boost > 0 && boost <= 100) {
    attributes["data-jones-thread-title-boost"] = "";
    variables["--jones-thread-title-boost"] = `${boost}%`;
  }
  return { attributes, variables };
}

export function deviceLabelHidden(input: {
  mode: SidebarAppearance["deviceLabel"];
  identity: "vps" | "test" | "laptop" | "mini" | "unknown";
  visibleLabel: string;
  shortLabel: string;
  iconOverridden: boolean;
}): boolean {
  const knownLabel =
    input.identity === "laptop" ? "Laptop" : input.identity === "vps" ? "VPS" : null;
  return (
    input.mode === "icon" &&
    !input.iconOverridden &&
    knownLabel !== null &&
    input.shortLabel === knownLabel &&
    input.visibleLabel === knownLabel
  );
}
