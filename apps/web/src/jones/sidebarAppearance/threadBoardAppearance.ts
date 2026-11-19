import * as Schema from "effect/Schema";

export const THREAD_BOARD_APPEARANCE_KEY = "jones:thread-board-appearance:v1";

export const THREAD_BOARD_MEASUREMENTS = {
  cardScale: { variable: "card-scale", min: 75, max: 125, unit: "%", initial: 93 },
  cardPadding: { variable: "card-padding", min: 2, max: 16, unit: "px", initial: 6 },
  cardGap: { variable: "card-gap", min: 0, max: 16, unit: "px", initial: 4 },
  cardTitleSize: { variable: "card-title-size", min: 10, max: 22, unit: "px", initial: 13 },
  cardTitleWeight: { variable: "card-title-weight", min: 400, max: 800, unit: "", initial: 500 },
  cardMetaSize: { variable: "card-meta-size", min: 9, max: 18, unit: "px", initial: 11 },
  cardBorderWidth: { variable: "card-border-width", min: 0, max: 3, unit: "px", initial: 1 },
  cardBorderOpacity: { variable: "card-border-opacity", min: 0, max: 65, unit: "%", initial: 17 },
  groupBorderWidth: { variable: "group-border-width", min: 0, max: 3, unit: "px", initial: 1 },
  groupBorderOpacity: { variable: "group-border-opacity", min: 0, max: 65, unit: "%", initial: 20 },
  tintOpacity: { variable: "tint-opacity", min: 0, max: 60, unit: "%", initial: 10 },
  tintBrightness: { variable: "tint-brightness", min: 50, max: 150, unit: "%", initial: 100 },
  headerPadding: { variable: "header-padding", min: 0, max: 18, unit: "px", initial: 6 },
  headerPaddingTop: { variable: "header-padding-top", min: 0, max: 18, unit: "px", initial: 4 },
  headerPaddingBottom: {
    variable: "header-padding-bottom",
    min: 0,
    max: 18,
    unit: "px",
    initial: 4,
  },
  headerPaddingInline: {
    variable: "header-padding-inline",
    min: 2,
    max: 18,
    unit: "px",
    initial: 8,
  },
  headerTitleSize: { variable: "header-title-size", min: 10, max: 22, unit: "px", initial: 14 },
  headerTitleWeight: {
    variable: "header-title-weight",
    min: 400,
    max: 800,
    unit: "",
    initial: 500,
  },
  countSize: { variable: "count-size", min: 8, max: 18, unit: "px", initial: 10 },
  countGap: { variable: "count-gap", min: 0, max: 14, unit: "px", initial: 4 },
  countOffsetX: { variable: "count-offset-x", min: -12, max: 8, unit: "px", initial: 0 },
  countOffsetY: { variable: "count-offset-y", min: -12, max: 12, unit: "px", initial: 0 },
  filterSize: { variable: "filter-size", min: 9, max: 18, unit: "px", initial: 12 },
  filterGap: { variable: "filter-gap", min: 0, max: 14, unit: "px", initial: 2 },
  filterOffsetX: { variable: "filter-offset-x", min: -12, max: 8, unit: "px", initial: 0 },
  filterOffsetY: { variable: "filter-offset-y", min: -12, max: 12, unit: "px", initial: 0 },
  headerGap: { variable: "header-gap", min: 0, max: 16, unit: "px", initial: 3 },
} as const;

export type ThreadBoardMeasurement = keyof typeof THREAD_BOARD_MEASUREMENTS;
const measurementKeys = Object.keys(THREAD_BOARD_MEASUREMENTS) as ThreadBoardMeasurement[];

function measurement(key: ThreadBoardMeasurement) {
  const { min, max } = THREAD_BOARD_MEASUREMENTS[key];
  return Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: min, maximum: max })));
}

export const ThreadBoardColor = Schema.String.check(Schema.isPattern(/^#[\da-f]{6}$/i));

export const ThreadBoardAppearanceSchema = Schema.Struct({
  cardScale: measurement("cardScale"),
  cardPadding: measurement("cardPadding"),
  cardGap: measurement("cardGap"),
  cardTitleSize: measurement("cardTitleSize"),
  cardTitleWeight: measurement("cardTitleWeight"),
  cardMetaSize: measurement("cardMetaSize"),
  cardBorderWidth: measurement("cardBorderWidth"),
  cardBorderOpacity: measurement("cardBorderOpacity"),
  groupBorderWidth: measurement("groupBorderWidth"),
  groupBorderOpacity: measurement("groupBorderOpacity"),
  tintOpacity: measurement("tintOpacity"),
  tintBrightness: measurement("tintBrightness"),
  headerPadding: measurement("headerPadding"),
  headerPaddingTop: measurement("headerPaddingTop"),
  headerPaddingBottom: measurement("headerPaddingBottom"),
  headerPaddingInline: measurement("headerPaddingInline"),
  headerTitleSize: measurement("headerTitleSize"),
  headerTitleWeight: measurement("headerTitleWeight"),
  countSize: measurement("countSize"),
  countGap: measurement("countGap"),
  countOffsetX: measurement("countOffsetX"),
  countOffsetY: measurement("countOffsetY"),
  filterSize: measurement("filterSize"),
  filterGap: measurement("filterGap"),
  filterOffsetX: measurement("filterOffsetX"),
  filterOffsetY: measurement("filterOffsetY"),
  headerGap: measurement("headerGap"),
  filterSpacing: Schema.optionalKey(Schema.Literals(["adaptive", "fixed"])),
  clickRailToCollapse: Schema.optionalKey(Schema.Boolean),
  cardBorderColor: Schema.optionalKey(ThreadBoardColor),
  background: Schema.optionalKey(ThreadBoardColor),
  unassignedBackground: Schema.optionalKey(ThreadBoardColor),
});

export type ThreadBoardAppearance = typeof ThreadBoardAppearanceSchema.Type;
export type ThreadBoardColorKey = "background" | "unassignedBackground" | "cardBorderColor";
export const StoredThreadBoardAppearance = Schema.NullOr(ThreadBoardAppearanceSchema);
const isBoardColor = Schema.is(ThreadBoardColor);
const decode = Schema.decodeSync(ThreadBoardAppearanceSchema);

export function copyThreadBoardCardVariables(source: HTMLElement | null, target: HTMLElement) {
  const borderColor = source?.style.getPropertyValue("--jones-board-card-border-color").trim();
  if (borderColor && isBoardColor(borderColor)) {
    target.style.setProperty("--jones-board-card-border-color", borderColor);
  } else {
    target.style.removeProperty("--jones-board-card-border-color");
  }
  for (const key of measurementKeys) {
    const { variable, unit, min, max } = THREAD_BOARD_MEASUREMENTS[key];
    if (!variable.startsWith("card-")) continue;
    const name = `--jones-board-${variable}`;
    const value = source?.style.getPropertyValue(name).trim() ?? "";
    const number = Number.parseFloat(value);
    const amount = key === "cardScale" ? Math.round(number * 100) : number;
    if (
      Number.isInteger(amount) &&
      amount >= min &&
      amount <= max &&
      value === `${number}${key === "cardScale" ? "" : unit}`
    ) {
      target.style.setProperty(name, value);
    } else {
      target.style.removeProperty(name);
    }
  }
}

export function threadBoardAppearanceScope(value: ThreadBoardAppearance) {
  const variables: Record<string, string> = {};
  for (const key of measurementKeys) {
    const amount = value[key];
    if (amount === undefined) continue;
    const { variable, unit } = THREAD_BOARD_MEASUREMENTS[key];
    variables[`--jones-board-${variable}`] =
      key === "cardScale" ? String(amount / 100) : `${amount}${unit}`;
  }
  if (value.background) variables["--jones-board-background"] = value.background;
  if (value.cardBorderColor) variables["--jones-board-card-border-color"] = value.cardBorderColor;
  if (value.unassignedBackground) {
    variables["--jones-board-unassigned-background"] = value.unassignedBackground;
  }
  return {
    attributes: {
      "data-jones-board-filter-spacing": value.filterSpacing ?? "adaptive",
      "data-jones-board-click-rail": value.clickRailToCollapse ? "enabled" : "disabled",
    },
    variables,
  };
}

export function setThreadBoardAppearanceField<Key extends keyof ThreadBoardAppearance>(
  current: ThreadBoardAppearance | null,
  key: Key,
  value: ThreadBoardAppearance[Key],
): ThreadBoardAppearance | null {
  const next = { ...current };
  if (value === undefined) delete next[key];
  else Object.assign(next, { [key]: value });
  return Object.keys(next).length > 0 ? decode(next) : null;
}
