import * as Schema from "effect/Schema";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readLocalStorageValue, writeLocalStorageValue } from "../../hooks/useLocalStorage";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  ORIGINAL_SIDEBAR_APPEARANCE,
  SIDEBAR_APPEARANCE_PRESETS,
  SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES,
  SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES,
  SIDEBAR_APPEARANCE_STORAGE_KEY,
  SidebarAppearance,
  StoredSidebarAppearance,
  deviceLabelHidden,
  patchSidebarAppearance,
  resolveSidebarAppearance,
  sidebarAppearancePresetName,
  sidebarAppearanceScope,
} from "./model";

const decode = Schema.decodeUnknownSync(StoredSidebarAppearance);

describe("sidebar appearance schema", () => {
  it("uses the owner-selected defaults for missing fields and null records", () => {
    expect(resolveSidebarAppearance(null)).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
    expect(resolveSidebarAppearance(decode(null))).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
    expect(decode({})).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
    expect(decode({ cardScale: 0.87 })).toEqual({ ...DEFAULT_SIDEBAR_APPEARANCE, cardScale: 0.87 });
    expect(DEFAULT_SIDEBAR_APPEARANCE).toEqual({
      cardScale: 0.93,
      workstreamSpacing: "roomy",
      workstreamTitleWeight: "medium",
      workstreamTitleBrightness: "muted",
      pillBackgroundOpacity: 4,
      threadTitleBrightnessBoost: 0,
      countStyle: "numeric-right",
      deviceLabel: "icon",
      laptopSymbol: "filled",
      vpsSymbol: "filled",
    });
  });

  it("strips extra fields without treating them as preferences", () => {
    expect(decode({ cardScale: 1, extra: true, threadTitleContrast: "bright" })).toEqual({
      ...DEFAULT_SIDEBAR_APPEARANCE,
      cardScale: 1,
    });
  });

  it.each([
    false, 4, "appearance", [],
    { cardScale: "0.93" }, { cardScale: 0.94 }, { cardScale: null },
    { workstreamSpacing: "wide" }, { workstreamTitleWeight: 500 },
    { workstreamTitleBrightness: 0.88 }, { pillBackgroundOpacity: 8 },
    { countStyle: "fraction" }, { deviceLabel: "none" },
    { laptopSymbol: "left" }, { vpsSymbol: "half" },
    { threadTitleBrightnessBoost: -1 }, { threadTitleBrightnessBoost: 101 },
    { threadTitleBrightnessBoost: 0.5 }, { threadTitleBrightnessBoost: NaN },
    { threadTitleBrightnessBoost: Infinity }, { threadTitleBrightnessBoost: "50" },
    { threadTitleBrightnessBoost: null }, { threadTitleBrightnessBoost: undefined },
  ])("rejects an invalid record atomically: %j", (value) => {
    expect(() => decode(value)).toThrow();
  });

  it.each([0, 1, 50, 99, 100])("accepts integer boost %i", (boost) => {
    expect(decode({ threadTitleBrightnessBoost: boost })).toEqual({
      ...DEFAULT_SIDEBAR_APPEARANCE,
      threadTitleBrightnessBoost: boost,
    });
  });
});

describe("storage policy with the shared reader and notifying writer", () => {
  const values = new Map<string, string>();
  const notify = vi.fn();
  const read = () => readLocalStorageValue(SIDEBAR_APPEARANCE_STORAGE_KEY, null, StoredSidebarAppearance);
  const update = (patch: Partial<SidebarAppearance>) => writeLocalStorageValue(
    SIDEBAR_APPEARANCE_STORAGE_KEY,
    (current: SidebarAppearance | null) => patchSidebarAppearance(current, patch),
    null,
    StoredSidebarAppearance,
  );

  beforeEach(() => {
    values.clear();
    notify.mockClear();
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (key: string) => values.get(key) ?? null,
        setItem: (key: string, value: string) => values.set(key, value),
        removeItem: (key: string) => values.delete(key),
      },
      dispatchEvent: notify,
    });
    vi.spyOn(console, "error").mockImplementation(() => {});
  });
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  it.each([undefined, "null", "{", "[]", '{"cardScale":0.87,"pillBackgroundOpacity":8}']) (
    "falls back atomically without rewriting absent or invalid bytes: %s", (raw) => {
      if (raw !== undefined) values.set(SIDEBAR_APPEARANCE_STORAGE_KEY, raw);
      expect(resolveSidebarAppearance(read())).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
      expect(values.get(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBe(raw);
      expect(notify).not.toHaveBeenCalled();
    },
  );

  it("merges each update with the latest record and strips unknown fields on write", () => {
    values.set(SIDEBAR_APPEARANCE_STORAGE_KEY, '{"cardScale":0.87,"extra":true}');
    update({ workstreamSpacing: "tight" });
    update({ threadTitleBrightnessBoost: 63 });
    expect(read()).toEqual({ ...DEFAULT_SIDEBAR_APPEARANCE, cardScale: 0.87, workstreamSpacing: "tight", threadTitleBrightnessBoost: 63 });
    expect(values.get(SIDEBAR_APPEARANCE_STORAGE_KEY)).not.toContain("extra");
    expect(notify).toHaveBeenCalledTimes(2);
    expect(notify.mock.calls[0]?.[0].detail).toEqual({ key: SIDEBAR_APPEARANCE_STORAGE_KEY });
  });

  it("replaces corrupt storage on the next functional update", () => {
    values.set(SIDEBAR_APPEARANCE_STORAGE_KEY, "{");
    update({ cardScale: 1 });
    expect(read()).toEqual({ ...DEFAULT_SIDEBAR_APPEARANCE, cardScale: 1 });
  });

  it("applies either preset while retaining the latest boost", () => {
    update({ threadTitleBrightnessBoost: 72 });
    update(SIDEBAR_APPEARANCE_PRESETS.original);
    expect(read()).toEqual({ ...ORIGINAL_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: 72 });
    update(SIDEBAR_APPEARANCE_PRESETS.selected);
    expect(read()).toEqual({ ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: 72 });
  });

  it("removes the key and notifies when returning to defaults or resetting", () => {
    update({ cardScale: 1 });
    update({ cardScale: 0.93 });
    expect(values.has(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBe(false);
    update({ threadTitleBrightnessBoost: 100 });
    writeLocalStorageValue(SIDEBAR_APPEARANCE_STORAGE_KEY, null, null, StoredSidebarAppearance);
    expect(values.has(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBe(false);
    expect(resolveSidebarAppearance(read())).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
    expect(notify).toHaveBeenCalledTimes(4);
  });

  it("safely falls back when access to localStorage is denied", () => {
    vi.stubGlobal("window", { get localStorage() { throw new Error("denied"); } });
    expect(resolveSidebarAppearance(read())).toEqual(DEFAULT_SIDEBAR_APPEARANCE);
    expect(() => update({ cardScale: 1 })).not.toThrow();
    expect(values.size).toBe(0);
  });
});

describe("presets and scoped presentation", () => {
  it("recognizes presets independently of brightness and identifies custom looks", () => {
    expect(sidebarAppearancePresetName({ ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: 100 })).toBe("selected");
    expect(sidebarAppearancePresetName({ ...ORIGINAL_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: 50 })).toBe("original");
    expect(sidebarAppearancePresetName({ ...DEFAULT_SIDEBAR_APPEARANCE, cardScale: 0.87 })).toBeNull();
    expect(patchSidebarAppearance(null, {})).toBeNull();
    expect(patchSidebarAppearance(ORIGINAL_SIDEBAR_APPEARANCE, DEFAULT_SIDEBAR_APPEARANCE)).toBeNull();
  });

  it("emits neutral Original scope without a title brightness override", () => {
    expect(sidebarAppearanceScope(ORIGINAL_SIDEBAR_APPEARANCE)).toEqual({
      attributes: {
        "data-jones-sidebar-appearance": "",
        "data-jones-card-scale": "1",
        "data-jones-ws-spacing": "default",
        "data-jones-ws-title-weight": "default",
        "data-jones-ws-title-brightness": "default",
        "data-jones-pill-bg": "30",
      },
      variables: {},
    });
    expect(sidebarAppearanceScope(DEFAULT_SIDEBAR_APPEARANCE).attributes["data-jones-card-scale"]).toBe("0.93");
  });

  it("emits only allowlisted scope values and bounds the inline boost", () => {
    const scope = sidebarAppearanceScope({ ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: 50 });
    expect(scope.attributes["data-jones-thread-title-boost"]).toBe("");
    expect(scope.variables).toEqual({ "--jones-thread-title-boost": "50%" });
    expect(Object.keys(scope.attributes).every((name) => SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES.includes(name))).toBe(true);
    expect(Object.keys(scope.variables)).toEqual(SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES);
    for (const boost of [0, -1, 101, NaN, Infinity, 0.5]) {
      const invalidScope = sidebarAppearanceScope({ ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: boost });
      expect(invalidScope.attributes).not.toHaveProperty("data-jones-thread-title-boost");
      expect(invalidScope.variables).toEqual({});
    }
  });
});

describe("device label boundaries", () => {
  const laptop = { mode: "icon", identity: "laptop", visibleLabel: "Laptop", shortLabel: "Laptop", iconOverridden: false } as const;
  it("hides only the known Laptop or VPS short label in icon mode", () => {
    expect(deviceLabelHidden(laptop)).toBe(true);
    expect(deviceLabelHidden({ ...laptop, identity: "vps", visibleLabel: "VPS", shortLabel: "VPS" })).toBe(true);
    expect(deviceLabelHidden({ ...laptop, mode: "label" })).toBe(false);
    expect(deviceLabelHidden({ ...laptop, iconOverridden: true })).toBe(false);
    for (const identity of ["mini", "test", "unknown"] as const) expect(deviceLabelHidden({ ...laptop, identity })).toBe(false);
    for (const label of ["", "My Laptop", "laptop", "Laptop ", "VPS"]) {
      expect(deviceLabelHidden({ ...laptop, visibleLabel: label })).toBe(false);
    }
    expect(deviceLabelHidden({ ...laptop, visibleLabel: "Custom", shortLabel: "Custom" })).toBe(false);
    expect(deviceLabelHidden({ ...laptop, shortLabel: "" })).toBe(false);
  });
});
