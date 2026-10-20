import { describe, expect, it } from "vite-plus/test";
import { compactJonesVersion, compactJonesVersionText, shortJonesBuildLabel } from "./buildVersion";

describe("compact Jones build versions", () => {
  it.each([
    ["0.0.45-preview.20261016.38974945620.1", "0.45 · 5620"],
    ["0.0.45-preview.20261013.38343490676.1", "0.45 · 0676"],
    ["0.0.45-preview.20261016.38974945620", "0.45 · 5620"],
    ["0.0.46-preview.20261101.812.1", "0.46 · 812"],
    ["0.3.2-preview.20261016.38974945620.1", "0.3.2 · 5620"],
    ["1.2.3", "1.2.3"],
    ["0.0.45", "0.45"],
  ])("shows %s as %s", (version, text) => {
    expect(compactJonesVersionText(version)).toBe(text);
  });

  it.each(["preview", "0.0.45-beta.1", "0.0.45-preview.2026101.1.1", ""])(
    "returns an unrecognized version %j whole",
    (version) => {
      expect(compactJonesVersion(version)).toEqual({ core: version });
    },
  );
});

describe("short Jones build label", () => {
  it.each([
    ["0.1.0-preview.20261020.39438472507.1", "2507.1"],
    ["0.1.0-preview.20261020.39438472507.12", "2507.12"],
    ["0.1.0-preview.20261020.39438472507", "2507"],
    ["0.1.0-preview.20261020.39438472507.1+build.7", "2507.1"],
    ["0.0.46-preview.20261101.812.1", "812.1"],
    ["0.0.45", "0.45"],
    ["1.2.3+sha.abc", "1.2.3"],
    ["0.3.2", "0.3.2"],
  ])("labels %s as %s", (version, label) => {
    expect(shortJonesBuildLabel(version)).toBe(label);
  });

  it.each(["0.0.0", "0.0.0+dev", "", "preview", "0.0.45-beta.1", "0.0.45-preview.2026101.1.1"])(
    "has no label for %j",
    (version) => {
      expect(shortJonesBuildLabel(version)).toBeUndefined();
    },
  );
});
