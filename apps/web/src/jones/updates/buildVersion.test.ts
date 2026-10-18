import { describe, expect, it } from "vite-plus/test";
import { compactJonesVersion, compactJonesVersionText } from "./buildVersion";

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
