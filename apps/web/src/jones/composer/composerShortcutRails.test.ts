import { describe, expect, it } from "vite-plus/test";
import { resolveComposerShortcutRailsVisibility } from "./composerShortcutRails";

const roomy = {
  eligible: true,
  formWidth: 900,
  hostWidth: 1000,
  conversationHeight: 700,
  groupWidths: [116, 200],
  groupHeights: [80, 80],
};

describe("composer shortcut rails availability", () => {
  it.each([620, 619, 536, 320])(
    "keeps the external controls at compact form width %i",
    (formWidth) => {
      expect(resolveComposerShortcutRailsVisibility({ ...roomy, formWidth })).toBe(true);
    },
  );

  it.each([332, 331, 200, 116])(
    "keeps groups available for wrapping or scrolling at host width %i",
    (hostWidth) => {
      expect(resolveComposerShortcutRailsVisibility({ ...roomy, hostWidth })).toBe(true);
    },
  );

  it.each([480, 320, 160])(
    "keeps the controls in short conversations of height %i",
    (conversationHeight) => {
      expect(resolveComposerShortcutRailsVisibility({ ...roomy, conversationHeight })).toBe(true);
    },
  );

  it.each([
    { formWidth: 0 },
    { hostWidth: 0 },
    { conversationHeight: 0 },
    { eligible: false },
    { groupWidths: [] },
    { groupHeights: [0] },
  ])("retains unavailable or unmounted controls outside the tab order: %j", (change) => {
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, ...change })).toBe(false);
  });
});
