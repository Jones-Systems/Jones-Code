import { describe, expect, it } from "vite-plus/test";
import { resolveComposerShortcutRailsVisibility } from "./composerShortcutRails";

const roomy = {
  eligible: true,
  formWidth: 900,
  hostWidth: 1000,
  conversationHeight: 700,
  composerHeight: 220,
  currentBandHeight: 0,
  groupWidths: [116, 200],
  groupHeights: [80, 80],
  hasWideActions: false,
  previousVisible: false,
};

describe("composer shortcut rails fit", () => {
  it("uses the complete natural footprints and a sixteen pixel gap", () => {
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, hostWidth: 332 })).toBe(false);
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, hostWidth: 333 })).toBe(true);
    expect(
      resolveComposerShortcutRailsVisibility({ ...roomy, hostWidth: 332, previousVisible: true }),
    ).toBe(true);
    expect(
      resolveComposerShortcutRailsVisibility({ ...roomy, hostWidth: 331, previousVisible: true }),
    ).toBe(false);
  });

  it("retains 160 pixels for conversation and restores with one pixel slack", () => {
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, conversationHeight: 460 })).toBe(
      false,
    );
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, conversationHeight: 461 })).toBe(
      true,
    );
    expect(
      resolveComposerShortcutRailsVisibility({
        ...roomy,
        conversationHeight: 460,
        previousVisible: true,
      }),
    ).toBe(true);
  });

  it("excludes the currently displayed band so it cannot toggle its own fit", () => {
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, conversationHeight: 461 })).toBe(
      true,
    );
    expect(
      resolveComposerShortcutRailsVisibility({
        ...roomy,
        conversationHeight: 461,
        composerHeight: 300,
        currentBandHeight: 80,
        previousVisible: true,
      }),
    ).toBe(true);
  });

  it.each([
    { formWidth: 619 },
    { formWidth: 779, hasWideActions: true },
    { hostWidth: 0 },
    { eligible: false },
    { groupWidths: [] },
    { groupHeights: [0] },
  ])("hides immediately when a required fit condition is lost: %j", (change) => {
    expect(
      resolveComposerShortcutRailsVisibility({ ...roomy, previousVisible: true, ...change }),
    ).toBe(false);
  });

  it("keeps the existing ordinary and wide-action compact boundaries", () => {
    expect(resolveComposerShortcutRailsVisibility({ ...roomy, formWidth: 620 })).toBe(true);
    expect(
      resolveComposerShortcutRailsVisibility({ ...roomy, formWidth: 780, hasWideActions: true }),
    ).toBe(true);
  });
});
