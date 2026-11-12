import { describe, expect, it } from "vite-plus/test";

import { derivePendingUserInputMaxHeight } from "../../features/threads/pendingUserInputLayout";

describe("question card beside the visible mobile composer", () => {
  it.each([
    { label: "iOS safe area", keyboardHeight: 336, bottomInset: 34 },
    { label: "Android keyboard", keyboardHeight: 300, bottomInset: 0 },
  ])("keeps both composer sizes below the card with $label", ({ keyboardHeight, bottomInset }) => {
    const viewport = {
      windowHeight: 844,
      keyboardHeight,
      navigationHeaderHeight: 103,
    };
    const collapsedComposerHeight = 60 + bottomInset;
    const expandedComposerHeight = 156 + bottomInset;
    const collapsedCardHeight = derivePendingUserInputMaxHeight({
      ...viewport,
      composerOverlapHeight: collapsedComposerHeight,
    });
    const expandedCardHeight = derivePendingUserInputMaxHeight({
      ...viewport,
      composerOverlapHeight: expandedComposerHeight,
    });

    expect(collapsedCardHeight + collapsedComposerHeight).toBe(
      expandedCardHeight + expandedComposerHeight,
    );
    expect(expandedCardHeight).toBeGreaterThanOrEqual(160);
    expect(
      expandedCardHeight +
        expandedComposerHeight +
        keyboardHeight +
        viewport.navigationHeaderHeight,
    ).toBeLessThan(viewport.windowHeight);
  });
});
