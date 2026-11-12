import { describe, expect, it } from "vite-plus/test";
import { PROVIDER_SEND_TURN_MAX_ATTACHMENTS } from "@t3tools/contracts";
import { remainingComposerAttachmentSlots } from "./attachmentCapacity";

describe("stash attachment capacity", () => {
  it("reserves pending imports alongside files and images", () => {
    expect(remainingComposerAttachmentSlots(PROVIDER_SEND_TURN_MAX_ATTACHMENTS - 3, 2)).toBe(1);
  });
  it("never restores beyond a full or over-reserved draft", () => {
    expect(remainingComposerAttachmentSlots(PROVIDER_SEND_TURN_MAX_ATTACHMENTS, 0)).toBe(0);
    expect(remainingComposerAttachmentSlots(PROVIDER_SEND_TURN_MAX_ATTACHMENTS - 1, 3)).toBe(0);
  });
});
