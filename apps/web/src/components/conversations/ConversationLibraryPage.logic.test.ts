import type { LibraryDetail } from "@t3tools/contracts/conversationLibrary";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  acknowledgeVisibleConversationDetail,
  conversationLibraryDate,
  conversationLibraryImportTargetAccount,
  conversationLibrarySnapshotRequest,
  createConversationLibraryAccountRequest,
} from "./ConversationLibraryPage.logic";

describe("conversation library page logic", () => {
  it("keeps a preview bound to its original account after selection changes", () => {
    const first = { id: "first", label: "First", workspace: "Home" };
    const second = { id: "second", label: "Second", workspace: "Work" };
    expect(conversationLibraryImportTargetAccount([first, second], second, "first")).toEqual(first);
    expect(conversationLibraryImportTargetAccount([second], second, "first")).toBeNull();
    expect(conversationLibraryImportTargetAccount([first, second], second, null)).toEqual(second);
  });

  it("acknowledges a detail only while its panel is visible", () => {
    const detail = {} as LibraryDetail;
    const acknowledge = vi.fn(() => true);
    const panel = (count: number): Pick<HTMLElement, "getClientRects"> => ({
      getClientRects: () => ({ length: count }) as DOMRectList,
    });
    expect(acknowledgeVisibleConversationDetail(panel(0), detail, acknowledge)).toBe(false);
    expect(acknowledgeVisibleConversationDetail(null, detail, acknowledge)).toBe(false);
    expect(acknowledge).not.toHaveBeenCalled();
    expect(acknowledgeVisibleConversationDetail(panel(1), detail, acknowledge)).toBe(true);
    expect(acknowledge).toHaveBeenCalledTimes(1);
    expect(acknowledge).toHaveBeenCalledWith(detail);
  });

  it("requires a connected writable target and bounded account labels", () => {
    expect(createConversationLibraryAccountRequest("Personal", "Home", false, true)).toBeNull();
    expect(createConversationLibraryAccountRequest("Personal", "Home", true, false)).toBeNull();
    expect(createConversationLibraryAccountRequest(" ", "Home", true, true)).toBeNull();
    expect(createConversationLibraryAccountRequest("Personal", " ", true, true)).toBeNull();
    expect(createConversationLibraryAccountRequest("x".repeat(201), "Home", true, true)).toBeNull();
    expect(createConversationLibraryAccountRequest(" Personal ", " Home ", true, true)).toEqual({
      kind: "createAccount",
      label: "Personal",
      workspace: "Home",
    });
  });

  it("interprets library timestamps as milliseconds", () => {
    expect(conversationLibraryDate(1_704_067_200_000)?.toISOString()).toBe(
      "2024-01-01T00:00:00.000Z",
    );
    expect(conversationLibraryDate(null)).toBeNull();
  });

  it("browses a snapshot with a detail read request", () => {
    const detail = {
      kind: "detail" as const,
      conversation: {
        key: "account:conversation",
        accountId: "account",
        conversationId: "conversation",
        title: "A conversation",
        snapshotId: "current",
        sourceUpdatedAt: 1_704_067_200_000,
        importedAt: 1_704_067_200_000,
        revision: 4,
        unread: false,
        pinned: false,
        archived: false,
        attention: false,
        conflicts: false,
        messageCount: 1,
        warningCount: 0,
      },
      account: { id: "account", label: "Home", workspace: "Personal" },
      snapshotId: "current",
      nodeId: null,
      snapshotSourceUpdatedAt: 1_704_067_200_000,
      snapshotImportedAt: 1_704_067_200_000,
      showHidden: true,
      messages: [],
      totalMessages: 1,
      readThrough: 0,
      offset: 50,
      previousOffset: 0,
      nextOffset: null,
      snapshots: [],
      snapshotOffset: 10,
      snapshotCount: 11,
      branches: [],
      branchOffset: 0,
      branchCount: 1,
      warnings: [],
    };

    expect(conversationLibrarySnapshotRequest(detail, "older")).toEqual({
      kind: "detail",
      key: "account:conversation",
      snapshotId: "older",
      offset: 0,
      snapshotOffset: 10,
      branchOffset: 0,
      showHidden: true,
    });
  });
});
