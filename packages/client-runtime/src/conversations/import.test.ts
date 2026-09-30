import { describe, expect, it } from "vite-plus/test";

import { LIBRARY_MAX_REQUEST_BYTES } from "@t3tools/contracts/conversationLibrary";
import {
  ConversationImportFileError,
  ConversationLibraryImport,
  readConversationExportFile,
} from "./import.ts";

const binding = { environmentId: "environment-a", generation: 1 };

const conversation = {
  id: "conversation-1",
  title: "Exported conversation",
  mapping: {
    node: {
      message: {
        author: { role: "user" },
        content: { parts: ["Hello"] },
      },
    },
  },
} as const;

function file(name = "conversations.json", value: unknown = [conversation], size?: number) {
  const text = JSON.stringify(value);
  return {
    name,
    size: size ?? new TextEncoder().encode(text).byteLength,
    text: async () => text,
  };
}

describe("conversation library import", () => {
  it("accepts only a valid conversations.json export", async () => {
    await expect(readConversationExportFile(file())).resolves.toMatchObject([
      { id: "conversation-1", title: "Exported conversation" },
    ]);
    await expect(readConversationExportFile(file("backup.json"))).rejects.toMatchObject({
      name: "ConversationImportFileError",
      code: "invalid",
    });
    await expect(
      readConversationExportFile(file("conversations.json", [{ title: "missing mapping" }])),
    ).rejects.toMatchObject({
      name: "ConversationImportFileError",
      code: "invalid",
    });
  });

  it("rejects an oversized file before reading it and checks the serialized request size", async () => {
    let read = false;
    const tooLargeFile = {
      name: "conversations.json",
      size: LIBRARY_MAX_REQUEST_BYTES + 1,
      text: async () => {
        read = true;
        return "[]";
      },
    };
    await expect(readConversationExportFile(tooLargeFile)).rejects.toBeInstanceOf(
      ConversationImportFileError,
    );
    expect(read).toBe(false);

    const largeConversation = {
      ...conversation,
      title: "x".repeat(LIBRARY_MAX_REQUEST_BYTES),
    };
    const importer = new ConversationLibraryImport();
    importer.bind(binding, true);
    expect(importer.requestPreview("account-1", [largeConversation])).toBeNull();
    expect(importer.getSnapshot()).toMatchObject({ status: "error" });
    expect(importer.getSnapshot().error).toContain("too large");
  });

  it("requires accepted preview and write capability before a separate import request", () => {
    const importer = new ConversationLibraryImport();
    importer.bind(binding, false);
    const preview = importer.requestPreview("account-1", [conversation]);
    expect(preview?.request.kind).toBe("preview");
    expect(
      importer.acceptPreview(preview!, {
        kind: "preview",
        conversations: [
          {
            id: "conversation-1",
            title: "Exported conversation",
            messageCount: 1,
            warningCount: 0,
          },
        ],
      }),
    ).toBe(true);
    expect(importer.getSnapshot()).toMatchObject({ status: "ready", accountId: "account-1" });
    expect(importer.requestImport()).toBeNull();

    importer.bind(binding, true);
    const confirmed = importer.requestImport();
    expect(confirmed?.request).toMatchObject({ kind: "import", accountId: "account-1" });
    expect(importer.getSnapshot().status).toBe("importing");
    expect(
      importer.acceptImport(confirmed!, {
        kind: "imported",
        inserted: 1,
        duplicates: 0,
        older: 0,
        conflicts: 0,
        revision: 2,
      }),
    ).toBe(true);
    expect(importer.getSnapshot()).toMatchObject({
      status: "complete",
      preview: null,
      result: { inserted: 1 },
    });
  });

  it("fences stale preview and import completions across connection generations", () => {
    const importer = new ConversationLibraryImport();
    importer.bind(binding, true);
    const preview = importer.requestPreview("account-1", [conversation]);
    expect(preview).not.toBeNull();
    importer.bind({ ...binding, generation: 2 }, true);
    expect(importer.acceptPreview(preview!, { kind: "preview", conversations: [] })).toBe(false);
    expect(importer.getSnapshot()).toMatchObject({ status: "idle", binding: { generation: 2 } });

    const currentPreview = importer.requestPreview("account-1", [conversation]);
    expect(currentPreview).not.toBeNull();
    expect(importer.acceptPreview(currentPreview!, { kind: "preview", conversations: [] })).toBe(
      true,
    );
    const importTicket = importer.requestImport();
    expect(importTicket).not.toBeNull();
    importer.bind({ ...binding, generation: 3 }, true);
    expect(
      importer.acceptImport(importTicket!, {
        kind: "imported",
        inserted: 1,
        duplicates: 0,
        older: 0,
        conflicts: 0,
        revision: 3,
      }),
    ).toBe(false);
    expect(importer.getSnapshot()).toMatchObject({ status: "idle", binding: { generation: 3 } });
  });

  it("fences a file read that finishes after the selected environment changes", () => {
    const importer = new ConversationLibraryImport();
    importer.bind(binding, true);
    const fileRead = importer.beginFileRead("account-1");
    expect(fileRead).not.toBeNull();
    importer.bind({ ...binding, generation: 2 }, true);
    expect(importer.requestPreviewForFile(fileRead!, [conversation])).toBeNull();
    expect(importer.getSnapshot()).toMatchObject({ status: "idle", binding: { generation: 2 } });
  });

  it("cancel releases the prepared payload and invalidates the pending reply", () => {
    const importer = new ConversationLibraryImport();
    importer.bind(binding, true);
    const ticket = importer.requestPreview("account-1", [conversation]);
    expect(ticket).not.toBeNull();
    importer.cancel();
    expect(importer.isPending(ticket!)).toBe(false);
    expect(importer.getSnapshot()).toMatchObject({
      status: "idle",
      accountId: null,
      preview: null,
    });
  });
});
