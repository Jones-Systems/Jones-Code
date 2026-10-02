import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  CONVERSATION_LIBRARY_PROTOCOL,
  LibraryErrorCodeSchema,
  LibraryReplySchema,
  LibraryRequestSchema,
  type LibraryRequest,
} from "./conversationLibrary.ts";

const decodeRequest = Schema.decodeUnknownSync(LibraryRequestSchema);
const decodeReply = Schema.decodeUnknownSync(LibraryReplySchema);
const decodeErrorCode = Schema.decodeUnknownSync(LibraryErrorCodeSchema);

describe("conversation library runtime contract", () => {
  it("decodes every existing request discriminator and rejects malformed variants", () => {
    const requests = [
      { kind: "hello" },
      { kind: "accounts" },
      { kind: "createAccount", label: "Personal", workspace: "local" },
      { kind: "preview", conversations: [{ title: "Sample", mapping: {} }] },
      { kind: "import", accountId: "account-1", conversations: [{ title: "Sample", mapping: {} }] },
      { kind: "list", view: "unread" },
      { kind: "detail", key: "conversation-1", offset: 0 },
      { kind: "update", key: "conversation-1", pinned: true },
      {
        kind: "selectSnapshot",
        key: "conversation-1",
        snapshotId: "snapshot-1",
        expectedRevision: 1,
      },
      { kind: "remove", key: "conversation-1", expectedRevision: 1 },
    ] satisfies readonly LibraryRequest[];

    expect(requests.map((request) => decodeRequest(request).kind)).toEqual(
      requests.map((request) => request.kind),
    );
    expect(() => decodeRequest({ kind: "import", accountId: 1, conversations: [] })).toThrow();
    expect(() => decodeRequest({ kind: "unknown" })).toThrow();
  });

  it("decodes every existing reply discriminator and its nested record shapes", () => {
    const replies = [
      {
        kind: "hello",
        protocol: CONVERSATION_LIBRARY_PROTOCOL,
        revision: 1,
        canWrite: false,
        capture: "not-enabled",
      },
      { kind: "accounts", accounts: [{ id: "account-1", label: "Personal", workspace: "local" }] },
      { kind: "account", account: { id: "account-1", label: "Personal", workspace: "local" } },
      {
        kind: "preview",
        conversations: [
          { id: "conversation-1", title: "Sample", messageCount: 2, warningCount: 0 },
        ],
      },
      { kind: "imported", inserted: 1, duplicates: 0, older: 0, conflicts: 0, revision: 2 },
      { kind: "list", rows: [], revision: 2, cursor: null },
      {
        kind: "detail",
        conversation: {
          key: "key-1",
          accountId: "account-1",
          conversationId: "conversation-1",
          title: "Sample",
          snapshotId: "snapshot-1",
          sourceUpdatedAt: null,
          importedAt: 1,
          revision: 2,
          unread: false,
          pinned: false,
          archived: false,
          attention: false,
          conflicts: false,
          messageCount: 1,
          warningCount: 0,
        },
        account: { id: "account-1", label: "Personal", workspace: "local" },
        snapshotId: "snapshot-1",
        snapshotSourceUpdatedAt: null,
        snapshotImportedAt: 1,
        showHidden: false,
        nodeId: null,
        messages: [],
        totalMessages: 0,
        readThrough: 0,
        offset: 0,
        previousOffset: null,
        nextOffset: null,
        snapshots: [],
        snapshotOffset: 0,
        snapshotCount: 1,
        branches: [],
        branchOffset: 0,
        branchCount: 0,
        warnings: [],
      },
      { kind: "updated", revision: 3 },
      { kind: "removed", revision: 4 },
    ] as const;

    expect(replies.map((reply) => decodeReply(reply).kind)).toEqual(
      replies.map((reply) => reply.kind),
    );
    expect(() =>
      decodeReply({
        kind: "hello",
        protocol: CONVERSATION_LIBRARY_PROTOCOL,
        revision: 1,
        capture: "not-enabled",
      }),
    ).toThrow();
  });

  it("validates the known library error codes", () => {
    expect(
      ["invalid", "too-large", "not-found", "conflict", "unsupported", "storage", "forbidden"].map(
        (code) => decodeErrorCode(code),
      ),
    ).toEqual([
      "invalid",
      "too-large",
      "not-found",
      "conflict",
      "unsupported",
      "storage",
      "forbidden",
    ]);
    expect(() => decodeErrorCode("private-detail")).toThrow();
  });
});
