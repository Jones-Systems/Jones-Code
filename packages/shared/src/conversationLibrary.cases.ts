import * as NodeAssert from "node:assert/strict";
import type { ExportConversation } from "@t3tools/contracts/conversationLibrary";
import {
  LibraryRequestFence,
  libraryBranch,
  libraryBranchIds,
  libraryOriginalUrl,
  libraryRequestMutates,
  normalizeConversationExport,
} from "./conversationLibrary.ts";

function sampleConversation(): ExportConversation {
  return {
    id: "chat-one",
    title: "A sample conversation",
    update_time: 1_700_000_000,
    current_node: "a",
    mapping: {
      root: { id: "root", parent: null, message: null },
      u: {
        id: "u",
        parent: "root",
        message: {
          id: "user-id",
          author: { role: "user" },
          content: { content_type: "text", parts: ["Question"] },
        },
      },
      a: {
        id: "a",
        parent: "u",
        message: {
          id: "answer-id",
          author: { role: "assistant" },
          content: { content_type: "text", parts: ["Answer"] },
        },
      },
      b: {
        id: "b",
        parent: "u",
        message: {
          id: "other-answer-id",
          author: { role: "assistant" },
          content: { content_type: "text", parts: ["Regenerated answer"] },
        },
      },
    },
  };
}

export const conversationLibraryCases: readonly {
  readonly name: string;
  readonly run: () => void;
}[] = [
  {
    name: "preserves the selected path and regenerated alternatives",
    run: () => {
      const snapshot = normalizeConversationExport([sampleConversation()])[0]!;
      NodeAssert.deepEqual(
        libraryBranch(snapshot.nodes, snapshot.currentNodeId).map((n) => n.text),
        ["Question", "Answer"],
      );
      NodeAssert.deepEqual(
        libraryBranch(snapshot.nodes, "b").map((n) => n.text),
        ["Question", "Regenerated answer"],
      );
      NodeAssert.deepEqual(libraryBranchIds(snapshot.nodes, snapshot.currentNodeId), ["a", "b"]);
    },
  },
  {
    name: "canonicalizes mapping order without merging distinct nodes",
    run: () => {
      const sample = sampleConversation();
      const reordered = {
        ...sample,
        mapping: Object.fromEntries(Object.entries(sample.mapping).reverse()),
      };
      NodeAssert.equal(
        JSON.stringify(normalizeConversationExport([sample])),
        JSON.stringify(normalizeConversationExport([reordered])),
      );
    },
  },
  {
    name: "rejects a cycle outside the selected path",
    run: () => {
      const sample = sampleConversation();
      const mapping = { ...sample.mapping, x: { parent: "y" }, y: { parent: "x" } };
      NodeAssert.throws(() => normalizeConversationExport([{ ...sample, mapping }]), /cycle/);
    },
  },
  {
    name: "retains incomplete branches with an explicit gap",
    run: () => {
      const sample = sampleConversation();
      const mapping = { ...sample.mapping, u: { ...sample.mapping.u!, parent: "missing" } };
      const snapshot = normalizeConversationExport([{ ...sample, mapping }])[0]!;
      NodeAssert.match(snapshot.warnings.join(" "), /history gap/);
      NodeAssert.equal(libraryBranch(snapshot.nodes, "a").length, 2);
    },
  },
  {
    name: "does not choose another branch when the exported selection is missing",
    run: () => {
      const snapshot = normalizeConversationExport([
        { ...sampleConversation(), current_node: "unknown" },
      ])[0]!;
      NodeAssert.equal(snapshot.currentNodeId, null);
      NodeAssert.deepEqual(libraryBranch(snapshot.nodes, null), []);
      NodeAssert.throws(() => libraryBranch(snapshot.nodes, "unknown"), /not in this snapshot/);
    },
  },
  {
    name: "reports unsupported image and tool parts without extracting their URLs",
    run: () => {
      const snapshot = normalizeConversationExport([
        {
          id: "media",
          title: "Media",
          current_node: "one",
          mapping: {
            one: {
              message: {
                author: { role: "assistant" },
                content: {
                  parts: ["Visible text", { asset_pointer: "https://example.invalid/private" }],
                },
              },
            },
          },
        },
      ])[0]!;
      NodeAssert.equal(snapshot.nodes[0]!.text, "Visible text");
      NodeAssert.equal(snapshot.nodes[0]!.unsupportedParts, 1);
      NodeAssert.match(snapshot.warnings.join(" "), /non-text parts/);
      NodeAssert.ok(!JSON.stringify(snapshot).includes("private"));
    },
  },
  {
    name: "keeps hidden exported messages without displaying them by default",
    run: () => {
      const snapshot = normalizeConversationExport([
        {
          id: "hidden",
          title: "Hidden",
          current_node: "one",
          mapping: {
            one: {
              message: {
                author: { role: "system" },
                metadata: { is_visually_hidden_from_conversation: true },
                content: { parts: ["Hidden text"] },
              },
            },
          },
        },
      ])[0]!;
      NodeAssert.equal(libraryBranch(snapshot.nodes, "one").length, 0);
      NodeAssert.equal(libraryBranch(snapshot.nodes, "one", true).length, 1);
    },
  },
  {
    name: "rejects duplicate conversation identities in a batch",
    run: () => {
      NodeAssert.throws(
        () => normalizeConversationExport([sampleConversation(), sampleConversation()]),
        /twice/,
      );
    },
  },
  {
    name: "rejects contradictory aliases and mismatched mapping IDs",
    run: () => {
      NodeAssert.throws(
        () => normalizeConversationExport([{ ...sampleConversation(), conversation_id: "other" }]),
        /conflicting/,
      );
      NodeAssert.throws(
        () =>
          normalizeConversationExport([
            { ...sampleConversation(), mapping: { key: { id: "different" } } },
          ]),
        /mapping key/,
      );
    },
  },
  {
    name: "never normalizes whitespace into a different identity",
    run: () => {
      NodeAssert.throws(
        () => normalizeConversationExport([{ ...sampleConversation(), id: " chat-one" }]),
        /whitespace/,
      );
      NodeAssert.throws(
        () =>
          normalizeConversationExport([
            { ...sampleConversation(), mapping: { " a": { parent: null } } },
          ]),
        /whitespace/,
      );
    },
  },
  {
    name: "validates timestamps instead of substituting import time",
    run: () => {
      NodeAssert.throws(
        () => normalizeConversationExport([{ ...sampleConversation(), update_time: Infinity }]),
        /timestamp/,
      );
      NodeAssert.equal(
        normalizeConversationExport([{ ...sampleConversation(), update_time: null }])[0]!
          .sourceUpdatedAt,
        null,
      );
    },
  },
  {
    name: "bounds node count and text bytes before admission",
    run: () => {
      NodeAssert.throws(
        () =>
          normalizeConversationExport([
            {
              ...sampleConversation(),
              mapping: Object.fromEntries(
                Array.from({ length: 10_001 }, (_, n) => [String(n), { parent: null }]),
              ),
            },
          ]),
        /exceeds/,
      );
      const big = "x".repeat(8 * 1024 * 1024 + 1);
      NodeAssert.throws(
        () =>
          normalizeConversationExport([
            {
              ...sampleConversation(),
              mapping: {
                a: { message: { author: { role: "assistant" }, content: { parts: [big] } } },
              },
            },
          ]),
        /text budget/,
      );
    },
  },
  {
    name: "constructs only fixed-origin original-chat links",
    run: () => {
      NodeAssert.equal(libraryOriginalUrl("chat-one"), "https://chatgpt.com/c/chat-one");
      for (const id of ["../logout", "https://evil.invalid", "a?token=secret", "a#x", ""])
        NodeAssert.equal(libraryOriginalUrl(id), null);
    },
  },
  {
    name: "invalidates delayed replies when the selected context changes",
    run: () => {
      const fence = new LibraryRequestFence();
      const accountA = fence.next();
      const accountB = fence.next();
      NodeAssert.equal(fence.accepts(accountA), false);
      NodeAssert.equal(fence.accepts(accountB), true);
      fence.next();
      NodeAssert.equal(fence.accepts(accountB), false);
    },
  },
  {
    name: "requires mutation authority for every state-changing request",
    run: () => {
      NodeAssert.equal(
        libraryRequestMutates({ kind: "import", accountId: "a", conversations: [] }),
        true,
      );
      NodeAssert.equal(
        libraryRequestMutates({ kind: "createAccount", label: "a", workspace: "Personal" }),
        true,
      );
      NodeAssert.equal(libraryRequestMutates({ kind: "update", key: "a", pinned: true }), true);
      NodeAssert.equal(
        libraryRequestMutates({
          kind: "selectSnapshot",
          key: "a",
          snapshotId: "b",
          expectedRevision: 1,
        }),
        true,
      );
      NodeAssert.equal(
        libraryRequestMutates({ kind: "remove", key: "a", expectedRevision: 1 }),
        true,
      );
      for (const request of [
        { kind: "hello" },
        { kind: "accounts" },
        { kind: "list" },
        { kind: "detail", key: "a" },
        { kind: "preview", conversations: [] },
      ] as const)
        NodeAssert.equal(libraryRequestMutates(request), false);
    },
  },
  {
    name: "handles prototype-looking node IDs as ordinary map keys",
    run: () => {
      const mapping = Object.fromEntries([
        [
          "__proto__",
          { parent: null, message: { author: { role: "user" }, content: { parts: ["Safe"] } } },
        ],
      ]);
      const snapshot = normalizeConversationExport([
        { id: "prototype", title: "Prototype", current_node: "__proto__", mapping },
      ])[0]!;
      NodeAssert.equal(libraryBranch(snapshot.nodes, "__proto__")[0]!.text, "Safe");
    },
  },
];
