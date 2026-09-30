import { assert } from "vite-plus/test";
import type {
  LibraryDetail,
  LibraryReply,
  LibrarySummary,
} from "@t3tools/contracts/conversationLibrary";
import { ConversationLibraryReader, libraryDetailPage, libraryReaderRowKey } from "./model.ts";

const binding = { environmentId: "environment-a", generation: 1 };
const hello = (revision = 1, canWrite = true): Extract<LibraryReply, { kind: "hello" }> => ({
  kind: "hello",
  protocol: "t3.conversation-library.v1",
  revision,
  canWrite,
  capture: "not-enabled",
});
const row = (patch: Partial<LibrarySummary> = {}): LibrarySummary => ({
  key: "account-a:conversation-1",
  accountId: "account-a",
  conversationId: "conversation-1",
  title: "Supplied export",
  snapshotId: "snapshot-1",
  sourceUpdatedAt: 1000,
  importedAt: 2000,
  revision: 1,
  unread: true,
  pinned: false,
  archived: false,
  attention: false,
  conflicts: false,
  messageCount: 1,
  warningCount: 1,
  ...patch,
});
const detail = (patch: Partial<LibraryDetail> = {}): LibraryDetail => ({
  kind: "detail",
  conversation: row(),
  account: { id: "account-a", label: "Personal", workspace: "Personal" },
  snapshotId: "snapshot-1",
  snapshotSourceUpdatedAt: 1000,
  snapshotImportedAt: 2000,
  showHidden: false,
  nodeId: "node-1",
  messages: [
    {
      id: "node-1",
      parentId: "missing-parent",
      messageId: "message-1",
      role: "assistant",
      text: "<script>not executable</script>",
      createdAt: 1000,
      hidden: false,
      unsupportedParts: 1,
    },
  ],
  totalMessages: 1,
  readThrough: 1,
  offset: 0,
  previousOffset: null,
  nextOffset: null,
  snapshots: [{ id: "snapshot-1", sourceUpdatedAt: 1000, importedAt: 2000, messageCount: 1 }],
  snapshotOffset: 0,
  snapshotCount: 1,
  branches: [{ id: "node-1", preview: "Supplied text" }],
  branchOffset: 0,
  branchCount: 1,
  warnings: ["This branch has a history gap; non-text media is unavailable."],
  ...patch,
});
const page = (
  rows: readonly LibrarySummary[] = [row()],
  revision = 1,
  cursor: string | null = null,
): Extract<LibraryReply, { kind: "list" }> => ({ kind: "list", rows, revision, cursor });

function present(reader: ConversationLibraryReader, value = detail()): LibraryDetail {
  const ticket = reader.requestDetail({
    kind: "detail",
    key: value.conversation.key,
    snapshotId: value.snapshotId,
  });
  assert.ok(ticket);
  assert.equal(reader.acceptDetail(ticket, value), true);
  return value;
}

function ready(canWrite = true): ConversationLibraryReader {
  const reader = new ConversationLibraryReader();
  reader.bind(binding);
  const ticket = reader.requestHello();
  assert.ok(ticket);
  assert.equal(reader.acceptHello(ticket, hello(1, canWrite)), true);
  return reader;
}

export const conversationLibraryReaderCases: readonly {
  readonly name: string;
  readonly run: () => void;
}[] = [
  {
    name: "disconnected readers cannot issue requests or grant themselves writes",
    run: () => {
      const reader = new ConversationLibraryReader();
      assert.equal(reader.requestHello(), null);
      assert.equal(reader.requestList(), null);
      assert.equal(reader.requestDetail({ kind: "detail", key: "x" }), null);
      assert.equal(reader.requestFlags({ pinned: true }), null);
    },
  },
  {
    name: "connection generation changes clear records and capabilities",
    run: () => {
      const reader = ready();
      present(reader);
      reader.bind({ ...binding, generation: 2 });
      assert.equal(reader.getSnapshot().detail.value, null);
      assert.equal(reader.getSnapshot().canWrite, false);
    },
  },
  {
    name: "environment changes reject old hello capabilities",
    run: () => {
      const reader = ready();
      const old = reader.requestHello();
      assert.ok(old);
      reader.bind({ environmentId: "environment-b", generation: 1 });
      assert.equal(reader.acceptHello(old, hello(10)), false);
      assert.equal(reader.getSnapshot().revision, 0);
      assert.equal(reader.getSnapshot().canWrite, false);
    },
  },
  {
    name: "same environment and generation is a stable subscription snapshot",
    run: () => {
      const reader = ready();
      const before = reader.getSnapshot();
      reader.bind({ ...binding });
      assert.equal(reader.getSnapshot(), before);
    },
  },
  {
    name: "unsubscribed listeners receive no further notifications",
    run: () => {
      const reader = ready();
      let calls = 0;
      const unsubscribe = reader.subscribe(() => {
        calls++;
      });
      present(reader);
      assert.equal(calls, 2);
      unsubscribe();
      reader.bind(null);
      assert.equal(calls, 2);
    },
  },
  {
    name: "account changes clear selection and fence in-flight detail",
    run: () => {
      const reader = ready();
      const old = reader.requestDetail({ kind: "detail", key: row().key });
      assert.ok(old);
      reader.setFilter({ accountId: "account-b", query: "", view: "all" });
      assert.equal(reader.acceptDetail(old, detail()), false);
      assert.equal(reader.getSnapshot().detail.value, null);
    },
  },
  {
    name: "latest selection wins when detail replies arrive out of order",
    run: () => {
      const reader = ready();
      const old = reader.requestDetail({ kind: "detail", key: "old" });
      assert.ok(old);
      const current = present(reader);
      assert.equal(reader.acceptDetail(old, detail({ conversation: row({ key: "old" }) })), false);
      assert.equal(reader.getSnapshot().detail.value, current);
    },
  },
  {
    name: "a different account reply is rejected even when the key matches",
    run: () => {
      const reader = ready();
      reader.setFilter({ accountId: "account-b", query: "", view: "all" });
      const ticket = reader.requestDetail({ kind: "detail", key: row().key });
      assert.ok(ticket);
      assert.equal(reader.acceptDetail(ticket, detail()), false);
      assert.equal(reader.getSnapshot().detail.status, "error");
    },
  },
  {
    name: "summary and account identities must agree",
    run: () => {
      const reader = ready();
      const ticket = reader.requestDetail({ kind: "detail", key: row().key });
      assert.ok(ticket);
      assert.equal(
        reader.acceptDetail(
          ticket,
          detail({ account: { id: "wrong", label: "Personal", workspace: "Personal" } }),
        ),
        false,
      );
    },
  },
  {
    name: "explicit snapshots never silently fall back to the default",
    run: () => {
      const reader = ready();
      const ticket = reader.requestDetail({ kind: "detail", key: row().key, snapshotId: "older" });
      assert.ok(ticket);
      assert.equal(reader.acceptDetail(ticket, detail()), false);
    },
  },
  {
    name: "explicit branches and page windows are verified",
    run: () => {
      for (const request of [
        { nodeId: "other" },
        { offset: 50 },
        { snapshotOffset: 50 },
        { branchOffset: 50 },
        { showHidden: true },
      ]) {
        const reader = ready();
        const ticket = reader.requestDetail({ kind: "detail", key: row().key, ...request });
        assert.ok(ticket);
        assert.equal(reader.acceptDetail(ticket, detail()), false);
      }
    },
  },
  {
    name: "search and view changes fence prior list replies",
    run: () => {
      const reader = ready();
      const old = reader.requestList();
      assert.ok(old);
      reader.setFilter({ accountId: null, query: " supplied ", view: "attention" });
      assert.equal(reader.acceptList(old, page()), false);
      assert.deepEqual(reader.requestList()?.request, {
        kind: "list",
        query: "supplied",
        view: "attention",
      });
    },
  },
  {
    name: "account filters use the binding ID rather than display labels",
    run: () => {
      const reader = ready();
      reader.setFilter({ accountId: "workspace-specific-account-id", query: "", view: "pinned" });
      assert.equal(reader.requestList()?.request.accountId, "workspace-specific-account-id");
    },
  },
  {
    name: "same-revision catalog pages append without losing order",
    run: () => {
      const reader = ready();
      const first = reader.requestList();
      assert.ok(first);
      assert.equal(reader.acceptList(first, page([row()], 1, "cursor")), true);
      const next = reader.requestList(true);
      assert.ok(next);
      assert.equal(next.request.cursor, "cursor");
      assert.equal(reader.acceptList(next, page([row({ key: "second" })])), true);
      assert.deepEqual(
        reader.getSnapshot().list.value?.rows.map((item) => item.key),
        [row().key, "second"],
      );
    },
  },
  {
    name: "catalog revision changes never splice incompatible pages",
    run: () => {
      const reader = ready();
      const first = reader.requestList();
      assert.ok(first);
      reader.acceptList(first, page([row()], 1, "cursor"));
      const next = reader.requestList(true);
      assert.ok(next);
      assert.equal(reader.acceptList(next, page([row({ key: "second" })], 2)), false);
      assert.equal(reader.getSnapshot().list.status, "error");
      assert.equal(reader.requestList(true), null);
      assert.equal(reader.getSnapshot().list.value?.rows.length, 1);
    },
  },
  {
    name: "stale first pages cannot overwrite a newer observed revision",
    run: () => {
      const reader = ready();
      reader.observeRevision(3);
      const ticket = reader.requestList();
      assert.ok(ticket);
      assert.equal(reader.acceptList(ticket, page([row()], 2)), false);
      assert.equal(reader.getSnapshot().revision, 3);
    },
  },
  {
    name: "duplicate and cross-account catalog rows fail closed",
    run: () => {
      for (const rows of [[row(), row()], [row({ accountId: "account-b" })]]) {
        const reader = ready();
        reader.setFilter({ accountId: "account-a", query: "", view: "all" });
        const ticket = reader.requestList();
        assert.ok(ticket);
        assert.equal(reader.acceptList(ticket, page(rows)), false);
      }
    },
  },
  {
    name: "observing a newer revision invalidates pending reads and displayed receipts",
    run: () => {
      const reader = ready();
      const value = present(reader);
      reader.acknowledgeDisplayed(value);
      const pending = reader.requestList();
      assert.ok(pending);
      reader.observeRevision(2);
      assert.equal(reader.acceptList(pending, page()), false);
      assert.equal(reader.getSnapshot().detail.status, "stale");
      assert.equal(reader.getSnapshot().displayed, false);
      assert.equal(reader.requestMarkRead(), null);
    },
  },
  {
    name: "network receipt alone never marks a conversation read",
    run: () => {
      const reader = ready();
      present(reader);
      assert.equal(reader.requestMarkRead(), null);
      assert.equal(reader.getSnapshot().detail.value?.conversation.unread, true);
    },
  },
  {
    name: "only the exact displayed current page can produce a read acknowledgement",
    run: () => {
      const reader = ready();
      const value = present(reader);
      assert.equal(reader.acknowledgeDisplayed({ ...value }), false);
      assert.equal(reader.acknowledgeDisplayed(value), true);
      assert.deepEqual(reader.requestMarkRead()?.request, {
        kind: "update",
        key: row().key,
        readThrough: 1,
      });
    },
  },
  {
    name: "historical snapshots, alternate branches, gaps without selection, and early pages stay unread",
    run: () => {
      const variants: readonly Partial<LibraryDetail>[] = [
        { snapshotId: "older", readThrough: 0 },
        { nodeId: "alternate", readThrough: 0 },
        { nodeId: null, messages: [], totalMessages: 0, readThrough: 0 },
        { nextOffset: 50, totalMessages: 100 },
        { nextOffset: null, totalMessages: 100 },
        { conversation: row({ conflicts: true }) },
        { conversation: row({ unread: false }) },
        { readThrough: 2 },
      ];
      for (const variant of variants) {
        const reader = ready();
        const value = present(reader, detail(variant));
        reader.acknowledgeDisplayed(value);
        assert.equal(reader.requestMarkRead(), null);
      }
    },
  },
  {
    name: "read-only clients cannot alter flags, select defaults, remove, or acknowledge reads",
    run: () => {
      const reader = ready(false);
      const value = present(reader);
      reader.acknowledgeDisplayed(value);
      assert.equal(reader.requestFlags({ pinned: true }), null);
      assert.equal(reader.requestSelectSnapshot("older"), null);
      assert.equal(reader.requestRemoveLocalCopy(), null);
      assert.equal(reader.requestMarkRead(), null);
    },
  },
  {
    name: "flag changes carry only explicit flag fields and never smuggle a read marker",
    run: () => {
      const reader = ready();
      present(reader);
      const flags = { pinned: true, readThrough: 999, key: "different" };
      assert.deepEqual(reader.requestFlags(flags)?.request, {
        kind: "update",
        key: row().key,
        pinned: true,
      });
      assert.equal(reader.getSnapshot().detail.value?.conversation.pinned, false);
    },
  },
  {
    name: "writes are single-flight and never applied optimistically",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestFlags({ attention: true });
      assert.ok(ticket);
      assert.equal(reader.requestFlags({ archived: true }), null);
      assert.equal(reader.getSnapshot().detail.value?.conversation.attention, false);
      assert.equal(reader.acceptMutation(ticket, { kind: "updated", revision: 2 }), true);
      assert.equal(reader.getSnapshot().mutationPending, false);
      assert.equal(reader.getSnapshot().detail.status, "stale");
      assert.equal(reader.requestFlags({ archived: true }), null);
    },
  },
  {
    name: "snapshot selection and local removal carry the observed content revision",
    run: () => {
      const select = ready();
      present(select);
      assert.deepEqual(select.requestSelectSnapshot("older")?.request, {
        kind: "selectSnapshot",
        key: row().key,
        snapshotId: "older",
        expectedRevision: 1,
      });
      const remove = ready();
      present(remove);
      assert.deepEqual(remove.requestRemoveLocalCopy()?.request, {
        kind: "remove",
        key: row().key,
        expectedRevision: 1,
      });
    },
  },
  {
    name: "local removal clears only the record whose removal was acknowledged",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestRemoveLocalCopy();
      assert.ok(ticket);
      const other = present(reader, detail({ conversation: row({ key: "other" }) }));
      assert.equal(reader.acceptMutation(ticket, { kind: "removed", revision: 2 }), true);
      assert.equal(reader.getSnapshot().detail.value, other);
      assert.equal(reader.getSnapshot().detail.status, "stale");
    },
  },
  {
    name: "removing the selected local copy clears its displayed state",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestRemoveLocalCopy();
      assert.ok(ticket);
      reader.acceptMutation(ticket, { kind: "removed", revision: 2 });
      assert.equal(reader.getSnapshot().detail.value, null);
    },
  },
  {
    name: "a mutation from the old connection cannot alter the new connection",
    run: () => {
      const reader = ready();
      present(reader);
      const old = reader.requestFlags({ pinned: true });
      assert.ok(old);
      reader.bind({ ...binding, generation: 2 });
      assert.equal(reader.acceptMutation(old, { kind: "updated", revision: 5 }), false);
      assert.equal(reader.getSnapshot().revision, 0);
    },
  },
  {
    name: "wrong mutation reply kinds invalidate the view instead of inventing success",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestRemoveLocalCopy();
      assert.ok(ticket);
      assert.equal(reader.acceptMutation(ticket, { kind: "updated", revision: 2 }), false);
      assert.equal(reader.getSnapshot().detail.status, "stale");
      assert.match(reader.getSnapshot().error ?? "", /reconcile/);
    },
  },
  {
    name: "unknown write outcomes require refresh and are never retried by the model",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestFlags({ pinned: true });
      assert.ok(ticket);
      assert.equal(reader.reject(ticket, "Connection lost; outcome unknown."), true);
      assert.equal(reader.getSnapshot().mutationPending, false);
      assert.equal(reader.getSnapshot().detail.status, "stale");
      assert.equal(reader.requestFlags({ pinned: true }), null);
    },
  },
  {
    name: "a failed obsolete request cannot replace the current error or page",
    run: () => {
      const reader = ready();
      const old = reader.requestDetail({ kind: "detail", key: "old" });
      assert.ok(old);
      const value = present(reader);
      assert.equal(reader.reject(old, "obsolete failure"), false);
      assert.equal(reader.getSnapshot().detail.value, value);
    },
  },
  {
    name: "capability refreshes can revoke write affordances without deleting reading data",
    run: () => {
      const reader = ready();
      const value = present(reader);
      const ticket = reader.requestHello();
      assert.ok(ticket);
      reader.acceptHello(ticket, hello(1, false));
      assert.equal(reader.getSnapshot().detail.value, value);
      assert.equal(reader.requestFlags({ pinned: true }), null);
    },
  },
  {
    name: "history gaps, unsupported media markers, roles, and branch order remain untouched",
    run: () => {
      const reader = ready();
      const value = present(reader);
      assert.equal(reader.getSnapshot().detail.value, value);
      assert.equal(reader.getSnapshot().detail.value?.messages[0]?.unsupportedParts, 1);
      assert.equal(reader.getSnapshot().detail.value?.warnings, value.warnings);
      assert.equal(
        reader.getSnapshot().detail.value?.messages[0]?.text,
        "<script>not executable</script>",
      );
    },
  },
  {
    name: "message, snapshot, and branch paging remain anchored to the visible snapshot",
    run: () => {
      const value = detail();
      assert.deepEqual(libraryDetailPage(value, { offset: 50, branchOffset: 50 }), {
        kind: "detail",
        key: row().key,
        snapshotId: "snapshot-1",
        nodeId: "node-1",
        offset: 50,
        snapshotOffset: 0,
        branchOffset: 50,
        showHidden: false,
      });
      const wider = { offset: 50, key: "wrong", snapshotId: "wrong" };
      assert.equal(libraryDetailPage(value, wider).key, row().key);
      assert.equal(libraryDetailPage(value, wider).snapshotId, "snapshot-1");
    },
  },
  {
    name: "render keys distinguish account/workspace bindings and connection generations",
    run: () => {
      const a = libraryReaderRowKey(binding, row());
      assert.notEqual(
        a,
        libraryReaderRowKey(binding, row({ accountId: "same-label-other-workspace" })),
      );
      assert.notEqual(a, libraryReaderRowKey({ ...binding, generation: 2 }, row()));
      assert.notEqual(
        a,
        libraryReaderRowKey({ ...binding, environmentId: "environment-b" }, row()),
      );
    },
  },
  {
    name: "capability replies older than observed data cannot restore write affordances",
    run: () => {
      const reader = ready();
      reader.observeRevision(2);
      const ticket = reader.requestHello();
      assert.ok(ticket);
      assert.equal(reader.acceptHello(ticket, hello(1, true)), false);
      assert.equal(reader.getSnapshot().canWrite, false);
    },
  },
  {
    name: "late successful mutation acknowledgements never regress a newer observed revision",
    run: () => {
      const reader = ready();
      present(reader);
      const ticket = reader.requestFlags({ pinned: true });
      assert.ok(ticket);
      reader.observeRevision(5);
      assert.equal(reader.acceptMutation(ticket, { kind: "updated", revision: 2 }), true);
      assert.equal(reader.getSnapshot().revision, 5);
      assert.equal(reader.getSnapshot().mutationPending, false);
      assert.equal(reader.getSnapshot().error, null);
    },
  },
  {
    name: "clearing selection fences its pending detail without changing the account filter",
    run: () => {
      const reader = ready();
      reader.setFilter({ accountId: "account-a", query: "", view: "all" });
      const ticket = reader.requestDetail({ kind: "detail", key: row().key });
      assert.ok(ticket);
      reader.clearSelection();
      assert.equal(reader.acceptDetail(ticket, detail()), false);
      assert.equal(reader.getSnapshot().selection, null);
      assert.equal(reader.getSnapshot().filter.accountId, "account-a");
    },
  },
  {
    name: "a failed detail keeps an explicit retry location instead of reusing another selection",
    run: () => {
      const reader = ready();
      const ticket = reader.requestDetail({ kind: "detail", key: row().key, snapshotId: "older" });
      assert.ok(ticket);
      reader.reject(ticket, "Temporary failure");
      assert.deepEqual(reader.getSnapshot().selection, ticket.request);
      assert.equal(reader.getSnapshot().detail.value, null);
    },
  },
  {
    name: "successful detail receipts anchor future refreshes to the displayed snapshot and branch",
    run: () => {
      const reader = ready();
      const value = present(reader);
      assert.deepEqual(reader.getSnapshot().selection, libraryDetailPage(value, {}));
    },
  },
  {
    name: "tickets cannot be replayed or replaced by a lookalike object",
    run: () => {
      const reader = ready();
      const ticket = reader.requestList();
      assert.ok(ticket);
      assert.equal(reader.acceptList({ ...ticket }, page()), false);
      assert.equal(reader.acceptList(ticket, page()), true);
      assert.equal(reader.acceptList(ticket, page([], 2)), false);
      assert.equal(reader.getSnapshot().list.value?.rows.length, 1);
    },
  },
  {
    name: "reply acceptance is atomic when a subscriber changes the active environment",
    run: () => {
      for (const lane of ["hello", "list", "detail"] as const) {
        const reader = ready();
        reader.subscribe(() => {
          const state = reader.getSnapshot();
          if (state.binding?.environmentId === binding.environmentId && state.revision === 2) {
            reader.bind({ environmentId: "environment-b", generation: 1 });
          }
        });
        if (lane === "hello") {
          const ticket = reader.requestHello();
          assert.ok(ticket);
          reader.acceptHello(ticket, hello(2));
        } else if (lane === "list") {
          const ticket = reader.requestList();
          assert.ok(ticket);
          reader.acceptList(ticket, page([row()], 2));
        } else {
          present(reader, detail({ conversation: row({ revision: 2 }), readThrough: 2 }));
        }
        assert.equal(reader.getSnapshot().binding?.environmentId, "environment-b");
        assert.equal(reader.getSnapshot().revision, 0);
        assert.equal(reader.getSnapshot().canWrite, false);
        assert.equal(reader.getSnapshot().list.value, null);
        assert.equal(reader.getSnapshot().detail.value, null);
      }
    },
  },
  {
    name: "a rebind during a request notification prevents dispatch of the obsolete ticket",
    run: () => {
      const reader = ready();
      reader.subscribe(() => {
        if (reader.getSnapshot().list.status === "loading") reader.bind(null);
      });
      const ticket = reader.requestList();
      assert.ok(ticket);
      assert.equal(reader.isPending(ticket), false);
    },
  },
  {
    name: "only the currently pending exact ticket is dispatchable",
    run: () => {
      const reader = ready();
      const old = reader.requestList();
      assert.ok(old);
      assert.equal(reader.isPending(old), true);
      const next = reader.requestList();
      assert.ok(next);
      assert.equal(reader.isPending(old), false);
      assert.equal(reader.isPending(next), true);
      reader.acceptList(next, page());
      assert.equal(reader.isPending(next), false);
    },
  },
];
