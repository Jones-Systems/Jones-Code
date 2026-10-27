import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { isThreadWorking } from "@t3tools/client-runtime/state/thread-inbox";
import { effectiveSnoozed } from "@t3tools/client-runtime/state/thread-settled";
import { EnvironmentId, RunId, ThreadId } from "@t3tools/contracts";
import { threadKey } from "@t3tools/client-runtime/state/entities";
import { describe, expect, it } from "vite-plus/test";

import { useSidebarListItems } from "../../components/Sidebar";
import {
  filterSidebarV2VisibleThreads,
  hasUnseenCompletion,
  resolveSidebarThreadSection,
  type SidebarListItem,
} from "../../components/Sidebar.logic";
import { makeThreadFixture } from "../../test-fixtures";
import { render, type Screen } from "../../../test/render";

const now = "2026-10-27T12:00:00.000Z";
const before = "2026-10-27T11:00:00.000Z";
type Thread = ReturnType<typeof makeThreadFixture>;
const keyOf = (thread: Thread) => scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
function delegate(overrides: Partial<Thread> = {}): Thread {
  return makeThreadFixture({
    id: ThreadId.make("delegate"),
    title: "Delegate asking the owner",
    delegatedFrom: {
      environmentId: EnvironmentId.make("origin"),
      threadId: ThreadId.make("settled-parent"),
      title: "Settled parent",
    },
    settledOverride: null,
    pinnedAt: null,
    snoozedAt: null,
    snoozedUntil: null,
    hasPendingApprovals: false,
    hasPendingUserInput: true,
    ...overrides,
  });
}

// Exercise the exported visibility/section policies and the real cached-list hook.
// Production's partition remains private; no upstream export is needed for this contract.
function Rows({
  thread,
  delegatedUnderParent = new Set<string>(),
}: {
  readonly thread: Thread;
  readonly delegatedUnderParent?: ReadonlySet<string> | undefined;
}) {
  const fresh = filterSidebarV2VisibleThreads([thread], null, delegatedUnderParent).map(
    (row): SidebarListItem => {
      const section = resolveSidebarThreadSection({
        snoozed: effectiveSnoozed(row, { now }),
        settled: row.settledOverride === "settled",
        pinned: row.pinnedAt != null,
      });
      return {
        kind: "thread",
        key: keyOf(row),
        section: section === "active" && isThreadWorking(row) ? "working" : section,
      };
    },
  );
  const { items } = useSidebarListItems(fresh);
  return (
    <ul>
      {items.map((item) =>
        item.kind === "thread" ? (
          <li key={item.key} data-section={item.section}>
            {item.key}
          </li>
        ) : null,
      )}
    </ul>
  );
}

const hiddenUnderParent = (thread: Thread) =>
  new Set([threadKey({ environmentId: thread.environmentId, threadId: thread.id })]);
const sections = (screen: Screen) =>
  [...screen.container.querySelectorAll("li")].map((row) => row.getAttribute("data-section"));

describe("revealed delegates in the sidebar", () => {
  it.each(["input", "unread approval"] as const)(
    "lists actionable %s in Active and removes it when re-hidden",
    async (kind) => {
      const thread = delegate(
        kind === "input"
          ? {}
          : {
              hasPendingUserInput: false,
              hasPendingApprovals: true,
              latestRun: {
                runId: RunId.make("completed-run"),
                status: "completed",
                requestedAt: before,
                startedAt: before,
                completedAt: now,
                assistantMessageId: null,
              },
              lastVisitedAt: before,
            },
      );
      if (kind === "unread approval") expect(hasUnseenCompletion(thread)).toBe(true);
      await using screen = await render(
        <Rows thread={thread} delegatedUnderParent={hiddenUnderParent(thread)} />,
        { router: false },
      );
      expect(sections(screen)).toEqual([]);
      await screen.rerender(<Rows thread={thread} />);
      expect(sections(screen)).toEqual(["active"]);
      await screen.rerender(
        <Rows thread={thread} delegatedUnderParent={hiddenUnderParent(thread)} />,
      );
      expect(sections(screen)).toEqual([]);
    },
  );

  it("keeps a revealed settled delegate in Settled", async () => {
    await using screen = await render(
      <Rows thread={delegate({ settledOverride: "settled", settledAt: before })} />,
      { router: false },
    );
    expect(sections(screen)).toEqual(["settled"]);
  });

  it("wakes a snoozed delegate when it asks", async () => {
    const thread = delegate({ snoozedAt: before, snoozedUntil: "2026-10-28T12:00:00.000Z" });
    expect(effectiveSnoozed({ ...thread, hasPendingUserInput: false }, { now })).toBe(true);
    await using screen = await render(<Rows thread={thread} />, { router: false });
    expect(sections(screen)).toEqual(["active"]);
  });

  it("keeps an already-read approval actionable in Active", async () => {
    const thread = delegate({
      hasPendingUserInput: false,
      hasPendingApprovals: true,
      lastVisitedAt: now,
    });
    expect(isThreadWorking(thread)).toBe(false);
    await using screen = await render(<Rows thread={thread} />, { router: false });
    expect(sections(screen)).toEqual(["active"]);
  });
});
