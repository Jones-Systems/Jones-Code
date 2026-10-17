// @vitest-environment jsdom

import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";

import { useSidebarListItems } from "./Sidebar";
import type { SidebarListItem } from "./Sidebar.logic";
import { makeThreadFixture } from "../test-fixtures";
import {
  classifyThreadStatus,
  selectThreadStatus,
  type ThreadStatusFilter,
} from "../jones/threadStatusFilter/model";

type Thread = ReturnType<typeof makeThreadFixture>;
const threadKey = (thread: Thread) =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));

let renderer: ReactTestRenderer | undefined;

beforeEach(() => vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true));
afterEach(async () => {
  if (renderer) await act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

function FilteredRows({
  threads,
  filter,
  onItems,
}: {
  readonly threads: readonly Thread[];
  readonly filter: ThreadStatusFilter;
  readonly onItems?: (items: readonly SidebarListItem[]) => void;
}) {
  const selected = selectThreadStatus(threads, filter, (thread) =>
    classifyThreadStatus(thread, "current"),
  );
  const byKey = new Map(selected.threads.map((thread) => [threadKey(thread), thread]));
  const fresh: SidebarListItem[] = selected.threads.map((thread) => ({
    kind: "thread",
    key: threadKey(thread),
    section: "active",
  }));
  const { items } = useSidebarListItems(fresh);
  onItems?.(items);
  return (
    <ul>
      {items.map((item) => {
        if (item.kind !== "thread") return null;
        // The row consumer must be able to resolve every key in this render,
        // including the render that schedules a cached-list state refresh.
        const thread = byKey.get(item.key)!;
        return (
          <li key={item.key} data-environment={thread.environmentId}>
            {thread.title}
          </li>
        );
      })}
    </ul>
  );
}

async function renderRows(props: Parameters<typeof FilteredRows>[0]) {
  await act(() => {
    if (renderer) renderer.update(<FilteredRows {...props} />);
    else renderer = create(<FilteredRows {...props} />);
  });
}

const titles = () => renderer!.root.findAllByType("li").map((row) => row.children.join(""));

describe("sidebar status-filter row snapshots", () => {
  it("hides a no-run thread in Working and restores it after cycling back to All", async () => {
    const thread = makeThreadFixture({ title: "Renamed thread", latestRun: null, runtime: null });
    await renderRows({ threads: [thread], filter: "all" });
    expect(titles()).toEqual(["Renamed thread"]);

    for (const filter of ["working", "input", "done"] as const) {
      await renderRows({ threads: [thread], filter });
      expect(titles()).toEqual([]);
    }
    await renderRows({ threads: [thread], filter: "all" });
    expect(titles()).toEqual(["Renamed thread"]);
  });

  it("keeps matching rows when a status filter removes only part of the roster", async () => {
    const idle = makeThreadFixture({ id: ThreadId.make("idle"), title: "Idle", runtime: null });
    const input = makeThreadFixture({
      id: ThreadId.make("input"),
      title: "Needs input",
      hasPendingUserInput: true,
    });
    await renderRows({ threads: [idle, input], filter: "all" });
    expect(titles()).toEqual(["Idle", "Needs input"]);
    await renderRows({ threads: [idle, input], filter: "input" });
    expect(titles()).toEqual(["Needs input"]);
    await renderRows({ threads: [idle, input], filter: "all" });
    expect(titles()).toEqual(["Idle", "Needs input"]);
  });

  it("retains list identity for shell updates while rendering current row data", async () => {
    const onItems = vi.fn();
    const thread = makeThreadFixture({ title: "Before" });
    await renderRows({ threads: [thread], filter: "all", onItems });
    const initialItems = onItems.mock.lastCall![0];
    await renderRows({ threads: [{ ...thread, title: "After" }], filter: "all", onItems });
    expect(titles()).toEqual(["After"]);
    expect(onItems.mock.lastCall![0]).toBe(initialItems);
  });
});
