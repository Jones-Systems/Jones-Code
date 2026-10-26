import * as DateTime from "effect/DateTime";
import {
  __setClientSettingsForTests,
  __resetClientSettingsPersistenceForTests,
  persistClientSettingsPatch,
} from "../../hooks/useSettings";
import { act, fakeTimers, render as renderTest, type Screen } from "../../../test/render";

import { describe, expect, it, vi } from "vite-plus/test";
import { DEFAULT_CLIENT_SETTINGS, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { makeThreadFixture } from "../../test-fixtures";
import { useUiStateStore } from "../../uiStateStore";
import type { SidebarThreadSummary } from "../../types";
import { useThreadStatusSelection } from "./useThreadStatusFilter";
import type { ThreadStatusFilter } from "./model";

const completed = "2026-10-17T12:00:00.000Z";
const visited = "2026-10-17T11:00:00.000Z";
const thread = makeThreadFixture({
  id: ThreadId.make("selected"),
  environmentId: EnvironmentId.make("first"),
  lastVisitedAt: visited,
  latestRun: {
    runId: "run" as never,
    status: "completed",
    assistantMessageId: null,
    requestedAt: visited,
    startedAt: visited,
    completedAt: completed,
  },
  runtime: null,
});
const key = (row: SidebarThreadSummary) =>
  scopedThreadKey(scopeThreadRef(row.environmentId, row.id));

type Input = {
  threads: SidebarThreadSummary[];
  layout: "current" | "legacy";
  filter: ThreadStatusFilter;
  selected: string | null;
};
function Selection(props: Input) {
  const selection = useThreadStatusSelection(
    props.threads,
    props.filter,
    props.layout,
    undefined,
    props.selected,
  );
  const secondPass = useThreadStatusSelection(
    props.threads,
    props.filter,
    props.layout,
    undefined,
    props.selected,
  );
  return (
    <>
      <output>{JSON.stringify(selection.counts)}</output>
      <ul>
        {selection.threads.map((row) => (
          <li key={key(row)}>{key(row)}</li>
        ))}
      </ul>
      <ol>
        {secondPass.threads.map((row) => (
          <li key={key(row)}>{key(row)}</li>
        ))}
      </ol>
    </>
  );
}
let root: Screen;
let container: HTMLDivElement;
let input: Input;
const render = async (updates: Partial<Input> = {}) => {
  input = { ...input, ...updates };
  return await act(async () => await root.rerender(<Selection {...input} />));
};
const visible = () => [...container.querySelectorAll("ul li")].map((row) => row.textContent);
async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
      await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        vi.restoreAllMocks();
        __resetClientSettingsPersistenceForTests();
        vi.unstubAllGlobals();
      }
    }
  }
  try {
    __setClientSettingsForTests(DEFAULT_CLIENT_SETTINGS);
    useUiStateStore.setState({ threadLastVisitedAtById: {} });
    container = document.createElement("div");
    root = await renderTest(null, { container: container, router: false });
    initialized = true;
    input = { threads: [thread], layout: "current", filter: "done", selected: key(thread) };

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

describe.each(["current", "legacy"] as const)("%s active status-filter retention", (layout) => {
  it("counts and positively filters only roots while keeping children reachable in All", async () => {
    await using _fixture = await setupTest();
    const working: SidebarThreadSummary = {
      ...thread,
      id: ThreadId.make("working"),
      pendingBackgroundTasks: [{ taskId: "monitor", kind: "monitor" }],
    };
    const inputThread = { ...thread, id: ThreadId.make("input"), hasPendingUserInput: true };
    const roots = [working, inputThread, thread];
    const children = roots.flatMap((root) => [
      ...(["subagent", "fork"] as const).map((relationshipToParent) => ({
        ...root,
        id: ThreadId.make(`${root.id}-${relationshipToParent}`),
        lineage: { rootThreadId: root.id, parentThreadId: root.id, relationshipToParent },
      })),
      {
        ...root,
        id: ThreadId.make(`${root.id}-delegated`),
        delegatedFrom: {
          environmentId: EnvironmentId.make("unlisted"),
          threadId: root.id,
          title: "Parent",
        },
      },
    ]);
    const roster = [...roots, ...children];
    await render({ layout, threads: roster, filter: "all", selected: null });
    const counts = () => container.querySelector("output")?.textContent;
    expect(counts()).toBe('{"working":1,"input":1,"done":1}');
    expect(visible()).toEqual(roster.map(key));
    for (const [filter, root] of [
      ["working", working],
      ["input", inputThread],
      ["done", thread],
    ] as const) {
      await render({ filter });
      expect(visible()).toEqual([key(root)]);
      expect(counts()).toBe('{"working":1,"input":1,"done":1}');
    }
    await render({ filter: "hide-working" });
    expect(visible()).toEqual(
      roster.filter((row) => !row.pendingBackgroundTasks?.length).map(key),
    );
    await render({ filter: "done", threads: children, selected: key(children[0]!) });
    expect(counts()).toBe('{"working":0,"input":0,"done":0}');
    expect(visible()).toEqual([]);
  });
  it("drops retained status rows when they become children", async () => {
    await using _fixture = await setupTest();
    await render({ layout });
    expect(visible()).toEqual([key(thread)]);
    await render({
      threads: [
        {
          ...thread,
          lineage: {
            rootThreadId: ThreadId.make("parent"),
            parentThreadId: ThreadId.make("parent"),
            relationshipToParent: "fork",
          },
        },
      ],
    });
    expect(visible()).toEqual([]);
    expect(container.querySelector("output")?.textContent).toBe('{"working":0,"input":0,"done":0}');
  });
  it("retains a selected completion after its read watermark changes, with actual counts", async () => {
    await using _fixture = await setupTest();
    await render({ layout });
    expect(visible()).toEqual([key(thread)]);
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([key(thread)]);
    expect(container.querySelector("ol li")?.textContent).toBe(key(thread));
    expect(container.querySelector("output")?.textContent).toBe('{"working":0,"input":0,"done":0}');
    await render({ selected: null });
    expect(visible()).toEqual([]);
  });
  it("retains Needs Input through a response, and resets on filter change", async () => {
    await using _fixture = await setupTest();
    await render({ layout, filter: "input", threads: [{ ...thread, hasPendingUserInput: true }] });
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([key(thread)]);
    await render({ filter: "all" });
    await render({ filter: "input" });
    expect(visible()).toEqual([]);
  });
  it("never retains a nonqualifying selection or the same ID in a different environment", async () => {
    await using _fixture = await setupTest();
    await render({ layout, threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([]);
    await render({ threads: [thread] });
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([key(thread)]);
    await render({ selected: key({ ...thread, environmentId: EnvironmentId.make("other") }) });
    expect(visible()).toEqual([]);
  });
  it("drops a removed scoped row and does not revive its old retention when it returns", async () => {
    await using _fixture = await setupTest();
    await render({ layout });
    await render({ threads: [] });
    expect(visible()).toEqual([]);
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([]);
  });
  it("preserves a completion read through the local compatibility watermark", async () => {
    await using _fixture = await setupTest();
    const legacyThread = { ...thread };
    delete legacyThread.lastVisitedAt;
    await render({ layout, threads: [legacyThread] });
    await act(async () =>
      useUiStateStore.setState({ threadLastVisitedAtById: { [key(thread)]: visited } }),
    );
    expect(visible()).toEqual([key(thread)]);
    await act(async () =>
      useUiStateStore.setState({ threadLastVisitedAtById: { [key(thread)]: completed } }),
    );
    expect(visible()).toEqual([key(thread)]);
  });
});

describe.each(["current", "legacy"] as const)("%s activity window selection", (layout) => {
  it("updates settings, includes read Done, preserves All and Hide working, then expires the active row", async () => {
    await using _fixture = await setupTest();
    using _timers = fakeTimers();
    vi.setSystemTime(new Date("2026-10-17T15:00:00.000Z"));
    const read = {
      ...thread,
      lastVisitedAt: completed,
      source: { ...thread.source, latestRunCompletedAt: DateTime.makeUnsafe(completed) },
    };
    const staleWorking = {
      ...thread,
      id: ThreadId.make("stale-working"),
      runtime: {
        status: "running" as const,
        providerName: "Codex",
        providerInstanceId: thread.providerInstanceId,
        activeRunId: null,
        lastError: null,
        updatedAt: completed,
      },
    };
    await render({ layout, threads: [read, staleWorking] });
    expect(visible()).toEqual([]);
    await act(async () => {
      await persistClientSettingsPatch({ threadActivityWindowEnabled: true }, async () => {});
    });
    expect(visible()).toEqual([key(thread)]);
    expect(container.querySelector("output")?.textContent).toBe('{"working":0,"input":0,"done":1}');
    await render({ filter: "all" });
    expect(visible()).toEqual([key(thread), key(staleWorking)]);
    await render({ filter: "hide-working" });
    expect(visible()).toEqual([key(thread)]);
    expect(container.querySelector("output")?.textContent).toBe('{"working":0,"input":0,"done":1}');
    await render({ filter: "done" });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });
    expect(visible()).toEqual([]);
    await act(async () => {
      await persistClientSettingsPatch({ threadActivityWindowAmount: 4 }, async () => {});
    });
    expect(visible()).toEqual([key(thread)]);
    await act(async () => {
      await persistClientSettingsPatch({ threadActivityWindowAmount: 1 }, async () => {});
    });
    expect(visible()).toEqual([]);
  });
  it("drops acknowledged Input retention at expiry and when the window changes", async () => {
    await using _fixture = await setupTest();
    using _timers = fakeTimers();
    vi.setSystemTime(new Date("2026-10-17T14:00:00.000Z"));
    await act(async () => {
      await persistClientSettingsPatch({ threadActivityWindowEnabled: true }, async () => {});
    });
    await render({ layout, filter: "input", threads: [{ ...thread, hasPendingUserInput: true }] });
    expect(visible()).toEqual([key(thread)]);
    await render({ threads: [thread] });
    expect(visible()).toEqual([key(thread)]);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });
    expect(visible()).toEqual([]);
    await render({
      threads: [
        { ...thread, hasPendingUserInput: true, latestUserMessageAt: "2026-10-17T14:01:00.000Z" },
      ],
    });
    expect(visible()).toEqual([key(thread)]);
    await render({ threads: [{ ...thread, latestUserMessageAt: "2026-10-17T14:01:00.000Z" }] });
    expect(visible()).toEqual([key(thread)]);
    await act(async () => {
      await persistClientSettingsPatch({ threadActivityWindowUnit: "days" }, async () => {});
    });
    expect(visible()).toEqual([]);
  });
});
