import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
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
let root: Root;
let container: HTMLDivElement;
let input: Input;
const render = (updates: Partial<Input> = {}) => {
  input = { ...input, ...updates };
  return act(async () => root.render(<Selection {...input} />));
};
const visible = () => [...container.querySelectorAll("ul li")].map((row) => row.textContent);
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  useUiStateStore.setState({ threadLastVisitedAtById: {} });
  container = document.createElement("div");
  root = createRoot(container);
  input = { threads: [thread], layout: "current", filter: "done", selected: key(thread) };
});
afterEach(async () => {
  await act(async () => root.unmount());
  vi.unstubAllGlobals();
});
describe.each(["current", "legacy"] as const)("%s active status-filter retention", (layout) => {
  it("retains a selected completion after its read watermark changes, with actual counts", async () => {
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
    await render({ layout, filter: "input", threads: [{ ...thread, hasPendingUserInput: true }] });
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([key(thread)]);
    await render({ filter: "all" });
    await render({ filter: "input" });
    expect(visible()).toEqual([]);
  });
  it("never retains a nonqualifying selection or the same ID in a different environment", async () => {
    await render({ layout, threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([]);
    await render({ threads: [thread] });
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([key(thread)]);
    await render({ selected: key({ ...thread, environmentId: EnvironmentId.make("other") }) });
    expect(visible()).toEqual([]);
  });
  it("drops a removed scoped row and does not revive its old retention when it returns", async () => {
    await render({ layout });
    await render({ threads: [] });
    expect(visible()).toEqual([]);
    await render({ threads: [{ ...thread, lastVisitedAt: completed }] });
    expect(visible()).toEqual([]);
  });
  it("preserves a completion read through the local compatibility watermark", async () => {
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
