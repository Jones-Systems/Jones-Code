import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { makeThreadFixture } from "../../test-fixtures";
import { ThreadStatusFilterPill } from "./ThreadStatusFilterPill";
import { useThreadStatusFilter } from "./useThreadStatusFilter";
import "~/index.css";

const idle = makeThreadFixture({ id: ThreadId.make("idle"), title: "Idle", runtime: null });
const working = makeThreadFixture({
  id: ThreadId.make("working"),
  title: "Working",
  runtime: {
    status: "running",
    providerName: "Codex",
    providerInstanceId: ProviderInstanceId.make("codex"),
    activeRunId: null,
    lastError: null,
    updatedAt: "2026-10-17T12:00:00.000Z",
  },
});
const input = makeThreadFixture({
  id: ThreadId.make("input"),
  title: "Input",
  hasPendingUserInput: true,
});
const done = makeThreadFixture({
  id: ThreadId.make("done"),
  title: "Done",
  runtime: null,
  lastVisitedAt: "2026-10-17T11:00:00.000Z",
  latestRun: {
    runId: "run" as never,
    status: "completed",
    assistantMessageId: null,
    requestedAt: "2026-10-17T11:00:00.000Z",
    startedAt: "2026-10-17T11:00:00.000Z",
    completedAt: "2026-10-17T12:00:00.000Z",
  },
});
let root: Root;
let container: HTMLDivElement;
function Fixture({
  layout,
  rows = [working, input, done, idle],
}: {
  layout: "current" | "legacy";
  rows?: ReturnType<typeof makeThreadFixture>[];
}) {
  const state = useThreadStatusFilter(
    rows,
    layout,
    undefined,
    scopedThreadKey(scopeThreadRef(idle.environmentId, idle.id)),
  );
  return (
    <>
      <ThreadStatusFilterPill
        counts={state.counts}
        filter={state.statusFilter}
        onCycle={state.cycleStatusFilter}
        onHideWorking={state.hideWorkingThreads}
      />
      <ul>
        {state.threads.map((row) => (
          <li key={row.id}>{row.title}</li>
        ))}
      </ul>
    </>
  );
}
const button = () => container.querySelector("button")!;
const titles = () => [...container.querySelectorAll("li")].map((row) => row.textContent);
const clickAt = async (timeStamp: number) =>
  act(async () => {
    const event = new MouseEvent("click", { bubbles: true, detail: 1 });
    Object.defineProperty(event, "timeStamp", { value: timeStamp });
    button().dispatchEvent(event);
  });
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
describe.each(["current", "legacy"] as const)("%s status shortcut", (layout) => {
  it("applies the first click immediately, accepts 250 ms, consumes a third click and reverses", async () => {
    await act(async () => root.render(<Fixture layout={layout} />));
    const target = button();
    const bounds = target.getBoundingClientRect();
    await clickAt(1000);
    expect(titles()).toEqual(["Working"]);
    await clickAt(1250);
    expect(titles()).toEqual(["Input", "Done", "Idle"]);
    expect(button()).toBe(target);
    expect(target.getBoundingClientRect().toJSON()).toEqual(bounds.toJSON());
    expect(target.getAttribute("aria-label")).toContain(
      "1 working, 1 needs input, 1 done. Showing Hide working.",
    );
    const workingNumber = [...target.querySelectorAll("span")].find((node) =>
      node.classList.contains("line-through"),
    )!;
    expect(getComputedStyle(workingNumber).textDecorationLine).toContain("line-through");
    await clickAt(1300);
    expect(titles()).toEqual(["Input", "Done", "Idle"]);
    await act(async () =>
      root.render(
        <Fixture
          layout={layout}
          rows={[working, input, done, { ...idle, runtime: working.runtime }]}
        />,
      ),
    );
    expect(titles()).toEqual(["Input", "Done"]);
    await clickAt(1551);
    expect(titles()).toEqual(["Working", "Input", "Done", "Idle"]);
  });
  it("cycles at 251 ms even if the operating system would group the clicks", async () => {
    await act(async () => root.render(<Fixture layout={layout} />));
    await clickAt(1000);
    await clickAt(1251);
    expect(titles()).toEqual(["Input"]);
  });
  it("keeps Enter and Space on the ordinary cycle and breaks the pointer sequence", async () => {
    await act(async () => root.render(<Fixture layout={layout} />));
    await clickAt(1000);
    await act(async () => {
      button().focus();
      await userEvent.keyboard("{Enter}");
    });
    expect(titles()).toEqual(["Input"]);
    await act(async () => {
      await userEvent.keyboard(" ");
    });
    expect(titles()).toEqual(["Done"]);
    await clickAt(1100);
    expect(titles()).toEqual(["Working", "Input", "Done", "Idle"]);
  });
});
