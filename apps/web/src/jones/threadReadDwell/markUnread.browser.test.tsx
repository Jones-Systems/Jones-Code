import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { makeThreadFixture } from "../../test-fixtures";
import { useUiStateStore } from "../../uiStateStore";
import type { SidebarThreadSummary } from "../../types";

const state = vi.hoisted(() => ({
  thread: null as SidebarThreadSummary | null,
  supportsVisits: true,
  visitCommand: {},
  unreadCommand: {},
  visit: vi.fn(),
  unread: vi.fn(),
}));
vi.mock(
  import("../../state/entities"),
  () =>
    ({
      useThreadShell: () => state.thread,
      readThreadShell: () => state.thread,
      readEnvironmentSupportsVisitedTracking: () => state.supportsVisits,
    }) as never,
);
vi.mock(
  import("../../state/threads"),
  () =>
    ({
      threadEnvironment: { visit: state.visitCommand, markUnread: state.unreadCommand },
    }) as never,
);
vi.mock(
  import("../../state/use-atom-command"),
  () =>
    ({
      useAtomCommand: (command: unknown) =>
        command === state.visitCommand ? state.visit : state.unread,
    }) as never,
);
import { ThreadVisitWatermark } from "./ThreadVisitWatermark";
import { useMarkThreadUnread } from "./useMarkThreadUnread";

const updatedAt = "2026-10-17T12:00:00.000Z";
const earlier = "2026-10-17T11:00:00.000Z";
const thread = makeThreadFixture({
  id: ThreadId.make("viewed"),
  environmentId: EnvironmentId.make("first"),
  updatedAt,
  lastVisitedAt: earlier,
  latestRun: {
    runId: "run" as never,
    status: "completed",
    assistantMessageId: null,
    requestedAt: earlier,
    startedAt: earlier,
    completedAt: updatedAt,
  },
});
let root: Root;
let container: HTMLDivElement;
function Viewing({ selected }: { selected: SidebarThreadSummary }) {
  const markUnread = useMarkThreadUnread();
  const threadRef = scopeThreadRef(selected.environmentId, selected.id);
  return (
    <>
      <ThreadVisitWatermark threadRef={threadRef} />
      <button onClick={() => markUnread(threadRef)}>Mark unread</button>
    </>
  );
}
const render = (next = thread) =>
  act(async () => {
    state.thread = next;
    root.render(<Viewing selected={next} />);
  });
const advance = (ms: number) => act(async () => vi.advanceTimersByTime(ms));
const markUnread = () => act(async () => container.querySelector("button")!.click());
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.useFakeTimers();
  vi.setSystemTime(new Date(updatedAt));
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
  vi.spyOn(document, "hasFocus").mockReturnValue(true);
  state.supportsVisits = true;
  state.visit.mockClear();
  state.unread.mockClear();
  useUiStateStore.setState({ threadLastVisitedAtById: {} });
  container = document.createElement("div");
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it.each([1000, 3000])(
  "preserves explicit server unread at %i ms through activity until the next visit",
  async (elapsed) => {
    await render();
    await advance(elapsed);
    const priorVisits = state.visit.mock.calls.length;
    await markUnread();
    expect(state.unread).toHaveBeenCalledExactlyOnceWith({
      environmentId: thread.environmentId,
      input: { threadId: thread.id },
    });
    await render({ ...thread, updatedAt: "2026-10-17T12:00:10.000Z" });
    await advance(30_000);
    expect(state.visit).toHaveBeenCalledTimes(priorVisits);
    await render({ ...thread, id: ThreadId.make("other") });
    await advance(500);
    await render(thread);
    await advance(2999);
    expect(state.visit).toHaveBeenCalledTimes(priorVisits);
    await advance(1);
    expect(state.visit).toHaveBeenCalledTimes(priorVisits + 1);
  },
);
it("keeps an older server's explicit unread watermark during pending dwell and later activity", async () => {
  state.supportsVisits = false;
  const legacy = { ...thread };
  delete legacy.lastVisitedAt;
  await render(legacy);
  await advance(1000);
  await markUnread();
  const key = scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
  const unreadAt = useUiStateStore.getState().threadLastVisitedAtById[key];
  expect(Date.parse(unreadAt!)).toBeLessThan(Date.parse(updatedAt));
  await render({ ...legacy, updatedAt: "2026-10-17T12:00:10.000Z" });
  await advance(30_000);
  expect(useUiStateStore.getState().threadLastVisitedAtById[key]).toBe(unreadAt);
  expect(state.visit).not.toHaveBeenCalled();
  expect(state.unread).not.toHaveBeenCalled();
});
it("cancels on unmount without leaving an automatic visit queued", async () => {
  await render();
  await advance(2000);
  await act(async () => root.render(null));
  await advance(30_000);
  expect(state.visit).not.toHaveBeenCalled();
});
