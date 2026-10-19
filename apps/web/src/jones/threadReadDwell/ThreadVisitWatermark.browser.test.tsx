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
  visit: vi.fn(),
}));
vi.mock(import("../../state/entities"), () => ({ useThreadShell: () => state.thread }) as never);
vi.mock(import("../../state/threads"), () => ({ threadEnvironment: { visit: {} } }) as never);
vi.mock(
  import("../../state/use-atom-command"),
  () => ({ useAtomCommand: () => state.visit }) as never,
);
import { ThreadVisitWatermark } from "./ThreadVisitWatermark";

const updatedAt = "2026-10-17T12:00:00.000Z";
const thread = makeThreadFixture({
  id: ThreadId.make("viewed"),
  environmentId: EnvironmentId.make("first"),
  updatedAt,
  lastVisitedAt: "2026-10-17T11:00:00.000Z",
});
let root: Root;
const render = (next = thread) =>
  act(async () => {
    state.thread = next;
    root.render(<ThreadVisitWatermark threadRef={scopeThreadRef(next.environmentId, next.id)} />);
  });
const advance = (ms: number) => act(async () => vi.advanceTimersByTime(ms));
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.useFakeTimers();
  vi.setSystemTime(new Date(updatedAt));
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
  vi.spyOn(document, "hasFocus").mockReturnValue(true);
  state.visit.mockClear();
  useUiStateStore.setState({ threadLastVisitedAtById: {} });
  root = createRoot(document.createElement("div"));
});
afterEach(async () => {
  await act(async () => root.unmount());
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("waits until 3000 continuous visible milliseconds before dispatching the visit", async () => {
  await render();
  await advance(2999);
  expect(state.visit).not.toHaveBeenCalled();
  await advance(1);
  expect(state.visit).toHaveBeenCalledExactlyOnceWith({
    environmentId: thread.environmentId,
    input: { threadId: thread.id, visitedAt: updatedAt },
  });
});
it("cancels rapid navigation and gives the same thread ID in another environment a fresh dwell", async () => {
  await render();
  await advance(2000);
  const next = { ...thread, environmentId: EnvironmentId.make("second") };
  await render(next);
  await advance(2999);
  expect(state.visit).not.toHaveBeenCalled();
  await advance(1);
  expect(state.visit).toHaveBeenCalledExactlyOnceWith({
    environmentId: next.environmentId,
    input: { threadId: next.id, visitedAt: updatedAt },
  });
});
it("does not restart initial dwell for streaming updates and dispatches the newest watermark", async () => {
  await render();
  await advance(1000);
  await render({ ...thread, updatedAt: "2026-10-17T12:00:01.000Z" });
  await advance(1000);
  await render({ ...thread, updatedAt: "2026-10-17T12:00:02.000Z" });
  await advance(1000);
  expect(state.visit).toHaveBeenCalledExactlyOnceWith({
    environmentId: thread.environmentId,
    input: { threadId: thread.id, visitedAt: "2026-10-17T12:00:02.000Z" },
  });
});
it("cancels hidden or unfocused viewing and starts a fresh foreground dwell", async () => {
  await render();
  await advance(2000);
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
  await act(async () => document.dispatchEvent(new Event("visibilitychange")));
  await advance(5000);
  expect(state.visit).not.toHaveBeenCalled();
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
  await act(async () => document.dispatchEvent(new Event("visibilitychange")));
  await advance(2000);
  await act(async () => window.dispatchEvent(new Event("blur")));
  await advance(5000);
  expect(state.visit).not.toHaveBeenCalled();
  await act(async () => window.dispatchEvent(new Event("focus")));
  await advance(2999);
  expect(state.visit).not.toHaveBeenCalled();
  await advance(1);
  expect(state.visit).toHaveBeenCalledTimes(1);
});
it("applies the same dwell to older servers' local watermark", async () => {
  const legacyThread = { ...thread };
  delete legacyThread.lastVisitedAt;
  await render(legacyThread);
  const key = scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
  await advance(2999);
  expect(useUiStateStore.getState().threadLastVisitedAtById[key]).toBeUndefined();
  await advance(1);
  expect(useUiStateStore.getState().threadLastVisitedAtById[key]).toBe(updatedAt);
  expect(state.visit).not.toHaveBeenCalled();
});
