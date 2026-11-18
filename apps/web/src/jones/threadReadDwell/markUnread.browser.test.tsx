import { act, render as renderTest, type Screen, fakeTimers } from "../../../test/render";

import { RegistryContext } from "@effect/atom-react";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
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
vi.mock(
  import("../../connection/catalog"),
  () =>
    ({
      environmentCatalog: {
        sessionGenerationAtom: (environmentId: string) => sessionGeneration(environmentId),
      },
    }) as never,
);
import { ThreadVisitWatermark } from "../../components/chat/ThreadVisitWatermark";
import { useMarkThreadUnread } from "./useMarkThreadUnread";

// Each test's registry owns the connection sessions its environments have seen.
const sessionGeneration = Atom.family((_environmentId: string) =>
  Atom.make(0).pipe(Atom.keepAlive),
);

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
let root: Screen;
let container: HTMLDivElement;
let registry = AtomRegistry.make();
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
const render = async (next = thread) =>
  await act(async () => {
    state.thread = next;
    await root.rerender(
      <RegistryContext.Provider value={registry}>
        <Viewing selected={next} />
      </RegistryContext.Provider>,
    );
  });
const advance = async (ms: number) => await act(async () => vi.advanceTimersByTime(ms));
const markUnread = async () => await act(async () => container.querySelector("button")!.click());
const reconnect = async (environmentId: EnvironmentId) =>
  await act(async () =>
    registry.update(sessionGeneration(environmentId), (generation) => generation + 1),
  );
async function setupTest(): Promise<AsyncDisposable> {
  let testTimers: Disposable | undefined;
  let initialized = false;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
      await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        registry.dispose();
        vi.useRealTimers();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
        testTimers?.[Symbol.dispose]();
      }
    }
  }
  try {
    testTimers = fakeTimers();
    vi.setSystemTime(new Date(updatedAt));
    vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    vi.spyOn(document, "hasFocus").mockReturnValue(true);
    state.supportsVisits = true;
    state.visit.mockReset().mockResolvedValue(AsyncResult.success(undefined));
    state.unread.mockClear();
    useUiStateStore.setState({ threadLastVisitedAtById: {} });
    registry = AtomRegistry.make();
    container = document.createElement("div");
    root = await renderTest(null, { container: container, router: false });
    initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

it.each([1000, 3000])(
  "preserves explicit server unread at %i ms through activity until the next visit",
  async (elapsed) => {
    await using _fixture = await setupTest();
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
  await using _fixture = await setupTest();
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
  await using _fixture = await setupTest();
  await render();
  await advance(2000);
  await act(async () => await root.rerender(null));
  await advance(30_000);
  expect(state.visit).not.toHaveBeenCalled();
});
it("keeps explicit unread over a visit rejected after it, through reconnects and activity", async () => {
  await using _fixture = await setupTest();
  let reject: () => void = () => undefined;
  state.visit.mockReturnValueOnce(
    new Promise<AtomCommandResult<void, string>>((resolve) => {
      reject = () => resolve(AsyncResult.failure(Cause.fail("disconnected")));
    }),
  );
  await render();
  await advance(3000);
  expect(state.visit).toHaveBeenCalledTimes(1);
  await markUnread();
  await act(async () => reject());
  await reconnect(thread.environmentId);
  await render({ ...thread, updatedAt: "2026-10-17T12:00:10.000Z" });
  await reconnect(thread.environmentId);
  await advance(30_000);
  expect(state.unread).toHaveBeenCalledOnce();
  expect(state.visit).toHaveBeenCalledTimes(1);
});
