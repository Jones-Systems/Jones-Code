import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { VisitThreadInput } from "@t3tools/client-runtime/state/threads";
import { claimThreadOpen } from "@t3tools/client-runtime/state/thread-search";
import { suppressThreadReadDwell } from "@t3tools/client-runtime/jones/thread-read-dwell";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { createMobileThreadVisitTracker } from "./tracker";

const registries: AtomRegistry.AtomRegistry[] = [];
const environmentId = EnvironmentId.make("mobile-dwell-environment");
const at = (seconds: number) => `2026-01-01T00:00:${String(seconds).padStart(2, "0")}.000Z`;

function fixture(name: string) {
  vi.useFakeTimers({ now: Date.UTC(2026, 0, 1) });
  const threadId = ThreadId.make(name);
  const ref = { environmentId, threadId };
  const shell = Atom.make({
    environmentId,
    id: threadId,
    updatedAt: at(10),
    lastVisitedAt: at(0),
    latestRun: null as { completedAt: string | null } | null,
    wokeAt: null as string | null,
  });
  const visits: VisitThreadInput[] = [];
  const tracker = createMobileThreadVisitTracker({
    threadShellAtom: () => shell,
    visit: {
      label: "mobile-dwell-test",
      run: async (_registry, input) => {
        visits.push(input.input);
        return AsyncResult.success(undefined);
      },
    },
  });
  const registry = AtomRegistry.make();
  registries.push(registry);
  return { ref, shell, registry, visits, mount: () => registry.mount(tracker(ref)) };
}

afterEach(() => {
  for (const registry of registries.splice(0)) registry.dispose();
  vi.useRealTimers();
});

it("records upstream open metadata after the read dwell and uses the latest streaming watermark", () => {
  const f = fixture("mobile-open");
  const release = f.mount();
  vi.advanceTimersByTime(2_999);
  expect(f.visits).toEqual([]);
  f.registry.set(f.shell, { ...f.registry.get(f.shell), updatedAt: at(20) });
  vi.advanceTimersByTime(1);
  expect(f.visits).toEqual([{ threadId: f.ref.threadId, visitedAt: at(20), opened: true }]);
  f.registry.set(f.shell, { ...f.registry.get(f.shell), updatedAt: at(21) });
  vi.advanceTimersByTime(9_999);
  expect(f.visits).toHaveLength(1);
  vi.advanceTimersByTime(1);
  expect(f.visits[1]).toEqual({ threadId: f.ref.threadId, visitedAt: at(21) });
  release();
});

it("cancels a short or backgrounded view and preserves palette-open deduplication", () => {
  const f = fixture("mobile-background");
  claimThreadOpen(environmentId, f.ref.threadId, Date.now());
  const release = f.mount();
  vi.advanceTimersByTime(2_000);
  release();
  vi.advanceTimersByTime(10_000);
  expect(f.visits).toEqual([]);
  const again = f.mount();
  vi.advanceTimersByTime(3_000);
  expect(f.visits).toEqual([{ threadId: f.ref.threadId, visitedAt: at(10) }]);
  again();
});

it("keeps mark-unread suppression sticky and advances a scheduled wake after the dwell", () => {
  const f = fixture("mobile-suppressed");
  const release = f.mount();
  suppressThreadReadDwell(`${environmentId}:${f.ref.threadId}`);
  f.registry.set(f.shell, { ...f.registry.get(f.shell), updatedAt: at(20) });
  vi.advanceTimersByTime(3_000);
  expect(f.visits).toEqual([]);
  release();
  const wake = fixture("mobile-wake");
  const stopWake = wake.mount();
  wake.registry.set(wake.shell, { ...wake.registry.get(wake.shell), wokeAt: at(30) });
  vi.advanceTimersByTime(3_000);
  expect(wake.visits).toEqual([{ threadId: wake.ref.threadId, visitedAt: at(30), opened: true }]);
  stopWake();
});
