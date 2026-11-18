import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { VisitThreadInput } from "@t3tools/client-runtime/operations";
import { claimThreadOpen } from "@t3tools/client-runtime/state/thread-search";
import { suppressThreadReadDwell } from "@t3tools/client-runtime/jones/thread-read-dwell";
import * as Cause from "effect/Cause";
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
  // Visits the server accepted; an offline one fails without reaching it.
  const visits: VisitThreadInput[] = [];
  const attempts: VisitThreadInput[] = [];
  let online = true;
  const sessionGeneration = Atom.make(0);
  const tracker = createMobileThreadVisitTracker({
    threadShellAtom: () => shell,
    sessionGenerationAtom: () => sessionGeneration,
    visit: {
      label: "mobile-dwell-test",
      run: async (_registry, input) => {
        attempts.push(input.input);
        if (!online) return AsyncResult.failure(Cause.fail("disconnected"));
        visits.push(input.input);
        return AsyncResult.success(undefined);
      },
    },
  });
  const registry = AtomRegistry.make();
  registries.push(registry);
  return {
    ref,
    shell,
    registry,
    visits,
    attempts,
    mount: () => registry.mount(tracker(ref)),
    setOnline: (value: boolean) => {
      online = value;
    },
    reconnect: () => registry.update(sessionGeneration, (generation) => generation + 1),
  };
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
  claimThreadOpen(environmentId, f.ref.threadId, Date.UTC(2026, 0, 1));
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

it("retries a rejected visit, with its open, once after the session reconnects", async () => {
  const f = fixture("mobile-reconnect");
  const release = f.mount();
  f.setOnline(false);
  await vi.advanceTimersByTimeAsync(3_000);
  expect(f.attempts).toEqual([{ threadId: f.ref.threadId, visitedAt: at(10), opened: true }]);
  f.registry.set(f.shell, { ...f.registry.get(f.shell), updatedAt: at(20) });
  await vi.advanceTimersByTimeAsync(30_000);
  expect(f.attempts).toHaveLength(1);
  f.setOnline(true);
  f.reconnect();
  await vi.advanceTimersByTimeAsync(0);
  expect(f.visits).toEqual([{ threadId: f.ref.threadId, visitedAt: at(20), opened: true }]);
  f.reconnect();
  await vi.advanceTimersByTimeAsync(30_000);
  expect(f.attempts).toHaveLength(2);
  release();
});

it("keeps mark-unread over a rejected visit through reconnects and a completion", async () => {
  const f = fixture("mobile-rejected-unread");
  const release = f.mount();
  f.setOnline(false);
  await vi.advanceTimersByTimeAsync(3_000);
  suppressThreadReadDwell(`${environmentId}:${f.ref.threadId}`);
  f.setOnline(true);
  f.reconnect();
  f.registry.set(f.shell, {
    ...f.registry.get(f.shell),
    updatedAt: at(20),
    latestRun: { completedAt: at(20) },
  });
  f.reconnect();
  await vi.advanceTimersByTimeAsync(30_000);
  expect(f.attempts).toHaveLength(1);
  expect(f.visits).toEqual([]);
  release();
});

it("drops a rejected visit when the view closes, so a reopen needs its own dwell", async () => {
  const f = fixture("mobile-rejected-closed");
  const release = f.mount();
  f.setOnline(false);
  await vi.advanceTimersByTimeAsync(3_000);
  release();
  f.setOnline(true);
  f.reconnect();
  await vi.advanceTimersByTimeAsync(30_000);
  expect(f.attempts).toHaveLength(1);
  const again = f.mount();
  await vi.advanceTimersByTimeAsync(2_999);
  expect(f.attempts).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(f.visits.map((visit) => visit.visitedAt)).toEqual([at(10)]);
  again();
});
