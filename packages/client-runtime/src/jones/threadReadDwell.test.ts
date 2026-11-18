// @effect-diagnostics globalDate:off - Vitest fake timers control the Date clock used by the view controller.
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  createThreadReadDwell,
  suppressThreadReadDwell,
  type ThreadVisitOutcome,
} from "./threadReadDwell.js";

const updatedAt = "2026-10-17T12:00:00.000Z";
const earlier = "2026-10-17T11:00:00.000Z";
const later = "2026-10-17T12:00:01.000Z";
let controller: ReturnType<typeof createThreadReadDwell>;
const onVisit = vi.fn();
const snapshot = (
  overrides: Partial<NonNullable<Parameters<typeof controller.update>[0]>> = {},
) => ({
  threadKey: "environment:thread",
  updatedAt,
  lastVisitedAt: earlier,
  visible: true,
  onVisit,
  ...overrides,
});
beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(updatedAt));
  onVisit.mockClear();
  controller = createThreadReadDwell();
});
afterEach(() => {
  controller.dispose();
  vi.useRealTimers();
});

describe("continuous thread read dwell", () => {
  it("visits only at 3000 ms and deduplicates a watermark pending its server echo", () => {
    controller.update(snapshot());
    vi.advanceTimersByTime(2999);
    expect(onVisit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onVisit).toHaveBeenCalledExactlyOnceWith(updatedAt);
    controller.update(snapshot());
    vi.advanceTimersByTime(20_000);
    expect(onVisit).toHaveBeenCalledTimes(1);
  });
  it("keeps the dwell deadline while streaming changes the eventual watermark", () => {
    controller.update(snapshot());
    vi.advanceTimersByTime(2000);
    controller.update(snapshot({ updatedAt: later }));
    vi.advanceTimersByTime(1000);
    expect(onVisit).toHaveBeenCalledExactlyOnceWith(later);
  });
  it("cancels on navigation and gives a different environment a complete new dwell", () => {
    controller.update(snapshot());
    vi.advanceTimersByTime(2999);
    controller.update(snapshot({ threadKey: "other:thread" }));
    vi.advanceTimersByTime(2999);
    expect(onVisit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onVisit).toHaveBeenCalledExactlyOnceWith(updatedAt);
  });
  it("requires a fresh continuous dwell after hiding or losing foreground", () => {
    controller.update(snapshot());
    vi.advanceTimersByTime(2999);
    controller.setVisible(false);
    vi.advanceTimersByTime(20_000);
    expect(onVisit).not.toHaveBeenCalled();
    controller.update(snapshot({ visible: false, updatedAt: later }));
    controller.setVisible(true);
    vi.advanceTimersByTime(2999);
    expect(onVisit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onVisit).toHaveBeenCalledExactlyOnceWith(later);
  });
  it("throttles later activity, while a new completion bypasses only that throttle", () => {
    controller.update(snapshot());
    vi.advanceTimersByTime(3000);
    controller.update(snapshot({ updatedAt: later, lastVisitedAt: updatedAt }));
    vi.advanceTimersByTime(9999);
    expect(onVisit).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(onVisit).toHaveBeenNthCalledWith(2, later);
    controller.update(
      snapshot({
        updatedAt: "2026-10-17T12:00:02.000Z",
        lastVisitedAt: later,
        completedAt: "2026-10-17T12:00:02.000Z",
      }),
    );
    expect(onVisit).toHaveBeenCalledTimes(3);
  });
  it("does not let an unseen completion bypass the initial dwell", () => {
    controller.update(snapshot({ completedAt: updatedAt }));
    vi.advanceTimersByTime(2999);
    expect(onVisit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onVisit).toHaveBeenCalledExactlyOnceWith(updatedAt);
  });
  it.each([1000, 3000])(
    "holds explicit unread for the entire viewing session at %i ms",
    (elapsed) => {
      controller.update(snapshot());
      vi.advanceTimersByTime(elapsed);
      const callsBeforeUnread = onVisit.mock.calls.length;
      suppressThreadReadDwell("environment:thread");
      controller.update(snapshot({ updatedAt: later, completedAt: later }));
      controller.setVisible(false);
      controller.setVisible(true);
      vi.advanceTimersByTime(30_000);
      expect(onVisit).toHaveBeenCalledTimes(callsBeforeUnread);
      controller.update(null);
      controller.update(snapshot({ updatedAt: later }));
      vi.advanceTimersByTime(2999);
      expect(onVisit).toHaveBeenCalledTimes(callsBeforeUnread);
      vi.advanceTimersByTime(1);
      expect(onVisit).toHaveBeenCalledTimes(callsBeforeUnread + 1);
    },
  );
  it("does not suppress another environment or retain listeners after disposal", () => {
    controller.update(snapshot());
    suppressThreadReadDwell("other:thread");
    vi.advanceTimersByTime(3000);
    expect(onVisit).toHaveBeenCalledTimes(1);
    controller.dispose();
    suppressThreadReadDwell("environment:thread");
    controller.update(snapshot());
    vi.advanceTimersByTime(3000);
    expect(onVisit).toHaveBeenCalledTimes(2);
  });
  it("cancels pending work on disposal", () => {
    controller.update(snapshot());
    controller.dispose();
    vi.advanceTimersByTime(30_000);
    expect(onVisit).not.toHaveBeenCalled();
  });
  it("does not dispatch invalid or already-visited watermarks", () => {
    controller.update(snapshot({ updatedAt: "invalid" }));
    vi.advanceTimersByTime(3000);
    controller.update(snapshot({ lastVisitedAt: updatedAt }));
    vi.advanceTimersByTime(3000);
    expect(onVisit).not.toHaveBeenCalled();
  });
});

describe("rejected visit retry", () => {
  // Each reply waits for the test, so it decides when and how the server answers.
  function server() {
    const replies: Array<(outcome: ThreadVisitOutcome) => void> = [];
    const visit = vi.fn(
      (_updatedAt: string) => new Promise<ThreadVisitOutcome>((resolve) => replies.push(resolve)),
    );
    const answer = async (outcome: ThreadVisitOutcome, index = replies.length - 1) => {
      replies[index]!(outcome);
      await vi.advanceTimersByTimeAsync(0);
    };
    const view = (overrides: Parameters<typeof snapshot>[0] = {}) =>
      snapshot({ onVisit: visit, sessionGeneration: 1, ...overrides });
    return { visit, answer, view };
  }

  it("retries once per new session, while streaming and completions wait", async () => {
    const { visit, answer, view } = server();
    controller.update(view());
    await vi.advanceTimersByTimeAsync(3000);
    expect(visit).toHaveBeenCalledExactlyOnceWith(updatedAt);
    await answer({ accepted: false });
    controller.update(view({ updatedAt: later, completedAt: later }));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(1);
    controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(0);
    expect(visit).toHaveBeenCalledTimes(2);
    expect(visit).toHaveBeenLastCalledWith(later);
    await answer({ accepted: true });
    controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 3 }));
    controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 4 }));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(2);
  });

  it("holds a retry behind the throttle and waits again when the retry is rejected", async () => {
    const { visit, answer, view } = server();
    controller.update(view());
    await vi.advanceTimersByTimeAsync(3000);
    await answer({ accepted: false });
    controller.update(view({ sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(9999);
    expect(visit).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(visit).toHaveBeenNthCalledWith(2, updatedAt);
    await answer({ accepted: false });
    controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(2);
    controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 3 }));
    await vi.advanceTimersByTimeAsync(0);
    expect(visit).toHaveBeenNthCalledWith(3, later);
  });

  it("retries when the new session came up before the rejection arrived", async () => {
    const { visit, answer, view } = server();
    controller.update(view());
    await vi.advanceTimersByTimeAsync(3000);
    controller.update(view({ sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(1000);
    expect(visit).toHaveBeenCalledTimes(1);
    await answer({ accepted: false });
    await vi.advanceTimersByTimeAsync(8999);
    expect(visit).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(visit).toHaveBeenNthCalledWith(2, updatedAt);
  });

  it("restores a rejected visit's metadata only on a retry after a fresh visible dwell", async () => {
    const { visit, answer, view } = server();
    const restore = vi.fn();
    controller.update(view());
    await vi.advanceTimersByTimeAsync(3000);
    await answer({ accepted: false, restore });
    controller.setVisible(false);
    controller.update(view({ visible: false, sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(1);
    expect(restore).not.toHaveBeenCalled();
    controller.setVisible(true);
    await vi.advanceTimersByTimeAsync(2999);
    expect(visit).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(visit).toHaveBeenCalledTimes(2);
    expect(restore).toHaveBeenCalledOnce();
    expect(restore.mock.invocationCallOrder[0]).toBeLessThan(visit.mock.invocationCallOrder[1]!);
  });

  it.each(["before", "after"] as const)(
    "keeps an explicit unread that arrives %s the rejection",
    async (order) => {
      const { visit, answer, view } = server();
      const restore = vi.fn();
      controller.update(view());
      await vi.advanceTimersByTimeAsync(3000);
      if (order === "before") suppressThreadReadDwell("environment:thread");
      await answer({ accepted: false, restore });
      if (order === "after") suppressThreadReadDwell("environment:thread");
      controller.update(view({ sessionGeneration: 2 }));
      controller.update(view({ updatedAt: later, completedAt: later, sessionGeneration: 3 }));
      await vi.advanceTimersByTimeAsync(30_000);
      expect(visit).toHaveBeenCalledTimes(1);
      expect(restore).not.toHaveBeenCalled();
    },
  );

  it.each(["before", "after"] as const)(
    "does not retry a visit the server showed %s rejecting it, even after an unread rewind",
    async (order) => {
      const { visit, answer, view } = server();
      const restore = vi.fn();
      controller.update(view());
      await vi.advanceTimersByTimeAsync(3000);
      if (order === "before") controller.update(view({ lastVisitedAt: updatedAt }));
      await answer({ accepted: false, restore });
      if (order === "after") controller.update(view({ lastVisitedAt: updatedAt }));
      // Another client marks the thread unread, then the session reconnects.
      controller.update(view({ lastVisitedAt: earlier }));
      controller.update(view({ lastVisitedAt: earlier, sessionGeneration: 2 }));
      await vi.advanceTimersByTimeAsync(30_000);
      expect(visit).toHaveBeenCalledTimes(1);
      expect(restore).not.toHaveBeenCalled();
    },
  );

  it("ignores a rejection made stale by a newer visit, navigation or disposal", async () => {
    const { visit, answer, view } = server();
    controller.update(view());
    await vi.advanceTimersByTimeAsync(3000);
    controller.update(view({ updatedAt: later }));
    await vi.advanceTimersByTimeAsync(10_000);
    expect(visit).toHaveBeenNthCalledWith(2, later);
    await answer({ accepted: false }, 0);
    controller.update(view({ updatedAt: later, sessionGeneration: 2 }));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(2);
    controller.update(view({ threadKey: "other:thread", sessionGeneration: 2 }));
    await answer({ accepted: false }, 1);
    controller.update(view({ threadKey: "other:thread", sessionGeneration: 3 }));
    await vi.advanceTimersByTimeAsync(3000);
    expect(visit).toHaveBeenCalledTimes(3);
    controller.dispose();
    await answer({ accepted: false }, 2);
    controller.update(view({ threadKey: "other:thread", sessionGeneration: 4 }));
    await vi.advanceTimersByTimeAsync(2999);
    expect(visit).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(visit).toHaveBeenCalledTimes(4);
  });
});
