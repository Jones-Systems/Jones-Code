// @effect-diagnostics globalDate:off - Vitest fake timers control the Date clock used by the view controller.
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { createThreadReadDwell, suppressThreadReadDwell } from "./threadReadDwell.js";

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
