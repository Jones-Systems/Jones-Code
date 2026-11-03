import { expect, it, vi } from "vite-plus/test";
import {
  RecordingAttempts,
  RecordingCaptureRefused,
  RecordingCommandLog,
  recordingDeadline,
} from "./recordingAttempt.ts";

it("drains every independent command, including error answers, without treating transport rejection as an answer", async () => {
  const log = new RecordingCommandLog();
  const stop = Promise.withResolvers<void>();
  const ack = Promise.withResolvers<void>();
  const a = log.track(
    () => stop.promise,
    () => true,
  );
  const b = log
    .track(
      () => ack.promise,
      () => true,
    )
    .catch(() => {});
  let drained = false;
  const draining = log.drain().then(() => {
    drained = true;
  });
  stop.resolve();
  await a;
  expect(drained).toBe(false);
  ack.reject(new Error("protocol error"));
  await b;
  await draining;
  expect(drained).toBe(true);
  await log
    .track(
      () => Promise.reject(new Error("connection closed")),
      () => false,
    )
    .catch(() => {});
  let unsafe = false;
  void log.drain().then(() => {
    unsafe = true;
  });
  await Promise.resolve();
  expect(unsafe).toBe(false);
});

it("keeps replacement barriers and conditional ownership independent from routing retirement", () => {
  const attempts = new RecordingAttempts<object>();
  const owner = {};
  const replacement = {};
  const a = attempts.create("tab", owner);
  a.phase = "active";
  a.startedAt = "old";
  attempts.drop(owner);
  expect(a.frames).toBe(false);
  expect(() => attempts.assertCapture("tab", replacement)).toThrow(RecordingCaptureRefused);
  expect(attempts.project("tab", replacement, true, false)).toEqual({ state: "unknown" });
  attempts.clear("tab", a);
  const b = attempts.create("tab", replacement);
  b.phase = "active";
  b.startedAt = "new";
  attempts.clear("tab", a);
  expect(attempts.project("tab", replacement, true, false)).toEqual({
    state: "active",
    startedAt: "new",
  });
  expect(attempts.project("tab", replacement, false, false)).toEqual({ state: "unknown" });
  expect(attempts.project("tab", replacement, true, true)).toEqual({ state: "unsupported" });
});

it.each(["preparing", "queued", "capturing"] as const)(
  "deadline requests abandonment in %s without releasing submitted work",
  async (phase) => {
    vi.useFakeTimers();
    vi.setSystemTime(1000);
    try {
      const attempt = new RecordingAttempts<object>().create("tab", {});
      attempt.phase = phase;
      const gate = Promise.withResolvers<void>();
      let returned = false;
      const result = recordingDeadline({
        attempt,
        deadlineMs: 1010,
        notStarted: () => new Error("inert"),
        abandon: () => {},
        run: () => gate.promise,
      })
        .catch((error: unknown) => error)
        .then((value) => {
          returned = true;
          return value;
        });
      await vi.advanceTimersByTimeAsync(10);
      expect(attempt.abandoned).toBe(true);
      expect(returned).toBe(phase !== "capturing");
      gate.resolve();
      await result;
    } finally {
      vi.useRealTimers();
    }
  },
);
