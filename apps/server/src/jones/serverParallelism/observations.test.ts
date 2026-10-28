import { expect, it } from "@effect/vitest";
import { observeRequestTiming, observeSafely, type ComputeObservation } from "./observations.ts";
import { summarizeComputeObservations } from "./combinedQualification.ts";

it("keeps resource lifecycle callbacks running when an observation sink fails", () => {
  const delivered: number[] = [];
  const observe = observeSafely<number>((event) => {
    delivered.push(event);
    throw new Error("telemetry unavailable");
  });
  expect(() => {
    observe(1);
    observe(2);
  }).not.toThrow();
  expect(delivered).toEqual([1, 2]);
  expect(() => observeSafely(undefined)(1)).not.toThrow();
});

it("reports absent optional telemetry as unavailable and never inserts zero samples", () => {
  const events: ComputeObservation[] = [];
  const base = { slotId: 1, generation: 2, queueDepth: 0, operation: "refresh" as const, outcome: "failed" as const };
  observeRequestTiming((event) => { events.push(event); throw new Error("sink"); }, base);
  expect(events.map((event) => "durationMs" in event ? event.durationMs : undefined)).toEqual([null, null, null, null]);
  expect(events.map((event) => event.phase)).toEqual(["execute", "reply", "reply_delivery", "parent_continuation"]);
  const summary = summarizeComputeObservations(events);
  expect(summary.execute).toMatchObject({ count: 0, unavailable: 1, max: null, availability: "unavailable" });
});

it("distinguishes successful execution from cancelled reply delivery and retains interval uncertainty", () => {
  const events: ComputeObservation[] = [];
  observeRequestTiming((event) => events.push(event), { slotId: 0, generation: 9, queueDepth: 2,
    operation: "search", outcome: "cancelled" }, {
    worker: { parentSentMs: 1, receivedMs: 10_000, executionStartedMs: 10_002, executionCompletedMs: 10_012,
      executionOutcome: "ok", replyStartedMs: 10_015, handoffMs: 10_030 },
    delivery: { lowerMs: 0, upperMs: 8, durationMs: 4, uncertaintyMs: 4, offsetLowerMs: -10_000, offsetUpperMs: -9992 },
    parentContinuationMs: 3,
  });
  expect(events).toMatchObject([
    { phase: "execute", durationMs: 10, outcome: "ok", workerStartedMs: 10_002, workerCompletedMs: 10_012 },
    { phase: "reply", durationMs: 15, outcome: "cancelled" },
    { phase: "reply_delivery", durationMs: 4, estimated: true, lowerMs: 0, upperMs: 8, uncertaintyMs: 4 },
    { phase: "parent_continuation", durationMs: 3 },
  ]);
});
