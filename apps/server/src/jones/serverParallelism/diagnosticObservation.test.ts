import { expect, it } from "vite-plus/test";
import {
  classifyCpuEnvelope,
  classifyResourceInterval,
  diagnosticTrace,
  type DiagnosticCpuPoint,
  type DiagnosticEvent,
} from "./diagnosticObservation.ts";

const event: DiagnosticEvent = { kind: "qualification", event: "phase-begin", atMs: 0, phase: "steady-0" };
it("latches loss without replacing earlier retained identity or admitting later events", () => {
  const trace = diagnosticTrace(1);
  trace.append(event);
  trace.append({ ...event, atMs: 2 });
  trace.append({ ...event, atMs: 3 });
  expect(trace.rows).toEqual([event]);
  expect(trace.report()).toEqual({ capacity: 1, attempted: 3, retained: 1, dropped: 2, overflow: true });
});

it.each([
  { startMs: 10, endMs: 20, retirementStartedMs: 15, capturedExitMs: 30, relation: "retirement-adjacent" },
  { startMs: 10, endMs: 20, retirementStartedMs: 25, capturedExitMs: 30, relation: "before-recorded-retirement" },
  { startMs: 10, endMs: 20, retirementStartedMs: 12, capturedExitMs: 17, relation: "crosses-captured-exit" },
  { startMs: 30, endMs: 40, capturedExitMs: 25, relation: "after-captured-exit" },
  { startMs: 10, endMs: 20, relation: "no-retirement-recorded" },
  { startMs: 20, endMs: 10, relation: "invalid-time" },
] as const)("retains unavailable evidence and charge ownership for $relation", ({ relation, ...input }) => {
  expect(classifyResourceInterval({ ...input, available: false, retireResolvedMs: 50, journalRetiredMs: 60 })).toEqual({ relation, available: false, kernelLiveness: "unknown", releaseCharge: false });
});

it("does not confuse retire resolution or stream close with captured exit, or accept PID reuse", () => {
  expect(classifyResourceInterval({ startMs: 10, endMs: 20, available: false, retireResolvedMs: 12, capturedCloseMs: 15 }).relation).toBe("no-retirement-recorded");
  expect(classifyResourceInterval({ startMs: 10, endMs: 20, available: true, startTimeTicks: "11", expectedStartTimeTicks: "10" })).toMatchObject({ relation: "pid-identity-mismatch", releaseCharge: false });
});

const before: DiagnosticCpuPoint = { pid: 2, generation: 3, nonce: "n", startTimeTicks: "17", atMs: 10, finishedAtMs: 11, cpuMs: 1 };
const after = { ...before, atMs: 19, finishedAtMs: 20, cpuMs: 2 };
const identity = { pid: 2, generation: 3, nonce: "n" };
it("accepts a whole envelope in one conservative request and keeps concurrent request identities separate", () => {
  expect(classifyCpuEnvelope(before, after, [
    { ...identity, pid: 3, requestId: 1, startMs: 0, endMs: 100 },
    { ...identity, requestId: 7, startMs: 9, endMs: 21 },
  ])).toEqual({ relation: "contained", requestId: 7 });
});
it("rejects CPU-only rises across serialized disjoint jobs, negative safe intervals and absent timing", () => {
  const jobs = [
    { ...identity, requestId: 1, startMs: 9, endMs: 14 },
    { ...identity, requestId: 2, startMs: 16, endMs: 21 },
    { ...identity, requestId: 3, startMs: 22, endMs: 8 },
  ];
  expect(classifyCpuEnvelope(before, after, jobs)).toEqual({ relation: "uncontained", requestId: null });
  expect(classifyCpuEnvelope(before, after, [])).toEqual({ relation: "uncontained", requestId: null });
  expect(classifyCpuEnvelope(before, { ...after, startTimeTicks: "18" }, jobs).relation).toBe("identity-mismatch");
  expect(classifyCpuEnvelope(before, { ...after, nonce: "replacement" }, jobs).relation).toBe("identity-mismatch");
});
