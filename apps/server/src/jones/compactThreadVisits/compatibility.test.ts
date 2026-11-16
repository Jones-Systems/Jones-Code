import { describe, expect, it } from "@effect/vitest";
import { EventId, ThreadId, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import { threadReplayEncodedBytes } from "../../orchestration-v2/ThreadStream.ts";
import { makeCompactVisitCompatibility } from "./compatibility.ts";

const thread = () => idleThreadProjection(liveThreadShell(ThreadId.make("visit-thread"))).thread;
const visit = (lastVisitedAt = DateTime.makeUnsafe("2026-10-01T00:00:00Z")): Extract<OrchestrationV2DomainEvent, { type: "thread.visit-recorded" }> => ({
  type: "thread.visit-recorded", id: EventId.make("visit-event"), threadId: ThreadId.make("visit-thread"),
  occurredAt: DateTime.makeUnsafe("2026-11-01T00:00:00Z"), payload: { lastVisitedAt },
});

describe("subscriber compact visit compatibility", () => {
  it("keeps simultaneous legacy and opted-in output separate without mutating the canonical event", () => {
    const old = makeCompactVisitCompatibility(false);
    const modern = makeCompactVisitCompatibility(true);
    const baseline = thread();
    old.seed(baseline);
    modern.seed(baseline);
    const canonical = visit();
    const expanded = old.project(canonical);
    expect(modern.project(canonical)).toBe(canonical);
    expect(canonical.type).toBe("thread.visit-recorded");
    expect(expanded).toEqual({ ...canonical, type: "thread.visited", payload: {
      ...baseline, lastVisitedAt: canonical.payload.lastVisitedAt,
    } });
    expect(baseline.lastVisitedAt).not.toEqual(canonical.payload.lastVisitedAt);
  });

  it("retains the last delivered full replacement and activity timestamp across older visits", () => {
    const adapter = makeCompactVisitCompatibility(false);
    const initial = thread();
    adapter.seed(initial);
    const updated = { ...initial, title: "delivered metadata", updatedAt: DateTime.makeUnsafe("2026-11-02T00:00:00Z") };
    const compact = visit();
    adapter.project({ ...compact, type: "thread.metadata-updated", payload: updated });
    const expanded = adapter.project(compact);
    expect(expanded.type).toBe("thread.visited");
    if (expanded.type !== "thread.visited") throw new Error("expected legacy visit");
    expect(expanded.payload.title).toBe(updated.title);
    expect(expanded.payload.updatedAt).toBe(updated.updatedAt);
    expect(expanded.id).toBe(compact.id);
    expect(expanded.occurredAt).toBe(compact.occurredAt);
  });

  it("requires a baseline per generation and measures expanded payloads", () => {
    const old = makeCompactVisitCompatibility(false);
    expect(() => old.project(visit())).toThrow("delivered thread baseline");
    old.seed({ ...thread(), title: "x".repeat(10000) });
    expect(threadReplayEncodedBytes([old.project(visit())])).toBeGreaterThan(10000);
    expect(() => makeCompactVisitCompatibility(false).project(visit())).toThrow();
  });
});
