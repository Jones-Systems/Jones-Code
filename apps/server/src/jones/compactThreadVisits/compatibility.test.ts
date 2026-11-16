import { describe, expect, it } from "@effect/vitest";
import { EventId, ThreadId, type ThreadPullRequestLink, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
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

  it("preserves narrow PR updates, explicit unread and omitted opens in delivery order", () => {
    const adapter = makeCompactVisitCompatibility(false);
    const link: ThreadPullRequestLink = { host: "github", repository: "Jones-Systems/Jones-Code", number: 1,
      url: "https://github.com/Jones-Systems/Jones-Code/pull/1", source: "manual",
      linkedAt: "2026-11-01T00:00:00.000Z", snapshot: null, stack: null };
    const initial = { ...thread(), pullRequests: [link], opens: { openedAt: 1, score: 2, picks: [] } };
    adapter.seed(initial);
    const compact = visit();
    const updatedLink = { ...link, url: "https://github.com/Jones-Systems/Jones-Code/pull/1?updated" };
    adapter.project({ ...compact, type: "thread.pull-request-link-updated", payload: updatedLink });
    const expanded = adapter.project(compact);
    if (expanded.type !== "thread.visited") throw new Error("expected legacy visit");
    expect(expanded.payload.pullRequests).toEqual([updatedLink]);
    expect(expanded.payload.opens).toEqual(initial.opens);
    adapter.project({ ...compact, type: "thread.marked-unread", payload: { ...expanded.payload, lastVisitedAt: null } });
    const opensOnly = adapter.project({ ...compact, payload: { lastVisitedAt: null, opens: null } });
    if (opensOnly.type !== "thread.visited") throw new Error("expected legacy visit");
    expect(opensOnly.payload.lastVisitedAt).toBeNull();
    expect(opensOnly.payload.opens).toBeNull();
    expect(opensOnly.payload.pullRequests).toEqual([updatedLink]);
  });

  it("requires a baseline per generation and measures expanded payloads", () => {
    const old = makeCompactVisitCompatibility(false);
    expect(() => old.project(visit())).toThrow("delivered thread baseline");
    old.seed({ ...thread(), title: "x".repeat(10000) });
    expect(threadReplayEncodedBytes([old.project(visit())])).toBeGreaterThan(10000);
    expect(() => makeCompactVisitCompatibility(false).project(visit())).toThrow();
  });
});
