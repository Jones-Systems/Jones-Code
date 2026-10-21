import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

import { observeEnvironment } from "./detect.ts";
import { baseline, env, shell, thread } from "./testFixtures.ts";

const input = (
  state: ReturnType<typeof shell>,
  nowMs = 1_000,
  staleSince: number | null = null,
) => ({ environmentId: env, state, nowMs, staleSince });

describe("settlement detection", () => {
  it("silently baselines first use with active and archived threads", () => {
    const result = observeEnvironment(
      null,
      input(shell(2, [thread("active", 200)], [thread("archived", 300)])),
    );
    expect(result.kind).toBe("baseline");
    if (result.kind === "baseline")
      expect(result.baseline).toMatchObject({
        threads: { active: 200, archived: 300 },
        watermarkMs: 300,
      });
  });

  it.each(["empty", "cached", "synchronizing"] as const)("ignores %s snapshots", (status) => {
    expect(
      observeEnvironment(baseline(), input(shell(2, [thread("thread-a", 200)], [], status))),
    ).toEqual({ kind: "skip" });
  });

  it("ignores live without a snapshot", () => {
    expect(
      observeEnvironment(
        baseline(),
        input({ status: "live", snapshot: Option.none(), error: Option.none() }),
      ),
    ).toEqual({ kind: "skip" });
  });

  it.each([
    { before: null, after: 200, count: 1 },
    { before: 200, after: 200, count: 0 },
    { before: 200, after: 300, count: 1 },
    { before: 200, after: null, count: 0 },
  ])("detects changed settlement epochs: $before -> $after", ({ before, after, count }) => {
    const result = observeEnvironment(
      baseline({ threads: { "thread-a": before } }),
      input(shell(2, [thread("thread-a", after)])),
    );
    expect(result.kind).toBe("compare");
    if (result.kind === "compare") expect(result.arrivals).toHaveLength(count);
  });

  it("catches new threads created at/after the watermark and silently absorbs imported history", () => {
    const rows = [99, 100, 101].map((createdAt) =>
      thread(`thread-${createdAt}`, 200, { createdAt: DateTime.makeUnsafe(createdAt) }),
    );
    const result = observeEnvironment(baseline(), input(shell(2, rows)));
    expect(result.kind).toBe("compare");
    if (result.kind === "compare") {
      expect(result.arrivals.map((arrival) => arrival.threadId)).toEqual([
        "thread-100",
        "thread-101",
      ]);
      expect(Object.keys(result.baseline.threads)).toHaveLength(3);
      expect(result.goneThreadIds.has(thread().id)).toBe(true);
    }
  });

  it("detects new arrivals after an empty baseline without inventing historical transitions", () => {
    const result = observeEnvironment(
      baseline({ threads: {}, watermarkMs: null }),
      input(shell(2, [thread("new", 200)])),
    );
    expect(result.kind === "compare" && result.arrivals.length).toBe(1);
  });

  it("includes archived/provider-native threads and excludes deleted settlements", () => {
    const result = observeEnvironment(
      baseline(),
      input(
        shell(
          2,
          [thread("deleted", 200, { deletedAt: DateTime.makeUnsafe(300) })],
          [
            thread("thread-a", 200, {
              title: "Renamed",
              archivedAt: DateTime.makeUnsafe(300),
              createdBy: "agent",
            }),
          ],
        ),
      ),
    );
    expect(result.kind).toBe("compare");
    if (result.kind === "compare") {
      expect(result.arrivals.map((arrival) => arrival.threadId)).toEqual(["thread-a"]);
      expect(result.threadTitles.get(thread().id)).toBe("Renamed");
      expect(result.goneThreadIds.has(thread("deleted").id)).toBe(true);
    }
  });

  it("suppresses direct, nested, archived and deleted subagents without losing baseline history", () => {
    const parent = thread("parent").id;
    const childLineage = {
      rootThreadId: parent,
      parentThreadId: parent,
      relationshipToParent: "subagent" as const,
    };
    const rows = [
      thread("parent", 200),
      thread("fork", 200, { lineage: { ...childLineage, relationshipToParent: "fork" } }),
      thread("mcp-child", 300, { lineage: childLineage, createdBy: "agent" }),
      thread("provider-child", 400, { lineage: childLineage }),
      thread("nested", 500, {
        lineage: { ...childLineage, parentThreadId: thread("mcp-child").id },
      }),
      thread("deleted-child", 600, { lineage: childLineage, deletedAt: DateTime.makeUnsafe(700) }),
    ];
    const archived = thread("archived-child", 800, {
      lineage: childLineage,
      archivedAt: DateTime.makeUnsafe(900),
    });
    const result = observeEnvironment(baseline(), input(shell(2, rows, [archived])));
    expect(result.kind).toBe("compare");
    if (result.kind === "compare") {
      expect(result.arrivals.map((arrival) => arrival.threadId)).toEqual(["parent", "fork"]);
      expect(result.baseline.threads).toEqual({
        parent: 200,
        fork: 200,
        "mcp-child": 300,
        "provider-child": 400,
        nested: 500,
        "deleted-child": 600,
        "archived-child": 800,
      });
      expect(result.baseline.watermarkMs).toBe(800);
      expect(result.threadTitles.get(thread("nested").id)).toBe("nested");
      expect(result.goneThreadIds.has(thread("deleted-child").id)).toBe(true);
    }
  });

  it("skips regressed sequences until the reset interval then silently rebaselines", () => {
    const prev = baseline({ snapshotSequence: 10 });
    const state = shell(1, [thread("thread-a", 200)]);
    expect(observeEnvironment(prev, input(state, 1_000))).toEqual({
      kind: "skip",
      staleSince: 1_000,
    });
    expect(observeEnvironment(prev, input(state, 60_999, 1_000))).toEqual({
      kind: "skip",
      staleSince: 1_000,
    });
    expect(observeEnvironment(prev, input(state, 61_000, 1_000)).kind).toBe("baseline");
    expect(
      observeEnvironment(prev, input(shell(10, [thread("thread-a", 200)]), 2_000, 1_000)).kind,
    ).toBe("compare");
  });
});
