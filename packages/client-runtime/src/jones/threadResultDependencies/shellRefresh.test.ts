import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { v2ShellSnapshot, v2ThreadShell } from "../../state/orchestrationV2TestFixtures.ts";
import { applyShellStreamEvent } from "../../state/shellReducer.ts";
import { advanceThreadRefreshWatermarks, shellRefreshSubscription } from "./shellRefresh.ts";

const task = {
  taskId: "jones-dependency:answer",
  kind: "monitor",
  awaited: true,
  description: "Waiting for VPS review",
} as const;
const snapshot = { ...v2ShellSnapshot, snapshotSequence: 10 };
const waiting = { ...v2ThreadShell, pendingBackgroundTasks: [task] };
const refresh = (sequence: number, thread = waiting) => ({
  kind: "thread.refreshed" as const,
  sequence,
  location: "active" as const,
  thread,
});

describe("dependency shell refresh", () => {
  it("CR-R3: removes an omitted child at equal sequence without skipping replay or treating it as deleted", () => {
    const current = { ...snapshot, threads: [waiting] };
    const omitted = { kind: "thread.omitted" as const, sequence: 10, location: "active" as const, threadId: waiting.id };
    const next = applyShellStreamEvent(current, omitted);
    expect(next.threads).toEqual([]);
    expect(next.snapshotSequence).toBe(10);
    expect(applyShellStreamEvent(next, omitted)).toBe(next);
    expect(applyShellStreamEvent(next, refresh(10)).threads).toEqual([waiting]);
  });

  it("CR-R3: fences omission 12 against refresh and replay 11 while preserving newer reappearance", () => {
    const omitted = { kind: "thread.omitted" as const, sequence: 12, location: "active" as const, threadId: waiting.id };
    const watermarks = advanceThreadRefreshWatermarks(new Map(), snapshot, omitted, new Map());
    const missing = applyShellStreamEvent({ ...snapshot, threads: [waiting] }, omitted);
    expect(missing.snapshotSequence).toBe(10);
    expect(applyShellStreamEvent(missing, refresh(11), new Map(), watermarks)).toBe(missing);
    const replay = { kind: "thread.updated" as const, sequence: 11, location: "active" as const, thread: waiting };
    const advanced = applyShellStreamEvent(missing, replay, new Map(), watermarks);
    expect(advanced.threads).toEqual([]);
    expect(advanced.snapshotSequence).toBe(11);
    expect(advanceThreadRefreshWatermarks(watermarks, missing, replay, new Map())).toBe(watermarks);
    expect(applyShellStreamEvent(advanced, refresh(12), new Map(), watermarks).threads).toEqual([waiting]);
    expect(applyShellStreamEvent(advanced, { ...omitted, sequence: 11 }, new Map(), watermarks)).toBe(advanced);
  });

  it("opts in only for an advertised capability, omitting new keys for old servers", () => {
    expect(shellRefreshSubscription({})).toEqual({});
    expect(shellRefreshSubscription({ shellThreadRefresh: false })).toEqual({});
    expect(shellRefreshSubscription({ shellThreadRefresh: true })).toEqual({
      acceptThreadRefresh: true,
    });
  });

  it("applies an equal-sequence refresh and clears a released wait at the same cursor", () => {
    const next = applyShellStreamEvent(snapshot, refresh(10));
    expect(next.threads[0]?.pendingBackgroundTasks).toEqual([task]);
    expect(next.snapshotSequence).toBe(10);
    expect(applyShellStreamEvent(next, refresh(10))).toBe(next);
    const cleared = applyShellStreamEvent(
      next,
      refresh(10, { ...waiting, pendingBackgroundTasks: [] }),
    );
    expect(cleared.threads[0]?.pendingBackgroundTasks).toEqual([]);
    expect(cleared.snapshotSequence).toBe(10);
  });

  it("ignores stale refreshes and leaves normal equal-sequence replay unchanged", () => {
    expect(applyShellStreamEvent(snapshot, refresh(9))).toBe(snapshot);
    expect(applyShellStreamEvent(snapshot, { ...refresh(10), kind: "thread.updated" })).toBe(
      snapshot,
    );
  });

  it("does not advance replay over undelivered application events", () => {
    const next = applyShellStreamEvent(snapshot, refresh(12));
    expect(next.snapshotSequence).toBe(10);
    const replay = applyShellStreamEvent(next, {
      kind: "project.removed",
      sequence: 11,
      projectId: v2ThreadShell.projectId,
    });
    expect(replay.projects).toEqual([]);
    expect(replay.snapshotSequence).toBe(11);
    expect(replay.threads[0]?.pendingBackgroundTasks).toEqual([task]);
  });

  it.each(["updated", "removed", "archived"] as const)(
    "keeps refresh 12 while advancing replay through the same thread's %s 11",
    (kind) => {
      const watermarks = advanceThreadRefreshWatermarks(new Map(), snapshot, refresh(12), new Map());
      const next = applyShellStreamEvent(snapshot, refresh(12));
      const event =
        kind === "removed"
          ? { kind: "thread.removed" as const, sequence: 11, location: "active" as const, threadId: waiting.id }
          : {
              kind: "thread.updated" as const,
              sequence: 11,
              location: kind === "archived" ? "archive" as const : "active" as const,
              thread: v2ThreadShell,
            };
      const replay = applyShellStreamEvent(next, event, new Map(), watermarks);
      expect(replay.snapshotSequence).toBe(11);
      expect(replay.threads).toBe(next.threads);
      expect(replay.threads[0]?.pendingBackgroundTasks).toEqual([task]);
      expect(advanceThreadRefreshWatermarks(watermarks, next, event, new Map())).toBe(watermarks);
    },
  );

  it("rejects refresh 11 after refresh 12 even while replay remains at 10", () => {
    const watermarks = advanceThreadRefreshWatermarks(new Map(), snapshot, refresh(12), new Map());
    const next = applyShellStreamEvent(snapshot, refresh(12));
    const released = { ...waiting, pendingBackgroundTasks: [] };
    expect(applyShellStreamEvent(next, refresh(11, released), new Map(), watermarks)).toBe(next);
    expect(next.snapshotSequence).toBe(10);
    expect(advanceThreadRefreshWatermarks(watermarks, next, refresh(11), new Map())).toBe(watermarks);
    expect(
      applyShellStreamEvent(next, refresh(12, released), new Map(), watermarks)
        .threads[0]?.pendingBackgroundTasks,
    ).toEqual([]);
  });

  it("records unchanged refresh rows and retires their fences when replay catches up", () => {
    const current = { ...snapshot, threads: [waiting] };
    expect(applyShellStreamEvent(current, refresh(12))).toBe(current);
    const watermarks = advanceThreadRefreshWatermarks(new Map(), current, refresh(12), new Map());
    expect(watermarks.get(waiting.id)).toBe(12);
    const event = {
      ...refresh(12, { ...waiting, pendingBackgroundTasks: [] }),
      kind: "thread.updated" as const,
    };
    const replay = applyShellStreamEvent(current, event, new Map(), watermarks);
    expect(replay.snapshotSequence).toBe(12);
    expect(replay.threads).toBe(current.threads);
    expect(advanceThreadRefreshWatermarks(watermarks, current, event, new Map()).size).toBe(0);
    expect(
      applyShellStreamEvent(replay, { ...event, sequence: 13 }).threads[0]?.pendingBackgroundTasks,
    ).toEqual([]);
    const unrelated = { kind: "project.removed" as const, sequence: 13, projectId: waiting.projectId };
    expect(advanceThreadRefreshWatermarks(watermarks, current, unrelated, new Map()).size).toBe(0);
  });

  it.each(["removed", "archived"] as const)(
    "lets an equal-sequence %s supersede the refresh",
    (kind) => {
      const watermarks = advanceThreadRefreshWatermarks(new Map(), snapshot, refresh(12), new Map());
      const next = applyShellStreamEvent(snapshot, refresh(12));
      const event =
        kind === "removed"
          ? { kind: "thread.removed" as const, sequence: 12, location: "active" as const, threadId: waiting.id }
          : { kind: "thread.updated" as const, sequence: 12, location: "archive" as const, thread: waiting };
      expect(applyShellStreamEvent(next, event, new Map(), watermarks).threads).toEqual([]);
    },
  );

  it("cannot revive a thread removed at or after the refresh, but allows a newer materialization", () => {
    const missing = { ...snapshot, threads: [] };
    for (const removedSequence of [10, 11]) {
      const removedAt = new Map([[v2ThreadShell.id, removedSequence]]);
      expect(applyShellStreamEvent(missing, refresh(10), removedAt)).toBe(missing);
    }
    expect(
      applyShellStreamEvent(missing, refresh(11), new Map([[v2ThreadShell.id, 10]])).threads,
    ).toEqual([waiting]);
  });

  it("can materialize an omitted thread without changing unrelated or archived rows", () => {
    const other = { ...v2ThreadShell, id: ThreadId.make("other") };
    const next = applyShellStreamEvent(
      { ...snapshot, threads: [other], archivedThreads: [other] },
      refresh(10),
    );
    expect(next.threads).toEqual([other, waiting]);
    expect(next.threads[0]).toBe(other);
    expect(next.archivedThreads).toEqual([other]);
  });
});
