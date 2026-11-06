import { describe, expect, it } from "vite-plus/test";

import { observeEnvironment } from "./detect.ts";
import { settlementThreadEvidence } from "./threadEvidence.ts";
import { baseline, shell, thread, env } from "./testFixtures.ts";

const id = thread().id;
const family = (threads: ReturnType<typeof thread>[], snapshotSequence = 2) => ({
  threadId: id,
  generation: 1,
  family: { snapshotSequence, threads },
});

describe("settlement thread evidence", () => {
  it("keeps omitted settlement epochs and does not notify again when an older row returns", () => {
    const previous = baseline({ threads: { [id]: 200 }, watermarkMs: 200 });
    const omitted = observeEnvironment(previous, {
      environmentId: env,
      state: shell(2, []),
      nowMs: 1_000,
      staleSince: null,
    });
    expect(omitted.kind).toBe("compare");
    if (omitted.kind !== "compare") throw new Error("Expected comparison");
    expect(omitted.goneThreadIds.size).toBe(0);
    expect(omitted.baseline.threads[id]).toBe(200);
    const returned = observeEnvironment(omitted.baseline, {
      environmentId: env,
      state: shell(3, [thread("thread-a", 200)]),
      nowMs: 2_000,
      staleSince: null,
    });
    expect(returned.kind === "compare" && returned.arrivals).toEqual([]);
  });

  it("requires a current successful family read or explicit removal to establish absence", () => {
    const state = shell(3, []);
    expect(settlementThreadEvidence(state).gone.has(id)).toBe(false);
    expect(settlementThreadEvidence(state, [family([])]).gone.has(id)).toBe(true);
    expect(settlementThreadEvidence({ ...state, generation: 2 }, [family([])]).gone.has(id)).toBe(
      false,
    );
    expect(settlementThreadEvidence({ ...state, removedAt: new Map([[id, 3]]) }).gone.has(id)).toBe(
      true,
    );
  });

  it("merges family rows behind shell rows and rejects copies older than a removal", () => {
    const cached = thread("thread-a", 200, { title: "Family title" });
    const current = thread("thread-a", 200, { title: "Shell title" });
    expect(settlementThreadEvidence(shell(3, [current]), [family([cached])]).threads).toEqual([
      current,
    ]);
    const removed = settlementThreadEvidence({ ...shell(3, []), removedAt: new Map([[id, 3]]) }, [
      family([cached]),
    ]);
    expect(removed.threads).toEqual([]);
    expect(removed.gone.has(id)).toBe(true);
  });
});
