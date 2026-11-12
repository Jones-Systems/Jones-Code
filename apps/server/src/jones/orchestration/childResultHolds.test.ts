import { NodeId, ThreadId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import * as ChildResultHolds from "./childResultHolds.ts";

const child = ThreadId.make("thread:child");
const task = NodeId.make("task:child");

const holds = (input: { readonly outgoing?: number; readonly failover?: boolean }) => {
  const asked: Array<unknown> = [];
  return {
    asked,
    holds: ChildResultHolds.make({
      duties: (threadId) =>
        Effect.sync(() => {
          asked.push(["duties", threadId]);
          return { outgoing: input.outgoing ?? 0 };
        }),
      holdsChildResult: (threadId, ranRun, taskId) =>
        Effect.sync(() => {
          asked.push(["failover", threadId, ranRun, taskId]);
          return input.failover ?? false;
        }),
    }),
  };
};

describe("child result holds", () => {
  it.effect("holds a child that still owes result dependencies", () =>
    Effect.gen(function* () {
      const { asked, holds: seam } = holds({ outgoing: 2 });
      assert.isTrue(yield* seam.owesDependencies(child));
      assert.deepStrictEqual(asked, [["duties", child]]);
    }),
  );

  it.effect("releases a child with no outgoing result dependencies", () =>
    Effect.gen(function* () {
      assert.isFalse(yield* holds({ outgoing: 0 }).holds.owesDependencies(child));
    }),
  );

  it.effect("holds a child while its Codex failover has not decided", () =>
    Effect.gen(function* () {
      const { asked, holds: seam } = holds({ failover: true });
      assert.isTrue(yield* seam.holdsFailover(child, null, task));
      assert.deepStrictEqual(asked, [["failover", child, null, task]]);
    }),
  );

  it.effect("releases a child with no pending failover and leaves the task scope optional", () =>
    Effect.gen(function* () {
      const { asked, holds: seam } = holds({ failover: false });
      assert.isFalse(yield* seam.holdsFailover(child, undefined));
      assert.deepStrictEqual(asked, [["failover", child, undefined, undefined]]);
    }),
  );
});
