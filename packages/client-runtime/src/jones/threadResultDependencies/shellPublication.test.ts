import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeShellPublicationHarness } from "../shellPublication/shellPublicationHarness.ts";

describe("result dependency shell consumers", () => {
  it.effect("publishes equal-sequence changes to shared list, quiet view and monitor consumers", () =>
    Effect.gen(function* () {
      const h = yield* makeShellPublicationHarness({ count: 1 });
      const environmentId = h.ref.environmentId;
      const original = h.registry.get(h.threads.threadShellAtom(h.ref));
      if (original === null) throw new Error("Expected a synthetic shell");
      const task = {
        taskId: "jones-dependency:review",
        kind: "monitor",
        awaited: true,
        description: "Waiting for VPS review: release readiness",
      } as const;
      const source = { ...original.source, pendingBackgroundTasks: [task] };
      yield* h.publish(
        environmentId,
        [{ kind: "thread.refreshed", sequence: 1, location: "active", thread: source }],
        1,
      );
      const quiet = h.registry.get(h.threads.quietThreadShellAtom(h.ref));
      expect(quiet?.pendingBackgroundTasks).toEqual([task]);
      expect(quiet?.runtime?.status).toBe("idle");
      expect(h.registry.get(h.threads.threadShellAtom(h.ref))?.pendingBackgroundTasks).toEqual([
        task,
      ]);
      expect(h.registry.get(h.threads.threadShellsAtom)[0]?.pendingBackgroundTasks).toEqual([task]);

      yield* h.publish(
        environmentId,
        [{
          kind: "thread.refreshed",
          sequence: 1,
          location: "active",
          thread: { ...source, pendingBackgroundTasks: [] },
        }],
        1,
      );
      expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.pendingBackgroundTasks).toEqual(
        [],
      );
      expect(h.registry.get(h.threads.threadShellsAtom)[0]?.pendingBackgroundTasks).toEqual([]);
    }),
  );
});
