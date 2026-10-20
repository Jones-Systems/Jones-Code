import { it } from "@effect/vitest";
import {
  JonesActionsError,
  type JonesSourceRelationKind,
} from "@t3tools/shared/jones/jonesActions";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { describe, expect } from "vite-plus/test";

import * as SourceRelation from "./sourceRelation.ts";

const BASE = "a".repeat(40),
  HEAD = "b".repeat(40),
  OTHER = "c".repeat(40);
const T0 = Date.parse("2026-10-20T12:00:00Z");

function harness(
  answer: (base: string, head: string) => Promise<JonesSourceRelationKind>,
  options: { readonly timeoutMs?: number; readonly cacheLimit?: number } = {},
) {
  let clock = T0;
  const calls: Array<readonly [string, string]> = [];
  const service = SourceRelation.make({
    compare: (base, head) => {
      calls.push([base, head]);
      return answer(base, head);
    },
    now: () => clock,
    failureBackoffMs: 30_000,
    ...options,
  });
  return {
    calls,
    advance: (ms: number) => void (clock += ms),
    relation: (base: string, head: string) => service.relation({ base, head }),
  };
}

describe("JonesSourceRelation", () => {
  it.effect("binds the proven relation to exactly the requested pair", () =>
    Effect.gen(function* () {
      const h = harness(async () => "ahead");
      expect(yield* h.relation(BASE, HEAD)).toEqual({
        repository: "Jones-Systems/Jones-Code",
        base: BASE,
        head: HEAD,
        relation: "ahead",
        observedAt: "2026-10-20T12:00:00.000Z",
      });
      expect(h.calls).toEqual([[BASE, HEAD]]);
    }),
  );

  it.effect(
    "answers identical for one commit and unknown for malformed commits without GitHub",
    () =>
      Effect.gen(function* () {
        const h = harness(async () => "diverged");
        expect((yield* h.relation(BASE, BASE)).relation).toBe("identical");
        expect((yield* h.relation(BASE.slice(0, 7), HEAD)).relation).toBe("unknown");
        expect((yield* h.relation(BASE, HEAD.toUpperCase())).relation).toBe("unknown");
        expect(h.calls).toEqual([]);
      }),
  );

  it.effect(
    "caches proven relations per ordered pair and keeps the original observation time",
    () =>
      Effect.gen(function* () {
        const h = harness(async (base) => (base === BASE ? "ahead" : "behind"));
        yield* h.relation(BASE, HEAD);
        h.advance(60_000);
        expect(yield* h.relation(BASE, HEAD)).toMatchObject({
          relation: "ahead",
          observedAt: "2026-10-20T12:00:00.000Z",
        });
        expect((yield* h.relation(HEAD, BASE)).relation).toBe("behind");
        expect(h.calls).toEqual([
          [BASE, HEAD],
          [HEAD, BASE],
        ]);
      }),
  );

  it.effect("evicts the least recently used relation beyond the bound", () =>
    Effect.gen(function* () {
      const h = harness(async () => "ahead", { cacheLimit: 2 });
      yield* h.relation(BASE, HEAD);
      yield* h.relation(BASE, OTHER);
      yield* h.relation(BASE, HEAD);
      yield* h.relation(HEAD, OTHER);
      expect(h.calls).toHaveLength(3);
      yield* h.relation(BASE, HEAD);
      expect(h.calls).toHaveLength(3);
      yield* h.relation(BASE, OTHER);
      expect(h.calls).toHaveLength(4);
    }),
  );

  it.effect("shares one in-flight lookup between concurrent requests for a pair", () =>
    Effect.gen(function* () {
      const started = Promise.withResolvers<void>();
      const released = Promise.withResolvers<JonesSourceRelationKind>();
      const h = harness(() => {
        started.resolve();
        return released.promise;
      });
      const first = yield* Effect.forkChild(h.relation(BASE, HEAD));
      const second = yield* Effect.forkChild(h.relation(BASE, HEAD));
      yield* Effect.promise(() => started.promise);
      yield* Effect.yieldNow;
      expect(h.calls).toHaveLength(1);
      released.resolve("diverged");
      expect((yield* Fiber.join(first)).relation).toBe("diverged");
      expect((yield* Fiber.join(second)).relation).toBe("diverged");
      expect(h.calls).toHaveLength(1);
    }),
  );

  it.effect("backs off briefly after a failure instead of caching unknown", () =>
    Effect.gen(function* () {
      let fail = true;
      const h = harness(async () => {
        if (fail) throw new JonesActionsError("gh-missing");
        return "behind";
      });
      expect((yield* h.relation(BASE, HEAD)).relation).toBe("unknown");
      fail = false;
      h.advance(29_999);
      expect((yield* h.relation(BASE, HEAD)).relation).toBe("unknown");
      expect(h.calls).toHaveLength(1);
      h.advance(1);
      expect((yield* h.relation(BASE, HEAD)).relation).toBe("behind");
      expect(h.calls).toHaveLength(2);
    }),
  );

  it.effect("reports a stalled lookup as unknown within its timeout", () =>
    Effect.gen(function* () {
      const h = harness(() => new Promise<never>(() => {}), { timeoutMs: 5 });
      expect((yield* h.relation(BASE, HEAD)).relation).toBe("unknown");
      h.advance(30_000);
      expect((yield* h.relation(BASE, HEAD)).relation).toBe("unknown");
      expect(h.calls).toHaveLength(2);
    }),
  );

  it.effect("caps distinct concurrent lookups", () =>
    Effect.gen(function* () {
      const h = harness(() => new Promise<never>(() => {}), { timeoutMs: 50 });
      const pairs = ["1", "2", "3", "4", "5"].map((digit) => digit.repeat(40));
      const results = yield* Effect.all(
        pairs.map((head) => h.relation(BASE, head)),
        { concurrency: "unbounded" },
      );
      expect(results.every((result) => result.relation === "unknown")).toBe(true);
      expect(h.calls).toHaveLength(4);
    }),
  );
});
