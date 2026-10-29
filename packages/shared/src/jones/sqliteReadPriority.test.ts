import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ReadPriority, withReadPriority } from "./sqliteReadPriority.ts";

it.effect("scopes priority overrides without leaking between concurrent readers", () =>
  Effect.gen(function* () {
    expect(yield* ReadPriority).toBe("interactive");
    const priorities = yield* Effect.all(
      [
        Effect.gen(function* () {
          expect(yield* ReadPriority).toBe("background");
          const nested = yield* ReadPriority.pipe(withReadPriority("interactive"));
          expect(nested).toBe("interactive");
          return yield* ReadPriority;
        }).pipe(withReadPriority("background")),
        ReadPriority,
      ],
      { concurrency: "unbounded" },
    );
    expect(priorities).toEqual(["background", "interactive"]);
    expect(yield* ReadPriority).toBe("interactive");
  }),
);
