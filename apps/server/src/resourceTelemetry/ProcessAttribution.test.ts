import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";

import * as ProcessAttribution from "./ProcessAttribution.ts";

describe("ProcessAttribution", () => {
  it.effect("keeps the newest scoped owner when an older registration closes", () =>
    Effect.gen(function* () {
      const attribution = yield* ProcessAttribution.make();
      const firstScope = yield* Scope.make();
      const secondScope = yield* Scope.make();

      yield* attribution
        .registerProviderRoot({ pid: 42, threadId: "thread-1", provider: "codex" })
        .pipe(Effect.provideService(Scope.Scope, firstScope));
      yield* attribution
        .registerProviderRoot({ pid: 42, threadId: "thread-2", provider: "codex" })
        .pipe(Effect.provideService(Scope.Scope, secondScope));

      yield* Scope.close(firstScope, Exit.void);
      expect((yield* attribution.snapshot).get(42)?.owner.threadId).toBe("thread-2");

      yield* Scope.close(secondScope, Exit.void);
      expect((yield* attribution.snapshot).has(42)).toBe(false);
    }),
  );
});
