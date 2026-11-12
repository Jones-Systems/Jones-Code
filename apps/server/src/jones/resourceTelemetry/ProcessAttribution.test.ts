import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";

import * as ProcessAttribution from "./ProcessAttribution.ts";

describe("ProcessAttribution", () => {
  it.effect("keeps the newest scoped owner when an older registration closes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const attribution = yield* ProcessAttribution.make();
        const firstScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );
        const secondScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );

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
    ),
  );
  it.effect("keeps exact-client launcher evidence distinct and fences delayed finalizers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* ProcessAttribution.make();
        const client = {};
        const first = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );
        const second = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );
        const identity = {
          platform: "linux" as const,
          bootId: "boot",
          pidNamespace: "pid:[1]",
          pid: 42,
          startTime: "1",
          startTimeUnit: "linux-clock-ticks-since-boot" as const,
          executableIdentity: "1:2:3:4",
          build: "unknown" as const,
        };
        yield* owner.bindLauncher(client, null).pipe(Effect.provideService(Scope.Scope, first));
        yield* owner
          .bindLauncher(client, identity)
          .pipe(Effect.provideService(Scope.Scope, second));
        yield* Scope.close(first, Exit.void);
        expect(yield* owner.launcherFor(client)).toEqual(identity);
        expect(yield* owner.launcherFor({})).toBeNull();
        yield* Scope.close(second, Exit.void);
        expect(yield* owner.launcherFor(client)).toBeNull();
      }),
    ),
  );
});
