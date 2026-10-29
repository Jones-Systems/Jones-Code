import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import {
  NativeProviderDispatchAdmission,
  NativeProviderDispatchRefused,
} from "./dispatchAdmission.ts";
import { makeWorkstreamsNativeProvider } from "./service.ts";
import * as Test from "./testFixtures.ts";

it.effect("checks original caller after durable dispatch claiming, before the engine", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(Test.now));
    const f = Test.makeProviderFixture();
    let current = true;
    let checked = 0;
    const provider = makeWorkstreamsNativeProvider(
      {
        ...f.ports,
        attempts: {
          ...f.ports.attempts,
          startDispatch: (request, at) =>
            f.ports.attempts.startDispatch(request, at).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  current = false;
                }),
              ),
            ),
        },
      },
      "v2",
    );
    const result = yield* provider.settle(Test.binding, Test.request, Test.requestBytesSha256).pipe(
      Effect.provideService(NativeProviderDispatchAdmission, (binding, request) =>
        Effect.gen(function* () {
          checked++;
          expect(binding).toBe(Test.binding);
          expect(request).toBe(Test.request);
          if (!current) return yield* Effect.fail(new NativeProviderDispatchRefused());
        }),
      ),
    );
    expect(checked).toBe(1);
    expect(result).toMatchObject({ state: "rejected", reason: "forbidden" });
    expect(f.calls).toEqual([]);
    expect(f.attempts.get(f.key(Test.request))?.dispatchStartedAt).not.toBeNull();
    // Losing this first dispatch opportunity does not turn a subsequent lookup into dispatch.
    const retained = yield* provider.lookup(Test.binding, Test.request, Test.requestBytesSha256);
    expect(retained.state).toBe("unknown");
    expect(f.calls).toEqual([]);
  }),
);

it.effect("retains existing provider behavior without a request-local peer admission", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(Test.now));
    const f = Test.makeProviderFixture();
    const provider = makeWorkstreamsNativeProvider(f.ports, "v2");
    expect(
      (yield* provider.settle(Test.binding, Test.request, Test.requestBytesSha256)).state,
    ).toBe("terminal");
    expect(f.calls).toHaveLength(1);
    expect(
      (yield* provider.lookup(Test.binding, Test.request, Test.requestBytesSha256)).state,
    ).toBe("terminal");
    expect(f.calls).toHaveLength(1);
  }),
);

it.effect("observing an existing attempt never consumes another dispatch admission", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(Test.now));
    const f = Test.makeProviderFixture();
    const provider = makeWorkstreamsNativeProvider(f.ports, "v2");
    let checked = 0;
    const admitted = (binding: typeof Test.binding, request: typeof Test.request) =>
      Effect.sync(() => {
        checked++;
        expect(binding).toBe(Test.binding);
        expect(request).toBe(Test.request);
      });
    yield* provider
      .settle(Test.binding, Test.request, Test.requestBytesSha256)
      .pipe(Effect.provideService(NativeProviderDispatchAdmission, admitted));
    yield* provider
      .settle(Test.binding, Test.request, Test.requestBytesSha256)
      .pipe(
        Effect.provideService(NativeProviderDispatchAdmission, () =>
          Effect.die("Replay must not dispatch"),
        ),
      );
    expect(checked).toBe(1);
    expect(f.calls).toHaveLength(1);
  }),
);
