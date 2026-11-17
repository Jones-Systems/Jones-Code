import { expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { EffectClaimLost } from "../workspaceLifecycle/ActiveEffectClaim.ts";
import { RecreationHold } from "./Native.ts";
import { classify, protect } from "./PreSessionFailure.ts";

class SyntheticFailure extends Schema.TaggedError<SyntheticFailure>()(
  "SyntheticPreSessionFailure",
  {
    detail: Schema.String,
  },
) {}

it.effect("only positive source refusal evidence permits recreation settlement", () =>
  Effect.gen(function* () {
    const safe = new RecreationHold(
      "source_unavailable",
      "private",
      "owner",
      "private diagnostic",
      "private action",
    );
    const unsafe = [
      new RecreationHold(
        "unknown",
        "private",
        "owner",
        "neither terminal receipt nor exact stale registration is available",
        "inspect",
      ),
      { _tag: "RecreationHold", reason: "source_unavailable" },
      new Error("source_unavailable"),
    ];
    for (const error of unsafe) expect(classify("recreation", error)).toBeUndefined();
    expect(classify("recreation", new Error("wrapped", { cause: safe }))?.failure.code).toBe(
      "checkout_source_unavailable",
    );
    expect(classify("preflight", safe)).toBeUndefined();
    let settlements = 0;
    for (const [willRetry, nativeGuarded] of [
      [true, false],
      [false, true],
    ] as const) {
      const result = yield* Effect.result(
        protect("recreation", Effect.fail(safe), {
          willRetry,
          nativeGuarded,
          settle: () =>
            Effect.sync(() => {
              settlements++;
            }),
        }),
      );
      expect(result).toMatchObject({ _tag: "Failure", failure: safe });
    }
    expect(settlements).toBe(0);
  }),
);

it.effect(
  "preflight preserves interruption, defects, claim loss and provider-control uncertainty",
  () =>
    Effect.gen(function* () {
      let settlements = 0;
      const options = {
        willRetry: false,
        nativeGuarded: false,
        settle: () =>
          Effect.sync(() => {
            settlements++;
          }),
      };
      const interrupted = yield* Effect.exit(protect("preflight", Effect.interrupt, options));
      expect(interrupted._tag).toBe("Failure");
      if (interrupted._tag === "Failure")
        expect(Cause.hasInterruptsOnly(interrupted.cause)).toBe(true);
      const defect = new Error("unexpected defect");
      const died = yield* Effect.exit(protect("preflight", Effect.die(defect), options));
      expect(died._tag).toBe("Failure");
      if (died._tag === "Failure") expect(Cause.hasDies(died.cause)).toBe(true);
      for (const error of [
        new EffectClaimLost({ effectId: "lost" }),
        new ProviderControlOutcomeUnknown({
          driver: "claudeAgent",
          providerSessionId: "session",
          nativeThreadId: "native",
          operation: "retire",
        }),
      ]) {
        const wrapped = new Error("wrapped", { cause: error });
        expect(
          yield* Effect.result(protect("preflight", Effect.fail(wrapped), options)),
        ).toMatchObject({ _tag: "Failure", failure: wrapped });
      }
      expect(settlements).toBe(0);
    }),
);

it.effect("failed persistence propagates without claiming durable settlement", () =>
  Effect.gen(function* () {
    const storage = new SyntheticFailure({ detail: "settlement write unavailable" });
    const result = yield* Effect.result(
      protect("preflight", Effect.fail(new SyntheticFailure({ detail: "preflight" })), {
        willRetry: false,
        nativeGuarded: false,
        settle: () => Effect.fail(storage),
      }),
    );
    expect(result).toMatchObject({ _tag: "Failure", failure: storage });
  }),
);
