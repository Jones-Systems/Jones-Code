import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ServiceLauncherClientError } from "../../cloud/serviceLauncherClient.ts";
import { make } from "./UpdateAdmissionFence.ts";
import { launcherHandoffAttempt } from "./launcherHandoff.ts";

it.effect("defers busy work without sending IPC and reopens the same admission fence", () =>
  Effect.gen(function* () {
    const fence = yield* make;
    yield* fence.attach;
    let calls = 0;
    expect(
      yield* launcherHandoffAttempt({
        fence,
        quiet: Effect.succeed(false),
        request: Effect.sync(() => {
          calls++;
          return "accepted" as const;
        }),
        reopened: Effect.void,
      }),
    ).toBe(true);
    expect(calls).toBe(0);
    const ticket = yield* fence.raise("manual");
    yield* ticket.release;
  }).pipe(Effect.scoped),
);

it.effect("keeps accepted and unknown handoffs fenced, but releases a definitive deferral", () =>
  Effect.gen(function* () {
    for (const outcome of ["accepted", "deferred", "unavailable", "unknown"] as const) {
      const fence = yield* make;
      yield* fence.attach;
      const request =
        outcome === "unknown"
          ? Effect.fail(new ServiceLauncherClientError({ operation: "timeout" }))
          : Effect.succeed(outcome);
      const again = yield* launcherHandoffAttempt({
        fence,
        quiet: Effect.succeed(true),
        request,
        reopened: Effect.void,
      });
      expect(again).toBe(outcome === "deferred");
      if (outcome === "accepted" || outcome === "unknown") {
        const refusal = yield* fence.raise("manual").pipe(Effect.flip);
        expect(refusal._tag).toBe("UpdateAdmissionBusy");
      } else {
        const ticket = yield* fence.raise("manual");
        yield* ticket.release;
      }
    }
  }).pipe(Effect.scoped),
);

it.effect("does not request a handoff through an unattached or already occupied fence", () =>
  Effect.gen(function* () {
    const fence = yield* make;
    let calls = 0;
    const attempt = launcherHandoffAttempt({
      fence,
      quiet: Effect.succeed(true),
      request: Effect.sync(() => {
        calls++;
        return "accepted" as const;
      }),
      reopened: Effect.void,
    });
    expect(yield* attempt).toBe(true);
    yield* fence.attach;
    const occupied = yield* fence.raise("manual");
    expect(yield* attempt).toBe(true);
    expect(calls).toBe(0);
    yield* occupied.release;
  }).pipe(Effect.scoped),
);
