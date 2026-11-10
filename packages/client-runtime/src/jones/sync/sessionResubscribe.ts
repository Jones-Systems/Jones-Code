import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

/** How a durable subscription ended while its session may still be current. */
export type SessionStreamEnd = "completed" | "transport-lost" | "interrupted" | "defect";

const MAX_RESUBSCRIBE_DELAY_MS = 30_000;
// A stream that stayed open this long was healthy, so its next ending restarts the
// backoff. Resetting on the first value instead would let a stream that emits and then
// fails deterministically resubscribe at the base delay forever.
const HEALTHY_STREAM_MS = 30_000;

/**
 * Classifies a failed stream that may be resubscribed on its still-current session.
 * Domain failures return `undefined` and keep the caller's existing handling.
 * An interrupt-only cause here came from the server's stream exit, not this fiber:
 * a local interruption also cancels the wait in `after`, so it never resubscribes.
 */
export function failedSessionStreamEnd(
  cause: Cause.Cause<unknown>,
  isTransportFailure: boolean,
): SessionStreamEnd | undefined {
  if (isTransportFailure) return "transport-lost";
  if (Cause.hasInterruptsOnly(cause)) return "interrupted";
  if (Cause.hasDies(cause)) return "defect";
  return undefined;
}

export interface SessionResubscriber {
  /** Records when one subscription attempt opened its stream. */
  readonly opened: Effect.Effect<void>;
  /**
   * Waits with backoff, records the ending, then continues with `next` only while the
   * same session is still current. A replacement session interrupts the wait instead.
   */
  readonly after: <A, E>(
    end: SessionStreamEnd,
    next: () => Stream.Stream<A, E>,
    cause?: Cause.Cause<unknown>,
  ) => Stream.Stream<A, E>;
}

export function makeSessionResubscriber(input: {
  readonly method: string;
  readonly environmentId: string;
  readonly delay: Duration.Input;
  readonly isSessionCurrent: () => boolean;
}): SessionResubscriber {
  let attempts = 0;
  let openedAt: number | undefined;
  return {
    opened: Clock.currentTimeMillis.pipe(
      Effect.map((now) => {
        openedAt = now;
      }),
    ),
    after: (end, next, cause) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const endedAt = yield* Clock.currentTimeMillis;
          if (openedAt !== undefined && endedAt - openedAt >= HEALTHY_STREAM_MS) attempts = 0;
          const delayMs = Math.min(
            Duration.toMillis(input.delay) * 2 ** attempts,
            MAX_RESUBSCRIBE_DELAY_MS,
          );
          attempts += 1;
          const attempt = attempts;
          yield* Effect.sleep(Duration.millis(delayMs));
          const sessionIsCurrent = input.isSessionCurrent();
          yield* Effect.logWarning("Durable RPC subscription ended on a live session.", {
            method: input.method,
            environmentId: input.environmentId,
            end,
            retryDelayMs: delayMs,
            resubscribing: sessionIsCurrent,
            ...(cause === undefined ? {} : { cause: Cause.pretty(cause) }),
          }).pipe(
            Effect.withSpan("EnvironmentRpc.subscriptionEnded", {
              attributes: {
                "rpc.method": input.method,
                "environment.id": input.environmentId,
                "subscription.end_reason": end,
                "subscription.retry_delay_ms": delayMs,
                "subscription.retry_attempt": attempt,
                "subscription.session_current": sessionIsCurrent,
              },
            }),
          );
          return sessionIsCurrent ? next() : Stream.empty;
        }),
      ),
  };
}
