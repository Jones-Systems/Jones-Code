import * as Effect from "effect/Effect";
import type { CompanionHostUnavailable } from "./CompanionHostRegistry.ts";

// Preserve the typed placement error across ServerBrowser's existing Promise boundary.
export const runPlacement = <A>(effect: Effect.Effect<A, CompanionHostUnavailable>): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.match({
        onSuccess: (value) => ({ ok: true as const, value }),
        onFailure: (error) => ({ ok: false as const, error }),
      }),
    ),
  ).then((result) => {
    if (!result.ok) throw result.error;
    return result.value;
  });
