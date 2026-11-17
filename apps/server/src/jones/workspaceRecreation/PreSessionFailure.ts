import * as Effect from "effect/Effect";
import { findProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { findEffectClaimLost } from "../workspaceLifecycle/ActiveEffectClaim.ts";
import { RecreationHold } from "./Native.ts";

export type Operation = "preflight" | "recreation";
export interface Notice {
  readonly signal: string;
  readonly title: string;
  readonly failure: {
    readonly class: "provider_error";
    readonly code: "checkout_source_unavailable" | "workspace_preflight_failed";
    readonly message: string;
    readonly retryable: false;
  };
}

function recreationHold(error: unknown): RecreationHold | undefined {
  const visited = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !visited.has(current)) {
    visited.add(current);
    if (current instanceof RecreationHold) return current;
    current = "cause" in current ? current.cause : undefined;
  }
  return undefined;
}

export function classify(operation: Operation, error: unknown): Notice | undefined {
  if (
    findEffectClaimLost(error) !== undefined ||
    findProviderControlOutcomeUnknown(error) !== undefined
  )
    return undefined;
  const hold = recreationHold(error);
  if (operation === "recreation") {
    // Only the source observer can prove this refusal preceded reconstruction.
    // Other recreation errors may describe an earlier unresolved external effect.
    if (hold?.reason !== "source_unavailable") return undefined;
    return {
      signal: "checkout-source-unavailable",
      title: "Checkout unavailable",
      failure: {
        class: "provider_error",
        code: "checkout_source_unavailable",
        message:
          "The checkout is unavailable and its original identity could not be verified. This turn did not start.",
        retryable: false,
      },
    };
  }
  if (hold !== undefined) return undefined;
  return {
    signal: "workspace-preflight-failed",
    title: "Workspace preparation failed",
    failure: {
      class: "provider_error",
      code: "workspace_preflight_failed",
      message: "The workspace could not be prepared. This turn did not start.",
      retryable: false,
    },
  };
}

// Call only around the named pre-session reads or recreation, never lifecycle
// transitions, native creation, or the subsequently returned provider-turn effect.
export function protect<A, E, R, E2, R2>(
  operation: Operation,
  effect: Effect.Effect<A, E, R>,
  options: {
    readonly willRetry: boolean;
    readonly nativeGuarded: boolean;
    readonly settle: (notice: Notice) => Effect.Effect<void, E2, R2>;
  },
): Effect.Effect<A, E | E2, R | R2> {
  return effect.pipe(
    Effect.catch((error) => {
      const notice =
        options.willRetry || options.nativeGuarded ? undefined : classify(operation, error);
      return notice === undefined
        ? Effect.fail(error)
        : options.settle(notice).pipe(Effect.andThen(Effect.fail(error)));
    }),
  );
}
