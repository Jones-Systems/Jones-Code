import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { LinkedPrObservationResult, LinkedPrObservationRequest } from "./githubPrObserver.ts";
import { LinkedPrObserver } from "./githubPrObserver.ts";
import { ProviderError } from "./localAttestation.ts";

export interface SyntheticPrScenario {
  readonly source_instance_id: string;
  readonly locator: LinkedPrObservationRequest["locator"];
  readonly result: LinkedPrObservationResult;
}

// Synthetic observations are explicit fixtures; they never claim native thread attestation.
export const syntheticGitHubLayer = (scenarios: ReadonlyArray<SyntheticPrScenario>) =>
  Layer.succeed(LinkedPrObserver, {
    observe: Effect.fn("SyntheticLinkedPrObserver.observe")(function* (input) {
      const scenario = scenarios.find(
        (entry) =>
          entry.source_instance_id === input.source_instance_id &&
          entry.locator.host === input.locator.host &&
          entry.locator.repository_owner === input.locator.repository_owner &&
          entry.locator.repository_name === input.locator.repository_name &&
          entry.locator.number === input.locator.number,
      );
      if (scenario === undefined)
        return yield* new ProviderError({ code: "provider_unconfigured" });
      return scenario.result;
    }),
  });
