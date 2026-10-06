import type { TrustedT3PlacementEnvironment } from "@t3tools/contracts";

// Only a native authority may provide placement trust; registry responses and clients cannot.
export interface T3PlacementTrustProvider {
  readonly readTrustSnapshot: () => {
    readonly trustedEnvironments: readonly TrustedT3PlacementEnvironment[];
    readonly readiness: "ready" | "trust-provider-required";
  };
}
