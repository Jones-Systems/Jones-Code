import type { ProviderRuntimeBinding } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";

export type CurrentThreadRuntimeAttachment =
  | {
      readonly status: "attached";
      readonly binding: ProviderRuntimeBinding;
      readonly evidenceRevision: number | null;
      readonly observedAt: string;
      readonly isCurrent: Effect.Effect<boolean>;
      // A session lease validates reads; it cannot authorize physical stop or native creation.
      readonly physicalIncarnation: {
        readonly status: "unknown";
        readonly reason: "physical_incarnation_capture_unavailable";
      };
    }
  | {
      readonly status: "stopped";
      readonly observedAt: string;
      readonly isCurrent: Effect.Effect<boolean>;
    }
  | { readonly status: "unknown"; readonly observedAt: string; readonly reason: string };
