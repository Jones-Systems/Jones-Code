import { ProviderRuntimeBinding } from "@t3tools/contracts";
import type {
  CapturedRuntimeRetirement,
  ProviderAdapterV2Error,
  RuntimeRetirementReceipt,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);

export function makeProviderRecoveryRetirements() {
  const observers: Array<{
    readonly binding: ProviderRuntimeBinding;
    readonly completion: Deferred.Deferred<RuntimeRetirementReceipt>;
    readonly handle: CapturedRuntimeRetirement;
  }> = [];
  const find = (binding: ProviderRuntimeBinding) =>
    observers.find((entry) => equivalentBinding(entry.binding, binding));
  return {
    retain: (
      binding: ProviderRuntimeBinding,
      cleanup: Effect.Effect<void, ProviderAdapterV2Error>,
    ): CapturedRuntimeRetirement => {
      const existing = find(binding);
      if (existing !== undefined) return existing.handle;
      const completion = Deferred.makeUnsafe<RuntimeRetirementReceipt>();
      const handle = {
        binding,
        awaitRetired: Deferred.await(completion).pipe(
          Effect.flatMap((receipt) => cleanup.pipe(Effect.as(receipt))),
        ),
      };
      observers.push({ binding, completion, handle });
      return handle;
    },
    capture: (binding: ProviderRuntimeBinding) => find(binding)?.handle ?? null,
    complete: (receipt: RuntimeRetirementReceipt) => {
      const observer = find(receipt.binding);
      return observer === undefined
        ? Effect.void
        : Deferred.succeed(observer.completion, receipt).pipe(Effect.asVoid);
    },
  };
}
