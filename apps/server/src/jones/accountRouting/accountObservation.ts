import type { RuntimeMode, ServerProviderModel } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import type * as Effect from "effect/Effect";

import type { AccountAllowance } from "./policy.ts";

interface AccountObservation {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly allowance: (model: string, now: number) => AccountAllowance | undefined;
}

type ObserveAccount = (
  cwd: string,
  runtimeMode: RuntimeMode,
) => Effect.Effect<AccountObservation | undefined>;

// The adapter is unique to a live driver lifetime and survives registry presentation wrapping.
const observers = new WeakMap<ProviderInstance["orchestrationAdapter"], ObserveAccount>();

export const registerAccountObserver = (
  adapter: ProviderInstance["orchestrationAdapter"],
  observe: ObserveAccount,
) => observers.set(adapter, observe);

export const accountObserverFor = (instance: ProviderInstance) =>
  observers.get(instance.orchestrationAdapter);
