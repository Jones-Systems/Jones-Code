import { TrustedT3PlacementEnvironment, type ServerConfig } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { T3PlacementTrustProvider } from "../../environment/NativePlacementTrust.ts";
import * as NativeStoreAuthority from "../../environment/NativeStoreAuthority.ts";

type NativeStoreAuthorityConfig = Pick<ServerConfig, "nativeStoreAuthority">;

const isDescriptor = Schema.is(TrustedT3PlacementEnvironment);

/**
 * Uses the same synchronous trust read as placement responses rather than
 * `readCurrent`, which would run a desktop owner challenge on every config read.
 * Anything short of one ready descriptor for this environment omits the key:
 * absence means unknown, so nothing is inferred or fabricated.
 */
export const nativeStoreAuthorityConfig = (
  trustProvider: T3PlacementTrustProvider,
  environmentId: string,
): NativeStoreAuthorityConfig => {
  let snapshot: ReturnType<T3PlacementTrustProvider["readTrustSnapshot"]>;
  try {
    snapshot = trustProvider.readTrustSnapshot();
  } catch {
    return {};
  }
  if (snapshot.readiness !== "ready" || snapshot.trustedEnvironments.length !== 1) return {};
  const [descriptor] = snapshot.trustedEnvironments;
  if (!isDescriptor(descriptor) || descriptor.environmentId !== environmentId) return {};
  return {
    nativeStoreAuthority: {
      environmentId: descriptor.environmentId,
      authorityNamespace: descriptor.authorityNamespace,
      storeGeneration: descriptor.storeGeneration,
    },
  };
};

export type NativeStoreAuthorityConfigReader = (
  environmentId: string,
) => Effect.Effect<NativeStoreAuthorityConfig>;

/** Binds the server's native store authority once, for per-connection config reads. */
export const makeNativeStoreAuthorityConfigReader = Effect.map(
  NativeStoreAuthority.NativeStoreAuthority,
  (authority): NativeStoreAuthorityConfigReader =>
    (environmentId) =>
      Effect.sync(() => nativeStoreAuthorityConfig(authority.trustProvider, environmentId)),
);
