import type { ProviderDriverKind, ServerProvider } from "@t3tools/contracts";

import type { ProviderInstance } from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";

/**
 * Stamp instance identity onto a `ServerProvider` snapshot produced by the
 * snapshot helpers. Drivers pipe drafts through this stamper before publishing.
 * Already-bound quota retains its object identity and private probe proof.
 * Once `buildServerProvider` accepts `instanceId`/`driver`, this wrapper disappears.
 */
export const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly driverKind: ProviderDriverKind;
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    ...(snapshot.qualifiedQuota && snapshot.qualifiedQuota.instanceId !== input.instanceId
      ? { qualifiedQuota: { ...snapshot.qualifiedQuota, instanceId: input.instanceId } }
      : {}),
    instanceId: input.instanceId,
    driver: input.driverKind,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });
