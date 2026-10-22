import type { ProviderDriverKind, ProviderInstanceId, ServerProvider } from "@t3tools/contracts";
import { resolveProviderInstanceDisplayName } from "@t3tools/client-runtime/state/provider-instance-display";

export function subagentAccountDisplayName(
  providers:
    | ReadonlyArray<Pick<ServerProvider, "instanceId" | "driver" | "displayName">>
    | null
    | undefined,
  instanceId: ProviderInstanceId,
  driver: ProviderDriverKind | undefined,
): string | undefined {
  // Remote children carry their parent's instance ID, not a local account identity.
  if (!driver || driver === "remote") return undefined;
  const provider = providers?.find((entry) => entry.instanceId === instanceId);
  return provider?.driver === driver ? resolveProviderInstanceDisplayName(provider) : undefined;
}
