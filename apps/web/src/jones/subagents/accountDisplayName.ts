import type { ProviderDriverKind, ServerProvider } from "@t3tools/contracts";
import { resolveProviderInstanceDisplayName } from "@t3tools/client-runtime/state/provider-instance-display";

export function subagentAccountDisplayName(
  provider: Pick<ServerProvider, "instanceId" | "driver" | "displayName"> | undefined,
  driver: ProviderDriverKind | undefined,
): string | undefined {
  // Remote children carry their parent's instance ID, not a local account identity.
  if (!driver || driver === "remote") return undefined;
  return provider?.driver === driver ? resolveProviderInstanceDisplayName(provider) : undefined;
}
