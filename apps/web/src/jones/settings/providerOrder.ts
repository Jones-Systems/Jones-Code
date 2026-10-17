import { defaultInstanceIdForDriver, type ProviderInstanceConfig } from "@t3tools/contracts";

export type ProviderOrderDirection = "up" | "down";

export function moveProviderInstance(
  instances: Readonly<Record<string, ProviderInstanceConfig>>,
  instanceId: string,
  direction: ProviderOrderDirection,
): Readonly<Record<string, ProviderInstanceConfig>> {
  const instance = instances[instanceId];
  if (!instance || instanceId === defaultInstanceIdForDriver(instance.driver)) return instances;
  const entries = Object.entries(instances);
  const peers = entries.filter(
    ([id, candidate]) =>
      candidate.driver === instance.driver && id !== defaultInstanceIdForDriver(candidate.driver),
  );
  const index = peers.findIndex(([id]) => id === instanceId);
  const neighbor = peers[index + (direction === "up" ? -1 : 1)];
  if (!neighbor) return instances;
  const sourceIndex = entries.findIndex(([id]) => id === instanceId);
  const targetIndex = entries.findIndex(([id]) => id === neighbor[0]);
  [entries[sourceIndex], entries[targetIndex]] = [entries[targetIndex]!, entries[sourceIndex]!];
  return Object.fromEntries(entries);
}
