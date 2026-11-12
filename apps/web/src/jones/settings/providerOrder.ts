import { defaultInstanceIdForDriver, type ProviderInstanceConfig } from "@t3tools/contracts";

export function reorderProviderInstance(
  instances: Readonly<Record<string, ProviderInstanceConfig>>,
  activeId: string,
  overId: string,
): Readonly<Record<string, ProviderInstanceConfig>> {
  const active = instances[activeId];
  const over = instances[overId];
  if (
    !active ||
    !over ||
    activeId === overId ||
    active.driver !== over.driver ||
    activeId === defaultInstanceIdForDriver(active.driver) ||
    overId === defaultInstanceIdForDriver(over.driver)
  )
    return instances;
  const entries = Object.entries(instances);
  const isPeer = ([id, candidate]: [string, ProviderInstanceConfig]) =>
    candidate.driver === active.driver && id !== defaultInstanceIdForDriver(candidate.driver);
  const peers = entries.filter(isPeer);
  const from = peers.findIndex(([id]) => id === activeId);
  const to = peers.findIndex(([id]) => id === overId);
  const [moved] = peers.splice(from, 1);
  peers.splice(to, 0, moved!);
  let index = 0;
  return Object.fromEntries(entries.map((entry) => (isPeer(entry) ? peers[index++]! : entry)));
}
