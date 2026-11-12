import { useAtomValue } from "@effect/atom-react";

import { useEnvironmentIdentities, useEnvironmentMachines } from "../../state/environments";
import { excludedSidebarEnvironmentIds, sidebarEnvironmentExclusionKey } from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

export function useSidebarEnvironmentFilter() {
  const identities = useEnvironmentIdentities();
  const machines = useEnvironmentMachines();
  const filters = useAtomValue(sidebarEnvironmentFiltersAtom);
  const excludedEnvironmentIds = excludedSidebarEnvironmentIds(identities, filters, machines);
  return {
    excludedEnvironmentIds,
    excludedEnvironmentKey: sidebarEnvironmentExclusionKey(excludedEnvironmentIds),
  };
}
