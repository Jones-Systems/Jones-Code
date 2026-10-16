import { useAtomValue } from "@effect/atom-react";
import { useMemo } from "react";
import { useEnvironmentIdentities, useEnvironmentMachines } from "../../state/environments";
import { excludedSidebarEnvironmentIds, sidebarEnvironmentExclusionKey } from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

export function useSidebarEnvironmentFilter() {
  const identities = useEnvironmentIdentities();
  const machines = useEnvironmentMachines();
  const filters = useAtomValue(sidebarEnvironmentFiltersAtom);
  const excludedEnvironmentIds = useMemo(
    () => excludedSidebarEnvironmentIds(identities, filters, machines),
    [identities, filters, machines],
  );
  return {
    excludedEnvironmentIds,
    excludedEnvironmentKey: sidebarEnvironmentExclusionKey(excludedEnvironmentIds),
  };
}
