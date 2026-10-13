import { useAtomValue } from "@effect/atom-react";
import { useMemo } from "react";
import { useEnvironmentIdentities } from "../../state/environments";
import { excludedSidebarEnvironmentIds, sidebarEnvironmentExclusionKey } from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

export function useSidebarEnvironmentFilter() {
  const identities = useEnvironmentIdentities();
  const filters = useAtomValue(sidebarEnvironmentFiltersAtom);
  const excludedEnvironmentIds = useMemo(
    () => excludedSidebarEnvironmentIds(identities, filters),
    [identities, filters],
  );
  return {
    excludedEnvironmentIds,
    excludedEnvironmentKey: sidebarEnvironmentExclusionKey(excludedEnvironmentIds),
  };
}
