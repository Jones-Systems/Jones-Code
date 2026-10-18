import type { UsageProviderKind } from "@t3tools/contracts";
import type { MergedUsage } from "@t3tools/shared/usageMerge";
import { useEffect, useMemo, useState } from "react";

import { selectUsageBreakdown } from "../../components/usage/usageBreakdown";
import { providersWithUsage } from "../../components/usage/usageProviders";

export function useUsageProviderDetails(merged: MergedUsage, isPending: boolean) {
  const [selectedProvider, setSelectedProvider] = useState<UsageProviderKind | null>(null);
  const [expandedModelKey, setExpandedModelKey] = useState<string | null>(null);
  const detailBreakdown = useMemo(
    () => selectUsageBreakdown(merged, selectedProvider),
    [merged, selectedProvider],
  );
  const focusedProvider = detailBreakdown.providerTotals?.provider ?? null;
  const activeProviders = useMemo(() => providersWithUsage(merged.providers), [merged.providers]);
  useEffect(() => {
    if (!isPending && selectedProvider !== null && !activeProviders.includes(selectedProvider)) {
      setSelectedProvider(null);
      setExpandedModelKey(null);
    }
  }, [activeProviders, isPending, selectedProvider]);
  const selectProvider = (provider: UsageProviderKind | null) => {
    setSelectedProvider(provider);
    setExpandedModelKey(null);
  };
  return {
    detailBreakdown,
    focusedProvider,
    activeProviders,
    expandedModelKey,
    setExpandedModelKey,
    selectProvider,
  };
}
