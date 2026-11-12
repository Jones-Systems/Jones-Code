import type { UsageProviderKind } from "@t3tools/contracts";
import type { MergedUsage } from "@t3tools/shared/usageMerge";
import { useState } from "react";

import { selectUsageBreakdown } from "../../components/usage/usageBreakdown";
import { providersWithUsage } from "../../components/usage/usageProviders";

export function useUsageProviderDetails(merged: MergedUsage, isPending: boolean, scope = "all") {
  const [selection, setSelection] = useState<{
    scope: string;
    provider: UsageProviderKind | null;
    expandedModelKey: string | null;
  }>({ scope, provider: null, expandedModelKey: null });
  const activeProviders = providersWithUsage(merged.providers);
  const shouldReset =
    selection.scope !== scope ||
    (!isPending && selection.provider !== null && !activeProviders.includes(selection.provider));
  const selectedProvider = shouldReset ? null : selection.provider;
  const expandedModelKey = shouldReset ? null : selection.expandedModelKey;
  if (shouldReset) {
    setSelection({ scope, provider: null, expandedModelKey: null });
  }
  const detailBreakdown = selectUsageBreakdown(merged, selectedProvider);
  const focusedProvider = detailBreakdown.providerTotals?.provider ?? null;
  const selectProvider = (provider: UsageProviderKind | null) => {
    setSelection({ scope, provider, expandedModelKey: null });
  };
  const setExpandedModelKey = (key: string | null) => {
    setSelection({ scope, provider: selectedProvider, expandedModelKey: key });
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
