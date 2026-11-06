import type { ModelSelection, ProviderInstanceId } from "@t3tools/contracts";

export interface CodexFailoverCandidate {
  readonly instanceId: ProviderInstanceId;
  readonly driver: string;
  readonly enabled: boolean;
  readonly projectEnabled: boolean;
  readonly continuationKey: string | null;
}

// A deterministic proposal list, not start admission. The caller must revalidate
// live configuration, continuation ownership and model/options before dispatch.
export function codexFailoverSelections(input: {
  readonly selection: ModelSelection;
  readonly source: Pick<CodexFailoverCandidate, "driver" | "continuationKey">;
  readonly candidates: ReadonlyArray<CodexFailoverCandidate>;
  readonly visited: ReadonlySet<ProviderInstanceId>;
}): ReadonlyArray<ModelSelection> {
  const continuationKey = input.source.continuationKey;
  if (input.source.driver !== "codex" || !continuationKey?.trim()) return [];
  const excluded = new Set(input.visited);
  excluded.add(input.selection.instanceId);
  return input.candidates
    .toSorted((left, right) =>
      left.instanceId < right.instanceId ? -1 : left.instanceId > right.instanceId ? 1 : 0,
    )
    .flatMap((candidate) => {
      if (
        excluded.has(candidate.instanceId) ||
        candidate.driver !== "codex" ||
        !candidate.enabled ||
        !candidate.projectEnabled ||
        candidate.continuationKey !== continuationKey
      )
        return [];
      excluded.add(candidate.instanceId);
      return [{ ...input.selection, instanceId: candidate.instanceId }];
    });
}
