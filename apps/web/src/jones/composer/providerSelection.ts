import type { ProviderInstanceId } from "@t3tools/contracts";
import { resolveComposerProviderSelection as resolveUpstream } from "../../components/ChatView.logic";
export function resolveComposerProviderSelection(input: Parameters<typeof resolveUpstream>[0] & { requiredInstanceId?: ProviderInstanceId | null | undefined }) {
  if (input.requiredInstanceId == null) return resolveUpstream(input);
  const required = input.requiredInstanceId;
  const result = resolveUpstream({ ...input, candidateInstanceIds: [required] });
  const selectedProviderEntry = result.selectedProviderEntry?.instanceId === required ? result.selectedProviderEntry : undefined;
  return { ...result, selectedProviderEntry, unavailableProviderInstanceId: selectedProviderEntry ? undefined : required };
}
