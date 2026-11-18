import type { PromptNavigation } from "../../jones/workQueue/promptSearch";
import {
  ensureClientSettingsHydrated,
  useClientSettings,
  useClientSettingsHydrationStatus,
} from "../../hooks/useSettings";
import {
  PromptQueueSettingsStatus,
  UnifiedQueuePage,
} from "../../jones/workQueue/UnifiedQueuePage";

export { validatePromptSearch } from "../../jones/workQueue/promptSearch";

export function WorkQueuePage({ navigation }: { navigation?: PromptNavigation } = {}) {
  const settings = useClientSettings();
  const status = useClientSettingsHydrationStatus();
  if (status !== "ready") {
    return (
      <PromptQueueSettingsStatus
        status={status}
        onRetry={() => {
          void ensureClientSettingsHydrated().catch(() => undefined);
        }}
      />
    );
  }
  return <UnifiedQueuePage defaultView={settings.promptsDefaultStage} navigation={navigation} />;
}
