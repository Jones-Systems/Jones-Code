import {
  ensureClientSettingsHydrated,
  useClientSettings,
  useClientSettingsHydrationStatus,
} from "../../hooks/useSettings";
import {
  PromptQueueSettingsStatus,
  UnifiedQueuePage,
} from "../../jones/workQueue/UnifiedQueuePage";

export function WorkQueuePage() {
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
  return <UnifiedQueuePage defaultView={settings.promptsDefaultStage} />;
}
