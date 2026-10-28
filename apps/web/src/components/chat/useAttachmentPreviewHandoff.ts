import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { usePendingPreviewUrls } from "../../jones/pendingSends/previews";

export function useAttachmentPreviewHandoff(input: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  return { timelineAttachmentUrlById: usePendingPreviewUrls(input.environmentId, input.threadId) };
}
