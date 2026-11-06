import { useEffect } from "react";
import type { EnvironmentId, OrchestrationV2ThreadProjection, ThreadId } from "@t3tools/contracts";
import { useAssetUrls } from "../../assets/assetUrls";
import { selectMessageImageResources } from "../../session-logic";
import { usePendingSends } from "./hooks";
import { pendingSends } from "./store";

export function usePendingPreviewUrls(environmentId: EnvironmentId, threadId: ThreadId) {
  const entries = usePendingSends();
  const urls = new Map<string, string>();
  for (const entry of entries) {
    if (entry.environmentId !== environmentId || entry.threadId !== threadId || !entry.visible)
      continue;
    for (const attachment of entry.message.attachments ?? []) {
      if ("previewUrl" in attachment && attachment.previewUrl)
        urls.set(attachment.id, attachment.previewUrl);
    }
  }
  return urls;
}

export function usePendingPreviewRelease(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  projection: OrchestrationV2ThreadProjection | null,
) {
  const entries = usePendingSends().filter(
    (entry) =>
      entry.environmentId === environmentId && entry.threadId === threadId && entry.visible,
  );
  const messages =
    projection?.visibleTurnItems.flatMap((row) => {
      const item = row.item;
      return item.type === "user_message" &&
        entries.some((entry) => entry.message.id === item.messageId)
        ? [item]
        : [];
    }) ?? [];
  const resources = messages.flatMap((message) => selectMessageImageResources(message.attachments));
  const urls = useAssetUrls(environmentId, resources);
  // oxlint-disable-next-line t3code/react-effect-budget -- Preload authoritative image URLs before releasing store-owned local previews.
  useEffect(() => {
    let cancelled = false;
    const images: HTMLImageElement[] = [];
    for (const entry of entries) {
      const message = messages.find((message) => message.messageId === entry.message.id);
      if (!message) continue;
      const imageIds = selectMessageImageResources(message.attachments).map(
        (resource) => resource.attachmentId,
      );
      const serverUrls = imageIds.map(
        (id) => urls[resources.findIndex((resource) => resource.attachmentId === id)],
      );
      if (serverUrls.some((url) => !url || url.startsWith("blob:"))) continue;
      void Promise.all(
        serverUrls.map(
          (url) =>
            new Promise<void>((resolve, reject) => {
              const image = new Image();
              images.push(image);
              image.onload = () => resolve();
              image.onerror = () => reject(new Error("Preview unavailable"));
              image.src = url!;
            }),
        ),
      ).then(
        () => {
          if (!cancelled) pendingSends.dismiss(entry.key);
        },
        () => undefined,
      );
    }
    return () => {
      cancelled = true;
      for (const image of images) {
        image.onload = null;
        image.onerror = null;
        image.src = "";
      }
    };
  }, [entries, messages, resources, urls]);
}
