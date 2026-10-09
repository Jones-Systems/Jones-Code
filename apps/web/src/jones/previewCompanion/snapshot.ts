import type { PreviewEvent, PreviewListResult } from "@t3tools/contracts";

export function applyCompanionSessionEvent(
  current: PreviewListResult | null,
  event: PreviewEvent,
): PreviewListResult | null {
  if (!current || current.serverEpoch !== event.serverEpoch) return null;
  if (event.revision <= current.revision) return current;
  const sessions = current.sessions.filter((session) => session.tabId !== event.tabId);
  if ("snapshot" in event) sessions.push(event.snapshot);
  else if (event.type === "failed") {
    const previous = current.sessions.find((session) => session.tabId === event.tabId);
    if (previous)
      sessions.push({
        ...previous,
        navStatus: {
          _tag: "LoadFailed",
          url: event.url,
          title: event.title,
          code: event.code,
          description: event.description,
        },
      });
  }
  return { serverEpoch: event.serverEpoch, revision: event.revision, sessions };
}
