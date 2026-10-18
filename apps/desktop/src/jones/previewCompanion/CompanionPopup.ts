export interface CompanionPopupNotice {
  readonly kind: "popup_blocked";
  readonly origin: string | null;
}

const listeners = new Set<{ readonly listener: (notice: CompanionPopupNotice) => void }>();

export function onCompanionPopupNotice(
  listener: (notice: CompanionPopupNotice) => void,
): () => void {
  const subscription = { listener };
  listeners.add(subscription);
  return () => {
    listeners.delete(subscription);
  };
}

function popupOrigin(url: string): string | null {
  if (url.length > 16_384) return null;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:" ? parsed.origin : null;
  } catch {
    return null;
  }
}

export function notifyCompanionPopupBlocked(url: string): void {
  const notice: CompanionPopupNotice = Object.freeze({
    kind: "popup_blocked",
    origin: popupOrigin(url),
  });
  for (const subscription of listeners) {
    // Only the redacted notice crosses the asynchronous boundary. Delivery
    // must never prevent the caller from denying a popup.
    queueMicrotask(() => {
      if (!listeners.has(subscription)) return;
      try {
        subscription.listener(notice);
      } catch {
        // A failed UI consumer cannot turn the popup into a browser action.
      }
    });
  }
}
