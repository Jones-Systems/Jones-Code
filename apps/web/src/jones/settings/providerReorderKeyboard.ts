const dragKeys = new Set([
  "ArrowUp",
  "ArrowDown",
  "ArrowLeft",
  "ArrowRight",
  " ",
  "Enter",
  "Escape",
]);

/** Only an active provider handle owns these keys; ordinary modal dismissal stays intact. */
export function ownsProviderReorderKey(key: string, target: EventTarget | null): boolean {
  return (
    dragKeys.has(key) &&
    target instanceof HTMLElement &&
    target.matches('[data-provider-reorder-handle][aria-pressed="true"]')
  );
}
