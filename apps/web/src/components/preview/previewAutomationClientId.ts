function createPreviewAutomationClientId(): string {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
  return `preview-${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

export function resolvePreviewAutomationClientId(
  environmentId: string,
  runtimeInstanceId?: string,
): string {
  if (!runtimeInstanceId) return createPreviewAutomationClientId();
  const key = `preview-automation-client:${runtimeInstanceId}:${environmentId}`;
  try {
    const existing = globalThis.sessionStorage.getItem(key);
    if (existing) return existing;
    const created = createPreviewAutomationClientId();
    globalThis.sessionStorage.setItem(key, created);
    return created;
  } catch {
    return createPreviewAutomationClientId();
  }
}
