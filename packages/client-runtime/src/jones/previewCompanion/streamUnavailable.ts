export interface PreviewHostUnavailable {
  readonly hostId: string | null;
  readonly label: string;
  readonly state: "unknown" | "offline" | "timeout" | "unavailable";
}

export function decodePreviewHostUnavailable(reason: string): PreviewHostUnavailable {
  const fallback: PreviewHostUnavailable = { hostId: null, label: "", state: "unavailable" };
  try {
    const value: unknown = JSON.parse(reason);
    if (typeof value !== "object" || value === null) return fallback;
    const input = value as Record<string, unknown>;
    return {
      hostId:
        typeof input.hostId === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(input.hostId)
          ? input.hostId
          : null,
      label: typeof input.label === "string" ? input.label : "",
      state:
        input.state === "unknown" || input.state === "offline" || input.state === "timeout"
          ? input.state
          : "unavailable",
    };
  } catch {
    return fallback;
  }
}

export function previewHostUnavailableMessage(host: PreviewHostUnavailable): string {
  return `${host.label || "Selected browser host"} is ${host.state === "timeout" ? "not ready" : host.state === "offline" ? "offline" : "unavailable"}. This tab stays on that host. Reconnect the host, then try again.`;
}
