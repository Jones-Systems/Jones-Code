export interface MonitorRectangle {
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}
export interface MonitorPlacement {
  readonly displayId: number | null;
  readonly vertical: number;
}
export interface MonitorPreferences extends MonitorPlacement {
  readonly enabled: boolean;
}

export function readMonitorPreferences(raw: unknown): MonitorPreferences {
  const value = typeof raw === "object" && raw !== null ? (raw as Record<string, unknown>) : {};
  return {
    enabled: typeof value.enabled === "boolean" ? value.enabled : true,
    displayId:
      typeof value.displayId === "number" && Number.isSafeInteger(value.displayId)
        ? value.displayId
        : null,
    vertical:
      typeof value.vertical === "number" && Number.isFinite(value.vertical)
        ? Math.max(0, Math.min(1, value.vertical))
        : 0.5,
  };
}

export function monitorBackend(input: {
  readonly platform: string;
  readonly ozonePlatform: string;
  readonly display: string | undefined;
  readonly waylandDisplay: string | undefined;
  readonly sessionType: string | undefined;
}): { readonly supported: boolean; readonly reason: string | null } {
  if (input.platform === "darwin" || input.platform === "win32")
    return { supported: true, reason: null };
  if (input.platform === "linux") {
    const platform = input.ozonePlatform.toLowerCase();
    if (
      platform === "x11" ||
      (!platform &&
        input.display &&
        !input.waylandDisplay &&
        input.sessionType?.toLowerCase() !== "wayland")
    ) {
      return { supported: true, reason: null };
    }
    return {
      supported: false,
      reason:
        "Thread monitor needs an X11 desktop. Native or ambiguous Wayland cannot guarantee its edge placement and visibility.",
    };
  }
  return { supported: false, reason: "Thread monitor is unavailable on this desktop platform." };
}

export function monitorBounds(
  workArea: MonitorRectangle,
  expanded: boolean,
  vertical: number,
): MonitorRectangle | null {
  if (workArea.width < 320 || workArea.height < 128) return null;
  const width = expanded ? Math.min(384, workArea.width) : 46;
  const height = expanded ? Math.min(704, workArea.height) : 128;
  return {
    x: Math.round(workArea.x + workArea.width - width),
    y: Math.round(workArea.y + Math.max(0, Math.min(1, vertical)) * (workArea.height - height)),
    width,
    height,
  };
}

export function monitorVertical(bounds: MonitorRectangle, workArea: MonitorRectangle): number {
  const travel = workArea.height - bounds.height;
  return travel <= 0 ? 0.5 : Math.max(0, Math.min(1, (bounds.y - workArea.y) / travel));
}

export interface MonitorSenderEvidence {
  readonly sender: unknown;
  readonly frame: unknown;
  readonly expectedSender: unknown;
  readonly mainFrame: unknown;
  readonly session: unknown;
  readonly expectedSession: unknown;
  readonly frameUrl: string;
  readonly contentsUrl: string;
  readonly expectedOrigin: string;
  readonly expectedEntry: string | null;
  readonly destroyed: boolean;
}

export function authorizeMonitorSender(input: MonitorSenderEvidence): boolean {
  if (
    input.destroyed ||
    !input.frame ||
    input.sender !== input.expectedSender ||
    input.frame !== input.mainFrame ||
    input.session !== input.expectedSession
  )
    return false;
  try {
    const frame = new URL(input.frameUrl);
    const contents = new URL(input.contentsUrl);
    // Custom schemes have an opaque URL.origin in Node; compare protocol and host explicitly.
    const origin = `${frame.protocol}//${frame.host}`;
    return (
      origin === input.expectedOrigin &&
      contents.href === frame.href &&
      !frame.username &&
      !frame.password &&
      (input.expectedEntry === null ||
        (frame.pathname === input.expectedEntry && !frame.search && !frame.hash))
    );
  } catch {
    return false;
  }
}
