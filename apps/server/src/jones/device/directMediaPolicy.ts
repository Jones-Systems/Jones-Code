export const DEVICE_HUB_POLICY = {
  allowed: [
    "^\\/api\\/devices$",
    "^\\/vendor\\/serve-sim\\/api$",
    "^\\/vendor\\/serve-sim\\/api\\/screenshot$",
    "^\\/vendor\\/serve-sim\\/api\\/event-log(\\/events)?$",
    "^\\/vendor\\/serve-sim\\/helper\\/[^/]+\\/(stream\\.mjpeg|stream\\.avcc|config|health|ax|foreground)$",
    "^\\/vendor\\/serve-sim\\/helper\\/[^/]+\\/panel\\/(1|3)\\/stream\\.avcc$",
    "^\\/vendor\\/serve-sim\\/appstate$",
    "^\\/vendor\\/serve-emu\\/api\\/(devices|screenshot|stream-mode|stream-settings|accessibility|fold)$",
    "^\\/vendor\\/serve-emu\\/health$",
  ],

  /** Read paths are GET-only; only these accept other methods (screenshot captures, stream tuning). */
  mutable: [
    "^\\/vendor\\/serve-sim\\/api\\/screenshot$",
    "^\\/vendor\\/serve-emu\\/api\\/(screenshot|stream-mode|stream-settings)$",
    "^\\/vendor\\/serve-emu\\/api\\/fold$",
  ],

  websocket: [
    "^\\/api\\/devices\\/ws$",
    "^\\/vendor\\/serve-sim\\/helper\\/ws$",
    "^\\/vendor\\/serve-emu\\/ws$",
  ],

  /** Hop-by-hop and credential headers that must not cross the proxy. */
  droppedHeaders: [
    "host",
    "connection",
    "upgrade",
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-extensions",
    "sec-websocket-protocol",
    "cookie",
    "authorization",
    "dpop",
    "content-length",
    "accept-encoding",
    "proxy-authorization",
    "proxy-authenticate",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
  ],
} as const;

export type DeviceHubPolicy = typeof DEVICE_HUB_POLICY;

/** Shared with the standalone gateway; keep these functions free of module captures. */
export function deviceHubRoutePolicy(
  policy: DeviceHubPolicy,
  path: string,
  method: string,
  upgrade: boolean,
): "read" | "operate" | 404 | 405 {
  if (
    !(upgrade ? policy.websocket : policy.allowed).some((pattern) => new RegExp(pattern).test(path))
  )
    return 404;
  const readOnly = method === "GET" || method === "HEAD";
  if (
    (!upgrade && !readOnly && !policy.mutable.some((pattern) => new RegExp(pattern).test(path))) ||
    (upgrade && method !== "GET")
  )
    return 405;
  return (upgrade && path !== "/api/devices/ws") ||
    (!readOnly && /\/api\/(stream-(mode|settings)|fold)$/.test(path))
    ? "operate"
    : "read";
}

/** Inventory and host-wide routes have no device binding and stay on the VPS proxy. */
export function directDeviceRouteMatches(
  path: string,
  search: string,
  deviceId: string,
  platform: string,
): boolean {
  if (path === "/readyz") return true;
  const vendor = platform === "ios" ? "/vendor/serve-sim" : "/vendor/serve-emu";
  if (!path.startsWith(vendor + "/")) return false;
  const query = new URLSearchParams(search);
  const targets = query.getAll("device");
  if (targets.length > 1 || (targets.length === 1 && targets[0] !== deviceId)) return false;
  if (platform === "ios") {
    const helper =
      /^\/vendor\/serve-sim\/helper\/([^/]+)\/(?:stream\.(?:avcc|mjpeg)|config|health|ax|foreground|panel\/(?:1|3)\/stream\.avcc)$/.exec(
        path,
      );
    if (helper) {
      try {
        return decodeURIComponent(helper[1]!) === deviceId;
      } catch {
        return false;
      }
    }
    return (
      targets.length === 1 &&
      /^(?:\/vendor\/serve-sim\/helper\/ws|\/vendor\/serve-sim\/api\/screenshot)$/.test(path)
    );
  }
  return (
    targets.length === 1 &&
    /^(?:\/vendor\/serve-emu\/ws|\/vendor\/serve-emu\/api\/(?:screenshot|stream-mode|stream-settings|fold))$/.test(
      path,
    )
  );
}

export function stripDeviceHubQuery(search: string): string {
  const query = new URLSearchParams(search);
  for (const key of ["wsTicket", "hostId", "grant", "clientOrigin"]) query.delete(key);
  return query.size > 0 ? `?${query.toString()}` : "";
}

export function deviceHubForwardHeaders(
  policy: DeviceHubPolicy,
  source: Readonly<Record<string, string | string[] | undefined>>,
  origin: string,
): Record<string, string> {
  const headers: Record<string, string> = {};
  const connection = new Set(
    String(source.connection ?? "")
      .toLowerCase()
      .split(",")
      .map((name) => name.trim()),
  );
  for (const [name, value] of Object.entries(source)) {
    if (
      policy.droppedHeaders.some((dropped) => dropped === name.toLowerCase()) ||
      connection.has(name.toLowerCase()) ||
      value === undefined
    )
      continue;
    headers[name] = Array.isArray(value) ? value.join(", ") : value;
  }
  // serve-emu rejects mutations whose Origin differs from its loopback origin.
  if (source.origin !== undefined) headers.origin = origin;
  return headers;
}

export function validDirectClientOrigin(origin: string): boolean {
  if (origin === "t3code://app" || origin === "t3code-dev://app") return true;
  try {
    const url = new URL(origin);
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) &&
      url.origin === origin
    );
  } catch {
    return false;
  }
}
