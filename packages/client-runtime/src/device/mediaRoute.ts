// @effect-diagnostics globalFetch:off globalTimers:off - Framework-free browser transport.
import { type DeviceHubAccess, withDeviceHubQuery } from "./hubAccess.ts";

export interface DeviceMediaTunnelBridge {
  openDeviceMediaTunnel(input: {
    target: string;
    gatewayPort: number;
    owner: string;
    generation: string;
  }): Promise<{ id: string; httpBase: string }>;
  closeDeviceMediaTunnel(id: string): Promise<void>;
}

export interface DeviceMediaRoute {
  readonly access: DeviceHubAccess | null;
  readonly kind: "direct" | "proxy";
  readonly phase: "connecting" | "connected" | "denied";
  readonly reason?: string;
  readonly generation: string;
}

interface DirectGrant {
  target: string;
  gatewayPort: number;
  owner: string;
  generation: string;
  grant: string;
  expiresAt: number;
}

function parseGrant(value: unknown, now: number): DirectGrant {
  if (typeof value !== "object" || value === null)
    throw new DirectUnavailable("Invalid direct access");
  const grant = value as Partial<DirectGrant>;
  if (
    typeof grant.target !== "string" ||
    !grant.target ||
    !Number.isInteger(grant.gatewayPort) ||
    grant.gatewayPort! < 1 ||
    grant.gatewayPort! > 65535 ||
    typeof grant.owner !== "string" ||
    !grant.owner ||
    typeof grant.generation !== "string" ||
    !grant.generation ||
    typeof grant.grant !== "string" ||
    !grant.grant ||
    typeof grant.expiresAt !== "number" ||
    !Number.isFinite(grant.expiresAt) ||
    grant.expiresAt > now + 300_000
  )
    throw new DirectUnavailable("Invalid direct access");
  if (grant.expiresAt! <= now) throw new AccessDenied();
  return grant as DirectGrant;
}

class AccessDenied extends Error {}
class DirectUnavailable extends Error {}

/** Stop the old stream synchronously in retire; no input is retained across routes. */
export function createDeviceMediaRouteManager(input: {
  readonly proxy: DeviceHubAccess;
  readonly hostId: string;
  readonly deviceId: string;
  readonly platform: string;
  readonly clientOrigin: string;
  readonly bridge?: DeviceMediaTunnelBridge;
  readonly fetch: typeof globalThis.fetch;
  readonly retire: () => void;
  readonly onRoute: (route: DeviceMediaRoute) => void;
  readonly refreshAccess: () => void;
  readonly now?: () => number;
}) {
  const now = input.now ?? Date.now;
  let stopped = false;
  let attempt = 0;
  let tunnel: { id: string; httpBase: string } | null = null;
  let direct: { access: DeviceHubAccess; grant: DirectGrant } | null = null;
  let pendingLoss: { access: DeviceHubAccess; generation: string } | null = null;
  let active: DeviceMediaRoute = {
    access: null,
    kind: "proxy",
    phase: "connecting",
    generation: "0",
  };
  let video = false;
  let controls = false;
  let failures = 0;
  let request: AbortController | null = null;
  let startup: ReturnType<typeof setTimeout> | undefined;
  let retry: ReturnType<typeof setTimeout> | undefined;
  let renewal: ReturnType<typeof setTimeout> | undefined;
  let lossDeadline: ReturnType<typeof setTimeout> | undefined;

  const close = (previous: { id: string } | null) => {
    if (previous) void input.bridge?.closeDeviceMediaTunnel(previous.id).catch(() => undefined);
  };
  const clearTransportTimers = () => {
    clearTimeout(startup);
    clearTimeout(renewal);
    clearTimeout(lossDeadline);
  };
  const publish = (route: DeviceMediaRoute) => {
    input.retire();
    video = false;
    controls = false;
    active = route;
    input.onRoute(route);
  };
  const deny = () => {
    ++attempt;
    request?.abort();
    clearTransportTimers();
    clearTimeout(retry);
    const previous = tunnel;
    tunnel = null;
    direct = null;
    pendingLoss = null;
    publish({
      access: null,
      kind: "proxy",
      phase: "denied",
      reason: "Device access expired or was denied. Refreshing access…",
      generation: String(attempt),
    });
    close(previous);
    input.refreshAccess();
  };
  const useProxy = (reason: string, retryDirect: boolean) => {
    if (stopped) return;
    ++attempt;
    request?.abort();
    clearTransportTimers();
    clearTimeout(retry);
    const previous = tunnel;
    tunnel = null;
    direct = null;
    pendingLoss = null;
    if (active.kind === "proxy" && active.access === input.proxy) {
      active = { ...active, reason, generation: String(attempt) };
      input.onRoute(active);
    } else {
      publish({
        access: input.proxy,
        kind: "proxy",
        phase: "connecting",
        reason,
        generation: String(attempt),
      });
    }
    close(previous);
    if (retryDirect) {
      const delay = Math.min(60_000, 5_000 * 2 ** Math.min(failures++, 4));
      retry = setTimeout(() => {
        void connectDirect();
      }, delay);
    }
  };
  const classifyLoss = async (access: DeviceHubAccess, reason: string) => {
    if (stopped || active.access !== access || direct?.access !== access || !tunnel) return;
    const { grant } = direct;
    const retainedTunnel = tunnel;
    const current = ++attempt;
    request?.abort();
    clearTransportTimers();
    clearTimeout(retry);
    const controller = new AbortController();
    request = controller;
    const loss = { access, generation: grant.generation };
    pendingLoss = loss;
    const isCurrent = () =>
      !stopped &&
      attempt === current &&
      pendingLoss === loss &&
      direct?.access === access &&
      direct.grant === grant &&
      tunnel === retainedTunnel &&
      active.access === null &&
      active.generation === loss.generation;
    // HTTP/MJPEG teardown has no terminal status. Retire input before checking the same grant.
    publish({
      access: null,
      kind: "direct",
      phase: "connecting",
      reason: "Checking direct device access…",
      generation: grant.generation,
    });
    if (!isCurrent()) return;
    if (grant.expiresAt <= now()) {
      deny();
      return;
    }
    const timeout = setTimeout(
      () => {
        if (!isCurrent()) return;
        if (grant.expiresAt <= now()) deny();
        else useProxy(reason, true);
      },
      Math.min(5_000, grant.expiresAt - now()),
    );
    lossDeadline = timeout;
    try {
      const response = await input.fetch(withDeviceHubQuery(`${access.httpBase}/readyz`, access), {
        credentials: "omit",
        redirect: "error",
        signal: controller.signal,
      });
      if (!isCurrent()) return;
      if (grant.expiresAt <= now() || response.status === 401 || response.status === 403)
        throw new AccessDenied();
      if (!response.ok) throw new DirectUnavailable();
      const identity: unknown = await response.json();
      if (!isCurrent()) return;
      if (grant.expiresAt <= now()) throw new AccessDenied();
      if (
        typeof identity !== "object" ||
        identity === null ||
        (identity as { owner?: unknown }).owner !== grant.owner ||
        (identity as { generation?: unknown }).generation !== grant.generation
      )
        throw new DirectUnavailable();
      useProxy(reason, true);
    } catch (error) {
      if (!isCurrent()) return;
      if (grant.expiresAt <= now() || error instanceof AccessDenied) deny();
      else useProxy(reason, true);
    } finally {
      clearTimeout(timeout);
    }
  };
  const connectDirect = async () => {
    if (stopped) return;
    if (!input.bridge) {
      useProxy("Direct connection is unavailable in this client.", false);
      return;
    }
    const current = ++attempt;
    clearTimeout(lossDeadline);
    pendingLoss = null;
    const controller = new AbortController();
    request?.abort();
    request = controller;
    let opened: { id: string; httpBase: string } | null = null;
    const timeout = setTimeout(() => {
      controller.abort();
      if (!stopped && attempt === current) useProxy("Direct connection timed out.", true);
    }, 5_000);
    try {
      const query = new URLSearchParams({
        hostId: input.hostId,
        deviceId: input.deviceId,
        platform: input.platform,
        clientOrigin: input.clientOrigin,
      });
      const response = await input.fetch(
        withDeviceHubQuery(`${input.proxy.httpBase}/direct-access?${query}`, input.proxy),
        { credentials: input.proxy.credentials ? "include" : "omit", signal: controller.signal },
      );
      if (stopped || attempt !== current) return;
      if (response.status === 401 || response.status === 403) throw new AccessDenied();
      if (response.status === 204) {
        useProxy("Direct connection is not configured for this host.", false);
        return;
      }
      if (!response.ok) throw new DirectUnavailable("Direct connection is unavailable.");
      const grant = parseGrant(await response.json(), now());
      if (stopped || attempt !== current) return;
      opened = await input.bridge.openDeviceMediaTunnel({
        target: grant.target,
        gatewayPort: grant.gatewayPort,
        owner: grant.owner,
        generation: grant.generation,
      });
      if (stopped || attempt !== current) {
        close(opened);
        opened = null;
        return;
      }
      const base = opened.httpBase.replace(/\/$/, "");
      const access: DeviceHubAccess = {
        httpBase: base,
        wsBase: base.replace(/^http/, "ws"),
        query: { grant: grant.grant, hostId: input.hostId, clientOrigin: input.clientOrigin },
        credentials: false,
      };
      const ready = await input.fetch(withDeviceHubQuery(`${base}/readyz`, access), {
        credentials: "omit",
        redirect: "error",
        signal: controller.signal,
      });
      if (ready.status === 401 || ready.status === 403) throw new AccessDenied();
      if (!ready.ok) throw new DirectUnavailable("Direct host did not accept the connection.");
      const identity: unknown = await ready.json();
      if (
        typeof identity !== "object" ||
        identity === null ||
        (identity as { owner?: unknown }).owner !== grant.owner ||
        (identity as { generation?: unknown }).generation !== grant.generation
      )
        throw new DirectUnavailable("Direct host changed. Using the server connection.");
      if (grant.expiresAt <= now()) throw new AccessDenied();
      if (stopped || attempt !== current) {
        close(opened);
        opened = null;
        return;
      }
      clearTransportTimers();
      const previous = tunnel;
      tunnel = opened;
      opened = null;
      direct = { access, grant };
      publish({
        access,
        kind: "direct",
        phase: "connecting",
        generation: grant.generation,
      });
      close(previous);
      startup = setTimeout(() => {
        if (!stopped && attempt === current && (!video || !controls))
          void classifyLoss(access, "Direct video or input did not connect.");
      }, 6_000);
      renewal = setTimeout(
        () => {
          if (!stopped && attempt === current) void connectDirect();
        },
        Math.max(1, grant.expiresAt - now() - 15_000),
      );
    } catch (error) {
      close(opened);
      if (stopped || attempt !== current) return;
      if (error instanceof AccessDenied) deny();
      else
        useProxy(
          error instanceof DirectUnavailable ? error.message : "Direct connection is unavailable.",
          true,
        );
    } finally {
      clearTimeout(timeout);
    }
  };
  const report = (
    access: DeviceHubAccess,
    channel: "video" | "input",
    connected: boolean,
    detail?: string,
  ) => {
    if (stopped || active.access !== access || active.phase === "denied") return;
    const wasConnected = channel === "video" ? video : controls;
    if (channel === "video") video = connected;
    else controls = connected;
    if (active.kind === "direct" && !connected && (wasConnected || detail)) {
      void classifyLoss(
        access,
        channel === "input" ? "Direct input disconnected." : "Direct video disconnected.",
      );
      return;
    }
    const phase = video && controls ? "connected" : "connecting";
    if (phase === "connected") {
      clearTimeout(startup);
      if (active.kind === "direct") failures = 0;
    }
    if (active.phase !== phase) {
      active = { ...active, phase };
      input.onRoute(active);
    }
  };
  return {
    start: () => connectDirect(),
    report,
    unauthorized: (access: DeviceHubAccess) => {
      if (!stopped && (active.access === access || pendingLoss?.access === access)) deny();
    },
    stop: () => {
      if (stopped) return;
      stopped = true;
      ++attempt;
      request?.abort();
      clearTransportTimers();
      clearTimeout(retry);
      input.retire();
      close(tunnel);
      tunnel = null;
      direct = null;
      pendingLoss = null;
    },
  };
}
