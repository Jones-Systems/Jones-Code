import { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import {
  createDeviceMediaRouteManager,
  type DeviceMediaRoute,
} from "@t3tools/client-runtime/device/hub-access";
import type { DeviceHubAccess } from "@t3tools/client-runtime/state/deviceHubAccess";
import { useDeviceHubAccess, refreshDeviceHubAccess } from "../../state/device";

export function useDeviceStreamRoute(input: {
  readonly environmentId: EnvironmentId;
  readonly hostId: string;
  readonly deviceId: string;
  readonly platform: string;
  readonly visible: boolean;
  readonly retire: () => void;
}) {
  const proxy = useDeviceHubAccess(input.environmentId, input.hostId);
  const identity = JSON.stringify([
    input.environmentId,
    input.hostId,
    input.deviceId,
    input.platform,
    input.visible,
  ]);
  // Returning to an earlier configuration must not revive its stopped route.
  const configuration = useMemo(() => ({ identity }), [identity]);
  const owner = useMemo(() => ({ configuration, proxy }), [configuration, proxy]);
  const [selection, setSelection] = useState<{
    owner: typeof owner;
    route: DeviceMediaRoute;
  } | null>(null);
  const route = selection?.owner === owner ? selection.route : null;
  const manager = useRef<ReturnType<typeof createDeviceMediaRouteManager> | null>(null);
  const refreshes = useRef<{ configuration: typeof configuration; count: number } | null>(null);
  const retire = useEffectEvent(input.retire);
  useEffect(() => {
    if (refreshes.current?.configuration !== configuration) {
      refreshes.current = { configuration, count: 0 };
    }
    const budget = refreshes.current;
    if (!proxy || !input.visible) return;
    let alive = true;
    const bridge = window.desktopBridge;
    const supported = bridge?.openDeviceMediaTunnel && bridge.closeDeviceMediaTunnel;
    const current = createDeviceMediaRouteManager({
      proxy,
      hostId: input.hostId,
      deviceId: input.deviceId,
      platform: input.platform,
      clientOrigin: window.location.origin,
      ...(supported
        ? {
            bridge: {
              openDeviceMediaTunnel: (request) => bridge.openDeviceMediaTunnel!(request),
              closeDeviceMediaTunnel: (id) => bridge.closeDeviceMediaTunnel!(id),
            },
          }
        : {}),
      fetch: globalThis.fetch.bind(globalThis),
      retire: () => retire(),
      onRoute: (next) => {
        if (!alive) return;
        if (next.phase === "connected") budget.count = 0;
        setSelection({ owner, route: next });
      },
      refreshAccess: () => {
        if (alive && budget.count++ === 0) refreshDeviceHubAccess(input.environmentId);
      },
    });
    manager.current = current;
    void current.start();
    return () => {
      alive = false;
      current.stop();
      if (manager.current === current) manager.current = null;
    };
  }, [
    proxy,
    owner,
    configuration,
    input.environmentId,
    input.hostId,
    input.deviceId,
    input.platform,
    input.visible,
  ]);
  const report = useCallback(
    (access: DeviceHubAccess, channel: "video" | "input", connected: boolean, detail?: string) =>
      manager.current?.report(access, channel, connected, detail),
    [],
  );
  const unauthorized = useCallback(
    (access: DeviceHubAccess) => manager.current?.unauthorized(access),
    [],
  );
  const retryAccess = useCallback(() => {
    if (refreshes.current?.configuration === configuration) refreshes.current.count = 0;
    refreshDeviceHubAccess(input.environmentId);
  }, [configuration, input.environmentId]);
  return { route, proxy, report, unauthorized, retryAccess };
}
