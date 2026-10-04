import { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { createDeviceEnvironmentAtoms } from "@t3tools/client-runtime/state/device";
import {
  type DeviceHubAccess,
  resolveDeviceHubAccess,
} from "@t3tools/client-runtime/state/deviceHubAccess";
import type { DeviceServiceState, EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import {
  createDeviceMediaRouteManager,
  type DeviceMediaRoute,
} from "@t3tools/client-runtime/device/hub-access";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentSession } from "./session";
import { useEnvironmentQuery } from "./query";

export const deviceEnvironment = createDeviceEnvironmentAtoms(connectionAtomRuntime);

const EMPTY_DEVICE_STATE: DeviceServiceState = {
  hosts: [],
  hostStatus: "disabled",
  hostStatuses: {},
  devices: [],
  sessions: [],
  onboardingCompleted: false,
  agentAccessEnabled: false,
  hubBasePath: "/api/device-hub",
  revision: 0,
};

export function useDeviceState(environmentId: EnvironmentId | null): {
  readonly state: DeviceServiceState;
  readonly loaded: boolean;
} {
  const query = useEnvironmentQuery(
    environmentId === null ? null : deviceEnvironment.state({ environmentId, input: {} }),
  );
  return { state: query.data ?? EMPTY_DEVICE_STATE, loaded: query.data !== undefined };
}

/**
 * Hub access for one environment. Bearer and DPoP connections mint a ticket
 * here; a stream that gets a 401 back refreshes this atom and reconnects.
 * Keyed on the prepared connection so a re-pair produces new credentials.
 */
const deviceHubAccessAtom = Atom.family((environmentId: EnvironmentId) =>
  connectionAtomRuntime
    .atom((get) => {
      const prepared = Option.getOrNull(
        get(environmentSession.preparedConnectionValueAtom(environmentId)),
      );
      if (prepared === null) return Effect.never;
      return resolveDeviceHubAccess({ prepared, hubBasePath: EMPTY_DEVICE_STATE.hubBasePath });
    })
    .pipe(Atom.setIdleTTL(60_000), Atom.withLabel(`device-hub-access:${environmentId}`)),
);

export function useDeviceHubAccess(
  environmentId: EnvironmentId | null,
  hostId = "local",
): DeviceHubAccess | null {
  const result = useAtomValue(
    environmentId === null ? EMPTY_ACCESS_ATOM : deviceHubAccessAtom(environmentId),
  );
  return useMemo(
    () =>
      AsyncResult.isSuccess(result)
        ? { ...result.value, query: { ...result.value.query, hostId } }
        : null,
    [result, hostId],
  );
}

const EMPTY_ACCESS_ATOM = Atom.make(AsyncResult.initial<DeviceHubAccess, never>()).pipe(
  Atom.withLabel("device-hub-access:empty"),
);

export function refreshDeviceHubAccess(environmentId: EnvironmentId): void {
  appAtomRegistry.refresh(deviceHubAccessAtom(environmentId));
}

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
