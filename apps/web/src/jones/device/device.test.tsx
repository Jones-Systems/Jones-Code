import { EnvironmentId } from "@t3tools/contracts";
import type {
  createDeviceMediaRouteManager,
  DeviceMediaRoute,
} from "@t3tools/client-runtime/device/hub-access";
import type { DeviceHubAccess } from "@t3tools/client-runtime/state/deviceHubAccess";
import { AsyncResult } from "effect/unstable/reactivity";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { useDeviceStreamRoute } from "./useDeviceStreamRoute";

type ManagerInput = Parameters<typeof createDeviceMediaRouteManager>[0];
type FakeManager = ReturnType<typeof createDeviceMediaRouteManager> & {
  readonly input: ManagerInput;
  readonly publish: (route: DeviceMediaRoute) => void;
};
type HookInput = Parameters<typeof useDeviceStreamRoute>[0];
type HookView = ReturnType<typeof useDeviceStreamRoute>;

const state = vi.hoisted(() => ({
  result: null as AsyncResult.AsyncResult<DeviceHubAccess, never> | null,
  managers: [] as FakeManager[],
  refresh: vi.fn(),
}));

vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.result }));
vi.mock("@t3tools/client-runtime/state/device", () => ({
  createDeviceEnvironmentAtoms: () => ({}),
}));
vi.mock("../../connection/runtime", async () => {
  const { AsyncResult, Atom } = await import("effect/unstable/reactivity");
  return { connectionAtomRuntime: { atom: () => Atom.make(AsyncResult.initial()) } };
});
vi.mock("../../rpc/atomRegistry", () => ({ appAtomRegistry: { refresh: state.refresh } }));
vi.mock("../../state/session", () => ({ environmentSession: {} }));
vi.mock("@t3tools/client-runtime/device/hub-access", () => ({
  createDeviceMediaRouteManager: (input: ManagerInput) => {
    const manager: FakeManager = {
      input,
      start: vi.fn(() => Promise.resolve()),
      stop: vi.fn(() => input.retire()),
      report: vi.fn(),
      unauthorized: vi.fn(),
      publish: (route) => {
        input.retire();
        input.onRoute(route);
      },
    };
    state.managers.push(manager);
    return manager;
  },
}));

function access(ticket: string): DeviceHubAccess {
  return {
    httpBase: "https://server.test/api/device-hub",
    wsBase: "wss://server.test/api/device-hub",
    query: { wsTicket: ticket },
    credentials: false,
  };
}

function mediaRoute(access: DeviceHubAccess, phase: DeviceMediaRoute["phase"] = "connecting") {
  return { access, kind: "proxy", phase, generation: "test" } satisfies DeviceMediaRoute;
}

let renderer: ReactTestRenderer | undefined;
let input: HookInput;
const committed: HookView[] = [];

function Probe({ input }: { input: HookInput }) {
  const view = useDeviceStreamRoute(input);
  useLayoutEffect(() => {
    committed.push(view);
  }, [view]);
  return null;
}

function latest() {
  return committed.at(-1)!;
}

function currentManager() {
  return state.managers.at(-1)!;
}

async function mount(retire = vi.fn()) {
  input = {
    environmentId: EnvironmentId.make("environment-a"),
    hostId: "host-a",
    deviceId: "device-a",
    platform: "ios",
    visible: true,
    retire,
  };
  await act(() => {
    renderer = create(<Probe input={input} />);
  });
}

async function update(patch: Partial<HookInput>) {
  input = { ...input, ...patch };
  committed.length = 0;
  await act(() => renderer!.update(<Probe input={input} />));
}

beforeEach(() => {
  state.result = AsyncResult.success(access("initial"));
  state.managers.length = 0;
  state.refresh.mockClear();
  committed.length = 0;
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", { location: { origin: "https://renderer.test" } });
  vi.stubGlobal("fetch", vi.fn());
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

it("keeps stopped media and input access out of every commit when hidden and reopened", async () => {
  await mount();
  const previous = currentManager();
  const oldRoute = mediaRoute(previous.input.proxy);
  await act(() => previous.publish(oldRoute));
  expect(latest().route?.access).toBe(oldRoute.access);

  await update({ visible: false });
  expect(previous.stop).toHaveBeenCalledOnce();
  expect(committed.every((view) => view.route === null)).toBe(true);
  expect(state.managers).toHaveLength(1);

  await update({ visible: true });
  expect(committed.every((view) => view.route === null)).toBe(true);
  const reopened = currentManager();
  expect(reopened).not.toBe(previous);

  await act(() => {
    previous.input.onRoute(oldRoute);
    previous.input.refreshAccess();
  });
  expect(latest().route).toBeNull();
  expect(state.refresh).not.toHaveBeenCalled();

  const newRoute = mediaRoute(access("reopened"));
  await act(() => reopened.publish(newRoute));
  latest().report(newRoute.access, "video", true);
  latest().report(newRoute.access, "input", true);
  latest().unauthorized(newRoute.access);
  expect(reopened.report).toHaveBeenCalledWith(newRoute.access, "video", true, undefined);
  expect(reopened.report).toHaveBeenCalledWith(newRoute.access, "input", true, undefined);
  expect(reopened.unauthorized).toHaveBeenCalledWith(newRoute.access);
  expect(previous.report).not.toHaveBeenCalled();
});

it.each([
  { environmentId: EnvironmentId.make("environment-b") },
  { hostId: "host-b" },
  { deviceId: "device-b" },
  { platform: "android" },
])("does not reuse a stopped route when the configuration returns from %j", async (patch) => {
  await mount();
  const original = input;
  const previous = currentManager();
  await act(() => previous.publish(mediaRoute(previous.input.proxy)));

  await update(patch);
  expect(committed.every((view) => view.route === null)).toBe(true);
  expect(previous.stop).toHaveBeenCalledOnce();
  const intervening = currentManager();

  await update(original);
  expect(committed.every((view) => view.route === null)).toBe(true);
  expect(intervening.stop).toHaveBeenCalledOnce();
  expect(currentManager()).not.toBe(previous);
});

it("shares the automatic refresh cap across access renewal and resets on connection or retry", async () => {
  await mount();
  const previous = currentManager();
  await act(() => {
    previous.input.refreshAccess();
    previous.input.refreshAccess();
  });
  expect(state.refresh).toHaveBeenCalledOnce();

  state.result = AsyncResult.success(access("renewed"));
  await update({});
  const renewed = currentManager();
  expect(previous.stop).toHaveBeenCalledOnce();
  await act(() => {
    previous.input.onRoute(mediaRoute(previous.input.proxy, "connected"));
    previous.input.refreshAccess();
    renewed.input.refreshAccess();
  });
  expect(state.refresh).toHaveBeenCalledOnce();

  await act(() => {
    renewed.input.onRoute(mediaRoute(renewed.input.proxy, "connected"));
    renewed.input.refreshAccess();
    renewed.input.refreshAccess();
  });
  expect(state.refresh).toHaveBeenCalledTimes(2);

  await act(() => latest().retryAccess());
  expect(state.refresh).toHaveBeenCalledTimes(3);
  await act(() => {
    renewed.input.refreshAccess();
    renewed.input.refreshAccess();
  });
  expect(state.refresh).toHaveBeenCalledTimes(4);

  await update({ visible: false });
  await update({ visible: true });
  await act(() => currentManager().input.refreshAccess());
  expect(state.refresh).toHaveBeenCalledTimes(5);
});

it("uses the latest committed retirement callback before route publication and during cleanup", async () => {
  const originalRetire = vi.fn();
  await mount(originalRetire);
  const manager = currentManager();
  const firstRoute = mediaRoute(manager.input.proxy);
  await act(() => manager.publish(firstRoute));
  originalRetire.mockClear();

  const latestRetire = vi.fn(() => expect(latest().route).toBe(firstRoute));
  await update({ retire: latestRetire });
  expect(state.managers).toHaveLength(1);
  const replacement = mediaRoute(access("replacement"));
  await act(() => manager.publish(replacement));
  expect(latestRetire).toHaveBeenCalledOnce();
  expect(originalRetire).not.toHaveBeenCalled();
  expect(latest().route).toBe(replacement);

  const cleanupRetire = vi.fn();
  await update({ retire: cleanupRetire });
  await act(() => renderer!.unmount());
  renderer = undefined;
  expect(manager.stop).toHaveBeenCalledOnce();
  expect(cleanupRetire).toHaveBeenCalledOnce();
  expect(latestRetire).toHaveBeenCalledOnce();
  await act(() => {
    manager.input.onRoute(firstRoute);
    manager.input.refreshAccess();
  });
  expect(state.refresh).not.toHaveBeenCalled();
});
