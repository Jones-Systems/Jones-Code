// @vitest-environment jsdom
import { EnvironmentId, type HostStatusSnapshot } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { TooltipProvider } from "../../components/ui/tooltip";
import { discoverGlobalHostStatusSource } from "./globalSource";

const connections = vi.hoisted(() => ({
  environmentIds: [] as EnvironmentId[],
  connectedEnvironmentIds: [] as EnvironmentId[],
  primaryEnvironmentId: null as EnvironmentId | null,
}));
const fetchStatus = vi.hoisted(() =>
  vi.fn<(environmentId: EnvironmentId, signal: AbortSignal) => Promise<HostStatusSnapshot>>(),
);
vi.mock("../../state/environments", () => ({
  useEnvironmentIds: () => connections.environmentIds,
  useConnectedEnvironmentIds: () => connections.connectedEnvironmentIds,
  usePrimaryEnvironmentId: () => connections.primaryEnvironmentId,
}));
vi.mock("./state", () => ({ fetchHostStatus: fetchStatus }));

const local = EnvironmentId.make("local");
const remote = EnvironmentId.make("remote-a");
const other = EnvironmentId.make("remote-b");
const unconfigured: HostStatusSnapshot = {
  hosts: [{ id: "vps", status: "unavailable", reason: "not_configured" }],
};
function available(cpuUsagePercent = 23): HostStatusSnapshot {
  return {
    hosts: [
      {
        id: "vps",
        status: "available",
        cpuUsagePercent,
        logicalCpuCount: 16,
        occupiedMemoryBytes: 12 * 1024 ** 3,
        availableMemoryBytes: 4 * 1024 ** 3,
        totalMemoryBytes: 16 * 1024 ** 3,
        sampledAt: new Date().toISOString(),
      },
    ],
  };
}

let container: HTMLDivElement;
let root: Root;
let GlobalHostStatusIndicators: typeof import("./GlobalHostStatusIndicators").GlobalHostStatusIndicators;
let visibility: DocumentVisibilityState;

beforeEach(async () => {
  vi.resetModules();
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-14T12:00:00Z"));
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  visibility = "visible";
  vi.spyOn(document, "visibilityState", "get").mockImplementation(() => visibility);
  vi.stubGlobal("ResizeObserver", class {
    observe() {}
    disconnect() {}
  });
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (
    this: HTMLElement,
  ) {
    return new DOMRect(0, 0, this.getAttribute("aria-label") === "Host status" ? 1000 : 100, 32);
  });
  connections.environmentIds = [local, remote];
  connections.connectedEnvironmentIds = [local, remote];
  connections.primaryEnvironmentId = local;
  fetchStatus.mockReset();
  fetchStatus.mockImplementation(async (id) => id === local ? unconfigured : available());
  ({ GlobalHostStatusIndicators } = await import("./GlobalHostStatusIndicators"));
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  expect(vi.getTimerCount()).toBe(0);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

async function render(threadEnvironmentId = local) {
  await act(async () => {
    root.render(
      <TooltipProvider>
        <div data-thread-environment={threadEnvironmentId}>
          <GlobalHostStatusIndicators />
        </div>
      </TooltipProvider>,
    );
  });
}
async function advance(milliseconds = 0) {
  await act(async () => vi.advanceTimersByTimeAsync(milliseconds));
}
function vps() {
  return container.querySelector('[role="img"]');
}
function requests(id: EnvironmentId) {
  return fetchStatus.mock.calls.filter(([environmentId]) => environmentId === id);
}

it("keeps the global VPS source across local, remote, and local thread navigation and header remounts", async () => {
  await render(local);
  expect(vps()?.textContent).toBe("VPS · 23% · 4");
  expect(requests(local)).toHaveLength(1);
  expect(requests(remote)).toHaveLength(2);
  await render(remote);
  await render(local);
  expect(vps()?.textContent).toBe("VPS · 23% · 4");
  expect(fetchStatus).toHaveBeenCalledTimes(3);
  await advance(10_000);
  expect(requests(local)).toHaveLength(1);
  expect(requests(remote)).toHaveLength(3);
  await act(async () => root.render(null));
  expect(vi.getTimerCount()).toBe(0);
  await render();
  expect(requests(local)).toHaveLength(1);
  expect(requests(remote)).toHaveLength(4);
  expect(vps()?.textContent).toBe("VPS · 23% · 4");
});

it("uses the primary first, then stable registered ready IDs, and skips denied or unconfigured gateways", async () => {
  const request = vi.fn(async (id: EnvironmentId) => {
    if (id === other) throw new Error("403 orchestration:read denied");
    return id === local ? unconfigured : available();
  });
  expect(await discoverGlobalHostStatusSource({
    environmentIds: [other, remote, local],
    connectedEnvironmentIds: [remote, other, local, EnvironmentId.make("unregistered")],
    primaryEnvironmentId: other,
  }, new AbortController().signal, request)).toBe(remote);
  expect(request.mock.calls.map(([id]) => id)).toEqual([other, local, remote]);
});

it.each(["upstream_unavailable", "invalid_response", "stale"] as const)(
  "selects a configured but %s VPS gateway and retains it when another becomes ready",
  async (reason) => {
    fetchStatus.mockResolvedValue({ hosts: [{ id: "vps", status: "unavailable", reason }] });
    await render();
    expect(requests(local)).toHaveLength(2);
    expect(requests(remote)).toHaveLength(0);
    expect(vps()?.textContent).toBe("VPS · — · —");
    connections.environmentIds = [local, remote, other];
    connections.connectedEnvironmentIds = [remote, other];
    connections.primaryEnvironmentId = other;
    await render(remote);
    await advance(10_000);
    expect(requests(local)).toHaveLength(3);
    expect(requests(other)).toHaveLength(0);
  },
);

it("retains a disconnected source on failure and recovers its values without probing other gateways", async () => {
  await render();
  connections.connectedEnvironmentIds = [local];
  fetchStatus.mockRejectedValue(new Error("disconnected"));
  await render();
  await advance(10_000);
  expect(vps()?.textContent).toBe("VPS · — · —");
  expect(requests(local)).toHaveLength(1);
  fetchStatus.mockResolvedValue(available(41));
  await advance(10_000);
  expect(vps()?.textContent).toBe("VPS · 41% · 4");
  expect(requests(local)).toHaveLength(1);
});

it("drops a removed source, aborts its poll, and never leaks a late sample into its replacement", async () => {
  await render();
  let resolve!: (snapshot: HostStatusSnapshot) => void;
  fetchStatus.mockImplementation((id) => id === remote
    ? new Promise((done) => { resolve = done; })
    : Promise.resolve(id === local ? unconfigured : available(61)));
  await advance(10_000);
  const oldSignal = requests(remote).at(-1)?.[1];
  connections.environmentIds = [local, other];
  connections.connectedEnvironmentIds = [local, other];
  await render();
  expect(oldSignal?.aborted).toBe(true);
  expect(vps()?.textContent).toBe("VPS · 61% · 4");
  await act(async () => resolve(available(99)));
  expect(vps()?.textContent).toBe("VPS · 61% · 4");
  await advance(10_000);
  expect(requests(remote)).toHaveLength(3);
});

it("aborts discovery on removal and ignores a configured late completion", async () => {
  let resolve!: (snapshot: HostStatusSnapshot) => void;
  fetchStatus.mockImplementation((id) => id === remote
    ? new Promise((done) => { resolve = done; })
    : Promise.resolve(id === local ? unconfigured : available(61)));
  await render();
  const oldSignal = requests(remote)[0]?.[1];
  connections.environmentIds = [local, other];
  connections.connectedEnvironmentIds = [local, other];
  await render();
  expect(oldSignal?.aborted).toBe(true);
  expect(vps()?.textContent).toBe("VPS · 61% · 4");
  await act(async () => resolve(available(99)));
  await advance(10_000);
  expect(vps()?.textContent).toBe("VPS · 61% · 4");
  expect(requests(remote)).toHaveLength(1);
});

it("waits for ready gateways and retries unconfigured or denied discovery only while visible", async () => {
  connections.environmentIds = [];
  connections.connectedEnvironmentIds = [];
  await render();
  await advance(10_000);
  expect(fetchStatus).not.toHaveBeenCalled();
  expect(container.querySelector('[role="status"]')?.textContent).toBe("Host status unavailable");
  connections.environmentIds = [local];
  await render();
  expect(fetchStatus).not.toHaveBeenCalled();
  connections.connectedEnvironmentIds = [local];
  fetchStatus.mockRejectedValueOnce(new Error("403"));
  await render();
  expect(fetchStatus).toHaveBeenCalledTimes(1);
  await advance(10_000);
  expect(fetchStatus).toHaveBeenCalledTimes(2);
  expect(vps()).toBeNull();
  expect(container.querySelector('[role="status"]')?.textContent).toBe("Host status unavailable");
  await act(async () => {
    visibility = "hidden";
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await advance(30_000);
  expect(fetchStatus).toHaveBeenCalledTimes(2);
  fetchStatus.mockResolvedValue(available());
  await act(async () => {
    visibility = "visible";
    document.dispatchEvent(new Event("visibilitychange"));
  });
  expect(fetchStatus).toHaveBeenCalledTimes(4);
  expect(vps()?.textContent).toBe("VPS · 23% · 4");
});

it("requires the VPS slot and does not continue discovery after cancellation", async () => {
  const abort = new AbortController();
  const request = vi.fn(async () => ({ hosts: [] } as HostStatusSnapshot));
  expect(await discoverGlobalHostStatusSource(connections, abort.signal, request)).toBeNull();
  expect(request).toHaveBeenCalledTimes(2);
  request.mockClear();
  abort.abort();
  await expect(discoverGlobalHostStatusSource(connections, abort.signal, request)).rejects.toThrow();
  expect(request).not.toHaveBeenCalled();
});
