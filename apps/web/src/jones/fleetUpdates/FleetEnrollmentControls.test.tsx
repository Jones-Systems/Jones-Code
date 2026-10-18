import { act, cloneElement, type ComponentProps, type ReactElement, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { FleetDesktopState, FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";

const h = vi.hoisted(() => ({
  state: null as FleetDesktopState | null,
  host: vi.fn(),
  refresh: vi.fn(),
  bridge: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: (atom: string) => (atom === "fleet-state" ? h.state : null),
}));
vi.mock("./runtime", () => ({
  fleetDesktopState: "fleet-state",
  fleetStatusError: "fleet-error",
  fleetHost: h.host,
  refreshFleetDesktopState: h.refresh,
}));
vi.mock("../../components/ui/button", () => ({
  Button: ({ children, ...props }: ComponentProps<"button">) => (
    <button {...props}>{children}</button>
  ),
}));
vi.mock("../../components/ui/tooltip", () => ({
  Tooltip: ({ children }: { children: ReactNode }) => <>{children}</>,
  TooltipTrigger: ({ render, children }: { render: ReactElement; children: ReactNode }) =>
    cloneElement(render, {}, children),
  TooltipPopup: () => null,
}));
import { FleetEnrollmentControls } from "./FleetEnrollmentControls";
const environmentId = EnvironmentId.make("selected-service");
const status: FleetHostStatus = {
  environmentId,
  operationProtocol: 1,
  enrollment: null,
  operation: null,
  update: {
    source: "jones-actions",
    channel: "jones-main",
    phase: "no-new",
    environmentId,
    currentVersion: "preview",
    installedSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
    capability: { check: true, download: true, install: true },
  },
};
let tree: ReactTestRenderer | undefined;
beforeEach(() => {
  h.state = { schema: 1, enrollments: [], campaigns: [] };
  h.host.mockReset().mockResolvedValue(status);
  h.refresh.mockReset().mockImplementation(async () => h.state);
  h.bridge.mockReset().mockImplementation(async () => h.state);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", { desktopBridge: { fleetUpdates: h.bridge } });
});
afterEach(() => {
  act(() => tree?.unmount());
  tree = undefined;
  vi.unstubAllGlobals();
});
async function mount() {
  await act(async () => {
    tree = create(
      <FleetEnrollmentControls environmentId={environmentId} label="Selected service" />,
    );
  });
}
const button = (label: string) =>
  tree!.root.findAllByType("button").find((node) => node.children.join("") === label)!;

describe("fleet enrollment settings", () => {
  it("does not enroll from pairing or discovery and requires actual operation protocol support", async () => {
    h.host.mockResolvedValue({ ...status, operationProtocol: null });
    await mount();
    expect(button("Turn on").props.disabled).toBe(true);
    expect(h.host.mock.calls.every(([, request]) => request.action === "status")).toBe(true);
    expect(h.bridge).not.toHaveBeenCalled();
    expect(JSON.stringify(tree!.toJSON())).toContain("host setup required");
  });
  it("reports an unreachable host as unavailable rather than as needing setup", async () => {
    h.host.mockRejectedValue(new Error("offline"));
    await mount();
    const text = JSON.stringify(tree!.toJSON());
    expect(text).toContain("host status unavailable");
    expect(text).not.toContain("host setup required");
    expect(button("Turn on").props.disabled).toBe(true);
    h.host.mockResolvedValue(status);
    await act(async () => {
      tree!.root
        .findAllByType("button")
        .find((node) => node.props["aria-label"]?.startsWith("Refresh"))!
        .props.onClick();
    });
    expect(JSON.stringify(tree!.toJSON())).not.toContain("host status unavailable");
    expect(button("Turn on").props.disabled).toBe(false);
  });
  it("enrolls only the selected service after an explicit click", async () => {
    h.host.mockImplementation(async (_id, request) =>
      request.action === "enroll" ? { ...status, enrollment: request.input.enrollment } : status,
    );
    await mount();
    expect(h.bridge).not.toHaveBeenCalled();
    await act(async () => {
      button("Turn on").props.onClick();
    });
    const call = h.host.mock.calls.find(([, request]) => request.action === "enroll")!;
    expect(call[0]).toBe(environmentId);
    expect(call[1].input.enrollment).toMatchObject({
      environmentId,
      enabled: true,
      continueRunningThreads: false,
    });
    expect(h.bridge).toHaveBeenCalledWith({
      action: "enroll",
      enrollment: call[1].input.enrollment,
    });
    expect(h.host.mock.calls.some(([, request]) => request.action === "activate")).toBe(false);
  });
  it("stops native automatic dispatch before attempting to disable an offline host", async () => {
    const enrollment = {
      environmentId,
      enrollmentId: "11111111-1111-4111-8111-111111111111",
      enabled: true,
      continueRunningThreads: false,
    };
    h.state = { schema: 1, enrollments: [enrollment], campaigns: [] };
    h.host.mockImplementation(async (_id, request) => {
      if (request.action === "enroll") {
        expect(h.bridge).toHaveBeenCalledWith({
          action: "enroll",
          enrollment: { ...enrollment, enabled: false },
        });
        throw new Error("offline");
      }
      return { ...status, enrollment };
    });
    await mount();
    await act(async () => {
      button("Turn off").props.onClick();
    });
    expect(h.bridge).toHaveBeenCalledWith({
      action: "enroll",
      enrollment: { ...enrollment, enabled: false },
    });
  });
});
