import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { act, cloneElement, isValidElement, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import {
  EnvironmentId, ThreadId, ProviderInstanceId, ProviderThreadId, ProviderSessionId,
  type OrchestrationV2CurrentThreadRuntimeTarget,
} from "@t3tools/contracts";
import type { CurrentThreadRuntimeStopState } from "@t3tools/client-runtime/state/thread-continuation";
import { renderToStaticMarkup } from "react-dom/server";

import {
  resolveThreadLineageWindow,
  ThreadLineageRowList,
  ThreadRelationshipsPanel,
} from "./ThreadRelationshipsControl";

const stop = vi.hoisted(() => ({
  capture: vi.fn(),
  request: vi.fn(),
  observe: vi.fn(),
  legacyStop: vi.fn(),
  canDetach: true,
  running: true,
}));
vi.mock("../../hooks/useCurrentRuntimeStop", () => ({ useCurrentRuntimeStop: () => stop }));
vi.mock("../../state/entities", () => ({
  useThreadProjection: () => ({
    projection: { subagents: stop.running ? [{ childThreadId: null, status: "running" }] : [] },
  }),
  useProjects: () => [],
  useServerConfigs: () => new Map(),
  useThreadShells: () => [],
}));
vi.mock("../../lib/archivedThreadsState", () => ({
  useArchivedThreadSnapshots: () => ({ snapshots: [] }),
}));
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => vi.fn() }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => stop.legacyStop }));
vi.mock("@t3tools/client-runtime/state/thread-workflows", () => ({
  canDetachThreadProviderSession: () => stop.canDetach,
  resolveLatestMergeBackRun: () => null,
}));
vi.mock("@t3tools/client-runtime/state/thread-relationships", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/state/thread-relationships")>()),
  deriveThreadRelationshipGraph: () => ({ nodes: new Map() }),
  immediateThreadRelationships: () => [],
  resolveMergeBackTargetThreadId: () => null,
}));
vi.mock("../ui/menu", () => ({
  Menu: ({ children }: { children: ReactNode }) => <>{children}</>,
  MenuPopup: ({ children }: { children: ReactNode }) => <>{children}</>,
  MenuItem: ({ children, onClick }: { children: ReactNode; onClick: () => void }) => (
    <button onClick={onClick}>{children}</button>
  ),
  MenuTrigger: ({ render }: { render: ReactNode }) =>
    isValidElement(render) ? cloneElement(render) : render,
}));

const rows = Array.from({ length: 20 }, (_, index) => `row-${index}`);

function renderRowList(visibleCount: number) {
  const { visibleRows, hiddenCount } = resolveThreadLineageWindow(rows, visibleCount);
  return renderToStaticMarkup(
    <ThreadLineageRowList hiddenCount={hiddenCount} onShowMore={() => {}}>
      {visibleRows.map((row) => (
        <li key={row}>{row}</li>
      ))}
    </ThreadLineageRowList>,
  );
}

describe("thread lineage row list", () => {
  it("shows six rows before the first expansion", () => {
    const { visibleRows, hiddenCount } = resolveThreadLineageWindow(rows, 6);

    expect(visibleRows).toEqual(rows.slice(0, 6));
    expect(hiddenCount).toBe(14);
  });

  it("offers one page at a time", () => {
    expect(renderRowList(6)).toContain("Show 12 more");
    expect(renderRowList(6 + 12)).toContain("Show 2 more");
  });

  it("omits the expansion affordance when everything fits", () => {
    const markup = renderRowList(rows.length);

    expect(markup).not.toContain("more");
    expect(resolveThreadLineageWindow(rows.slice(0, 6), 6).hiddenCount).toBe(0);
  });

  it("keeps the rows in a bounded, labelled scroll region and the button outside it", () => {
    const markup = renderRowList(6);
    const list = /<ul([^>]*)>/.exec(markup)?.[1] ?? "";

    expect(list).toContain('aria-label="Related threads"');
    expect(list).toContain("max-h-[13.5rem]");
    expect(list).toContain("overflow-y-auto");
    expect(list).toContain("overscroll-contain");
    expect(markup.indexOf("</ul>")).toBeLessThan(markup.indexOf("<button"));
  });
});

const environmentId = EnvironmentId.make("detach-environment");
const threadId = ThreadId.make("detach-thread");
const ref = { environmentId, threadId };
const target = {
  driver: "codex",
  evidenceRevision: 7,
  binding: {
    threadId,
    providerThreadId: ProviderThreadId.make("current-provider-thread"),
    providerSessionId: ProviderSessionId.make("current-provider-session"),
    instanceId: ProviderInstanceId.make("current-instance"),
    runtimeGeneration: "current-generation",
    nativeThreadId: "current-native-thread",
  },
} satisfies OrchestrationV2CurrentThreadRuntimeTarget;
const pending = {
  status: "pending",
  commandAccepted: true,
  queueFenceInstalled: true,
  reason: "Native runtime closure is still pending.",
} satisfies CurrentThreadRuntimeStopState;
let renderer: ReactTestRenderer | undefined;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  stop.canDetach = true;
  stop.running = true;
  stop.capture.mockReset().mockReturnValue({ status: "current", target });
  stop.request.mockReset().mockResolvedValue(pending);
  stop.observe.mockReset().mockResolvedValue(null);
  stop.legacyStop.mockReset().mockResolvedValue({ _tag: "Success" });
});
afterEach(async () => {
  await act(async () => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});
async function mountPanel() {
  await act(async () => {
    renderer = create(<ThreadRelationshipsPanel environmentId={environmentId} threadId={threadId} />);
  });
  return renderer!;
}
function detachButton() {
  return renderer!.root.findAllByType("button").find((button) =>
    button.children.some((child) => typeof child === "string" && /disconnect/i.test(child)),
  )!;
}
function panelText() {
  return JSON.stringify(renderer!.toJSON());
}
async function disconnect() {
  await act(async () => {
    await detachButton().props.onClick();
  });
}

describe("current runtime session disconnect", () => {
  it("targets the captured current owner and exposes pending closure", async () => {
    await mountPanel();
    await disconnect();

    expect(stop.request).toHaveBeenCalledExactlyOnceWith(ref, target);
    expect(stop.legacyStop).not.toHaveBeenCalled();
    expect(panelText()).toContain("Native runtime closure is still pending.");
    expect(panelText()).not.toContain("Runtime stopped");
  });

  it("observes the saved operation before capturing another current owner", async () => {
    stop.observe.mockResolvedValue({ ...pending, status: "unknown", reason: "Original stop is unknown." });
    await mountPanel();
    await disconnect();
    await disconnect();

    expect(stop.observe).toHaveBeenCalledTimes(2);
    expect(stop.capture).not.toHaveBeenCalled();
    expect(stop.request).not.toHaveBeenCalled();
    expect(panelText()).toContain("Original stop is unknown.");
  });

  it("reconciles the original stop after remount instead of requesting a new target", async () => {
    await mountPanel();
    await disconnect();
    await act(async () => renderer?.unmount());
    stop.observe.mockResolvedValue({ ...pending, status: "unknown", reason: "Original command still unknown." });
    await mountPanel();
    await disconnect();

    expect(stop.request).toHaveBeenCalledTimes(1);
    expect(stop.capture).toHaveBeenCalledTimes(1);
    expect(panelText()).toContain("Original command still unknown.");
  });

  it("keeps pending status reachable after the attachment disappears", async () => {
    await mountPanel();
    await disconnect();
    stop.canDetach = false;
    stop.running = false;
    await act(async () => {
      renderer!.update(<ThreadRelationshipsPanel environmentId={environmentId} threadId={threadId} />);
    });
    expect(panelText()).toContain("Disconnect pending");
    expect(panelText()).toContain("Check disconnect status");
    stop.observe.mockResolvedValue(pending);
    await disconnect();
    expect(stop.request).toHaveBeenCalledTimes(1);
  });

  it("reports unavailable runtime evidence without dispatching a stop", async () => {
    stop.capture.mockReturnValue({ status: "unavailable", reason: "Runtime is not resident." });
    await mountPanel();
    await disconnect();

    expect(stop.request).not.toHaveBeenCalled();
    expect(panelText()).toContain("Runtime is not resident.");
    expect(panelText()).not.toContain("Runtime stopped");
  });

  it("clears busy after a lost response and reconciles on the next action", async () => {
    stop.request.mockRejectedValueOnce(new Error("Stop response was lost."));
    await mountPanel();
    await disconnect();

    expect(panelText()).toContain("Stop response was lost.");
    expect(renderer!.root.findAllByType("button").find(
      (button) => button.props["aria-label"] === "More thread actions",
    )!.props.disabled).toBe(false);
    stop.observe.mockResolvedValue({ ...pending, status: "unknown", reason: "Original command pending." });
    await disconnect();
    expect(stop.request).toHaveBeenCalledTimes(1);
    expect(panelText()).toContain("Original command pending.");
  });

  it("shows rejection and limited attachment evidence without claiming full shutdown", async () => {
    stop.request.mockResolvedValue({
      status: "rejected", commandAccepted: false, queueFenceInstalled: false,
      reason: "Only the pooled attachment disconnected; native shutdown is unproved.",
    });
    await mountPanel();
    await disconnect();

    expect(panelText()).toContain("Disconnect rejected");
    expect(panelText()).toContain("native shutdown is unproved.");
    expect(panelText()).not.toContain("Runtime stopped");
  });

  it("reports stopped only for the qualified typed outcome", async () => {
    stop.request.mockResolvedValue({ ...pending, status: "stopped", reason: null });
    await mountPanel();
    await disconnect();
    expect(panelText()).toContain("Runtime stopped");
  });
});
