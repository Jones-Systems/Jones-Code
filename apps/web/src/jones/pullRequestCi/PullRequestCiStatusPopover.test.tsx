import { act } from "../testing/nodeRenderer";
import { EnvironmentId, type PullRequestCiStatusResult } from "@t3tools/contracts";
import { type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const host = vi.hoisted(() => ({
  read: vi.fn(() => ({})),
  refresh: vi.fn(),
  data: null as PullRequestCiStatusResult | null,
}));
vi.mock("~/state/pullRequests", () => ({ pullRequestEnvironment: { ciStatus: host.read } }));
vi.mock("~/state/query", () => ({
  useEnvironmentQuery: (atom: unknown) => ({
    data: atom ? host.data : null,
    isPending: false,
    error: null,
    refresh: host.refresh,
  }),
}));
vi.mock("~/components/ui/popover", () => ({
  Popover: ({ children, ...props }: { children: ReactNode }) => (
    <div data-popover-root {...props}>
      {children}
    </div>
  ),
  PopoverPopup: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  PopoverTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  PopoverTrigger: ({ children }: { children: ReactNode }) => <button>{children}</button>,
}));
vi.mock("~/components/ui/tooltip", () => ({
  Tooltip: ({ children }: { children: ReactNode }) => children,
  TooltipTrigger: ({ render }: { render: ReactNode }) => render,
  TooltipPopup: () => null,
}));

import { PullRequestCiStatusPopover, runnerRuntime } from "./PullRequestCiStatusPopover";

type Job = PullRequestCiStatusResult["jobs"]["items"][number];
const runner = { id: 7, name: "linux-7", status: "online" as const, busy: true, labels: [] };
const now = Date.parse("2026-10-04T16:00:00Z");
const job: Job = {
  id: 1,
  runId: 2,
  repository: "Jones-Systems/example",
  name: "Tests",
  status: "in_progress",
  url: null,
  runnerName: "linux-7",
  runnerId: 7,
  startedAt: "2026-10-04T15:58:35Z",
};

describe("runner runtime", () => {
  it("uses an in-progress job assigned by runner ID", () => {
    expect(runnerRuntime(runner, [job], now)).toBe("Busy · 1m 25s");
    expect(runnerRuntime(runner, [{ ...job, runnerName: "old-name" }], now)).toBe("Busy · 1m 25s");
  });
  it.each([
    { ...job, runnerId: 8 },
    { ...job, status: "queued" as const },
    { ...job, startedAt: null },
    { ...job, startedAt: "invalid" },
    { ...job, startedAt: "2026-10-04T16:01:00Z" },
  ])("does not invent elapsed time for unmatched or invalid jobs", (input) => {
    expect(runnerRuntime(runner, [input], now)).toBe("Busy · duration unavailable");
  });
  it("keeps idle and offline inventory states", () => {
    expect(runnerRuntime({ ...runner, busy: false }, [job], now)).toBe("Idle");
    expect(runnerRuntime({ ...runner, status: "offline" }, [job], now)).toBe("Offline");
  });
});

let renderer: ReactTestRenderer | undefined;
let page: EventTarget & { visibilityState: string };
beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(now);
  page = Object.assign(new EventTarget(), { visibilityState: "visible" });
  vi.stubGlobal("document", page);
  vi.stubGlobal("window", new EventTarget());
  host.read.mockClear();
  host.refresh.mockClear();
  host.data = null;
});
afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("reads only while open and visible, and stops refreshing after closing", async () => {
  await act(() => {
    renderer = create(
      <PullRequestCiStatusPopover
        environments={[{ environmentId: EnvironmentId.make("ci-test"), label: "VPS" }]}
        scopedEnvironmentId={null}
      />,
    );
  });
  expect(host.read).not.toHaveBeenCalled();
  const changeOpen = async (open: boolean) =>
    await act(() => renderer!.root.findByProps({ "data-popover-root": true }).props.onOpenChange(open));
  await changeOpen(true);
  expect(host.read).toHaveBeenCalled();
  await act(() => vi.advanceTimersByTime(60_000));
  expect(host.refresh).toHaveBeenCalledTimes(1);
  await act(() => {
    page.visibilityState = "hidden";
    page.dispatchEvent(new Event("visibilitychange"));
  });
  host.read.mockClear();
  await act(() => vi.advanceTimersByTime(120_000));
  expect(host.read).not.toHaveBeenCalled();
  expect(host.refresh).toHaveBeenCalledTimes(1);
  await changeOpen(false);
  await act(() => {
    page.visibilityState = "visible";
    page.dispatchEvent(new Event("visibilitychange"));
    vi.advanceTimersByTime(60_000);
  });
  expect(host.refresh).toHaveBeenCalledTimes(1);
});

it("summarizes online capacity separately from offline inventory and workflow runs", async () => {
  host.data = {
    host: "github.com",
    organization: "Jones-Systems",
    accountId: "owner",
    observedAt: new Date(now).toISOString(),
    repositories: ["Jones-Systems/example"],
    scopeTruncated: false,
    jobs: {
      state: "partial",
      reasons: ["Some repositories unavailable"],
      items: [job, { ...job, id: 3, status: "queued", runnerId: null, startedAt: null }],
    },
    workflows: {
      state: "available",
      reasons: [],
      items: [
        {
          id: 4,
          repository: job.repository,
          name: "Waiting workflow",
          status: "waiting",
          url: null,
        },
      ],
    },
    runners: {
      state: "partial",
      reasons: ["Inventory truncated"],
      items: [
        { ...runner, id: 9, name: "retired", status: "offline" },
        runner,
        { ...runner, id: 8, name: "idle", busy: false },
      ],
    },
  };
  await act(() => {
    renderer = create(
      <PullRequestCiStatusPopover
        environments={[{ environmentId: EnvironmentId.make("ci-summary"), label: "VPS" }]}
        scopedEnvironmentId={null}
      />,
    );
  });
  await act(() => renderer!.root.findByProps({ "data-popover-root": true }).props.onOpenChange(true));
  const text = (node: import("react-test-renderer").ReactTestInstance): string =>
    node.children.map((child) => (typeof child === "string" ? child : text(child))).join("");
  const summary = renderer!.root.findByProps({ "aria-label": "CI summary" });
  expect(text(summary)).toBe("1Waiting jobs observed1 / 2Busy / online runners observed");
  expect(text(renderer!.root.findByProps({ "aria-label": "Online runners" }))).toContain(
    "linux-7Busy · 1m 25sidleIdle",
  );
  const offline = renderer!.root.findByType("details");
  expect(offline.props.open).not.toBe(true);
  expect(text(offline)).toContain("1 offline runnersretiredOffline");
  expect(text(renderer!.root)).toContain("1 workflows without job details");
  expect(text(renderer!.root)).toContain("Partial results");
  host.data = {
    ...host.data,
    jobs: { state: "unavailable", reasons: [], items: [] },
    runners: { state: "unavailable", reasons: [], items: [] },
  };
  await act(() =>
    renderer!.update(
      <PullRequestCiStatusPopover
        environments={[{ environmentId: EnvironmentId.make("ci-summary"), label: "VPS" }]}
        scopedEnvironmentId={null}
      />,
    ),
  );
  expect(text(renderer!.root.findByProps({ "aria-label": "CI summary" }))).toBe(
    "UnavailableWaiting jobsUnavailableBusy / online runners",
  );
});
