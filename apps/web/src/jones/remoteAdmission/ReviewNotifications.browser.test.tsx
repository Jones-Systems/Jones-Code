import "../../index.css";
import type { ReactNode } from "react";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  ChangeBatch,
  Item,
  Page,
  ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { TooltipProvider } from "../../components/ui/tooltip";
import { RemoteAdmissionNotifications } from "./RemoteAdmissionNotifications";
import { installReviewClient, reviewControllerFor } from "./reviewRuntime";
import { admissionFixture } from "./testFixtures";
import { requestHref, threadHref } from "./navigation";

const mocks = vi.hoisted(() => ({ add: vi.fn(), close: vi.fn() }));
let receiver = EnvironmentId.make("review-notifications-initial");
vi.mock("../../components/ui/toast", () => ({ toastManager: mocks }));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadShell: () => null,
  useProject: () => null,
}));
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironments: () => ({
    environments: [{ environmentId: receiver, label: "Laptop", displayLabel: "Laptop" }],
  }),
}));
vi.mock("../../hooks/useSettings", async (original) => ({
  ...(await original<typeof import("../../hooks/useSettings")>()),
  useClientSettings: (
    select: (settings: { inAppNotificationsEnabled: boolean; timestampFormat: string }) => unknown,
  ) => select({ inAppNotificationsEnabled: true, timestampFormat: "12-hour" }),
}));
vi.mock("./useAdmission", () => ({
  useAdmission: () => {
    throw new Error("A registered V2 receiver must not use the legacy client");
  },
}));

function setup(id: string) {
  vi.clearAllMocks();
  mocks.add.mockImplementation(() => `toast-${mocks.add.mock.calls.length}`);
  receiver = EnvironmentId.make(id);
  const item: Item = {
    record: admissionFixture({ id: `${id}-request` }),
    attempt: "original",
    blocked: false,
    accessId: null,
    grantEligible: true,
    outcome: null,
    availability: "unknown",
    threadId: null,
    messageStage: null,
    reason: null,
    canRetry: false,
    canRecheck: false,
  };
  let records = [item];
  const page = (): Page => ({
    revision: "snapshot-1",
    policyRevision: "synthetic-policy",
    records,
    access: [],
    nextCursor: null,
    recordCount: records.length,
    counts: {
      currentHeldAdmissions: records.length,
      blockedHeldAdmissions: 0,
      heldAdmissions: records.length,
      inFlightAdmissions: 0,
      failedAdmissions: 0,
      unresolvedAdmissions: 0,
      activeAccessScopes: 0,
      activeThreads: 0,
    },
  });
  let deliver: ((batch: ChangeBatch) => void) | undefined;
  const client: ReviewClient = {
    capabilities: async () => ({
      canDecide: true,
      features: new Set(["reviewV2", "grants", "changeFeed"]),
    }),
    page: vi.fn(async (view) => ({ ...page(), records: view === "current" ? records : [] })),
    lookup: async () => item,
    lookupAccess: async () => {
      throw new Error("No access selected");
    },
    mutate: vi.fn(async () => ({})),
    head: vi.fn(async () => "current-head"),
    changes: vi.fn(
      (_after, signal) =>
        new Promise<ChangeBatch>((resolve, reject) => {
          deliver = resolve;
          signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        }),
    ),
  };
  installReviewClient(receiver, client);
  const controller = reviewControllerFor(receiver)!;
  return {
    item,
    client,
    controller,
    setRecords(next: Item[]) {
      records = next;
    },
    deliver(batch: ChangeBatch) {
      if (!deliver) throw new Error("No pending feed read");
      deliver(batch);
    },
  };
}
function body(index: number): ReactNode {
  return mocks.add.mock.calls[index]![0].data.bodyContent;
}

it("collapses an incoming row independently and uses compact controls and an exact-request arrow", async () => {
  const f = setup("review-notifications-incoming");
  await f.controller.load();
  await using _root = await render(<RemoteAdmissionNotifications />);
  await vi.waitFor(() => expect(mocks.add).toHaveBeenCalledTimes(1));
  await using screen = await render(<TooltipProvider>{body(0)}</TooltipProvider>);
  const title = screen.container.querySelector<HTMLButtonElement>("article button[aria-expanded]")!;
  expect(screen.container.textContent).toContain("1 incoming approval request");
  expect(
    screen.container.querySelector('[aria-label="Open exact request"]')?.getAttribute("href"),
  ).toBe(requestHref(receiver, f.item.record.id, f.item.record.bindingDigest));
  const duration = screen.container.querySelector<HTMLButtonElement>('[aria-label="Duration"]')!;
  // Geometry assertions must exercise the app stylesheet, not browser-default buttons.
  expect(getComputedStyle(duration).display).toBe("inline-flex");
  const buttons = [...screen.container.querySelectorAll("button")];
  const approve = buttons.find((button) => button.textContent === "Approve")!;
  const deny = buttons.find((button) => button.textContent === "Deny")!;
  expect(duration.textContent).toContain("1 day");
  expect(
    Math.abs(approve.getBoundingClientRect().height - deny.getBoundingClientRect().height),
  ).toBeLessThanOrEqual(1);
  const controlSizing = [duration, deny, approve].map((control) => {
    const style = getComputedStyle(control);
    return {
      label: control.textContent,
      height: control.getBoundingClientRect().height,
      minHeight: style.minHeight,
      lineHeight: style.lineHeight,
      fontSize: style.fontSize,
      padding: style.padding,
    };
  });
  expect(
    Math.abs(duration.getBoundingClientRect().height - approve.getBoundingClientRect().height),
    JSON.stringify(controlSizing),
  ).toBeLessThanOrEqual(1);
  await act(() => title.click());
  expect(title.getAttribute("aria-expanded")).toBe("false");
  expect(screen.container.querySelector('[aria-label="Duration"]')).toBeNull();
  expect(f.client.mutate).not.toHaveBeenCalled();
  await act(() => title.click());
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent === "Approve")!
      .click(),
  );
  expect(f.client.mutate).toHaveBeenCalledWith({
    action: "decide",
    input: {
      id: f.item.record.id,
      bindingDigest: f.item.record.bindingDigest,
      decision: "approve",
      grant: { durationSeconds: 86400 },
    },
  });
});

it("keeps creation outcomes separate, routes available threads and unknown requests exactly, and collapses each entry", async () => {
  const f = setup("review-notifications-created");
  f.setRecords([]);
  await f.controller.load();
  await using _root = await render(<RemoteAdmissionNotifications />);
  await vi.waitFor(() => expect(f.client.changes).toHaveBeenCalled());
  const created: Item = {
    ...f.item,
    record: { ...f.item.record, status: "dispatched" },
    outcome: "created",
    availability: "available",
    threadId: "confirmed-destination",
    messageStage: "Initial message accepted",
  };
  const unknown: Item = {
    ...f.item,
    record: admissionFixture({ id: "unknown-launch", status: "dispatched" }),
    outcome: "unknown",
    reason: "Creation outcome unavailable",
  };
  await act(() =>
    f.deliver({ cursor: "after-results", revision: "snapshot-2", results: [created, unknown] }),
  );
  await vi.waitFor(() => expect(mocks.add).toHaveBeenCalledTimes(1));
  await using screen = await render(<TooltipProvider>{body(0)}</TooltipProvider>);
  expect(screen.container.textContent).toContain("1 thread created · 1 unknown");
  expect(
    screen.container.querySelector('[aria-label="Open exact thread"]')?.getAttribute("href"),
  ).toBe(threadHref(receiver, "confirmed-destination"));
  expect(
    screen.container.querySelector('[aria-label="Open exact request"]')?.getAttribute("href"),
  ).toBe(requestHref(receiver, unknown.record.id, unknown.record.bindingDigest));
  const titles = [
    ...screen.container.querySelectorAll<HTMLButtonElement>("article button[aria-expanded]"),
  ];
  await act(() => titles[0]!.click());
  expect(screen.container.textContent).not.toContain("Initial message accepted");
  expect(screen.container.textContent).toContain("Creation outcome unavailable");
  expect(titles[1]!.getAttribute("aria-expanded")).toBe("true");
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("retains incoming dismissal across remounts and removals, and re-raises for a new immutable request", async () => {
  const f = setup("review-notifications-dismiss");
  await f.controller.load();
  await using root = await render(<RemoteAdmissionNotifications />);
  await vi.waitFor(() => expect(mocks.add).toHaveBeenCalledTimes(1));
  await act(() => mocks.add.mock.calls[0]![0].data.onClose());
  await root.unmount();
  await using _again = await render(<RemoteAdmissionNotifications />);
  expect(mocks.add).toHaveBeenCalledTimes(1);
  f.setRecords([]);
  await act(() => f.controller.load());
  expect(mocks.add).toHaveBeenCalledTimes(1);
  f.setRecords([{ ...f.item, record: admissionFixture({ id: "unseen-binding" }) }]);
  await act(() => f.controller.load());
  expect(mocks.add).toHaveBeenCalledTimes(2);
});
