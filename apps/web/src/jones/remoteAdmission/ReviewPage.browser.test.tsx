import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  Item,
  Page,
  ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { TooltipProvider } from "../../components/ui/tooltip";
import { installReviewClient, reviewControllerFor } from "./reviewRuntime";
import { ReviewPage } from "./ReviewPage";
import { admissionFixture } from "./testFixtures";

vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironments: () => ({ environments: [] }),
}));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadShell: () => null,
}));
function setup(receiver: string) {
  const record = admissionFixture();
  const item: Item = {
    record,
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
  const page: Page = {
    revision: "snapshot-1",
    policyRevision: record.envelope.receiverPolicy.revision,
    counts: {
      currentHeldAdmissions: 1,
      blockedHeldAdmissions: 0,
      heldAdmissions: 1,
      inFlightAdmissions: 0,
      failedAdmissions: 0,
      unresolvedAdmissions: 0,
      activeAccessScopes: 1,
      activeThreads: 1,
    },
    records: [item],
    access: [
      {
        id: "access-1",
        generation: 3,
        state: "active",
        title: "Exact destination",
        origin: "Synthetic VPS",
        destinationThreadId: "destination",
        expiresAt: "2026-11-17T15:00:00Z",
      },
    ],
    nextCursor: null,
    recordCount: 1,
  };
  const client: ReviewClient = {
    capabilities: async () => ({
      canDecide: true,
      features: new Set(["reviewV2", "grants", "managedAccess", "grantRestore", "history"]),
    }),
    page: vi.fn(async (view) =>
      view === "access" || view === "inflight" || view === "unresolved"
        ? { ...page, records: [], access: [] }
        : page,
    ),
    lookup: async () => item,
    lookupAccess: vi.fn(async () => {
      throw new Error("Access unavailable");
    }),
    mutate: vi.fn(async () => ({})),
    head: async () => "head",
    changes: vi.fn(),
  };
  const environmentId = EnvironmentId.make(receiver);
  installReviewClient(environmentId, client);
  return { environmentId, client, item, page };
}
it("selects cards into the real pane, keeps exact request and thread links separate, and submits duration only on approval", async () => {
  const f = setup("review-browser-exact");
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  expect(screen.container.textContent).toContain("Current · 1");
  expect(
    [...screen.container.querySelectorAll('[role="tab"]')].find(
      (tab) => tab.textContent === "Done",
    ),
  ).toBeTruthy();
  const exact = screen.container.querySelector<HTMLAnchorElement>(
    '[aria-label="Open exact request"]',
  )!;
  expect(exact.getAttribute("href")).toContain(
    `receiver=${f.environmentId}&request=synthetic-request&binding=${f.item.record.bindingDigest}`,
  );
  expect(
    screen.container.querySelector('[aria-label="Open exact thread"]')?.getAttribute("href"),
  ).toBe(`/${f.environmentId}/destination`);
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Inspect synthetic project"))!
      .click(),
  );
  expect(f.client.mutate).not.toHaveBeenCalled();
  expect(screen.container.textContent).toContain("1 day");
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
it("keeps access selection local and shows full Choose Duration before an explicit update", async () => {
  const f = setup("review-browser-access");
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Exact destination"))!
      .click(),
  );
  expect(screen.container.textContent).toContain("Choose Duration");
  expect(f.client.mutate).not.toHaveBeenCalled();
  const update = [...screen.container.querySelectorAll("button")].find(
    (button) => button.textContent === "Update access",
  )!;
  expect(update.disabled).toBe(true);
  const revoke = [...screen.container.querySelectorAll("button")].find(
    (button) => button.textContent === "Revoke access",
  )!;
  await act(() => revoke.click());
  expect(f.client.mutate).toHaveBeenCalledWith(
    expect.objectContaining({ action: "revoke", id: "access-1", expectedGeneration: 3 }),
  );
});

it("retains focused action targets through a removal and insertion until focus leaves", async () => {
  const f = setup("review-browser-focus-freeze");
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  const target = [...screen.container.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Inspect synthetic project"),
  )!;
  await act(() => target.focus());
  const next = {
    ...f.item,
    record: admissionFixture({
      id: "new-arrival",
      envelope: { ...f.item.record.envelope, payload: { params: { title: "New arrival" } } },
    }),
  };
  f.client.page = vi.fn(async (view) => ({
    ...f.page,
    records: view === "current" ? [next] : [],
    access: [],
  }));
  await act(() => reviewControllerFor(f.environmentId)!.load());
  expect(target.isConnected).toBe(true);
  expect(target.getAttribute("aria-disabled")).toBe("true");
  expect(screen.container.textContent).not.toContain("New arrival");
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent === "Refresh")!
      .focus(),
  );
  expect(screen.container.textContent).toContain("New arrival");
});

it("opens Manage Access for an exact offpage grant without leaving Requests", async () => {
  const f = setup("review-browser-offpage");
  const access = f.page.access[0]!;
  f.item.blocked = true;
  f.item.accessId = "offpage-grant";
  f.client.page = vi.fn(async (view) => ({
    ...f.page,
    records: view === "current" ? [f.item] : [],
    access: [],
  }));
  f.client.lookupAccess = vi.fn(async () => ({ ...access, id: "offpage-grant" }));
  const before = window.location.href;
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Inspect synthetic project"))!
      .click(),
  );
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent === "Manage Access")!
      .click(),
  );
  expect(f.client.lookupAccess).toHaveBeenCalledWith("offpage-grant");
  expect(screen.container.textContent).toContain("Choose Duration");
  expect(screen.container.textContent).toContain("Update access");
  expect(window.location.href).toBe(before);
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("shows collapsible Current groups with independently paged active and unresolved cards", async () => {
  const f = setup("review-browser-current-groups");
  const active = { ...f.page.access[0]!, id: "current-active-access", title: "Active destination" };
  const unknown: Item = {
    ...f.item,
    record: admissionFixture({ id: "unknown-in-current", status: "dispatching" }),
    outcome: "unknown",
    threadId: null,
  };
  f.client.lookupAccess = vi.fn(async () => active);
  f.client.page = vi.fn(async (view, cursor) => ({
    ...f.page,
    records: view === "current" ? [f.item] : view === "unresolved" ? [unknown] : [],
    access: view === "access" ? [{ ...active, id: cursor ? "next-access" : active.id }] : [],
    nextCursor: view === "access" && !cursor ? "access-page-2" : null,
  }));
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  const progress = [...screen.container.querySelectorAll("details")].find(
    (node) => node.querySelector("summary")?.textContent === "In progress",
  )!;
  const awaiting = [...screen.container.querySelectorAll("details")].find(
    (node) => node.querySelector("summary")?.textContent === "Awaiting confirmation",
  )!;
  expect(progress.open).toBe(true);
  expect(awaiting.open).toBe(true);
  expect(progress.textContent).toContain("Active destination");
  expect(awaiting.textContent).toContain("Outcome unknown");
  expect(screen.container.querySelector('[role="tab"][aria-selected="true"]')?.textContent).toBe(
    "Current · 1",
  );
  await act(() => progress.querySelector("summary")!.click());
  expect(progress.open).toBe(false);
  await act(() => progress.querySelector("summary")!.click());
  const activeButton = [...progress.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Active destination"),
  )!;
  await act(() => activeButton.click());
  expect(f.client.lookupAccess).toHaveBeenCalledWith(active.id);
  expect(screen.container.textContent).toContain("Update access");
  const more = [...progress.querySelectorAll("button")].find(
    (button) => button.textContent === "Load more active access",
  )!;
  await act(() => more.click());
  expect(f.client.page).toHaveBeenCalledWith("access", "access-page-2");
  expect(screen.container.querySelector('[role="tab"][aria-selected="true"]')?.textContent).toBe(
    "Current · 1",
  );
});

it("freezes Current active group targets until keyboard focus leaves the group", async () => {
  const f = setup("review-browser-group-freeze");
  const access = { ...f.page.access[0]!, title: "Pinned active target" };
  let active = [access];
  f.client.page = vi.fn(async (view) => ({
    ...f.page,
    records: view === "current" ? [f.item] : [],
    access: view === "access" ? active : [],
  }));
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  const target = [...screen.container.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Pinned active target"),
  )!;
  await act(() => target.focus());
  active = [{ ...access, id: "new-active", title: "New active target" }];
  await act(() => reviewControllerFor(f.environmentId)!.loadGroup("access"));
  expect(target.isConnected).toBe(true);
  expect(target.getAttribute("aria-disabled")).toBe("true");
  expect(screen.container.textContent).not.toContain("New active target");
  await act(() =>
    [...screen.container.querySelectorAll("button")]
      .find((button) => button.textContent === "Refresh")!
      .focus(),
  );
  expect(screen.container.textContent).toContain("New active target");
});

it.each([
  ["not_created", "not_created", "unknown", "Not created"],
  ["unknown", "unknown", "unknown", "Outcome unknown · do not resend"],
  ["refused", "refused", "unknown", "Creation failed"],
  ["created", "confirmed", "available", "Created"],
  ["created", "confirmed", "deleted", "Created then deleted"],
  ["created", "confirmed", "unknown", "Creation confirmed · availability unknown"],
] as const)("shows canonical %s / %s / %s in both the compact card and detail", async (outcome, creation, availability, expected) => {
  const f = setup(`review-browser-outcome-${outcome}-${availability}`);
  f.item.record = { ...f.item.record, status: "dispatched" };
  f.page.counts = { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 };
  // Deliberately stale flattened state must not override retained canonical evidence.
  f.item.outcome = "awaiting";
  const initialMessage = outcome === "not_created" || outcome === "refused" ? "none" : "unconfirmed";
  f.item.launchResult = {
    outcome, stages: { creation, availability, initialMessage, preparation: "ready", providerProgress: "not_started" },
    threadId: availability === "available" ? "created-thread" : null,
    recordedAt: "2026-11-17T13:00:00Z", reasonCode: "synthetic_creation_evidence",
  };
  await using screen = await render(<TooltipProvider><ReviewPage environmentId={f.environmentId} label="Laptop" /></TooltipProvider>);
  const card = [...screen.container.querySelectorAll("button")].find((button) => button.textContent?.includes("Inspect synthetic project"))!;
  expect(card.textContent).toContain(expected);
  expect(card.textContent).not.toContain("Dispatched");
  await act(() => card.click());
  expect(screen.container.textContent!.split(expected).length).toBeGreaterThanOrEqual(3);
  const stages = screen.container.querySelector('[aria-label="Recorded launch stages"]')!;
  expect([...stages.querySelectorAll("dd")].map((node) => node.textContent)).toEqual([creation, availability, "ready", initialMessage, "not_started", "2026-11-17T13:00:00Z"]);
  expect([...screen.container.querySelectorAll("p")].some((node) => node.textContent?.includes("Reason: synthetic creation evidence"))).toBe(true);
  await act(() => [...screen.container.querySelectorAll("summary")].find((node) => node.textContent === "Bound request details")!.click());
  const bound = JSON.parse(screen.container.querySelector("pre")!.textContent!);
  expect(bound.record.status).toBe("dispatched");
  expect(bound.launchResult).toEqual(f.item.launchResult);
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("copies exact recovery details and stage evidence without executing recovery or inventing a locator", async () => {
  const f = setup("review-browser-copy-recovery");
  f.item.record = { ...f.item.record, status: "dispatched" };
  f.page.counts = { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 };
  f.item.outcome = "unknown";
  f.item.launchResult = {
    outcome: "unknown", stages: { creation: "unknown", availability: "unknown", initialMessage: "unconfirmed", preparation: "ready", providerProgress: "unknown" },
    threadId: null, recordedAt: "2026-11-17T13:00:00Z", reasonCode: "receipt_read_incomplete",
  };
  using write = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
  await using screen = await render(<TooltipProvider><ReviewPage environmentId={f.environmentId} label="Laptop" /></TooltipProvider>);
  await act(() => [...screen.container.querySelectorAll("button")].find((button) => button.textContent?.includes("Inspect synthetic project"))!.click());
  await act(() => [...screen.container.querySelectorAll("button")].find((button) => button.textContent === "Copy recovery details")!.click());
  expect(write).toHaveBeenCalledTimes(1);
  const copied = write.mock.calls[0]![0];
  expect(copied).toContain(`Receiver environment ID: ${f.environmentId}`);
  expect(copied).toContain(`Admission ID: ${f.item.record.id}`);
  expect(copied).toContain(`${window.location.origin}/requests?receiver=${f.environmentId}&request=${f.item.record.id}&binding=${f.item.record.bindingDigest}`);
  expect(copied).toContain("Outcome unknown · do not resend");
  expect(copied).toContain(JSON.stringify(f.item.launchResult));
  expect(copied).toContain("receipt_read_incomplete");
  expect(copied).toContain("No original-request inspection locator was returned");
  expect(copied).toContain("Review this exact request in the receiving owner's Requests UI");
  expect(copied).toContain("Reading or copying these details executes nothing");
  expect(copied).not.toContain("t3_remote_admission_recovery_status");
  expect(copied).not.toContain("admission-retry:");
  expect(screen.container.textContent).toContain("Recovery details copied");
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("does not turn a legacy dispatched launch without result evidence into confirmed creation", async () => {
  const f = setup("review-browser-dispatch-without-result");
  f.item.record = { ...f.item.record, status: "dispatched" };
  f.page.counts = { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 };
  await using screen = await render(<TooltipProvider><ReviewPage environmentId={f.environmentId} label="Laptop" /></TooltipProvider>);
  const card = [...screen.container.querySelectorAll("button")].find((button) => button.textContent?.includes("Inspect synthetic project"))!;
  expect(card.textContent).toContain("Dispatch recorded · creation unconfirmed");
  await act(() => card.click());
  expect(screen.container.querySelector('[aria-label="Recorded launch stages"]')).toBeNull();
  expect(screen.container.textContent).toContain("Copy recovery details");
  expect(f.item.record.status).toBe("dispatched");
  expect(f.client.mutate).not.toHaveBeenCalled();
});
