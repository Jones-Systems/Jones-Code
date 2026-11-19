import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  Item,
  Page,
  ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { TooltipProvider } from "../../components/ui/tooltip";
import { fallBackToLegacyReview, installReviewClient, reviewControllerFor } from "./reviewRuntime";
import { ReviewPage } from "./ReviewPage";
import { renderRequestsRouter } from "./requestsRouterTestHarness";
import { admissionFixture, deferred } from "./testFixtures";
import "../../index.css";

let routedReceiver: EnvironmentId | null = null;
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironments: () => ({
    isReady: true,
    environments: routedReceiver ? [{ environmentId: routedReceiver, displayLabel: "Laptop" }] : [],
  }),
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

it("keeps the V2 controller while a decision is pending or unknown", async () => {
  const f = setup("review-unknown-downgrade");
  const controller = reviewControllerFor(f.environmentId)!;
  const reply = deferred<never>();
  vi.mocked(f.client.mutate).mockReturnValueOnce(reply.promise);
  await controller.load();
  const effect = controller.mutate({
    action: "decide",
    input: { id: f.item.record.id, bindingDigest: f.item.record.bindingDigest, decision: "deny" },
  });
  expect(controller.getSnapshot().pending.size).toBe(1);
  expect(fallBackToLegacyReview(f.environmentId)).toBe(false);
  reply.reject(new Error("reply lost"));
  await effect;
  expect(controller.getSnapshot().uncertain.size).toBe(1);
  expect(fallBackToLegacyReview(f.environmentId)).toBe(false);
  expect(reviewControllerFor(f.environmentId)).toBe(controller);
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
});

it("reports a verified empty V2 inbox separately from a receiver requiring owner access", async () => {
  const f = setup("review-empty-vs-owner");
  vi.mocked(f.client.page).mockResolvedValue({
    ...f.page,
    records: [],
    access: [],
    counts: { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 },
  });
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  await expect.element(screen.getByText("No requests need review.")).toBeVisible();
  f.client.capabilities = async () => ({ canDecide: false, features: new Set(["reviewV2"]) });
  await act(() => reviewControllerFor(f.environmentId)!.load());
  await expect
    .element(screen.getByText("Owner access is required to review requests."))
    .toBeVisible();
  expect(screen.getByText("No requests need review.").query()).toBeNull();
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("selects cards into the real pane, keeps exact request and thread links separate, and submits duration only on approval", async () => {
  const f = setup("review-browser-exact");
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  await expect
    .element(screen.getByRole("button", { name: "Current 1" }))
    .toHaveAttribute("aria-pressed", "true");
  await expect.element(screen.getByRole("button", { name: "Done" })).toBeVisible();
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

it.each(["browser", "hash"] as const)(
  "opens exact rows and fences superseded request and access readbacks with %s history",
  async (historyType) => {
    const f = setup(`review-routed-${historyType}`);
    const first = f.item;
    const second = {
      ...first,
      record: admissionFixture({ id: "second", bindingDigest: "b".repeat(64) }),
    };
    const grant = f.page.access[0]!;
    const pending = deferred<Item>();
    f.client.page = vi.fn(async () => ({ ...f.page, records: [first, second] }));
    const lookup = vi.fn(async (id: string, _digest?: string) =>
      id === first.record.id ? pending.promise : second,
    );
    f.client.lookup = lookup;
    f.client.lookupAccess = vi.fn(async () => grant);
    routedReceiver = f.environmentId;
    try {
      await using routed = await renderRequestsRouter(historyType);
      const { screen, router } = routed;
      const clickRequest = (index: number) =>
        act(() => {
          const links = screen.container.querySelectorAll<HTMLElement>(
            '[aria-label="Open exact request"]',
          );
          links[index]!.click();
        });
      const details = screen.getByRole("region", { name: "Request details" });
      const approve = () => screen.getByRole("button", { name: "Approve", exact: true }).query();
      const deny = () => screen.getByRole("button", { name: "Deny", exact: true }).query();
      await expect.element(screen.getByRole("button", { name: "Current 1" })).toBeVisible();
      const mountedSection = screen.container.querySelector('[aria-label="Requests on Laptop"]');
      await clickRequest(0);
      await expect.element(details.getByText("Loading exact request…")).toBeVisible();
      expect(approve()).toBeNull();
      expect(deny()).toBeNull();
      await clickRequest(1);
      await expect
        .element(screen.getByRole("button", { name: "Approve", exact: true }))
        .toBeVisible();
      expect(screen.container.querySelector('[aria-label="Requests on Laptop"]')).toBe(
        mountedSection,
      );
      expect(router.state.location.search).toMatchObject({
        receiver: f.environmentId,
        request: second.record.id,
        binding: second.record.bindingDigest,
      });
      await act(() => pending.resolve(first));
      expect(lookup.mock.calls).toEqual([
        [first.record.id, first.record.bindingDigest],
        [second.record.id, second.record.bindingDigest],
      ]);
      expect(
        screen.container.querySelector("article:has(button[aria-current]) a")?.getAttribute("href"),
      ).toContain("request=second");
      await act(() =>
        router.navigate({
          to: "/requests",
          search: { receiver: f.environmentId, request: second.record.id, binding: "wrong-digest" },
        }),
      );
      await expect.element(details.getByText("Exact request unavailable.")).toBeVisible();
      expect(approve()).toBeNull();
      expect(deny()).toBeNull();
      const grantRead = deferred<typeof grant>();
      vi.mocked(f.client.lookupAccess).mockReturnValueOnce(grantRead.promise);
      await act(() =>
        router.navigate({
          to: "/requests",
          search: { receiver: f.environmentId, access: grant.id },
        }),
      );
      await expect.element(details.getByText("Loading exact access…")).toBeVisible();
      expect(screen.getByRole("button", { name: "Revoke access" }).query()).toBeNull();
      await act(() => grantRead.resolve(grant));
      await expect
        .element(details.getByRole("heading", { name: "Exact destination" }))
        .toBeVisible();
      await expect.element(screen.getByRole("button", { name: "Revoke access" })).toBeVisible();
      await act(() =>
        screen.container.querySelector<HTMLElement>('[aria-label="Open exact thread"]')!.click(),
      );
      await expect
        .element(screen.getByText(`Thread destination on ${f.environmentId}`))
        .toBeVisible();
      expect(f.client.mutate).not.toHaveBeenCalled();
    } finally {
      routedReceiver = null;
    }
  },
);

it("keeps long labels in compact rows and brings selected details into view on narrow screens", async () => {
  const f = setup("review-browser-layout");
  const record = admissionFixture({
    id: "long-operation",
    envelope: {
      ...f.item.record.envelope,
      operation: `t3_extension_${"long_operation_".repeat(12)}`,
    },
  });
  const longItem = { ...f.item, record };
  f.client.lookup = async () => longItem;
  f.client.page = vi.fn(async () => ({ ...f.page, records: [longItem], access: [] }));
  const container = document.createElement("div");
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
    { container },
  );
  const row = container.querySelector("article")!;
  const titleButton = row.querySelector("button")!;
  for (const width of [320, 900, 1440]) {
    container.style.width = `${width}px`;
    expect(titleButton.offsetWidth).toBeGreaterThan(row.offsetWidth / 2);
    expect(row.scrollWidth).toBeLessThanOrEqual(row.clientWidth);
    expect(row.offsetHeight).toBeLessThan(144);
  }
  container.style.width = "320px";
  await act(() => titleButton.click());
  await expect.element(screen.getByRole("button", { name: "Back to requests" })).toBeVisible();
  await expect.element(screen.getByRole("button", { name: "Approve", exact: true })).toBeVisible();
  await screen.getByRole("button", { name: "Back to requests" }).click();
  expect(titleButton.getBoundingClientRect().width).toBeGreaterThan(0);
});

it("opens Access at zero active scopes and keeps pending and revoked grants across pages", async () => {
  const f = setup("review-browser-access-pages");
  const pending = {
    ...f.page.access[0]!,
    id: "pending-access",
    title: "Pending destination",
    state: "pending" as const,
  };
  const revoked = {
    ...pending,
    id: "revoked-access",
    title: "Revoked destination",
    state: "revoked" as const,
  };
  f.client.page = vi.fn(async (view, cursor) => ({
    ...f.page,
    counts: { ...f.page.counts, activeAccessScopes: 0 },
    records: view === "current" ? [f.item] : [],
    access: view === "access" ? [cursor ? revoked : pending] : [],
    nextCursor: view === "access" && !cursor ? "access-page-2" : null,
  }));
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  const accessView = screen.getByRole("button", { name: "Access 0 active" });
  await accessView.click();
  await expect.element(accessView).toHaveAttribute("aria-pressed", "true");
  await expect.element(screen.getByText("Pending destination")).toBeVisible();
  await screen.getByRole("button", { name: "Load older records" }).click();
  expect(f.client.page).toHaveBeenCalledWith("access", "access-page-2");
  await expect.element(screen.getByText("Pending destination")).toBeVisible();
  await expect.element(screen.getByText("Revoked destination")).toBeVisible();
  expect(screen.container.querySelectorAll('nav button[aria-pressed="true"]')).toHaveLength(1);
});
it("freezes Access targets until keyboard focus leaves the group", async () => {
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
  await screen.getByRole("button", { name: "Access 1 active" }).click();
  const target = [...screen.container.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Pinned active target"),
  )!;
  await act(() => target.focus());
  active = [{ ...access, id: "new-active", title: "New active target" }];
  await act(() => reviewControllerFor(f.environmentId)!.load("access"));
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
] as const)(
  "shows canonical %s / %s / %s in both the compact card and detail",
  async (outcome, creation, availability, expected) => {
    const f = setup(`review-browser-outcome-${outcome}-${availability}`);
    f.item.record = { ...f.item.record, status: "dispatched" };
    f.page.counts = { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 };
    // Deliberately stale flattened state must not override retained canonical evidence.
    f.item.outcome = "awaiting";
    const initialMessage =
      outcome === "not_created" || outcome === "refused" ? "none" : "unconfirmed";
    f.item.launchResult = {
      outcome,
      stages: {
        creation,
        availability,
        initialMessage,
        preparation: "ready",
        providerProgress: "not_started",
      },
      threadId: availability === "available" ? "created-thread" : null,
      recordedAt: "2026-11-17T13:00:00Z",
      reasonCode: "synthetic_creation_evidence",
    };
    await using screen = await render(
      <TooltipProvider>
        <ReviewPage environmentId={f.environmentId} label="Laptop" />
      </TooltipProvider>,
    );
    const card = [...screen.container.querySelectorAll("button")].find((button) =>
      button.textContent?.includes("Inspect synthetic project"),
    )!;
    expect(card.textContent).toContain(expected);
    expect(card.textContent).not.toContain("Dispatched");
    await act(() => card.click());
    expect(screen.container.textContent!.split(expected).length).toBeGreaterThanOrEqual(3);
    const stages = screen.container.querySelector('[aria-label="Recorded launch stages"]')!;
    expect([...stages.querySelectorAll("dd")].map((node) => node.textContent)).toEqual([
      creation,
      availability,
      "ready",
      initialMessage,
      "not_started",
      "2026-11-17T13:00:00Z",
    ]);
    expect(
      [...screen.container.querySelectorAll("p")].some((node) =>
        node.textContent?.includes("Reason: synthetic creation evidence"),
      ),
    ).toBe(true);
    await act(() =>
      [...screen.container.querySelectorAll("summary")]
        .find((node) => node.textContent === "Bound request details")!
        .click(),
    );
    const bound = JSON.parse(screen.container.querySelector("pre")!.textContent!);
    expect(bound.record.status).toBe("dispatched");
    expect(bound.launchResult).toEqual(f.item.launchResult);
    expect(f.client.mutate).not.toHaveBeenCalled();
  },
);

it("copies exact recovery details and stage evidence without executing recovery or inventing a locator", async () => {
  const f = setup("review-browser-copy-recovery");
  f.item.record = { ...f.item.record, status: "dispatched" };
  f.page.counts = { ...f.page.counts, currentHeldAdmissions: 0, heldAdmissions: 0 };
  f.item.outcome = "unknown";
  f.item.launchResult = {
    outcome: "unknown",
    stages: {
      creation: "unknown",
      availability: "unknown",
      initialMessage: "unconfirmed",
      preparation: "ready",
      providerProgress: "unknown",
    },
    threadId: null,
    recordedAt: "2026-11-17T13:00:00Z",
    reasonCode: "receipt_read_incomplete",
  };
  using write = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
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
      .find((button) => button.textContent === "Copy recovery details")!
      .click(),
  );
  expect(write).toHaveBeenCalledTimes(1);
  const copied = write.mock.calls[0]![0];
  expect(copied).toContain(`Receiver environment ID: ${f.environmentId}`);
  expect(copied).toContain(`Admission ID: ${f.item.record.id}`);
  expect(copied).toContain(
    `${window.location.origin}/requests?receiver=${f.environmentId}&request=${f.item.record.id}&binding=${f.item.record.bindingDigest}`,
  );
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
  await using screen = await render(
    <TooltipProvider>
      <ReviewPage environmentId={f.environmentId} label="Laptop" />
    </TooltipProvider>,
  );
  const card = [...screen.container.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Inspect synthetic project"),
  )!;
  expect(card.textContent).toContain("Dispatch recorded · creation unconfirmed");
  await act(() => card.click());
  expect(screen.container.querySelector('[aria-label="Recorded launch stages"]')).toBeNull();
  expect(screen.container.textContent).toContain("Copy recovery details");
  expect(f.item.record.status).toBe("dispatched");
  expect(f.client.mutate).not.toHaveBeenCalled();
});
