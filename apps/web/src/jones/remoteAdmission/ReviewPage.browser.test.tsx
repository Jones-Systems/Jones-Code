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

vi.mock("../../state/environments", () => ({ useEnvironments: () => ({ environments: [] }) }));
vi.mock("../../state/entities", () => ({ useThreadShell: () => null }));
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
