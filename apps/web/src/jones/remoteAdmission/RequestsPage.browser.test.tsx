import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { TooltipProvider } from "../../components/ui/tooltip";
import { EnvironmentRequests, RequestsPage } from "./RequestsPage";
import { admissionFixture, admissionSnapshot } from "./testFixtures";
import { RemoteAdmissionCount } from "./RemoteAdmissionAttention";

const request = vi.hoisted(() => vi.fn());
let overviewIds: EnvironmentId[] = [];
vi.mock("./runtime", () => ({ remoteAdmissionRequest: request }));
vi.mock("../../state/environments", () => ({
  useEnvironmentIds: () => overviewIds,
  useEnvironments: () => ({
    isReady: true,
    environments: [
      { environmentId: EnvironmentId.make("requests-linked"), label: "Linked receiver" },
      { environmentId: EnvironmentId.make("requests-primary"), label: "Primary receiver" },
    ],
  }),
}));

it("reviews exact bound requests inline and never restores approval after a failed refresh", async () => {
  const environmentId = EnvironmentId.make("requests-decision");
  const held = admissionFixture();
  let decided = false;
  request.mockReset().mockImplementation(async (_environmentId, input) => {
    if (input.action === "decide") {
      decided = true;
      return { ...held, status: "approved" };
    }
    if (decided) throw new Error("offline private-detail");
    return admissionSnapshot();
  });
  await using screen = await render(
    <TooltipProvider>
      <EnvironmentRequests environmentId={environmentId} label="Synthetic receiver" />
    </TooltipProvider>,
  );
  expect(screen.container.textContent).toContain("1 need review");
  await act(async () => screen.container.querySelector("summary")!.click());
  expect(screen.container.querySelector("details")!.open).toBe(true);
  const approve = [...screen.container.querySelectorAll("button")].find(
    (button) => button.textContent === "Approve",
  )!;
  await act(async () => approve.click());
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "decide",
    input: { id: held.id, bindingDigest: held.bindingDigest, decision: "approve" },
  });
  expect(screen.container.textContent).toContain("Approved · awaiting dispatch");
  expect(screen.container.textContent).toContain("last known state");
  expect(screen.container.textContent).not.toContain("private-detail");
  expect(
    [...screen.container.querySelectorAll("button")].some(
      (button) => button.textContent === "Approve",
    ),
  ).toBe(false);
});

it("includes authorized linked receivers and shows inaccessible ones without asserting no requests", async () => {
  request.mockReset().mockImplementation(async (environmentId) => {
    if (environmentId === "requests-primary") throw new Error("forbidden");
    return admissionSnapshot([
      admissionFixture({ category: "evaluation_unavailable" }),
      admissionFixture({ id: "waiting", status: "dispatching" }),
    ]);
  });
  await using screen = await render(
    <TooltipProvider>
      <RequestsPage />
    </TooltipProvider>,
  );
  const linked = screen.container.querySelector('[aria-label="Requests on Linked receiver"]')!;
  const primary = screen.container.querySelector('[aria-label="Requests on Primary receiver"]')!;
  expect(linked.textContent).toContain("1 need review");
  expect(linked.textContent).toContain("Evaluator service failure");
  expect(linked.textContent).toContain("not a finding of unsafe content");
  expect(linked.textContent).toContain("Dispatch outcome unknown");
  expect(primary.textContent).toContain("inaccessible");
  expect(primary.textContent).not.toContain("No pending requests");
  expect(request.mock.calls.map(([id]) => id)).toContain("requests-linked");
});

it("bounds collapsed previews, discloses the cap and leaves full payload available", async () => {
  const long = "Synthetic long content ".repeat(100);
  request.mockReset().mockResolvedValue(
    admissionSnapshot(
      Array.from({ length: 200 }, (_, index) => {
        const record = admissionFixture({ id: `cap-${index}` });
        return {
          ...record,
          envelope: {
            ...record.envelope,
            payload: { params: { title: `Request ${index}`, message: long } },
          },
        };
      }),
    ),
  );
  await using screen = await render(
    <TooltipProvider>
      <EnvironmentRequests
        environmentId={EnvironmentId.make("requests-cap")}
        label="Capped receiver"
      />
    </TooltipProvider>,
  );
  expect(screen.container.textContent).toContain("Showing at most 200");
  const summary = screen.container.querySelector("summary")!;
  expect(summary.textContent!.length).toBeLessThan(500);
  await act(async () => summary.click());
  expect(summary.parentElement!.textContent).toContain(long);
});

it.each([
  [403, "Owner access is unavailable"],
  [404, "unsupported on Older receiver"],
])("keeps HTTP %s distinct from an empty inbox and recovers with Refresh", async (status, message) => {
  const environmentId = EnvironmentId.make(`requests-access-${status}`);
  request.mockReset().mockRejectedValue({
    _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
    status,
    message: "synthetic private response",
  });
  await using screen = await render(
    <TooltipProvider>
      <EnvironmentRequests environmentId={environmentId} label="Older receiver" />
    </TooltipProvider>,
  );
  expect(screen.container.textContent).toContain(message);
  expect(screen.container.textContent).not.toContain("No pending requests");
  expect(screen.container.textContent).not.toContain("Retrying automatically");
  expect(screen.container.textContent).not.toContain("synthetic private response");
  if (status === 403) {
    expect(screen.container.textContent).toContain("from Older receiver's own Jones Code app or signed-in browser");
    expect(screen.container.textContent).toContain("Settings → Connections");
  }
  request.mockResolvedValue(admissionSnapshot([]));
  const refresh = [...screen.container.querySelectorAll("button")].find((button) => button.textContent === "Refresh")!;
  await act(async () => refresh.click());
  expect(screen.container.textContent).toContain("No pending requests on this environment");
  expect(screen.container.querySelector('[role="alert"]')).toBeNull();
});

it("keeps owner-only receivers out of unknown attention and restores counts after Refresh", async () => {
  const environmentId = EnvironmentId.make("requests-attention-owner");
  overviewIds = [environmentId];
  request.mockReset().mockRejectedValue({ _tag: "EnvironmentOperationForbiddenError" });
  try {
    await using screen = await render(
      <TooltipProvider>
        <div data-attention><RemoteAdmissionCount /></div>
        <EnvironmentRequests environmentId={environmentId} label="Owner receiver" />
      </TooltipProvider>,
    );
    const attention = screen.container.querySelector("[data-attention]")!;
    expect(attention.textContent).toBe("");
    expect(screen.container.textContent).toContain("owner access required");
    const refresh = [...screen.container.querySelectorAll("button")].find((button) => button.textContent === "Refresh")!;
    request.mockResolvedValue(admissionSnapshot());
    await act(async () => refresh.click());
    expect(attention.textContent).toBe("1");
    request.mockRejectedValue({ _tag: "RemoteEnvironmentAuthFetchError" });
    await act(async () => refresh.click());
    expect(attention.textContent).toBe("1?");
  } finally {
    overviewIds = [];
  }
});
