import { AVAILABLE_CONNECTION_STATE } from "@t3tools/client-runtime/connection";
import type {
  ChangeBatch,
  Item,
  ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { EnvironmentId } from "@t3tools/contracts";
import {
  REMOTE_ADMISSION_REVIEW_VERSION,
  type RemoteAdmissionReviewCapabilities,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import { AsyncResult, Atom } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { AppAtomRegistryProvider, appAtomRegistry } from "../../rpc/atomRegistry";
import { ReviewClientsCoordinator } from "./ReviewClientsCoordinator";
import { installReviewClient, reviewControllerFor } from "./reviewRuntime";
import { admissionFixture, deferred } from "./testFixtures";

const { capabilities, refresh } = vi.hoisted(() => ({ capabilities: vi.fn(), refresh: vi.fn() }));
const receiver = EnvironmentId.make("review-reconnect");
const environment = {
  environmentId: receiver,
  entry: {},
  connection: { phase: "connected" },
  serverConfig: { environment: { serverVersion: "synthetic" } },
};
const connected = (generation: number) =>
  AsyncResult.success({ ...AVAILABLE_CONNECTION_STATE, phase: "connected" as const, generation });
const connection = Atom.make(connected(1));
const supported = {
  version: REMOTE_ADMISSION_REVIEW_VERSION,
  features: { reviewV2: true, grants: true, conflicts: true, changeFeed: false },
  principal: { canDecide: false },
} satisfies RemoteAdmissionReviewCapabilities;

vi.mock("../../connection/catalog", async (original) => {
  const actual = await original<typeof import("../../connection/catalog")>();
  return { environmentCatalog: { ...actual.environmentCatalog, stateAtom: () => connection } };
});
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  readEnvironment: () => environment,
  useEnvironments: () => ({ environments: [environment] }),
}));
vi.mock("./reviewTransport", () => ({ createReviewTransport: () => ({ capabilities }) }));
vi.mock("./useAdmission", () => ({
  admissionStoreFor: () => ({ refresh }),
  hasLegacyReviewBarrier: () => false,
}));

it("downgrades only for the current connection and restores the same V2 controller on reconnect", async () => {
  capabilities.mockResolvedValue(supported);
  await using _screen = await render(
    <AppAtomRegistryProvider>
      <ReviewClientsCoordinator />
    </AppAtomRegistryProvider>,
  );
  await vi.waitFor(() =>
    expect(reviewControllerFor(receiver)?.getSnapshot().loadFailed).toBe(true),
  );
  const original = reviewControllerFor(receiver);
  expect(original?.getSnapshot().error).toContain("Owner access is required");
  expect(refresh).not.toHaveBeenCalled();

  const stale = deferred<RemoteAdmissionReviewCapabilities>();
  capabilities.mockReturnValueOnce(stale.promise);
  await act(() => appAtomRegistry.set(connection, connected(2)));
  await act(() => appAtomRegistry.set(connection, connected(3)));
  const missingEndpoint = { _tag: "RemoteEnvironmentAuthUndeclaredStatusError", status: 404 };
  await act(() => stale.reject(missingEndpoint));
  expect(reviewControllerFor(receiver)).toBe(original);

  capabilities.mockRejectedValue(missingEndpoint);
  await act(() => appAtomRegistry.set(connection, connected(4)));
  await vi.waitFor(() => expect(reviewControllerFor(receiver)).toBeNull());
  expect(refresh).toHaveBeenCalledTimes(1);

  capabilities.mockResolvedValue(supported);
  await act(() => appAtomRegistry.set(connection, connected(5)));
  await vi.waitFor(() => expect(reviewControllerFor(receiver)).toBe(original));
});

it("restarts the feed on the new connection even if the old poll has not settled", async () => {
  const id = EnvironmentId.make("review-reconnect-stale-feed");
  environment.environmentId = id;
  capabilities.mockResolvedValue({
    ...supported,
    features: { ...supported.features, changeFeed: true },
    principal: { canDecide: true },
  });
  const held: Item = {
    record: admissionFixture({ id: "held-during-route-change" }),
    attempt: "original",
    blocked: false,
    accessId: null,
    grantEligible: false,
    outcome: null,
    availability: "unknown",
    threadId: null,
    messageStage: null,
    reason: null,
    canRetry: false,
    canRecheck: false,
  };
  let incoming: readonly Item[] = [];
  const old = deferred<ChangeBatch>();
  const changes = vi.fn<ReviewClient["changes"]>(
    (_cursor, signal) =>
      new Promise<never>((_resolve, reject) =>
        signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true }),
      ),
  );
  changes.mockReturnValueOnce(old.promise);
  const client: ReviewClient = {
    capabilities: async () => ({ canDecide: true, features: new Set(["reviewV2", "changeFeed"]) }),
    page: async (view) => ({
      revision: "snapshot",
      policyRevision: "synthetic-policy",
      records: view === "current" ? incoming : [],
      access: [],
      recordCount: view === "current" ? incoming.length : 0,
      nextCursor: null,
      counts: {
        currentHeldAdmissions: incoming.length,
        blockedHeldAdmissions: 0,
        heldAdmissions: incoming.length,
        inFlightAdmissions: 0,
        failedAdmissions: 0,
        unresolvedAdmissions: 0,
        activeAccessScopes: 0,
        activeThreads: 0,
      },
    }),
    head: async () => "retained-head",
    changes,
    lookup: vi.fn(),
    lookupAccess: vi.fn(),
    mutate: vi.fn(),
  };
  installReviewClient(id, client);
  const controller = reviewControllerFor(id)!;
  await using _screen = await render(
    <AppAtomRegistryProvider>
      <ReviewClientsCoordinator />
    </AppAtomRegistryProvider>,
  );
  try {
    await vi.waitFor(() => expect(changes).toHaveBeenCalledTimes(1));
    await controller.load("done");
    incoming = [held];
    await act(() => appAtomRegistry.set(connection, connected(6)));
    await vi.waitFor(() => expect(changes).toHaveBeenCalledTimes(2));
    expect(changes.mock.calls[0]?.[1].aborted).toBe(true);
    await act(() => old.reject(new Error("old transport closed")));
    expect(controller.getSnapshot()).toMatchObject({ view: "done", incoming: [held], error: null });
    expect(client.mutate).not.toHaveBeenCalled();
  } finally {
    old.reject(new Error("fixture closed"));
    controller.stopResults();
    environment.environmentId = receiver;
  }
});
