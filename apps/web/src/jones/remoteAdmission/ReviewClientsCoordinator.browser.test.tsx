import { AVAILABLE_CONNECTION_STATE } from "@t3tools/client-runtime/connection";
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
import { reviewControllerFor } from "./reviewRuntime";
import { deferred } from "./testFixtures";

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
