import { useSyncExternalStore } from "react";
import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createAdmissionStore } from "./store";
import { admissionFixture, admissionSnapshot } from "./testFixtures";
import { RemoteAdmissionNotifications } from "./RemoteAdmissionNotifications";

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  update: vi.fn(),
  close: vi.fn(),
  navigate: vi.fn(),
}));
const environmentId = EnvironmentId.make("notification-receiver");
let store: ReturnType<typeof createAdmissionStore>;
vi.mock("../../components/ui/toast", () => ({ toastManager: mocks }));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: [{ environmentId, label: "Synthetic receiver" }] }),
}));
vi.mock("../../hooks/useSettings", () => ({
  useClientSettings: (
    select: (settings: { inAppNotificationsEnabled: boolean; notificationMode: string }) => unknown,
  ) => select({ inAppNotificationsEnabled: true, notificationMode: "off" }),
}));
vi.mock("@tanstack/react-router", async (original) => ({
  ...(await original<typeof import("@tanstack/react-router")>()),
  useNavigate: () => mocks.navigate,
}));
vi.mock("./useAdmission", () => ({
  useAdmission: () => {
    const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
    return { ...state, store };
  },
}));

it("separates evaluator failures from risk, groups sources, dismisses only the cue, and opens the inbox", async () => {
  vi.clearAllMocks();
  mocks.add.mockImplementation(() => `toast-${mocks.add.mock.calls.length}`);
  const request = vi
    .fn()
    .mockResolvedValue(
      admissionSnapshot([
        admissionFixture(),
        admissionFixture({ id: "risk-2" }),
        admissionFixture({ id: "service-failed", category: "evaluation_unavailable" }),
        admissionFixture({ id: "service-interrupted", category: "evaluation_interrupted" }),
        admissionFixture({ id: "evaluating", status: "evaluating" }),
        admissionFixture({ id: "dispatching", status: "dispatching" }),
      ]),
    );
  store = createAdmissionStore(request);
  await store.refresh();
  await using _screen = await render(<RemoteAdmissionNotifications />);
  expect(mocks.add).toHaveBeenCalledTimes(2);
  const [risk] = mocks.add.mock.calls.find(([options]) =>
    options.title.startsWith("Request review"),
  )!;
  const [failure] = mocks.add.mock.calls.find(([options]) =>
    options.title.startsWith("Evaluator service failure"),
  )!;
  expect(risk.description).toContain("2 held");
  expect(failure.description).toContain("2 held");
  expect(failure.description).toContain("could not complete");
  risk.data.onClose();
  expect(store.getSnapshot().snapshot?.pending).toHaveLength(6);
  expect(request.mock.calls.every(([input]) => input.action === "snapshot")).toBe(true);
  risk.actionProps.onClick();
  expect(mocks.navigate).toHaveBeenCalledWith({ to: "/requests" });
  await act(async () => store.refresh());
  expect(mocks.add).toHaveBeenCalledTimes(2);
  request.mockResolvedValue(admissionSnapshot([admissionFixture({ id: "new-risk" })]));
  await act(async () => store.refresh());
  expect(mocks.add).toHaveBeenCalledTimes(3);
  expect(mocks.close).toHaveBeenCalled();
});

it("closes stale inline actions while owner review is inaccessible", async () => {
  vi.clearAllMocks();
  mocks.add.mockReturnValue("synthetic-toast");
  const request = vi.fn().mockResolvedValue(admissionSnapshot());
  store = createAdmissionStore(request);
  await store.refresh();
  await using _screen = await render(<RemoteAdmissionNotifications />);
  request.mockRejectedValue(new Error("forbidden private-detail"));
  await act(async () => store.refresh());
  expect(mocks.close).toHaveBeenCalledWith("synthetic-toast");
  expect(store.getSnapshot().loadFailed).toBe(true);
  expect(store.getSnapshot().snapshot?.pending).toHaveLength(1);
});
