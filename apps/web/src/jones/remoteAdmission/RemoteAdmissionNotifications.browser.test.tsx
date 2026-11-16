import { useSyncExternalStore } from "react";
import { act, render, stubGlobal } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { TooltipProvider } from "../../components/ui/tooltip";
import { createAdmissionStore } from "./store";
import { admissionFixture, admissionSnapshot, contextAdmissionFixture } from "./testFixtures";
import { RemoteAdmissionNotifications } from "./RemoteAdmissionNotifications";

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  update: vi.fn(),
  close: vi.fn(),
  navigate: vi.fn(),
}));
const environmentId = EnvironmentId.make("notification-receiver");
let store: ReturnType<typeof createAdmissionStore>;
let notificationMode = "off";
vi.mock("../../components/ui/toast", () => ({ toastManager: mocks }));
vi.mock("../../state/entities", () => ({ useProject: () => null, useThreadShell: () => null }));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({
    environments: [
      { environmentId, label: "Synthetic receiver", displayLabel: "Synthetic receiver" },
    ],
  }),
}));
vi.mock("../../hooks/useSettings", () => ({
  useClientSettings: (
    select: (settings: {
      inAppNotificationsEnabled: boolean;
      notificationMode: string;
      timestampFormat: string;
    }) => unknown,
  ) => select({ inAppNotificationsEnabled: true, notificationMode, timestampFormat: "12-hour" }),
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

it("uses one incoming stack, dismisses immutable keys, and re-raises only for unseen bindings", async () => {
  vi.clearAllMocks();
  mocks.add.mockImplementation(() => `toast-${mocks.add.mock.calls.length}`);
  const first = admissionFixture({ id: "dismiss-first" });
  const second = admissionFixture({ id: "dismiss-second", category: "evaluation_unavailable" });
  const request = vi.fn().mockResolvedValue(admissionSnapshot([first, second]));
  store = createAdmissionStore(request);
  await store.refresh();
  await using _screen = await render(<RemoteAdmissionNotifications />);
  expect(mocks.add).toHaveBeenCalledTimes(1);
  const [options] = mocks.add.mock.calls[0]!;
  expect(options.title).toContain("2 incoming approval requests");
  await act(() => options.data.onClose());
  request.mockResolvedValue(admissionSnapshot([second]));
  await act(() => store.refresh());
  expect(mocks.add).toHaveBeenCalledTimes(1);
  request.mockResolvedValue(admissionSnapshot([second, admissionFixture({ id: "dismiss-new" })]));
  await act(() => store.refresh());
  expect(mocks.add).toHaveBeenCalledTimes(2);
  expect(request.mock.calls.every(([input]) => input.action === "snapshot")).toBe(true);
});

it("closes stale inline actions while owner review is inaccessible", async () => {
  vi.clearAllMocks();
  mocks.add.mockReturnValue("synthetic-toast");
  const request = vi
    .fn()
    .mockResolvedValue(admissionSnapshot([admissionFixture({ id: "stale-inline-test" })]));
  store = createAdmissionStore(request);
  await store.refresh();
  await using _screen = await render(<RemoteAdmissionNotifications />);
  request.mockRejectedValue(new Error("forbidden private-detail"));
  await act(async () => store.refresh());
  expect(mocks.close).toHaveBeenCalledWith("synthetic-toast");
  expect(store.getSnapshot().loadFailed).toBe(true);
  expect(store.getSnapshot().snapshot?.pending).toHaveLength(1);
});

it("renders the same context card inside the real notification's expandable content", async () => {
  vi.clearAllMocks();
  mocks.add.mockReturnValue("context-toast");
  store = createAdmissionStore(
    vi.fn().mockResolvedValue(admissionSnapshot([contextAdmissionFixture()])),
  );
  await store.refresh();
  await using _notifications = await render(<RemoteAdmissionNotifications />);
  const [options] = mocks.add.mock.calls[0]!;
  await using screen = await render(<TooltipProvider>{options.data.bodyContent}</TooltipProvider>);
  const summary = screen.container.querySelector("summary")!;
  expect(summary.textContent).toContain("From Authenticated laptop");
  expect(summary.textContent).toContain("Sender project");
  expect(summary.textContent).toContain("To synthetic-project / receiver-thread");
  expect(summary.querySelector("time")?.dateTime).toBe("2026-10-24T11:59:00Z");
  await act(() => summary.click());
  expect(
    [...screen.container.querySelectorAll("button")].map((button) => button.textContent),
  ).toEqual(["Approve", "Deny"]);
  expect(screen.container.querySelector("strong")?.textContent).toBe("synthetic workspace");
});

it.each(["off", "notifications", "both"])(
  "never creates request OS notifications in mode %s",
  async (mode) => {
    vi.clearAllMocks();
    mocks.add.mockReturnValue("in-app-only");
    const native = vi.fn();
    using _notification = stubGlobal(
      "Notification",
      class {
        static permission = "granted";
        constructor() {
          native();
        }
      },
    );
    const focus = vi.spyOn(document, "hasFocus").mockReturnValue(false);
    notificationMode = mode;
    try {
      store = createAdmissionStore(
        vi.fn().mockResolvedValue(admissionSnapshot([admissionFixture({ id: `no-os-${mode}` })])),
      );
      await store.refresh();
      await using _screen = await render(<RemoteAdmissionNotifications />);
      expect(mocks.add).toHaveBeenCalledTimes(1);
      expect(native).not.toHaveBeenCalled();
    } finally {
      notificationMode = "off";
      focus.mockRestore();
    }
  },
);
