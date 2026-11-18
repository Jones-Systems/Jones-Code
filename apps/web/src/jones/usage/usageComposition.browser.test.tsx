import {
  EnvironmentId,
  USAGE_CONTRACT_VERSION,
  type TokenAccountingReadResult,
  type UsageSummaryInput,
} from "@t3tools/contracts";
import { AVAILABLE_CONNECTION_STATE } from "@t3tools/client-runtime/connection";
import { mergeUsage } from "@t3tools/shared/usageMerge";
import { AsyncResult, type Atom } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import { act, render } from "../../../test/render";
import { environmentCatalog } from "../../connection/catalog";
import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { saveUsagePagePreferences } from "../../components/usage/usagePagePreferences";

const testState = vi.hoisted(() => ({
  presentations: new Map(),
  connections: new Map(),
  useUsage: vi.fn(),
  read: vi.fn(),
  refreshProviders: vi.fn(async () => undefined),
  navigate: vi.fn(),
}));

vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("@tanstack/react-router", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-router")>()),
  useNavigate: () => testState.navigate,
  useCanGoBack: () => true,
}));
vi.mock("@effect/atom-react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@effect/atom-react")>();
  return {
    ...actual,
    useAtomValue: (atom: Atom.Atom<unknown>) => {
      if (
        atom === environmentPresentations.presentationsAtom ||
        atom === environmentPresentations.presentationsWithProvidersAtom
      ) {
        return testState.presentations;
      }
      if (testState.connections.has(atom)) return testState.connections.get(atom);
      return actual.useAtomValue(atom);
    },
  };
});
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) =>
    command === serverEnvironment.readTokenAccounting ? testState.read : testState.refreshProviders,
}));
vi.mock("../../state/usage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/usage")>()),
  useUsage: testState.useUsage,
}));
vi.mock("../../components/usage/UsageProviderChart", () => ({ UsageProviderChart: () => null }));
vi.mock("../../components/usage/UsagePriceOverrides", () => ({ UsagePriceOverrides: () => null }));
vi.mock("../../components/usage/UsageLimits", () => ({ UsageLimitsSection: () => null }));

import { UsagePage } from "../../components/usage/UsagePage";

const environmentId = EnvironmentId.make("usage-composition-primary");

function setup(): Disposable {
  const key = "t3code:usage-page-preferences:v1";
  const previous = localStorage.getItem(key);
  saveUsagePagePreferences({ metric: "tokens", windowDays: 30 });
  testState.presentations = new Map([
    [
      environmentId,
      {
        entry: {
          target: { _tag: "PrimaryConnectionTarget", label: "Synthetic primary" },
          enabled: true,
        },
        connection: { phase: "connected" },
        serverConfig: {
          providers: [],
          environment: { capabilities: { savedTokenAccounting: true } },
        },
      },
    ],
  ]);
  testState.connections = new Map([
    [
      environmentCatalog.stateAtom(environmentId),
      AsyncResult.success({
        ...AVAILABLE_CONNECTION_STATE,
        phase: "connected",
        generation: 1,
      }),
    ],
  ]);
  const environment = {
    environmentId,
    label: "Synthetic primary",
    isPending: false,
    canReadDiagnostics: false,
    isConnected: true,
    error: "Diagnostics are unavailable for this synthetic session.",
    summary: null,
  };
  const merged = mergeUsage([], USAGE_CONTRACT_VERSION);
  testState.useUsage.mockImplementation((window: UsageSummaryInput) => ({
    merged,
    environments: [environment],
    selectedEnvironments: [environment],
    shown: { window, merged },
    isPartial: false,
    isPending: false,
    refresh: vi.fn(async () => undefined),
  }));
  const unavailable: TokenAccountingReadResult = {
    state: "unavailable",
    status: "unconfigured",
    reason: "report_unconfigured",
    readAt: "2026-11-16T12:00:00.000Z",
    configuredReportId: null,
  };
  testState.read.mockReset().mockResolvedValue(AsyncResult.success(unavailable));
  testState.refreshProviders.mockClear();
  return {
    [Symbol.dispose]() {
      if (previous === null) localStorage.removeItem(key);
      else localStorage.setItem(key, previous);
      testState.presentations = new Map();
      testState.connections = new Map();
      testState.useUsage.mockReset();
      testState.read.mockReset();
      testState.refreshProviders.mockClear();
      testState.navigate.mockReset();
    },
  };
}

describe("UsagePage saved accounting composition", () => {
  it("mounts the real opt-in reader independently of the activity range and hides it for limits", async () => {
    using _state = setup();
    await using _screen = await render(<UsagePage />, { router: "/usage" });
    const dialog = document.querySelector<HTMLElement>('[role="dialog"][aria-label="Usage"]')!;
    const panel = [...dialog.querySelectorAll("details")].find(
      (element) =>
        element.querySelector("summary")?.textContent?.trim() === "Saved token accounting",
    );
    expect(panel).toBeDefined();
    expect(panel!.open).toBe(false);
    expect(dialog.textContent).toContain("Diagnostics are unavailable for this synthetic session.");
    expect(testState.read).not.toHaveBeenCalled();

    await act(() => panel!.querySelector("summary")!.click());
    expect(panel!.textContent).toContain("Its window is independent of Usage date");
    expect(testState.read).not.toHaveBeenCalled();
    const load = [...panel!.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Load saved report",
    )!;
    await act(() => load.click());
    expect(testState.read).toHaveBeenCalledExactlyOnceWith({ environmentId, input: {} });
    expect(panel!.textContent).toContain(
      "A saved accounting report has not been selected for this environment.",
    );

    const limits = [
      ...dialog.querySelectorAll<HTMLButtonElement>('[aria-label="Usage metric"] button'),
    ].find((button) => button.textContent?.trim() === "Limits")!;
    expect(limits).toBeDefined();
    await act(() => limits.click());
    expect(
      [...dialog.querySelectorAll("summary")].some(
        (element) => element.textContent?.trim() === "Saved token accounting",
      ),
    ).toBe(false);
    expect(dialog.querySelector('[aria-label="Additional usage ranges"]')).toBeNull();
    expect(testState.read).toHaveBeenCalledTimes(1);
  });
});
