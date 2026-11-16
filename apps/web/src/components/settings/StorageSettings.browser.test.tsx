import { act, fakeTimers, render } from "../../../test/render";
import { RegistryContext, RegistryProvider } from "@effect/atom-react";
import { AtomRegistry } from "effect/reactivity";
import {
  DEFAULT_SERVER_SETTINGS,
  type EnvironmentId,
  type StorageCleanupReport,
} from "@t3tools/contracts";
import { type ReactNode } from "react";
import { expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  settings: {} as typeof DEFAULT_SERVER_SETTINGS,
  targets: [] as Array<{ environmentId: string; settings: typeof DEFAULT_SERVER_SETTINGS }>,
  run: vi.fn(),
  report: null as StorageCleanupReport | null,
  reports: {} as Record<string, StorageCleanupReport>,
  installRetentionSupported: true,
}));

vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const report = Atom.family((environmentId: string) =>
    Atom.make(() => ({
      _tag: "Success",
      value: state.reports[environmentId] ?? state.report,
      waiting: false,
    })),
  );
  const allowed = Atom.make(true);
  return {
    ...actual,
    serverEnvironment: {
      ...actual.serverEnvironment,
      storageCleanupReport: (({ environmentId }: { environmentId: string }) =>
        report(environmentId)) as never,
      runStorageCleanup: {
        ...actual.serverEnvironment.runStorageCleanup,
        permissionAtom: () => allowed,
      },
    },
  };
});
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => state.run) as never,
}));
vi.mock(import("./SettingsScopeContext"), async (original) => ({
  ...(await original()),
  useSettingsScope: (() => {
    const environments = state.targets.map(({ environmentId, settings }) => ({
      environmentId,
      label: environmentId,
      serverConfig: {
        settings,
        environment: {
          capabilities: {
            storageCleanup: true,
            installRetention: state.installRetentionSupported,
            storageCleanupRun: true,
            settledWorktreeCleanup: true,
          },
        },
      },
    }));
    return {
      scope: { kind: "all" },
      environments,
      connectedEnvironments: environments,
      targets: state.targets,
      target: null,
    };
  }) as never,
}));
vi.mock(import("./useScopedSettings"), async (original) => ({
  ...(await original()),
  useScopedSettings: (() => state.settings) as never,
  useScopedSettingsMixed: () => false,
  useUpdateScopedSettings: () => vi.fn(),
  useClearScopedSettings: () => vi.fn(),
}));
vi.mock(import("./settingsLayout"), async (original) => ({
  ...(await original()),
  SettingsPageContainer: (({ children }: { children: ReactNode }) => children) as never,
  SettingsRow: ({ title, description, status, control, children }) => (
    <div>
      <h3>{title}</h3>
      {description}
      {status && (
        <div
          data-testid={`${title === "Delete old browser artifacts" ? "artifact" : "logs"}-retention-status`}
        >
          {status}
        </div>
      )}
      {control}
      {children}
    </div>
  ),
  SettingResetButton: (() => null) as never,
}));
vi.mock(import("../ui/toast"), async (original) => {
  const actual = await original();
  return { ...actual, toastManager: { ...actual.toastManager, add: vi.fn() } };
});

import { StorageCleanupSections } from "./StorageSettings";
import { serverEnvironment } from "../../state/server";

function setup() {
  state.installRetentionSupported = true;
  state.settings = {
    ...DEFAULT_SERVER_SETTINGS,
    storageCleanup: {
      installRetention: "propose",
      worktreeKeepWhen: "uncommitted-changes",
      worktreeAfterDays: null,
      worktreeSettledAfterDays: null,
      worktreeSettledScope: "branch-retained",
      worktreeCoveredDiscard: [],
      worktreeOnMerge: false,
      worktreeOnDelete: false,
      worktreeUnchanged: false,
      browserArtifactsAfterDays: null,
      logsAfterDays: null,
    },
    projectSettingsOverrides: {},
  };
  state.targets = ["env-a", "env-b"].map((environmentId) => ({
    environmentId,
    settings: state.settings,
  }));
  state.report = null;
  state.reports = {};
  state.run.mockReset();
}

function setRetentionValues(
  artifactDays: ReadonlyArray<number | null>,
  logDays: ReadonlyArray<number | null> = artifactDays.map(() => null),
  environmentIds: readonly string[] = ["env-a", "env-b"],
) {
  state.targets = artifactDays.map((browserArtifactsAfterDays, index) => ({
    environmentId: environmentIds[index]!,
    settings: {
      ...state.settings,
      storageCleanup: {
        ...state.settings.storageCleanup,
        browserArtifactsAfterDays,
        logsAfterDays: logDays[index]!,
      },
    },
  }));
  state.settings = state.targets[0]?.settings ?? state.settings;
}

const panel = () => (
  <RegistryProvider>
    <StorageCleanupSections />
  </RegistryProvider>
);
const deleteButton = (container: HTMLElement) =>
  Array.from(container.querySelectorAll("button")).find(
    (button) => button.textContent === "Delete now",
  )!;

it("keeps cleanup disabled when settled retention and all other rules are off", async () => {
  setup();
  await using screen = await render(panel());
  const button = deleteButton(screen.container);

  expect(button.disabled).toBe(true);
  await act(async () => button.click());
  expect(state.run).not.toHaveBeenCalled();
});

it("settles mixed retention statuses independently across selected machines", async () => {
  setup();
  using _timers = fakeTimers();
  await using screen = await render(panel());
  const advance = (ms: number) => act(() => vi.advanceTimersByTime(ms));

  await advance(300);
  setRetentionValues([8, null], [8, null]);
  await screen.rerender(panel());
  await advance(125);
  await expect.element(screen.getByTestId("artifact-retention-status")).not.toBeInTheDocument();
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();
  setRetentionValues([null, null], [8, null]);
  await screen.rerender(panel());
  await advance(8);
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();
  await advance(117);
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();
  setRetentionValues([null, null]);
  await screen.rerender(panel());
  await advance(300);
  await expect.element(screen.getByTestId("artifact-retention-status")).not.toBeInTheDocument();
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();

  setRetentionValues([8, null]);
  await screen.rerender(panel());
  await advance(300);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();

  setRetentionValues([null, null]);
  await screen.rerender(panel());
  await advance(75);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  setRetentionValues([8, null]);
  await screen.rerender(panel());
  await advance(133);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  setRetentionValues([null, null]);
  await screen.rerender(panel());
  await advance(133);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  await advance(117);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  setRetentionValues([8, null]);
  await screen.rerender(panel());
  await advance(300);
  await expect
    .element(screen.getByTestId("artifact-retention-status"))
    .toHaveTextContent("Mixed across selected machines");

  setRetentionValues([8], [null], ["env-c"]);
  await screen.rerender(panel());
  await expect.element(screen.getByTestId("artifact-retention-status")).not.toBeInTheDocument();

  setRetentionValues([null, null], [null, null], ["env-c", "env-d"]);
  await screen.rerender(panel());
  await expect.element(screen.getByTestId("artifact-retention-status")).not.toBeInTheDocument();

  setRetentionValues([null, null], [8, null]);
  await screen.rerender(panel());
  await advance(300);
  await expect.element(screen.getByTestId("artifact-retention-status")).not.toBeInTheDocument();
  await expect
    .element(screen.getByTestId("logs-retention-status"))
    .toHaveTextContent("Mixed across selected machines");
  setRetentionValues([null, null]);
  await screen.rerender(panel());
  await advance(300);
  await expect.element(screen.getByTestId("logs-retention-status")).not.toBeInTheDocument();
});

it("shows propose by default when the selected server supports install retention", async () => {
  setup();
  await using screen = await render(panel());
  const control = screen.container.querySelector('[aria-label="Install retention"]');
  expect(control?.textContent).toContain("Propose");
});

it("hides install retention for a server without the additive capability", async () => {
  setup();
  state.installRetentionSupported = false;
  await using screen = await render(panel());
  expect(screen.container.querySelector('[aria-label="Install retention"]')).toBeNull();
});

function cleanupReport(environmentId: string, count: number, omittedCount = 0): StorageCleanupReport {
  return {
    trigger: "manual",
    startedAt: "2026-11-16T12:00:00.000Z",
    finishedAt: "2026-11-16T12:01:00.000Z",
    counts: { removed: 0, kept: count + omittedCount, failed: 0 },
    bytesFreed: 0,
    omittedCount,
    entries: Array.from({ length: count }, (_, index) => ({
      kind: "worktree",
      outcome: "kept",
      reason: `Preserved evidence ${environmentId}-${index}`,
      path: `/cleanup-test/${environmentId}/worktree-${index}`,
      projectPath: "/project",
      threadId: null,
      threadTitle: null,
      bytes: null,
      files: null,
      retirement: {
        state: "held",
        reasonCodes: ["live_consumer"],
        receiptId: `${environmentId}-receipt-${index}`,
        fingerprint: null,
        paths: [],
        details: [
          {
            reason: "live_consumer",
            target: "checkout",
            root: "/project",
            fact: `Retirement evidence ${environmentId}-${index}`,
            missingFact: null,
            lineage: [{ owner: "synthetic" }],
            nextAction: "Keep the checkout.",
          },
        ],
        outputObligations: [],
      },
    })),
  };
}

const cleanupRows = (container: HTMLElement) =>
  [...container.querySelectorAll("h3")].filter((heading) =>
    heading.textContent?.startsWith("/cleanup-test/"),
  );

it("shows five results total across hosts, adds ten, handles the short final page and shows all", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 3, 7), "env-b": cleanupReport("env-b", 20) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(5);
  expect(screen.container.textContent).toContain("env-a: 0 removed, 10 kept");
  expect(screen.container.textContent).toContain("env-b: 0 removed, 20 kept");
  expect(screen.container.textContent).toContain("env-a: 7 additional results omitted from this report.");
  await expect
    .element(screen.getByRole("heading", { name: "/cleanup-test/env-b/worktree-1", exact: true }))
    .toBeInTheDocument();
  expect(screen.container.textContent).not.toContain("Preserved evidence env-b-2");
  expect(screen.container.textContent).not.toContain("Retirement evidence env-b-2");
  expect(screen.container.querySelectorAll("details")).toHaveLength(5);

  await screen.getByRole("button", { name: "Show more", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(15);
  expect(screen.container.textContent).toContain("Showing 15 of 23 results.");
  await screen.getByRole("button", { name: "Show more", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(23);
  await expect
    .element(screen.getByRole("button", { name: "Show more", exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(screen.getByRole("button", { name: "Show all", exact: true }))
    .not.toBeInTheDocument();

  await screen.getByRole("button", { name: "Show less", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(5);
  expect(screen.container.querySelectorAll("details")).toHaveLength(5);
  await screen.getByRole("button", { name: "Show all", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(23);
  expect(screen.container.textContent).toContain("Showing 23 of 23 results.");
});

it("preserves expansion on query rerenders and resets it for a new report or selected hosts", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 3), "env-b": cleanupReport("env-b", 20) };
  const registry = AtomRegistry.make();
  using _registry = { [Symbol.dispose]: () => registry.dispose() };
  const view = () => (
    <RegistryContext.Provider value={registry}>
      <StorageCleanupSections />
    </RegistryContext.Provider>
  );
  const refreshReport = () =>
    act(() =>
      registry.refresh(
        serverEnvironment.storageCleanupReport({
          environmentId: "env-b" as EnvironmentId,
          input: {},
        }),
      ),
    );
  await using screen = await render(view());
  await screen.getByRole("button", { name: /Last run/ }).click();
  await screen.getByRole("button", { name: "Show more", exact: true }).click();
  state.reports["env-b"] = cleanupReport("env-b", 20);
  await refreshReport();
  await screen.rerender(view());
  expect(cleanupRows(screen.container)).toHaveLength(15);

  state.reports["env-b"] = { ...state.reports["env-b"]!, finishedAt: "2026-11-16T12:02:00.000Z" };
  await refreshReport();
  expect(cleanupRows(screen.container)).toHaveLength(5);
  await screen.getByRole("button", { name: "Show all", exact: true }).click();
  state.targets = state.targets.filter(({ environmentId }) => environmentId === "env-b");
  await screen.rerender(view());
  expect(cleanupRows(screen.container)).toHaveLength(5);
  expect(cleanupRows(screen.container).every((row) => row.textContent?.includes("env-b"))).toBe(true);
});

it("mounts only five rows for a report with more than a thousand entries", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 3), "env-b": cleanupReport("env-b", 1001) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(5);
  expect(screen.container.querySelectorAll("details")).toHaveLength(5);
  expect(screen.container.textContent).not.toContain("Retirement evidence env-b-1000");
});

it("has no expansion controls for five or fewer available entries, including server omissions", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 2, 9), "env-b": cleanupReport("env-b", 3) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(5);
  await expect
    .element(screen.getByRole("button", { name: /^Show (more|all|less)$/ }))
    .not.toBeInTheDocument();
  expect(screen.container.textContent).toContain("env-a: 9 additional results omitted from this report.");
});
