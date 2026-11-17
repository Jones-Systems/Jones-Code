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
import { page } from "vite-plus/test/browser";

const state = vi.hoisted(() => ({
  settings: {} as typeof DEFAULT_SERVER_SETTINGS,
  targets: [] as Array<{ environmentId: string; settings: typeof DEFAULT_SERVER_SETTINGS }>,
  scopeListeners: new Set<() => void>(),
  disconnectedIds: [] as string[],
  deniedIds: [] as string[],
  run: vi.fn(),
  report: null as StorageCleanupReport | null,
  reports: {} as Record<string, StorageCleanupReport>,
  installRetentionSupported: true,
  retirementSupported: false,
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
  const allowed = Atom.family((environmentId: string) =>
    Atom.make(() => !state.deniedIds.includes(environmentId)),
  );
  return {
    ...actual,
    serverEnvironment: {
      ...actual.serverEnvironment,
      storageCleanupReport: (({ environmentId }: { environmentId: string }) =>
        report(environmentId)) as never,
      runStorageCleanup: {
        ...actual.serverEnvironment.runStorageCleanup,
        permissionAtom: (environmentId: EnvironmentId | null) => allowed(environmentId ?? ""),
      },
    },
  };
});
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => state.run) as never,
}));
vi.mock(import("./SettingsScopeContext"), async (original) => {
  const { useSyncExternalStore } = await import("react");
  const { selectScopedSettingsEnvironments } = await import("./scopedSettings");
  const subscribe = (listener: () => void) => {
    state.scopeListeners.add(listener);
    return () => state.scopeListeners.delete(listener);
  };
  return {
    ...(await original()),
    useSettingsScope: (() => {
      const targets = useSyncExternalStore(subscribe, () => state.targets);
      const environments = targets.map(({ environmentId, settings }) => ({
        environmentId: environmentId as EnvironmentId,
        label: environmentId,
        connection: {
          phase: state.disconnectedIds.includes(environmentId)
            ? ("reconnecting" as const)
            : ("connected" as const),
        },
        serverConfig: {
          settings,
          environment: {
            capabilities: {
              storageCleanup: true,
              installRetention: state.installRetentionSupported,
              storageCleanupRun: true,
              settledWorktreeCleanup: true,
              settledWorktreeRetirement: state.retirementSupported,
            },
          },
        },
      }));
      const scope = {
        kind: "all" as const,
        label: "All machines",
        members: [],
        environmentIds: environments.map(({ environmentId }) => environmentId),
      };
      const selected = selectScopedSettingsEnvironments(scope, environments, null);
      return {
        scope,
        ...selected,
        targets: targets.filter(
          ({ environmentId }) => !state.disconnectedIds.includes(environmentId),
        ),
        target: null,
      };
    }) as never,
  };
});
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

import { CleanupResults } from "../../jones/storageCleanupResults/CleanupResults";
import { StorageCleanupSections } from "./StorageSettings";
import { serverEnvironment } from "../../state/server";

function setup() {
  state.installRetentionSupported = true;
  state.retirementSupported = false;
  state.disconnectedIds = [];
  state.deniedIds = [];
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

function cleanupReport(
  environmentId: string,
  count: number,
  omittedCount = 0,
): StorageCleanupReport {
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

it("pages each device independently, adds ten and shows all", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 3, 7), "env-b": cleanupReport("env-b", 20) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(8);
  expect(screen.container.textContent).toContain(
    "env-a: Run (action unavailable): Completed · 0 removed, 10 kept",
  );
  expect(screen.container.textContent).toContain(
    "env-b: Run (action unavailable): Completed · 0 removed, 20 kept",
  );
  expect(screen.container.textContent).toContain(
    "env-a: 7 additional results omitted from this report. These were not classified here.",
  );
  await expect
    .element(screen.getByRole("heading", { name: "/cleanup-test/env-b/worktree-4", exact: true }))
    .toBeInTheDocument();
  expect(screen.container.textContent).not.toContain("Preserved evidence env-b-5");
  expect(screen.container.textContent).not.toContain("Retirement evidence env-b-5");
  expect(screen.container.querySelectorAll("details")).toHaveLength(8);

  await screen.getByRole("button", { name: "Show more", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(18);
  expect(screen.container.textContent).toContain("Showing 15 of 20 results.");
  await screen.getByRole("button", { name: "Show more", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(23);
  await expect
    .element(screen.getByRole("button", { name: "Show more", exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(screen.getByRole("button", { name: "Show all", exact: true }))
    .not.toBeInTheDocument();

  await screen.getByRole("button", { name: "Show less", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(8);
  expect(screen.container.querySelectorAll("details")).toHaveLength(8);
  await screen.getByRole("button", { name: "Show all", exact: true }).click();
  expect(cleanupRows(screen.container)).toHaveLength(23);
  expect(screen.container.textContent).toContain("Showing 20 of 20 results.");
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
  expect(cleanupRows(screen.container)).toHaveLength(18);

  state.reports["env-b"] = { ...state.reports["env-b"]!, finishedAt: "2026-11-16T12:02:00.000Z" };
  await refreshReport();
  expect(cleanupRows(screen.container)).toHaveLength(8);
  await screen.getByRole("button", { name: "Show all", exact: true }).click();
  await act(() => {
    state.targets = state.targets.filter(({ environmentId }) => environmentId === "env-b");
    for (const listener of state.scopeListeners) listener();
  });
  expect(cleanupRows(screen.container)).toHaveLength(5);
  expect(cleanupRows(screen.container).every((row) => row.textContent?.includes("env-b"))).toBe(
    true,
  );
});

it("mounts only five rows per device for a report with more than a thousand entries", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 3), "env-b": cleanupReport("env-b", 1001) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(8);
  expect(screen.container.querySelectorAll("details")).toHaveLength(8);
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
  expect(screen.container.textContent).toContain(
    "env-a: 9 additional results omitted from this report. These were not classified here.",
  );
});

it("unmounts a device and its evidence when collapsed without hiding other devices", async () => {
  setup();
  state.reports = { "env-a": cleanupReport("env-a", 7), "env-b": cleanupReport("env-b", 2) };
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  await screen.getByRole("button", { name: /env-a.*7 results/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(2);
  expect(screen.container.textContent).not.toContain("Preserved evidence env-a-0");
  await screen.getByRole("button", { name: /env-a.*7 results/ }).click();
  expect(cleanupRows(screen.container)).toHaveLength(7);
});

it("prioritizes actionable entries, preserves guarded actions and collapses categories independently", async () => {
  const base = cleanupReport("grouped", 1).entries[0]!;
  const ready = {
    ...base,
    path: "/ready",
    threadId: "synthetic-thread" as NonNullable<typeof base.threadId>,
    retirement: {
      ...base.retirement!,
      state: "proposed" as const,
      fingerprint: "synthetic",
      reasonCodes: [],
    },
  };
  const kept = { ...base, path: "/kept" };
  delete kept.retirement;
  const entries = [
    kept,
    { ...base, path: "/removed", outcome: "removed" as const },
    { ...base, path: "/failed", outcome: "failed" as const },
    ready,
  ];
  const onRetire = vi.fn();
  await using screen = await render(
    <CleanupResults
      selectedEnvironmentIds={["grouped"]}
      reports={[
        {
          environmentId: "grouped",
          environmentLabel: "Laptop",
          report: { ...cleanupReport("grouped", 0), entries },
          onRetire,
        },
      ]}
    />,
  );
  const headers = [...screen.container.querySelectorAll("button[aria-expanded]")].map(
    (button) => button.textContent,
  );
  expect(headers[0]).toContain("Laptop4 results");
  const timestamp = screen.container.querySelector('[aria-label^="Completed:"]');
  expect(timestamp?.textContent).not.toBe("");
  expect(timestamp?.getAttribute("aria-label")).toContain("2026");
  expect(headers.slice(1)).toEqual([
    "▾Ready to retire 1",
    "▸Needs attention 1",
    "▸Kept 1",
    "▸Removed 1",
  ]);
  expect(screen.container.textContent).not.toContain("/failed");
  await screen.getByRole("button", { name: "Retire", exact: true }).click();
  expect(onRetire).toHaveBeenCalledWith(ready);
  await screen.getByRole("button", { name: /Ready to retire 1/ }).click();
  expect(screen.container.textContent).not.toContain("/ready");
  await screen.getByRole("button", { name: /Needs attention 1/ }).click();
  expect(screen.container.textContent).toContain("/failed");
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .not.toBeInTheDocument();
});

it("keeps the last completion timestamp in the header when cleanup rules are off", async () => {
  setup();
  state.report = { ...cleanupReport("env-a", 0), operation: "propose" };
  await using screen = await render(panel());
  const toggle = screen.container.querySelector("#storage-cleanup-last-run button")!;
  const timestamp = toggle.parentElement!.querySelector('[aria-label^="Last completed run:"]');
  expect(timestamp?.textContent).not.toBe("");
  expect(timestamp?.getAttribute("aria-label")).toContain("2026");
  expect(toggle.parentElement?.lastElementChild?.contains(timestamp!)).toBe(true);
  await act(() => (timestamp as HTMLElement).focus());
  await expect
    .element(page.getByRole("tooltip"))
    .toHaveTextContent(timestamp!.getAttribute("aria-label")!.replace("Last completed run: ", ""));
  await act(() => (timestamp as HTMLElement).blur());
  expect(deleteButton(screen.container).disabled).toBe(true);
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(screen.container.textContent).toContain(
    "No retirement candidates reported. No deletion requested.",
  );
  expect(screen.container.textContent).not.toContain("Nothing needed cleanup");
});

it("shows per-machine times and a range instead of assigning the newest time to every machine", async () => {
  setup();
  state.reports = {
    "env-a": cleanupReport("env-a", 0),
    "env-b": { ...cleanupReport("env-b", 0), finishedAt: "2026-11-15T10:00:00.000Z" },
  };
  await using screen = await render(panel());
  const timestamp = screen.container.querySelector('[aria-label^="Last completed run:"]')!;
  expect(timestamp.textContent).toContain(" – ");
  expect(timestamp.getAttribute("aria-label")).toContain("env-a:");
  expect(timestamp.getAttribute("aria-label")).toContain("env-b:");
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(screen.container.querySelectorAll('[aria-label^="Completed:"]')).toHaveLength(2);
});

it("distinguishes no retained report from a completed zero-count run", async () => {
  setup();
  await using screen = await render(panel());
  expect(screen.container.querySelector('[aria-label^="Last completed run:"]')?.textContent).toBe(
    "No report",
  );
  await screen.getByRole("button", { name: /Last run/ }).click();
  expect(screen.container.textContent).toContain(
    "No completed report available for this server session.",
  );
  expect(screen.container.textContent).not.toContain("Completed · 0 removed");
});

it("keeps proposal progress and an unconfirmed request visible beside the previous report", async () => {
  setup();
  state.retirementSupported = true;
  state.targets = state.targets.slice(0, 1);
  state.report = { ...cleanupReport("env-a", 0), operation: "cleanup" };
  let fail!: (error: Error) => void;
  state.run.mockImplementation(
    () =>
      new Promise((_resolve, reject) => {
        fail = reject;
      }),
  );
  await using screen = await render(panel());
  await screen.getByRole("button", { name: /Last run/ }).click();
  await screen.getByRole("button", { name: "Propose retirements", exact: true }).click();
  expect(screen.container.textContent).toContain("Retirement proposal: Running…");
  expect(deleteButton(screen.container).textContent).toBe("Delete now");
  expect(screen.container.textContent).toContain("Previous completed report shown below.");
  const previousTime = screen.container
    .querySelector('[aria-label^="Last completed run:"]')
    ?.getAttribute("aria-label");
  await act(() => fail(new Error("Synthetic connection failure")));
  expect(screen.container.textContent).toContain(
    "Could not confirm completion. Synthetic connection failure",
  );
  expect(
    screen.container
      .querySelector('[aria-label^="Last completed run:"]')
      ?.getAttribute("aria-label"),
  ).toBe(previousTime);
  expect(state.run).toHaveBeenCalledTimes(1);
  expect(state.run).toHaveBeenCalledWith({
    environmentId: "env-a",
    input: { retirement: { action: "propose" } },
  });
});

it("keeps a disconnected pending target unconfirmed when the other machine completes, including all offline", async () => {
  setup();
  setRetentionValues([8, 8]);
  state.retirementSupported = true;
  const base = cleanupReport("env-a", 1);
  const completed: StorageCleanupReport = {
    ...base,
    operation: "cleanup",
    entries: [
      {
        ...base.entries[0]!,
        threadId: "synthetic-thread" as NonNullable<
          StorageCleanupReport["entries"][number]["threadId"]
        >,
        retirement: {
          ...base.entries[0]!.retirement!,
          state: "proposed",
          fingerprint: "synthetic",
          reasonCodes: [],
        },
      },
    ],
  };
  let finishA!: (value: { _tag: "Success"; value: StorageCleanupReport }) => void;
  let failB!: (error: Error) => void;
  state.run.mockImplementation(({ environmentId }: { environmentId: string }) =>
    environmentId === "env-a"
      ? new Promise((resolve) => {
          finishA = resolve;
        })
      : new Promise((_resolve, reject) => {
          failB = reject;
        }),
  );
  const registry = AtomRegistry.make();
  using _registry = { [Symbol.dispose]: () => registry.dispose() };
  await using screen = await render(
    <RegistryContext.Provider value={registry}>
      <StorageCleanupSections />
    </RegistryContext.Provider>,
  );
  await screen.getByRole("button", { name: /Last run/ }).click();
  await act(() => deleteButton(screen.container).click());
  expect(state.run).toHaveBeenCalledTimes(2);
  await act(() => {
    state.reports["env-a"] = completed;
    registry.refresh(
      serverEnvironment.storageCleanupReport({
        environmentId: "env-a" as EnvironmentId,
        input: {},
      }),
    );
    finishA({ _tag: "Success", value: completed });
  });
  await act(() => {
    state.disconnectedIds = ["env-b"];
    state.targets = [...state.targets];
    for (const listener of state.scopeListeners) listener();
  });
  expect(screen.container.textContent).toContain("env-b: Cleanup: Running…");
  await act(() => failB(new Error("Synthetic transport disconnected")));
  const lastRun = screen.container.querySelector("#storage-cleanup-last-run")!;
  expect(lastRun.textContent).toContain("env-b: Cleanup: Could not confirm completion.");
  const timestamp = lastRun.querySelector('[aria-label^="Last completed run:"]')!;
  expect(timestamp.textContent).toBe("Time unavailable");
  expect(timestamp.getAttribute("aria-label")).toContain("env-b: No completion time available");
  expect(lastRun.textContent).not.toContain("on every machine");
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .toBeInTheDocument();
  await act(() => {
    state.deniedIds = ["env-a"];
    registry.refresh(serverEnvironment.runStorageCleanup.permissionAtom("env-a" as EnvironmentId));
  });
  expect(deleteButton(screen.container).disabled).toBe(true);
  await expect
    .element(screen.getByRole("button", { name: "Propose retirements", exact: true }))
    .toBeDisabled();
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .not.toBeInTheDocument();

  await act(() => {
    state.disconnectedIds = ["env-a", "env-b"];
    state.targets = [...state.targets];
    for (const listener of state.scopeListeners) listener();
  });
  expect(screen.container.querySelector("#storage-cleanup-last-run")?.textContent).toContain(
    "env-b: Cleanup: Could not confirm completion.",
  );
  expect(deleteButton(screen.container).disabled).toBe(true);
  await expect
    .element(screen.getByRole("button", { name: "Propose retirements", exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .not.toBeInTheDocument();
  await act(() => deleteButton(screen.container).click());
  expect(state.run).toHaveBeenCalledTimes(2);
});

it("does not resurrect a completed request warning after its report is replaced by a newer manual report", async () => {
  setup();
  state.targets = state.targets.slice(0, 1);
  state.retirementSupported = true;
  state.report = cleanupReport("env-a", 0);
  const completed = {
    ...cleanupReport("env-a", 0),
    operation: "propose" as const,
    startedAt: "2026-11-16T13:00:00.000Z",
    finishedAt: "2026-11-16T13:01:00.000Z",
  };
  let finish!: (value: { _tag: "Success"; value: StorageCleanupReport }) => void;
  state.run.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const registry = AtomRegistry.make();
  using _registry = { [Symbol.dispose]: () => registry.dispose() };
  await using screen = await render(
    <RegistryContext.Provider value={registry}>
      <StorageCleanupSections />
    </RegistryContext.Provider>,
  );
  const publish = (report: StorageCleanupReport) =>
    act(() => {
      state.reports["env-a"] = report;
      registry.refresh(
        serverEnvironment.storageCleanupReport({
          environmentId: "env-a" as EnvironmentId,
          input: {},
        }),
      );
    });
  await screen.getByRole("button", { name: /Last run/ }).click();
  await screen.getByRole("button", { name: "Propose retirements", exact: true }).click();
  await act(() => finish({ _tag: "Success", value: completed }));
  expect(screen.container.textContent).toContain("Waiting for the retained report to update.");
  await publish(completed);
  expect(screen.container.textContent).not.toContain("Waiting for the retained report to update.");
  expect(screen.container.textContent).not.toContain("Previous completed report shown below.");
  await publish({
    ...completed,
    operation: "cleanup",
    startedAt: "2026-11-16T14:00:00.000Z",
    finishedAt: "2026-11-16T14:01:00.000Z",
    counts: { removed: 3, kept: 0, failed: 0 },
  });
  expect(screen.container.textContent).toContain("Cleanup: Completed · 3 removed");
  expect(screen.container.textContent).not.toContain("Waiting for the retained report to update.");
  expect(screen.container.textContent).not.toContain("Previous completed report shown below.");
  expect(state.run).toHaveBeenCalledTimes(1);
});
