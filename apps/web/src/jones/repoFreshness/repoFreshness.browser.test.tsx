import { RegistryContext } from "@effect/atom-react";
import {
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  AuthSourceControlWriteScope,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import type { RepoFreshnessStatus } from "@t3tools/contracts/jones/repoFreshness";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "~/index.css";
import { act, fakeTimers, render } from "../../../test/render";
import type { SidebarProjectGroupMember } from "../../sidebarProjectGrouping";
import type { EnvironmentPresentation } from "../../state/environments";
import { RepoFreshnessControl } from "./RepoFreshnessControl";
import { RepoFreshnessSettings } from "./RepoFreshnessSettings";

const fixture = vi.hoisted(() => ({
  environments: [] as EnvironmentPresentation[],
  scopes: new Map<EnvironmentId, Set<AuthEnvironmentScope>>(),
  status: vi.fn(),
  refresh: vi.fn(),
  settings: vi.fn(),
  toast: vi.fn(),
}));
vi.mock(import("@t3tools/client-runtime/jones/repo-freshness"), async (original) => ({
  ...(await original()),
  createRepoFreshnessAtoms: (() => ({
    status: fixture.status,
    refresh: { label: "jones:repo-freshness:refresh" },
  })) as never,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: (id) => fixture.environments.find((entry) => entry.environmentId === id) ?? null,
  readEnvironment: (id) => fixture.environments.find((entry) => entry.environmentId === id) ?? null,
  useEnvironments: () => ({ environments: fixture.environments, isReady: true }) as never,
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: (id, scope) => id !== null && (fixture.scopes.get(id)?.has(scope) ?? false),
  readEnvironmentScope: (id, scope) => fixture.scopes.get(id)?.has(scope) ?? false,
  useEnvironmentsWithScope: (targets, scope) =>
    new Set(
      targets
        .map(({ environmentId }) => environmentId)
        .filter((id) => fixture.scopes.get(id)?.has(scope)),
    ),
}));
vi.mock(import("../../state/use-atom-command"), () => ({
  useAtomCommand: (command: { label: string }) =>
    command.label === "jones:repo-freshness:refresh" ? fixture.refresh : fixture.settings,
}));
vi.mock(import("../../components/ui/toast"), async (original) => ({
  ...(await original()),
  toastManager: { add: fixture.toast } as never,
}));

const primary = EnvironmentId.make("primary");
const remote = EnvironmentId.make("remote");
const projectId = ProjectId.make("same-project-id");
const project = { environmentId: remote, projectId };
const remoteTarget = { environmentId: remote, input: { projectId } };
const verification = {
  remoteName: "upstream",
  branchName: "release",
  commitSha: "b".repeat(40),
  checkedAtMs: 20_000,
};
const status: RepoFreshnessStatus = {
  projectId,
  cadenceSeconds: 120,
  target: { status: "resolved", remoteName: "upstream" },
  latestVerified: verification,
  latestAttempt: { status: "failed", checkedAtMs: 310_000 },
  freshness: "fresh",
  sampledAtMs: 315_000,
};
const makeEnvironment = (
  environmentId: EnvironmentId,
  supported = true,
  cadence: RepoFreshnessStatus["cadenceSeconds"] = 120,
) =>
  ({
    environmentId,
    connection: { phase: "connected" },
    serverConfig: {
      settings: {
        ...DEFAULT_SERVER_SETTINGS,
        projectSettingsOverrides: {
          [projectId]: { defaultAutoPull: true, repoFreshnessCadenceSeconds: cadence },
        },
      },
      environment: { capabilities: supported ? { repoFreshness: true } : {} },
    },
  }) as EnvironmentPresentation;

function setupFixture() {
  fixture.environments = [makeEnvironment(primary), makeEnvironment(remote)];
  fixture.scopes = new Map<EnvironmentId, Set<AuthEnvironmentScope>>([
    [primary, new Set([AuthSourceControlWriteScope, AuthSettingsWriteScope])],
    [remote, new Set([AuthOrchestrationReadScope])],
  ]);
  fixture.status.mockReset();
  fixture.refresh.mockReset();
  fixture.settings.mockReset();
  fixture.toast.mockReset();
}

const action = () => page.getByRole("menuitem", { name: "Fetch latest now" });
const triggerName = "Repository freshness";

it("hides freshness for an older server even when another environment supports it", async () => {
  setupFixture();
  fixture.environments[1] = makeEnvironment(remote, false);
  await using screen = await render(<RepoFreshnessControl project={project} />);
  await expect.element(screen.getByRole("button", { name: triggerName })).not.toBeInTheDocument();
  expect(fixture.status).not.toHaveBeenCalled();
});

it("keeps the last verification visible through a failed attempt and ages it locally", async () => {
  setupFixture();
  const registry = AtomRegistry.make();
  const atom = Atom.make<AsyncResult.AsyncResult<RepoFreshnessStatus>>(AsyncResult.initial());
  fixture.status.mockReturnValue(atom);
  await using screen = await render(
    <RegistryContext.Provider value={registry}>
      <RepoFreshnessControl project={project} />
    </RegistryContext.Provider>,
  );
  expect(fixture.status).not.toHaveBeenCalled();
  await screen.getByRole("button", { name: triggerName }).click();
  await expect.element(action()).toBeDisabled();
  expect(fixture.status).toHaveBeenCalledWith(remoteTarget);
  using _timers = fakeTimers();
  await act(() => registry.set(atom, AsyncResult.success(status)));
  expect(page.getByText("Latest fetch failed").element()).toBeVisible();
  expect(page.getByText(/Last verified 4m ago/).element()).toBeVisible();
  expect(page.getByText("· Stale").query()).toBeNull();
  await act(() => vi.advanceTimersByTime(5_000));
  expect(page.getByText("· Stale").element()).toBeVisible();
  fixture.environments[1] = makeEnvironment(remote, true, 0);
  await screen.rerender(
    <RegistryContext.Provider value={registry}>
      <RepoFreshnessControl project={project} />
    </RegistryContext.Provider>,
  );
  expect(page.getByText("· Stale").query()).toBeNull();
  expect(page.getByText(/Last verified 5m ago/).element()).toBeVisible();
  expect(fixture.refresh).not.toHaveBeenCalled();
});

it("reports only the scoped manual attempt and leaves the verified age after failure", async () => {
  setupFixture();
  fixture.scopes.get(remote)!.add(AuthSourceControlWriteScope);
  const atom = Atom.make(AsyncResult.success(status));
  fixture.status.mockReturnValue(atom);
  const checkout = { status: "skipped", reason: "auto-pull-disabled" };
  fixture.refresh.mockResolvedValueOnce(AsyncResult.success({ status, checkout }));
  const exact = { ...verification, commitSha: "e".repeat(40) };
  fixture.refresh.mockResolvedValueOnce(
    AsyncResult.success({
      status: {
        ...status,
        latestVerified: exact,
        latestAttempt: { status: "succeeded", verification: exact },
      },
      checkout,
    }),
  );
  await using screen = await render(<RepoFreshnessControl project={project} />);
  await screen.getByRole("button", { name: triggerName }).click();
  await action().click();
  await expect
    .poll(() => fixture.toast.mock.lastCall?.[0])
    .toMatchObject({
      type: "error",
      title: "Latest fetch failed",
    });
  await expect.element(page.getByText(/Last verified/)).toBeVisible();
  await action().click();
  await expect
    .poll(() => fixture.toast.mock.lastCall?.[0])
    .toMatchObject({
      type: "success",
      description: "upstream/release at eeeeeeeeeeee. Automatically pull is off for this checkout.",
    });
  expect(fixture.refresh.mock.calls).toEqual([[remoteTarget], [remoteTarget]]);
});

it("leaves status reads idle whenever the open menu is hidden", async () => {
  setupFixture();
  const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
  const read = vi.fn(() => AsyncResult.success(status));
  fixture.status.mockReturnValue(Atom.make(read));
  try {
    await using screen = await render(<RepoFreshnessControl project={project} />);
    await screen.getByRole("button", { name: triggerName }).click();
    await expect.element(action()).toBeDisabled();
    expect(fixture.status).not.toHaveBeenCalled();

    using _timers = fakeTimers();
    await act(() => {
      visibility.mockReturnValue("visible");
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(read).toHaveBeenCalledTimes(1);
    await act(() => {
      visibility.mockReturnValue("hidden");
      document.dispatchEvent(new Event("visibilitychange"));
    });
    await act(() => vi.advanceTimersByTime(120_000));
    expect(read).toHaveBeenCalledTimes(1);
    expect(fixture.refresh).not.toHaveBeenCalled();
  } finally {
    visibility.mockRestore();
  }
});

const extraProjectId = ProjectId.make("extra-project");
const settingsMembers = [
  {
    id: projectId,
    environmentId: primary,
    environmentLabel: "Primary",
    workspaceRoot: "/local",
    physicalProjectKey: "primary/checkout",
  },
  {
    id: projectId,
    environmentId: remote,
    environmentLabel: "Remote",
    workspaceRoot: "/remote",
    physicalProjectKey: "remote/checkout",
  },
  {
    id: extraProjectId,
    environmentId: remote,
    environmentLabel: "Remote",
    workspaceRoot: "/remote/version",
    physicalProjectKey: "remote/extra",
  },
] as SidebarProjectGroupMember[];

const pickCadence = async (name: string) => {
  await page.getByRole("combobox", { name: "Automatic fetch" }).click();
  await page.getByRole("option", { name, exact: true }).click();
};

it("applies a single cadence to the selected environments and only their chosen checkouts", async () => {
  setupFixture();
  fixture.scopes.get(remote)!.add(AuthSettingsWriteScope);
  fixture.environments[1] = makeEnvironment(remote, true, 300);
  fixture.settings.mockResolvedValue(
    AsyncResult.success({
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: {
        [projectId]: { defaultAutoPull: true, repoFreshnessCadenceSeconds: 0 },
      },
    }),
  );
  await using screen = await render(<RepoFreshnessSettings members={settingsMembers} />);
  const cadence = screen.getByRole("combobox", { name: "Automatic fetch" });
  await expect.element(cadence).toBeDisabled();
  await expect.element(cadence).toHaveTextContent("Choose checkouts");
  expect(fixture.settings).not.toHaveBeenCalled();

  await screen.getByRole("combobox", { name: "Remote checkout" }).click();
  await page.getByRole("option", { name: "/remote", exact: true }).click();
  await expect.element(cadence).toBeEnabled();
  await expect.element(cadence).toHaveTextContent("Mixed");
  await pickCadence("Off");

  await expect
    .poll(() => fixture.settings.mock.calls)
    .toEqual(
      [primary, remote].map((environmentId) => [
        {
          environmentId,
          input: {
            patch: {
              projectSettingsOverrides: { [projectId]: { repoFreshnessCadenceSeconds: 0 } },
            },
          },
        },
      ]),
    );
  expect(fixture.toast).not.toHaveBeenCalled();
});

it("reports which environment could not confirm a bulk save", async () => {
  setupFixture();
  fixture.scopes.get(remote)!.add(AuthSettingsWriteScope);
  fixture.settings
    .mockResolvedValueOnce(
      AsyncResult.success({
        ...DEFAULT_SERVER_SETTINGS,
        projectSettingsOverrides: { [projectId]: { repoFreshnessCadenceSeconds: 300 } },
      }),
    )
    .mockRejectedValueOnce(new Error("Connection lost"));
  await using screen = await render(
    <RepoFreshnessSettings members={settingsMembers.slice(0, 2)} />,
  );
  await expect
    .element(screen.getByRole("combobox", { name: "Automatic fetch" }))
    .toHaveTextContent("2 minutes");
  await pickCadence("5 minutes");
  await expect
    .poll(() => fixture.toast.mock.lastCall?.[0])
    .toMatchObject({
      title: "Automatic fetch could not be confirmed everywhere",
      description: "Check the setting on Remote.",
    });
});

it("blocks a bulk save when a selected environment lacks settings access", async () => {
  setupFixture();
  await using screen = await render(
    <RepoFreshnessSettings members={settingsMembers.slice(0, 2)} />,
  );
  await expect.element(screen.getByRole("combobox", { name: "Automatic fetch" })).toBeDisabled();
  await expect.element(screen.getByText(/Automatic fetch is unavailable on Remote/)).toBeVisible();
  expect(fixture.settings).not.toHaveBeenCalled();
});
