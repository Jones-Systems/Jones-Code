import { RegistryContext } from "@effect/atom-react";
import {
  BearerConnectionTarget,
  PrimaryConnectionTarget,
} from "@t3tools/client-runtime/connection";
import type { UpdateProjectInput } from "@t3tools/client-runtime/state/projects";
import {
  AuthFilesystemReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_VERSION,
  ProjectId,
  ProviderInstanceId,
  ServerConfig,
  type AuthEnvironmentScope,
  type AuthSessionState,
  type ProjectSearchEntriesResult,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { act, fakeTimers, render } from "../../../test/render";
import type { Project } from "~/types";

type SessionResult = AsyncResult.AsyncResult<AuthSessionState, Error>;
type SearchTarget = Parameters<typeof projectEnvironment.searchEntries>[0];
const state = vi.hoisted(() => ({
  sessions: new Map<EnvironmentId, Atom.Writable<SessionResult>>(),
  projects: [] as Project[],
  search: vi.fn<(target: SearchTarget) => ProjectSearchEntriesResult>(),
  update:
    vi.fn<
      (target: {
        readonly environmentId: EnvironmentId;
        readonly input: UpdateProjectInput;
      }) => ReturnType<typeof projectEnvironment.update.run>
    >(),
}));

vi.mock(import("@t3tools/client-runtime/state/session"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    createEnvironmentSessionAtoms: ((
      ...args: Parameters<typeof actual.createEnvironmentSessionAtoms>
    ) => ({
      ...actual.createEnvironmentSessionAtoms(...args),
      sessionStateAtom: (id: EnvironmentId) => state.sessions.get(id)!,
    })) as typeof actual.createEnvironmentSessionAtoms,
  };
});
vi.mock(import("~/state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: () => ({
    isReady: true,
    networkStatus: "online" as const,
    environments,
    presentationById: new Map(
      environments.map((environment) => [environment.environmentId, environment]),
    ),
  }),
  usePrimaryEnvironmentId: () => primaryId,
}));
vi.mock(import("~/state/presentation"), async (original) => ({
  ...(await original()),
  useEnvironmentPresentation: (id: EnvironmentId | null) => ({
    isReady: true,
    presentation: environments.find((environment) => environment.environmentId === id) ?? null,
  }),
}));
vi.mock(import("~/state/entities"), async (original) => ({
  ...(await original()),
  useProjects: () => state.projects,
  useThreadShells: () => [],
}));
vi.mock(import("~/state/projects"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    projectEnvironment: {
      ...actual.projectEnvironment,
      searchEntries: (target: SearchTarget) => searchAtom(target),
      update: {
        label: "favicon-search-fixture:update",
        run: (
          _registry: Parameters<typeof actual.projectEnvironment.update.run>[0],
          target: Parameters<typeof actual.projectEnvironment.update.run>[1],
        ) => state.update(target),
      } satisfies typeof actual.projectEnvironment.update,
    },
  };
});
vi.mock(import("~/hooks/useSettings"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    useClientSettings: ((selector) =>
      selector
        ? selector(DEFAULT_CLIENT_SETTINGS)
        : DEFAULT_CLIENT_SETTINGS) as typeof actual.useClientSettings,
  };
});
// These independent settings sections and image URL minting are outside search admission.
vi.mock(import("~/components/settings/ProjectDefaultsSettings"), () => ({
  ProjectDefaultsSettings: () => <></>,
}));
vi.mock(import("~/components/settings/ProjectActionsSettings"), () => ({
  ProjectActionsSettings: () => <></>,
}));
vi.mock(import("~/state/assets"), async (original) => ({
  ...(await original()),
  projectFaviconUrlAtom: () => emptyFavicon,
}));
vi.mock(import("~/assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrlState: () => ({ _tag: "Loading" as const }),
}));

import { ProjectSettingsPanel } from "~/components/settings/ProjectSettingsPanel";
import {
  retainSettingsScope,
  validateSettingsRouteSearch,
} from "~/components/settings/settingsScopeNavigation";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { buildSidebarProjectSnapshots } from "~/sidebarProjectGrouping";
import { projectEnvironment } from "~/state/projects";
import { readEnvironmentScope } from "~/state/session";

const primaryId = EnvironmentId.make("favicon-hosted");
const remoteId = EnvironmentId.make("favicon-direct-remote");
const instanceId = ProviderInstanceId.make("codex");
const auth = {
  policy: "remote-reachable",
  bootstrapMethods: ["one-time-token"],
  sessionMethods: ["bearer-access-token"],
  sessionCookieName: "t3_session",
} as const;
const decodeServerConfig = Schema.decodeSync(ServerConfig);
const environments: ReturnType<
  typeof import("~/state/environments").useEnvironments
>["environments"] = [primaryId, remoteId].map((environmentId) => ({
  environmentId,
  label: environmentId,
  displayUrl: null,
  relayManaged: false,
  entry: {
    target:
      environmentId === primaryId
        ? new PrimaryConnectionTarget({
            environmentId,
            label: environmentId,
            httpBaseUrl: "https://hosted.example.test",
            wsBaseUrl: "wss://hosted.example.test",
          })
        : new BearerConnectionTarget({
            environmentId,
            label: environmentId,
            connectionId: "synthetic-direct",
          }),
    enabled: true,
    profile: Option.none(),
  },
  connection: { phase: "connected", error: null, traceId: null },
  serverConfig: {
    ...decodeServerConfig({
      environment: {
        environmentId,
        label: environmentId,
        platform: { os: "linux", arch: "x64" },
        serverVersion: "0.1.2",
        orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        capabilities: {},
      },
      auth,
      cwd: `/work/${environmentId}`,
      keybindingsConfigPath: "/synthetic/keybindings.json",
      keybindings: [],
      issues: [],
      providers: [],
      availableEditors: [],
      observability: {
        logsDirectoryPath: "/synthetic/logs",
        localTracingEnabled: false,
        otlpTracesEnabled: false,
        otlpMetricsEnabled: false,
      },
      settings: {},
    }),
    keybindings: DEFAULT_RESOLVED_KEYBINDINGS,
  },
}));
const emptyFavicon = Atom.make<string | null>(null);
const searchAtoms = new Map<string, ReturnType<typeof projectEnvironment.searchEntries>>();
const searchSources = new Map<string, ReturnType<typeof projectEnvironment.searchEntries>>();

// The receipt is emitted on lazy query execution, never on factory lookup/render.
function searchSource(target: SearchTarget): ReturnType<typeof projectEnvironment.searchEntries> {
  const key = JSON.stringify(target);
  let source = searchSources.get(key);
  if (!source) {
    source = Atom.make(Effect.sync(() => state.search(target))).pipe(Atom.setIdleTTL(60_000));
    searchSources.set(key, source);
  }
  return source;
}

// Keep the source separate so a stale result can predate its first SWR reader.
// An already-retained SWR reader need not rebuild or issue another request on remount.
function searchAtom(target: SearchTarget): ReturnType<typeof projectEnvironment.searchEntries> {
  const key = JSON.stringify(target);
  let atom = searchAtoms.get(key);
  if (!atom) {
    atom = searchSource(target).pipe(
      Atom.swr({ staleTime: 15_000, revalidateOnMount: true }),
      Atom.setIdleTTL(60_000),
    );
    searchAtoms.set(key, atom);
  }
  return atom;
}

function session(scopes: ReadonlyArray<AuthEnvironmentScope>): AuthSessionState {
  return { authenticated: true, scopes, auth };
}
const grants = [AuthFilesystemReadScope, AuthOrchestrationOperateScope];
const target = (
  environmentId: EnvironmentId,
  cwd = `/work/${environmentId}`,
  query = "",
): SearchTarget => ({
  environmentId,
  input: { cwd, query, limit: 200, kind: "file", imageOnly: true },
});
const receipts = () =>
  state.search.mock.calls
    .filter(([request]) => request.input.imageOnly)
    .map(([request]) => request);
const chooseFile = () => page.getByRole("button", { name: "Choose a project icon file" });
const searchInput = () => page.getByPlaceholder("Search image files…");
const imageResult = () => page.getByRole("option", { name: /logo\.png/ });

function setup() {
  state.search.mockReset().mockImplementation(({ input }) => ({
    entries:
      input.query === "" || "logo".includes(input.query)
        ? [{ path: "images/logo.png", kind: "file" }]
        : [],
    truncated: false,
  }));
  state.update.mockReset().mockImplementation(async ({ environmentId, input }) => {
    state.projects = state.projects.map((project) =>
      project.environmentId === environmentId && project.id === input.projectId
        ? {
            ...project,
            ...(input.faviconPath !== undefined ? { faviconPath: input.faviconPath } : {}),
            ...(input.projectIcon !== undefined ? { projectIcon: input.projectIcon } : {}),
          }
        : project,
    );
    const updated = state.projects.find(
      (project) => project.environmentId === environmentId && project.id === input.projectId,
    );
    if (!updated) throw new Error("Synthetic update target was not found");
    return AsyncResult.success({ ...updated, deletedAt: null });
  });
  searchAtoms.clear();
  searchSources.clear();
  state.sessions.clear();
  for (const id of [primaryId, remoteId]) {
    state.sessions.set(
      id,
      Atom.make<SessionResult>(AsyncResult.success(session(grants))).pipe(Atom.keepAlive),
    );
  }
  state.projects = [primaryId, remoteId].map((environmentId) => ({
    id: ProjectId.make(`project-${environmentId}`),
    environmentId,
    title: "Synthetic favicon repo",
    workspaceRoot: `/work/${environmentId}`,
    repositoryIdentity: {
      canonicalKey: "github.com/example/favicon-fixture",
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: "https://github.com/example/favicon-fixture.git",
      },
    },
    defaultModelSelection: { instanceId, model: "gpt-5-codex" },
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    scripts: [],
  }));
  for (const id of [primaryId, remoteId]) {
    expect(readEnvironmentScope(id, AuthFilesystemReadScope)).toBe(true);
    expect(readEnvironmentScope(id, AuthOrchestrationOperateScope)).toBe(true);
  }
}

async function advance(milliseconds = 120) {
  await act(async () => vi.advanceTimersByTimeAsync(milliseconds));
}

async function grant(id: EnvironmentId, scopes: ReadonlyArray<AuthEnvironmentScope>) {
  await act(() =>
    appAtomRegistry.set(state.sessions.get(id)!, AsyncResult.success(session(scopes))),
  );
}

async function mountPanel(initialEnvironmentId: EnvironmentId | null = primaryId) {
  let select!: (environmentId: EnvironmentId | null) => void;
  function SelectedPanel() {
    const [selection, setSelection] = useState({ environmentId: initialEnvironmentId });
    useEffect(() => {
      select = (environmentId) => setSelection({ environmentId });
    }, []);
    const projectKey = buildSidebarProjectSnapshots({
      projects: state.projects,
      settings: DEFAULT_CLIENT_SETTINGS,
      primaryEnvironmentId: primaryId,
      resolveEnvironmentLabel: (id) => id,
    })[0]!.projectKey;
    return <ProjectSettingsPanel projectKey={projectKey} environmentId={selection.environmentId} />;
  }
  const rootRoute = createRootRoute();
  const settingsRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "settings",
    validateSearch: validateSettingsRouteSearch,
    search: { middlewares: [retainSettingsScope] },
  });
  const projectsRoute = createRoute({
    getParentRoute: () => settingsRoute,
    path: "projects",
    component: SelectedPanel,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([settingsRoute.addChildren([projectsRoute])]),
    history: createMemoryHistory({ initialEntries: ["/settings/projects"] }),
    disableGlobalCatchBoundary: true,
  });
  await router.load();
  const screen = await render(
    <RegistryContext.Provider value={appAtomRegistry}>
      <RouterProvider router={router} />
    </RegistryContext.Provider>,
    { router: false },
  );
  return { ...screen, select: (id: EnvironmentId | null) => act(() => select(id)) };
}

async function openPicker() {
  await userEvent.click(chooseFile());
  await advance();
  await expect.element(searchInput()).toHaveValue("");
  await expect.element(imageResult()).toBeVisible();
}

async function expectOpenSearch(expected: SearchTarget) {
  await openPicker();
  expect(receipts()).toContainEqual(expected);
}

describe("project favicon search demand", () => {
  it.each(["cold", "stale"] as const)(
    "does not search a %s target while closed, then searches on Choose file",
    async (cache) => {
      using _timers = fakeTimers();
      setup();
      if (cache === "stale") {
        const release = appAtomRegistry.mount(searchSource(target(primaryId)));
        try {
          await advance(0);
          expect(receipts()).toContainEqual(target(primaryId));
        } finally {
          release();
        }
        await advance(15_001);
        state.search.mockClear();
      }
      await using _screen = await mountPanel();
      await advance();
      expect(receipts()).toEqual([]);
      await expect.element(page.getByRole("textbox", { name: "Project name" })).toBeEnabled();
      await expectOpenSearch(target(primaryId));
    },
  );

  it("does not search on closed environment or cwd changes and opens on the current server workspace", async () => {
    using _timers = fakeTimers();
    setup();
    await using screen = await mountPanel();
    await advance();
    expect(receipts()).toEqual([]);
    await screen.select(remoteId);
    await advance();
    expect(receipts()).toEqual([]);
    state.projects = state.projects.map((project) =>
      project.environmentId === remoteId
        ? { ...project, workspaceRoot: "/work/new-remote-checkout" }
        : project,
    );
    await screen.select(remoteId);
    await advance();
    expect(receipts()).toEqual([]);
    await expectOpenSearch(target(remoteId, "/work/new-remote-checkout"));
    expect(
      receipts().every(
        (request) =>
          request.environmentId === remoteId && request.input.cwd === "/work/new-remote-checkout",
      ),
    ).toBe(true);
  });

  it("types, selects an image for the intended project, closes without demand, and resets the query on reopen", async () => {
    using _timers = fakeTimers();
    setup();
    await using screen = await mountPanel(remoteId);
    await expectOpenSearch(target(remoteId));
    await userEvent.fill(searchInput(), "logo");
    await advance();
    expect(receipts()).toContainEqual(target(remoteId, `/work/${remoteId}`, "logo"));
    await expect.element(imageResult()).toBeVisible();
    // Age the empty-query cache before close; closing must not start more work.
    await advance(15_001);
    state.search.mockClear();
    await userEvent.click(imageResult());
    await advance();
    expect(state.update).toHaveBeenCalledExactlyOnceWith({
      environmentId: remoteId,
      input: {
        projectId: ProjectId.make(`project-${remoteId}`),
        faviconPath: "images/logo.png",
        projectIcon: null,
      },
    });
    await expect.element(searchInput()).not.toBeInTheDocument();
    expect(receipts()).toEqual([]);
    await screen.select(remoteId);
    expect(screen.container.textContent).toContain("images/logo.png");
    expect(
      state.projects.find((project) => project.environmentId === primaryId)?.faviconPath,
    ).toBeUndefined();
    await openPicker();
    expect(receipts()).toEqual([]);
    await userEvent.keyboard("{Escape}");
    await advance();
    await expect.element(searchInput()).not.toBeInTheDocument();
    // A fresh cache hit on this reopen need not execute another request.
    await userEvent.click(chooseFile());
    await advance();
    await expect.element(searchInput()).toHaveValue("");
    await expect.element(imageResult()).toBeVisible();
  });

  it("unmounts on group edit loss despite retained filesystem access and stays closed after permission recovery", async () => {
    using _timers = fakeTimers();
    setup();
    await using _screen = await mountPanel(null);
    await expectOpenSearch(target(primaryId));
    await userEvent.fill(searchInput(), "logo");
    await advance();
    await expect.element(imageResult()).toBeVisible();
    await advance(15_001);
    state.search.mockClear();
    await grant(remoteId, [AuthFilesystemReadScope]);
    await advance();
    expect(readEnvironmentScope(primaryId, AuthFilesystemReadScope)).toBe(true);
    expect(readEnvironmentScope(remoteId, AuthFilesystemReadScope)).toBe(true);
    await expect.element(chooseFile()).toBeDisabled();
    await expect.element(searchInput()).not.toBeInTheDocument();
    expect(receipts()).toEqual([]);
    expect(state.update).not.toHaveBeenCalled();
    await grant(remoteId, grants);
    await advance();
    await expect.element(chooseFile()).toBeEnabled();
    expect(receipts()).toEqual([]);
    await openPicker();
    expect(receipts()).toEqual([]);
  });
});
