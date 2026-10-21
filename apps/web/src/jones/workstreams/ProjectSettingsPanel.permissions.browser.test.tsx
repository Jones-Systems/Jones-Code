import { RegistryContext } from "@effect/atom-react";
import type {
  DeleteProjectInput,
  UpdateProjectInput,
} from "@t3tools/client-runtime/state/projects";
import {
  AuthOrchestrationOperateScope,
  AuthSettingsWriteScope,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  type AuthEnvironmentScope,
  type AuthSessionState,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import type { Project } from "~/types";

type SessionResult = AsyncResult.AsyncResult<AuthSessionState, Error>;
const state = vi.hoisted(() => ({
  sessions: new Map<EnvironmentId, Atom.Writable<SessionResult>>(),
  projects: [] as Project[],
  update:
    vi.fn<
      (request: {
        environmentId: EnvironmentId;
        input: UpdateProjectInput;
      }) => Promise<AsyncResult.Success<void>>
    >(),
  delete:
    vi.fn<
      (request: {
        environmentId: EnvironmentId;
        input: DeleteProjectInput;
      }) => Promise<AsyncResult.Success<void>>
    >(),
  confirm: vi.fn<() => Promise<boolean>>(),
  releaseUploads: vi.fn(),
  clearDraft: vi.fn(),
}));

// Session grants come from the server; each test drives them through these atoms.
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
  useEnvironments: (() => ({ environments })) as never,
  usePrimaryEnvironmentId: () => primaryId,
}));
vi.mock(import("~/state/entities"), async (original) => ({
  ...(await original()),
  useProjects: (() => state.projects) as never,
  useThreadShells: () => [],
  waitForProjectRemoval: async () => true,
}));
vi.mock(import("~/state/projects"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    projectEnvironment: {
      ...actual.projectEnvironment,
      update: "update",
      delete: "delete",
    } as unknown as typeof actual.projectEnvironment,
  };
});
vi.mock(import("~/state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: "update" | "delete") => state[command]) as never,
}));
vi.mock(import("~/hooks/useSettings"), async (original) => ({
  ...(await original()),
  useClientSettings: ((selector?: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    selector ? selector(DEFAULT_CLIENT_SETTINGS) : DEFAULT_CLIENT_SETTINGS) as never,
}));
vi.mock(import("~/localApi"), async (original) => ({
  ...(await original()),
  readLocalApi: (() => ({ dialogs: { confirm: state.confirm } })) as never,
}));
vi.mock(import("~/lib/composerDraftUploads"), async (original) => ({
  ...(await original()),
  releaseProjectDraftUploads: state.releaseUploads,
}));
// Sections with their own data sources and tests; this file covers project permissions.
vi.mock(import("~/components/settings/ProjectDefaultsSettings"), async (original) => ({
  ...(await original()),
  ProjectDefaultsSettings: (() => null) as never,
}));
vi.mock(import("~/components/settings/ProjectActionsSettings"), async (original) => ({
  ...(await original()),
  ProjectActionsSettings: (() => null) as never,
}));

import { useComposerDraftStore } from "~/composerDraftStore";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { buildSidebarProjectSnapshots } from "~/sidebarProjectGrouping";
import { readEnvironmentScopeAccess } from "~/state/session";
import { ProjectSettingsPanel } from "~/components/settings/ProjectSettingsPanel";

const primaryId = EnvironmentId.make("primary");
const remoteId = EnvironmentId.make("remote");
const instanceId = ProviderInstanceId.make("codex");
const environments = [primaryId, remoteId].map((environmentId) => ({
  environmentId,
  label: environmentId,
  connection: { phase: "connected" },
  serverConfig: { keybindings: DEFAULT_RESOLVED_KEYBINDINGS },
}));
const session = (scopes: ReadonlyArray<AuthEnvironmentScope>): AuthSessionState => ({
  authenticated: true,
  scopes,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["bearer-access-token"],
    sessionCookieName: "t3_session",
  },
});
const writable = () => AsyncResult.success(session([AuthOrchestrationOperateScope]));

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.clearAllMocks();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  state.sessions.clear();
  for (const id of [primaryId, remoteId])
    state.sessions.set(id, Atom.make<SessionResult>(writable()).pipe(Atom.keepAlive));
  state.projects = [primaryId, remoteId].map((environmentId) => ({
    id: ProjectId.make(`project-${environmentId}`),
    environmentId,
    title: "Shared repo",
    workspaceRoot: `/work/${environmentId}`,
    repositoryIdentity: {
      canonicalKey: "github.com/example/shared-repo",
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: "https://github.com/example/shared-repo.git",
      },
    },
    defaultModelSelection: { instanceId, model: "gpt-5-codex" },
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    scripts: [],
  }));
  state.update.mockImplementation(async ({ environmentId, input }) => {
    const { projectId, ...updates } = input;
    const patch = Object.fromEntries(
      Object.entries(updates).filter(([, value]) => value !== undefined),
    ) as Partial<Project>;
    state.projects = state.projects.map((project) =>
      project.environmentId === environmentId && project.id === projectId
        ? { ...project, ...patch }
        : project,
    );
    return AsyncResult.success(undefined);
  });
  state.delete.mockResolvedValue(AsyncResult.success(undefined));
  state.confirm.mockResolvedValue(true);
  useComposerDraftStore.setState({ clearProjectDraftThreadId: state.clearDraft });
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

function projectKey() {
  return buildSidebarProjectSnapshots({
    projects: state.projects,
    settings: DEFAULT_CLIENT_SETTINGS,
    primaryEnvironmentId: primaryId,
    resolveEnvironmentLabel: (id) => id,
  })[0]!.projectKey;
}

async function mountPanel() {
  const key = projectKey();
  const router = createRouter({
    routeTree: createRootRoute({ component: () => <ProjectSettingsPanel projectKey={key} /> }),
    history: createMemoryHistory({ initialEntries: ["/settings/projects"] }),
  });
  await router.load();
  await act(async () =>
    root.render(
      <RegistryContext.Provider value={appAtomRegistry}>
        <RouterProvider router={router} />
      </RegistryContext.Provider>,
    ),
  );
  await vi.waitFor(() => expect(nameInput()).not.toBeNull());
}

const nameInput = () => container.querySelector<HTMLInputElement>('[aria-label="Project name"]')!;
const removeCheckout = (root: string) =>
  container.querySelector<HTMLButtonElement>(`[aria-label="Remove checkout ${root}"]`)!;
const removeAll = () =>
  [...container.querySelectorAll("button")].find((button) =>
    button.textContent?.includes("Remove all entries"),
  )!;

async function grant(id: EnvironmentId, result: SessionResult) {
  await act(async () => appAtomRegistry.set(state.sessions.get(id)!, result));
}

async function rename(title: string) {
  await userEvent.fill(nameInput(), title);
  await userEvent.keyboard("{Enter}");
}

describe("project settings permissions", () => {

  it.each([
    ["loading", () => AsyncResult.initial<AuthSessionState, Error>(), "checking"],
    [
      "failed",
      () => AsyncResult.failure<AuthSessionState, Error>(Cause.fail(new Error("Disconnected"))),
      "check_failed",
    ],
    ["refreshing", () => AsyncResult.waiting(writable()), "checking"],
  ] as const)(
    "disables shared mutations while a member's session is %s and restores them after a fresh grant",
    async (_state, result, status) => {
      await grant(remoteId, result());
      await mountPanel();
      expect(readEnvironmentScopeAccess(remoteId, AuthOrchestrationOperateScope)).toEqual({
        status,
      });
      expect(nameInput().value).toBe("Shared repo");
      expect(nameInput().disabled).toBe(true);
      expect(removeAll().disabled).toBe(true);
      expect(removeCheckout("/work/remote").disabled).toBe(true);
      expect(removeCheckout("/work/primary").disabled).toBe(false);
      expect(state.update).not.toHaveBeenCalled();
      expect(state.delete).not.toHaveBeenCalled();

      await grant(remoteId, writable());
      expect(nameInput().disabled).toBe(false);
      expect(removeAll().disabled).toBe(false);
      expect(removeCheckout("/work/remote").disabled).toBe(false);
      await rename("Renamed");
      expect(state.update).toHaveBeenCalledTimes(2);
      expect(state.projects.map((project) => project.title)).toEqual(["Renamed", "Renamed"]);
    },
  );

  it.each([
    ["scope_missing", () => AsyncResult.success(session([AuthSettingsWriteScope]))],
    ["signed_out", () => AsyncResult.success({ ...session([]), authenticated: false })],
    [
      "check_failed",
      () =>
        AsyncResult.failure<AuthSessionState, Error>(Cause.fail(new Error("Disconnected")), {
          previousSuccess: Option.some(writable()),
        }),
    ],
  ] as const)(
    "keeps an in-progress edit unsaved when session refresh resolves to %s",
    async (status, result) => {
      await mountPanel();
      await userEvent.fill(nameInput(), "Unsaved rename");

      await grant(remoteId, AsyncResult.waiting(writable()));
      expect(readEnvironmentScopeAccess(remoteId, AuthOrchestrationOperateScope)).toEqual({
        status: "checking",
      });
      expect(nameInput().disabled).toBe(true);
      await act(async () => nameInput().blur());
      expect(state.update).not.toHaveBeenCalled();

      await grant(remoteId, result());
      expect(readEnvironmentScopeAccess(remoteId, AuthOrchestrationOperateScope)).toEqual({
        status,
      });
      expect(nameInput().disabled).toBe(true);
      expect(removeAll().disabled).toBe(true);
      expect(removeCheckout("/work/remote").disabled).toBe(true);
      expect(removeCheckout("/work/primary").disabled).toBe(false);
      expect(state.update).not.toHaveBeenCalled();
      expect(state.delete).not.toHaveBeenCalled();
      expect(state.projects.map((project) => project.title)).toEqual([
        "Shared repo",
        "Shared repo",
      ]);
    },
  );
});
