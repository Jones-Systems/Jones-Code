import { afterEach, beforeEach } from "vite-plus/test";
import {
  AuthOrchestrationOperateScope,
  AuthSourceControlWriteScope,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { memo } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { act, render as renderUi, type Screen } from "../../test/render";

type Branched = { readonly branch: string };
type GitActionInput = { featureBranch?: boolean; threadId?: string; action: string };
type Toast = { actionProps?: { onClick: () => void } };

const state = vi.hoisted(() => ({
  scopes: new Set<string>(),
  primaryScopes: new Set<string>(),
  shell: null as Branched | null,
  detail: null as Branched | null,
  draft: null as (Branched & { worktreePath: null; envMode: "local" }) | null,
  branch: "main",
  isDefaultRef: false,
  hasPrimaryRemote: true,
  queries: [] as unknown[],
  actions: [] as { scope: { cwd: string | null }; input: GitActionInput }[],
  metadataRequests: [] as { environmentId: string; branch: string }[],
  toasts: [] as Toast[],
  afterGitAction: undefined as (() => void) | undefined,
  workingTreeRoot: undefined as string | undefined,
  files: [{ path: "file.ts", insertions: 1, deletions: 0 }],
  editorOpens: [] as { environmentId: string; input: { cwd: string; editor: string } }[],
}));

vi.mock(import("~/state/session"), async (original) => {
  const hasScope = (environmentId: unknown, scope: string) =>
    (environmentId === "environment" ? state.scopes : state.primaryScopes).has(scope);
  return {
    ...(await original()),
    useEnvironmentScope: hasScope as never,
    readEnvironmentScope: hasScope as never,
  };
});
vi.mock(import("~/state/entities"), async (original) => ({
  ...(await original()),
  useQuietThreadShell: ((ref: { threadId: string } | null) =>
    ref === null || state.shell === null ? null : { ...state.shell, id: ref.threadId }) as never,
  useThreadProjection: ((ref: unknown) =>
    ref === null || state.detail === null
      ? null
      : { projection: { thread: state.detail } }) as never,
}));
vi.mock(import("~/state/query"), async (original) => ({
  ...(await original()),
  // Serves both the VCS status and the source-control discovery queries.
  useEnvironmentQuery: ((atom: unknown) => {
    state.queries.push(atom);
    return ({
    data: {
      sourceControlProviders: [],
      isRepo: true,
      refName: state.branch,
      isDefaultRef: state.isDefaultRef,
      hasPrimaryRemote: state.hasPrimaryRemote,
      hasUpstream: state.hasPrimaryRemote,
      aheadCount: 0,
      behindCount: 0,
      pr: null,
      hasWorkingTreeChanges: true,
      workingTree: {
        ...(state.workingTreeRoot === undefined ? {} : { rootPath: state.workingTreeRoot }),
        files: state.files,
        insertions: 1,
        deletions: 0,
      },
      error: null,
    },
  });
  }) as never,
}));
vi.mock(import("~/state/use-atom-command"), async (original) => {
  const { shellEnvironment } = await import("~/state/shell");
  return {
    ...(await original()),
    // Only the thread's branch metadata update and editor opens are observed here.
    useAtomCommand: ((command: unknown, options: unknown) =>
      options === "thread branch metadata update"
        ? async ({
            environmentId,
            input,
          }: {
            environmentId: string;
            input: { branch: string };
          }) => {
            state.metadataRequests.push({ environmentId, branch: input.branch });
            if (!state.scopes.has(AuthOrchestrationOperateScope))
              return AsyncResult.failure(Cause.fail(new Error("Task denied")));
            if (state.shell) state.shell = { branch: input.branch };
            if (state.detail) state.detail = { branch: input.branch };
            return AsyncResult.success(undefined);
          }
        : command === shellEnvironment.openInEditor
          ? async (request: (typeof state.editorOpens)[number]) => {
              state.editorOpens.push(request);
              return AsyncResult.success(undefined);
            }
          : async () => AsyncResult.success(undefined)) as never,
  };
});
// The preferred-editor flow needs one editor the environment reports as installed.
vi.mock(import("~/state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const config = Atom.make(() => ({ availableEditors: ["cursor"] }));
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, configValueAtom: (() => config) as never },
  };
});
vi.mock(import("~/lib/sourceControlActions"), async (original) => ({
  ...(await original()),
  useSourceControlActionRunning: () => false,
  useGitStackedAction: ((scope: { cwd: string | null }) => ({
    run: async (input: GitActionInput) => {
      state.actions.push({ scope, input });
      if (input.featureBranch) state.branch = "feature";
      state.afterGitAction?.();
      return {
        _tag: "Success",
        value: {
          branch: input.featureBranch
            ? { status: "created", name: "feature" }
            : { status: "skipped_not_requested" },
          toast: {
            title: "Committed",
            description: "Committed",
            cta: {
              kind: "run_action",
              label: "Push & create PR",
              action: { kind: "commit_push_pr" },
            },
          },
        },
      };
    },
  })) as never,
}));
vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  useNavigate: (() => async () => {}) as never,
}));
vi.mock(import("~/components/ui/toast"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    toastManager: {
      ...actual.toastManager,
      add: ((toast: Toast) => {
        state.toasts.push(toast);
        return "toast";
      }) as never,
    },
  };
});

import { useComposerDraftStore } from "~/composerDraftStore";
import { sourceControlEnvironment } from "~/state/sourceControl";
import GitActionsControl from "./GitActionsControl";

// ThreadDetailsPanel keeps this component mounted behind React.memo when threads change.
const MemoGitActionsControl = memo(GitActionsControl);

function control(thread = "thread", cwd = "/repo") {
  return (
    <MemoGitActionsControl
      gitCwd={cwd}
      activeThreadRef={{
        environmentId: EnvironmentId.make("environment"),
        threadId: ThreadId.make(thread),
      }}
    />
  );
}

const button = (name: string | RegExp) =>
  page.getByRole("button", { name, exact: typeof name === "string" });

// The mocked modules read `state`, so every test starts by resetting all of it.
function setup() {
  state.scopes = new Set([AuthSourceControlWriteScope]);
  state.primaryScopes = new Set([AuthSourceControlWriteScope, AuthOrchestrationOperateScope]);
  state.shell = { branch: "main" };
  state.detail = null;
  state.draft = null;
  state.branch = "main";
  state.isDefaultRef = false;
  state.hasPrimaryRemote = true;
  state.queries = [];
  state.actions = [];
  state.metadataRequests = [];
  state.toasts = [];
  state.afterGitAction = undefined;
  state.workingTreeRoot = undefined;
  state.files = [{ path: "file.ts", insertions: 1, deletions: 0 }];
  state.editorOpens = [];
  useComposerDraftStore.setState({
    getDraftSession: () => state.draft as never,
    getDraftThreadByRef: () => state.draft as never,
    setDraftThreadContext: (_target, input) => {
      if (state.draft && input.branch) state.draft = { ...state.draft, branch: input.branch };
    },
  });
}

let retainedScreen: Screen | null = null;
async function renderControl() {
  if (retainedScreen) await retainedScreen.rerender(control());
  else retainedScreen = await renderUi(control());
}
const commits = () => state.actions.length;

async function openCommitDialog() {
  await userEvent.click(button("Git action options"));
  await userEvent.click(page.getByRole("menuitem", { name: "Commit" }));
  await expect.element(page.getByRole("dialog", { name: "Commit changes" })).toBeVisible();
}

// Revokes the task grant as the click reaches the control, after its last render saw the grant.
// Revoking any earlier lets an unrelated render disable the control before the click lands.
function revokeTaskGrantOnClick(control: ReturnType<typeof button>) {
  const revoke = () => state.scopes.delete(AuthOrchestrationOperateScope);
  control.element().addEventListener("click", revoke, { once: true });
}

beforeEach(setup);
afterEach(async () => {
  if (retainedScreen) await retainedScreen[Symbol.asyncDispose]();
  retainedScreen = null;
});

describe("Git action targets across thread navigation", () => {
  const quickAction = () => button(/^Commit, push & /);

  it("runs a retained PR toast in the thread that produced it", async () => {
    setup();
    await using screen = await renderUi(control("first", "/first"));
    await userEvent.click(quickAction());
    await vi.waitFor(() => expect(state.toasts.some((toast) => toast.actionProps)).toBe(true));
    const toast = state.toasts.find((toast) => toast.actionProps)!;

    await screen.rerender(control("second", "/second"));
    await act(async () => toast.actionProps!.onClick());

    expect(state.actions.at(-1)).toMatchObject({
      scope: { cwd: "/first" },
      input: { threadId: "first", action: "commit_push_pr" },
    });
  });
});

describe("Source-control discovery for publishing", () => {
  // The family returns the same atom the dialog subscribes to for this environment.
  const discovery = () =>
    sourceControlEnvironment.discovery({
      environmentId: EnvironmentId.make("environment"),
      input: {},
    });
  const publishDialog = () => page.getByRole("dialog", { name: "Publish repository" });

  it("does not probe providers for a repository that already has a remote", async () => {
    await renderControl();
    await expect.element(button("Git action options")).toBeVisible();

    expect(state.queries.length).toBeGreaterThan(0);
    expect(state.queries).not.toContain(discovery());
  });

  it("prefetches while publish is offered and holds the query only while open", async () => {
    state.hasPrimaryRemote = false;
    await renderControl();
    expect(state.queries).toContain(discovery());

    await userEvent.click(button("Git action options"));
    await userEvent.click(page.getByRole("menuitem", { name: "Publish repository..." }));
    await expect.element(publishDialog()).toBeVisible();

    // The remote can appear while the dialog is open, as it does after a successful publish.
    state.hasPrimaryRemote = true;
    state.queries = [];
    await renderControl();
    expect(state.queries).toContain(discovery());

    await userEvent.click(publishDialog().getByRole("button", { name: "Cancel", exact: true }));
    await expect.element(publishDialog()).not.toBeInTheDocument();
    state.queries = [];
    await renderControl();
    expect(state.queries.length).toBeGreaterThan(0);
    expect(state.queries).not.toContain(discovery());
  });
});

describe("Git actions while thread details load", () => {
  it("does not offer a feature branch for a server thread without task permission", async () => {
    await renderControl();
    await openCommitDialog();

    // The primary environment's task grant must not leak into this thread's environment.
    await expect.element(button("Commit on new branch")).toBeDisabled();
    expect(commits()).toBe(0);
  });

  it("checks the task grant again when the feature-branch commit runs", async () => {
    state.scopes.add(AuthOrchestrationOperateScope);
    await renderControl();
    await openCommitDialog();
    revokeTaskGrantOnClick(button("Commit on new branch"));

    await userEvent.click(button("Commit on new branch"));

    // Nothing else here closes the dialog, so the button's handler ran without the grant.
    await expect
      .element(page.getByRole("dialog", { name: "Commit changes" }))
      .not.toBeInTheDocument();
    expect(state.scopes.has(AuthOrchestrationOperateScope)).toBe(false);
    expect(commits()).toBe(0);
    expect(state.branch).toBe("main");
    expect(state.shell?.branch).toBe("main");
  });

  it("commits and synchronizes the server thread before details finish loading", async () => {
    state.primaryScopes.clear();
    state.scopes.add(AuthOrchestrationOperateScope);
    await renderControl();
    await openCommitDialog();
    await userEvent.click(button("Commit on new branch"));

    await vi.waitFor(() => expect(state.shell?.branch).toBe("feature"));
    expect(commits()).toBe(1);
    expect(state.actions[0]?.input).toMatchObject({ action: "commit", featureBranch: true });
    expect(state.metadataRequests).toEqual([{ environmentId: "environment", branch: "feature" }]);
  });

  it("synchronizes archived thread detail after its shell disappears", async () => {
    state.scopes.add(AuthOrchestrationOperateScope);
    state.detail = { branch: "main" };
    await renderControl();
    state.shell = null;
    await renderControl();
    await openCommitDialog();
    await userEvent.click(button("Commit on new branch"));

    await vi.waitFor(() => expect(state.detail?.branch).toBe("feature"));
    expect(commits()).toBe(1);
    expect(state.metadataRequests).toEqual([{ environmentId: "environment", branch: "feature" }]);
  });

  it("requires task permission before changing an archived thread's branch", async () => {
    state.shell = null;
    state.detail = { branch: "main" };
    await renderControl();
    await openCommitDialog();

    await expect.element(button("Commit on new branch")).toBeDisabled();
    expect(state.metadataRequests).toEqual([]);
  });

  it("uses the current shell branch when cached detail names the new branch", async () => {
    state.scopes.add(AuthOrchestrationOperateScope);
    state.detail = { branch: "feature" };
    await renderControl();
    await openCommitDialog();
    await userEvent.click(button("Commit on new branch"));

    await vi.waitFor(() => expect(state.shell?.branch).toBe("feature"));
    expect(state.metadataRequests).toEqual([{ environmentId: "environment", branch: "feature" }]);
  });

  it.each([
    [AuthOrchestrationOperateScope, false],
    [AuthSourceControlWriteScope, true],
  ] as const)(
    "uses the fresh task grant after Git finishes, revoking %s",
    async (revokedScope, syncsThread) => {
      state.scopes.add(AuthOrchestrationOperateScope);
      state.afterGitAction = () => state.scopes.delete(revokedScope);
      await renderControl();
      await openCommitDialog();
      await userEvent.click(button("Commit on new branch"));

      await vi.waitFor(() => expect(state.branch).toBe("feature"));
      expect(commits()).toBe(1);
      expect(state.shell?.branch).toBe(syncsThread ? "feature" : "main");
      expect(state.metadataRequests).toHaveLength(syncsThread ? 1 : 0);
    },
  );

  it("keeps ordinary commits available while details load", async () => {
    await renderControl();
    await openCommitDialog();
    await userEvent.click(button("Commit"));

    await vi.waitFor(() => expect(commits()).toBe(1));
    expect(state.actions[0]?.input).not.toHaveProperty("featureBranch");
    expect(state.branch).toBe("main");
  });

  it("keeps feature-branch commits available for a local draft", async () => {
    state.shell = null;
    state.draft = { branch: "main", worktreePath: null, envMode: "local" };
    await renderControl();
    await openCommitDialog();
    await userEvent.click(button("Commit on new branch"));

    await vi.waitFor(() => expect(state.draft?.branch).toBe("feature"));
    expect(commits()).toBe(1);
    expect(state.metadataRequests).toEqual([]);
  });
});

describe("Default branch prompt", () => {
  beforeEach(() => {
    state.isDefaultRef = true;
    state.scopes.add(AuthOrchestrationOperateScope);
  });

  it("moves the commit to a feature branch when asked", async () => {
    await renderControl();
    await userEvent.click(button(/^Commit & push/));
    await expect
      .element(page.getByRole("dialog", { name: "Commit & push to default ref?" }))
      .toBeVisible();
    expect(commits()).toBe(0);

    await userEvent.click(button("Check out feature branch & continue"));

    await vi.waitFor(() => expect(state.shell?.branch).toBe("feature"));
    expect(state.actions[0]?.input).toMatchObject({ action: "commit_push", featureBranch: true });
  });

  it("checks the task grant again when the feature branch is chosen", async () => {
    await renderControl();
    await userEvent.click(button(/^Commit & push/));
    await expect
      .element(page.getByRole("dialog", { name: "Commit & push to default ref?" }))
      .toBeVisible();
    revokeTaskGrantOnClick(button("Check out feature branch & continue"));

    await userEvent.click(button("Check out feature branch & continue"));

    await expect
      .element(page.getByRole("dialog", { name: "Commit & push to default ref?" }))
      .not.toBeInTheDocument();
    expect(state.scopes.has(AuthOrchestrationOperateScope)).toBe(false);
    expect(commits()).toBe(0);
    expect(state.shell?.branch).toBe("main");
  });
});
