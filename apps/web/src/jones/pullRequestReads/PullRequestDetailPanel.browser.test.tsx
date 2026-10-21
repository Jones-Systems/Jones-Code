import {
  EnvironmentId,
  ProjectId,
  PullRequestOperationError,
  SourceControlProviderKind,
  ThreadId,
  type PullRequestDetailView,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import type { ComponentProps } from "react";
import { render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { DraftId, useComposerDraftStore } from "~/composerDraftStore";

import "~/index.css";

const { newThread, prepareThread, refresh, invalidate } = vi.hoisted(() => ({
  newThread: vi.fn(),
  prepareThread: vi.fn(),
  refresh: vi.fn(),
  invalidate: vi.fn(),
}));

vi.mock(import("~/state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: () => true,
  readEnvironmentScope: () => true,
}));
vi.mock(import("~/hooks/useSettings"), async (original) => ({
  ...(await original()),
  useEnvironmentSettings: (() => undefined) as never,
}));
vi.mock(import("~/hooks/useHandleNewThread"), async (original) => ({
  ...(await original()),
  useNewThreadHandler: (() => newThread) as never,
}));
vi.mock(import("~/lib/sourceControlActions"), async (original) => ({
  ...(await original()),
  usePreparePullRequestThreadAction: (() => ({
    run: prepareThread,
    isAllowed: true,
    error: null,
  })) as never,
}));
vi.mock(import("~/state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => invalidate) as never,
}));
vi.mock(import("~/state/pullRequests"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const cannotWriteGuides = Atom.make(false);
  return {
    ...actual,
    pullRequestEnvironment: {
      ...actual.pullRequestEnvironment,
      detail: (() => "detail") as never,
      activity: (() => "activity") as never,
      reviewGuide: (() => "review-guide") as never,
      generateReviewGuide: {
        ...actual.pullRequestEnvironment.generateReviewGuide,
        permissionAtom: () => cannotWriteGuides,
      },
    },
  };
});
vi.mock(import("~/state/query"), async (original) => ({
  ...(await original()),
  useEnvironmentQuery: ((query: unknown) => ({
    data: query === "detail" ? detail : null,
    dataUpdatedAt: query === "detail" ? 1 : 0,
    failure: query === "detail" ? detailError : null,
    isPending: false,
    isSuccess: true,
    error: query === "detail" ? (detailError?.message ?? null) : null,
    refresh,
  })) as never,
}));
vi.mock(import("~/state/usePullRequestStack"), async (original) => ({
  ...(await original()),
  usePullRequestStack: (() => ({
    data: null,
    isFresh: true,
    isSuccess: true,
    isPending: false,
    error: null,
    refresh,
  })) as never,
}));
// These cases exercise held detail and panel actions, not the tabs' environment reads.
vi.mock(import("~/components/pullRequest/PullRequestSummaryTab"), async (original) => ({
  ...(await original()),
  PullRequestSummaryTab: (() => <div />) as never,
}));
vi.mock(import("~/components/pullRequest/PullRequestCodeTab"), async (original) => ({
  ...(await original()),
  default: (() => <div />) as never,
}));

import { PullRequestDetailPanel } from "~/components/pullRequest/PullRequestDetailPanel";

let detailError: PullRequestOperationError | null = null;

const detail: PullRequestDetailView = {
  observedAt: Date.parse("2026-09-01T12:00:00Z"),
  provider: SourceControlProviderKind.make("github"),
  projectId: ProjectId.make("project"),
  projectTitle: "Project",
  workspaceRoot: "/workspace",
  repository: "owner/repo",
  number: 1,
  title: "Test pull request",
  body: "Original description",
  url: "https://github.com/owner/repo/pull/1",
  author: { login: "author", name: null, avatarUrl: null },
  viewer: "author",
  state: "open",
  isDraft: false,
  mergeability: "conflicting",
  additions: 1,
  deletions: 0,
  changedFiles: 1,
  headBranch: "feature",
  baseBranch: "main",
  createdAt: "2026-09-01T00:00:00Z",
  updatedAt: "2026-09-01T00:00:00Z",
  mergedAt: null,
  closedAt: null,
  reviewers: [],
  labels: [],
  checks: [{ name: "Unit tests", status: "success", description: null, url: null }],
  comments: [],
  commentCount: 0,
  commentsTruncated: false,
  reviewThreads: [],
  commits: [],
  mergeCapabilities: { merge: false, squash: false, rebase: false },
  capabilities: {
    diff: true,
    comment: false,
    search: true,
    actions: [],
    mergeMethods: [],
    review: { inlineComment: false, reply: false, resolve: false, verdicts: [] },
    reviewers: { request: false, listCandidates: false },
    edit: { changeRequest: true, comment: false },
  },
  viewerPermissions: {
    actions: [],
    comment: false,
    resolve: false,
    verdicts: [],
    requestReviewers: false,
  },
};

const threadRef: ScopedThreadRef = {
  environmentId: EnvironmentId.make("env-1"),
  threadId: ThreadId.make("thread-1"),
};
const newDraftId = DraftId.make("new-draft");
const shortcutContext = () => ({
  terminalFocus: false,
  terminalOpen: false,
  previewFocus: false,
  previewOpen: false,
  isWeb: true,
  isDesktop: false,
});

function renderPanel(props: Partial<ComponentProps<typeof PullRequestDetailPanel>> = {}) {
  const container = document.createElement("div");
  container.className = "flex h-[700px] w-[900px]";
  return render(
    <PullRequestDetailPanel
      environmentId={threadRef.environmentId}
      reference={detail}
      context="page"
      shortcutsEnabled={false}
      getShortcutContext={shortcutContext}
      {...props}
    />,
    { container },
  );
}

function setup(): Disposable {
  const { draftsByThreadKey } = useComposerDraftStore.getState();
  detailError = null;
  refresh.mockReset();
  invalidate.mockReset().mockResolvedValue(undefined);
  useComposerDraftStore.setState({ draftsByThreadKey: {} });
  newThread
    .mockReset()
    .mockResolvedValue({ draftId: newDraftId, threadId: ThreadId.make("new-thread") });
  prepareThread.mockReset().mockResolvedValue({
    _tag: "Success",
    value: { branch: "feature", worktreePath: "/workspace/pr" },
  });
  return {
    [Symbol.dispose]() {
      useComposerDraftStore.setState({ draftsByThreadKey });
    },
  };
}

describe("held pull request during a provider pause", () => {
  async function renderPaused(reason?: "rate-limited") {
    detailError = new PullRequestOperationError({
      operation: "detail",
      detail: "Provider reads are paused.",
      ...(reason ? { reason, retryAt: Date.parse("2026-09-01T13:00:00Z") } : {}),
    });
    return renderPanel();
  }

  it("shows a typed pause beside held detail with its original observation time", async () => {
    using _state = setup();
    await using screen = await renderPaused("rate-limited");
    const status = document.querySelector('[role="status"]');
    expect(status?.textContent).toContain("paused");
    expect([...status!.querySelectorAll("time")].map((time) => time.dateTime)).toEqual([
      "2026-09-01T13:00:00.000Z",
      "2026-09-01T12:00:00.000Z",
    ]);
    expect(
      [...document.querySelectorAll("h1")].some((node) => node.textContent?.includes(detail.title)),
    ).toBe(true);
  });

  it("does not infer a typed pause from a legacy error message", async () => {
    using _state = setup();
    await using screen = await renderPaused();
    expect(document.querySelector('[role="status"]')).toBeNull();
  });

  it("keeps manual refresh immediate and invalidates before refreshing", async () => {
    using _state = setup();
    await using screen = await renderPaused("rate-limited");
    refresh.mockClear();
    await page.getByRole("button", { name: "More pull request actions", exact: true }).click();
    await page.getByRole("menuitem", { name: "Refresh", exact: true }).click();
    expect(invalidate).toHaveBeenCalledOnce();
    expect(refresh).toHaveBeenCalled();
    expect(invalidate.mock.invocationCallOrder[0]).toBeLessThan(
      refresh.mock.invocationCallOrder[0]!,
    );
  });

  it("keeps title editing available during a read pause", async () => {
    using _state = setup();
    await using screen = await renderPaused("rate-limited");
    await page.getByRole("button", { name: "Edit title", exact: true }).click();
    const input = document.querySelector<HTMLInputElement>('[aria-label="Pull request title"]');
    expect(input?.value).toBe(detail.title);
    expect(input?.disabled).toBe(false);
  });
});
