import { describe, expect, it, vi } from "vite-plus/test";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2CurrentThreadRuntimeTarget,
} from "@t3tools/contracts";
import type {
  CurrentRuntimeStopOperation,
  CurrentRuntimeStopOutcome,
} from "../hooks/useCurrentRuntimeStop";
import { restartCommandPaletteRuntime, type CommandPaletteRestartNotice } from "./CommandPalette";
import type { Project, Thread } from "../types";
import { makeThreadFixture } from "../test-fixtures";
import {
  buildBrowseGroups,
  buildCommandPaletteProjectMetadata,
  buildProjectActionItems,
  buildThreadActionItems,
  buildLinkedThreadActionItems,
  enumerateCommandPaletteItems,
  filterPinnedBrowseEntries,
  filterCommandPaletteGroups,
  reduceCommandPaletteUiState,
  type CommandPaletteActionItem,
  type CommandPaletteGroup,
} from "./CommandPalette.logic";

describe("linked pull request thread navigation", () => {
  it("keeps archived relations searchable and routes them through the PR environment", async () => {
    const environmentId = EnvironmentId.make("remote");
    const id = ThreadId.make("archived-thread");
    const runThread = vi.fn(async () => {});
    const query = "https://github.com/acme/web/pull/42";
    const linkedThreads = {
      environmentId,
      threads: [
        {
          id,
          projectId: ProjectId.make("project"),
          title: "Completed work",
          archivedAt: "2026-09-01T00:00:00.000Z",
        },
      ],
    };
    const state = reduceCommandPaletteUiState(
      { open: false, mode: "command", openIntent: null },
      {
        _tag: "OpenSearch",
        query,
        linkedThreads,
      },
    );
    expect(state.openIntent).toEqual({ kind: "search", query, linkedThreads });
    const items = buildLinkedThreadActionItems({ ...linkedThreads, query, icon: null, runThread });
    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query,
      isInSubmenu: false,
      projectSearchItems: [],
      settingsSearchItems: [],
      threadSearchItems: items,
    });
    expect(groups.flatMap((group) => group.items)).toEqual(items);
    expect(items[0]?.description).toBe("Archived thread");
    await items[0]?.run();
    expect(runThread).toHaveBeenCalledWith({ environmentId, id });
  });
});

describe("buildCommandPaletteProjectMetadata", () => {
  const localEnvironmentId = EnvironmentId.make("environment-local");
  const remoteEnvironmentId = EnvironmentId.make("environment-build-box");
  const locations = new Map([
    [localEnvironmentId, { kind: "local" as const, label: "Local", machine: "laptop" as const }],
    [
      remoteEnvironmentId,
      { kind: "remote" as const, label: "Build box", machine: "server" as const },
    ],
  ]);

  it("makes every member environment and path searchable", () => {
    const metadata = buildCommandPaletteProjectMetadata({
      projects: [
        {
          environmentId: localEnvironmentId,
          title: "T3 Code",
          workspaceRoot: "/Users/theo/Projects/t3code",
        },
        {
          environmentId: remoteEnvironmentId,
          title: "t3code",
          workspaceRoot: "/srv/t3code",
        },
      ],
      locationByEnvironmentId: locations,
    });

    expect(metadata.searchTerms).toEqual([
      "T3 Code",
      "/Users/theo/Projects/t3code",
      "Local",
      "t3code",
      "/srv/t3code",
      "Build box",
    ]);
    expect(metadata.environmentLabels).toEqual(["Local", "Build box"]);

    const [filteredGroup] = filterCommandPaletteGroups({
      activeGroups: [],
      query: "build box",
      isInSubmenu: false,
      projectSearchItems: [
        {
          kind: "action",
          value: "project:t3code",
          title: "T3 Code",
          searchTerms: metadata.searchTerms,
          icon: null,
          run: async () => undefined,
        },
      ],
      threadSearchItems: [],
    });
    expect(filteredGroup?.items).toHaveLength(1);
  });

  it("deduplicates grouped checkouts by environment", () => {
    const metadata = buildCommandPaletteProjectMetadata({
      projects: [
        {
          environmentId: remoteEnvironmentId,
          title: "T3 Code",
          workspaceRoot: "/srv/t3code",
        },
        {
          environmentId: remoteEnvironmentId,
          title: "T3 Code worktree",
          workspaceRoot: "/srv/t3code-feature",
        },
      ],
      locationByEnvironmentId: locations,
    });

    expect(metadata.environmentLabels).toEqual(["Build box"]);
  });

  it("deduplicates distinct environments with the same label", () => {
    const secondRemoteEnvironmentId = EnvironmentId.make("environment-build-box-2");
    const metadata = buildCommandPaletteProjectMetadata({
      projects: [
        {
          environmentId: remoteEnvironmentId,
          title: "T3 Code",
          workspaceRoot: "/srv/t3code",
        },
        {
          environmentId: secondRemoteEnvironmentId,
          title: "T3 Code mirror",
          workspaceRoot: "/srv/mirror/t3code",
        },
      ],
      locationByEnvironmentId: new Map([
        [remoteEnvironmentId, { label: "Build box" }],
        [secondRemoteEnvironmentId, { label: "Build box" }],
      ]),
    });

    expect(metadata.environmentLabels).toEqual(["Build box"]);
  });

  it("uses a human-readable fallback when presentation data is unavailable", () => {
    const metadata = buildCommandPaletteProjectMetadata({
      projects: [
        {
          environmentId: remoteEnvironmentId,
          title: "T3 Code",
          workspaceRoot: "/srv/t3code",
        },
      ],
      locationByEnvironmentId: new Map(),
    });

    expect(metadata.searchTerms).toContain("Remote");
    expect(metadata.environmentLabels).toEqual(["Remote"]);
  });
});

describe("reduceCommandPaletteUiState", () => {
  const closedState = { open: false, mode: "command", openIntent: null } as const;

  it("toggles each overlay mode open and closed", () => {
    const filesOpen = reduceCommandPaletteUiState(closedState, {
      _tag: "ToggleMode",
      mode: "files",
    });
    expect(filesOpen).toEqual({ open: true, mode: "files", openIntent: null });

    const contentOpen = reduceCommandPaletteUiState(filesOpen, {
      _tag: "ToggleMode",
      mode: "content",
    });
    expect(contentOpen).toEqual({ open: true, mode: "content", openIntent: null });

    expect(
      reduceCommandPaletteUiState(contentOpen, { _tag: "ToggleMode", mode: "content" }),
    ).toEqual({ open: false, mode: "content", openIntent: null });
  });

  it("switches between open modes without closing", () => {
    const filesOpen = reduceCommandPaletteUiState(closedState, {
      _tag: "ToggleMode",
      mode: "files",
    });
    expect(reduceCommandPaletteUiState(filesOpen, { _tag: "ToggleMode", mode: "command" })).toEqual(
      {
        open: true,
        mode: "command",
        openIntent: null,
      },
    );
  });

  it("opens PR search from another overlay and replaces an earlier search", () => {
    const first = reduceCommandPaletteUiState(
      { open: true, mode: "files", openIntent: null },
      {
        _tag: "OpenSearch",
        query: "https://github.com/acme/web/pull/7",
      },
    );
    expect(first).toEqual({
      open: true,
      mode: "command",
      openIntent: { kind: "search", query: "https://github.com/acme/web/pull/7" },
    });
    const second = reduceCommandPaletteUiState(first, {
      _tag: "OpenSearch",
      query: "https://github.com/acme/web/pull/8",
    });
    expect(second.openIntent).toEqual({
      kind: "search",
      query: "https://github.com/acme/web/pull/8",
    });
    expect(
      reduceCommandPaletteUiState(second, { _tag: "SetOpen", open: false }).openIntent,
    ).toBeNull();
  });

  it("routes open intents to command mode", () => {
    const filesOpen = reduceCommandPaletteUiState(closedState, {
      _tag: "ToggleMode",
      mode: "files",
    });
    expect(reduceCommandPaletteUiState(filesOpen, { _tag: "OpenAddProject" })).toEqual({
      open: true,
      mode: "command",
      openIntent: { kind: "add-project" },
    });
    expect(reduceCommandPaletteUiState(filesOpen, { _tag: "OpenNewThreadIn" })).toEqual({
      open: true,
      mode: "command",
      openIntent: { kind: "new-thread-in" },
    });
  });

  it("preserves the mode on close and resets it on open", () => {
    const filesOpen = reduceCommandPaletteUiState(closedState, {
      _tag: "ToggleMode",
      mode: "files",
    });

    expect(reduceCommandPaletteUiState(filesOpen, { _tag: "SetOpen", open: false })).toEqual({
      open: false,
      mode: "files",
      openIntent: null,
    });
    expect(reduceCommandPaletteUiState(filesOpen, { _tag: "SetOpen", open: true })).toEqual({
      open: true,
      mode: "command",
      openIntent: null,
    });
  });
});

describe("enumerateCommandPaletteItems", () => {
  it("assigns positional jump shortcuts to the first nine displayed items", () => {
    const items = Array.from({ length: 10 }, (_, index) => ({
      kind: "action" as const,
      value: `project-${index + 1}`,
      searchTerms: [],
      title: `Project ${index + 1}`,
      icon: null,
      shortcutCommand: "chat.new" as const,
      run: async () => undefined,
    }));

    expect(enumerateCommandPaletteItems(items).map((item) => item.shortcutCommand)).toEqual([
      "thread.jump.1",
      "thread.jump.2",
      "thread.jump.3",
      "thread.jump.4",
      "thread.jump.5",
      "thread.jump.6",
      "thread.jump.7",
      "thread.jump.8",
      "thread.jump.9",
      undefined,
    ]);
  });
});

const LOCAL_ENVIRONMENT_ID = EnvironmentId.make("environment-local");
const PROJECT_ID = ProjectId.make("project-1");

function makeProject(overrides: Partial<Project> = {}): Project {
  return {
    id: PROJECT_ID,
    environmentId: LOCAL_ENVIRONMENT_ID,
    title: "Project",
    workspaceRoot: "/workspace/project",
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-03-01T00:00:00.000Z",
    updatedAt: "2026-03-01T00:00:00.000Z",
    ...overrides,
  };
}

function makeThread(overrides: Partial<Thread> = {}): Thread {
  return makeThreadFixture({
    id: ThreadId.make("thread-1"),
    environmentId: LOCAL_ENVIRONMENT_ID,
    projectId: PROJECT_ID,
    title: "Thread",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
    runtimeMode: "full-access",
    interactionMode: "default",
    runtime: null,
    messages: [],
    proposedPlans: [],
    createdAt: "2026-03-01T00:00:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    deletedAt: null,
    updatedAt: "2026-03-01T00:00:00.000Z",
    latestRun: null,
    branch: null,
    worktreePath: null,
    ...overrides,
  });
}

describe("buildProjectActionItems", () => {
  it("shows the grouped display name but keeps the real title for icons", () => {
    const project = makeProject({ title: "fleet", workspaceRoot: "/Users/theo/Code/p/fleet" });
    const iconTitles: string[] = [];
    const [item] = buildProjectActionItems({
      projects: [{ ...project, displayName: "t3dotgg/fleet" }],
      valuePrefix: "project",
      icon: (candidate) => {
        iconTitles.push(candidate.title);
        return null;
      },
      runProject: async () => undefined,
    });

    expect(item?.title).toBe("t3dotgg/fleet");
    expect(item?.searchTerms).toEqual(
      expect.arrayContaining(["t3dotgg/fleet", "fleet", "/Users/theo/Code/p/fleet"]),
    );
    expect(iconTitles).toEqual(["fleet"]);
  });
});

describe("buildThreadActionItems", () => {
  it("orders threads by most recent activity and formats timestamps from updatedAt", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-25T12:00:00.000Z"));

    try {
      const items = buildThreadActionItems({
        threads: [
          makeThread({
            id: ThreadId.make("thread-older"),
            title: "Older thread",
            updatedAt: "2026-03-24T12:00:00.000Z",
          }),
          makeThread({
            id: ThreadId.make("thread-newer"),
            title: "Newer thread",
            createdAt: "2026-03-20T00:00:00.000Z",
            updatedAt: "2026-03-20T00:00:00.000Z",
          }),
        ],
        projectTitleById: new Map([[PROJECT_ID, "Project"]]),
        sortOrder: "updated_at",
        icon: null,
        runThread: async (_thread) => undefined,
      });

      expect(items.map((item) => item.value)).toEqual([
        "thread:thread-older",
        "thread:thread-newer",
      ]);
      expect(items[0]?.timestamp).toBe("1d ago");
      expect(items[1]?.timestamp).toBe("5d ago");
    } finally {
      vi.useRealTimers();
    }
  });

  it("ranks thread title matches ahead of contextual project-name matches", () => {
    const threadItems = buildThreadActionItems({
      threads: [
        makeThread({
          id: ThreadId.make("thread-context-match"),
          title: "Fix navbar spacing",
          updatedAt: "2026-03-20T00:00:00.000Z",
        }),
        makeThread({
          id: ThreadId.make("thread-title-match"),
          title: "Project kickoff notes",
          createdAt: "2026-03-02T00:00:00.000Z",
          updatedAt: "2026-03-19T00:00:00.000Z",
        }),
      ],
      projectTitleById: new Map([[PROJECT_ID, "Project"]]),
      sortOrder: "updated_at",
      icon: null,
      runThread: async (_thread) => undefined,
    });

    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query: "project",
      isInSubmenu: false,
      projectSearchItems: [],
      threadSearchItems: threadItems,
    });

    expect(groups).toHaveLength(1);
    expect(groups[0]?.value).toBe("threads-search");
    expect(groups[0]?.items.map((item) => item.value)).toEqual([
      "thread:thread-title-match",
      "thread:thread-context-match",
    ]);
  });

  it("orders title matches by recent activity before older prefix matches", () => {
    const threads = [
      makeThread({
        id: ThreadId.make("old-prefix"),
        title: "Convex InvalidCursor in Convex threads query",
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-02T00:00:00.000Z",
      }),
      makeThread({
        id: ThreadId.make("recent-title"),
        title: "Disable Convex schema validation",
        createdAt: "2025-12-01T00:00:00.000Z",
        updatedAt: "2026-03-24T00:00:00.000Z",
      }),
      makeThread({
        id: ThreadId.make("recent-content"),
        title: "Fix schema validation",
        createdAt: "2026-03-25T00:00:00.000Z",
        updatedAt: "2026-03-25T00:00:00.000Z",
      }),
    ];
    const items = buildThreadActionItems({
      threads,
      projectTitleById: new Map([[PROJECT_ID, "T3 Code"]]),
      sortOrder: "created_at",
      icon: null,
      getContentMatch: (thread) =>
        thread.id === ThreadId.make("recent-content")
          ? { source: "user", snippet: "Please check Convex", query: "convex" }
          : undefined,
      runThread: async () => undefined,
    });

    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query: "convex",
      isInSubmenu: false,
      projectSearchItems: [],
      threadSearchItems: items,
    });

    expect(groups[0]?.items.map((item) => item.value)).toEqual([
      "thread:recent-title",
      "thread:old-prefix",
      "thread:recent-content",
    ]);
  });

  it("preserves thread project-name matches when there is no stronger title match", () => {
    const group: CommandPaletteGroup = {
      value: "threads-search",
      label: "Threads",
      items: [
        {
          kind: "action",
          value: "thread:project-context-only",
          searchTerms: ["Fix navbar spacing", "Project"],
          title: "Fix navbar spacing",
          description: "Project",
          icon: null,
          run: async () => undefined,
        },
      ],
    };

    const groups = filterCommandPaletteGroups({
      activeGroups: [group],
      query: "project",
      isInSubmenu: false,
      projectSearchItems: [],
      threadSearchItems: [],
    });

    expect(groups).toHaveLength(1);
    expect(groups[0]?.items.map((item) => item.value)).toEqual(["thread:project-context-only"]);
  });

  it("ranks an order-independent setting title match above a split context match", () => {
    const settingsSearchItems = [
      {
        kind: "action" as const,
        value: "setting:context-match",
        searchTerms: ["Pairing settings", "remote backend"],
        title: "Context match",
        icon: null,
        run: async () => undefined,
      },
      {
        kind: "action" as const,
        value: "setting:remote-pairing",
        searchTerms: ["Remote pairing", "connections"],
        title: "Remote pairing",
        icon: null,
        run: async () => undefined,
      },
    ];

    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query: "pairing remote",
      isInSubmenu: false,
      projectSearchItems: [],
      settingsSearchItems,
      threadSearchItems: [],
    });

    expect(groups).toHaveLength(1);
    expect(groups[0]?.value).toBe("settings-search");
    expect(groups[0]?.items.map((item) => item.value)).toEqual([
      "setting:remote-pairing",
      "setting:context-match",
    ]);
  });

  it("keeps accent-insensitive setting results", () => {
    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query: "thè\u{1ab0}mes",
      isInSubmenu: false,
      projectSearchItems: [],
      settingsSearchItems: [
        {
          kind: "action",
          value: "setting:theme",
          searchTerms: ["Themes", "Appearance"],
          title: "Themes",
          icon: null,
          run: async () => undefined,
        },
      ],
      threadSearchItems: [],
    });

    expect(groups[0]?.items.map((item) => item.value)).toEqual(["setting:theme"]);
  });

  it("normalizes case independently of the host locale", () => {
    const toLocaleLowerCase = String.prototype.toLocaleLowerCase;
    const localeLowerCase = vi
      .spyOn(String.prototype, "toLocaleLowerCase")
      .mockImplementation(function (this: string) {
        return toLocaleLowerCase.call(this, "tr");
      });
    try {
      const groups = filterCommandPaletteGroups({
        activeGroups: [],
        query: "GIT",
        isInSubmenu: false,
        projectSearchItems: [],
        threadSearchItems: [],
        settingsSearchItems: [
          {
            kind: "action",
            value: "setting:version-control",
            title: "Version control",
            searchTerms: ["git"],
            icon: null,
            run: async () => undefined,
          },
        ],
      });
      expect(groups.flatMap((group) => group.items.map((item) => item.value))).toEqual([
        "setting:version-control",
      ]);
    } finally {
      localeLowerCase.mockRestore();
    }
  });

  it("keeps message excerpts searchable without replacing thread metadata", () => {
    const [item] = buildThreadActionItems({
      threads: [makeThread({ branch: "feat/search" })],
      projectTitleById: new Map([[PROJECT_ID, "T3 Code"]]),
      sortOrder: "updated_at",
      icon: null,
      getContentMatch: () => ({
        source: "assistant",
        snippet: "The relay reconnect is now bounded.",
        query: "reconnect",
      }),
      runThread: async (_thread) => undefined,
    });

    expect(item?.searchTerms).toContain("The relay reconnect is now bounded.");
    expect(item?.threadContentMatch).toEqual({
      source: "assistant",
      snippet: "The relay reconnect is now bounded.",
      query: "reconnect",
    });
    expect(item?.description).toBe("T3 Code · #feat/search");
  });

  it("surfaces threads when the query is their ID, without outranking title matches", () => {
    const idThread = makeThread({
      id: ThreadId.make("thread-alpha-1234"),
      title: "Unrelated work",
      updatedAt: "2026-03-05T00:00:00.000Z",
    });
    const titleThread = makeThread({
      id: ThreadId.make("thread-other-9999"),
      title: "Fix thread-alpha-1234 flakes",
      updatedAt: "2026-03-04T00:00:00.000Z",
    });
    const items = buildThreadActionItems({
      threads: [idThread, titleThread],
      projectTitleById: new Map([[PROJECT_ID, "T3 Code"]]),
      sortOrder: "updated_at",
      icon: null,
      runThread: async (_thread) => undefined,
    });

    const groups = filterCommandPaletteGroups({
      activeGroups: [],
      query: "  THREAD-ALPHA-1234  ",
      isInSubmenu: false,
      projectSearchItems: [],
      settingsSearchItems: [],
      threadSearchItems: items,
    });

    expect(groups.flatMap((group) => group.items)).toEqual([
      expect.objectContaining({ value: `thread:${titleThread.id}` }),
      expect.objectContaining({ value: `thread:${idThread.id}` }),
    ]);
  });

  it("prefers renderDescription when provided", () => {
    const [item] = buildThreadActionItems({
      threads: [makeThread({ branch: "feat/search", worktreePath: "/tmp/wt" })],
      projectTitleById: new Map([[PROJECT_ID, "T3 Code"]]),
      sortOrder: "updated_at",
      icon: null,
      renderDescription: (thread, { projectTitle }) =>
        `${projectTitle}:${thread.branch}:${thread.worktreePath ? "wt" : "local"}`,
      runThread: async (_thread) => undefined,
    });

    expect(item?.description).toBe("T3 Code:feat/search:wt");
  });

  it("filters archived threads out of thread search items", () => {
    const items = buildThreadActionItems({
      threads: [
        makeThread({
          id: ThreadId.make("thread-active"),
          title: "Active thread",
          createdAt: "2026-03-02T00:00:00.000Z",
          updatedAt: "2026-03-19T00:00:00.000Z",
        }),
        makeThread({
          id: ThreadId.make("thread-archived"),
          title: "Archived thread",
          archivedAt: "2026-03-20T00:00:00.000Z",
          updatedAt: "2026-03-20T00:00:00.000Z",
        }),
      ],
      projectTitleById: new Map([[PROJECT_ID, "Project"]]),
      sortOrder: "updated_at",
      icon: null,
      runThread: async (_thread) => undefined,
    });

    expect(items.map((item) => item.value)).toEqual(["thread:thread-active"]);
  });
});

describe("buildBrowseGroups", () => {
  it("waits for asynchronous browse navigation actions", async () => {
    let finishNavigation: (() => void) | undefined;
    const browseTo = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finishNavigation = resolve;
        }),
    );
    const groups = buildBrowseGroups({
      browseEntries: [{ name: "Downloads", fullPath: "/Users/test/Downloads" }],
      browseQuery: "~/",
      canBrowseUp: false,
      upIcon: null,
      directoryIcon: null,
      browseUp: vi.fn(),
      browseTo,
    });
    const item = groups[0]?.items[0];
    if (!item || item.kind !== "action") {
      throw new Error("Expected a browse action");
    }

    let actionSettled = false;
    const action = item.run().then(() => {
      actionSettled = true;
    });
    await Promise.resolve();

    expect(browseTo).toHaveBeenCalledWith("Downloads");
    expect(actionSettled).toBe(false);

    finishNavigation?.();
    await action;
    expect(actionSettled).toBe(true);
  });
});

describe("filterPinnedBrowseEntries", () => {
  const entries = [
    { name: "repo", fullPath: "/projects/repo" },
    { name: "work", fullPath: "/projects/work" },
  ];

  it("shows sibling folders without losing an existing pinned destination", () => {
    expect(
      filterPinnedBrowseEntries({
        browseEntries: entries,
        filterQuery: "repo",
        pinnedDirectoryName: "repo",
        caseSensitive: true,
      }),
    ).toEqual({ visibleEntries: entries, exactEntry: entries[0] });
  });

  it("matches an existing pinned destination without Windows casing", () => {
    const windowsEntries = [
      { name: "Repo", fullPath: "C:\\projects\\Repo" },
      { name: "work", fullPath: "C:\\projects\\work" },
    ];
    expect(
      filterPinnedBrowseEntries({
        browseEntries: windowsEntries,
        filterQuery: "repo",
        pinnedDirectoryName: "repo",
        caseSensitive: false,
      }),
    ).toEqual({
      visibleEntries: windowsEntries,
      exactEntry: windowsEntries[0],
    });
  });
});

it.each([
  "#10839",
  "10839",
  "pingdotgg/t3code#10839",
  "https://github.com/pingdotgg/t3code/pull/10839",
])("finds linked threads from PR query %s", (query) => {
  const items = buildThreadActionItems({
    threads: [
      makeThread({
        title: "Implementation",
        pullRequests: [
          {
            host: "github.com",
            repository: "pingdotgg/t3code",
            number: 10839,
            url: "https://github.com/pingdotgg/t3code/pull/10839",
            source: "manual",
            linkedAt: "2026-09-08T00:00:00Z",
            snapshot: null,
            stack: null,
          },
        ],
      }),
      makeThread({ id: ThreadId.make("unrelated"), title: "Other work" }),
    ],
    projectTitleById: new Map(),
    sortOrder: "updated_at",
    icon: null,
    runThread: async () => undefined,
  });
  const groups = filterCommandPaletteGroups({
    activeGroups: [],
    query,
    isInSubmenu: false,
    projectSearchItems: [],
    threadSearchItems: items,
  });
  expect(groups.flatMap((group) => group.items.map((item) => item.title))).toEqual([
    "Implementation",
  ]);
});

describe("filterCommandPaletteGroups", () => {
  it("sorts secondary settings results after other matches", () => {
    const item = (value: string, title: string, secondary?: boolean) =>
      ({
        kind: "action",
        value,
        title,
        searchTerms: [title, "General"],
        icon: null,
        run: async () => undefined,
        ...(secondary ? { secondary } : {}),
      }) satisfies CommandPaletteActionItem;
    const [group] = filterCommandPaletteGroups({
      activeGroups: [],
      query: "model",
      isInSubmenu: false,
      projectSearchItems: [],
      settingsSearchItems: [
        item("setting:keybinding-modelPicker.toggle", "Model Picker: Toggle", true),
        item("setting:default-model", "Default model"),
      ],
      threadSearchItems: [],
    });
    expect(group?.items.map((entry) => entry.value)).toEqual([
      "setting:default-model",
      "setting:keybinding-modelPicker.toggle",
    ]);
  });
});

describe("command palette runtime restart", () => {
  const threadRef = {
    environmentId: LOCAL_ENVIRONMENT_ID,
    threadId: ThreadId.make("restart-thread"),
  };
  const target = {
    binding: {
      threadId: threadRef.threadId,
      providerThreadId: "provider:current",
      providerSessionId: "session:current",
      instanceId: ProviderInstanceId.make("current-owner"),
      runtimeGeneration: "generation:current",
      nativeThreadId: "native:current",
    },
    driver: "codex",
    evidenceRevision: 7,
  } as OrchestrationV2CurrentThreadRuntimeTarget;
  const stopState = (status: CurrentRuntimeStopOutcome["status"]): CurrentRuntimeStopOutcome => ({
    status,
    commandAccepted: status !== "rejected",
    queueFenceInstalled: status !== "rejected",
    reason: null,
    commandId: CommandId.make("stop:original"),
    target,
  });
  const harness = () => {
    const runtimeStop = {
      capture: vi.fn(async () => ({ status: "current" as const, target })),
      request: vi.fn(async () => stopState("pending")),
      observe: vi.fn<
        (
          _ref: typeof threadRef,
          _expected?: CurrentRuntimeStopOperation,
        ) => Promise<CurrentRuntimeStopOutcome | null>
      >(async () => null),
    };
    const refresh = vi.fn(async (_target: OrchestrationV2CurrentThreadRuntimeTarget) => {});
    const notify = vi.fn<(_: CommandPaletteRestartNotice) => void>();
    return { threadRef, runtimeStop, refresh, notify };
  };

  it("does not refresh or claim restart success from acceptance and a queue fence", async () => {
    const input = harness();
    await restartCommandPaletteRuntime(input);
    expect(input.runtimeStop.request).toHaveBeenCalledWith(threadRef, target);
    expect(input.refresh).not.toHaveBeenCalled();
    expect(input.notify).toHaveBeenCalledWith(expect.objectContaining({ status: "pending" }));
    expect(input.notify.mock.calls.some(([notice]) => notice.status === "stopped")).toBe(false);
  });

  it("observes the original stop after pending or lost responses and refreshes its captured owner", async () => {
    const input = harness();
    input.runtimeStop.request.mockResolvedValue(stopState("unknown"));
    await restartCommandPaletteRuntime(input);
    const firstNotice = input.notify.mock.calls[0]?.[0];
    input.runtimeStop.observe.mockResolvedValueOnce(null);
    await firstNotice?.checkStatus?.();
    expect(input.refresh).not.toHaveBeenCalled();
    const unknownNotice = input.notify.mock.calls.at(-1)?.[0];
    expect(unknownNotice?.status).toBe("unknown");
    input.runtimeStop.observe.mockResolvedValueOnce(stopState("stopped"));
    await unknownNotice?.checkStatus?.();
    expect(input.runtimeStop.capture).toHaveBeenCalledTimes(1);
    expect(input.runtimeStop.request).toHaveBeenCalledTimes(1);
    expect(input.runtimeStop.observe).toHaveBeenLastCalledWith(threadRef, {
      commandId: CommandId.make("stop:original"),
      target,
    });
    expect(input.refresh).toHaveBeenCalledWith(target);
    expect(input.notify.mock.calls.at(-1)?.[0].status).toBe("stopped");
  });

  it("waits for the captured owner's refresh before announcing restart success", async () => {
    const input = harness();
    input.runtimeStop.request.mockResolvedValue(stopState("stopped"));
    let finishRefresh: (() => void) | undefined;
    let signalRefreshStarted: (() => void) | undefined;
    const refreshStarted = new Promise<void>((resolve) => {
      signalRefreshStarted = resolve;
    });
    input.refresh.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishRefresh = resolve;
          signalRefreshStarted?.();
        }),
    );
    const restarting = restartCommandPaletteRuntime(input);
    await refreshStarted;
    expect(input.refresh).toHaveBeenCalledWith(target);
    expect(input.notify).not.toHaveBeenCalled();
    finishRefresh?.();
    await restarting;
    expect(input.notify).toHaveBeenCalledWith({ status: "stopped", reason: null });
  });

  it.each(["unavailable", "known-stopped"] as const)(
    "does not claim it stopped a runtime from %s capture",
    async (status) => {
      const input = harness();
      const capture =
        status === "unavailable"
          ? { status, reason: "Runtime not resident; native closure is unproved." }
          : { status, observedAt: "2026-10-03T00:00:00Z" };
      await restartCommandPaletteRuntime({
        ...input,
        runtimeStop: { ...input.runtimeStop, capture: vi.fn(async () => capture) },
      });
      expect(input.runtimeStop.request).not.toHaveBeenCalled();
      expect(input.refresh).not.toHaveBeenCalled();
      expect(input.notify).toHaveBeenCalledWith({
        status,
        reason: "reason" in capture ? capture.reason : null,
      });
    },
  );

  it("recovers a remounted original stop before reading a different current runtime", async () => {
    const input = harness();
    const originalTarget = {
      ...target,
      binding: { ...target.binding, instanceId: ProviderInstanceId.make("original-owner") },
    };
    input.runtimeStop.observe.mockResolvedValueOnce({
      ...stopState("stopped"),
      target: originalTarget,
    });
    await restartCommandPaletteRuntime(input);
    expect(input.runtimeStop.capture).not.toHaveBeenCalled();
    expect(input.runtimeStop.request).not.toHaveBeenCalled();
    expect(input.refresh).toHaveBeenCalledWith(originalTarget);
    expect(input.notify).toHaveBeenCalledWith({ status: "stopped", reason: null });
  });

  it("keeps a stale status callback bound to its original operation after pointer replacement", async () => {
    const input = harness();
    await restartCommandPaletteRuntime(input);
    const checkStatus = input.notify.mock.calls[0]?.[0].checkStatus;
    input.runtimeStop.observe.mockResolvedValue({
      ...stopState("unknown"),
      commandId: null,
      target: null,
      reason: "The saved stop no longer matches the original operation.",
    });
    await checkStatus?.();
    const retryOriginal = input.notify.mock.calls.at(-1)?.[0].checkStatus;
    await retryOriginal?.();
    expect(input.runtimeStop.observe).toHaveBeenLastCalledWith(threadRef, {
      commandId: CommandId.make("stop:original"),
      target,
    });
    expect(input.runtimeStop.request).toHaveBeenCalledTimes(1);
    expect(input.runtimeStop.capture).toHaveBeenCalledTimes(1);
    expect(input.refresh).not.toHaveBeenCalled();
    expect(input.notify.mock.calls.at(-1)?.[0].status).toBe("unknown");
  });

  it.each(["pending", "stopped"] as const)(
    "does not refresh from %s without original correlation",
    async (status) => {
      const input = harness();
      input.runtimeStop.observe.mockResolvedValueOnce({
        ...stopState(status),
        commandId: null,
        target: null,
      });
      await restartCommandPaletteRuntime(input);
      expect(input.runtimeStop.capture).not.toHaveBeenCalled();
      expect(input.runtimeStop.request).not.toHaveBeenCalled();
      expect(input.refresh).not.toHaveBeenCalled();
      expect(input.notify).toHaveBeenCalledWith(expect.objectContaining({ status: "unknown" }));
    },
  );

  it("propagates refresh failure without a premature success notice", async () => {
    const input = harness();
    input.runtimeStop.request.mockResolvedValue(stopState("stopped"));
    input.refresh.mockRejectedValue(new Error("refresh unavailable"));
    await expect(restartCommandPaletteRuntime(input)).rejects.toThrow("refresh unavailable");
    expect(input.notify).not.toHaveBeenCalled();
  });

  it("reports a rejected stop without refreshing or offering redispatch", async () => {
    const input = harness();
    input.runtimeStop.request.mockResolvedValue(stopState("rejected"));
    await restartCommandPaletteRuntime(input);
    expect(input.refresh).not.toHaveBeenCalled();
    expect(input.notify).toHaveBeenCalledWith({ status: "rejected", reason: null });
  });
});
