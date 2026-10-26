import { cloneElement, type ReactElement, type ReactNode } from "react";
import { act, render as renderTest, type Screen } from "../../../test/render";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { expect, it, vi } from "vite-plus/test";

import "~/index.css";

function expectCollapsedGroup(id: string, label: string, count: number) {
  const toggle = container.querySelector(`[data-linked-group="${id}"] button`);
  expect(toggle?.textContent).toContain(label);
  expect(toggle?.getAttribute("aria-expanded")).toBe("false");
  expect(toggle?.querySelector(".tabular-nums")?.textContent?.trim()).toBe(String(count));
}

const state = vi.hoisted(() => ({
  stopWorkflowLabel: "environment-data:orchestration:stop-workflow",
  projection: null as unknown,
  navigate: vi.fn(),
  shells: [] as unknown[],
  projects: [] as unknown[],
  configs: new Map<string, unknown>(),
  showTooltips: false,
  environments: new Map<string, unknown>(),
  quietShells: new Map<string, unknown>(),
  command: vi.fn().mockResolvedValue({ _tag: "Success" }),
  workflowCommand: vi.fn().mockResolvedValue({ _tag: "Success" }),
  toast: vi.fn(),
}));

vi.mock(import("../linkedThreads/useLinkedThreads"), () => ({
  useLinkedThreads: () => ({
    links: [],
    relations: [],
    snapshots: new Map(),
    environments: [],
    errors: [],
    offline: [],
    answering: new Set(),
    shells: [],
    loading: false,
    incomplete: false,
    truncated: false,
    visible: false,
    canLink: false,
    canCopy: false,
    hasSuccessor: false,
    canAccept: () => false,
    canUnlink: () => false,
    onExpand: () => {},
    link: async () => false,
    unlink: async () => false,
    accept: async () => false,
  }),
}));
vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  useNavigate: (() => state.navigate) as never,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadProjection: (() => ({ projection: state.projection })) as never,
  useThreadRelationshipShells: ((ref: { readonly environmentId: string }) =>
    (state.shells as ReadonlyArray<{ readonly environmentId: string; readonly source: unknown }>)
      .filter((shell) => shell.environmentId === ref.environmentId)
      .map((shell) => shell.source)) as never,
  useProjects: (() => state.projects) as never,
  useQuietThreadShell: ((ref: { environmentId: string; threadId: string } | null) =>
    ref ? (state.quietShells.get(`${ref.environmentId}:${ref.threadId}`) ?? null) : null) as never,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: ((id: string) => state.environments.get(id) ?? null) as never,
  useEnvironmentProviders: (() =>
    new Map(
      [...state.configs].map(([id, config]) => [id, (config as { providers: unknown }).providers]),
    )) as never,
}));
vi.mock(import("../../lib/archivedThreadsState"), async (original) => ({
  ...(await original()),
  useArchivedThreadSnapshots: (() => ({ snapshots: [] })) as never,
}));
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: { label?: string }) =>
    command.label === state.stopWorkflowLabel ? state.workflowCommand : state.command) as never,
}));
vi.mock(import("../../components/ui/toast"), async (original) => {
  const actual = await original();
  return { ...actual, toastManager: { ...actual.toastManager, add: state.toast } as never };
});
vi.mock(import("../../state/orchestration"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const granted = Atom.make(true);
  const stopWorkflow = actual.orchestrationEnvironment.stopWorkflow;
  return {
    ...actual,
    orchestrationEnvironment: Object.assign(Object.create(actual.orchestrationEnvironment), {
      stopWorkflow: Object.assign(Object.create(stopWorkflow), {
        label: state.stopWorkflowLabel,
        permissionAtom: () => granted,
      }),
    }),
  };
});
// Tooltip and hover card content renders inline so the suite can read every row's agent
// details across many states without hovering each row.
vi.mock(import("../../components/ui/tooltip"), async () => {
  const { cloneElement } = await import("react");
  return {
    Tooltip: (({ children }: { children: ReactNode }) => children) as never,
    TooltipTrigger: (({ render, children }: { render: ReactElement; children: ReactNode }) =>
      cloneElement(render, {}, children)) as never,
    TooltipPopup: (({ children }: { children: ReactNode }) =>
      state.showTooltips ? children : null) as never,
  };
});

import { ThreadRelationshipsPanel } from "../../components/chat/ThreadRelationshipsControl";

let root: Screen;
let container: HTMLDivElement;
let current: ReactElement;

async function render(element: ReactElement) {
  current = element;
  await act(async () => root.rerender(element));
}
// The mocked hooks read `state` while rendering, so a new element picks up changes.
const update = () => act(async () => root.rerender(cloneElement(current)));
const click = (element: HTMLElement) => act(async () => element.click());
const buttons = () => [...container.querySelectorAll("button")];
const allByLabel = (label: string) =>
  buttons().filter((button) => button.getAttribute("aria-label") === label);
function byLabel(label: string) {
  const matches = allByLabel(label);
  if (matches.length !== 1)
    throw new Error(`Expected one "${label}" button, got ${matches.length}`);
  return matches[0]!;
}
const sectionToggles = (expanded: boolean) =>
  buttons().filter(
    (button) =>
      button.getAttribute("aria-expanded") === String(expanded) &&
      !button.hasAttribute("aria-label"),
  );
function sectionToggle(expanded: boolean) {
  const [toggle] = sectionToggles(expanded);
  if (!toggle) throw new Error(`No ${expanded ? "expanded" : "collapsed"} section toggle`);
  return toggle;
}
const heading = () => container.querySelector("h3")?.textContent;
const linkedToggle = () => byLabel("Linked threads");
// Joins rendered text nodes; `visibleOnly` drops screen-reader-only text.
function textNodes(separator: string, visibleOnly = false) {
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
  const parts: string[] = [];
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    if (visibleOnly && node.parentElement?.closest(".sr-only")) continue;
    parts.push(node.textContent ?? "");
  }
  return parts.join(separator);
}

async function setup() {
  state.shells = [];
  state.projects = [];
  state.configs.clear();
  state.environments.clear();
  state.quietShells.clear();
  state.showTooltips = false;
  state.navigate.mockClear();
  state.command.mockClear();
  state.workflowCommand.mockReset().mockResolvedValue({ _tag: "Success" });
  state.toast.mockClear();
  state.projection = null;
  state.environments.set("test", { label: "VPS", connection: { phase: "connected" } });
  root = await renderTest(null, { router: false });
  container = root.container as HTMLDivElement;
  return root;
}

it("shows the matching child agent details and refreshes them when the agent settles", async () => {
  await using _screen = await setup();
  const agent = {
    id: "agent-1",
    threadId: "parent",
    origin: "app_owned",
    driver: "codex",
    providerInstanceId: "codex",
    childThreadId: "child-1",
    title: "Checker",
    prompt: "Check the change",
    model: "gpt-5.4",
    status: "running",
    progress: "Running checks",
    result: null,
    startedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z"),
    completedAt: null,
    updatedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z"),
  };
  const projection = {
    thread: {
      id: "parent",
      lineage: { relationshipToParent: null },
      activeProviderThreadId: null,
    },
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [
      { ...agent, id: "unlinked", childThreadId: null, title: "Unlinked agent" },
      agent,
      { ...agent, id: "agent-2", childThreadId: "child-2", title: "Worker", model: "gpt-5.3" },
    ],
  };
  state.projection = projection;
  const panel = (
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("parent")}
    />
  );
  await render(panel);
  const text = () => textNodes(" ").replace(/\s+/g, " ");
  expect(linkedToggle().getAttribute("aria-expanded")).toBe("false");
  expect(text()).toContain("Linked · 3 running");
  expect(text()).not.toContain("Checker");
  expect(allByLabel("Stop subagent Checker")).toHaveLength(0);
  await click(linkedToggle());
  expect(text()).toContain("Checker");
  expect(text()).toContain("Linked · 3 running");
  expect(text()).toContain("running");
  expect(text()).not.toContain("gpt-5.4");
  expect(text()).not.toContain("gpt-5.3");
  expect(text()).not.toContain("tok");
  expect(text()).not.toContain("Unlinked agent");
  expect(text()).not.toContain("Active agents");
  expect(sectionToggles(true)).toHaveLength(0);
  await click(byLabel("Stop subagent Checker"));
  expect(state.command).toHaveBeenLastCalledWith({
    environmentId: "test",
    input: { threadId: "parent", subagentId: "agent-1" },
  });
  await click(linkedToggle());
  expect(text()).not.toContain("Checker");
  expect(text()).toContain("Linked · 3 running");
  await click(linkedToggle());
  expect(text()).toContain("Checker");

  state.projection = {
    ...projection,
    subagents: [
      {
        ...agent,
        status: "completed",
        progress: undefined,
        result: "All checks passed",
        completedAt: DateTime.makeUnsafe("2026-09-16T12:02:15Z"),
      },
    ],
  };
  await update();
  expect(heading()).toBe("Linked (1)");
  expectCollapsedGroup("previous", "Previous agents", 1);
  expect(text()).not.toContain("Checker");
  await click(sectionToggle(false));
  expect(text()).toContain("Checker");
  const previousGroup = container.querySelector('[data-linked-group="previous"]')!;
  const previousRow = previousGroup.querySelector("li")!;
  expect(previousRow.textContent).toContain("Checker");
  expect(
    [...previousRow.querySelectorAll("span")].some((span) => span.textContent === "Delegated task"),
  ).toBe(true);
  expect(previousRow.textContent).toContain("on VPS · Done");
  expect(previousGroup.querySelector("button")?.textContent?.trim()).toBe("Previous agents");
  expect(heading()).toBe("Linked (1)");
  expect(text()).toContain("Done");
  expect(text()).not.toContain("running");
  expect(text()).not.toContain("Worker");
  await click(sectionToggle(true));
  expect(text()).not.toContain("Checker");
  expectCollapsedGroup("previous", "Previous agents", 1);
  await click(sectionToggle(false));
  expect(text()).toContain("Checker");

  state.projection = {
    ...projection,
    subagents: Array.from({ length: 8 }, (_, index) => ({
      ...agent,
      id: `running-agent-${index}`,
      childThreadId: `running-child-${index}`,
    })),
  };
  await update();
  expect(text()).toContain("Linked · 8 running");

  state.projection = {
    ...projection,
    subagents: Array.from({ length: 8 }, (_, index) => ({
      ...agent,
      id: `old-agent-${index}`,
      childThreadId: `old-child-${index}`,
      status: index === 7 ? "failed" : "completed",
      title: `Old agent ${index}`,
      result: index === 7 ? "Earlier build failed" : "Done",
      completedAt: DateTime.makeUnsafe("2026-09-16T12:02:15Z"),
    })),
  };
  await update();
  expect(text()).toContain("1 failed");
  expect(text()).not.toContain("Old agent 7");
  await click(buttons().find((button) => button.textContent?.startsWith("Show "))!);
  expect(text()).toContain("Old agent 7");

  state.projection = {
    ...projection,
    subagents: [{ ...agent, childThreadId: null }],
  };
  await update();
  expect(text()).toContain("Linked · 1 running");
});

it("keeps fork ancestry navigable inside Linked and closes Linked when the selected thread changes", async () => {
  await using _screen = await setup();
  const source = {
    id: "source",
    title: "Source conversation",
    status: "idle",
    lineage: { parentThreadId: null, relationshipToParent: null },
  };
  const selected = {
    ...source,
    id: "selected",
    title: "Selected fork",
    forkedFrom: { type: "run", threadId: "source", runId: "source-run" },
    lineage: { parentThreadId: "source", relationshipToParent: "fork" },
  };
  const child = {
    ...source,
    id: "child",
    title: "Child fork",
    forkedFrom: { type: "run", threadId: "selected", runId: "selected-run" },
    lineage: { parentThreadId: "selected", relationshipToParent: "fork" },
  };
  state.shells = [source, selected, child].map((shell) => ({
    environmentId: "test",
    source: shell,
  }));
  state.projection = {
    thread: selected,
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [],
  };
  await render(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("selected")}
    />,
  );
  expect(container.querySelectorAll("li")).toHaveLength(0);
  await click(linkedToggle());
  expect(textNodes(" ")).toContain("Forked from");
  expectCollapsedGroup("children", "Children", 1);
  await click(buttons().find((button) => button.textContent?.includes("Children"))!);
  expect(textNodes(" ")).not.toContain("Delegated tasks");
  const sourceButton = buttons().find((button) =>
    button.textContent?.includes("Source conversation"),
  )!;
  await click(sourceButton);
  expect(state.navigate).toHaveBeenLastCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "test", threadId: "source" },
  });
  await click(buttons().find((button) => button.textContent?.includes("Child fork"))!);
  expect(state.navigate).toHaveBeenLastCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "test", threadId: "child" },
  });
  state.projection = { ...(state.projection as object), thread: child };
  await render(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("child")}
    />,
  );
  expect(linkedToggle().getAttribute("aria-expanded")).toBe("false");
  expect(container.querySelectorAll("li")).toHaveLength(0);
});

it("combines remote and local history before collapse and pagination, then restores resumed tasks", async () => {
  await using _screen = await setup();
  const timestamp = DateTime.makeUnsafe("2026-09-21T12:00:00Z");
  const statuses = ["completed", "failed", "cancelled", "interrupted", "idle", "completed"];
  const agents = statuses.map((status, index) => ({
    id: `remote-${index}`,
    threadId: "parent",
    childThreadId: null,
    driver: "codex",
    providerInstanceId: "codex",
    title: `Remote task ${index}`,
    status,
    startedAt: timestamp,
    updatedAt: timestamp,
    completedAt: timestamp,
    remoteChild: {
      environmentId: "destination",
      threadId: `child-${index}`,
      label: "Test Server",
      stoppedThere: index % 2 === 0,
    },
  }));
  const local = {
    ...agents[0],
    id: "local",
    childThreadId: "local-child",
    title: "Local history",
    remoteChild: undefined,
  };
  const projection = {
    thread: { id: "parent", lineage: { relationshipToParent: null } },
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [local, ...agents],
  };
  state.projection = projection;
  state.environments.set("destination", {
    label: "Test Server",
    connection: { phase: "connected" },
  });
  await render(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("parent")}
    />,
  );
  await click(linkedToggle());
  expectCollapsedGroup("previous", "Previous agents", 7);
  expect(textNodes(" ").replace(/\s+/g, " ")).not.toContain("Remote task");
  await click(sectionToggle(false));
  expect(container.querySelectorAll("li")).toHaveLength(6);
  const more = buttons().find((button) => button.textContent?.includes("Show 1 more"))!;
  await click(more);
  expect(container.querySelectorAll("li")).toHaveLength(7);
  expect(textNodes(" ").replace(/\s+/g, " ")).toContain("on TSV");
  state.projection = {
    ...projection,
    subagents: [
      local,
      ...agents.map((agent, index) =>
        index === 0
          ? {
              ...agent,
              status: "running",
              completedAt: null,
              remoteChild: { ...agent.remoteChild, stoppedThere: true },
            }
          : agent,
      ),
    ],
  };
  await update();
  expect(heading()).toBe("Linked · 1 running (7)");
  await click(sectionToggle(true));
  expectCollapsedGroup("previous", "Previous agents", 6);
  expect(container.querySelectorAll("li")).toHaveLength(1);
  const resumed = buttons().find((button) => button.textContent?.includes("Remote task 0"))!;
  await click(resumed);
  expect(state.navigate).toHaveBeenLastCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "destination", threadId: "child-0" },
  });
});

it("opens a scoped remote parent, retains its cached title offline, and preserves local precedence", async () => {
  await using _screen = await setup();
  const thread = {
    id: "child",
    lineage: { relationshipToParent: null },
    delegatedFrom: { environmentId: "origin", threadId: "equal-id", title: "Delegation title" },
    linkOrigin: { label: "Laptop" },
  };
  const projection = {
    thread,
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [],
  };
  state.projection = projection;
  state.showTooltips = true;
  state.quietShells.set("test:equal-id", { title: "Wrong local title" });
  await render(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("child")}
    />,
  );
  await click(linkedToggle());
  expect(byLabel("Parent agent: Delegation title").disabled).toBe(true);
  expect(textNodes(" ").replace(/\s+/g, " ")).toContain("Connect to it to open the parent");
  expect(textNodes(" ").replace(/\s+/g, " ")).not.toContain("Wrong local title");
  state.quietShells.set("origin:equal-id", { title: "Cached parent" });
  state.environments.set("origin", { label: "Laptop", connection: { phase: "offline" } });
  await update();
  expect(byLabel("Parent agent: Cached parent").disabled).toBe(true);
  state.environments.set("origin", { label: "Laptop", connection: { phase: "connected" } });
  await update();
  await click(byLabel("Parent agent: Cached parent"));
  expect(state.navigate).toHaveBeenLastCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "origin", threadId: "equal-id" },
  });
  state.projection = {
    ...projection,
    thread: {
      ...thread,
      lineage: { relationshipToParent: "subagent", parentThreadId: "equal-id" },
    },
  };
  state.shells = [
    { environmentId: "test", source: (state.projection as typeof projection).thread },
    {
      environmentId: "test",
      source: {
        id: "equal-id",
        title: "Local parent",
        lineage: { relationshipToParent: null, parentThreadId: null },
        status: "idle",
      },
    },
  ];
  await update();
  expect(allByLabel("Parent agent: Cached parent")).toHaveLength(0);
  expect(textNodes(" ").replace(/\s+/g, " ")).toContain("Local parent");
});

it("keeps a long remote title ellipsized and full on hover beside unknown environment metadata", async () => {
  await using _screen = await setup();
  const title = "A long remote task title ".repeat(20);
  const timestamp = DateTime.makeUnsafe("2026-09-21T12:00:00Z");
  container.style.width = "360px";
  state.projection = {
    thread: { id: "parent", lineage: { relationshipToParent: null } },
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [
      {
        id: "task",
        title,
        status: "running",
        driver: "codex",
        childThreadId: null,
        startedAt: timestamp,
        completedAt: null,
        updatedAt: timestamp,
        remoteChild: { environmentId: "unknown", threadId: "child", label: "Other host" },
      },
    ],
  };
  await render(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("parent")}
    />,
  );
  await click(linkedToggle());
  const label = container.querySelector<HTMLElement>("span.truncate")!;
  expect(label.closest("button")?.getAttribute("aria-label")).toContain(title);
  state.showTooltips = true;
  await update();
  expect(textNodes(" ")).toContain(title);
  expect(getComputedStyle(label).textOverflow).toBe("ellipsis");
  expect(label.scrollWidth).toBeGreaterThan(label.clientWidth);
  expect(textNodes(" ").replace(/\s+/g, " ")).toContain("on Other host");
  expect(buttons().find((button) => button.textContent?.includes("Other host"))?.disabled).toBe(
    true,
  );
});
