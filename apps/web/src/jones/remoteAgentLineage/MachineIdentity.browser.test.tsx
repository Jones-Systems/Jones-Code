import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import type { ComponentProps } from "react";
import { act, render as renderTest, type Screen } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import "~/index.css";

const state = vi.hoisted(() => ({
  worktreePath: "/tmp/worktree" as string | null,
  delegated: false,
  savedOrigin: null as string | null,
  environments: new Map<string, unknown>(),
  showContextMenu: vi.fn(),
  writeTextToClipboard: vi.fn(),
}));

vi.mock(import("../../localApi"), async (original) => ({
  ...(await original()),
  readLocalApi: (() => ({ contextMenu: { show: state.showContextMenu } })) as never,
}));
vi.mock(import("../../hooks/useCopyToClipboard"), async (original) => ({
  ...(await original()),
  writeTextToClipboard: state.writeTextToClipboard,
}));
vi.mock(import("../../components/BranchToolbarBranchSelector"), async (original) => ({
  ...(await original()),
  BranchToolbarBranchSelector: (() => null) as never,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useQuietThreadShell: (() => ({
    environmentId: "local",
    projectId: "project",
    worktreePath: state.worktreePath,
    ...(state.delegated
      ? {
          delegatedFrom: { environmentId: "origin", threadId: "parent", title: "Parent" },
          linkOrigin: { label: "Laptop" },
        }
      : {}),
    ...(state.savedOrigin ? { linkOrigin: { label: state.savedOrigin } } : {}),
  })) as never,
  useProject: (() => ({ workspaceRoot: "/tmp/project" })) as never,
  useThreadShellsForProjectRefs: () => [],
}));

vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: ((id: string) => state.environments.get(id) ?? null) as never,
}));
import { MachineRouteBadge } from "./MachineIdentity";
import { ChatHeader } from "../../components/chat/ChatHeader";
import { BranchToolbar } from "../../components/BranchToolbar";

let root: Screen;
let container: HTMLDivElement;

async function setup() {
  state.worktreePath = "/tmp/worktree";
  state.delegated = false;
  state.savedOrigin = null;
  state.environments.clear();
  state.showContextMenu.mockReset().mockResolvedValue("copy-path");
  state.writeTextToClipboard.mockReset().mockResolvedValue(true);
  root = await renderTest(null, { router: false });
  container = root.container as HTMLDivElement;
  return root;
}

function render(props: Partial<ComponentProps<typeof BranchToolbar>>) {
  return act(async () =>
    root.rerender(
      <BranchToolbar
        layout="panel"
        panelSection="workspace"
        environmentId={EnvironmentId.make("local")}
        threadId={ThreadId.make("thread")}
        projectId={ProjectId.make("project")}
        worktreePath={state.worktreePath}
        isServerThread={false}
        showGitControls
        envMode="local"
        envLocked={false}
        startFromOrigin={false}
        onStartFromOriginChange={vi.fn()}
        onEnvModeChange={vi.fn()}
        {...props}
      />,
    ),
  );
}

it("shows destination identity and a quiet origin independently of the project workspace", async () => {
  await using _screen = await setup();
  state.delegated = true;
  state.environments.set("origin", { label: "Laptop", connection: { phase: "offline" } });
  await render({
    envLocked: true,
    availableEnvironments: [
      {
        environmentId: EnvironmentId.make("local"),
        projectId: ProjectId.make("unrelated-project"),
        label: "VPS",
        displayLabel: "Office",
        machine: "server",
        isPrimary: true,
      },
    ],
  });
  const row = container.querySelector<HTMLElement>('[aria-label^="Run context:"]')!;
  expect(row.textContent).toContain("🧠");
  expect(row.textContent).toContain("Office");
  expect(row.textContent).not.toContain("VPS");
  expect(row.textContent).toContain("from 🏡");
  expect(row.textContent).not.toContain("unrelated-project");
  const origin = row.querySelector<HTMLElement>('[aria-label="From Laptop"]')!;
  expect(origin.getBoundingClientRect().left).toBeGreaterThan(row.getBoundingClientRect().left);
  state.worktreePath = "/another/project";
  await render({
    envLocked: true,
    availableEnvironments: [
      {
        environmentId: EnvironmentId.make("local"),
        projectId: null,
        label: "VPS",
        machine: "server",
        isPrimary: true,
      },
    ],
  });
  expect(container.textContent).toContain("🧠");
  expect(container.textContent).toContain("from 🏡");
});

it("keeps the approved touching emoji pair and scaled outlined route arrow when disconnected", async () => {
  await using _screen = await setup();
  state.delegated = true;
  state.environments.set("origin", { label: "Laptop", connection: { phase: "offline" } });
  state.environments.set("local", { label: "Test Server", connection: { phase: "connected" } });
  await act(async () =>
    root.rerender(
      <MachineRouteBadge
        environmentId={EnvironmentId.make("local")}
        threadId={ThreadId.make("thread")}
      />,
    ),
  );
  const pair = container.querySelector<HTMLElement>('[role="img"]')!;
  expect(pair.getAttribute("aria-label")).toBe("From Laptop to Test Server");
  expect(pair.textContent).toBe("🏡🤖");
  const [source, destination] = [...pair.children].map((element) =>
    element.getBoundingClientRect(),
  );
  expect(pair.getBoundingClientRect().width).toBe(32);
  expect(source!.width).toBe(16);
  expect(destination!.width).toBe(16);
  expect(destination!.left - source!.right).toBe(0);
  const arrow = pair.querySelector("svg")!;
  expect(arrow.getBoundingClientRect().width).toBeCloseTo(12.96, 1);
  expect(arrow.getBoundingClientRect().height).toBeCloseTo(10.8, 1);
  const arrowCenter = arrow.getBoundingClientRect().top + arrow.getBoundingClientRect().height / 2;
  const pairCenter = pair.getBoundingClientRect().top + pair.getBoundingClientRect().height / 2;
  expect(arrowCenter - pairCenter).toBeCloseTo(3.2, 1);
});

it("shows the saved Mini → VPS route alongside the clickable parent breadcrumb", async () => {
  await using screen = await setup();
  state.savedOrigin = "T3 Code · Example’s Mac mini (2)";
  state.environments.set("local", { label: "VPS" });
  const openParent = vi.fn();
  await act(async () =>
    screen.rerender(
      <ChatHeader
        activeThreadEnvironmentId={EnvironmentId.make("local")}
        activeThreadId={ThreadId.make("thread")}
        activeThreadTitle="Review the deployment"
        isServerThread
        activeProject={null}
        parentThreadLink={{
          threadId: ThreadId.make("parent"),
          title: "Parent task",
          open: openParent,
        }}
        onOpenThread={vi.fn()}
        rightPanelOpen={false}
        onNewThreadInProject={vi.fn()}
      />,
    ),
  );

  const breadcrumb = screen.getByRole("navigation", { name: "Thread breadcrumb" });
  const route = breadcrumb.getByRole("img", { name: "From Example’s Mac mini (2) to VPS" });
  await expect.element(route).toHaveTextContent("🍎🧠");
  await expect.element(breadcrumb.getByText(/T3 Code/)).not.toBeInTheDocument();
  const parent = breadcrumb.getByRole("button", { name: "Open parent thread: Parent task" });
  await parent.click();
  expect(openParent).toHaveBeenCalledOnce();

  const title = breadcrumb.getByRole("button", {
    name: "Thread actions for Review the deployment",
  });
  await expect.element(title).toBeVisible();
  await userEvent.tab();
  await expect.element(title).toHaveFocus();
});
