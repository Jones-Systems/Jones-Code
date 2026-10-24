import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import type { ComponentProps } from "react";
import { act, render as renderTest, type Screen } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";

import "~/index.css";

const state = vi.hoisted(() => ({
  worktreePath: "/tmp/worktree" as string | null,
  delegated: false,
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
  })) as never,
  useProject: (() => ({ workspaceRoot: "/tmp/project" })) as never,
  useThreadShellsForProjectRefs: () => [],
}));

vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: ((id: string) => state.environments.get(id) ?? null) as never,
}));
import { MachineRouteBadge } from "./MachineIdentity";
import { BranchToolbar } from "../../components/BranchToolbar";

let root: Screen;
let container: HTMLDivElement;

async function setup() {
  state.worktreePath = "/tmp/worktree";
  state.delegated = false;
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
        machine: "server",
        isPrimary: true,
      },
    ],
  });
  const row = container.querySelector<HTMLElement>('[aria-label="Run context"]')!;
  expect(row.textContent).toContain("🧠");
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
