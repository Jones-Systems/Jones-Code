import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { act, type ComponentProps } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";

const state = vi.hoisted(() => ({
  worktreePath: "/tmp/worktree" as string | null,
  delegated: false,
  environments: new Map<string, unknown>(),
  showContextMenu: vi.fn(),
  writeTextToClipboard: vi.fn(),
}));

vi.mock(import("../localApi"), async (original) => ({
  ...(await original()),
  readLocalApi: (() => ({ contextMenu: { show: state.showContextMenu } })) as never,
}));
vi.mock(import("../hooks/useCopyToClipboard"), async (original) => ({
  ...(await original()),
  writeTextToClipboard: state.writeTextToClipboard,
}));
vi.mock(import("./BranchToolbarBranchSelector"), async (original) => ({
  ...(await original()),
  BranchToolbarBranchSelector: (() => null) as never,
}));
vi.mock(import("../state/entities"), async (original) => ({
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

vi.mock(import("../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: ((id: string) => state.environments.get(id) ?? null) as never,
}));
import { MachineRouteBadge } from "../jones/remoteAgentLineage/MachineIdentity";
import { BranchToolbar } from "./BranchToolbar";

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.worktreePath = "/tmp/worktree";
  state.delegated = false;
  state.environments.clear();
  state.showContextMenu.mockReset().mockResolvedValue("copy-path");
  state.writeTextToClipboard.mockReset().mockResolvedValue(true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

function render(props: Partial<ComponentProps<typeof BranchToolbar>>) {
  return act(async () =>
    root.render(
      <BranchToolbar
        layout="panel"
        panelSection="workspace"
        environmentId={EnvironmentId.make("local")}
        threadId={ThreadId.make("thread")}
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

it("keeps machine choices usable when the combined row's workspace is locked", async () => {
  const onEnvironmentChange = vi.fn();
  await render({
    onEnvironmentChange,
    availableEnvironments: ["local", "remote"].map((id) => ({
      environmentId: EnvironmentId.make(id),
      projectId: ProjectId.make("project"),
      label: id,
      isPrimary: id === "local",
      machine: "server",
    })),
  });
  const trigger = container.querySelector("button")!;
  expect(trigger.textContent).toBe("local");
  expect(container.querySelectorAll("button")).toHaveLength(1);

  // A ctrl-click is a macOS secondary click; it must not open the menu.
  await act(async () => {
    trigger.dispatchEvent(new MouseEvent("mousedown", { bubbles: true, ctrlKey: true }));
  });
  expect(document.querySelector('[role="menu"]')).toBeNull();

  await userEvent.click(trigger);
  await expect
    .element(page.getByRole("menuitemradio", { name: "New worktree" }))
    .toHaveAttribute("aria-disabled", "true");
  await userEvent.click(page.getByRole("menuitemradio", { name: "remote" }));
  expect(onEnvironmentChange).toHaveBeenCalledWith("remote");
});

it.each([
  ["local", null, "/tmp/project"],
  ["worktree", null, null],
  ["worktree", "/tmp/worktree", "/tmp/worktree"],
] as const)(
  "copies only an existing workspace path with mode %s and worktree %s",
  async (envMode, worktreePath, copiedPath) => {
    state.worktreePath = worktreePath;
    await render({ envMode });
    const event = new MouseEvent("contextmenu", { bubbles: true, cancelable: true });
    await act(async () => {
      container.querySelector('[aria-label="Run context"]')!.dispatchEvent(event);
    });
    expect(event.defaultPrevented).toBe(copiedPath !== null);
    if (copiedPath === null) {
      expect(state.showContextMenu).not.toHaveBeenCalled();
      expect(state.writeTextToClipboard).not.toHaveBeenCalled();
    } else {
      await vi.waitFor(() =>
        expect(state.writeTextToClipboard).toHaveBeenCalledWith(copiedPath, "workspace path"),
      );
    }
  },
);

it("shows destination identity and a quiet origin independently of the project workspace", async () => {
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
  state.delegated = true;
  state.environments.set("origin", { label: "Laptop", connection: { phase: "offline" } });
  state.environments.set("local", { label: "Test Server", connection: { phase: "connected" } });
  await act(async () =>
    root.render(
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
