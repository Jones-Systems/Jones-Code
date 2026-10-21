import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const harness = vi.hoisted(() => ({
  discoveryTargets: [] as unknown[],
}));

// Each discovery run spawns provider CLIs on the server, so a thread screen must not start one.
vi.mock("./sourceControl", () => ({
  sourceControlEnvironment: {
    discovery: (target: unknown) => {
      harness.discoveryTargets.push(target);
      return null;
    },
  },
}));
vi.mock("./queries", () => ({
  useBranches: () => ({
    data: {
      refs: [
        { name: "main", isRemote: false, current: true, isDefault: true, worktreePath: null },
        { name: "origin/main", isRemote: true, remoteName: "origin", current: false },
      ],
    },
    isPending: false,
  }),
}));
vi.mock("./use-vcs-action-state", () => ({
  useVcsActionState: () => ({ currentLabel: "Pushing" }),
}));
vi.mock("./use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: { environmentId: "environment-1", id: "thread-1" },
    selectedThreadProject: { workspaceRoot: "/repo" },
  }),
}));
vi.mock("./use-selected-thread-worktree", () => ({
  useSelectedThreadWorktree: () => ({ selectedThreadCwd: "/repo" }),
}));

import { useSelectedThreadGitState } from "./use-selected-thread-git-state";

let root: Root | null = null;
let gitState: ReturnType<typeof useSelectedThreadGitState> | null = null;

function Consumer() {
  gitState = useSelectedThreadGitState();
  return null;
}

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  harness.discoveryTargets = [];
  gitState = null;
});

afterEach(() => {
  act(() => root?.unmount());
  root = null;
});

describe("useSelectedThreadGitState", () => {
  it("serves a selected thread's git state without starting source-control discovery", () => {
    root = createRoot(document.createElement("div"));
    act(() => root!.render(<Consumer />));

    expect(gitState?.gitOperationLabel).toBe("Pushing");
    expect(gitState?.selectedThreadBranches.map((branch) => branch.name)).toEqual(["main"]);
    expect(harness.discoveryTargets).toEqual([]);
  });
});
