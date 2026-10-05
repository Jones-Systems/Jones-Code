import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  resolveProjectThreadCreationBranch,
  validateProjectThreadCreation,
} from "./projectThreadCreationValidation";

describe("validateProjectThreadCreation", () => {
  it("accepts a worktree task while its automatic base is still loading", () => {
    expect(
      validateProjectThreadCreation({
        environmentId: EnvironmentId.make("environment"),
        projectId: ProjectId.make("project"),
        environmentMode: "worktree",
        branch: null,
        initialMessageText: "Start the task",
      }),
    ).toBeNull();
  });

  it("still rejects an empty worktree task", () => {
    expect(
      validateProjectThreadCreation({
        environmentId: EnvironmentId.make("environment"),
        projectId: ProjectId.make("project"),
        environmentMode: "worktree",
        branch: null,
        initialMessageText: " \n ",
      }),
    ).toMatchObject({ _tag: "ProjectThreadTaskRequiredError" });
  });
});

describe("resolveProjectThreadCreationBranch", () => {
  it("uses the live checkout for an untouched local draft label and recorded branch", () => {
    expect(
      resolveProjectThreadCreationBranch({
        workspaceMode: "local",
        selectedBranch: null,
        currentCheckoutBranch: "feature/x",
      }),
    ).toBe("feature/x");
  });

  it("prefers an explicit picker choice over the current checkout", () => {
    expect(
      resolveProjectThreadCreationBranch({
        workspaceMode: "local",
        selectedBranch: "main",
        currentCheckoutBranch: "feature/x",
      }),
    ).toBe("main");
  });

  it("stays null when no ref is checked out (detached HEAD, non-repository, status not loaded)", () => {
    expect(
      resolveProjectThreadCreationBranch({
        workspaceMode: "local",
        selectedBranch: null,
        currentCheckoutBranch: null,
      }),
    ).toBeNull();
  });

  it("never borrows the current checkout for a worktree draft", () => {
    expect(
      resolveProjectThreadCreationBranch({
        workspaceMode: "worktree",
        selectedBranch: null,
        currentCheckoutBranch: "feature/x",
      }),
    ).toBeNull();
  });

  it("keeps the explicit base branch for a worktree draft", () => {
    expect(
      resolveProjectThreadCreationBranch({
        workspaceMode: "worktree",
        selectedBranch: "main",
        currentCheckoutBranch: "feature/x",
      }),
    ).toBe("main");
  });
});
