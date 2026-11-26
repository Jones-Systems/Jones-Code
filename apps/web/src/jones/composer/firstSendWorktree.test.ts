import { describe, expect, it } from "vite-plus/test";

import { resolveFirstSendWorktreePreparation } from "./firstSendWorktree";

const draft = {
  isFirstMessage: true,
  sendEnvMode: "worktree",
  worktreePath: null,
  projectCwd: "/repo",
  projectIsGitRepo: true,
  tracksGitRepositories: false,
  baseBranch: null,
  startFromOrigin: false,
} as const;

describe("first-send Git freshness", () => {
  it("uses the last verified default unless the sender requests a new fetch", () => {
    expect(resolveFirstSendWorktreePreparation({ ...draft, verifiedDefault: true })).toEqual({
      projectCwd: "/repo",
      baseFreshness: "verified",
    });
    expect(
      resolveFirstSendWorktreePreparation({
        ...draft,
        verifiedDefault: true,
        startFromOrigin: true,
      }),
    ).toEqual({
      projectCwd: "/repo",
      baseFreshness: "fetch-now",
    });
  });

  it("leaves the automatic base to the server for a Git project with tracked repositories", () => {
    for (const repositories of [undefined, [{ path: "/repo/api" }], []]) {
      expect(
        resolveFirstSendWorktreePreparation({
          ...draft,
          tracksGitRepositories: true,
          repositories,
          verifiedDefault: true,
        }),
      ).toEqual({
        projectCwd: "/repo",
        ...(repositories === undefined ? {} : { repositories }),
      });
    }
  });

  it("uses the task-folder fetch flag when the sender requests a new fetch", () => {
    expect(
      resolveFirstSendWorktreePreparation({
        ...draft,
        tracksGitRepositories: true,
        startFromOrigin: true,
        verifiedDefault: true,
      }),
    ).toEqual({
      projectCwd: "/repo",
      startFromOrigin: true,
    });
  });

  it("leaves freshness to the server for a custom layout with no tracked repositories", () => {
    for (const startFromOrigin of [false, true]) {
      expect(
        resolveFirstSendWorktreePreparation({
          ...draft,
          verifiedDefault: true,
          hasCustomLayout: true,
          repositories: [],
          startFromOrigin,
        }),
      ).toEqual({
        projectCwd: "/repo",
        repositories: [],
        ...(startFromOrigin ? { startFromOrigin: true } : {}),
      });
    }
  });

  it("passes the plain workspace's base choice and empty repository selection", () => {
    for (const baseBranch of ["HEAD", "work/parent"]) {
      expect(
        resolveFirstSendWorktreePreparation({
          ...draft,
          projectIsGitRepo: false,
          tracksGitRepositories: true,
          repositories: [],
          baseBranch,
          verifiedDefault: true,
        }),
      ).toEqual({
        projectCwd: "/repo",
        baseBranch,
        repositories: [],
      });
    }
  });

  it("preserves explicitly chosen branches and the original path for unenrolled projects", () => {
    expect(
      resolveFirstSendWorktreePreparation({
        ...draft,
        verifiedDefault: true,
        baseBranch: "work/parent",
      }),
    ).toEqual({
      projectCwd: "/repo",
      baseBranch: "work/parent",
    });
    expect(resolveFirstSendWorktreePreparation({ ...draft, startFromOrigin: true })).toEqual({
      projectCwd: "/repo",
      startFromOrigin: true,
    });
    expect(
      resolveFirstSendWorktreePreparation({
        ...draft,
        verifiedDefault: true,
        isFirstMessage: false,
      }),
    ).toBeUndefined();
  });
});
