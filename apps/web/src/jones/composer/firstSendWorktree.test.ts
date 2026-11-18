import { describe, expect, it } from "vite-plus/test";

import { resolveFirstSendWorktreePreparation } from "./firstSendWorktree";

const draft = {
  isFirstMessage: true,
  sendEnvMode: "worktree",
  worktreePath: null,
  projectCwd: "/repo",
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
