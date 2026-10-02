import { describe, expect, it } from "vite-plus/test";
import { nativeWorktreePath } from "./worktreePath.ts";

describe("native worktree path identity", () => {
  it("uses the actual root, checkout basename and slash-only branch replacement", () => {
    expect(
      nativeWorktreePath({
        worktreesDir: "/synthetic/native/worktrees",
        cwd: "/synthetic/checkouts/repository",
        branch: "voice/branch.name",
      }),
    ).toBe("/synthetic/native/worktrees/repository/voice-branch.name");
  });
  it("preserves explicit paths exactly", () => {
    expect(
      nativeWorktreePath({
        worktreesDir: "/synthetic/native",
        cwd: "/synthetic/repository",
        branch: "branch",
        path: "/exact/claimed/path",
      }),
    ).toBe("/exact/claimed/path");
  });
});
