import { describe, expect, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import {
  planStandaloneBirthPlacement,
  planStandaloneCheckout,
  planDelegatedCheckout,
} from "./DelegatedCheckoutPolicy.ts";

const input = {
  commandId: CommandId.make("delegate:one"),
  parentThreadId: ThreadId.make("parent"),
  parentCheckoutPath: "/workspace/parent",
  parentCommit: "a".repeat(40),
  childThreadId: ThreadId.make("child:one"),
  canonicalProjectRoot: "/workspace/project",
  canonicalWorktreesDir: "/workspace/worktrees",
};

describe("DP1 committed-base delegated checkout plan", () => {
  it("pins the exact local parent commit in a distinct child checkout", () => {
    const plan = planDelegatedCheckout(input);
    expect(plan.workspaceStrategy).toEqual({
      type: "worktree",
      baseRef: input.parentCommit,
      branch: plan.branch,
      startFromOrigin: false,
    });
    expect(plan.worktreePath).not.toBe(input.parentCheckoutPath);
    expect(plan.worktreePath).not.toBe(input.canonicalProjectRoot);
  });
  it("retains one child's path and gives another issuance a distinct branch and path", () => {
    const first = planDelegatedCheckout(input);
    expect(planDelegatedCheckout(input)).toEqual(first);
    const next = planDelegatedCheckout({
      ...input,
      commandId: CommandId.make("delegate:two"),
      childThreadId: ThreadId.make("child:two"),
    });
    expect(next.branch).not.toBe(first.branch);
    expect(next.worktreePath).not.toBe(first.worktreePath);
  });
  it("refuses a mutable parent ref and a child that aliases its parent", () => {
    expect(() => planDelegatedCheckout({ ...input, parentCommit: "main" })).toThrow(
      "resolved immutable local parent HEAD",
    );
    expect(() => planDelegatedCheckout({ ...input, childThreadId: input.parentThreadId })).toThrow(
      "distinct application thread",
    );
  });
  it("refuses a planned checkout that aliases the captured parent path", () => {
    const first = planDelegatedCheckout(input);
    expect(() =>
      planDelegatedCheckout({ ...input, parentCheckoutPath: first.worktreePath }),
    ).toThrow("physically distinct");
  });
});

describe("standalone committed-base checkout placement", () => {
  const birth = {
    kind: "fork" as const,
    birthCommandId: CommandId.make("fork:one"),
    targetThreadId: input.childThreadId,
    canonicalWorktreesDir: input.canonicalWorktreesDir,
    projectWorkspaceRoot: input.canonicalProjectRoot,
  };
  it("keeps idle placement deterministic and separates siblings and MCP roots", () => {
    const placement = planStandaloneBirthPlacement(birth);
    expect(planStandaloneBirthPlacement(birth)).toEqual(placement);
    expect(placement.branch).toMatch(/^t3code\/fork-/);
    expect(
      planStandaloneBirthPlacement({ ...birth, targetThreadId: ThreadId.make("sibling") })
        .worktreePath,
    ).not.toBe(placement.worktreePath);
    expect(planStandaloneBirthPlacement({ ...birth, kind: "mcp_create" }).branch).toMatch(
      /^t3code\/thread-/,
    );
  });
  it("pins firstsend without changing birth placement and refuses path/base/identity substitution", () => {
    const placement = planStandaloneBirthPlacement(birth);
    const capture = {
      ...input,
      ...birth,
      ...placement,
      projectWorkspaceRoot: birth.projectWorkspaceRoot,
    };
    const plan = planStandaloneCheckout(capture);
    expect(plan.workspaceStrategy).toEqual({
      type: "worktree",
      baseRef: input.parentCommit,
      branch: placement.branch,
      startFromOrigin: false,
    });
    expect(planStandaloneCheckout({ ...capture, parentCommit: "b".repeat(40) }).worktreePath).toBe(
      placement.worktreePath,
    );
    expect(() => planStandaloneCheckout({ ...capture, branch: "other" })).toThrow(
      "placement changed",
    );
    expect(() => planStandaloneCheckout({ ...capture, worktreePath: "/other" })).toThrow(
      "placement changed",
    );
    expect(() => planStandaloneCheckout({ ...capture, parentCommit: "HEAD" })).toThrow("immutable");
    expect(() =>
      planStandaloneCheckout({ ...capture, parentCheckoutPath: placement.worktreePath }),
    ).toThrow("distinct");
    expect(() =>
      planStandaloneCheckout({ ...capture, parentThreadId: input.childThreadId }),
    ).toThrow("distinct");
  });
});
