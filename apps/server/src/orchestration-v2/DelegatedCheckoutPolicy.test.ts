import { describe, expect, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import { planDelegatedCheckout } from "./DelegatedCheckoutPolicy.ts";

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
