import { PlanId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";
import { dispatchPlanFollowUp, planFollowUpSource } from "./planFollowUp";
describe("shared genuine plan follow-up lifecycle", () => {
  it("never dispatches a turn when settings persistence is rejected", async () => {
    let started = false;
    const result = await dispatchPlanFollowUp({
      persistSettings: async () => AsyncResult.failure(Cause.fail("denied")),
      onSettingsPersisted: () => {
        throw new Error("must not run");
      },
      startTurn: async () => {
        started = true;
        return AsyncResult.success(undefined);
      },
    });
    expect(result._tag).toBe("Failure");
    expect(started).toBe(false);
  });
  it("persists settings before turn dispatch and binds only implementation to the exact source plan", async () => {
    const order: string[] = [];
    const result = await dispatchPlanFollowUp({
      persistSettings: async () => {
        order.push("settings");
        return AsyncResult.success(undefined);
      },
      onSettingsPersisted: () => {
        order.push("committed");
      },
      startTurn: async () => {
        order.push("turn");
        return AsyncResult.success("accepted");
      },
    });
    expect(result).toMatchObject({ _tag: "Success", value: "accepted" });
    expect(order).toEqual(["settings", "committed", "turn"]);
    expect(
      planFollowUpSource("default", ThreadId.make("thread"), PlanId.make("current-plan")),
    ).toEqual({ sourceProposedPlan: { threadId: "thread", planId: "current-plan" } });
    expect(
      planFollowUpSource("plan", ThreadId.make("thread"), PlanId.make("current-plan")),
    ).toEqual({});
  });
});
