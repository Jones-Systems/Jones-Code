import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type {
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderGoal,
} from "@t3tools/contracts";
import type * as CodexClient from "effect-codex-app-server/client";
import { controlCodexGoal } from "./CodexGoalControl.ts";

const providerThread = {
  nativeThreadRef: { nativeId: "native", driver: "codex" },
} as OrchestrationV2ProviderThread;
const nativeGoal = (status = "active", objective = "Original") => ({
  threadId: "native",
  status,
  objective,
  tokensUsed: 42,
  tokenBudget: 1000,
  timeUsedSeconds: 17,
  createdAt: 10,
  updatedAt: 20,
});
function fixture(
  options: {
    status?: string;
    lostAck?: boolean;
    failedReadback?: boolean;
    stale?: boolean;
    mismatch?: boolean;
  } = {},
) {
  let goal: ReturnType<typeof nativeGoal> | null = nativeGoal(options.status);
  let mutated = false;
  let observed: OrchestrationV2ProviderGoal | null = null;
  const requests: Array<{ method: string; params: unknown }> = [];
  const request = (method: string, params: { status?: string; objective?: string }) =>
    Effect.suspend(() => {
      requests.push({ method, params });
      if (method === "thread/goal/get") {
        if (mutated && options.failedReadback) return Effect.fail("unavailable");
        return Effect.succeed({
          goal: options.mismatch && goal ? { ...goal, threadId: "foreign" } : goal,
        });
      }
      mutated = true;
      if (method === "thread/goal/clear") goal = null;
      else goal = { ...goal!, ...params };
      return options.lostAck ? Effect.fail("ack lost") : Effect.succeed({ goal });
    });
  return {
    run: (action: "read" | "pause" | "clear" | "replace", objective?: string) =>
      controlCodexGoal(
        {
          providerThread,
          action,
          ...(objective === undefined ? {} : { objective }),
          revalidate: Effect.sync(() => !(options.stale && mutated)),
        },
        {
          client: { request } as unknown as Pick<
            CodexClient.CodexAppServerClient["Service"],
            "request"
          >,
          current: Effect.succeed(true),
          beforeMutation: () => Effect.void,
          observe: (value) =>
            Effect.sync(() => {
              observed = value;
            }),
        },
      ),
    requests,
    observed: () => observed,
  };
}

describe("native goal control", () => {
  it.effect.each(["active", "paused", "complete", "blocked", "budgetLimited", "usageLimited"])(
    "replaces a %s objective in place without resetting usage or starting a turn",
    (status) =>
      Effect.gen(function* () {
        const f = fixture({ status });
        expect(yield* f.run("replace", "Full accepted objective")).toMatchObject({
          outcome: "applied",
          goal: {
            objective: "Full accepted objective",
            status:
              status === "budgetLimited"
                ? "budget_limited"
                : status === "usageLimited"
                  ? "usage_limited"
                  : status,
            tokensUsed: 42,
            timeUsedSeconds: 17,
          },
        });
        expect(f.requests.map((r) => r.method)).toEqual([
          "thread/goal/get",
          "thread/goal/set",
          "thread/goal/get",
        ]);
        expect(f.requests[1]?.params).toEqual({
          threadId: "native",
          objective: "Full accepted objective",
        });
      }),
  );
  it.effect("clear reads back null without interrupting", () =>
    Effect.gen(function* () {
      const f = fixture();
      expect(yield* f.run("clear")).toMatchObject({ outcome: "applied", goal: null });
      expect(f.requests.map((r) => r.method)).toEqual([
        "thread/goal/get",
        "thread/goal/clear",
        "thread/goal/get",
      ]);
    }),
  );
  it.effect("reconciles a lost mutation acknowledgement through readback", () =>
    Effect.gen(function* () {
      const f = fixture({ lostAck: true });
      expect(yield* f.run("pause")).toMatchObject({
        outcome: "applied",
        reason: "reconciled_after_lost_acknowledgement",
        goal: { status: "paused" },
      });
      expect(f.requests.filter((r) => r.method === "thread/goal/set")).toHaveLength(1);
    }),
  );
  it.effect("failed readback remains unknown", () =>
    Effect.gen(function* () {
      const f = fixture({ failedReadback: true });
      expect(yield* f.run("pause")).toMatchObject({
        outcome: "unknown",
        reason: "readback_failed",
      });
    }),
  );
  it.effect("a binding changed during mutation never reports applied", () =>
    Effect.gen(function* () {
      const f = fixture({ stale: true });
      expect(yield* f.run("pause")).toMatchObject({
        outcome: "unknown",
        reason: "binding_changed_after_mutation",
      });
    }),
  );
  it.effect("a foreign native goal is refused before mutation", () =>
    Effect.gen(function* () {
      const f = fixture({ mismatch: true });
      expect(yield* f.run("clear")).toMatchObject({ outcome: "stale" });
      expect(f.requests).toHaveLength(1);
    }),
  );
});
