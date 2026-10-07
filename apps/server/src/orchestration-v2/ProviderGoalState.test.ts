import { describe, expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { makeProviderGoalState } from "./ProviderGoalState.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const threadId = ThreadId.make("queue-goal-thread");
const instanceId = ProviderInstanceId.make("queue-goal-codex");
const providerThreadId = ProviderThreadId.make("queue-goal-provider-thread");
const sessionId = ProviderSessionId.make("queue-goal-session");

describe("existing V2 provider goal observation", () => {
  const fixture = (condition: string) => {
    let reads = 0;
    let calls = 0;
    const provider = {
      id: providerThreadId,
      providerInstanceId: instanceId,
      providerSessionId: sessionId,
      nativeThreadRef:
        condition === "cursor_missing"
          ? null
          : { driver: "codex", nativeId: "native-thread", strength: "strong" },
    } as OrchestrationV2ProviderThread;
    const runtime = {
      instanceId: condition === "runtime_mismatch" ? ProviderInstanceId.make("other") : instanceId,
      ...(condition === "unsupported"
        ? {}
        : {
            readGoalState: () =>
              Effect.sync(() => {
                calls++;
                return {
                  nativeThreadId:
                    condition === "malformed"
                      ? null
                      : condition === "result_mismatch"
                        ? "other"
                        : "native-thread",
                  state: "inactive" as const,
                  reasonCode: "goal_null" as const,
                  objective: "private goal text",
                };
              }),
          }),
    } as unknown as ProviderAdapterV2SessionRuntime;
    const projections = Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadShell: () => Effect.succeed(condition === "missing" ? null : ({} as never)),
      getThreadRecords: () =>
        Effect.sync(() => {
          reads++;
          return {
            thread: {
              providerInstanceId:
                condition === "instance_mismatch" || (condition === "changed" && reads > 1)
                  ? ProviderInstanceId.make("other")
                  : instanceId,
              activeProviderThreadId: providerThreadId,
            },
            providerThreads: condition === "no_session" ? [] : [provider],
          } as never;
        }),
    });
    const sessions = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      get: () =>
        Effect.sync(() => (condition === "stopped" ? Option.none() : Option.some(runtime))),
    });
    const program = Effect.gen(function* () {
      const read = yield* makeProviderGoalState();
      return yield* read({ threadId, expectedInstanceId: instanceId });
    }).pipe(
      Effect.provide(
        condition === "missing_service" ? projections : Layer.merge(projections, sessions),
      ),
    );
    return { program, calls: () => calls };
  };
  it.effect.each(
    (
      [
        ["missing", "no_session"],
        ["no_session", "no_session"],
        ["instance_mismatch", "instance_mismatch"],
        ["runtime_mismatch", "instance_mismatch"],
        ["cursor_missing", "native_cursor_missing"],
        ["stopped", "session_stopped"],
        ["unsupported", "unsupported"],
        ["missing_service", "unsupported"],
      ] as const
    ).map(([condition, reason]) => ({
      condition,
      reason,
      name: `holds ${condition} without starting or recovering a provider`,
    })),
  )("$name", ({ condition, reason }) =>
    Effect.gen(function* () {
      const f = fixture(condition);
      const result = yield* f.program;
      expect(result.state).toBe("unknown");
      expect(result.reasonCode).toBe(reason);
      expect(f.calls()).toBe(0);
    }),
  );
  it.effect.each(
    (
      [
        ["changed", "context_changed"],
        ["result_mismatch", "context_changed"],
        ["malformed", "malformed"],
      ] as const
    ).map(([condition, reason]) => ({
      condition,
      reason,
      name: `invalidates ${condition} after the RPC`,
    })),
  )("$name", ({ condition, reason }) =>
    Effect.gen(function* () {
      const f = fixture(condition);
      const result = yield* f.program;
      expect(result.state).toBe("unknown");
      expect(result.reasonCode).toBe(reason);
      expect(f.calls()).toBe(1);
    }),
  );
  it.effect("returns bounded public state with no goal objective", () =>
    Effect.gen(function* () {
      const f = fixture("valid");
      const result = yield* f.program;
      expect(result).toMatchObject({
        schema: "t3.provider-goal-state/v1",
        threadId,
        providerInstanceId: instanceId,
        nativeThreadId: "native-thread",
        state: "inactive",
        reasonCode: "goal_null",
      });
      const serialized = yield* encodeJson(result);
      expect(serialized).not.toContain("private goal text");
      expect(f.calls()).toBe(1);
    }),
  );
});
