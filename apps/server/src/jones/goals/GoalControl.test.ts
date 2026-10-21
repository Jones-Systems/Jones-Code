import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  OrchestratorMcpFailure,
  EnvironmentId,
  ThreadId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderDriverKind,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderGoal,
  type ThreadGoalInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import type { ProviderGoalControlInput } from "@t3tools/provider-core/server/jones/runtimeTypes";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as GoalControl from "./GoalControl.ts";
import * as GoalStart from "./GoalStartDispatch.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";

const threadId = ThreadId.make("goal-thread");
const instanceId = ProviderInstanceId.make("codex");
const sessionId = ProviderSessionId.make("goal-session");
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("test"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test-client",
  thread: undefined,
  client: { sessionId: "client", label: "Test", access: "full-access" },
};
function fixture(
  options: {
    active?: boolean;
    driver?: string;
    unknown?: boolean;
    failedCapture?: boolean;
    caller?: "self" | "other";
  } = {},
) {
  let goal: OrchestrationV2ProviderGoal | null = {
    objective: "Finish everything",
    status: "active",
    tokensUsed: 42,
  };
  let provider = {
    id: ProviderThreadId.make("provider"),
    driver: ProviderDriverKind.make(options.driver ?? "codex"),
    providerInstanceId: instanceId,
    providerSessionId: sessionId,
    nativeThreadRef: { driver: options.driver ?? "codex", nativeId: "native", strength: "strong" },
    goal,
  } as OrchestrationV2ProviderThread;
  const callerId = options.caller === "other" ? ThreadId.make("caller-thread") : threadId;
  const callerSessionId =
    options.caller === "other" ? ProviderSessionId.make("caller-runtime") : sessionId;
  const credentialId = "mcp-credential";
  const invocation: McpInvocationScope =
    options.caller === undefined
      ? scope
      : {
          ...scope,
          capabilities: new Set(scope.capabilities),
          client: undefined,
          thread: {
            threadId: callerId,
            providerInstanceId: instanceId,
            providerSessionId: credentialId,
          },
        };
  let callerProvider: OrchestrationV2ProviderThread = {
    ...provider,
    providerSessionId: callerSessionId,
  };
  let attached = true;
  let callerLive = true;
  const callerMode = "full-access";
  let runtimeMode = "full-access";
  let mutationCalls = 0;
  let unknown = options.unknown ?? false;
  let starts = 0;
  let runtime = {
    instanceId,
    providerSessionId: sessionId,
    controlGoal: (input: ProviderGoalControlInput) =>
      Effect.gen(function* () {
        if (!(yield* input.revalidate))
          return { outcome: "stale" as const, goal: null, reason: "stale" };
        if (input.action !== "read") {
          mutationCalls++;
          if (unknown) return { outcome: "unknown" as const, goal, reason: "readback_failed" };
          goal =
            input.action === "clear"
              ? null
              : {
                  ...goal!,
                  ...(input.action === "pause"
                    ? { status: "paused" as const }
                    : { objective: input.objective! }),
                };
        }
        return {
          outcome: input.action === "read" ? ("observed" as const) : ("applied" as const),
          goal,
          reason: "readback",
        };
      }),
  } as ProviderAdapterV2SessionRuntime;
  const thread = () => ({
    id: threadId,
    projectId: "project",
    activeProviderThreadId: provider.id,
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "test" },
    runtimeMode,
    interactionMode: "default",
    activeRunId: null,
    pendingBackgroundTasks: callerLive
      ? [{ kind: "subagent", providerInstanceId: instanceId }]
      : [],
    archivedAt: null,
    deletedAt: null,
  });
  const records = () => ({
    thread: thread(),
    providerThreads: [provider],
    runs: options.active ? [{ status: "running" }] : [],
  });
  const dependencies = Layer.mergeAll(
    NodeServices.layer,
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.sync(
          () =>
            (id === threadId
              ? thread()
              : {
                  ...thread(),
                  id: callerId,
                  runtimeMode: callerMode,
                }) as never,
        ),
      getThreadRecords: () =>
        Effect.sync(
          () =>
            ({
              thread: thread(),
              providerThreads: [options.caller === "self" ? provider : callerProvider],
            }) as never,
        ),
      getProjectThreadRecords: () => Effect.sync(() => records() as never),
    }),
    Layer.mock(Sessions.ProviderSessionManagerV2)({
      get: () => Effect.sync(() => Option.some(runtime)),
      isMcpCallerAttached: (input) =>
        Effect.sync(
          () =>
            attached &&
            input.threadId === callerId &&
            input.providerSessionId === callerSessionId &&
            input.providerInstanceId === instanceId &&
            input.mcpCredentialId === credentialId,
        ),
    }),
    Layer.mock(GoalStart.GoalStartDispatch)({
      capture: () =>
        options.failedCapture
          ? Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic read failure",
              }),
            )
          : Effect.succeed({ requireIdle: true } as never),
      start: () =>
        Effect.sync(() => {
          starts++;
          return {
            outcome: "accepted" as const,
            reason: "durable_start_accepted_native_state_pending",
          };
        }),
    }),
  );
  return {
    layer: GoalControl.layer.pipe(Layer.provide(dependencies)),
    invocation,
    invalidateCaller: (change: string) => {
      if (change === "detached") attached = false;
      else if (change === "inactive") callerLive = false;
      else if (change === "permission") (invocation.capabilities as Set<string>).clear();
      else {
        const current = options.caller === "self" ? provider : callerProvider;
        const changed =
          change === "session"
            ? { ...current, providerSessionId: ProviderSessionId.make("changed") }
            : change === "missing-session"
              ? { ...current, providerSessionId: null }
              : change === "missing-provider"
                ? { ...current, id: ProviderThreadId.make("inactive-provider") }
                : { ...current, providerInstanceId: ProviderInstanceId.make("changed") };
        if (options.caller === "self") provider = changed;
        else callerProvider = changed;
      }
    },
    counts: () => ({ mutations: mutationCalls, starts }),
    reconcile: () => {
      goal = null;
      unknown = false;
    },
    restart: () => {
      runtime = { ...runtime };
    },
    changeBinding: () => {
      provider = {
        ...provider,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "other",
          strength: "strong",
        },
      };
    },
    mode: (mode: string) => {
      runtimeMode = mode;
    },
  };
}
const read = (service: GoalControl.GoalControl["Service"]) =>
  service.execute(scope, { threadId, action: "read" });
const mutate = (
  service: GoalControl.GoalControl["Service"],
  token: string,
  action: ThreadGoalInput["action"],
  key = "request",
  objective?: string,
) =>
  service.execute(scope, {
    threadId,
    action,
    controlToken: token,
    clientRequestId: key,
    ...(objective === undefined ? {} : { objective }),
  });

describe("agent goal controls", () => {
  for (const caller of ["self", "other"] as const) {
    it.effect.each(["pause", "replace", "clear", "resume"] as const)(
      `${caller} thread caller controls %s with a distinct MCP credential and replays once`,
      (action) => {
        const f = fixture({ caller, active: action !== "resume" });
        return Effect.gen(function* () {
          const service = yield* GoalControl.GoalControl;
          const state = yield* service.execute(f.invocation, { threadId, action: "read" });
          expect(state.outcome).toBe("observed");
          const input = {
            threadId,
            action,
            controlToken: state.controlToken!,
            clientRequestId: "thread-request",
            ...(action === "replace" ? { objective: "Updated objective" } : {}),
          };
          const first = yield* service.execute(f.invocation, input);
          expect(first.outcome).toBe(action === "resume" ? "accepted" : "applied");
          if (action === "pause") expect(first.goal?.status).toBe("paused");
          if (action === "replace") expect(first.goal?.objective).toBe("Updated objective");
          if (action === "clear") expect(first.goal).toBeNull();
          expect(yield* service.execute(f.invocation, input)).toEqual({ ...first, replayed: true });
          expect(f.counts()).toEqual({
            mutations: action === "resume" ? 0 : 1,
            starts: action === "resume" ? 1 : 0,
          });
        }).pipe(Effect.provide(f.layer));
      },
    );
    it.effect.each([
      "credential",
      "detached",
      "session",
      "missing-session",
      "missing-provider",
      "instance",
      "inactive",
      "permission",
      "native",
    ])(`${caller} thread caller refuses changed %s before side effects`, (change) => {
      const f = fixture({ caller });
      return Effect.gen(function* () {
        const service = yield* GoalControl.GoalControl;
        const state = yield* service.execute(f.invocation, { threadId, action: "read" });
        if (change === "native") f.changeBinding();
        else if (change !== "credential") f.invalidateCaller(change);
        const invocation =
          change === "credential"
            ? {
                ...f.invocation,
                thread: { ...f.invocation.thread!, providerSessionId: "wrong-credential" },
              }
            : f.invocation;
        for (const action of ["pause", "resume"] as const) {
          const result = yield* service
            .execute(invocation, {
              threadId,
              action,
              controlToken: state.controlToken!,
              clientRequestId: action,
            })
            .pipe(Effect.result);
          if (change === "inactive" || change === "permission") {
            expect(result._tag).toBe("Failure");
            if (result._tag === "Failure")
              expect(result.failure.code).toBe(
                change === "inactive" ? "parent_not_active" : "capability_denied",
              );
          } else {
            expect(result._tag).toBe("Success");
            if (result._tag === "Success")
              expect(["stale", "unavailable", "unsupported"]).toContain(result.success.outcome);
          }
        }
        expect(f.counts()).toEqual({ mutations: 0, starts: 0 });
      }).pipe(Effect.provide(f.layer));
    });
  }

  it.effect("pauses an active goal directly and replays the same receipt", () => {
    const f = fixture({ active: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      const first = yield* mutate(service, state.controlToken!, "pause");
      expect(first).toMatchObject({ outcome: "applied", goal: { status: "paused" } });
      expect(yield* mutate(service, state.controlToken!, "pause")).toEqual({
        ...first,
        replayed: true,
      });
      expect(f.counts()).toEqual({ mutations: 1, starts: 0 });
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("idle resume records exactly one start across retries", () => {
    const action = "resume";
    const f = fixture();
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      const objective = undefined;
      expect(
        yield* mutate(service, state.controlToken!, action, "request", objective),
      ).toMatchObject({ outcome: "accepted" });
      expect(
        yield* mutate(service, state.controlToken!, action, "request", objective),
      ).toMatchObject({ outcome: "accepted", replayed: true });
      expect(f.counts()).toEqual({ mutations: 0, starts: 1 });
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("failed pre-dispatch observation does not poison later controls", () => {
    const f = fixture({ failedCapture: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      expect(yield* mutate(service, state.controlToken!, "resume")).toMatchObject({
        outcome: "unavailable",
        reason: "start_guard_unavailable",
      });
      expect(yield* mutate(service, state.controlToken!, "pause", "pause-request")).toMatchObject({
        outcome: "applied",
      });
      expect(f.counts()).toEqual({ mutations: 1, starts: 0 });
    }).pipe(Effect.provide(f.layer));
  });
  it.effect.each([false, true])(
    "replace preserves status and usage with active=%s and starts no run",
    (active) => {
      const f = fixture({ active });
      return Effect.gen(function* () {
        const service = yield* GoalControl.GoalControl;
        const state = yield* read(service);
        expect(
          yield* mutate(service, state.controlToken!, "replace", "request", "Full objective"),
        ).toMatchObject({
          outcome: "applied",
          goal: { objective: "Full objective", status: "active", tokensUsed: 42 },
        });
        expect(f.counts()).toEqual({ mutations: 1, starts: 0 });
      }).pipe(Effect.provide(f.layer));
    },
  );
  it.effect("unknown acknowledgement blocks both repeated and new mutation IDs", () => {
    const f = fixture({ active: true, unknown: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      expect(yield* mutate(service, state.controlToken!, "clear")).toMatchObject({
        outcome: "unknown",
      });
      expect(yield* mutate(service, state.controlToken!, "clear")).toMatchObject({
        outcome: "unknown",
        replayed: true,
      });
      expect(yield* mutate(service, state.controlToken!, "clear", "new-id")).toMatchObject({
        outcome: "unknown",
        reason: "prior_mutation_unresolved",
      });
      expect(f.counts().mutations).toBe(1);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("another authorized caller cannot bypass an unresolved native mutation", () => {
    const f = fixture({ active: true, unknown: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      yield* mutate(service, state.controlToken!, "pause");
      const other = { ...scope, requestNamespace: "another-client" };
      const observed = yield* service.execute(other, { threadId, action: "read" });
      expect(
        yield* service.execute(other, {
          threadId,
          action: "clear",
          controlToken: observed.controlToken!,
          clientRequestId: "new",
        }),
      ).toMatchObject({ outcome: "unknown", reason: "prior_mutation_unresolved" });
      expect(f.counts().mutations).toBe(1);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("fresh read reconciles an uncertain clear without repeating it", () => {
    const f = fixture({ active: true, unknown: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      yield* mutate(service, state.controlToken!, "clear");
      f.reconcile();
      expect(yield* read(service)).toMatchObject({ outcome: "observed", goal: null });
      expect(yield* mutate(service, state.controlToken!, "clear")).toMatchObject({
        outcome: "applied",
        reason: "reconciled_by_native_read",
        replayed: true,
      });
      expect(f.counts().mutations).toBe(1);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("reusing a key for a different mutation is rejected", () => {
    const f = fixture({ active: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const state = yield* read(service);
      yield* mutate(service, state.controlToken!, "pause");
      expect((yield* mutate(service, state.controlToken!, "clear").pipe(Effect.flip)).code).toBe(
        "invalid_request",
      );
      expect(f.counts().mutations).toBe(1);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect("a target permission increase is refused before native mutation", () => {
    const f = fixture({ active: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      f.mode("approval-required");
      const limited = {
        ...scope,
        client: { ...scope.client!, access: "approval-required" as const },
      };
      const state = yield* service.execute(limited, { threadId, action: "read" });
      f.mode("full-access");
      const refused = yield* service
        .execute(limited, {
          threadId,
          action: "pause",
          controlToken: state.controlToken!,
          clientRequestId: "key",
        })
        .pipe(Effect.flip);
      expect(refused.code).toBe("runtime_mode_escalation_denied");
      expect(f.counts().mutations).toBe(0);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect.each(["restart", "changeBinding"] as const)(
    "%s invalidates old tokens without a native mutation",
    (change) => {
      const f = fixture({ active: true });
      return Effect.gen(function* () {
        const service = yield* GoalControl.GoalControl;
        const state = yield* read(service);
        f[change]();
        expect(yield* mutate(service, state.controlToken!, "pause")).toMatchObject({
          outcome: "stale",
        });
        expect(f.counts().mutations).toBe(0);
      }).pipe(Effect.provide(f.layer));
    },
  );
  it.effect("read-only access can read but cannot mutate", () => {
    const f = fixture({ active: true });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      const clientScope = { ...scope, client: { ...scope.client!, access: "read-only" as const } };
      const state = yield* service.execute(clientScope, { threadId, action: "read" });
      const refused = yield* service
        .execute(clientScope, {
          threadId,
          action: "clear",
          controlToken: state.controlToken!,
          clientRequestId: "key",
        })
        .pipe(Effect.flip);
      expect(refused.code).toBe("capability_denied");
      expect(f.counts().mutations).toBe(0);
    }).pipe(Effect.provide(f.layer));
  });
  it.effect.each(["claudeAgent", "opencode"])("%s never simulates native controls", (driver) => {
    const f = fixture({ driver });
    return Effect.gen(function* () {
      const service = yield* GoalControl.GoalControl;
      expect(yield* read(service)).toMatchObject(
        driver === "claudeAgent"
          ? { outcome: "observed", source: "claude_transcript" }
          : { outcome: "unsupported", source: "none" },
      );
      expect(yield* mutate(service, "token", "clear")).toMatchObject({ outcome: "unsupported" });
      expect(f.counts()).toEqual({ mutations: 0, starts: 0 });
    }).pipe(Effect.provide(f.layer));
  });
});
