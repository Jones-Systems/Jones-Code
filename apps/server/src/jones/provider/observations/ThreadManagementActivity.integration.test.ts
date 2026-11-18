import { expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import type { ProviderRuntimeObservation } from "./ProviderThreadRuntimeObservation.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import { unusedCorePorts } from "../../testing/UnusedCorePorts.testkit.ts";
import * as ProviderSessionManager from "../../../orchestration-v2/ProviderSessionManager.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import * as ServerSettings from "../../../serverSettings.ts";

it.effect.each(["starting", "running"] as const)(
  "does not restore working for a nonresident %s runtime at the service seam",
  (status) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("synthetic:fleet-activity");
      let pendingRuntimeRequest: OrchestrationV2ThreadShell["pendingRuntimeRequest"] = null;
      let native: ProviderRuntimeObservation = {
        status: "unknown",
        reason: "runtime_not_resident",
      };
      const dependencies = Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          ...unusedCorePorts(),
          getThreadShell: () =>
            Effect.succeed({
              id: threadId,
              activeProviderThreadId: ProviderThreadId.make("synthetic:provider"),
              activityRunStatus: status,
              archivedAt: null,
              deletedAt: null,
              interactionMode: "default",
              pendingRuntimeRequest,
              hasActionableProposedPlan: false,
              latestRunCompletedAt: null,
            } as OrchestrationV2ThreadShell),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          observeThreadActivity: () => Effect.succeed(native),
        }),
        ServerSettings.layerTest(),
        CommandReceiptStore.layer.pipe(Layer.provide(Sqlite.layerMemory)),
      );
      yield* Effect.gen(function* () {
        const service = yield* ThreadManagementService.ThreadManagementService;
        const observe = service.observeThreadActivity;
        if (observe === undefined) return yield* Effect.die("Missing activity read method");
        expect(yield* observe(threadId)).toEqual({
          foreground: null,
          background: null,
          backgroundStatus: "known",
          reason: "runtime_not_resident",
        });
        for (const [kind, foreground] of [
          ["command", "waiting_approval"],
          ["user_input", "waiting_input"],
        ] as const) {
          pendingRuntimeRequest = { kind } as NonNullable<
            OrchestrationV2ThreadShell["pendingRuntimeRequest"]
          >;
          expect((yield* observe(threadId)).foreground).toBe(foreground);
        }
        pendingRuntimeRequest = null;
        native = { status: "unknown", reason: "native_activity_unsupported" };
        expect((yield* observe(threadId)).foreground).toBe("working");
        native = {
          status: "working",
          backgroundCoverage: "complete",
          observedAt: DateTime.formatIso(yield* DateTime.now),
          binding: {
            threadId,
            providerThreadId: ProviderThreadId.make("synthetic:provider"),
            providerSessionId: ProviderSessionId.make("synthetic:session"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            driver: ProviderDriverKind.make("codex"),
            nativeThreadId: "synthetic-native",
            runtimeGeneration: "synthetic-generation",
          },
        };
        expect((yield* observe(threadId)).foreground).toBe("working");
      }).pipe(Effect.provide(ThreadManagementService.layer.pipe(Layer.provide(dependencies))));
    }),
);
