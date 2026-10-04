import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpInvocationContext, type McpCapability } from "../../McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { CollectorFailure, DecisionSnapshotCollector } from "./collector.ts";
import { DecisionSnapshotToolkitHandlersLive } from "./handlers.ts";
import { DecisionSnapshotToolkit } from "./tools.ts";

const harness = Effect.fnUntraced(function* (invalidOutput = false, backgroundUnknown = 0) {
  let reads = 0;
  let launches = 0;
  let envelope = "";
  const dependencies = Layer.mergeAll(
    Layer.mock(ThreadManagementService)({
      getOperatingCounts: () => {
        reads++;
        return Effect.succeed({
          total: 9,
          operating: 3,
          foregroundWaitingApproval: 1,
          foregroundWaitingInput: 2,
          foregroundWaitingPlan: 1,
          backgroundOperating: 2,
          backgroundUnknown,
          snapshotSequence: 42,
          backgroundSampledAt: "2026-10-02T00:00:00Z",
          observedAt: "2026-10-02T00:00:01Z",
        });
      },
    }),
    Layer.mock(WorkstreamGateway)({
      purgeAuthorization: () => {},
      readRegistryCounts: () =>
        Effect.fail(
          new WorkstreamGatewayError({
            reason: "contract-mismatch",
            detail: "fixture unsupported",
          }),
        ),
    }),
    Layer.succeed(DecisionSnapshotCollector, {
      collect: (_purpose, input) =>
        Effect.gen(function* () {
          launches++;
          envelope = yield* input;
          return yield* invalidOutput
            ? Effect.succeed("{}")
            : Effect.fail(new CollectorFailure("runtime_unavailable"));
        }),
    }),
  );
  const toolkit = yield* DecisionSnapshotToolkit.pipe(
    Effect.provide(DecisionSnapshotToolkitHandlersLive.pipe(Layer.provide(dependencies))),
  );
  const call = (capabilities: McpCapability[]) =>
    toolkit.handle("decision_snapshot", {}).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map((results) => results.at(-1)!.result),
      Effect.provideService(McpInvocationContext, {
        environmentId: EnvironmentId.make("fixture-environment"),
        threadId: ThreadId.make("fixture-thread"),
        providerSessionId: "fixture-session",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(capabilities),
        issuedAt: 1,
      }),
      Effect.provide(dependencies),
    );
  return { call, state: () => ({ reads, launches, envelope }) };
});
it.effect("denies snapshot without its focused capability before collecting any source", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    const failure = yield* h.call(["organization"]).pipe(Effect.flip);
    expect(failure).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "decision-snapshot",
    });
    expect(h.state().reads).toBe(0);
    expect(h.state().launches).toBe(0);
  }),
);
it.effect("retains native thread facts when registry and bound runtime are unavailable", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    const result = yield* h.call(["decision-snapshot"]);
    expect(result).toMatchObject({
      coverage: "partial",
      authority_effect: "none",
      purpose: "display",
      sources: {
        threads: {
          values: { operating: 3, total: 9 },
          scope: { environment_id: "fixture-environment", include_archived: false },
        },
        workstreams: { status: "unavailable" },
        host: { status: "unavailable", reason: "runtime_unavailable" },
      },
    });
    expect(
      yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(h.state().envelope),
    ).toMatchObject({ schema: "codex.decision-snapshot-native/v1", provenance: "native_observed" });
    expect(h.state().reads).toBe(1);
    expect(h.state().launches).toBe(1);
  }),
);

it.effect("rejects invalid composer output while preserving independent native observations", () =>
  Effect.gen(function* () {
    const h = yield* harness(true);
    expect(yield* h.call(["decision-snapshot"])).toMatchObject({
      coverage: "partial",
      sources: {
        threads: { values: { operating: 3, total: 9 } },
        host: {
          reason: "invalid_collector_output",
          values: { agent_batch_ceiling: 0, start_fenced: true },
        },
      },
    });
  }),
);


it.effect("preserves incomplete V2 native background coverage as partial counts", () =>
  Effect.gen(function* () {
    const h = yield* harness(false, 2);
    expect(yield* h.call(["decision-snapshot"])).toMatchObject({
      coverage: "partial",
      sources: { threads: { status: "partial", values: { operating: 3, total: 9 }, reason: "native_background_coverage_incomplete" } },
    });
    expect(JSON.parse(h.state().envelope).sources.threads).toMatchObject({
      status: "partial", reason: "native_background_coverage_incomplete",
    });
  }),
);
