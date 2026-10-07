import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpInvocationContext, type McpCapability } from "../../../mcp/McpInvocationContext.ts";
import { CollectorFailure, DecisionSnapshotCollector } from "./collector.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../../../orchestration-v2/ProviderSessionManager.ts";
import {
  DecisionSnapshotNativeCountsLive,
  DecisionSnapshotNativeCounts,
  DecisionSnapshotToolkitHandlersLive,
  NativeCountReadError,
} from "./handlers.ts";
import { DecisionSnapshotToolkit } from "./tools.ts";

const harness = Effect.fnUntraced(function* (
  invalidOutput = false,
  backgroundUnknown = 0,
  nativePorts?: DecisionSnapshotNativeCounts["Service"],
) {
  let reads = 0;
  let registryReads = 0;
  let launches = 0;
  let envelope = "";
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      DecisionSnapshotNativeCounts,
      nativePorts ?? {
        readOperatingCounts: () => {
          reads++;
          return Effect.succeed({
            total: 9,
            operating: 3,
            foregroundWaitingApproval: 1,
            foregroundWaitingInput: 2,
            foregroundWaitingPlan: 1,
            backgroundOperating: 2,
            backgroundUnknown,
            backgroundSampledAt: "2026-10-02T00:00:00Z",
          });
        },
        readRegistryCounts: () => {
          registryReads++;
          return Effect.fail(new NativeCountReadError({ source: "workstreams" }));
        },
      },
    ),
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
  return { call, state: () => ({ reads, registryReads, launches, envelope }) };
});
it.effect("denies snapshot without its focused capability before collecting any source", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    const failure = yield* h.call(["orchestration"]).pipe(Effect.flip);
    expect(failure).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "decision-snapshot",
    });
    expect(h.state().reads).toBe(0);
    expect(h.state().registryReads).toBe(0);
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
    expect(h.state().registryReads).toBe(1);
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
      sources: {
        threads: {
          status: "partial",
          values: { operating: 3, total: 9 },
          reason: "native_background_coverage_incomplete",
        },
      },
    });
    expect(
      yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(h.state().envelope),
    ).toMatchObject({
      sources: {
        threads: { status: "partial", reason: "native_background_coverage_incomplete" },
      },
    });
  }),
);

it.effect("reports typed unavailable for both native readers when no ports are bound", () =>
  Effect.gen(function* () {
    const h = yield* harness(false, 0, {});
    expect(yield* h.call(["decision-snapshot"])).toMatchObject({
      coverage: "unavailable",
      sources: {
        threads: {
          status: "unavailable",
          values: {},
          reason: "native_operating_counts_unavailable",
        },
        workstreams: { status: "unavailable", values: {}, reason: "registry_counts_unavailable" },
        host: {
          status: "unavailable",
          reason: "runtime_unavailable",
          values: { start_fenced: true },
        },
      },
    });
    expect(h.state().reads).toBe(0);
  }),
);
it.effect.each([false, true])(
  "preserves registry completeness=%s without promoting incomplete counts",
  (complete) =>
    Effect.gen(function* () {
      const h = yield* harness(false, 0, {
        readRegistryCounts: () =>
          Effect.succeed({
            complete,
            counts: {
              context: { owner_id: "fixture-owner", registry_version: 7, server_generation: 1 },
              principal_id: "fixture-principal",
              observed_at: "2026-10-02T00:00:00Z",
              active: 1,
              total: 2,
              unknown_lifecycle: 0,
            },
            binding: {
              registryId: "fixture-registry",
              ownerId: "fixture-owner",
              principalId: "fixture-principal",
              registryVersion: 7,
              serverGeneration: 1,
              authorizationRevision: 1,
            },
          }),
      });
      expect(yield* h.call(["decision-snapshot"])).toMatchObject({
        coverage: "partial",
        sources: {
          workstreams: {
            status: complete ? "observed" : "partial",
            values: { active: 1, total: 2 },
            reason: complete ? null : "registry_counts_incomplete",
          },
        },
      });
    }),
);
it.effect("refuses registry counts from a changed native binding", () =>
  Effect.gen(function* () {
    const h = yield* harness(false, 0, {
      readRegistryCounts: () =>
        Effect.succeed({
          complete: true,
          counts: {
            context: { owner_id: "other-owner", registry_version: 7, server_generation: 1 },
            principal_id: "fixture-principal",
            observed_at: "2026-10-02T00:00:00Z",
            active: 1,
            total: 2,
            unknown_lifecycle: 0,
          },
          binding: {
            registryId: "fixture-registry",
            ownerId: "fixture-owner",
            principalId: "fixture-principal",
            registryVersion: 7,
            serverGeneration: 1,
            authorizationRevision: 1,
          },
        }),
    });
    expect(yield* h.call(["decision-snapshot"])).toMatchObject({
      coverage: "unavailable",
      sources: {
        workstreams: { status: "unavailable", values: {}, reason: "registry_binding_changed" },
      },
    });
  }),
);

it.effect(
  "binds the application census while keeping an empty external background scope partial",
  () =>
    Effect.gen(function* () {
      const nativePorts = yield* DecisionSnapshotNativeCounts.pipe(
        Effect.provide(
          DecisionSnapshotNativeCountsLive.pipe(
            Layer.provide(
              Layer.merge(
                ProjectionStore.layerMemory,
                Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
              ),
            ),
          ),
        ),
      );
      const h = yield* harness(false, 0, nativePorts);
      assertApplicationScope(yield* h.call(["decision-snapshot"]));
      expect(
        yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(h.state().envelope),
      ).toMatchObject({
        sources: {
          threads: {
            status: "partial",
            scope: { count_scope: "application", native_background_coverage: "unknown" },
            values: { total: 0 },
          },
        },
      });
    }),
);
function assertApplicationScope(result: unknown) {
  expect(result).toMatchObject({
    coverage: "partial",
    authority_effect: "none",
    sources: {
      threads: {
        status: "partial",
        reason: "native_background_coverage_incomplete",
        scope: {
          count_scope: "application",
          application_census_coverage: "complete",
          native_background_coverage: "unknown",
          snapshot_sequence: 0,
        },
        values: { total: 0, operating: 0, background_unknown: 0, foreground_unknown: 0 },
      },
    },
  });
}
