// @effect-diagnostics-next-line nodeBuiltinImport:off -- The integration test runs only the immutable synthetic collector fixture.
import * as NodeChildProcess from "node:child_process";
// The fixture path is resolved without touching application state.
import * as NodeURL from "node:url";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
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
import { DecisionSnapshot, DecisionSnapshotToolkit } from "./tools.ts";

const decodeUnknownJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeSnapshotJson = Schema.encodeEffect(Schema.fromJsonString(DecisionSnapshot));
const decodeSnapshotJson = Schema.decodeEffect(Schema.fromJsonString(DecisionSnapshot));
const harness = Effect.fnUntraced(function* (
  invalidOutput = false,
  backgroundUnknown = 0,
  nativePorts?: DecisionSnapshotNativeCounts["Service"],
  collectorPort?: DecisionSnapshotCollector["Service"],
) {
  let reads = 0;
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
      },
    ),
    Layer.succeed(
      DecisionSnapshotCollector,
      collectorPort ?? {
        collect: (_purpose, input) =>
          Effect.gen(function* () {
            launches++;
            envelope = yield* input;
            return yield* invalidOutput
              ? Effect.succeed("{}")
              : Effect.fail(new CollectorFailure("runtime_unavailable"));
          }),
      },
    ),
  );
  const toolkit = yield* DecisionSnapshotToolkit.pipe(
    Effect.provide(DecisionSnapshotToolkitHandlersLive.pipe(Layer.provide(dependencies))),
  );
  const call = (capabilities: McpCapability[]) =>
    toolkit.handle("decision_snapshot", {}).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.flatMap((results) => {
        const result = results.at(-1)!.result;
        return "sources" in result ? Effect.succeed(result) : Effect.fail(result);
      }),
      Effect.provideService(McpInvocationContext, {
        environmentId: EnvironmentId.make("fixture-environment"),
        requestNamespace: "fixture-session",
        thread: {
          threadId: ThreadId.make("fixture-thread"),
          providerSessionId: "fixture-session",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
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
    const failure = yield* h.call(["orchestration"]).pipe(Effect.flip);
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
    expect(yield* decodeUnknownJson(h.state().envelope)).toMatchObject({
      schema: "codex.decision-snapshot-native/v1",
      provenance: "native_observed",
    });
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
      sources: {
        threads: {
          status: "partial",
          values: { operating: 3, total: 9 },
          reason: "native_background_coverage_incomplete",
        },
      },
    });
    expect(yield* decodeUnknownJson(h.state().envelope)).toMatchObject({
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
it.effect("binds the application census without any Workstream registry runtime", () =>
  Effect.gen(function* () {
    const nativePorts = yield* DecisionSnapshotNativeCounts.pipe(
      Effect.provide(
        DecisionSnapshotNativeCountsLive.pipe(
          Layer.provide(Layer.mergeAll(
            ProjectionStore.layerMemory,
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
          )),
        ),
      ),
    );
    const result = yield* (yield* harness(false, 0, nativePorts)).call(["decision-snapshot"]);
    expect(result).toMatchObject({
      coverage: "partial",
      sources: {
        threads: {
          status: "partial",
          scope: {
            count_scope: "application",
            application_census_coverage: "complete",
            native_background_coverage: "unknown",
          },
          values: { total: 0, operating: 0 },
        },
        workstreams: { status: "unavailable", reason: "registry_counts_unavailable" },
      },
    });
  }),
);

it.effect("keeps other collector sources and cannot promote unsupported Workstream counts", () =>
  Effect.gen(function* () {
    const old = yield* (yield* harness()).call(["decision-snapshot"]);
    const host = { ...old.sources.host, status: "observed" as const, reason: null, values: { cpu_used_percent: 17 } };
    const h = yield* harness(false, 0, {}, {
      collect: () => encodeSnapshotJson({
        ...old,
        coverage: "complete",
        sources: {
          ...old.sources,
          host,
          workstreams: { ...old.sources.workstreams, status: "observed", values: { total: 99, active: 99 } },
        },
      }).pipe(Effect.mapError(() => new CollectorFailure("collector_failed"))),
    });
    const result = yield* h.call(["decision-snapshot"]);
    expect(result.coverage).toBe("partial");
    expect(result.sources.host).toEqual(host);
    expect(result.sources.workstreams).toMatchObject({
      status: "unavailable",
      observed_at: null,
      reason: "registry_counts_unavailable",
      values: {},
      scope: {
        registry_id: null,
        owner_id: null,
        principal_id: null,
        server_generation: null,
        registry_version: null,
        authorization_revision: null,
      },
    });
    const legacy = { ...result, sources: {
      ...result.sources,
      workstreams: { ...old.sources.workstreams, status: "partial" as const, scope: {
        native_read: { contract: "workstreams-read/1.0.0", historical: true },
        registry_id: "https://historical.example.test",
        owner_id: "historical-owner",
        registry_version: 7,
      } },
    } };
    expect(yield* decodeSnapshotJson(yield* encodeSnapshotJson(legacy))).toEqual(legacy);
  }),
);

it.effect("round trips the fallback through the immutable collector and proves personal scope is rejected", () =>
  Effect.gen(function* () {
    let launches = 0;
    const collector: DecisionSnapshotCollector["Service"] = {
      collect: (purpose, input) => Effect.gen(function* () {
        const packet = yield* input;
        return yield* Effect.tryPromise({
          try: (signal) => new Promise<string>((resolve, reject) => {
            launches++;
            NodeChildProcess.execFile("/usr/bin/python3", ["-I", "-S", "-B", "-c", `
import asyncio, copy, hashlib, importlib.machinery, importlib.util, json, pathlib, sys
path = pathlib.Path(sys.argv[1])
assert hashlib.sha256(path.read_bytes()).hexdigest() == '089bd5fc2c03ec7b7dd2b3b031a68bc1145d3d38f7ad74a9cf4e0c653555d40a'
loader = importlib.machinery.SourceFileLoader('collector_fixture', str(path))
spec = importlib.util.spec_from_loader(loader.name, loader)
module = importlib.util.module_from_spec(spec)
sys.modules[loader.name] = module
loader.exec_module(module)
packet = json.loads(sys.argv[3])
module.native_envelope(packet)
personal = copy.deepcopy(packet)
personal['sources']['workstreams']['scope'].update(count_scope='environment_local', environment_id='synthetic-environment', sequence=1)
try:
    module.native_envelope(personal)
except module.Invalid:
    pass
else:
    raise AssertionError('collector unexpectedly accepts personal scope')
print(json.dumps(asyncio.run(module.compose({}, purpose=sys.argv[2], native=packet))))
`, NodeURL.fileURLToPath(new URL("./fixtures/decision_snapshot.py.fixture", import.meta.url)), purpose, packet],
              { signal, timeout: 5_000, maxBuffer: 262_144 },
              (error, stdout) => error ? reject(error) : resolve(stdout));
          }),
          catch: () => new CollectorFailure("collector_failed"),
        });
      }),
    };
    const h = yield* harness(false, 0, {
      readOperatingCounts: () => Effect.succeed({
        total: 4,
        operating: 2,
        foregroundWaitingApproval: 0,
        foregroundWaitingInput: 0,
        foregroundWaitingPlan: 0,
        backgroundOperating: 0,
        backgroundUnknown: 0,
        backgroundSampledAt: DateTime.formatIso(DateTime.nowUnsafe()),
      }),
    }, collector);
    const result = yield* h.call(["decision-snapshot"]);
    expect(launches).toBe(1);
    expect(result.sources.threads).toMatchObject({ status: "observed", values: { total: 4, operating: 2 } });
    expect(result.sources.host.reason).toBe("source_not_bound");
    expect(result.sources.workstreams).toMatchObject({
      status: "unavailable", reason: "registry_counts_unavailable", values: {},
    });
    expect(yield* decodeSnapshotJson(yield* encodeSnapshotJson(result))).toEqual(result);
  }),
);
