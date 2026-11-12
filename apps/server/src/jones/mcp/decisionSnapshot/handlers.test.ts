// @effect-diagnostics-next-line nodeBuiltinImport:off -- The integration test runs only the immutable synthetic collector fixture.
import * as NodeChildProcess from "node:child_process";
// The fixture path is resolved without touching application state.
import * as NodeURL from "node:url";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  WORKSTREAM_COUNTS_MANIFEST_SHA256,
} from "@t3tools/contracts";
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
import * as WorkstreamGateway from "../../../workstreams/WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "../../../workstreams/SyntheticWorkstreamTransport.ts";
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
    expect(yield* decodeUnknownJson(h.state().envelope)).toMatchObject({
      schema: "codex.decision-snapshot-native/v1",
      provenance: "native_observed",
    });
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
              Layer.mergeAll(
                ProjectionStore.layerMemory,
                Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
                Layer.mock(WorkstreamGateway.WorkstreamGateway)({
                  purgeAuthorization: () => {},
                  readRegistryCounts: () =>
                    Effect.fail(
                      new WorkstreamGateway.WorkstreamGatewayError({
                        reason: "contract-mismatch",
                        detail: "Synthetic provider lacks counts.",
                      }),
                    ),
                }),
              ),
            ),
          ),
        ),
      );
      const h = yield* harness(false, 0, nativePorts);
      assertApplicationScope(yield* h.call(["decision-snapshot"]));
      expect(yield* decodeUnknownJson(h.state().envelope)).toMatchObject({
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

const liveCounts = Effect.fnUntraced(function* (supported = true) {
  const fixture = makeSyntheticWorkstreamTransport();
  const binding = {
    registryId: "https://registry.example.test",
    ownerId: "owner-fixture",
    principalId: "principal-fixture",
    authorizationRevision: 3,
  };
  const gateway = yield* WorkstreamGateway.make(
    {
      ...fixture,
      ...(supported
        ? {
            getCountsCapabilities: () =>
              fixture
                .getCapabilities({
                  contractVersion: "workstreams/1.0.0",
                  contractManifest: WorkstreamGateway.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
                })
                .pipe(
                  Effect.map((caps) => ({
                    ...caps,
                    manifest_sha256: WORKSTREAM_COUNTS_MANIFEST_SHA256,
                    registry_counts: {
                      version: 1,
                      path: "/workstreams/v1/counts",
                      active_definition: "latest_owner_lifecycle_declaration_active",
                    } as const,
                  })),
                ),
            getRegistryCounts: () =>
              Effect.succeed({
                context: { owner_id: binding.ownerId, server_generation: 7, registry_version: 11 },
                principal_id: binding.principalId,
                observed_at: DateTime.formatIso(DateTime.nowUnsafe()),
                authority_effect: "none" as const,
                total: 9,
                active: 3,
                unknown_lifecycle: 1,
              }),
          }
        : {}),
    },
    { binding },
  );
  return yield* DecisionSnapshotNativeCounts.pipe(
    Effect.provide(
      DecisionSnapshotNativeCountsLive.pipe(
        Layer.provide(
          Layer.mergeAll(
            ProjectionStore.layerMemory,
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
            Layer.succeed(WorkstreamGateway.WorkstreamGateway, gateway),
          ),
        ),
      ),
    ),
  );
});

it.effect(
  "wires negotiated exact counts into the Live adapter and fails closed without support",
  () =>
    Effect.gen(function* () {
      const live = yield* liveCounts();
      expect(yield* live.readRegistryCounts!()).toMatchObject({
        complete: true,
        counts: { total: 9, active: 3, unknown_lifecycle: 1 },
        binding: { authorizationRevision: 3, serverGeneration: 7, registryVersion: 11 },
      });
      const result = yield* (yield* harness(false, 0, live)).call(["decision-snapshot"]);
      expect(result.sources.workstreams).toMatchObject({
        status: "observed",
        values: { total: 9, active: 3, unknown_lifecycle: 1 },
        provenance: { timestamp_basis: "native_observation" },
      });
      const unsupported = yield* liveCounts(false);
      expect(yield* unsupported.readRegistryCounts!().pipe(Effect.flip)).toMatchObject({
        _tag: "NativeCountReadError",
        source: "workstreams",
      });
      expect(
        (yield* (yield* harness(false, 0, unsupported)).call(["decision-snapshot"])).sources
          .workstreams,
      ).toMatchObject({ status: "unavailable", values: {}, reason: "registry_counts_unavailable" });
    }),
);

it.effect.each(["legacy", "native"] as const)(
  "round-trips %s adapter counts and a v1 thread entry through the unchanged real collector",
  (mode) =>
    Effect.gen(function* () {
      const legacy = yield* liveCounts();
      const live: DecisionSnapshotNativeCounts["Service"] =
        mode === "legacy"
          ? legacy
          : {
              ...legacy,
              readRegistryCounts: () =>
                legacy.readRegistryCounts!().pipe(
                  Effect.map((observation) => ({
                    ...observation,
                    envelope: {
                      contract: "workstreams-read/1.0.0",
                      registry: {
                        registry_instance_id: "11111111-1111-4111-8111-111111111111",
                        owner_id: observation.binding.ownerId,
                        canonical_origin: observation.binding.registryId,
                        server_generation: observation.binding.serverGeneration,
                        registry_version: observation.binding.registryVersion,
                      },
                      provenance: {
                        authority: "native-home",
                        home_environment_id: "native:home",
                        served_by_environment_id: "native:home",
                        source: "live",
                        stale: false,
                        observed_at: observation.counts.observed_at,
                      },
                      coverage: { registry: "complete", sources: [] },
                    },
                  })),
                ),
            };
      let launches = 0;
      // Only native sources are bound. compose never invokes a host/account collector in this fixture.
      const collector: DecisionSnapshotCollector["Service"] = {
        collect: (purpose, input) =>
          Effect.gen(function* () {
            const packet = yield* input;
            return yield* Effect.tryPromise({
              try: (signal) =>
                new Promise<string>((resolve, reject) => {
                  launches++;
                  NodeChildProcess.execFile(
                    "/usr/bin/python3",
                    [
                      "-I",
                      "-S",
                      "-B",
                      "-c",
                      `
import asyncio, hashlib, importlib.machinery, importlib.util, json, pathlib, sys
path = pathlib.Path(sys.argv[1])
assert hashlib.sha256(path.read_bytes()).hexdigest() == '089bd5fc2c03ec7b7dd2b3b031a68bc1145d3d38f7ad74a9cf4e0c653555d40a'
loader = importlib.machinery.SourceFileLoader('collector_fixture', str(path))
spec = importlib.util.spec_from_loader(loader.name, loader)
module = importlib.util.module_from_spec(spec)
sys.modules[loader.name] = module
loader.exec_module(module)
print(json.dumps(asyncio.run(module.compose({}, purpose=sys.argv[2], native=json.loads(sys.argv[3])))))
`,
                      NodeURL.fileURLToPath(
                        new URL("./fixtures/decision_snapshot.py.fixture", import.meta.url),
                      ),
                      purpose,
                      packet,
                    ],
                    { signal, timeout: 5_000, maxBuffer: 262_144 },
                    (error, stdout) => (error ? reject(error) : resolve(stdout)),
                  );
                }),
              catch: () => new CollectorFailure("collector_failed"),
            });
          }),
      };
      const h = yield* harness(
        false,
        0,
        {
          readRegistryCounts: live.readRegistryCounts!,
          // Production's richer thread packet is the separately deferred P2; do not downlevel it here.
          readOperatingCounts: () =>
            Effect.succeed({
              total: 4,
              operating: 2,
              foregroundWaitingApproval: 0,
              foregroundWaitingInput: 0,
              foregroundWaitingPlan: 0,
              backgroundOperating: 0,
              backgroundUnknown: 0,
              backgroundSampledAt: DateTime.formatIso(DateTime.nowUnsafe()),
            }),
        },
        collector,
      );
      const result = yield* h.call(["decision-snapshot"]);
      expect(launches).toBe(1);
      expect(result.sources.workstreams).toMatchObject({
        status: "observed",
        values: { total: 9, active: 3, unknown_lifecycle: 1 },
        scope: {
          registry_id: "https://registry.example.test",
          owner_id: "owner-fixture",
          principal_id: "principal-fixture",
          server_generation: 7,
          registry_version: 11,
          authorization_revision: 3,
        },
        provenance: {
          schema: "codex.decision-snapshot-native/v1",
          kind: "native_observed",
          timestamp_basis: "native_observation",
        },
      });
      expect(result.sources.threads).toMatchObject({
        status: "observed",
        values: { total: 4, operating: 2 },
      });
      expect(result.sources.host.reason).toBe("source_not_bound");
      if (mode === "native")
        expect(result.sources.workstreams.scope.native_read).toMatchObject({
          contract: "workstreams-read/1.0.0",
          coverage: { registry: "complete" },
          registry: { registry_version: 11, server_generation: 7 },
        });
      const encoded = yield* encodeSnapshotJson(result);
      expect(yield* decodeSnapshotJson(encoded)).toEqual(result);
    }),
);

import type { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as NativeReadRoutes from "../../workstreams/native/read/routes.ts";
const nativeEnvelope: WorkstreamsNativeReadEnvelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "native:owner",
    canonical_origin: "https://native.workstreams.invalid",
    server_generation: 2,
    registry_version: 8,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "native:home",
    served_by_environment_id: "native:home",
    source: "live",
    stale: false,
    observed_at: "2026-10-02T00:00:00Z",
  },
  coverage: {
    registry: "complete",
    sources: [
      {
        source_instance_id: "orphan:source",
        mapping: "unmapped",
        attestation: "unknown",
        observed_at: null,
      },
    ],
  },
};
const nativeObservation = (envelope = nativeEnvelope) => ({
  envelope,
  complete: true,
  counts: {
    context: {
      owner_id: envelope.registry.owner_id,
      server_generation: envelope.registry.server_generation,
      registry_version: envelope.registry.registry_version,
    },
    principal_id: "native:principal",
    observed_at: envelope.provenance.observed_at,
    active: 2,
    total: 3,
    unknown_lifecycle: 1,
  },
  binding: {
    registryId: envelope.registry.canonical_origin,
    ownerId: envelope.registry.owner_id,
    principalId: "native:principal",
    serverGeneration: envelope.registry.server_generation,
    registryVersion: envelope.registry.registry_version,
    authorizationRevision: 4,
  },
});
it.effect.each(["live", "cache"] as const)(
  "retains native %s coverage in decision counts without promoting cache to complete",
  (source) =>
    Effect.gen(function* () {
      const envelope: WorkstreamsNativeReadEnvelope =
        source === "live"
          ? nativeEnvelope
          : {
              ...nativeEnvelope,
              provenance: { ...nativeEnvelope.provenance, source: "cache", stale: true },
              coverage: { ...nativeEnvelope.coverage, registry: "stale" },
            };
      const h = yield* harness(false, 0, {
        readRegistryCounts: () => Effect.succeed(nativeObservation(envelope)),
      });
      const result = yield* h.call(["decision-snapshot"]);
      expect(result.sources.workstreams).toMatchObject({
        status: source === "live" ? "observed" : "stale",
        scope: { native_read: envelope, registry_version: 8, authorization_revision: 4 },
        values: { active: 2, total: 3, unknown_lifecycle: 1 },
      });
      expect(JSON.parse(h.state().envelope).sources.workstreams.scope).not.toHaveProperty(
        "native_read",
      );
    }),
);
it.effect("rejects incoherent native versions and retains old result decoding", () =>
  Effect.gen(function* () {
    const observation = nativeObservation();
    const h = yield* harness(false, 0, {
      readRegistryCounts: () =>
        Effect.succeed({
          ...observation,
          envelope: {
            ...observation.envelope,
            registry: { ...observation.envelope.registry, registry_version: 9 },
          },
        }),
    });
    expect((yield* h.call(["decision-snapshot"])).sources.workstreams).toMatchObject({
      status: "unavailable",
      values: {},
    });
    const unsupported = yield* harness(false, 0, {
      readRegistryCounts: () =>
        Effect.succeed({
          ...observation,
          envelope: {
            ...observation.envelope,
            contract: "workstreams-read/9.0.0",
          } as unknown as WorkstreamsNativeReadEnvelope,
        }),
    });
    expect((yield* unsupported.call(["decision-snapshot"])).sources.workstreams.status).toBe(
      "unavailable",
    );
  }),
);
it.effect(
  "uses admitted native counts in the Live adapter and never calls the legacy gateway on native failure",
  () =>
    Effect.gen(function* () {
      let legacyReads = 0,
        nativeReads = 0,
        unavailable = false;
      const dependencies = Layer.mergeAll(
        ProjectionStore.layerMemory,
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
        Layer.mock(WorkstreamGateway.WorkstreamGateway)({
          purgeAuthorization: () => {},
          readRegistryCounts: () => {
            legacyReads++;
            return Effect.die("Legacy transport must not be called");
          },
        }),
        Layer.succeed(NativeReadRoutes.AuthorizedReads, {
          read: (request) => {
            nativeReads++;
            expect(request).toEqual({ route: "counts", params: {} });
            return unavailable
              ? Effect.fail(new NativeRegistryError({ code: "unavailable" }))
              : Effect.succeed({
                  route: "counts",
                  envelope: nativeEnvelope,
                  authorization_revision: 4,
                  value: { ...nativeObservation().counts, authority_effect: "none" },
                });
          },
        }),
      );
      const live = yield* DecisionSnapshotNativeCounts.pipe(
        Effect.provide(DecisionSnapshotNativeCountsLive.pipe(Layer.provide(dependencies))),
      );
      expect(yield* live.readRegistryCounts!()).toMatchObject({
        envelope: nativeEnvelope,
        binding: { authorizationRevision: 4 },
        counts: { active: 2, total: 3 },
      });
      unavailable = true;
      expect(yield* live.readRegistryCounts!().pipe(Effect.flip)).toMatchObject({
        source: "workstreams",
      });
      expect(nativeReads).toBe(2);
      expect(legacyReads).toBe(0);
    }),
);
import { RegistryError as NativeRegistryError } from "../../workstreams/native/registry/session.ts";
