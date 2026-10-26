// @effect-diagnostics nodeBuiltinImport:off -- Synthetic file-byte inspection and deterministic receipt assertions.
import { vi } from "vite-plus/test";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as EnvironmentAuth from "../../../../auth/EnvironmentAuth.ts";
import { layerAuthenticatedAuth } from "../../../../auth/http.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../../workstreams/http.ts";
import * as LegacyRegistration from "../../registrationContext/service.ts";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as Wire from "@t3tools/contracts";
import { WorkstreamsNativePlacementResult } from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as NativeRuntimeEntry from "../runtime/entry.ts";
import * as NativeAdmission from "../runtime/admission.ts";
import {
  WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
  WorkstreamsLegacyReceiptLookupResult,
} from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as TestClock from "effect/testing/TestClock";
import * as EffectNodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Durable from "../operations/durableEntries.ts";
import * as HomeExecutor from "../operations/homeExecutor.ts";
import * as Operations from "../../operations/service.ts";
import { Ports } from "../../operations/ports.ts";
import type { WorkstreamOperationRequest } from "@t3tools/contracts/jones/workstreamOperations";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as McpHandlers from "../../../mcp/workstreams/handlers.ts";
import * as McpTools from "../../../mcp/workstreams/tools.ts";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "../registry/session.ts";
import * as Registry from "../registry/service.ts";
import * as Archive from "../store/archiveStore.ts";
import * as OperationStore from "../store/operationStore.ts";
import * as Admin from "../cli.ts";
import * as Local from "../registry/localTransport.ts";
import * as Appearance from "../registry/appearance.ts";
import * as Counts from "../registry/counts.ts";
import * as Placements from "../registry/placements.ts";
import * as Runner from "../registry/effectRunner.ts";
import * as Registration from "../registry/registration.ts";
import * as Store from "../store/registryStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as History from "../store/historyStore.ts";
import * as Effects from "../store/effectStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Attestation from "../providers/localAttestation.ts";
import * as Settlement from "../providers/localSettlement.ts";
import * as Resolver from "../providers/referenceResolver.ts";
import * as GitHub from "../providers/githubPrObserver.ts";
import { syntheticGitHubLayer } from "../providers/synthetic.ts";
import * as Provider from "../../nativeProvider/service.ts";
import { NativeProviderAttempts } from "../../nativeProvider/attemptRepository.ts";
import { binding, makeProviderFixture } from "../../nativeProvider/testFixtures.ts";
import { NativeStoreAuthority } from "../../../../environment/NativeStoreAuthority.ts";
import { canonicalJson } from "../domain/validation.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";
import type { WorkstreamAction, ExternalIntent, RegistryExternalResult } from "../domain/state.ts";
import { resolveNativeMode } from "../mode/config.ts";

const decodeSettlementRequest = Schema.decodeUnknownEffect(Wire.WorkstreamsNativeSettlementRequest);
const decodeNativePlacementResult = Schema.decodeUnknownEffect(WorkstreamsNativePlacementResult);
const decodeLegacyReceiptLookupResult = Schema.decodeUnknownEffect(
  WorkstreamsLegacyReceiptLookupResult,
);
const decodeReceipt = Schema.decodeUnknownEffect(Wire.WorkstreamReceipt);
const decodeListResult = Schema.decodeUnknownEffect(Wire.T3WorkstreamListResult);
const home = {
  registry_instance_id: Fixture.scope.registry_instance_id,
  owner_id: binding.owner_id,
  canonical_origin: "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
  home_environment_id: binding.source_instance_id,
  served_by_environment_id: binding.source_instance_id,
  server_generation: 1,
};
const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
const caller: AdmittedCaller = {
  principal_id: binding.principal_id,
  source_environment_id: binding.source_instance_id,
  binding_version: 1,
  origin: { kind: "app" },
};
const headers = {
  contractVersion: Wire.WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
};

const runtime = (
  options: { filename?: string; keys?: Map<string, Uint8Array>; github?: boolean } = {},
) => {
  const fixture = makeProviderFixture();
  const providerPorts: Provider.NativeProviderPorts = {
    ...fixture.ports,
    engine: {
      dispatch: (command) =>
        Effect.sync(() => {
          if (command.type !== "thread.settle" && command.type !== "thread.unsettle")
            throw new Error("Unexpected synthetic native command");
          const attempt = [...fixture.attempts.values()].find(
            (a) => a.nativeCommandId === command.commandId,
          );
          if (!attempt || attempt.dispatchStartedAt === null)
            throw new Error("Source dispatch preceded its admitted attempt");
          fixture.calls.push({
            commandId: command.commandId,
            threadId: command.threadId,
            action: command.type,
          });
          fixture.commit(
            command.commandId,
            command.type === "thread.settle" ? "settle" : "unsettle",
          );
          return { sequence: 1, storedEvents: [] };
        }),
    },
  };
  const persistence = Fixture.testLayer(options).pipe(
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
  const registry = Registry.layer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(persistence),
  );
  const source = Layer.mergeAll(Attestation.layer, Settlement.layer).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(
          Attestation.NativeProvider,
          Provider.makeWorkstreamsNativeProvider(providerPorts, "v2"),
        ),
        Layer.succeed(NativeProviderAttempts, fixture.ports.attempts),
        Layer.succeed(Attestation.SourceAdmission, {
          authorize: (input) =>
            Effect.gen(function* () {
              expect(input.expected_home).toEqual({
                registry_instance_id: home.registry_instance_id,
                owner_id: home.owner_id,
                home_environment_id: home.home_environment_id,
                server_generation: home.server_generation,
              });
              if (input.thread_id !== null && input.thread_id !== "thread-synthetic")
                return yield* new Attestation.ProviderError({ code: "not_granted" });
              return binding;
            }),
        }),
      ),
    ),
  );
  const github = options.github
    ? syntheticGitHubLayer([
        {
          source_instance_id: "github-synthetic",
          locator: {
            host: "github.com",
            repository_owner: "jones",
            repository_name: "example",
            number: 7,
          },
          result: {
            outcome: "observed",
            observed_at: Fixture.now,
            retry_after_seconds: null,
            etag: '"synthetic-etag"',
            last_success: {
              state: "open",
              draft: false,
              observed_at: Fixture.now,
              provider_updated_at: Fixture.now,
            },
          },
        },
      ])
    : GitHub.unconfiguredLayer;
  const sources = Layer.effect(
    Resolver.ReferenceSources,
    Effect.gen(function* () {
      const attestation = yield* Attestation.SourceAttestation;
      return {
        github: options.github
          ? {
              owner_id: home.owner_id,
              principal_id: caller.principal_id,
              source_instance_id: "github-synthetic",
              authority_namespace: "github-authority",
              store_generation: 1,
            }
          : null,
        selectT3: (input) =>
          Effect.succeed(
            input.identity.source_instance_id === binding.source_instance_id
              ? {
                  expected_home: {
                    registry_instance_id: home.registry_instance_id,
                    owner_id: home.owner_id,
                    home_environment_id: home.home_environment_id,
                    server_generation: 1,
                  },
                  authority_namespace: binding.authority_namespace,
                  store_generation: binding.store_generation,
                  source: attestation,
                }
              : null,
          ),
      };
    }),
  ).pipe(Layer.provideMerge(source));
  const providers = Resolver.layer.pipe(Layer.provideMerge(sources), Layer.provideMerge(github));
  const trust = {
    readTrustSnapshot: () => ({
      trustedEnvironments: [
        {
          environmentId: binding.source_instance_id,
          authorityNamespace: binding.authority_namespace,
          storeGeneration: binding.store_generation,
        },
      ],
      readiness: "ready" as const,
    }),
  };
  const services = Layer.mergeAll(
    Runner.layer,
    Appearance.layer,
    Counts.layer,
    Placements.layer,
    Registration.layer,
  ).pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(providers),
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(Placements.PlacementTrust, trust),
        Layer.succeed(NativeStoreAuthority, { ...fixture.ports.authority, trustProvider: trust }),
      ),
    ),
  );
  return { fixture, layer: Local.layer.pipe(Layer.provideMerge(services)) };
};
const seed = Effect.gen(function* () {
  yield* migration;
  const store = yield* Store.RegistryStore,
    sql = yield* SqlClient.SqlClient,
    bindings = yield* Bindings.BindingStore;
  yield* store.initialize({
    ...scope,
    canonical_origin: home.canonical_origin,
    home_environment_id: home.home_environment_id,
    server_generation: 1,
    registry_version: 0,
    lineage: "native",
    active_key_id: "test-home",
    state: "active",
  });
  yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('test-home',${home.registry_instance_id},'home','active',${Fixture.now}),('test-local',${home.registry_instance_id},'local','active',${Fixture.now})`;
  yield* store.appendPlain(scope, "principals", {
    principal_id: binding.principal_id,
    environment_id: binding.source_instance_id,
    kind: "environment",
    provenance: '{"kind":"synthetic-source-admission"}',
  });
  yield* store.appendPlain(scope, "grants", {
    principal_id: binding.principal_id,
    grant_version: 1,
    grant_id: "grant-synthetic",
    permissions: '["workstreams:read","workstreams:write"]',
    state: "active",
  });
  yield* store.appendPlain(scope, "source_bindings", {
    provider: "t3",
    source_instance_id: binding.source_instance_id,
    binding_version: 1,
    authority_namespace: binding.authority_namespace,
    store_generation: binding.store_generation,
    state: "active",
  });
  yield* bindings.bind(
    {
      ...scope,
      home_environment_id: home.home_environment_id,
      home_generation: 1,
      principal_id: binding.principal_id,
      binding_version: 1,
      state: "active",
    },
    null,
  );
});
const harness = Effect.gen(function* () {
  const local = yield* Local.LocalTransport,
    registry = yield* Registry.Registry;
  const adapter = yield* local.forCaller(caller);
  let sequence = 0;
  const command = (action: WorkstreamAction) =>
    registry.snapshot(caller).pipe(
      Effect.map((s) => ({
        command_id: "command-s1-" + String(++sequence).padStart(16, "0"),
        expected_server_generation: 1,
        expected_registry_version: s.context.registry_version,
        action,
      })),
    );
  const submit = (action: WorkstreamAction) =>
    command(action).pipe(Effect.flatMap((c) => adapter.gateway.submit(c)));
  const committed = (action: WorkstreamAction) =>
    submit(action).pipe(
      Effect.map((receipt) => {
        expect(receipt.state).toBe("committed");
        if (receipt.state !== "committed")
          throw new Error("Expected a committed synthetic result.");
        return receipt;
      }),
    );
  const create = (name: string) =>
    committed({
      operation: "create_workstream",
      name,
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    }).pipe(Effect.map((r) => r.effects.workstream_versions[0]!.workstream_id));
  const register = () =>
    committed({
      operation: "register_reference",
      identity: {
        provider: "t3",
        source_instance_id: binding.source_instance_id,
        resource_kind: "thread",
        id_kind: "internal",
        native_id: "thread-synthetic",
        account_provenance: { kind: "not_account_scoped" },
      },
      pr_locator: null,
    }).pipe(Effect.map((r) => r.effects.native_reference_id!));
  const verify = (id: string) =>
    committed({
      operation: "verify_reference",
      native_reference_id: id,
      expected_attestation_version: 0,
    });
  return { ...adapter, registry, command, submit, committed, create, register, verify };
});

it.effect(
  "real encrypted home serves gateway create/read/CAS/replay and rejected/unchanged history",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("S1 sentinel registry name");
      const original = yield* h.command({
        operation: "update_workstream",
        workstream_id: a,
        expected_version: 1,
        name: "renamed",
        lifecycle: "active",
        progress: { state: "progressing" },
        sort_order: 4,
      });
      const saved = yield* h.gateway.submit(original);
      expect(saved.state).toBe("committed");
      const body = canonicalJson(original);
      const replay = yield* h.transport.resubmitCommand!({
        ...headers,
        command: original,
        body,
        idempotencyKey: original.command_id,
      });
      expect(replay).toEqual({ kind: "receipt", receipt: saved });
      const changed = yield* h.transport.resubmitCommand!({
        ...headers,
        command: original,
        body: " " + body,
        idempotencyKey: original.command_id,
      });
      expect(changed).toEqual({ kind: "idempotency_conflict" });
      const stale = yield* h.transport.submitCommand({
        ...headers,
        command: { ...original, command_id: "stale-command-000000000" },
        body: canonicalJson({ ...original, command_id: "stale-command-000000000" }),
        idempotencyKey: "stale-command-000000000",
      });
      expect(stale.state).toBe("rejected");
      if (stale.state === "rejected") expect(stale.error.code).toBe("version_conflict");
      expect((yield* h.gateway.readDetail(a)).workstream.name).toBe("renamed");
      const counts = yield* (yield* Counts.Counts).read(caller);
      expect(counts).toMatchObject({ total: 1, active: 1, unknown_lifecycle: 0 });
      const events = yield* h.transport.listHistory({ ...headers, limit: 100 });
      expect(events.items).toHaveLength(3);
      expect(
        events.items.filter((x) => x.registry_version === counts.context.registry_version),
      ).toHaveLength(2);
      const appearance = yield* h.gateway.saveAppearance({
        command_id: "appearance-command-000001",
        workstream_id: a,
        expected_server_generation: 1,
        expected_version: 0,
        border_color: "#A1B2C3",
      });
      expect(appearance.version).toBe(1);
      expect(yield* h.gateway.readAppearance([a])).toMatchObject({
        supported: true,
        page: { items: [appearance] },
      });
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "source-issued attestation and atomic primary moves use real SQLite; no source membership copy",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("a"),
        b = yield* h.create("b"),
        r = yield* h.register();
      yield* h.verify(r);
      const before = yield* h.registry.snapshot(caller);
      const attached = yield* h.committed({
        operation: "attach_primary",
        workstream_id: a,
        expected_version: before.state.workstreams.get(a)!.version,
        native_reference_id: r,
      });
      const membership = attached.effects.membership_ids[0]!;
      const after = yield* h.registry.snapshot(caller);
      yield* h.committed({
        operation: "move_primary",
        source_workstream_id: a,
        destination_workstream_id: b,
        expected_source_version: after.state.workstreams.get(a)!.version,
        expected_destination_version: after.state.workstreams.get(b)!.version,
        source_membership_id: membership,
      });
      const moved = yield* h.registry.snapshot(caller);
      expect([...moved.state.memberships.values()].filter((m) => m.closed === null)).toHaveLength(
        1,
      );
      expect(moved.state.memberships.get(membership)?.closed?.reason).toBe("continued-elsewhere");
      expect(moved.state.workstreams.get(a)!.version).toBe(
        after.state.workstreams.get(a)!.version + 1,
      );
      expect(moved.state.workstreams.get(b)!.version).toBe(
        after.state.workstreams.get(b)!.version + 1,
      );
      const page = yield* (yield* Placements.Placements).load(caller, {
        identities: [
          { source_instance_id: binding.source_instance_id, native_thread_id: "thread-synthetic" },
        ],
      });
      yield* decodeNativePlacementResult(page, { onExcessProperty: "error" });
      expect(page.page.items[0]).toMatchObject({
        workstream_id: b,
        attestation_source: "native",
        authority_namespace: binding.authority_namespace,
        store_generation: binding.store_generation,
      });
      const v1 = yield* h.gateway.readThreadPlacements({
        identities: [
          { source_instance_id: binding.source_instance_id, native_thread_id: "thread-synthetic" },
        ],
      });
      expect(v1.page.items[0]).not.toHaveProperty("attestation_source");
      expect(
        (yield* (yield* Registration.Registration).forCaller(caller).read()).t3Availability.state,
      ).toBe("ready");
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "history snapshots exclude later same-version rejections and reject stale/cross-scope cursors",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("history-a"),
        b = yield* h.create("history-b");
      for (let i = 0; i < 8; i++)
        yield* h.submit({
          operation: "update_workstream",
          workstream_id: a,
          expected_version: 999,
          name: "bad",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
      const first = yield* h.transport.listHistory({ ...headers, limit: 1 });
      expect(first.next_cursor).not.toBeNull();
      yield* h.submit({
        operation: "update_workstream",
        workstream_id: a,
        expected_version: 999,
        name: "late bad",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const items = [...first.items];
      let cursor = first.next_cursor;
      while (cursor) {
        const page = yield* h.transport.listHistory({ ...headers, limit: 1, cursor });
        items.push(...page.items);
        cursor = page.next_cursor;
      }
      expect(items).toHaveLength(10);
      const wrong = yield* h.transport
        .listHistory({ ...headers, limit: 1, workstreamId: b, cursor: first.next_cursor! })
        .pipe(Effect.flip);
      expect(wrong.detail).toBe("cursor_invalid");
      yield* h.create("advance registry");
      const stale = yield* h.transport
        .listHistory({ ...headers, limit: 1, cursor: first.next_cursor! })
        .pipe(Effect.flip);
      expect(stale.detail).toBe("cursor_stale");
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect("grants, exact binding, and generation fence both reads and dispatch", () =>
  Effect.gen(function* () {
    yield* seed;
    const h = yield* harness,
      local = yield* Local.LocalTransport,
      store = yield* Store.RegistryStore;
    for (const invalid of [
      { ...caller, principal_id: "foreign" },
      { ...caller, binding_version: 2 },
      { ...caller, source_environment_id: "foreign" },
    ]) {
      const error = yield* local.forCaller(invalid).pipe(Effect.flip);
      expect(error.code).toBe("forbidden");
    }
    const wrong = yield* h.homeTransport
      .lookup(
        { ...home, home_generation: 2, principal_id: caller.principal_id, binding_version: 1 },
        "absent-command-000000",
      )
      .pipe(Effect.flip);
    expect(wrong.reason).toBe("unknown-effect");
    yield* store.appendPlain(scope, "grants", {
      principal_id: binding.principal_id,
      grant_version: 2,
      grant_id: "grant-synthetic",
      permissions: '["workstreams:read","workstreams:write"]',
      state: "revoked",
    });
    const error = yield* h.registry.snapshot(caller).pipe(Effect.flip);
    expect(error.code).toBe("forbidden");
  }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "concurrent registry CAS keeps one primary and a failed move leaves both aggregates unchanged",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("race-a"),
        b = yield* h.create("race-b"),
        r = yield* h.register();
      yield* h.verify(r);
      const ca = yield* h.command({
        operation: "attach_primary",
        workstream_id: a,
        expected_version: 1,
        native_reference_id: r,
      });
      const cb = {
        ...ca,
        command_id: "concurrent-command-000000",
        action: { ...ca.action, workstream_id: b },
      };
      const results = yield* Effect.all(
        [ca, cb].map((c) =>
          h.transport.submitCommand({
            ...headers,
            command: c,
            body: canonicalJson(c),
            idempotencyKey: c.command_id,
          }),
        ),
        { concurrency: 2 },
      );
      expect(results.filter((x) => x.state === "committed")).toHaveLength(1);
      const before = yield* h.registry.snapshot(caller),
        m = [...before.state.memberships.values()].find((x) => x.closed === null)!;
      const rejected = yield* h.submit({
        operation: "move_primary",
        source_workstream_id: m.workstream_id,
        destination_workstream_id: m.workstream_id === a ? b : a,
        expected_source_version: 999,
        expected_destination_version: 1,
        source_membership_id: m.membership_id,
      });
      expect(rejected.state).toBe("rejected");
      const after = yield* h.registry.snapshot(caller);
      expect([...after.state.workstreams]).toEqual([...before.state.workstreams]);
      expect([...after.state.memberships]).toEqual([...before.state.memberships]);
    }).pipe(Effect.provide(runtime().layer)),
);

it("mode fencing never treats an existing binding as native selection", () => {
  expect(
    resolveNativeMode({ config: null, legacyActivationPresent: false, keyPresent: true }),
  ).toBe("disabled");
  expect(
    resolveNativeMode({
      config: { mode: "native-home", registry: home },
      legacyActivationPresent: true,
      keyPresent: true,
    }),
  ).toBe("transition-fenced");
  expect(
    resolveNativeMode({
      config: { mode: "native-home", registry: home },
      legacyActivationPresent: false,
      keyPresent: false,
    }),
  ).toBe("transition-fenced");
});

const withOperations = Effect.fn("S1.withOperations")(function* <A, E, R>(
  h: Effect.Success<typeof harness>,
  run: (service: Operations.WorkstreamOperations["Service"]) => Effect.Effect<A, E, R>,
) {
  const registration = (yield* Registration.Registration).forCaller(caller);
  return yield* Effect.gen(function* () {
    const entries = yield* Durable.DurableEntries,
      executor = yield* HomeExecutor.HomeExecutor;
    return yield* Effect.gen(function* () {
      return yield* run(yield* Operations.WorkstreamOperations);
    }).pipe(
      Effect.provide(
        Operations.layer.pipe(
          Layer.provide(
            Layer.succeed(Ports, {
              native: { mode: "native", entries, executor: h.guardExecutor(executor) },
              environmentId: home.home_environment_id,
              registryId: home.registry_instance_id,
              activation: { state: "enabled", missing_keys: [] },
              gateway: h.gateway,
              registration,
              threadExists: (id) => Effect.succeed(id === "thread-synthetic"),
              now: Clock.currentTimeMillis,
              sleep: Effect.sleep,
              jitter: Effect.succeed(0),
            }),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(Durable.layer, HomeExecutor.layer).pipe(
        Layer.provide(Layer.succeed(HomeExecutor.HomeTransport, h.homeTransport)),
        Layer.provideMerge(EffectNodeCrypto.layer),
      ),
    ),
  );
});

it.effect(
  "durable operations and raw commands share the same encrypted home and observed primary",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("operations target");
      const request: WorkstreamOperationRequest = {
        contract: "workstream-operations/1.0.0",
        operation_id: "operation-s1-000000001",
        origin: { kind: "app" },
        kind: "set_primary",
        thread: { environment_id: home.home_environment_id, thread_id: "thread-synthetic" },
        target: {
          kind: "workstream",
          registry_id: home.registry_instance_id,
          owner_id: home.owner_id,
          workstream_id: a,
        },
        expected: { kind: "unassigned" },
      };
      yield* withOperations(h, (service) =>
        Effect.gen(function* () {
          const result = yield* service.submit({ request, wait_ms: 3000 });
          expect(result.state).toBe("committed");
          expect(yield* service.submit({ request, wait_ms: 0 })).toEqual(result);
          const operation = (yield* (yield* OperationStore.OperationStore).read(
            request.operation_id,
          ))!;
          const nativeExecutor = h.guardExecutor(yield* HomeExecutor.HomeExecutor);
          for (const invalid of [
            { ...operation, origin: { kind: "agent" as const } },
            { ...operation, thread_environment_id: "foreign-source" },
          ]) {
            const denied = yield* nativeExecutor
              .readReceipt(invalid, operation.child_command_ids[0]!)
              .pipe(Effect.flip);
            expect(denied.detail).toBe("Operation origin does not match source admission.");
          }
          const s = yield* h.registry.snapshot(caller);
          expect(
            [...s.state.memberships.values()].filter(
              (m) => m.closed === null && m.kind === "primary",
            ),
          ).toHaveLength(1);
          expect([...s.state.receipts.values()]).toHaveLength(0); // Only active effects are loaded into the decider snapshot.
          const status = yield* service.setupStatus();
          expect(status.registry).toMatchObject({
            state: "ready",
            registry_id: home.registry_instance_id,
            can_write: true,
          });
          expect(status.next_steps.some((x) => x.code === "enroll_native_source")).toBe(false);
          const backend = yield* Effect.gen(function* () {
            return yield* McpTools.WorkstreamToolsBackend;
          }).pipe(
            Effect.provide(
              McpHandlers.backendLayer.pipe(
                Layer.provide(Layer.succeed(Operations.WorkstreamOperations, service)),
                Layer.provide(Layer.succeed(Gateway.WorkstreamGateway, h.gateway)),
              ),
            ),
          );
          expect(
            (yield* backend.list({ limit: 100 })).items.some((x) => x.workstreamId === a),
          ).toBe(true);
        }),
      );
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "lost reply is adopted from the real home receipt and a destination mismatch never becomes absence",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        c = yield* h.command({
          operation: "create_workstream",
          name: "lost reply",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        }),
        body = canonicalJson(c);
      const accepted = yield* h.registry.submit(caller, body);
      const destination = {
        ...scope,
        home_environment_id: home.home_environment_id,
        home_generation: 1,
        principal_id: caller.principal_id,
        binding_version: 1,
      };
      const found = yield* h.homeTransport.lookup(destination, c.command_id);
      expect(found).toEqual({ kind: "present", source: "native", receipt: accepted.receipt });
      expect(yield* h.homeTransport.resubmit(destination, c.command_id, body)).toEqual({
        kind: "receipt",
        receipt: accepted.receipt,
      });
      const absent = yield* h.homeTransport.lookup(destination, "never-admitted-0000000");
      expect(absent.kind).toBe("proven_absent");
      const wrong = yield* h.homeTransport
        .lookup({ ...destination, home_generation: 2 }, "never-admitted-0000000")
        .pipe(Effect.flip);
      expect(wrong.reason).toBe("unknown-effect");
      expect((yield* h.registry.snapshot(caller)).state.workstreams.size).toBe(1);
    }).pipe(Effect.provide(runtime().layer)),
);

const scratchFile = () => {
  const dir = NodePath.resolve("node_modules/.cache/nw-j2b-conformance", NodeCrypto.randomUUID());
  NodeFS.mkdirSync(dir, { recursive: true });
  return NodePath.join(dir, "home.sqlite");
};

it.effect(
  "restart recovers a persisted pending verification and keeps exact terminal receipts encrypted in DB/WAL",
  () =>
    Effect.gen(function* () {
      const filename = yield* Effect.sync(scratchFile);
      let command: Wire.WorkstreamCommand | undefined;
      let original: Wire.WorkstreamReceipt | undefined;
      yield* Effect.gen(function* () {
        yield* seed;
        const h = yield* harness;
        const c = yield* h.command({
          operation: "create_workstream",
          name: "SENTINEL-S1-encrypted-workstream",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        command = c;
        original = (yield* h.registry.submit(caller, canonicalJson(c))).receipt;
        const ref = yield* h.register();
        const verify = yield* h.command({
          operation: "verify_reference",
          native_reference_id: ref,
          expected_attestation_version: 0,
        });
        const pending = yield* h.registry.submit(caller, canonicalJson(verify));
        expect(pending.receipt.state).toBe("pending");
        expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(1);
        for (const suffix of ["", "-wal"]) {
          if (NodeFS.existsSync(filename + suffix))
            expect(
              NodeFS.readFileSync(filename + suffix).includes(
                Buffer.from("SENTINEL-S1-encrypted-workstream"),
              ),
            ).toBe(false);
        }
      }).pipe(Effect.provide(runtime({ filename }).layer));
      yield* Effect.gen(function* () {
        yield* (yield* Runner.EffectRunner).recover();
        const h = yield* harness;
        const replay = yield* h.homeTransport.resubmit(
          {
            ...scope,
            home_environment_id: home.home_environment_id,
            home_generation: 1,
            principal_id: caller.principal_id,
            binding_version: 1,
          },
          command!.command_id,
          canonicalJson(command),
        );
        expect(replay).toEqual({ kind: "receipt", receipt: original });
        expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(0);
        expect(
          [...(yield* h.registry.snapshot(caller)).state.references.values()][0]!.registration
            .state,
        ).toBe("attested");
      }).pipe(Effect.provide(runtime({ filename }).layer));
      const keys = Fixture.secrets();
      keys.delete("workstreams-content-key-test-home");
      yield* Effect.gen(function* () {
        const result = yield* (yield* Receipts.ReceiptStore).lookup(scope, command!.command_id, 1);
        expect(result).toEqual({ kind: "unknown", reason: "key_unavailable" });
        const error = yield* (yield* Registry.Registry).snapshot(caller).pipe(Effect.flip);
        expect(error.code).toBe("key_unavailable");
      }).pipe(Effect.provide(runtime({ filename, keys }).layer));
    }),
);

it.effect(
  "secondary links, reattach, declarations and reciprocal edge cycles survive the transport",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("secondary a"),
        b = yield* h.create("secondary b"),
        ref = yield* h.register();
      yield* h.verify(ref);
      const primary = yield* h.committed({
        operation: "attach_primary",
        workstream_id: a,
        expected_version: 1,
        native_reference_id: ref,
      });
      yield* h.committed({
        operation: "link_secondary",
        workstream_id: b,
        expected_version: 1,
        native_reference_id: ref,
      });
      const membership = primary.effects.membership_ids[0]!;
      yield* h.committed({
        operation: "remove_membership",
        workstream_id: a,
        expected_version: 2,
        membership_id: membership,
      });
      yield* h.committed({
        operation: "reattach_primary",
        workstream_id: a,
        expected_version: 3,
        native_reference_id: ref,
      });
      const s = yield* h.registry.snapshot(caller),
        wa = s.state.workstreams.get(a)!,
        wb = s.state.workstreams.get(b)!;
      yield* h.committed({
        operation: "add_edge",
        from_workstream_id: a,
        to_workstream_id: b,
        expected_from_version: wa.version,
        expected_to_version: wb.version,
        relation: "continues_as",
      });
      const next = yield* h.registry.snapshot(caller);
      const cycle = yield* h.submit({
        operation: "add_edge",
        from_workstream_id: b,
        to_workstream_id: a,
        expected_from_version: next.state.workstreams.get(b)!.version,
        expected_to_version: next.state.workstreams.get(a)!.version,
        relation: "superseded_by",
      });
      expect(cycle.state).toBe("rejected");
      if (cycle.state === "rejected") expect(cycle.error.code).toBe("cycle_conflict");
      expect(
        (yield* h.transport.listMemberships({ ...headers, limit: 100, workstreamId: a })).items,
      ).toHaveLength(2);
      expect(
        (yield* h.transport.listEdges({ ...headers, limit: 100, workstreamId: b })).items,
      ).toHaveLength(1);
      const current = (yield* h.registry.snapshot(caller)).state.workstreams.get(a)!;
      const declaration = yield* h.committed({
        operation: "set_declaration",
        workstream_id: a,
        expected_version: current.version,
        declaration_id: null,
        expected_revision: 0,
        text: "Synthetic owner statement",
      });
      yield* h.committed({
        operation: "withdraw_declaration",
        workstream_id: a,
        expected_version: current.version + 1,
        declaration_id: declaration.effects.declaration_id!,
        expected_revision: 1,
      });
      expect(
        (yield* h.transport.listDeclarations({
          ...headers,
          limit: 100,
          workstreamId: a,
        })).items.map((x) => x.state),
      ).toEqual(["active", "withdrawn"]);
    }).pipe(Effect.provide(runtime().layer)),
);

class NativeTestApi extends HttpApi.make("environment").add(
  Wire.EnvironmentHttpApi.groups.workstreams,
) {}

it.effect(
  "authenticated HTTP raw commands and operations share the native home with Control Plane egress denied",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        local = yield* Local.LocalTransport,
        registrations = yield* Registration.Registration,
        registration = registrations.forCaller(caller),
        admission = yield* NativeAdmission.Admission.pipe(Effect.provide(NativeAdmission.layer));
      const dynamicEntry = NativeRuntimeEntry.layer.pipe(
        Layer.provide(Layer.succeed(NativeHome, home)),
        Layer.provide(Layer.succeed(Local.LocalTransport, local)),
        Layer.provide(Layer.succeed(Registration.Registration, registrations)),
        Layer.provide(Layer.succeed(NativeAdmission.Admission, admission)),
        Layer.provide(
          Layer.succeed(NativeAdmission.CallerBoundary, {
            current: () => Effect.succeed({ origin: caller.origin, check: () => Effect.void }),
          }),
        ),
      );
      const cp = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport").mockImplementation(() => {
            throw new Error("CP construction denied");
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const egress = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("Network denied")),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      yield* withOperations(h, (service) =>
        Effect.gen(function* () {
          const authenticate: EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"] =
            () =>
              Effect.succeed({
                sessionId: Wire.AuthSessionId.make("synthetic-native-http"),
                subject: "synthetic-native-owner",
                method: "bearer-access-token",
                scopes: [Wire.AuthOrchestrationReadScope, Wire.AuthOrchestrationOperateScope],
              });
          const auth = new Proxy(
            {},
            {
              get: (_target, key) => {
                if (key !== "authenticateHttpRequest")
                  throw new Error("Unexpected synthetic auth operation");
                return authenticate;
              },
            },
          ) as EnvironmentAuth.EnvironmentAuth["Service"];
          const routes = HttpApiBuilder.layer(NativeTestApi).pipe(
            Layer.provide(workstreamHttpApiLayer),
            Layer.provide(Layer.succeed(Gateway.WorkstreamGateway, h.gateway)),
            Layer.provide(dynamicEntry),
            Layer.provide(
              Layer.succeed(LegacyRegistration.WorkstreamsRegistrationContext, registration),
            ),
            Layer.provide(Layer.succeed(Operations.WorkstreamOperations, service)),
            Layer.provide(layerAuthenticatedAuth),
            Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, auth)),
            Layer.provide(workstreamResponseHeadersLayer),
            Layer.provideMerge(
              HttpPlatform.layer.pipe(
                Layer.provideMerge(NodeServices.layer),
                Layer.provideMerge(Etag.layerWeak),
              ),
            ),
            Layer.provide(NodeServices.layer),
          );
          const app = yield* Effect.acquireRelease(
            Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
            (app) => Effect.promise(() => app.dispose()),
          );
          const command = yield* h.command({
            operation: "create_workstream",
            name: "HTTP native raw",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 0,
          });
          const raw = yield* Effect.promise(() =>
            app.handler(
              new Request("http://synthetic/api/workstreams/commands", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ command }),
              }),
            ),
          );
          expect(raw.status).toBe(200);
          expect(raw.headers.get("cache-control")).toBe("private, no-store");
          const first = yield* Effect.promise(() => raw.json());
          yield* decodeReceipt(first);
          const receiptRequest = {
            contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
            command_id: command.command_id,
            expected_server_generation: 1,
          };
          const lookup = (payload: unknown) =>
            Effect.promise(() =>
              app.handler(
                new Request("http://synthetic/api/workstreams/legacy-receipts/v1/lookup", {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify(payload),
                }),
              ),
            );
          const found = yield* lookup(receiptRequest);
          expect(found.status).toBe(200);
          expect(found.headers.get("cache-control")).toBe("private, no-store");
          const decoded = yield* decodeLegacyReceiptLookupResult(
            yield* Effect.promise(() => found.json()),
          );
          expect(decoded.result).toEqual({ kind: "present", source: "native", receipt: first });
          expect((yield* lookup({ ...receiptRequest, principal_id: "forged" })).status).toBe(400);
          const mismatch = yield* lookup({ ...receiptRequest, expected_server_generation: 2 });
          expect(yield* Effect.promise(() => mismatch.json())).toMatchObject({
            result: { kind: "unknown", reason: "generation_mismatch" },
          });
          const request = {
            contract: "workstream-operations/1.0.0",
            operation_id: "http-operation-000000001",
            origin: { kind: "app" },
            kind: "create_workstream",
            registry: { registry_id: home.registry_instance_id, owner_id: home.owner_id },
            workstream: {
              name: "HTTP native operation",
              lifecycle: "active",
              progress: { state: "unknown" },
              sort_order: 1,
            },
          };
          const operation = yield* Effect.promise(() =>
            app.handler(
              new Request("http://synthetic/api/workstreams/operations/submit", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ request, wait_ms: 3000 }),
              }),
            ),
          );
          expect(operation.status).toBe(200);
          const result = yield* Effect.promise(() => operation.json());
          expect(result).toMatchObject({ state: "committed" });
          const list = yield* Effect.promise(() =>
            app.handler(new Request("http://synthetic/api/workstreams?limit=100")),
          );
          expect(list.status).toBe(200);
          const payload = yield* decodeListResult(yield* Effect.promise(() => list.json()));
          expect(payload.items.map((x) => x.name).sort()).toEqual([
            "HTTP native operation",
            "HTTP native raw",
          ]);
          const grant = (yield* h.registry.snapshot(caller)).authorization.grant;
          yield* (yield* Store.RegistryStore).appendPlain(scope, "grants", {
            principal_id: caller.principal_id,
            grant_version: grant.grant_version + 1,
            grant_id: grant.grant_id,
            permissions: JSON.stringify(grant.permissions),
            state: "revoked",
          });
          expect((yield* lookup(receiptRequest)).status).toBe(409);
          expect(cp).not.toHaveBeenCalled();
          expect(egress).not.toHaveBeenCalled();
        }),
      );
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "placement v2 negotiation is explicit; unsupported contracts and historical unproved references fail closed",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        a = yield* h.create("proof"),
        r = yield* h.register();
      yield* h.verify(r);
      yield* h.committed({
        operation: "attach_primary",
        workstream_id: a,
        expected_version: 1,
        native_reference_id: r,
      });
      const input = {
        identities: [
          { source_instance_id: binding.source_instance_id, native_thread_id: "thread-synthetic" },
        ],
      };
      const native = yield* h.placementResponse(input, "workstreams-t3-placement/2.0.0");
      yield* decodeNativePlacementResult(native, { onExcessProperty: "error" });
      const old = yield* h.placementResponse(input);
      expect(old.page).not.toHaveProperty("provenance");
      expect(old.page.items[0]).not.toHaveProperty("attestation_source");
      const unsupported = yield* h
        .placementResponse(input, "workstreams-t3-placement/3.0.0")
        .pipe(Effect.flip);
      expect(unsupported.code).toBe("contract_mismatch");
      const store = yield* Store.RegistryStore,
        stored = (yield* store.list(scope, "references"))[0]!;
      const ref = (yield* h.registry.snapshot(caller)).state.references.get(r)!;
      yield* store.put(
        scope,
        "references",
        {
          metadata: { ...stored.metadata, attestation_version: 2 },
          value: { ...ref, registration: { ...ref.registration, attestation_version: 2 } },
        },
        1,
      );
      const historical = yield* h
        .placementResponse(input, "workstreams-t3-placement/2.0.0")
        .pipe(Effect.flip);
      expect(historical.code).toBe("registration_unverified");
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "source caller limits deny verification without inventing attestation or falling back",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness;
      const registered = yield* h.committed({
        operation: "register_reference",
        identity: {
          provider: "t3",
          source_instance_id: binding.source_instance_id,
          resource_kind: "thread",
          id_kind: "internal",
          native_id: "thread-outside-caller-limits",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: null,
      });
      const id = registered.effects.native_reference_id!;
      const error = yield* h
        .submit({
          operation: "verify_reference",
          native_reference_id: id,
          expected_attestation_version: 0,
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({ reason: "unknown-effect" });
      const s = yield* h.registry.snapshot(caller);
      expect(s.state.references.get(id)?.registration.state).toBe("quarantined");
      expect(s.state.memberships.size).toBe(0);
      expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(1);
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "archived command IDs replay without execution and fenced imports never prove absence",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        archive = yield* Archive.ArchiveStore,
        store = yield* Store.RegistryStore;
      const command = yield* h.command({
        operation: "create_workstream",
        name: "legacy reserved",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const body = canonicalJson(command);
      const template = Fixture.terminal(Fixture.pending(command.command_id, body)).receipt;
      const receipt = {
        ...template,
        owner_id: home.owner_id,
        actor: { principal_id: caller.principal_id },
        operation: command.action.operation,
      };
      yield* archive.startImport(scope, {
        import_run_id: "synthetic-archive-run",
        snapshot_id: "synthetic-archive-snapshot",
        snapshot_sha256: Fixture.hash,
        producer: "control-plane",
        contract_version: "1.0.0",
        manifest: {
          registry: { registry_instance_id: home.registry_instance_id },
          owners: [{ owner_id: home.owner_id }],
        },
      });
      yield* archive.addReceipt(
        scope,
        "synthetic-archive-run",
        receipt,
        Fixture.hash,
        Fixture.hash,
      );
      expect((yield* h.registry.submit(caller, body)).receipt).toEqual(receipt);
      expect((yield* h.registry.snapshot(caller)).state.workstreams.size).toBe(0);
      const conflict = yield* h.registry.submit(caller, " " + body).pipe(Effect.flip);
      expect(conflict.code).toBe("idempotency_conflict");
      const context = (yield* store.context(scope))!;
      yield* store.advance(scope, context, { state: "importing" });
      expect(
        yield* (yield* Receipts.ReceiptStore).lookup(scope, "never-recorded-000000", 1),
      ).toEqual({ kind: "unknown", reason: "import_incomplete" });
      const denied = yield* h.registry.submit(caller, body).pipe(Effect.flip);
      expect(denied.code).toBe("import_incomplete");
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect("explicit init verifies chosen keys and never creates a source enrollment", () =>
  Effect.gen(function* () {
    yield* migration;
    const admin = yield* Admin.NativeAdmin;
    const selection = {
      registry: {
        registry_instance_id: home.registry_instance_id,
        owner_id: home.owner_id,
        canonical_origin: home.canonical_origin,
        home_environment_id: home.home_environment_id,
        server_generation: 1,
      },
      principal_id: caller.principal_id,
      content_key_id: "test-home",
      local_key_id: "test-local",
    };
    const missing = yield* admin
      .initialize({ ...selection, content_key_id: "missing-home" })
      .pipe(Effect.flip);
    expect(missing.code).toBe("key_unavailable");
    expect(yield* (yield* Store.RegistryStore).context(selection.registry)).toBeNull();
    const context = yield* admin.initialize(selection);
    expect(context.registry_version).toBe(1);
    expect(yield* (yield* Store.RegistryStore).listPlain(scope, "source_bindings")).toEqual([]);
    const again = yield* admin.initialize(selection).pipe(Effect.flip);
    expect(again.code).toBe("already_initialized");
  }).pipe(Effect.provide(Admin.layer.pipe(Layer.provideMerge(Fixture.testLayer())))),
);

it.effect(
  "native settlement uses source lookup, one admitted dispatch and real provider result evidence",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const h = yield* harness,
        ref = yield* h.register();
      yield* h.verify(ref);
      const command = yield* h.command({
        operation: "request_native_t3_settlement",
        native_reference_id: ref,
        expected_attestation_version: 1,
        native_action: "settle",
      });
      const receipt = yield* h.gateway.submit(command);
      expect(receipt.state).toBe("committed");
      if (receipt.state === "committed")
        expect(receipt.effects.native_settlement?.outcome).toBe("committed");
      const body = canonicalJson(command);
      expect(
        yield* h.homeTransport.resubmit(
          {
            ...scope,
            home_environment_id: home.home_environment_id,
            home_generation: 1,
            principal_id: caller.principal_id,
            binding_version: 1,
          },
          command.command_id,
          body,
        ),
      ).toEqual({ kind: "receipt", receipt });
      expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(0);
    }).pipe(Effect.provide(runtime().layer)),
);

it.effect(
  "configured synthetic GitHub verification and observation use the same encrypted registry",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(Fixture.now));
      yield* seed;
      const h = yield* harness,
        store = yield* Store.RegistryStore,
        a = yield* h.create("synthetic PR");
      yield* store.appendPlain(scope, "source_bindings", {
        provider: "github",
        source_instance_id: "github-synthetic",
        binding_version: 1,
        authority_namespace: "github-authority",
        store_generation: 1,
        state: "active",
      });
      const ref = (yield* h.committed({
        operation: "register_reference",
        identity: {
          provider: "github",
          source_instance_id: "github-synthetic",
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: "jones/example#7",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: {
          host: "github.com",
          repository_owner: "jones",
          repository_name: "example",
          number: 7,
        },
      })).effects.native_reference_id!;
      yield* h.verify(ref);
      const membership = (yield* h.committed({
        operation: "link_secondary",
        workstream_id: a,
        expected_version: 1,
        native_reference_id: ref,
      })).effects.membership_ids[0]!;
      yield* h.committed({
        operation: "refresh_linked_pr",
        workstream_id: a,
        expected_version: 2,
        membership_id: membership,
        expected_observation_version: 0,
      });
      expect((yield* h.gateway.readReference(ref)).reference.registration.state).toBe("attested");
      expect((yield* h.gateway.readReference(ref)).latest_observation?.outcome).toBe("observed");
    }).pipe(Effect.provide(runtime({ github: true }).layer)),
);

it.effect.each([
  {
    retainedResult: false,
    name: "native outcome recovery completes after an unrelated registry advance using committed source lookup",
  },
  {
    retainedResult: true,
    name: "restart reconciles the retained native result after an unrelated registry advance and lost reply",
  },
])("$name", ({ retainedResult }) =>
  Effect.gen(function* () {
    const cp = yield* Effect.acquireRelease(
      Effect.sync(() =>
        vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport").mockImplementation(() => {
          throw new Error("CP construction denied");
        }),
      ),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
    const egress = yield* Effect.acquireRelease(
      Effect.sync(() =>
        vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("Network denied")),
      ),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
    const filename = yield* Effect.sync(scratchFile);
    const sourceRuntime = runtime({ filename });
    let command!: Wire.WorkstreamCommand;
    let intent!: ExternalIntent;
    let result!: RegistryExternalResult;
    let terminal!: Wire.WorkstreamReceipt;
    let body = "";
    let ref = "";
    let currentVersion = 0;
    yield* Effect.gen(function* () {
      yield* seed;
      const h = yield* harness;
      ref = yield* h.register();
      yield* h.verify(ref);
      command = yield* h.command({
        operation: "request_native_t3_settlement",
        native_reference_id: ref,
        expected_attestation_version: 1,
        native_action: "settle",
      });
      body = " \n" + JSON.stringify(command, null, 2) + "\n";
      const accepted = yield* h.registry.submit(caller, body);
      expect(accepted.receipt.state).toBe("pending");
      expect(accepted.intent?.kind).toBe("native_t3_settlement");
      intent = accepted.intent!;
      const unretained = yield* h.registry
        .complete(intent, nativeSettlementResult(intent, "committed"), true)
        .pipe(Effect.flip);
      expect(unretained.code).toBe("unknown_effect");
      expect(yield* (yield* Effects.EffectStore).result(scope, command.command_id)).toBeNull();
      expect(sourceRuntime.fixture.calls).toHaveLength(0);
      yield* h.create("SENTINEL-retained-native-unrelated-advance");
      currentVersion = (yield* h.registry.snapshot(caller)).context.registry_version;
      const request = yield* decodeSettlementRequest({
        owner_id: intent.owner_id,
        principal_id: intent.actor.principal_id,
        command_id: command.command_id,
        native_reference_id: ref,
        native_action: "settle",
        request_sha256: intent.request_sha256,
        reservation_sha256: NodeCrypto.createHash("sha256")
          .update(canonicalJson(intent))
          .digest("hex"),
        identity: intent.reference.identity,
        expected_authority_namespace: intent.source_binding.authority_namespace,
        expected_store_generation: intent.source_binding.store_generation,
      });
      const outcome = yield* (yield* Settlement.SourceSettlement).settle({
        expected_home: {
          registry_instance_id: home.registry_instance_id,
          owner_id: home.owner_id,
          home_environment_id: home.home_environment_id,
          server_generation: home.server_generation,
        },
        request,
      });
      expect(outcome.state).toBe("terminal");
      if (outcome.state !== "terminal") throw new Error("Expected committed source outcome");
      result = outcome.result;
      const effects = yield* Effects.EffectStore;
      expect(yield* effects.result(scope, command.command_id)).toBeNull();
      if (retainedResult) yield* effects.saveResult(scope, command.command_id, result);
      expect(yield* effects.active(scope)).toHaveLength(1);
      expect(yield* h.registry.lookup(caller, command.command_id, 1)).toMatchObject({
        kind: "present",
        receipt: { state: "pending" },
      });
      expect(sourceRuntime.fixture.calls).toHaveLength(1);
    }).pipe(Effect.provide(sourceRuntime.layer));
    yield* Effect.gen(function* () {
      const source = yield* Settlement.SourceSettlement;
      const lookup = yield* Effect.acquireRelease(
        Effect.sync(() => vi.spyOn(source, "lookup")),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const dispatch = yield* Effect.acquireRelease(
        Effect.sync(() => vi.spyOn(source, "settle")),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      yield* (yield* Runner.EffectRunner).recover();
      const h = yield* harness;
      const lookupResult = yield* h.registry.lookup(caller, command.command_id, 1);
      expect(lookupResult.kind).toBe("present");
      if (lookupResult.kind !== "present") throw new Error("Expected durable terminal receipt");
      terminal = lookupResult.receipt;
      expect(terminal).toMatchObject({
        state: "committed",
        registry_version: currentVersion + 1,
        server_generation: 1,
        changed: true,
        request_sha256: intent.request_sha256,
        actor: intent.actor,
        effects: { native_settlement: { outcome: "committed", native_reference_id: ref } },
      });
      const effects = yield* Effects.EffectStore;
      expect(yield* effects.result(scope, command.command_id)).toEqual(result);
      expect(yield* effects.active(scope)).toHaveLength(0);
      expect(
        (yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id))?.request_body,
      ).toBe(body);
      expect(
        (yield* h.registry.snapshot(caller)).state.references.get(ref)?.registration.state,
      ).toBe("attested");
      const history = yield* History.HistoryStore;
      const before = yield* history.page(scope, { limit: 100 });
      expect(before.events.filter((e) => e.command_id === command.command_id)).toMatchObject([
        {
          registry_version: currentVersion + 1,
          value: {
            actor: intent.actor,
            operation: "request_native_t3_settlement",
            changed: true,
          },
        },
      ]);
      yield* (yield* Runner.EffectRunner).recover();
      expect(yield* h.registry.complete(intent, result, true)).toEqual(terminal);
      expect(yield* h.registry.submit(caller, body)).toEqual({
        receipt: terminal,
        intent: null,
      });
      expect(yield* history.page(scope, { limit: 100 })).toEqual(before);
      expect(lookup).toHaveBeenCalledTimes(retainedResult ? 0 : 1);
      expect(dispatch).not.toHaveBeenCalled();
      expect(sourceRuntime.fixture.calls).toHaveLength(1);
      for (const suffix of ["", "-wal"])
        if (NodeFS.existsSync(filename + suffix)) {
          const bytes = NodeFS.readFileSync(filename + suffix);
          for (const plaintext of [
            body,
            canonicalJson(intent),
            canonicalJson(result),
            "SENTINEL-retained-native-unrelated-advance",
          ])
            expect(bytes.includes(Buffer.from(plaintext))).toBe(false);
        }
    }).pipe(Effect.provide(sourceRuntime.layer));
    const restarted = runtime({ filename });
    yield* Effect.gen(function* () {
      yield* (yield* Runner.EffectRunner).recover();
      const h = yield* harness;
      expect(yield* h.registry.submit(caller, body)).toEqual({
        receipt: terminal,
        intent: null,
      });
      expect((yield* h.registry.snapshot(caller)).context.registry_version).toBe(
        currentVersion + 1,
      );
      expect(
        (yield* (yield* History.HistoryStore).page(scope, { limit: 100 })).events.filter(
          (e) => e.command_id === command.command_id,
        ),
      ).toHaveLength(1);
      expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(0);
      expect(restarted.fixture.calls).toHaveLength(0);
    }).pipe(Effect.provide(restarted.layer));
    expect(cp).not.toHaveBeenCalled();
    expect(egress).not.toHaveBeenCalled();
  }).pipe(Effect.scoped),
);
