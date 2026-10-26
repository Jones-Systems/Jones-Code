import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Fixture from "../store/sql.testkit.ts";
import * as Registry from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Bindings from "./bindings.ts";
import * as Manifests from "./manifestSource.ts";
import * as Application from "./application.ts";
import * as Store from "./store.ts";
import * as Evidence from "./evidence.ts";
import { syntheticFixture, syntheticEvidenceLayer, syntheticGenerationLayer } from "./synthetic.ts";

const f = syntheticFixture();
const v2 = {
  ...f,
  binding: {
    ...f.binding,
    binding_version: 2,
    selection: { kind: "project" as const, project_id: "project:v2", project_version: 2 },
  },
  evidence: { ...f.evidence, source_set_sha256: Evidence.digest("selection:v2") },
};
const home = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const WriterDepth = Context.Reference<number>("test/rollup-discovery/writer-depth", {
  defaultValue: () => 0,
});
function layers(
  options: {
    afterList?: (depth: number) => Effect.Effect<void, Evidence.RollupError>;
    evidence?: Layer.Layer<Evidence.RollupEvidenceProvider>;
  } = {},
) {
  const foundation = Layer.mergeAll(Fixture.testLayer(), Layer.succeed(NativeHome, home));
  // Observe the real transaction boundary; all SQL, crypto and CAS operations remain real.
  const observedDb = Layer.effect(
    NativeSql.NativeSql,
    Effect.gen(function* () {
      const db = yield* NativeSql.NativeSql;
      const transaction: NativeSql.NativeSql["Service"]["transaction"] = (scope, effect) =>
        Effect.gen(function* () {
          const depth = yield* WriterDepth;
          return yield* db.transaction(
            scope,
            effect.pipe(Effect.provideService(WriterDepth, depth + 1)),
          );
        });
      return { ...db, transaction };
    }),
  ).pipe(Layer.provideMerge(foundation));
  const bindings = Bindings.layer.pipe(Layer.provideMerge(observedDb));
  const observedBindings = Layer.effect(
    Bindings.Bindings,
    Effect.gen(function* () {
      const b = yield* Bindings.Bindings;
      return {
        ...b,
        list: () =>
          Effect.gen(function* () {
            const result = yield* b.list();
            const depth = yield* WriterDepth;
            yield* options.afterList?.(depth) ?? Effect.void;
            return result;
          }),
      };
    }),
  ).pipe(Layer.provideMerge(bindings));
  const ports = Layer.mergeAll(
    options.evidence ?? syntheticEvidenceLayer([f, v2]),
    syntheticGenerationLayer(f.identity, f.result),
  );
  const base = Layer.mergeAll(observedBindings, ports);
  const services = Layer.mergeAll(
    Manifests.layer.pipe(Layer.provideMerge(base)),
    Store.layer.pipe(Layer.provideMerge(base)),
  );
  return Application.layer.pipe(
    Layer.provideMerge(services),
    Layer.provideMerge(Application.admissionLayer),
  );
}
const seed = Effect.gen(function* () {
  yield* Fixture.seed;
  yield* TestClock.setTime(Date.parse(Fixture.now));
  const r = yield* Registry.RegistryStore;
  yield* r.appendPlain(Fixture.scope, "principals", {
    principal_id: "principal:test",
    environment_id: "home:test",
    kind: "environment",
    provenance: '{"source":"synthetic"}',
  });
  yield* r.appendPlain(Fixture.scope, "grants", {
    principal_id: "principal:test",
    grant_version: 1,
    grant_id: "grant:test",
    permissions: '["workstreams:read","workstreams:write"]',
    state: "active",
  });
  yield* r.put(
    Fixture.scope,
    "records",
    {
      metadata: { workstream_id: "ws:test", version: 1, sort_order: 0 },
      value: { synthetic: true },
    },
    null,
  );
  yield* r.put(
    Fixture.scope,
    "references",
    {
      metadata: {
        native_reference_id: "ref:test",
        provider: "synthetic",
        resource_kind: "thread",
        id_kind: "internal",
        identity_index: Fixture.hash,
        legacy_identity_sha256: null,
        attestation_version: 1,
        registration_state: "attested",
        created_by_principal: "principal:test",
        routing_source_instance_id: null,
        routing_native_id: null,
        routing_authority_namespace: null,
        routing_store_generation: null,
        routing_attested_at: null,
        routing_expires_at: null,
        routing_evidence_sha256: null,
      },
      value: { synthetic: true },
    },
    null,
  );
  yield* r.put(
    Fixture.scope,
    "memberships",
    {
      metadata: {
        membership_id: "membership:test",
        workstream_id: "ws:test",
        native_reference_id: "ref:test",
        kind: "primary",
        opened_registry_version: 1,
        closed_registry_version: null,
        closed_at: null,
      },
      value: {
        membership_id: "membership:test",
        workstream_id: "ws:test",
        native_reference_id: "ref:test",
        kind: "primary",
        opened: {
          at: Fixture.now,
          actor: { principal_id: "principal:test" },
          command_id: "command:synthetic-0001",
          registry_version: 1,
        },
        closed: null,
      },
    },
    null,
  );
  yield* (yield* Bindings.Bindings).put(f.binding, null);
});

it.effect("never stamps a cached v1 selection with the context committed by a v2 edit", () =>
  Effect.gen(function* () {
    const captured = yield* Deferred.make<number>(),
      release = yield* Deferred.make<void>(),
      edited = yield* Deferred.make<void>();
    let armed = false;
    const inputReads: string[] = [],
      described: number[] = [];
    const evidence = Layer.effect(
      Evidence.RollupEvidenceProvider,
      Effect.map(Evidence.RollupEvidenceProvider, (provider) => ({
        ...provider,
        describe: (bindings: readonly Bindings.RollupBinding[]) =>
          Effect.gen(function* () {
            described.push(...bindings.map((b) => b.binding_version));
            yield* Deferred.await(edited);
            return yield* provider.describe(bindings);
          }),
        readExtractionInputs: (manifest: Evidence.Manifest, signal: AbortSignal) => {
          inputReads.push(manifest.sources[0]!.source_set_sha256);
          return provider.readExtractionInputs(manifest, signal);
        },
      })),
    ).pipe(Layer.provide(syntheticEvidenceLayer([f, v2])));
    yield* Effect.gen(function* () {
      yield* seed;
      armed = true;
      const app = yield* Application.RollupApplication;
      const discovery = yield* app.processOnce().pipe(Effect.forkChild);
      const depth = yield* Deferred.await(captured);
      const b = yield* Bindings.Bindings;
      const edit = yield* b
        .put(v2.binding, 1)
        .pipe(Effect.andThen(Deferred.succeed(edited, undefined)), Effect.forkChild);
      // Before repair the list has already committed, so commit the edit before context is read.
      // With the snapshot writer held, release it and let describe await that same real edit.
      if (depth === 0) yield* Fiber.join(edit);
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(discovery);
      yield* Fiber.join(edit);
      const published = yield* (yield* Store.RollupStore).latestMetadata("ws:test");
      expect({
        inputReads,
        status: result.status,
        discovered: result.discovered,
        published,
      }).toEqual({
        inputReads: [],
        status: "idle",
        discovered: 0,
        published: null,
      });
      const fresh = yield* app.processOnce();
      expect(fresh.status).toBe("published");
      expect(inputReads).toEqual([v2.evidence.source_set_sha256]);
      expect(described).toEqual([1, 2]);
      expect(
        (yield* (yield* Registry.RegistryStore).context(Fixture.scope))?.registry_version,
      ).toBe(3);
    }).pipe(
      Effect.provide(
        layers({
          evidence,
          afterList: (depth) =>
            Effect.gen(function* () {
              if (!armed) return;
              armed = false;
              yield* Deferred.succeed(captured, depth);
              yield* Deferred.await(release);
            }),
        }),
      ),
      Effect.ensuring(Deferred.succeed(release, undefined)),
    );
  }),
);

it.effect("allows binding writes while external describe waits and fences its stale result", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>(),
      finish = yield* Deferred.make<void>();
    const evidence = Layer.effect(
      Evidence.RollupEvidenceProvider,
      Effect.map(Evidence.RollupEvidenceProvider, (provider) => ({
        ...provider,
        describe: (bindings: readonly Bindings.RollupBinding[]) =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
            Effect.andThen(provider.describe(bindings)),
          ),
      })),
    ).pipe(Layer.provide(syntheticEvidenceLayer([f, v2])));
    yield* Effect.gen(function* () {
      yield* seed;
      const source = yield* Manifests.ManifestSource;
      const discovery = yield* source.discoverDirtyManifests(1, Fixture.now).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* (yield* Bindings.Bindings).put(v2.binding, 1);
      yield* Deferred.succeed(finish, undefined);
      expect((yield* Fiber.join(discovery)).manifests).toEqual([]);
      const fresh = yield* source.discoverDirtyManifests(1, Fixture.now);
      expect(fresh.manifests[0]?.registry_version).toBe(3);
      expect(fresh.manifests[0]?.sources[0]?.source_set_sha256).toBe(v2.evidence.source_set_sha256);
    }).pipe(
      Effect.provide(layers({ evidence })),
      Effect.ensuring(Deferred.succeed(finish, undefined)),
    );
  }),
);

it.effect(
  "rolls back cursor selection on snapshot failure and resumes bounded durable fairness",
  () => {
    return Effect.gen(function* () {
      yield* seed;
      const b = yield* Bindings.Bindings;
      yield* b.put({ ...f.binding, workstream_id: "ws:zz", native_reference_id: "ref:zz" }, null);
      const source = yield* Manifests.ManifestSource,
        sql = yield* SqlClient.SqlClient;
      const first = yield* source.discoverDirtyManifests(1, Fixture.now);
      expect(first.manifests.map((m) => m.workstream_id)).toEqual(["ws:test"]);
      const before = yield* sql`SELECT * FROM jones_workstreams_rollup_discovery_cursors`;
      const db = yield* NativeSql.NativeSql;
      const failed = yield* Effect.exit(
        db.transaction(
          Fixture.scope,
          source
            .discoverDirtyManifests(1, Fixture.now)
            .pipe(Effect.andThen(Effect.fail("synthetic rollback"))),
        ),
      );
      expect(Exit.isFailure(failed)).toBe(true);
      expect(yield* sql`SELECT * FROM jones_workstreams_rollup_discovery_cursors`).toEqual(before);
      expect((yield* source.discoverDirtyManifests(1, Fixture.now)).manifests).toEqual([]);
      expect(
        (yield* sql`SELECT last_group_key FROM jones_workstreams_rollup_discovery_cursors`)[0]
          ?.last_group_key,
      ).toBe("ws:zz");
      expect((yield* source.discoverDirtyManifests(1, Fixture.now)).manifests).toHaveLength(1);
    }).pipe(Effect.provide(layers()));
  },
);
