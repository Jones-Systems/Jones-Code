// @effect-diagnostics nodeBuiltinImport:off -- Each disk fixture owns one unique task-evidence root, retained through parent delivery.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Fixture from "../store/sql.testkit.ts";
import * as Registry from "../store/registryStore.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as NativeSql from "../store/sql.ts";
import * as Session from "../registry/session.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Bindings from "./bindings.ts";
import * as Store from "./store.ts";
import * as Evidence from "./evidence.ts";
import * as Generation from "./generation.ts";
import * as Manifests from "./manifestSource.ts";
import * as Application from "./application.ts";
import * as SummaryRead from "./summaryRead.ts";
import { syntheticFixture, syntheticEvidenceLayer, syntheticGenerationLayer } from "./synthetic.ts";
const fixture = syntheticFixture(),
  f = {
    ...fixture,
    manifest: Generation.createManifest({ ...fixture.manifest, registry_version: 2 }),
  },
  now = Fixture.now;
const home = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const base = Fixture.scope;
function layers(
  options: {
    filename?: string;
    keys?: Map<string, Uint8Array>;
    authorized?: () => boolean;
    generation?: Layer.Layer<Generation.RollupGenerationProvider>;
    evidence?: Layer.Layer<Evidence.RollupEvidenceProvider>;
    readObserver?: () => void;
    authorizer?: SummaryRead.SummaryAuthorizer["Service"];
  } = {},
) {
  const ports = Layer.mergeAll(
    Layer.succeed(NativeHome, home),
    options.evidence ?? syntheticEvidenceLayer([{ ...f, authorized: options.authorized }]),
    options.generation ?? syntheticGenerationLayer(f.identity, f.result),
  );
  const foundation = Layer.mergeAll(Fixture.testLayer(options), ports);
  const binding = Bindings.layer.pipe(Layer.provideMerge(foundation));
  const store = Store.layer.pipe(Layer.provideMerge(binding));
  const manifests = Manifests.layer.pipe(Layer.provideMerge(binding));
  const session = Session.layer.pipe(Layer.provideMerge(foundation));
  const observedStore = Layer.effect(
    Store.RollupStore,
    Effect.map(Store.RollupStore, (s) => ({
      ...s,
      readSummary: (m: Store.SummaryMetadata) =>
        Effect.suspend(() => {
          options.readObserver?.();
          return s.readSummary(m);
        }),
    })),
  ).pipe(Layer.provideMerge(store));
  const summaries = SummaryRead.layer.pipe(
    Layer.provideMerge(observedStore),
    Layer.provideMerge(session),
    Layer.provideMerge(
      Layer.succeed(
        SummaryRead.SummaryAuthorizer,
        options.authorizer ?? {
          authorize: (p, o, w, accounts) =>
            Effect.succeed(
              accounts.map((a) => ({
                principal_id: p,
                owner_id: o,
                workstream_id: w,
                account_id: a,
                decision_id: "summary:test",
                authorization_revision: 1,
                allowed: true,
              })),
            ),
        },
      ),
    ),
  );
  return Layer.mergeAll(
    summaries,
    Application.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(store, manifests)),
      Layer.provideMerge(Application.admissionLayer),
    ),
  );
}
const seed = Effect.gen(function* () {
  yield* Fixture.seed;
  // Isolated projection of the additive parent migration; integrated migration qualification remains parent-owned.
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ name: string }>`PRAGMA table_info(jones_workstreams_rollup_queue)`;
  if (!columns.some((column) => column.name === "attempt_count"))
    yield* sql`ALTER TABLE jones_workstreams_rollup_queue ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0)`;
  yield* TestClock.setTime(Date.parse(now));
  const r = yield* Registry.RegistryStore;
  yield* r.appendPlain(base, "principals", {
    principal_id: caller.principal_id,
    environment_id: caller.source_environment_id,
    kind: "environment",
    provenance: '{"source":"synthetic"}',
  });
  yield* r.appendPlain(base, "grants", {
    principal_id: caller.principal_id,
    grant_version: 1,
    grant_id: "grant:test",
    permissions: '["workstreams:read","workstreams:write"]',
    state: "active",
  });
  yield* (yield* BindingStore.BindingStore).bind(Fixture.binding, null);
  yield* r.put(
    base,
    "records",
    {
      metadata: { workstream_id: "ws:test", version: 1, sort_order: 0 },
      value: {
        workstream_id: "ws:test",
        owner_id: base.owner_id,
        name: "Synthetic workstream",
        lifecycle: "active",
        progress: { state: "progressing" },
        delivery: "none-observed",
        freshness: "current",
        sort_order: 0,
        version: 1,
        created_at: now,
        updated_at: now,
        created_by: { principal_id: caller.principal_id },
        updated_by: { principal_id: caller.principal_id },
      },
    },
    null,
  );
  yield* (yield* Bindings.Bindings).put(f.binding, null);
  yield* r.put(
    base,
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
        created_by_principal: caller.principal_id,
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
    base,
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
          at: now,
          actor: { principal_id: caller.principal_id },
          command_id: "command:synthetic-0001",
          registry_version: 1,
        },
        closed: null,
      },
    },
    null,
  );
});
const publish = Effect.gen(function* () {
  yield* seed;
  const result = yield* (yield* Application.RollupApplication).processOnce();
  expect(result.status).toBe("published");
  return yield* (yield* Store.RollupStore).latestMetadata("ws:test");
});
it.effect("discovers, leases, publishes and reads exact account-authorized native summaries", () =>
  Effect.gen(function* () {
    const metadata = yield* publish;
    expect(metadata?.account_ids).toEqual(["account:test"]);
    const reader = yield* SummaryRead.SummaryRead;
    expect(yield* reader.read(caller, "ws:test")).toMatchObject({
      state: "ready",
      freshness: "current",
      summary: { rollup_version: 1, output_sha256: f.input.output_sha256 },
    });
    expect((yield* reader.view(caller, "ws:test")).coverage).toBe("complete");
    expect((yield* (yield* Application.RollupApplication).processOnce()).status).toBe("idle");
    const rows =
      yield* (yield* SqlClient.SqlClient)`SELECT state,failure_code FROM jones_workstreams_rollup_queue`;
    expect(rows).toEqual([{ state: "dead_letter", failure_code: "published" }]);
  }).pipe(Effect.provide(layers())),
);
it.effect(
  "coalesces competing discoveries and claims once; changed input fences the old claim",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const store = yield* Store.RollupStore;
      const enqueued = yield* Effect.all(
        [store.enqueue(f.manifest, now), store.enqueue(f.manifest, now)],
        { concurrency: 2 },
      );
      expect(enqueued.toSorted()).toEqual(["coalesced", "enqueued"]);
      const claims = yield* Effect.all([store.claim(now, 60000), store.claim(now, 60000)], {
        concurrency: 2,
      });
      expect(claims.filter(Boolean)).toHaveLength(1);
      const claim = claims.find((c) => c !== null)!;
      const changed = Generation.createManifest({ ...f.manifest, registry_version: 3 });
      expect(yield* store.enqueue(changed, now)).toBe("coalesced");
      const old = Generation.createSummary(f.manifest, {
        workstream_id: "ws:test",
        rollup_version: 1,
        manifest_sha256: f.manifest.manifest_sha256,
        generated_at: now,
        result: f.result,
      });
      expect(Exit.isFailure(yield* Effect.exit(store.completeClaim(claim, old, now)))).toBe(true);
      expect((yield* store.claim(now, 60000))?.fence).toBe(2);
    }).pipe(Effect.provide(layers())),
);
it.effect(
  "expires the exact lease, deadletters failures, and limits retries until publication",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const store = yield* Store.RollupStore;
      yield* store.enqueue(f.manifest, now);
      const c = (yield* store.claim(now, 1000))!;
      expect(yield* store.expireClaims("2026-01-01T00:00:01.000Z")).toBe(1);
      expect(Exit.isFailure(yield* Effect.exit(store.failClaim(c, "provider_failure", now)))).toBe(
        true,
      );
      yield* store.enqueue(f.manifest, now);
      expect(yield* store.claim(now, 1000)).toBeNull();
      const next = Generation.createManifest({ ...f.manifest, registry_version: 3 });
      yield* store.enqueue(next, now);
      const c2 = (yield* store.claim(now, 1000))!;
      yield* store.failClaim(c2, "input_unavailable", now);
      yield* store.enqueue(next, now);
      const c3 = (yield* store.claim(now, 1000))!;
      expect(c3.fence).toBe(3);
      yield* store.failClaim(c3, "input_unavailable", now);
      yield* store.enqueue(next, now);
      expect(yield* store.claim(now, 1000)).toBeNull();
    }).pipe(Effect.provide(layers())),
);
it.effect("preserves publication failure after account revocation during generation", () => {
  let authorized = true;
  const generation = Layer.succeed(Generation.RollupGenerationProvider, {
    configured: true,
    identity: f.identity,
    generate: () =>
      Effect.sync(() => {
        authorized = false;
        return f.result;
      }),
  });
  return Effect.gen(function* () {
    yield* seed;
    const result = yield* (yield* Application.RollupApplication).processOnce();
    expect(result.status).toBe("provider_failed");
    expect(yield* (yield* Store.RollupStore).latestMetadata("ws:test")).toBeNull();
  }).pipe(Effect.provide(layers({ authorized: () => authorized, generation })));
});
it.effect("never calls generation for evidence already revoked or a missing native grant", () => {
  let called = 0;
  return Effect.gen(function* () {
    yield* seed;
    yield* (yield* Registry.RegistryStore).appendPlain(base, "grants", {
      principal_id: caller.principal_id,
      grant_version: 2,
      grant_id: "grant:test",
      permissions: "[]",
      state: "revoked",
    });
    const result = yield* (yield* Application.RollupApplication).processOnce();
    expect(result.status).toBe("provider_failed");
    expect(called).toBe(0);
  }).pipe(
    Effect.provide(
      layers({
        generation: Layer.succeed(Generation.RollupGenerationProvider, {
          configured: true,
          identity: f.identity,
          generate: () =>
            Effect.sync(() => {
              called++;
              return f.result;
            }),
        }),
      }),
    ),
  );
});
it.effect("returns forbidden404 without reading a summary body when one account is denied", () => {
  let bodyReads = 0;
  return Effect.gen(function* () {
    const metadata = yield* publish;
    expect(metadata).not.toBeNull();
    const reader = yield* SummaryRead.SummaryRead;
    const denied = yield* Effect.flip(reader.read(caller, "ws:test"));
    expect(denied.code).toBe("not_found");
    expect(bodyReads).toBe(0);
  }).pipe(
    Effect.provide(
      layers({
        readObserver: () => {
          bodyReads++;
        },
        authorizer: {
          authorize: (p, o, w, accounts) =>
            Effect.succeed(
              accounts.map((a) => ({
                principal_id: p,
                owner_id: o,
                workstream_id: w,
                account_id: a,
                decision_id: "denied",
                authorization_revision: 1,
                allowed: false,
              })),
            ),
        },
      }),
    ),
  );
});
it.effect(
  "rejects a revised allowed decision after payload access and keeps payload out of the error",
  () => {
    let calls = 0;
    return Effect.gen(function* () {
      yield* publish;
      const error = yield* Effect.flip((yield* SummaryRead.SummaryRead).read(caller, "ws:test"));
      expect(error.code).toBe("not_found");
      expect(JSON.stringify(error).includes(f.result.outcome.text)).toBe(false);
      expect(calls).toBe(2);
    }).pipe(
      Effect.provide(
        layers({
          authorizer: {
            authorize: (p, o, w, accounts) =>
              Effect.sync(() => {
                calls++;
                return accounts.map((a) => ({
                  principal_id: p,
                  owner_id: o,
                  workstream_id: w,
                  account_id: a,
                  decision_id: "summary:test",
                  authorization_revision: calls,
                  allowed: true,
                }));
              }),
          },
        }),
      ),
    );
  },
);
it.effect("labels an older summary stale and isolates owner and caller identities", () =>
  Effect.gen(function* () {
    yield* publish;
    yield* (yield* Registry.RegistryStore).advance(base, {
      server_generation: 1,
      registry_version: 2,
    });
    const reader = yield* SummaryRead.SummaryRead;
    expect((yield* reader.read(caller, "ws:test")).freshness).toBe("stale");
    expect(
      (yield* Effect.flip(reader.read({ ...caller, principal_id: "other:principal" }, "ws:test")))
        .code,
    ).toBe("not_found");
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          (yield* Store.RollupStore).readSummary({
            owner_id: "other:owner",
            workstream_id: "ws:test",
            rollup_version: 1,
            manifest_sha256: f.manifest.manifest_sha256,
            output_sha256: f.input.output_sha256,
            account_ids: ["account:test"],
            coverage: "complete",
            freshness: "current",
          }),
        ),
      ),
    ).toBe(true);
  }).pipe(Effect.provide(layers())),
);
it.effect("reads imported encrypted summaries with unconfigured generation and evidence", () =>
  Effect.gen(function* () {
    yield* seed;
    const registry = yield* Registry.RegistryStore;
    yield* registry.put(
      base,
      "rollup_manifests",
      {
        metadata: {
          workstream_id: "ws:test",
          manifest_sha256: f.manifest.manifest_sha256,
          workstream_version: 1,
          registry_version: f.manifest.registry_version,
        },
        value: f.manifest,
      },
      null,
    );
    const s = Generation.createSummary(f.manifest, {
      workstream_id: "ws:test",
      rollup_version: 1,
      manifest_sha256: f.manifest.manifest_sha256,
      generated_at: now,
      result: f.result,
    });
    yield* registry.put(
      base,
      "rollup_summaries",
      {
        metadata: {
          workstream_id: s.workstream_id,
          rollup_version: 1,
          manifest_sha256: s.manifest_sha256,
          output_sha256: s.output_sha256,
          coverage: "complete",
          freshness: "current",
          generated_at: now,
        },
        value: s,
      },
      null,
    );
    expect((yield* (yield* SummaryRead.SummaryRead).read(caller, "ws:test")).state).toBe("ready");
  }).pipe(
    Effect.provide(
      layers({ generation: Generation.unconfiguredLayer, evidence: Evidence.unconfiguredLayer }),
    ),
  ),
);
it.effect(
  "fails closed for a corrupt digest or a missing decryption key without plaintext fallback",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const r = yield* Registry.RegistryStore;
      yield* r.put(
        base,
        "rollup_manifests",
        {
          metadata: {
            workstream_id: "ws:test",
            manifest_sha256: f.manifest.manifest_sha256,
            workstream_version: 1,
            registry_version: f.manifest.registry_version,
          },
          value: f.manifest,
        },
        null,
      );
      const s = Generation.createSummary(f.manifest, {
        workstream_id: "ws:test",
        rollup_version: 1,
        manifest_sha256: f.manifest.manifest_sha256,
        generated_at: now,
        result: f.result,
      });
      yield* r.put(
        base,
        "rollup_summaries",
        {
          metadata: {
            workstream_id: s.workstream_id,
            rollup_version: 1,
            manifest_sha256: s.manifest_sha256,
            output_sha256: s.output_sha256,
            coverage: "complete",
            freshness: "current",
            generated_at: now,
          },
          value: { ...s, result: { ...s.result, confidence: 0.1 } },
        },
        null,
      );
      expect(
        (yield* Effect.flip((yield* SummaryRead.SummaryRead).read(caller, "ws:test"))).code,
      ).toBe("content_unavailable");
    }).pipe(Effect.provide(layers())),
);
it.effect(
  "stores all protected text encrypted in SQLite and WAL, with no plaintext disk cache",
  () => {
    const root = NodeFS.mkdtempSync(
        NodePath.resolve(process.env.TMPDIR ?? "node_modules/.cache", "nw-j8-at-rest-"),
      ),
      filename = NodePath.join(root, "state.sqlite");
    return Effect.gen(function* () {
      yield* publish;
      const sql = yield* SqlClient.SqlClient;
      const envelopes = yield* sql<{
        envelope: string;
      }>`SELECT envelope FROM jones_workstreams_rollup_manifests UNION ALL SELECT envelope FROM jones_workstreams_rollup_summaries UNION ALL SELECT envelope FROM jones_workstreams_rollup_bindings`;
      expect(envelopes.length).toBe(3);
      expect(envelopes.every((r) => !r.envelope.includes(f.result.outcome.text))).toBe(true);
      for (const path of [filename, filename + "-wal"]) {
        if (NodeFS.existsSync(path))
          expect(NodeFS.readFileSync(path).includes(Buffer.from(f.result.outcome.text))).toBe(
            false,
          );
      }
      const db = yield* NativeSql.NativeSql;
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            db.openJson(
              { ...base, purpose: "rollup_summary", record_id: '["ws:test",1]', revision: 2 },
              envelopes[1]!.envelope,
            ),
          ),
        ),
      ).toBe(true);
    }).pipe(Effect.provide(layers({ filename })));
  },
);

it.effect("withholds inference capacity after timeout until the actual provider settles", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>(),
      finish = yield* Deferred.make<Evidence.Result>(),
      settled = yield* Deferred.make<void>();
    const generation = Layer.succeed(Generation.RollupGenerationProvider, {
      configured: true,
      identity: f.identity,
      generate: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.flatMap(() => Deferred.await(finish)),
          Effect.ensuring(Deferred.succeed(settled, undefined)),
        ),
    });
    yield* Effect.gen(function* () {
      yield* seed;
      const app = yield* Application.RollupApplication,
        admission = yield* Application.RollupAdmission;
      const running = yield* app.processOnce().pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust(1000);
      expect((yield* Fiber.join(running)).status).toBe("provider_failed");
      expect(yield* admission.tryAcquire()).toBeNull();
      yield* Deferred.succeed(finish, f.result);
      yield* Deferred.await(settled);
      yield* Effect.yieldNow;
      const release = yield* admission.tryAcquire();
      expect(release !== null).toBe(true);
      if (release) yield* release;
      const rows =
        yield* (yield* SqlClient.SqlClient)`SELECT failure_code FROM jones_workstreams_rollup_queue`;
      expect(rows).toEqual([{ failure_code: "provider_timeout" }]);
    }).pipe(
      Effect.provide(layers({ generation })),
      Effect.provideService(Application.ApplicationOptions, {
        discovery_limit: 100,
        lease_ms: 60000,
        provider_timeout_ms: 1000,
      }),
    );
  }),
);
it.effect("discovery preserves active secondary contributions and unavailable exclusions", () =>
  Effect.gen(function* () {
    yield* seed;
    const r = yield* Registry.RegistryStore;
    const members = yield* r.list(base, "memberships"),
      m = members[0]!;
    // Switch kind by seeding a separate fresh active secondary episode; closed history stays excluded.
    yield* r.put(
      base,
      "memberships",
      {
        metadata: { ...m.metadata, closed_registry_version: 2, closed_at: now },
        value: {
          ...(m.value as object),
          closed: {
            at: now,
            actor: { principal_id: caller.principal_id },
            command_id: "command:synthetic-0002",
            registry_version: 2,
            reason: "removed",
            other_reason: null,
          },
        },
      },
      1,
    );
    yield* r.put(
      base,
      "memberships",
      {
        metadata: {
          ...m.metadata,
          membership_id: "membership:secondary",
          kind: "secondary",
          opened_registry_version: 2,
        },
        value: {
          membership_id: "membership:secondary",
          workstream_id: "ws:test",
          native_reference_id: "ref:test",
          kind: "secondary",
          opened: {
            at: now,
            actor: { principal_id: caller.principal_id },
            command_id: "command:synthetic-0003",
            registry_version: 2,
          },
          closed: null,
        },
      },
      null,
    );
    yield* r.put(
      base,
      "memberships",
      {
        metadata: {
          ...m.metadata,
          membership_id: "membership:unmapped",
          native_reference_id: "ref:unmapped",
          kind: "secondary",
        },
        value: {
          membership_id: "membership:unmapped",
          workstream_id: "ws:test",
          native_reference_id: "ref:unmapped",
          kind: "secondary",
          opened: {
            at: now,
            actor: { principal_id: caller.principal_id },
            command_id: "command:synthetic-0004",
            registry_version: 1,
          },
          closed: null,
        },
      },
      null,
    );
    const discovery = yield* (yield* Manifests.ManifestSource).discoverDirtyManifests(1, now);
    expect(discovery.manifests[0]?.sources.map((s) => s.membership_id)).toEqual([
      "membership:secondary",
    ]);
    expect(discovery.manifests[0]?.exclusions).toEqual([
      { native_reference_id: "ref:unmapped", reason: "unsupported" },
    ]);
  }).pipe(Effect.provide(layers())),
);
it.effect("unconfigured evidence is explicit and imported keys never silently fall back", () =>
  Effect.gen(function* () {
    yield* seed;
    const result = yield* (yield* Application.RollupApplication).processOnce();
    expect(result.status).toBe("provider_unconfigured");
    expect(result.inspected[0]).toMatchObject({
      state: "provider_unconfigured",
      freshness: "inaccessible",
    });
    expect((yield* (yield* SummaryRead.SummaryRead).read(caller, "ws:test")).state).toBe(
      "provider_unconfigured",
    );
  }).pipe(
    Effect.provide(
      layers({ evidence: Evidence.unconfiguredLayer, generation: Generation.unconfiguredLayer }),
    ),
  ),
);
it.effect("missing synthetic content keys fail closed on an already published summary", () => {
  const keys = Fixture.secrets();
  return Effect.gen(function* () {
    yield* publish;
    keys.delete("workstreams-content-key-test-home");
    expect(
      (yield* Effect.flip((yield* SummaryRead.SummaryRead).read(caller, "ws:test"))).code,
    ).toBe("content_unavailable");
  }).pipe(Effect.provide(layers({ keys })));
});

it.effect(
  "durable leases and unknown provider outcomes remain quarantined after database reopen",
  () => {
    const root = NodeFS.mkdtempSync(
        NodePath.resolve(process.env.TMPDIR ?? "node_modules/.cache", "nw-j8-reopen-"),
      ),
      filename = NodePath.join(root, "state.sqlite");
    return Effect.gen(function* () {
      const claim = yield* Effect.gen(function* () {
        yield* seed;
        const s = yield* Store.RollupStore;
        yield* s.enqueue(f.manifest, now);
        return (yield* s.claim(now, 1000))!;
      }).pipe(Effect.provide(layers({ filename })));
      yield* Effect.gen(function* () {
        const s = yield* Store.RollupStore;
        expect(yield* s.claim(now, 1000)).toBeNull();
        expect(yield* s.expireClaims("2026-01-01T00:00:01.000Z")).toBe(1);
        yield* s.enqueue(f.manifest, "2026-01-01T00:00:01.000Z");
        expect(yield* s.claim("2026-01-01T00:00:01.000Z", 1000)).toBeNull();
        expect(
          Exit.isFailure(yield* Effect.exit(s.failClaim(claim, "provider_failure", now))),
        ).toBe(true);
      }).pipe(Effect.provide(layers({ filename })));
    });
  },
);
it.effect(
  "persists discovery fairness across bounded sweeps and truthfully records an unavailable group",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const r = yield* Registry.RegistryStore,
        bindings = yield* Bindings.Bindings;
      const existing = (yield* r.list(base, "records"))[0]!;
      yield* r.put(
        base,
        "records",
        {
          metadata: { ...existing.metadata, workstream_id: "ws:second" },
          value: { ...(existing.value as object), workstream_id: "ws:second" },
        },
        null,
      );
      yield* bindings.put(
        { ...f.binding, workstream_id: "ws:second", native_reference_id: "ref:second" },
        null,
      );
      const manifests = yield* Manifests.ManifestSource;
      const a = yield* manifests.discoverDirtyManifests(1, now),
        b = yield* manifests.discoverDirtyManifests(1, now);
      expect(a.inspected.map((v) => v.workstream_id)).toEqual(["ws:second"]);
      expect(a.inspected[0]?.state).toBe("content_unavailable");
      expect(b.inspected.map((v) => v.workstream_id)).toEqual(["ws:test"]);
    }).pipe(Effect.provide(layers())),
);
it.effect("serializes finite queue capacity while preserving same-subject coalescing", () =>
  Effect.gen(function* () {
    yield* seed;
    const s = yield* Store.RollupStore,
      other = Generation.createManifest({ ...f.manifest, workstream_id: "ws:other" });
    const values = yield* Effect.all([s.enqueue(f.manifest, now), s.enqueue(other, now)], {
      concurrency: 2,
    });
    expect(values.toSorted()).toEqual(["enqueued", "overloaded"]);
    expect(yield* s.enqueue(f.manifest, now)).toBe("coalesced");
  }).pipe(
    Effect.provide(
      layers().pipe(
        Layer.provide(
          Layer.succeed(Store.StoreOptions, { max_dirty_subjects: 1, max_concurrent: 1 }),
        ),
      ),
    ),
  ),
);

it.effect("rechecks revocation after extraction input access before invoking generation", () => {
  let authorized = true,
    generated = 0,
    inputReads = 0;
  const evidence = Layer.effect(
    Evidence.RollupEvidenceProvider,
    Effect.map(Evidence.RollupEvidenceProvider, (base) => ({
      ...base,
      readExtractionInputs: (m: Evidence.Manifest, signal: AbortSignal) =>
        base.readExtractionInputs(m, signal).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              inputReads++;
              authorized = false;
            }),
          ),
        ),
    })),
  ).pipe(Layer.provide(syntheticEvidenceLayer([{ ...f, authorized: () => authorized }])));
  return Effect.gen(function* () {
    yield* seed;
    const outcome = yield* (yield* Application.RollupApplication).processOnce();
    expect(outcome.status).toBe("provider_failed");
    expect(inputReads).toBe(1);
    expect(generated).toBe(0);
    expect(yield* (yield* Store.RollupStore).latestMetadata("ws:test")).toBeNull();
    const rows =
      yield* (yield* SqlClient.SqlClient)`SELECT state,failure_code FROM jones_workstreams_rollup_queue`;
    expect(rows[0]).toEqual({ state: "dead_letter", failure_code: "input_unavailable" });
  }).pipe(
    Effect.provide(
      layers({
        evidence,
        generation: Layer.succeed(Generation.RollupGenerationProvider, {
          configured: true,
          identity: f.identity,
          generate: () =>
            Effect.sync(() => {
              generated++;
              return f.result;
            }),
        }),
      }),
    ),
  );
});
it.effect("quarantines failed inference without redispatching the same manifest", () => {
  let generated = 0;
  return Effect.gen(function* () {
    yield* seed;
    const app = yield* Application.RollupApplication;
    expect((yield* app.processOnce()).status).toBe("provider_failed");
    expect((yield* app.processOnce()).status).toBe("idle");
    expect(generated).toBe(1);
    expect(yield* (yield* Store.RollupStore).latestMetadata("ws:test")).toBeNull();
    const rows =
      yield* (yield* SqlClient.SqlClient)`SELECT state,failure_code FROM jones_workstreams_rollup_queue`;
    expect(rows[0]).toEqual({ state: "dead_letter", failure_code: "provider_failure" });
  }).pipe(
    Effect.provide(
      layers({
        generation: Layer.succeed(Generation.RollupGenerationProvider, {
          configured: true,
          identity: f.identity,
          generate: () =>
            Effect.suspend(() => {
              generated++;
              return new Evidence.RollupError({ code: "provider_failure" });
            }),
        }),
      }),
    ),
  );
});

it.effect("retains logical queue absence for an imported already-published manifest", () =>
  Effect.gen(function* () {
    const metadata = yield* publish;
    const sql = yield* SqlClient.SqlClient,
      store = yield* Store.RollupStore;
    // Exercise CP's explicit enqueue contract for a snapshot-shaped matching overlap.
    yield* sql`UPDATE jones_workstreams_rollup_queue SET state='pending',attempt_id=NULL,lease_expires_at=NULL,failure_code=NULL,failed_at=NULL`;
    const manifest = yield* store.manifest("ws:test", metadata!.manifest_sha256);
    expect(yield* store.enqueue(manifest, now)).toBe("unchanged");
    expect((yield* (yield* Application.RollupApplication).processOnce()).status).toBe("idle");
    expect((yield* (yield* Store.RollupStore).latestMetadata("ws:test"))?.rollup_version).toBe(1);
    const rows = yield* sql`SELECT state,failure_code FROM jones_workstreams_rollup_queue`;
    expect(rows).toEqual([{ state: "dead_letter", failure_code: "published" }]);
  }).pipe(Effect.provide(layers())),
);

it.effect.each([1, 2, 3])(
  "bounds stalled authorization at phase %i and releases the writer and application admission",
  (phase) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        finish = yield* Deferred.make<boolean>(),
        stopped = yield* Deferred.make<void>();
      let calls = 0;
      const evidence = Layer.effect(
        Evidence.RollupEvidenceProvider,
        Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
          ...p,
          verifyCurrentAuthorization: (m: Evidence.Manifest) =>
            Effect.suspend(() => {
              calls++;
              return calls === phase
                ? Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(Deferred.await(finish)),
                    Effect.ensuring(Deferred.succeed(stopped, undefined)),
                  )
                : p.verifyCurrentAuthorization(m);
            }),
        })),
      ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
      yield* Effect.gen(function* () {
        yield* seed;
        const app = yield* Application.RollupApplication,
          store = yield* Store.RollupStore;
        const running = yield* app.processOnce().pipe(Effect.forkChild);
        yield* Deferred.await(started);
        expect(yield* store.expireClaims(now)).toBe(0);
        yield* TestClock.adjust(1000);
        expect((yield* Fiber.join(running)).status).toBe("provider_failed");
        yield* Deferred.await(stopped);
        expect(yield* store.latestMetadata("ws:test")).toBeNull();
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql`SELECT failure_code FROM jones_workstreams_rollup_queue`).toEqual([
          { failure_code: "provider_timeout" },
        ]);
        const release = yield* (yield* Application.RollupAdmission).tryAcquire();
        expect(release).not.toBeNull();
        if (release) yield* release;
      }).pipe(
        Effect.provide(layers({ evidence })),
        Effect.provideService(Application.ApplicationOptions, {
          discovery_limit: 100,
          lease_ms: 60000,
          provider_timeout_ms: 1000,
        }),
        Effect.ensuring(Deferred.succeed(finish, true)),
      );
    }),
);

it.effect(
  "rechecks a changed branch after authorization and before reading extraction payloads",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        finish = yield* Deferred.make<boolean>();
      let inputReads = 0;
      const evidence = Layer.effect(
        Evidence.RollupEvidenceProvider,
        Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
          ...p,
          verifyCurrentAuthorization: () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(finish))),
          readExtractionInputs: (m: Evidence.Manifest, signal: AbortSignal) =>
            Effect.suspend(() => {
              inputReads++;
              return p.readExtractionInputs(m, signal);
            }),
        })),
      ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
      yield* Effect.gen(function* () {
        yield* seed;
        const running = yield* (yield* Application.RollupApplication)
          .processOnce()
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* (yield* Bindings.Bindings).put(
          {
            ...f.binding,
            binding_version: 2,
            selection: {
              kind: "thread",
              conversation_id: "conversation:test",
              thread_id: "thread:test",
              selected_branch_id: "branch:changed",
            },
          },
          1,
        );
        yield* Deferred.succeed(finish, true);
        expect((yield* Fiber.join(running)).status).toBe("provider_failed");
        expect(inputReads).toBe(0);
        expect(yield* (yield* Store.RollupStore).latestMetadata("ws:test")).toBeNull();
      }).pipe(
        Effect.provide(layers({ evidence })),
        Effect.ensuring(Deferred.succeed(finish, true)),
      );
    }),
);

it.effect(
  "rejects a changed project after generation even when the old provider authorization still passes",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        finish = yield* Deferred.make<Evidence.Result>();
      const generation = Layer.succeed(Generation.RollupGenerationProvider, {
        configured: true,
        identity: f.identity,
        generate: () =>
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(finish))),
      });
      yield* Effect.gen(function* () {
        yield* seed;
        const running = yield* (yield* Application.RollupApplication)
          .processOnce()
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* (yield* Bindings.Bindings).put(
          {
            ...f.binding,
            binding_version: 2,
            selection: { kind: "project", project_id: "project:changed", project_version: 2 },
          },
          1,
        );
        expect(
          yield* (yield* Evidence.RollupEvidenceProvider).verifyCurrentAuthorization(f.manifest),
        ).toBe(true);
        yield* Deferred.succeed(finish, f.result);
        expect((yield* Fiber.join(running)).status).toBe("provider_failed");
        expect(yield* (yield* Store.RollupStore).latestMetadata("ws:test")).toBeNull();
      }).pipe(
        Effect.provide(layers({ generation })),
        Effect.ensuring(Deferred.succeed(finish, f.result)),
      );
    }),
);
