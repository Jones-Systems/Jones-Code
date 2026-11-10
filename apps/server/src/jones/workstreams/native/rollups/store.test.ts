// @effect-diagnostics nodeBuiltinImport:off -- Synthetic restart fixtures are retained through parent delivery.
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
import * as NativeSql from "../store/sql.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Bindings from "./bindings.ts";
import * as Store from "./store.ts";
import * as Evidence from "./evidence.ts";
import * as Generation from "./generation.ts";
import { syntheticFixture, syntheticEvidenceLayer } from "./synthetic.ts";

const f = syntheticFixture(),
  scope = Fixture.scope,
  now = Fixture.now;
function layers(
  options: {
    filename?: string;
    evidence?: Layer.Layer<
      Evidence.RollupEvidenceProvider,
      never,
      Registry.RegistryStore | SqlClient.SqlClient
    >;
  } = {},
) {
  return Store.layer.pipe(
    Layer.provideMerge(Bindings.layer),
    Layer.provideMerge(
      Layer.mergeAll(
        options.evidence ?? syntheticEvidenceLayer([f]),
        Layer.succeed(NativeHome, {
          ...scope,
          canonical_origin: "https://native.workstreams.invalid",
          home_environment_id: "home:test",
          served_by_environment_id: "home:test",
          server_generation: 1,
        }),
      ),
    ),
    Layer.provideMerge(Fixture.testLayer(options)),
  );
}
const seed = Effect.gen(function* () {
  yield* Fixture.seed;
  // Project the unapplied additive parent migration only in this isolated test database.
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ name: string }>`PRAGMA table_info(jones_workstreams_rollup_queue)`;
  if (!columns.some((column) => column.name === "attempt_count"))
    yield* sql`ALTER TABLE jones_workstreams_rollup_queue ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0)`;
  yield* TestClock.setTime(Date.parse(now));
  const r = yield* Registry.RegistryStore;
  yield* r.appendPlain(scope, "principals", {
    principal_id: "principal:test",
    environment_id: "home:test",
    kind: "environment",
    provenance: '{"source":"synthetic"}',
  });
  yield* r.appendPlain(scope, "grants", {
    principal_id: "principal:test",
    grant_version: 1,
    grant_id: "grant:test",
    permissions: '["workstreams:read"]',
    state: "active",
  });
  yield* r.put(
    scope,
    "records",
    {
      metadata: { workstream_id: "ws:test", version: 1, sort_order: 0 },
      value: { synthetic: true },
    },
    null,
  );
  yield* (yield* Bindings.Bindings).put(f.binding, null);
  return Generation.createManifest({
    ...f.manifest,
    registry_version: (yield* r.context(scope))!.registry_version,
  });
});
const summary = (c: Evidence.Claim, version: number) =>
  Generation.createSummary(c.manifest, {
    workstream_id: c.workstream_id,
    rollup_version: version,
    manifest_sha256: c.manifest.manifest_sha256,
    generated_at: now,
    result: f.result,
  });

it.effect("P1 selection change fences admission and publication of the claimed old selection", () =>
  Effect.gen(function* () {
    const m = yield* seed,
      store = yield* Store.RollupStore;
    yield* store.enqueue(m, now);
    const c = (yield* store.claim(now, 1000))!;
    expect(yield* store.verifyAdmission(m)).toBe(true);
    yield* (yield* Bindings.Bindings).put(
      {
        ...f.binding,
        binding_version: 2,
        selection: { kind: "project", project_id: "project:changed", project_version: 1 },
      },
      1,
    );
    expect(yield* store.verifyAdmission(m)).toBe(false);
    expect(Exit.isFailure(yield* Effect.exit(store.completeClaim(c, summary(c, 1), now)))).toBe(
      true,
    );
    expect(yield* store.latestMetadata("ws:test")).toBeNull();
  }).pipe(Effect.provide(layers())),
);

it.effect(
  "P1 publication authorization leaves the SQL writer free and times out at the lease",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        finish = yield* Deferred.make<boolean>(),
        stopped = yield* Deferred.make<void>();
      const evidence = Layer.effect(
        Evidence.RollupEvidenceProvider,
        Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
          ...p,
          verifyCurrentAuthorization: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(finish)),
              Effect.ensuring(Deferred.succeed(stopped, undefined)),
            ),
        })),
      ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
      yield* Effect.gen(function* () {
        const m = yield* seed,
          store = yield* Store.RollupStore;
        yield* store.enqueue(m, now);
        const c = (yield* store.claim(now, 1000))!;
        const publishing = yield* store
          .completeClaim(c, summary(c, 1), now)
          .pipe(Effect.exit, Effect.forkChild);
        yield* Deferred.await(started);
        yield* Effect.gen(function* () {
          const written = yield* Deferred.make<void>();
          const sql = yield* SqlClient.SqlClient;
          const writing =
            yield* sql`INSERT INTO jones_workstreams_rollup_discovery_cursors(cursor_id,last_group_key,updated_at) VALUES('writer:probe',NULL,${now})`.pipe(
              Effect.tap(() => Deferred.succeed(written, undefined)),
              Effect.forkChild,
            );
          yield* TestClock.adjust(100);
          expect(yield* Deferred.isDone(written)).toBe(true);
          yield* Fiber.join(writing);
          yield* TestClock.adjust(1000);
          const result = yield* Fiber.join(publishing);
          expect(Exit.isFailure(result)).toBe(true);
          yield* Deferred.await(stopped);
          expect(yield* store.latestMetadata("ws:test")).toBeNull();
        }).pipe(Effect.ensuring(Deferred.succeed(finish, true)));
      }).pipe(Effect.provide(layers({ evidence })));
    }),
);

it.effect(
  "P1 publication revalidates native authority after the external authorization wait",
  () => {
    const evidence = Layer.effect(
      Evidence.RollupEvidenceProvider,
      Effect.gen(function* () {
        const p = yield* Evidence.RollupEvidenceProvider,
          r = yield* Registry.RegistryStore;
        return {
          ...p,
          verifyCurrentAuthorization: () =>
            Effect.gen(function* () {
              const c = (yield* r.context(scope))!;
              yield* r.advance(scope, c);
              return true;
            }).pipe(Effect.orDie),
        };
      }),
    ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
    return Effect.gen(function* () {
      const m = yield* seed,
        store = yield* Store.RollupStore;
      yield* store.enqueue(m, now);
      const c = (yield* store.claim(now, 1000))!;
      expect(Exit.isFailure(yield* Effect.exit(store.completeClaim(c, summary(c, 1), now)))).toBe(
        true,
      );
      expect(yield* store.latestMetadata("ws:test")).toBeNull();
    }).pipe(Effect.provide(layers({ evidence })));
  },
);

it.effect(
  "P1 repeated publications leave a fresh retry budget without resetting claim fences",
  () =>
    Effect.gen(function* () {
      let m = yield* seed;
      const store = yield* Store.RollupStore,
        r = yield* Registry.RegistryStore;
      for (let version = 1; version <= 2; version++) {
        yield* store.enqueue(m, now);
        const c = (yield* store.claim(now, 1000))!;
        expect(c.fence).toBe(version);
        yield* store.completeClaim(c, summary(c, version), now);
        const context = yield* r.advance(scope, (yield* r.context(scope))!);
        m = Generation.createManifest({ ...m, registry_version: context.registry_version });
      }
      yield* store.enqueue(m, now);
      const c = (yield* store.claim(now, 1000))!;
      expect(c.fence).toBe(3);
      yield* store.failClaim(c, "input_unavailable", now);
      yield* store.enqueue(m, now);
      const retry = yield* store.claim(now, 1000);
      expect(retry).not.toBeNull();
      if (!retry) return;
      expect(retry.fence).toBe(4);
      expect(Exit.isFailure(yield* Effect.exit(store.completeClaim(c, summary(c, 3), now)))).toBe(
        true,
      );
      yield* store.completeClaim(retry, summary(retry, 3), now);
      expect((yield* store.latestMetadata("ws:test"))?.rollup_version).toBe(3);
      const sql = yield* SqlClient.SqlClient;
      expect(
        Exit.isFailure(yield* Effect.exit(sql`UPDATE jones_workstreams_rollup_queue SET fence=0`)),
      ).toBe(true);
    }).pipe(Effect.provide(layers())),
);

it.effect(
  "revalidates the claim fence when a newer manifest arrives during publication authorization",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        finish = yield* Deferred.make<boolean>();
      const evidence = Layer.effect(
        Evidence.RollupEvidenceProvider,
        Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
          ...p,
          verifyCurrentAuthorization: () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(finish))),
        })),
      ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
      yield* Effect.gen(function* () {
        const m = yield* seed,
          store = yield* Store.RollupStore,
          r = yield* Registry.RegistryStore;
        yield* store.enqueue(m, now);
        const c = (yield* store.claim(now, 1000))!;
        const publishing = yield* store
          .completeClaim(c, summary(c, 1), now)
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(started);
        const context = yield* r.advance(scope, (yield* r.context(scope))!);
        yield* store.enqueue(
          Generation.createManifest({ ...m, registry_version: context.registry_version }),
          now,
        );
        yield* Deferred.succeed(finish, true);
        expect((yield* Fiber.join(publishing)).code).toBe("stale_claim");
        expect(yield* store.latestMetadata("ws:test")).toBeNull();
        expect((yield* store.claim(now, 1000))?.fence).toBe(2);
      }).pipe(
        Effect.provide(layers({ evidence })),
        Effect.ensuring(Deferred.succeed(finish, true)),
      );
    }),
);

it.effect("cancels authorization without retaining the writer or publishing late", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>(),
      finish = yield* Deferred.make<boolean>(),
      stopped = yield* Deferred.make<void>();
    const evidence = Layer.effect(
      Evidence.RollupEvidenceProvider,
      Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
        ...p,
        verifyCurrentAuthorization: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
            Effect.ensuring(Deferred.succeed(stopped, undefined)),
          ),
      })),
    ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
    yield* Effect.gen(function* () {
      const m = yield* seed,
        store = yield* Store.RollupStore;
      yield* store.enqueue(m, now);
      const c = (yield* store.claim(now, 1000))!;
      const publishing = yield* store.completeClaim(c, summary(c, 1), now).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(publishing);
      yield* Deferred.await(stopped);
      expect(yield* store.expireClaims("2026-01-01T00:00:01.000Z")).toBe(1);
      yield* Deferred.succeed(finish, true);
      expect(yield* store.latestMetadata("ws:test")).toBeNull();
    }).pipe(Effect.provide(layers({ evidence })), Effect.ensuring(Deferred.succeed(finish, true)));
  }),
);

it.effect("bounds noncooperative authorization without waiting for its eventual result", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>(),
      finish = yield* Deferred.make<boolean>(),
      stopped = yield* Deferred.make<void>();
    const evidence = Layer.effect(
      Evidence.RollupEvidenceProvider,
      Effect.map(Evidence.RollupEvidenceProvider, (p) => ({
        ...p,
        verifyCurrentAuthorization: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
            Effect.ensuring(Deferred.succeed(stopped, undefined)),
            Effect.uninterruptible,
          ),
      })),
    ).pipe(Layer.provide(syntheticEvidenceLayer([f])));
    yield* Effect.gen(function* () {
      const m = yield* seed,
        store = yield* Store.RollupStore;
      yield* store.enqueue(m, now);
      const c = (yield* store.claim(now, 1000))!;
      const publishing = yield* store
        .completeClaim(c, summary(c, 1), now)
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust(1000);
      expect((yield* Fiber.join(publishing)).code).toBe("provider_timeout");
      expect(yield* Deferred.isDone(stopped)).toBe(false);
      expect(yield* store.expireClaims("2026-01-01T00:00:01.000Z")).toBe(1);
      yield* Deferred.succeed(finish, true);
      yield* Deferred.await(stopped);
      expect(yield* store.latestMetadata("ws:test")).toBeNull();
    }).pipe(Effect.provide(layers({ evidence })), Effect.ensuring(Deferred.succeed(finish, true)));
  }),
);

it.effect(
  "persists retry accounting across reopen and rolls back failed claims and publication",
  () => {
    const parent = NodePath.resolve(process.env.TMPDIR ?? "node_modules/.cache");
    NodeFS.mkdirSync(parent, { recursive: true });
    const root = NodeFS.mkdtempSync(NodePath.join(parent, "nw-rollup-p1-restart-")),
      filename = NodePath.join(root, "state.sqlite");
    return Effect.gen(function* () {
      const claim = yield* Effect.gen(function* () {
        const m = yield* seed,
          store = yield* Store.RollupStore,
          db = yield* NativeSql.NativeSql;
        yield* store.enqueue(m, now);
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              db.transaction(
                scope,
                store.claim(now, 1000).pipe(Effect.andThen(Effect.fail("synthetic rollback"))),
              ),
            ),
          ),
        ).toBe(true);
        const c = (yield* store.claim(now, 1000))!;
        expect(c.fence).toBe(1);
        yield* store.failClaim(c, "input_unavailable", now);
        return c;
      }).pipe(Effect.provide(layers({ filename })));
      yield* Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(now));
        const store = yield* Store.RollupStore,
          db = yield* NativeSql.NativeSql,
          sql = yield* SqlClient.SqlClient;
        yield* store.enqueue(claim.manifest, now);
        const retry = (yield* store.claim(now, 1000))!;
        expect(retry.fence).toBe(2);
        expect((yield* sql`SELECT attempt_count FROM jones_workstreams_rollup_queue`)[0]).toEqual({
          attempt_count: 2,
        });
        // Abort the queue tombstone after the summary INSERT to prove the final transaction rolls both back.
        yield* sql`CREATE TRIGGER test_abort_publication BEFORE UPDATE ON jones_workstreams_rollup_queue WHEN NEW.failure_code='published' BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END`;
        expect(
          Exit.isFailure(yield* Effect.exit(store.completeClaim(retry, summary(retry, 1), now))),
        ).toBe(true);
        expect(yield* store.latestMetadata("ws:test")).toBeNull();
        expect(
          (yield* sql`SELECT state,attempt_count,fence FROM jones_workstreams_rollup_queue`)[0],
        ).toEqual({ state: "running", attempt_count: 2, fence: 2 });
        yield* sql`DROP TRIGGER test_abort_publication`;
        yield* store.failClaim(retry, "input_unavailable", now);
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              db.transaction(
                scope,
                store
                  .enqueue(claim.manifest, now)
                  .pipe(Effect.andThen(Effect.fail("synthetic rollback"))),
              ),
            ),
          ),
        ).toBe(true);
        expect(
          (yield* sql`SELECT state,attempt_count FROM jones_workstreams_rollup_queue`)[0],
        ).toEqual({ state: "dead_letter", attempt_count: 2 });
      }).pipe(Effect.provide(layers({ filename })));
      yield* Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(now));
        const store = yield* Store.RollupStore;
        yield* store.enqueue(claim.manifest, now);
        const last = (yield* store.claim(now, 1000))!;
        expect(last.fence).toBe(3);
        yield* store.failClaim(last, "input_unavailable", now);
        yield* store.enqueue(claim.manifest, now);
        expect(yield* store.claim(now, 1000)).toBeNull();
        expect((yield* Effect.flip(store.completeClaim(claim, summary(claim, 1), now))).code).toBe(
          "stale_claim",
        );
      }).pipe(Effect.provide(layers({ filename })));
    });
  },
);
