import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { ServerProvider } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

import * as ModelManifest from "../../../provider/ModelManifest.ts";
import { readProviderStatusCache } from "../../../provider/providerStatusCache.ts";
import {
  configuration,
  driverKind,
  instanceA,
  instanceB,
  makeCatalogHarness,
  makeGate,
  nextCatalog,
  providerSnapshot,
} from "./catalog.testkit.ts";

const versionOf = (providers: ReadonlyArray<ServerProvider>, id = instanceA) =>
  providers.find((provider) => provider.instanceId === id)?.version;

describe("catalog publication ownership (Audit 4.4 / R4)", () => {
  for (const replacement of [false, true]) {
    it.effect(`revokes a source before awaiting ${replacement ? "replacement" : "removal"} retirement`, () =>
      Effect.gen(function* () {
        const harness = yield* makeCatalogHarness();
        const retiring = yield* makeGate();
        const refresh = yield* makeGate();
        const source = harness.source();
        source.retirement = retiring.hold;
        source.refreshRead = () => refresh.hold.pipe(Effect.as(providerSnapshot(instanceA, "retired")));
        const stale = yield* harness.registry.refreshInstance(instanceA).pipe(Effect.forkScoped);
        yield* Deferred.await(refresh.started);
        const changed = yield* nextCatalog(harness.registry, (providers) =>
          replacement ? versionOf(providers) === "replacement" : versionOf(providers) === undefined,
        );
        const reconcile = yield* harness.reconcile(
          replacement ? configuration([instanceA, "replacement"]) : configuration(),
        ).pipe(Effect.forkScoped);
        yield* Effect.addFinalizer(() => retiring.release);
        yield* Deferred.await(retiring.started);
        assert.isUndefined(harness.instances.getCatalogSource(instanceA));
        yield* refresh.release;
        assert.equal(versionOf(yield* Fiber.join(stale)), "original");
        assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "original");
        yield* retiring.release;
        yield* Fiber.join(reconcile);
        yield* changed;
        assert.equal(versionOf(yield* harness.registry.getProviders), replacement ? "replacement" : undefined);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.effect("rechecks the exact source after awaited manifest enrichment", () =>
    Effect.gen(function* () {
      const harness = yield* makeCatalogHarness();
      const enrichment = yield* makeGate();
      harness.controls.enrichment = enrichment.hold.pipe(Effect.as(ModelManifest.BUNDLED_MODEL_MANIFEST));
      harness.source().provider = providerSnapshot(instanceA, "stale-enriched");
      const stale = yield* harness.registry.refreshInstance(instanceA).pipe(Effect.forkScoped);
      yield* Deferred.await(enrichment.started);
      const changed = yield* nextCatalog(harness.registry, (providers) => versionOf(providers) === "replacement");
      yield* harness.reconcile(configuration([instanceA, "replacement"]));
      yield* changed;
      yield* enrichment.release;
      assert.equal(versionOf(yield* Fiber.join(stale)), "replacement");
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "replacement");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("a retired subscription cannot publish after enrichment", () =>
    Effect.gen(function* () {
      const delivery = yield* makeGate();
      const processed = yield* Deferred.make<void>();
      const harness = yield* makeCatalogHarness({
        configureSource: (source, generation) => {
          if (generation !== "original") return;
          source.stream = Stream.fromEffect(delivery.hold.pipe(Effect.as(providerSnapshot(instanceA, "old-event"))))
            .pipe(Stream.ensuring(Deferred.succeed(processed, undefined)));
        },
      });
      const enrichment = yield* makeGate();
      harness.controls.enrichment = enrichment.hold.pipe(Effect.as(ModelManifest.BUNDLED_MODEL_MANIFEST));
      yield* delivery.release;
      yield* Deferred.await(enrichment.started);
      const changed = yield* nextCatalog(harness.registry, (providers) => versionOf(providers) === "replacement");
      yield* harness.reconcile(configuration([instanceA, "replacement"]));
      yield* changed;
      yield* enrichment.release;
      yield* Deferred.await(processed);
      assert.equal(versionOf(yield* harness.registry.getProviders), "replacement");
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "replacement");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("a delayed initial read does not block a successor or regain publication authority", () =>
    Effect.gen(function* () {
      const initial = yield* makeGate();
      const initialWorker = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
      const harness = yield* makeCatalogHarness({
        configureSource: (source, generation) => {
          if (generation !== "delayed") return;
          source.initialRead = () => Effect.gen(function* () {
            yield* Deferred.succeed(initialWorker, yield* Effect.fiber);
            yield* initial.hold;
            return source.provider;
          });
        },
      });
      yield* harness.reconcile(configuration([instanceA, "delayed"]));
      yield* Deferred.await(initial.started);
      const changed = yield* nextCatalog(harness.registry, (providers) => versionOf(providers) === "successor");
      yield* harness.reconcile(configuration([instanceA, "successor"]));
      yield* changed;
      yield* initial.release;
      const exit = yield* Fiber.await(yield* Deferred.await(initialWorker));
      assert.isTrue(Exit.isSuccess(exit));
      assert.equal(versionOf(yield* harness.registry.getProviders), "successor");
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "successor");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("a replacement with equal metadata restores its cache after invalidation", () =>
    Effect.gen(function* () {
      const initialWorker = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
      const harness = yield* makeCatalogHarness({
        configureSource: (source, generation) => {
          source.provider = providerSnapshot(instanceA, "same-metadata");
          if (generation !== "replacement") return;
          source.initialRead = () => Effect.gen(function* () {
            yield* Deferred.succeed(initialWorker, yield* Effect.fiber);
            return source.provider;
          });
        },
      });
      yield* harness.reconcile(configuration([instanceA, "replacement"]));
      const exit = yield* Fiber.await(yield* Deferred.await(initialWorker));
      assert.isTrue(Exit.isSuccess(exit));
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "same-metadata");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  for (const failure of [false, true]) {
    it.effect(`post-update ${failure ? "failure" : "success"} belongs to the captured instance`, () =>
      Effect.gen(function* () {
        const harness = yield* makeCatalogHarness();
        const update = yield* makeGate();
        const original = harness.source();
        if (failure) original.maintenance = () => update.hold.pipe(Effect.andThen(Effect.die("old maintenance failed")));
        else original.refreshRead = () => update.hold.pipe(Effect.as(providerSnapshot(instanceA, "old-update")));
        const stale = yield* harness.registry.refreshAfterUpdate(driverKind, instanceA).pipe(Effect.forkScoped);
        yield* Deferred.await(update.started);
        const changed = yield* nextCatalog(harness.registry, (providers) => versionOf(providers) === "replacement");
        yield* harness.reconcile(configuration([instanceA, "replacement"]));
        yield* changed;
        yield* update.release;
        assert.deepStrictEqual(yield* Fiber.join(stale), []);
        const providers = yield* harness.registry.getProviders;
        assert.equal(versionOf(providers), "replacement");
        assert.equal(providers[0]?.status, "ready");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.effect("independent B completes while A persists, and A returns and emits the current aggregate", () =>
    Effect.gen(function* () {
      const harness = yield* makeCatalogHarness({ entries: [[instanceA, "original"], [instanceB, "original"]] });
      const published = yield* harness.registry.streamChanges.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped({ startImmediately: true }),
      );
      const rename = yield* makeGate();
      harness.controls.beforeRename = (_, to) => to === harness.cachePath(instanceA) ? rename.hold : Effect.void;
      harness.source().provider = providerSnapshot(instanceA, "A-new");
      const delayedA = yield* harness.registry.refreshInstance(instanceA).pipe(Effect.forkScoped);
      yield* Effect.addFinalizer(() => rename.release);
      yield* Deferred.await(rename.started);
      harness.source(instanceB).provider = providerSnapshot(instanceB, "B-new");
      const bResult = yield* harness.registry.refreshInstance(instanceB);
      assert.equal(versionOf(bResult, instanceB), "B-new");
      yield* rename.release;
      const aResult = yield* Fiber.join(delayedA);
      assert.equal(versionOf(aResult, instanceB), "B-new");
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version, "A-new");
      assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceB)))?.version, "B-new");
      const emissions = yield* Fiber.join(published);
      assert.equal(emissions.length, 2);
      assert.isTrue(emissions.every((providers) => versionOf(providers, instanceB) === "B-new"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  for (const replacement of [false, true]) {
    it.effect(`fences a rename already in flight across ${replacement ? "replacement" : "removal"}`, () =>
      Effect.gen(function* () {
        const harness = yield* makeCatalogHarness();
        const rename = yield* makeGate();
        const removed = yield* Deferred.make<void>();
        harness.controls.afterRemove = (path) => path === harness.cachePath(instanceA)
          ? Deferred.succeed(removed, undefined).pipe(Effect.asVoid) : Effect.void;
        harness.controls.beforeRename = (_, to) => {
          if (to !== harness.cachePath(instanceA)) return Effect.void;
          delete harness.controls.beforeRename;
          return rename.hold;
        };
        harness.source().provider = providerSnapshot(instanceA, "retired-write");
        const stale = yield* harness.registry.refreshInstance(instanceA).pipe(Effect.forkScoped);
        yield* Effect.addFinalizer(() => rename.release);
        yield* Deferred.await(rename.started);
        const changed = yield* nextCatalog(harness.registry, (providers) => replacement
          ? versionOf(providers) === "replacement" : versionOf(providers) === undefined);
        yield* harness.reconcile(replacement ? configuration([instanceA, "replacement"]) : configuration());
        yield* rename.release;
        yield* Fiber.join(stale);
        yield* changed;
        if (!replacement) yield* Deferred.await(removed);
        assert.equal((yield* readProviderStatusCache(harness.cachePath(instanceA)))?.version,
          replacement ? "replacement" : undefined);
        assert.equal(versionOf(yield* harness.registry.getProviders), replacement ? "replacement" : undefined);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }
});
