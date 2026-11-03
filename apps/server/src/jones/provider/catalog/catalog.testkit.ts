import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceConfigMap,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ProviderDriver, ProviderInstance } from "@t3tools/provider-core/server/driver";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../../config.ts";
import * as ModelManifest from "../../../provider/ModelManifest.ts";
import * as ProviderInstanceRegistry from "../../../provider/ProviderInstanceRegistry.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";

export const driverKind = ProviderDriverKind.make("opencode");
export const instanceA = ProviderInstanceId.make("catalog-a");
export const instanceB = ProviderInstanceId.make("catalog-b");

export const providerSnapshot = (
  instanceId: ProviderInstanceId = instanceA,
  version = "original",
): ServerProvider => ({
  instanceId,
  driver: driverKind,
  enabled: true,
  installed: true,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-11-02T00:00:00.000Z",
  version,
  models: [],
  slashCommands: [],
  skills: [],
});

export const configuration = (
  ...entries: ReadonlyArray<readonly [ProviderInstanceId, string]>
): ProviderInstanceConfigMap => Object.fromEntries(entries.map(([id, generation]) => [
  id,
  { driver: driverKind, enabled: true, config: { generation } },
]));

export const makeGate = Effect.fn("CatalogTest.makeGate")(function* () {
  const started = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
  return {
    started,
    release: Deferred.succeed(release, undefined),
    hold: Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
  };
});

interface CatalogTestSource {
  readonly instance: ProviderInstance;
  readonly changes: PubSub.PubSub<ServerProvider>;
  provider: ServerProvider;
  initialRead: () => Effect.Effect<ServerProvider>;
  refreshRead: () => Effect.Effect<ServerProvider>;
  maintenance: () => Effect.Effect<void>;
  retirement: Effect.Effect<void>;
  stream: Stream.Stream<ServerProvider>;
}

export const makeCatalogHarness = Effect.fn("CatalogTest.makeHarness")(function* (options?: {
  readonly entries?: ReadonlyArray<readonly [ProviderInstanceId, string]>;
  readonly configureSource?: (source: CatalogTestSource, generation: string) => void;
}) {
  const sources = new Map<string, CatalogTestSource>();
  const controls: {
    enrichment?: Effect.Effect<ModelManifest.ModelManifestData>;
    beforeRename?: (from: string, to: string) => Effect.Effect<void>;
    afterRemove?: (path: string) => Effect.Effect<void>;
  } = {};
  const driver: ProviderDriver<{ readonly generation: string }> = {
    driverKind,
    metadata: { displayName: "Synthetic OpenCode" },
    configSchema: Schema.Struct({ generation: Schema.String }),
    defaultConfig: () => ({ generation: "original" }),
    create: (input) => Effect.gen(function* () {
      const changes = yield* PubSub.unbounded<ServerProvider>();
      yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
      const source: CatalogTestSource = {
        provider: providerSnapshot(input.instanceId, input.config.generation),
        changes,
        initialRead: () => Effect.succeed(source.provider),
        refreshRead: () => Effect.succeed(source.provider),
        maintenance: () => Effect.void,
        retirement: Effect.void,
        stream: Stream.fromPubSub(changes),
        instance: {
          instanceId: input.instanceId,
          driverKind,
          continuationIdentity: { driverKind, continuationKey: input.instanceId },
          displayName: undefined,
          enabled: true,
          snapshot: {
            getSnapshot: Effect.suspend(() => source.initialRead()),
            refresh: Effect.suspend(() => source.refreshRead()),
            streamChanges: Stream.unwrap(Effect.sync(() => source.stream)),
            resolveMaintenance: () => Effect.suspend(() => source.maintenance()).pipe(Effect.as(
              makeManualOnlyProviderMaintenanceCapabilities({ provider: driverKind, packageName: null }),
            )),
            applyUsageLimits: () => Effect.void,
          },
          orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
          textGeneration: {} as ProviderInstance["textGeneration"],
        },
      };
      options?.configureSource?.(source, input.config.generation);
      sources.set(`${input.instanceId}:${input.config.generation}`, source);
      yield* Effect.addFinalizer(() => source.retirement);
      return source.instance;
    }),
  };
  const entries: ReadonlyArray<readonly [ProviderInstanceId, string]> =
    options?.entries ?? [[instanceA, "original"]];
  const instances = yield* ProviderInstanceRegistry.makeProviderInstanceRegistry({
    drivers: [driver],
    configMap: configuration(...entries),
  });
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const services = yield* Layer.build(ProviderRegistry.layer.pipe(
    Layer.provideMerge(Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, instances.registry)),
    Layer.provideMerge(Layer.succeed(ModelManifest.ModelManifest, {
      current: Effect.suspend(() => {
        const enrichment = controls.enrichment;
        delete controls.enrichment;
        return enrichment ?? Effect.succeed(ModelManifest.BUNDLED_MODEL_MANIFEST);
      }),
      // Hold the unrelated background fetch; the service scope owns its interruption.
      refresh: Effect.never,
      forceRefresh: Effect.succeed(ModelManifest.BUNDLED_MODEL_MANIFEST),
      refreshInBackground: Effect.void,
    })),
    Layer.provideMerge(Layer.mock(ProviderLatestVersions.ProviderLatestVersions)({
      invalidate: () => Effect.void,
    })),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "jones-catalog-publication-" })),
    Layer.provide(Layer.succeed(FileSystem.FileSystem, {
      ...fs,
      rename: (from, to) => Effect.suspend(() => controls.beforeRename?.(from, to) ?? Effect.void)
        .pipe(Effect.andThen(fs.rename(from, to))),
      remove: (path, removeOptions) => fs.remove(path, removeOptions).pipe(
        Effect.andThen(Effect.suspend(() => controls.afterRemove?.(path) ?? Effect.void)),
      ),
    })),
  ));
  const registry = Context.get(services, ProviderRegistry.ProviderRegistry);
  const config = Context.get(services, ServerConfig.ServerConfig);
  return {
    registry,
    instances: instances.registry,
    reconcile: instances.mutator.reconcile,
    controls,
    cachePath: (id: ProviderInstanceId) => path.join(config.providerStatusCacheDir, `${id}.json`),
    source: (id = instanceA, generation = "original") => {
      const source = sources.get(`${id}:${generation}`);
      if (!source) throw new Error("Expected a synthetic catalog source.");
      return source;
    },
  };
});

export const nextCatalog = (
  registry: ProviderRegistry.ProviderRegistry["Service"],
  predicate: (providers: ReadonlyArray<ServerProvider>) => boolean,
) => Stream.toPull(registry.streamChanges.pipe(Stream.filter(predicate)));
