import {
  defaultInstanceIdForDriver,
  isProviderWorkspaceSnapshotCurrent,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUpdateState,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ModelManifest from "./ModelManifest.ts";
import { applyProviderCompatibility } from "./providerCompatibility.ts";
import * as ServerConfig from "../config.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderInstanceRegistryMutator from "./ProviderInstanceRegistryMutator.ts";
import { makeCatalogCachePublication } from "../jones/provider/catalog/cachePublication.ts";
import { hasAuthoritativeEmptyModelOptions } from "../jones/provider/catalog/modelOptions.ts";
import {
  hydrateCachedProvider,
  isCachedProviderCorrelated,
  orderProviderSnapshots,
  readProviderStatusCache,
  resolveProviderStatusCachePath,
  writeProviderStatusCache,
} from "./providerStatusCache.ts";
import type {
  ProviderInstance,
  ProviderWorkspaceSnapshot,
} from "@t3tools/provider-core/server/driver";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  type ProviderMaintenanceCapabilities,
  latestProviderVersionCacheKeys,
} from "@t3tools/provider-core/server/maintenanceResolver";
import type { ProviderSnapshotSource } from "./builtInProviderCatalog.ts";

export type ProviderMaintenanceActionKind = "update";

export class ProviderRegistry extends Context.Service<
  ProviderRegistry,
  {
    readonly getProviders: Effect.Effect<ReadonlyArray<ServerProvider>>;

    readonly refresh: (
      provider?: ProviderDriverKind,
    ) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    /** Unknown instance ids return the cached list without error. */
    readonly refreshInstance: (
      instanceId: ProviderInstanceId,
    ) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    /** Re-reads secret-backed environments first, so a fixed secret applies without a restart. */
    readonly reloadSecretsAndRefresh: (
      instanceId?: ProviderInstanceId,
    ) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    /** Only refreshed instances can verify an update; the published list still includes every row. */
    readonly refreshAfterUpdate: (
      provider: ProviderDriverKind,
      selectedInstanceId: ProviderInstanceId,
    ) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    readonly refreshWorkspaceSnapshot: (input: {
      readonly instanceId: ProviderInstanceId;
      readonly cwd: string;
      readonly fresh?: boolean;
    }) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    /**
     * Drop T3-owned model caches before an explicit model refresh. Pass the
     * target of the refresh that follows. Background and ordinary refreshes
     * keep their cache timers and must not call this.
     */
    readonly invalidateModelCaches: (input: {
      readonly instanceId?: ProviderInstanceId | undefined;
      readonly cwd?: string | undefined;
      readonly fresh?: boolean | undefined;
    }) => Effect.Effect<void>;

    /**
     * For an opened model picker: refetch a model manifest older than a few
     * minutes. When it changed, recheck latest CLI versions and probe providers.
     */
    readonly refreshModelCatalog: Effect.Effect<void>;

    readonly getProviderMaintenanceCapabilitiesForInstance: (
      instanceId: ProviderInstanceId,
      provider: ProviderDriverKind,
      options?: { readonly fresh?: boolean },
    ) => Effect.Effect<ProviderMaintenanceCapabilities>;

    readonly setProviderMaintenanceActionState: (input: {
      readonly instanceId: ProviderInstanceId;
      readonly action: ProviderMaintenanceActionKind;
      readonly state: ServerProviderUpdateState | null;
    }) => Effect.Effect<ReadonlyArray<ServerProvider>>;

    readonly streamChanges: Stream.Stream<ReadonlyArray<ServerProvider>>;
  }
>()("t3/provider/ProviderRegistry") {}

const loadProviders = (
  providerSources: ReadonlyArray<ProviderSnapshotSource>,
): Effect.Effect<ReadonlyArray<ServerProvider>> =>
  Effect.forEach(
    providerSources,
    (providerSource) =>
      providerSource.getSnapshot.pipe(
        Effect.flatMap((snapshot) => correlateSnapshotWithSource(providerSource, snapshot)),
      ),
    {
      concurrency: "unbounded",
    },
  );

const makeManualProviderMaintenanceCapabilities = (provider: ProviderDriverKind) =>
  makeManualOnlyProviderMaintenanceCapabilities({
    provider,
    packageName: null,
  });

const hasModelCapabilities = (model: ServerProvider["models"][number]): boolean =>
  (model.capabilities?.optionDescriptors?.length ?? 0) > 0;

const MAX_WORKSPACE_SNAPSHOTS_PER_PROVIDER = 16;

function dropProviderWorkspaceSnapshot(provider: ServerProvider, cwd: string): ServerProvider {
  return provider.workspaceSnapshots?.some((snapshot) => snapshot.cwd === cwd)
    ? {
        ...provider,
        workspaceSnapshots: provider.workspaceSnapshots.filter((snapshot) => snapshot.cwd !== cwd),
      }
    : provider;
}

export function upsertProviderWorkspaceSnapshot(
  provider: ServerProvider,
  cwd: string,
  scopedSnapshot: ProviderWorkspaceSnapshot,
): ServerProvider {
  const previousSnapshot = provider.workspaceSnapshots?.find((snapshot) => snapshot.cwd === cwd);
  const optionDescriptors = scopedSnapshot.optionDescriptors ?? previousSnapshot?.optionDescriptors;
  const workspaceSnapshot = {
    cwd,
    checkedAt: scopedSnapshot.checkedAt,
    slashCommands: scopedSnapshot.slashCommandsPending
      ? (previousSnapshot?.slashCommands ?? scopedSnapshot.slashCommands)
      : scopedSnapshot.slashCommands,
    ...(scopedSnapshot.slashCommandsPending ? { slashCommandsPending: true } : {}),
    skills: scopedSnapshot.skills,
    ...(optionDescriptors === undefined ? {} : { optionDescriptors }),
  } satisfies NonNullable<ServerProvider["workspaceSnapshots"]>[number];
  return {
    ...provider,
    workspaceSnapshots: [
      ...(provider.workspaceSnapshots ?? []).filter((snapshot) => snapshot.cwd !== cwd),
      workspaceSnapshot,
    ].slice(-MAX_WORKSPACE_SNAPSHOTS_PER_PROVIDER),
  };
}

const shouldRetainMissingProviderModels = (provider: ServerProvider): boolean => {
  if (provider.driver === ProviderDriverKind.make("pi")) {
    // Pi auth becomes known only after inventory discovery: empty means unauthenticated; failed/interactive discovery preserves unknown auth and cached models.
    return !(
      provider.installed &&
      (provider.status === "ready" || provider.status === "warning") &&
      provider.auth.status !== "unknown"
    );
  }

  if (provider.driver === ProviderDriverKind.make("acpRegistry")) {
    // Only completed ACP discovery provides nativeSessions and inventory; placeholder probes cannot establish auth for agents without sign-in.
    return !(
      provider.installed &&
      provider.status === "ready" &&
      provider.nativeSessions !== undefined
    );
  }

  if (provider.driver === ProviderDriverKind.make("grok")) {
    // Grok reports ready only after it discovers a model list; the pending,
    // ACP-failed, empty-inventory, and logged-out probes are warning/error.
    return !(provider.installed && provider.status === "ready");
  }

  const isAntigravity = provider.driver === ProviderDriverKind.make("antigravity");
  const isCodex = provider.driver === ProviderDriverKind.make("codex");
  if (!isAntigravity && !isCodex && provider.driver !== ProviderDriverKind.make("opencode")) {
    return true;
  }

  if (
    (isAntigravity || isCodex) &&
    (!provider.enabled || provider.auth.status === "unauthenticated")
  ) {
    return false;
  }

  // Antigravity's local health check does not authenticate or discover models.
  const isPendingAntigravityAuthentication =
    isAntigravity && provider.status === "warning" && provider.auth.status === "unknown";
  const isPendingInitialProbe =
    provider.enabled && !provider.installed && provider.status === "warning";
  const isIncompleteInstalledProbe =
    provider.installed &&
    (provider.status === "error" || (isCodex && provider.status === "warning"));
  return isPendingAntigravityAuthentication || isPendingInitialProbe || isIncompleteInstalledProbe;
};

const shouldRetainMissingOpenCodeMetadata = (provider: ServerProvider): boolean =>
  provider.driver === ProviderDriverKind.make("opencode") &&
  shouldRetainMissingProviderModels(provider);

const mergeProviderModels = (
  provider: ServerProvider,
  previousModels: ReadonlyArray<ServerProvider["models"][number]>,
  nextModels: ReadonlyArray<ServerProvider["models"][number]>,
): ReadonlyArray<ServerProvider["models"][number]> => {
  const shouldRetainMissingModels = shouldRetainMissingProviderModels(provider);
  const updateRequiredSlugs = new Set(provider.updateRequiredModels?.map((model) => model.slug));
  const retainablePreviousModels = previousModels.filter(
    (model) => !model.isCustom && !updateRequiredSlugs.has(model.slug),
  );

  if (shouldRetainMissingModels && nextModels.length === 0 && retainablePreviousModels.length > 0) {
    return retainablePreviousModels;
  }

  const previousBySlug = new Map(previousModels.map((model) => [model.slug, model] as const));
  const mergedModels = nextModels.map((model) => {
    const previousModel = previousBySlug.get(model.slug);
    if (
      !previousModel ||
      hasModelCapabilities(model) ||
      !hasModelCapabilities(previousModel) ||
      hasAuthoritativeEmptyModelOptions(provider, model)
    ) {
      return model;
    }
    return {
      ...model,
      capabilities: previousModel.capabilities,
    };
  });
  const nextSlugs = new Set(nextModels.map((model) => model.slug));
  return shouldRetainMissingModels
    ? [...mergedModels, ...retainablePreviousModels.filter((model) => !nextSlugs.has(model.slug))]
    : mergedModels;
};

/** Antigravity health checks only initialize; preserve previously verified Google auth across restarts. */
const carrySavedAntigravityAccount = (
  previousProvider: ServerProvider,
  nextProvider: ServerProvider,
): Pick<ServerProvider, "auth" | "status"> | undefined => {
  const antigravity = ProviderDriverKind.make("antigravity");
  if (
    nextProvider.driver !== antigravity ||
    previousProvider.driver !== antigravity ||
    !nextProvider.enabled ||
    nextProvider.auth.status !== "unknown" ||
    previousProvider.auth.status !== "authenticated" ||
    (nextProvider.auth.type !== undefined &&
      nextProvider.auth.type !== previousProvider.auth.type) ||
    (!nextProvider.installed && nextProvider.status !== "warning")
  ) {
    return undefined;
  }
  const status =
    nextProvider.installed && nextProvider.status === "warning" ? "ready" : nextProvider.status;
  return { auth: previousProvider.auth, status };
};

export const mergeProviderSnapshot = (
  previousProvider: ServerProvider | undefined,
  nextProvider: ServerProvider,
): ServerProvider => {
  if (!previousProvider) {
    return nextProvider;
  }
  const savedAccount = carrySavedAntigravityAccount(previousProvider, nextProvider);
  const { message: _uncheckedMessage, ...nextWithoutMessage } = nextProvider;
  return {
    ...(savedAccount?.status === "ready" ? nextWithoutMessage : nextProvider),
    ...savedAccount,
    models: mergeProviderModels(nextProvider, previousProvider.models, nextProvider.models),
    ...(nextProvider.workspaceSnapshots !== undefined
      ? { workspaceSnapshots: nextProvider.workspaceSnapshots }
      : previousProvider.workspaceSnapshots !== undefined
        ? { workspaceSnapshots: previousProvider.workspaceSnapshots }
        : {}),
    ...(shouldRetainMissingOpenCodeMetadata(nextProvider)
      ? {
          slashCommands:
            nextProvider.slashCommands.length === 0
              ? previousProvider.slashCommands
              : nextProvider.slashCommands,
          skills: nextProvider.skills.length === 0 ? previousProvider.skills : nextProvider.skills,
        }
      : {}),
  };
};

const haveProvidersChanged = (
  previousProviders: ReadonlyArray<ServerProvider>,
  nextProviders: ReadonlyArray<ServerProvider>,
): boolean => !Equal.equals(previousProviders, nextProviders);

const correlateSnapshotWithSource = (
  source: ProviderSnapshotSource,
  snapshot: ServerProvider,
): Effect.Effect<ServerProvider> => {
  if (snapshot.instanceId !== source.instanceId) {
    return Effect.die(
      new Error(
        `Provider snapshot instance mismatch: source '${source.instanceId}' emitted '${snapshot.instanceId}'.`,
      ),
    );
  }
  if (snapshot.driver !== source.driverKind) {
    return Effect.die(
      new Error(
        `Provider snapshot driver mismatch for instance '${source.instanceId}': source '${source.driverKind}' emitted '${snapshot.driver}'.`,
      ),
    );
  }
  return Effect.succeed(snapshot);
};

const snapshotInstanceKey = (provider: ServerProvider): ProviderInstanceId => {
  return provider.instanceId;
};

// Rebuilt instances have new PubSubs; capture their fresh closures instead of a closed source.
type InstanceSnapshotSource = ProviderSnapshotSource & { readonly instance: ProviderInstance };

const buildSnapshotSource = (instance: ProviderInstance): InstanceSnapshotSource => ({
  instance,
  instanceId: instance.instanceId,
  driverKind: instance.driverKind,
  getSnapshot: instance.snapshot.getSnapshot,
  refresh: instance.snapshot.refresh,
  streamChanges: instance.snapshot.streamChanges,
});

export const layer = Layer.effect(
  ProviderRegistry,
  Effect.gen(function* () {
    const instanceRegistry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
    const instanceRegistryMutator =
      yield* ProviderInstanceRegistryMutator.ProviderInstanceRegistryMutator;
    const manifestService = yield* ModelManifest.ModelManifest;
    const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
    const serviceScope = yield* Effect.scope;
    const config = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const changesPubSub = yield* Effect.acquireRelease(
      PubSub.unbounded<ReadonlyArray<ServerProvider>>(),
      PubSub.shutdown,
    );

    const bootInstances = yield* instanceRegistry.listInstances;
    const bootSources = bootInstances.map(buildSnapshotSource);
    const fallbackProviders = yield* loadProviders(bootSources);
    const fallbackByInstance = new Map<ProviderInstanceId, ServerProvider>();
    for (let index = 0; index < fallbackProviders.length; index++) {
      const provider = fallbackProviders[index];
      const source = bootSources[index];
      if (provider === undefined || source === undefined) {
        continue;
      }
      fallbackByInstance.set(source.instanceId, provider);
    }

    const cachedProviders = yield* Effect.forEach(
      bootSources,
      (source) =>
        Effect.gen(function* () {
          const filePath = yield* resolveProviderStatusCachePath({
            cacheDir: config.providerStatusCacheDir,
            instanceId: source.instanceId,
          }).pipe(Effect.provideService(Path.Path, path));
          const fallbackProvider = fallbackByInstance.get(source.instanceId);
          if (fallbackProvider === undefined) {
            return undefined;
          }
          return yield* readProviderStatusCache(filePath).pipe(
            Effect.provideService(FileSystem.FileSystem, fileSystem),
            Effect.flatMap((cachedProvider) => {
              if (cachedProvider === undefined) {
                return Effect.void.pipe(Effect.as(undefined as ServerProvider | undefined));
              }
              const correlation = {
                cachedProvider,
                fallbackProvider,
              } as const;
              if (!isCachedProviderCorrelated(correlation)) {
                return Effect.logWarning("provider status cache identity mismatch, ignoring", {
                  path: filePath,
                  instanceId: source.instanceId,
                  cachedInstanceId: cachedProvider.instanceId ?? null,
                  driver: source.driverKind,
                  cachedDriver: cachedProvider.driver ?? null,
                }).pipe(Effect.as(undefined as ServerProvider | undefined));
              }
              return Effect.succeed(hydrateCachedProvider(correlation));
            }),
          );
        }),
      { concurrency: "unbounded" },
    ).pipe(
      Effect.map((providers) =>
        orderProviderSnapshots(
          providers.filter((provider): provider is ServerProvider => provider !== undefined),
        ),
      ),
    );
    const initialManifest = yield* manifestService.current;
    const classifyCompatibility = (
      provider: ServerProvider,
      manifest: ModelManifest.ModelManifestData,
    ) =>
      applyProviderCompatibility(
        provider,
        manifest.compatibility,
        ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility,
      );
    const providersRef = yield* Ref.make<ReadonlyArray<ServerProvider>>(
      cachedProviders
        .filter((provider) =>
          bootInstances.some(
            (instance) =>
              instance.instanceId === provider.instanceId &&
              instanceRegistry.getCatalogSource(instance.instanceId) === instance,
          ),
        )
        .map((provider) => classifyCompatibility(provider, initialManifest)),
    );
    const workspaceRefreshesRef = yield* Ref.make<
      ReadonlyMap<ProviderInstance, ReadonlyMap<string, Deferred.Deferred<void>>>
    >(new Map());
    const maintenanceActionStatesRef = yield* Ref.make<
      ReadonlyMap<ProviderInstanceId, { readonly update?: ServerProviderUpdateState | undefined }>
    >(new Map());

    const liveSubsRef = yield* Ref.make<ReadonlyMap<ProviderInstanceId, ProviderInstance>>(
      new Map(),
    );
    const syncSemaphore = yield* Semaphore.make(1);

    const claimCachePublication = makeCatalogCachePublication();
    type CachePublication = ReturnType<typeof claimCachePublication>;

    // Read and publish the unbounded aggregate without yielding between them.
    const publishCurrentProviders = Effect.sync(() => {
      const providers = Ref.getUnsafe(providersRef);
      PubSub.publishUnsafe(changesPubSub, providers);
      return providers;
    });

    const persistProvider = Effect.fn("ProviderRegistry.persistProvider")(function* (
      provider: ServerProvider,
      publication: CachePublication,
    ) {
      const key = snapshotInstanceKey(provider);
      const filePath = yield* resolveProviderStatusCachePath({
        cacheDir: config.providerStatusCacheDir,
        instanceId: key,
      }).pipe(Effect.provideService(Path.Path, path));
      const { workspaceSnapshots: _workspaceSnapshots, ...machineProvider } = provider;
      yield* publication
        .run(
          writeProviderStatusCache({
            filePath,
            provider: machineProvider,
            isCurrent: publication.isCurrent,
          }),
        )
        .pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          Effect.tapError(Effect.logError),
          Effect.ignore,
        );
    });

    const applyProviderUpdateState = (provider: ServerProvider): ServerProvider => {
      const maintenanceActionStates = Ref.getUnsafe(maintenanceActionStatesRef);
      const updateState = maintenanceActionStates.get(provider.instanceId)?.update;
      if (!updateState) {
        const { updateState: _updateState, ...providerWithoutUpdateState } = provider;
        return providerWithoutUpdateState;
      }
      return {
        ...provider,
        updateState,
      };
    };

    const upsertProviders = Effect.fn("upsertProviders")(function* (
      nextProviders: ReadonlyArray<ServerProvider>,
      options?: {
        readonly publish?: boolean;
        readonly persist?: boolean;
        readonly persistUnchanged?: boolean;
        readonly replace?: boolean;
        readonly isCurrent?: (provider: ServerProvider) => boolean;
      },
    ) {
      const manifest = yield* manifestService.current;
      const [previousProviders, providers, publications] = yield* Ref.modify(
        providersRef,
        (previousProviders) => {
          const mergedProviders = new Map(
            previousProviders.map((provider) => [snapshotInstanceKey(provider), provider] as const),
          );
          const updatedKeys = new Set<ProviderInstanceId>();

          for (const nextProvider of nextProviders) {
            if (options?.isCurrent && !options.isCurrent(nextProvider)) continue;
            const provider = applyProviderUpdateState(nextProvider);
            const key = snapshotInstanceKey(provider);
            updatedKeys.add(key);
            mergedProviders.set(
              key,
              options?.replace === true
                ? provider
                : mergeProviderSnapshot(mergedProviders.get(key), provider),
            );
          }

          const providers =
            nextProviders.length > 0 && updatedKeys.size === 0
              ? previousProviders
              : orderProviderSnapshots(
                  [...mergedProviders.values()].map((provider) =>
                    classifyCompatibility(provider, manifest),
                  ),
                );
          const publications =
            options?.persist === false ||
            (!options?.persistUnchanged && !haveProvidersChanged(previousProviders, providers))
              ? []
              : providers
                  .filter((provider) => updatedKeys.has(snapshotInstanceKey(provider)))
                  .map((provider) => ({
                    provider,
                    publication: claimCachePublication(
                      provider.instanceId,
                      () => options?.isCurrent?.(provider) ?? true,
                    ),
                  }));
          return [[previousProviders, providers, publications] as const, providers];
        },
      );

      yield* Effect.forEach(
        publications,
        ({ provider, publication }) => persistProvider(provider, publication),
        { concurrency: "unbounded", discard: true },
      );
      if (options?.publish !== false && haveProvidersChanged(previousProviders, providers)) {
        yield* publishCurrentProviders;
      }

      return yield* Ref.get(providersRef);
    });

    const compatibilityRefreshRunning = yield* Ref.make(false);
    const syncProvider = Effect.fn("syncProvider")(function* (
      instance: ProviderInstance,
      provider: ServerProvider,
      options?: {
        readonly publish?: boolean;
        readonly persistUnchanged?: boolean;
      },
    ) {
      yield* upsertProviders([provider], {
        ...options,
        isCurrent: () => instanceRegistry.getCatalogSource(instance.instanceId) === instance,
      });
      // Reclassify the current model after fetch; republishing the old probe could overwrite newer health results.
      if (!(yield* Ref.getAndSet(compatibilityRefreshRunning, true))) {
        yield* manifestService.refresh.pipe(
          Effect.andThen(upsertProviders([], { persist: false })),
          Effect.ensuring(Ref.set(compatibilityRefreshRunning, false)),
          Effect.forkIn(serviceScope),
        );
      }
      return yield* Ref.get(providersRef);
    });

    const setProviderMaintenanceActionState = Effect.fn("setProviderMaintenanceActionState")(
      function* (input: {
        readonly instanceId: ProviderInstanceId;
        readonly action: "update";
        readonly state: ServerProviderUpdateState | null;
      }) {
        yield* Ref.update(maintenanceActionStatesRef, (previous) => {
          const previousActions = previous.get(input.instanceId);
          const nextActions = { ...previousActions };
          if (input.state === null || input.state.status === "idle") {
            delete nextActions[input.action];
          } else {
            nextActions[input.action] = input.state;
          }

          const next = new Map(previous);
          if (Object.keys(nextActions).length === 0) {
            next.delete(input.instanceId);
          } else {
            next.set(input.instanceId, nextActions);
          }
          return next;
        });

        return yield* updateProviders((providers) =>
          providers.map((provider) => {
            if (provider.instanceId !== input.instanceId) return provider;
            const updateState = Ref.getUnsafe(maintenanceActionStatesRef).get(
              input.instanceId,
            )?.update;
            const { updateState: _previousUpdateState, ...withoutUpdateState } = provider;
            return updateState ? { ...provider, updateState } : withoutUpdateState;
          }),
        );
      },
    );

    // The catalog source, not the live registry, decides which instance owns an id.
    const isCurrentInstance = (instance: ProviderInstance) =>
      Effect.sync(() => instanceRegistry.getCatalogSource(instance.instanceId) === instance);

    const refreshOneSource = Effect.fn("refreshOneSource")(function* (instance: ProviderInstance) {
      if (!(yield* isCurrentInstance(instance))) {
        return yield* Ref.get(providersRef);
      }
      const providerSource = buildSnapshotSource(instance);
      // Enrichment can publish a newer snapshot before refresh returns its pre-enrichment probe.
      const probe = yield* providerSource.refresh.pipe(
        Effect.andThen(providerSource.getSnapshot),
        Effect.exit,
      );
      // Settings can replace or remove the instance mid-probe; its late result or failure is stale.
      if (!(yield* isCurrentInstance(instance))) {
        return yield* Ref.get(providersRef);
      }
      const nextProvider = yield* probe;
      return yield* correlateSnapshotWithSource(providerSource, nextProvider).pipe(
        Effect.flatMap((provider) => syncProvider(instance, provider)),
      );
    });

    type RefreshAllCompletion = Deferred.Deferred<void>;
    const refreshAllInFlightRef = yield* Ref.make<Option.Option<RefreshAllCompletion>>(
      Option.none(),
    );

    // What the last full pass could see; a later manifest fetch does not reach providers by itself.
    const probedManifestUpdatedAt = yield* Ref.make(initialManifest.updatedAt);

    const refreshAll = Effect.fn("refreshAll")(function* () {
      const claimed = yield* Ref.modify(
        refreshAllInFlightRef,
        (
          inFlight,
        ): readonly [
          { readonly owner: boolean; readonly completion: RefreshAllCompletion },
          Option.Option<RefreshAllCompletion>,
        ] => {
          if (Option.isSome(inFlight)) {
            return [{ owner: false, completion: inFlight.value }, inFlight];
          }
          const completion = Deferred.makeUnsafe<void>();
          return [{ owner: true, completion }, Option.some(completion)];
        },
      );
      if (claimed.owner) {
        // Detach shared passes so the first caller's interruption cannot cancel other callers' work.
        // Syncing first keeps a refresh right after a rebuild from probing the replaced instance.
        yield* manifestService.current.pipe(
          Effect.flatMap(({ updatedAt }) =>
            syncLiveSourcesAndContinue.pipe(
              Effect.andThen(Ref.get(liveSubsRef)),
              Effect.flatMap((instances) =>
                Effect.forEach(instances.values(), (instance) => refreshOneSource(instance), {
                  concurrency: "unbounded",
                  discard: true,
                }),
              ),
              Effect.andThen(Ref.get(providersRef)),
              Effect.tap(() => Ref.set(probedManifestUpdatedAt, updatedAt)),
            ),
          ),
          Effect.asVoid,
          Effect.onExit((exit) =>
            Ref.set(refreshAllInFlightRef, Option.none()).pipe(
              Effect.andThen(Deferred.done(claimed.completion, exit)),
            ),
          ),
          Effect.forkDetach,
        );
      }
      return yield* Deferred.await(claimed.completion).pipe(Effect.andThen(Ref.get(providersRef)));
    });

    const refresh = Effect.fn("refresh")(function* (provider?: ProviderDriverKind) {
      if (provider === undefined) {
        return yield* refreshAll();
      }
      const instance = (yield* Ref.get(liveSubsRef)).get(defaultInstanceIdForDriver(provider));
      if (!instance) {
        return yield* Ref.get(providersRef);
      }
      return yield* refreshOneSource(instance);
    });

    const refreshInstance = Effect.fn("refreshInstance")(function* (
      instanceId: ProviderInstanceId,
    ) {
      yield* syncLiveSourcesAndContinue;
      const instance = (yield* Ref.get(liveSubsRef)).get(instanceId);
      if (!instance) {
        return yield* Ref.get(providersRef);
      }
      return yield* refreshOneSource(instance);
    });

    const reloadSecretsAndRefresh = Effect.fn("reloadSecretsAndRefresh")(function* (
      instanceId?: ProviderInstanceId,
    ) {
      yield* instanceRegistryMutator.reloadSecrets(instanceId);
      return yield* instanceId === undefined ? refresh() : refreshInstance(instanceId);
    });

    const refreshAfterUpdate = Effect.fn("refreshAfterUpdate")(function* (
      provider: ProviderDriverKind,
      selectedInstanceId: ProviderInstanceId,
    ) {
      yield* manifestService.forceRefresh;
      const instances = (yield* instanceRegistry.listInstances).filter(
        (instance) =>
          instance.driverKind === provider &&
          (instance.enabled || instance.instanceId === selectedInstanceId),
      );
      const failedInstances = new Set<ProviderInstanceId>();
      const recoverInstanceFailure = Effect.fn("recoverPostUpdateRefreshFailure")(function* (
        instance: ProviderInstance,
        cause: Cause.Cause<unknown>,
      ) {
        const instanceId = instance.instanceId;
        if (Cause.hasInterruptsOnly(cause)) {
          return yield* Effect.interrupt;
        }
        yield* Effect.logError("provider post-update refresh failed", {
          instanceId,
          cause: Cause.pretty(cause),
        });
        failedInstances.add(instanceId);
        // A cached ready snapshot cannot verify a successful update.
        return yield* updateProviders((providers) =>
          instanceRegistry.getCatalogSource(instanceId) !== instance
            ? providers
            : providers.map((candidate) =>
                candidate.instanceId === instanceId
                  ? {
                      ...candidate,
                      status: "error",
                      message: "Provider refresh failed after update.",
                    }
                  : candidate,
              ),
        );
      });
      // Accounts can share an installation, but discovery must use each
      // instance's own executable, environment, and credentials.
      yield* Effect.forEach(
        instances,
        (instance) =>
          Effect.gen(function* () {
            yield* instance.invalidateCaches ?? Effect.void;
            const maintenance = yield* instance.snapshot.resolveMaintenance({ fresh: true });
            yield* Effect.forEach(
              latestProviderVersionCacheKeys(maintenance),
              latestVersions.invalidate,
              { discard: true },
            );
          }).pipe(Effect.catchCause((cause) => recoverInstanceFailure(instance, cause))),
        { concurrency: "unbounded", discard: true },
      );
      yield* Effect.forEach(
        instances.filter((instance) => !failedInstances.has(instance.instanceId)),
        (instance) =>
          refreshOneSource(instance).pipe(
            Effect.catchCause((cause) => recoverInstanceFailure(instance, cause)),
          ),
        { concurrency: "unbounded", discard: true },
      );
      // Read the registry and the cache in one synchronous step, so a replacement cannot slip between.
      return yield* Effect.sync(() =>
        Ref.getUnsafe(providersRef).filter((candidate) =>
          instances.some(
            (instance) =>
              instance.instanceId === candidate.instanceId &&
              !failedInstances.has(instance.instanceId) &&
              instanceRegistry.getCatalogSource(instance.instanceId) === instance,
          ),
        ),
      );
    });

    const getProviderMaintenanceCapabilitiesForInstance = Effect.fn(
      "getProviderMaintenanceCapabilitiesForInstance",
    )(function* (
      instanceId: ProviderInstanceId,
      provider: ProviderDriverKind,
      options?: { readonly fresh?: boolean },
    ) {
      // liveSubsRef trails reconciliation and could run a retired instance's command; read the registry.
      const instance = yield* instanceRegistry.getInstance(instanceId);
      if (!instance || instance.driverKind !== provider) {
        return makeManualProviderMaintenanceCapabilities(provider);
      }
      return yield* instance.snapshot.resolveMaintenance(options);
    });

    const updateProviders = (
      update: (providers: ReadonlyArray<ServerProvider>) => ReadonlyArray<ServerProvider>,
    ) =>
      Ref.modify(providersRef, (currentProviders) => {
        const nextProviders = update(currentProviders);
        if (haveProvidersChanged(currentProviders, nextProviders)) {
          PubSub.publishUnsafe(changesPubSub, nextProviders);
        }
        return [nextProviders, nextProviders] as const;
      });

    const clearProviderCache = Effect.fn("ProviderRegistry.clearProviderCache")(function* (
      instanceId: ProviderInstanceId,
      publication: CachePublication,
    ) {
      const filePath = yield* resolveProviderStatusCachePath({
        cacheDir: config.providerStatusCacheDir,
        instanceId,
      }).pipe(Effect.provideService(Path.Path, path));
      yield* publication
        .run(fileSystem.remove(filePath, { force: true }))
        .pipe(Effect.tapError(Effect.logError), Effect.ignore);
    });

    const syncLiveSources = Effect.fn("ProviderRegistry.syncLiveSources")(function* (
      waitForInitial: boolean,
    ) {
      const { newlyAdded, unavailableProviders, invalidations } = yield* syncSemaphore.withPermits(
        1,
      )(
        Effect.gen(function* () {
          const instances = yield* instanceRegistry.listInstances;
          const unavailableProviders = yield* instanceRegistry.listUnavailable;
          const previousSubs = yield* Ref.get(liveSubsRef);
          const nextByInstance = new Map(
            [...previousSubs.values(), ...instances]
              .filter(
                (instance) => instanceRegistry.getCatalogSource(instance.instanceId) === instance,
              )
              .map((instance) => [instance.instanceId, instance] as const),
          );
          const newlyAdded = [...nextByInstance.values()].filter(
            (instance) => previousSubs.get(instance.instanceId) !== instance,
          );
          const invalidations = new Map<ProviderInstanceId, CachePublication>();
          for (const [id, previous] of previousSubs) {
            if (nextByInstance.get(id) === previous) continue;
            const nextSource = instanceRegistry.getCatalogSource(id);
            if (
              nextSource !== undefined &&
              nextSource !== nextByInstance.get(id) &&
              !unavailableProviders.some((provider) => provider === nextSource)
            )
              continue;
            invalidations.set(
              id,
              claimCachePublication(id, () => instanceRegistry.getCatalogSource(id) === nextSource),
            );
          }

          // Install subscription ownership before any initial read or cache work can await.
          yield* Ref.set(liveSubsRef, nextByInstance);
          // Subscribe before reading so a probe completing mid-sync is observed by at least one path.
          for (const instance of newlyAdded) {
            const source = buildSnapshotSource(instance);
            yield* Stream.runForEach(source.streamChanges, (provider) =>
              correlateSnapshotWithSource(source, provider).pipe(
                Effect.flatMap((snapshot) => syncProvider(instance, snapshot)),
              ),
            ).pipe(Effect.forkIn(serviceScope, { startImmediately: true }));
          }
          yield* updateProviders((providers) =>
            providers
              .filter(
                (provider) => instanceRegistry.getCatalogSource(provider.instanceId) !== undefined,
              )
              .map((provider) => {
                if (
                  !invalidations.has(provider.instanceId) ||
                  instanceRegistry.getCatalogSource(provider.instanceId) !==
                    nextByInstance.get(provider.instanceId)
                )
                  return provider;
                const { workspaceSnapshots: _workspaceSnapshots, ...machineSnapshot } = provider;
                return machineSnapshot;
              }),
          );
          yield* Ref.update(
            maintenanceActionStatesRef,
            (previous) =>
              new Map(
                [...previous].filter(([id]) => instanceRegistry.getCatalogSource(id) !== undefined),
              ),
          );
          return { newlyAdded, unavailableProviders, invalidations };
        }),
      );

      const initialReads = Effect.forEach(
        newlyAdded,
        (instance) =>
          Effect.gen(function* () {
            const invalidation = invalidations.get(instance.instanceId);
            if (invalidation) yield* clearProviderCache(instance.instanceId, invalidation);
            if (instanceRegistry.getCatalogSource(instance.instanceId) !== instance) return;
            const source = buildSnapshotSource(instance);
            const provider = yield* source.getSnapshot;
            yield* correlateSnapshotWithSource(source, provider).pipe(
              Effect.flatMap((snapshot) =>
                syncProvider(instance, snapshot, {
                  persistUnchanged: invalidation !== undefined,
                }),
              ),
            );
          }).pipe(Effect.ignoreCause({ log: true })),
        { concurrency: "unbounded", discard: true },
      );
      const cleanupRemoved = Effect.forEach(
        [...invalidations].filter(
          ([id]) => !newlyAdded.some((instance) => instance.instanceId === id),
        ),
        ([id, publication]) => clearProviderCache(id, publication),
        { concurrency: "unbounded", discard: true },
      );
      yield* upsertProviders(unavailableProviders, {
        persist: false,
        replace: true,
        isCurrent: (provider) =>
          instanceRegistry.getCatalogSource(provider.instanceId) === provider,
      });
      const work = Effect.all([initialReads, cleanupRemoved], {
        concurrency: "unbounded",
        discard: true,
      });
      if (waitForInitial) yield* work;
      else yield* work.pipe(Effect.forkIn(serviceScope));
    });
    const syncLiveSourcesAndContinue = syncLiveSources(false).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logError(
          "provider registry instance sync failed; keeping subscription alive",
          {
            cause: Cause.pretty(cause),
          },
        );
      }),
      Scope.provide(serviceScope),
    );

    yield* upsertProviders(fallbackProviders, {
      publish: false,
      isCurrent: (provider) =>
        bootInstances.some(
          (instance) =>
            instance.instanceId === provider.instanceId &&
            instanceRegistry.getCatalogSource(instance.instanceId) === instance,
        ),
    });
    const instanceChanges = yield* instanceRegistry.subscribeChanges;
    yield* syncLiveSources(true);
    yield* Stream.runForEach(
      Stream.fromSubscription(instanceChanges),
      () => syncLiveSourcesAndContinue,
    ).pipe(Effect.forkScoped);

    const recoverRefreshFailure = Effect.fn("recoverRefreshFailure")(function* (
      cause: Cause.Cause<unknown>,
    ) {
      if (Cause.hasInterruptsOnly(cause)) {
        return yield* Effect.interrupt;
      }
      yield* Effect.logError("provider registry refresh failed; preserving cached providers", {
        cause: Cause.pretty(cause),
      });
      return yield* Ref.get(providersRef);
    });

    const refreshWorkspaceSnapshot = Effect.fn("refreshWorkspaceSnapshot")(function* (input: {
      readonly instanceId: ProviderInstanceId;
      readonly cwd: string;
      readonly fresh?: boolean;
    }) {
      if (input.fresh) {
        yield* updateProviders((providers) =>
          providers.map((candidate) =>
            candidate.instanceId === input.instanceId
              ? candidate
              : dropProviderWorkspaceSnapshot(candidate, input.cwd),
          ),
        );
      }
      const providers = yield* Ref.get(providersRef);
      const provider = providers.find((candidate) => candidate.instanceId === input.instanceId);
      const workspaceSnapshotOf = (candidate: ServerProvider | undefined) =>
        candidate?.workspaceSnapshots?.find((s) => s.cwd === input.cwd);
      const scannedFrom = workspaceSnapshotOf(provider);
      const now = yield* DateTime.now;
      if (
        !provider ||
        !provider.enabled ||
        (!input.fresh &&
          scannedFrom &&
          !scannedFrom.slashCommandsPending &&
          isProviderWorkspaceSnapshotCurrent(scannedFrom, DateTime.toEpochMillis(now)))
      ) {
        return yield* Ref.get(providersRef);
      }
      // Use scan start for TTL, not checkedAt inherited from the machine health check.
      const scannedAt = DateTime.formatIso(now);
      const instance = yield* instanceRegistry.getInstance(input.instanceId);
      if (!instance?.snapshotForCwd) return yield* Ref.get(providersRef);
      const token = yield* Deferred.make<void>();
      const refreshMachineSnapshot = input.fresh
        ? (instance.invalidateCaches ?? Effect.void).pipe(
            Effect.andThen(refreshInstance(input.instanceId)),
          )
        : Effect.void;
      const runScan = refreshMachineSnapshot.pipe(
        Effect.andThen(instance.snapshotForCwd(input.cwd)),
        // Scans fail on error; returned inventory remains valid even when provider health reports an error.
        Effect.flatMap((scopedSnapshot) =>
          instanceRegistry.getInstance(input.instanceId).pipe(
            Effect.flatMap((currentInstance) => {
              if (currentInstance !== instance) return Ref.get(providersRef);
              return updateProviders((currentProviders) => {
                if (instanceRegistry.getCatalogSource(input.instanceId) !== instance) {
                  return currentProviders;
                }
                // Read ownership inside the update so a newer claim cannot race publication.
                if (Ref.getUnsafe(workspaceRefreshesRef).get(instance)?.get(input.cwd) !== token) {
                  return currentProviders;
                }
                return currentProviders.map((candidate) =>
                  candidate.instanceId === input.instanceId &&
                  Equal.equals(workspaceSnapshotOf(candidate), scannedFrom)
                    ? upsertProviderWorkspaceSnapshot(candidate, input.cwd, {
                        ...scopedSnapshot,
                        checkedAt: scannedAt,
                        // Without driver command discovery, retain previous commands after failed health checks and retry.
                        slashCommandsPending:
                          scopedSnapshot.slashCommandsPending ?? scopedSnapshot.status === "error",
                      })
                    : candidate,
                );
              });
            }),
          ),
        ),
        Effect.ensuring(
          Ref.update(workspaceRefreshesRef, (refreshes) => {
            const current = refreshes.get(instance);
            if (current?.get(input.cwd) !== token) return refreshes;
            const next = new Map(refreshes);
            const nextCwds = new Map(current);
            nextCwds.delete(input.cwd);
            if (nextCwds.size) next.set(instance, nextCwds);
            else next.delete(instance);
            return next;
          }).pipe(Effect.andThen(Deferred.succeed(token, undefined))),
        ),
      );
      // The claim and the fork happen together, so the token is always completed. The scan is
      // detached so a disconnecting owner cannot cancel the result its waiters depend on.
      const claim = yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const inFlight = yield* Ref.modify(workspaceRefreshesRef, (refreshes) => {
            const current = refreshes.get(instance);
            const running = current?.get(input.cwd);
            if (!input.fresh && running) return [running, refreshes] as const;
            const next = new Map(refreshes);
            next.set(instance, new Map(current).set(input.cwd, token));
            return [undefined, next] as const;
          });
          if (inFlight) return { inFlight } as const;
          return {
            // Starting now attaches the scan's `ensuring` before anything can close the scope.
            scan: yield* Effect.forkIn(runScan, serviceScope, { startImmediately: true }),
          } as const;
        }),
      );
      // Wait for the running scan: on-demand clients get its result only in this reply.
      if ("inFlight" in claim) {
        return yield* Deferred.await(claim.inFlight).pipe(Effect.andThen(Ref.get(providersRef)));
      }
      return yield* Fiber.join(claim.scan);
    });

    const invalidateModelCaches = Effect.fn("ProviderRegistry.invalidateModelCaches")(function* (
      input: Parameters<ProviderRegistry["Service"]["invalidateModelCaches"]>[0],
    ) {
      yield* manifestService.forceRefresh;
      const instances = yield* instanceRegistry.listInstances;
      yield* Effect.forEach(
        instances.filter(
          (instance) => input.instanceId === undefined || input.instanceId === instance.instanceId,
        ),
        (instance) =>
          Effect.gen(function* () {
            yield* instance.invalidateCaches ?? Effect.void;
            // Instance and full refreshes resolve installer metadata fresh
            // themselves; a cached workspace refresh needs it resolved here.
            const maintenance = yield* instance.snapshot.resolveMaintenance({
              fresh:
                input.instanceId !== undefined && input.cwd !== undefined && input.fresh !== true,
            });
            yield* Effect.forEach(
              latestProviderVersionCacheKeys(maintenance),
              latestVersions.invalidate,
              { discard: true },
            );
          }),
        { concurrency: "unbounded", discard: true },
      );
    });

    const refreshModelCatalog = Effect.fn("ProviderRegistry.refreshModelCatalog")(function* () {
      const { updatedAt } = yield* manifestService.refreshOnDemand;
      // The first pass can be one that started before the fetch, so allow one more.
      for (let pass = 0; pass < 2; pass++) {
        if ((yield* Ref.get(probedManifestUpdatedAt)) === updatedAt) return;
        // A new model usually ships with the CLI release that supports it.
        yield* latestVersions.invalidateAll;
        yield* refreshAll();
      }
    });

    return {
      getProviders: Ref.get(providersRef),
      refresh: (provider?: ProviderDriverKind) =>
        refresh(provider).pipe(Effect.catchCause(recoverRefreshFailure)),
      refreshInstance: (instanceId: ProviderInstanceId) =>
        refreshInstance(instanceId).pipe(Effect.catchCause(recoverRefreshFailure)),
      reloadSecretsAndRefresh: (instanceId?: ProviderInstanceId) =>
        reloadSecretsAndRefresh(instanceId).pipe(Effect.catchCause(recoverRefreshFailure)),
      refreshWorkspaceSnapshot: (input) =>
        refreshWorkspaceSnapshot(input).pipe(Effect.catchCause(recoverRefreshFailure)),
      refreshAfterUpdate,
      invalidateModelCaches,
      refreshModelCatalog: refreshModelCatalog().pipe(
        Effect.catchCause(recoverRefreshFailure),
        Effect.asVoid,
      ),
      getProviderMaintenanceCapabilitiesForInstance,
      setProviderMaintenanceActionState,
      get streamChanges() {
        return Stream.fromPubSub(changesPubSub);
      },
    } satisfies ProviderRegistry["Service"];
  }),
);
