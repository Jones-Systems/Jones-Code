import * as NativeReadProjection from "./read/projection.ts";
import * as NativeReadRoutes from "./read/routes.ts";
import * as RollupBindings from "./rollups/bindings.ts";
import * as RollupStore from "./rollups/store.ts";
import * as RollupManifests from "./rollups/manifestSource.ts";
import * as RollupSummary from "./rollups/summaryRead.ts";
import * as RollupDashboard from "./rollups/dashboard.ts";
import * as RollupRoutes from "./rollups/routes.ts";
import * as RollupApplication from "./rollups/application.ts";
import * as SourceEnrollments from "./runtime/sourceEnrollment.ts";
import * as SourceEnrollmentStore from "./store/sourceEnrollmentStore.ts";
import * as SourceAuthority from "../../../environment/NativeStoreAuthority.ts";
import { readNativeProviderBuild } from "../nativeProvider/service.ts";
import * as NativeSession from "./registry/session.ts";
import * as NativeRegistry from "./registry/service.ts";
import * as NativeEffectRunner from "./registry/effectRunner.ts";
import * as NativeAppearance from "./registry/appearance.ts";
import * as NativeCounts from "./registry/counts.ts";
import * as NativePlacements from "./registry/placements.ts";
import * as NativeRegistration from "./registry/registration.ts";
import * as NativeLocalTransport from "./registry/localTransport.ts";
import * as Layer from "effect/Layer";
import * as KeyProvider from "./crypto/keyProvider.ts";
import * as Envelope from "./crypto/envelope.ts";
import * as IdentityIndex from "./crypto/identityIndex.ts";
import * as NativeSql from "./store/sql.ts";
import * as RegistryStore from "./store/registryStore.ts";
import * as ArchiveStore from "./store/archiveStore.ts";
import * as ReceiptStore from "./store/receiptStore.ts";
import * as HistoryStore from "./store/historyStore.ts";
import * as EffectStore from "./store/effectStore.ts";
import * as BindingStore from "./store/bindingStore.ts";
import * as OperationStore from "./store/operationStore.ts";
import * as Effect from "effect/Effect";
import * as NativeRuntime from "../runtimeIntegration/native.ts";
import * as Attempts from "../nativeProvider/attemptRepository.ts";
import * as LocalAttestation from "./providers/localAttestation.ts";
import * as LocalSettlement from "./providers/localSettlement.ts";

const receiptLayer = ReceiptStore.layer.pipe(Layer.provideMerge(ArchiveStore.layer));
const effectLayer = EffectStore.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(receiptLayer, HistoryStore.layer)),
);
const operationLayer = OperationStore.layer.pipe(Layer.provideMerge(BindingStore.layer));
const cryptoLayer = Envelope.layer.pipe(Layer.provideMerge(KeyProvider.layer));
const sqlLayer = NativeSql.layer.pipe(Layer.provideMerge(cryptoLayer));

export const nativePersistenceLayer = Layer.mergeAll(
  RegistryStore.layer,
  effectLayer,
  operationLayer,
  IdentityIndex.layer,
  SourceEnrollmentStore.layer,
).pipe(Layer.provideMerge(sqlLayer));

/** Selected native composition only; reuses application SQL and key resources. */
export const nativeSourceEnrollmentLayer = SourceEnrollments.layer.pipe(
  Layer.provide(
    Layer.effect(
      SourceEnrollments.SourceIssuer,
      Effect.map(SourceAuthority.NativeStoreAuthority, (authority) => ({
        authority,
        build: readNativeProviderBuild,
      })),
    ),
  ),
);

const sourceProviderLayer = Layer.effect(
  LocalAttestation.NativeProvider,
  Effect.gen(function* () {
    // Resolve at build time: startup and the source runtime import one another.
    const runtime = yield* NativeRuntime.NativeWorkstreamsRuntime;
    return runtime.providerV2;
  }),
);

export const nativeSourceProvidersLayer = Layer.merge(
  LocalAttestation.layer,
  LocalSettlement.layer,
).pipe(Layer.provide(sourceProviderLayer), Layer.provide(Attempts.NativeProviderAttemptsLive));

const nativeRegistryLayer = NativeRegistry.layer.pipe(
  Layer.provideMerge(NativeSession.layer),
  Layer.provideMerge(nativePersistenceLayer),
);

/** Requires explicit NativeHome, source/provider routing and source trust. No mode is inferred here. */
const nativeLocalRuntimeLayer = NativeLocalTransport.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      NativeEffectRunner.layer,
      NativeAppearance.layer,
      NativeCounts.layer,
      NativePlacements.layer,
      NativeRegistration.layer,
    ).pipe(Layer.provideMerge(nativeRegistryLayer)),
  ),
);

// Parent hook: explicit ports and the shared admission owner remain required.
const nativeRollupBindingsLayer = RollupBindings.layer.pipe(
  Layer.provideMerge(nativePersistenceLayer),
);
const nativeRollupStoreLayer = RollupStore.layer.pipe(
  Layer.provideMerge(nativeRollupBindingsLayer),
);
const nativeRollupManifestsLayer = RollupManifests.layer.pipe(
  Layer.provideMerge(nativeRollupBindingsLayer),
);
const nativeRollupSummaryLayer = RollupSummary.layer.pipe(
  Layer.provideMerge(nativeRollupStoreLayer),
  Layer.provideMerge(NativeSession.layer.pipe(Layer.provideMerge(nativePersistenceLayer))),
);
const nativeRollupDashboardLayer = RollupDashboard.layer.pipe(
  Layer.provideMerge(RollupDashboard.nativeClientLayer),
  Layer.provideMerge(nativeLocalRuntimeLayer),
);
// Read routes and startup share the source runtime; resolve their live bindings only at layer build.
const nativeRollupReadLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      NativeReadRoutes.RollupSummaryRead,
      Effect.map(RollupSummary.SummaryRead, (summaries) => ({
        read: Effect.fn("NativeRead.rollupSummary")(function* (caller, id) {
          const read = yield* summaries
            .read(caller, id)
            .pipe(
              Effect.mapError(
                (cause) => new NativeSession.RegistryError({ code: cause.code, cause }),
              ),
            );
          if (read.summary === null)
            return yield* new NativeSession.RegistryError({
              code:
                read.state === "provider_unconfigured"
                  ? "provider_unconfigured"
                  : "content_unavailable",
            });
          return read.summary;
        }),
      })),
    ).pipe(Layer.provideMerge(nativeRollupSummaryLayer)),
  ),
);

/** Source composition only; parent startup owns timers and admitted route mounts. */
export const nativeRollupRuntimeLayer = Layer.mergeAll(
  nativeRollupReadLayer,
  RollupRoutes.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(nativeRollupSummaryLayer, nativeRollupDashboardLayer)),
  ),
  RollupApplication.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(nativeRollupStoreLayer, nativeRollupManifestsLayer)),
  ),
);

/** Coherent home reads preserve optional rollup configuration at the composition boundary. */
export const nativeHomeRuntimeLayer = Layer.unwrap(
  Effect.sync(() =>
    NativeReadRoutes.layer.pipe(
      Layer.provideMerge(NativeReadProjection.layer),
      Layer.provideMerge(nativeLocalRuntimeLayer),
    ),
  ),
);
