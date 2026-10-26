import * as Context from "effect/Context";
import type { AdmittedCaller } from "./mode/authorityMode.ts";
import * as NativeSession from "./registry/session.ts";
import * as NativeRegistry from "./registry/service.ts";
import * as NativeEffectRunner from "./registry/effectRunner.ts";
import * as NativeAppearance from "./registry/appearance.ts";
import * as NativeCounts from "./registry/counts.ts";
import * as NativePlacements from "./registry/placements.ts";
import * as NativeRegistration from "./registry/registration.ts";
import * as NativeLocalTransport from "./registry/localTransport.ts";
import type * as RegistrationContext from "../registrationContext/service.ts";
import * as Transition from "./transition/cli.ts";
import * as DurableEntries from "./operations/durableEntries.ts";
import * as HomeExecutor from "./operations/homeExecutor.ts";
import { ExecutionContext } from "../operations/ports.ts";
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
).pipe(Layer.provideMerge(sqlLayer));

const sourceProviderLayer = Layer.effect(
  LocalAttestation.NativeProvider,
  Effect.map(NativeRuntime.NativeWorkstreamsRuntime, (runtime) => runtime.providerV2),
);

export const nativeSourceProvidersLayer = Layer.merge(
  LocalAttestation.layer,
  LocalSettlement.layer,
).pipe(Layer.provide(sourceProviderLayer), Layer.provide(Attempts.NativeProviderAttemptsLive));

export const nativeOperationsLayer = Layer.mergeAll(DurableEntries.layer, HomeExecutor.layer).pipe(
  Layer.provideMerge(nativePersistenceLayer),
);

export const nativeOperationsExecutionLayer = Layer.effect(
  ExecutionContext,
  Effect.gen(function* () {
    return {
      mode: "native" as const,
      entries: yield* DurableEntries.DurableEntries,
      executor: yield* HomeExecutor.HomeExecutor,
    };
  }),
).pipe(Layer.provideMerge(nativeOperationsLayer));

export const nativeTransitionLayer = Transition.layer.pipe(
  Layer.provideMerge(nativePersistenceLayer),
);

const nativeRegistryLayer = NativeRegistry.layer.pipe(
  Layer.provideMerge(NativeSession.layer),
  Layer.provideMerge(nativePersistenceLayer),
);

/** Requires explicit NativeHome, source/provider routing and source trust. No mode is inferred here. */
export const nativeHomeRuntimeLayer = NativeLocalTransport.layer.pipe(
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

export class NativeEntry extends Context.Service<
  NativeEntry,
  {
    readonly caller: AdmittedCaller;
    readonly adapter: NativeLocalTransport.NativeAdapter;
    readonly registration: RegistrationContext.WorkstreamsRegistrationContext["Service"];
  }
>()("t3/jones/workstreams/native/integration/NativeEntry") {}

/** Apply only inside a source-owned, admitted native entry context. */
export const nativeEntryLayer = (caller: AdmittedCaller) =>
  Layer.effect(
    NativeEntry,
    Effect.gen(function* () {
      const local = yield* NativeLocalTransport.LocalTransport;
      const registration = yield* NativeRegistration.Registration;
      return {
        caller,
        adapter: yield* local.forCaller(caller),
        registration: registration.forCaller(caller),
      };
    }),
  );

const admittedHomeTransportLayer = Layer.effect(
  HomeExecutor.HomeTransport,
  Effect.map(NativeEntry, (entry) => entry.adapter.homeTransport),
);

/** Source admission selects this layer; it must never be installed as a global native default. */
export const nativeAdmittedOperationsExecutionLayer = Layer.effect(
  ExecutionContext,
  Effect.gen(function* () {
    const entry = yield* NativeEntry;
    return {
      mode: "native" as const,
      entries: yield* DurableEntries.DurableEntries,
      executor: entry.adapter.guardExecutor(yield* HomeExecutor.HomeExecutor),
    };
  }),
).pipe(Layer.provideMerge(nativeOperationsLayer), Layer.provide(admittedHomeTransportLayer));
