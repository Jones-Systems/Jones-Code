import * as PeerPins from "./peerSessionPins.ts";
import * as PeerReadBoundary from "../forwarding/peerReadAdmission.ts";
import * as HomeReadProtocol from "../forwarding/homeReadProtocol.ts";
import * as RollupEvidence from "../rollups/evidence.ts";
import * as RollupGeneration from "../rollups/generation.ts";
import * as RollupSummary from "../rollups/summaryRead.ts";
import * as RollupApplication from "../rollups/application.ts";
import * as Context from "effect/Context";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { WorkstreamsNativeCapability } from "@t3tools/contracts/jones/workstreamsNative";
import * as ServerConfig from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Registries from "../store/registryStore.ts";
import { decodeNativeConfig, resolveNativeMode, type NativeConfig } from "../mode/config.ts";
import type { NativeHome } from "../mode/authorityMode.ts";
import { nativeCapability } from "../mode/capability.ts";

export class StartupError extends Schema.TaggedError<StartupError>()("NativeStartupError", {
  code: Schema.Literals([
    "invalid_config",
    "explicit_selection_required",
    "database_mismatch",
    "key_unavailable",
    "home_unavailable",
    "binding_mismatch",
    "member_not_supported",
    "authority_fenced",
  ]),
}) {
  override get message(): string {
    return `Native Workstreams setup: ${this.code}. See docs/operations/workstreams-native.md.`;
  }
}
const isStartupError = Schema.is(StartupError);
const isKeyUnavailable = Schema.is(Keys.KeyUnavailable);

export type Selection = {
  readonly mode: NativeConfig["mode"];
  readonly config: NativeConfig | null;
  readonly home: NativeHome["Service"] | null;
  readonly sourceSessionId: string | null;
  readonly problem: StartupError | null;
};
export class Startup extends Context.Service<Startup, Selection>()(
  "t3/jones/workstreams/native/runtime/startup",
) {}

/** Checks the already selected application SQL resource. It never opens or initializes SQLite. */
export const select = Effect.fn("NativeStartup.select")(function* (input: {
  readonly text: string | null;
  readonly legacyActivationPresent: boolean;
  readonly home: string;
  readonly database: string;
  readonly environmentId: string;
}): Effect.fn.Return<
  Selection,
  never,
  | Path.Path
  | SqlClient.SqlClient
  | Keys.WorkstreamsKeyProvider
  | Registries.RegistryStore
  | Bindings.BindingStore
> {
  let config: NativeConfig | null = null;
  const blocked = (problem: StartupError): Selection => ({
    mode: "transition-fenced",
    config,
    home: null,
    sourceSessionId: null,
    problem,
  });
  if (input.text !== null) {
    const decoded = yield* Effect.try({
      try: () => decodeNativeConfig(input.text!),
      catch: () => new StartupError({ code: "invalid_config" }),
    }).pipe(Effect.result);
    if (decoded._tag === "Failure") return blocked(decoded.failure);
    config = decoded.success;
  }
  const mode = resolveNativeMode({
    config,
    legacyActivationPresent: input.legacyActivationPresent,
    keyPresent: true,
  });
  if (mode === "legacy" || mode === "disabled")
    return { mode, config, home: null, sourceSessionId: null, problem: null };
  if (mode === "transition-fenced") return blocked(new StartupError({ code: "authority_fenced" }));
  if (mode === "native-member") return blocked(new StartupError({ code: "member_not_supported" }));
  if (!config?.registry || !config.bootstrap)
    return blocked(new StartupError({ code: "explicit_selection_required" }));
  const registry = config.registry,
    bootstrap = config.bootstrap;
  const path = yield* Path.Path;
  if (
    !path.isAbsolute(bootstrap.home) ||
    !path.isAbsolute(bootstrap.database) ||
    path.resolve(bootstrap.home) !== path.resolve(input.home) ||
    path.resolve(bootstrap.database) !== path.resolve(input.database)
  )
    return blocked(new StartupError({ code: "database_mismatch" }));
  if (registry.home_environment_id !== input.environmentId)
    return blocked(new StartupError({ code: "binding_mismatch" }));
  const sql = yield* SqlClient.SqlClient,
    stores = yield* Registries.RegistryStore,
    bindings = yield* Bindings.BindingStore,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const checked = yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`;
        const main = databases.find((row) => row.name === "main");
        if (!main || path.resolve(main.file) !== path.resolve(bootstrap.database))
          return yield* new StartupError({ code: "database_mismatch" });
        const current = yield* stores.context(registry),
          binding = yield* bindings.read();
        if (!current || current.state !== "active")
          return yield* new StartupError({ code: "home_unavailable" });
        if (
          !binding ||
          binding.state !== "active" ||
          binding.registry_instance_id !== registry.registry_instance_id ||
          binding.owner_id !== registry.owner_id ||
          binding.home_environment_id !== registry.home_environment_id ||
          binding.home_generation !== registry.server_generation ||
          current.home_environment_id !== registry.home_environment_id ||
          current.server_generation !== registry.server_generation ||
          current.canonical_origin !== registry.canonical_origin
        )
          return yield* new StartupError({ code: "binding_mismatch" });
        for (const key_scope of ["home", "local"] as const)
          yield* keys.get(yield* keys.activeKeyId({ ...registry, key_scope }));
        yield* keys.indexKey(registry.registry_instance_id);
      }),
    )
    .pipe(
      Effect.mapError((error) =>
        isStartupError(error)
          ? error
          : new StartupError({
              code: isKeyUnavailable(error) ? "key_unavailable" : "home_unavailable",
            }),
      ),
      Effect.result,
    );
  if (checked._tag === "Failure") return blocked(checked.failure);
  return {
    mode: "native-home",
    config,
    home: { ...registry, served_by_environment_id: input.environmentId },
    sourceSessionId: bootstrap.source_session_id ?? null,
    problem: null,
  };
});

/** The frozen capability requires J5 reads. Omit it until those routes are mounted. */
export function capability(
  selection: Selection,
  mounted: { readonly readProjection: boolean; readonly placementsV2: boolean },
): WorkstreamsNativeCapability | undefined {
  if (
    selection.mode !== "native-home" ||
    !selection.home ||
    !mounted.readProjection ||
    !mounted.placementsV2
  )
    return undefined;
  return nativeCapability({ mode: "native-home", registry: selection.home }, mounted);
}

export const layer = Layer.effect(
  Startup,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig,
      fs = yield* FileSystem.FileSystem,
      path = yield* Path.Path,
      environment = yield* Environment.ServerEnvironmentIdentity;
    const file = path.join(config.baseDir, "workstreams-native.json");
    const read = yield* fs.readFileString(file).pipe(Effect.result);
    if (read._tag === "Failure" && read.failure.reason._tag !== "NotFound")
      return {
        mode: "transition-fenced" as const,
        config: null,
        home: null,
        sourceSessionId: null,
        problem: new StartupError({ code: "invalid_config" }),
      };
    const environmentVariables = yield* HostProcess.Environment;
    const legacyActivationPresent =
      (yield* fs.exists(path.join(config.baseDir, "workstreams", "activation.env"))) ||
      [
        "T3_WORKSTREAM_CONTROL_PLANE_URL",
        "T3_WORKSTREAM_OWNER_ID",
        "T3_WORKSTREAM_PRINCIPAL_ID",
        "T3_WORKSTREAM_KEY_ID",
        "T3_WORKSTREAM_SIGNING_SECRET",
        "T3_WORKSTREAM_AUTHORIZATION_REVISION",
        "T3_WORKSTREAM_REGISTRY_BINDING",
        "T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION",
      ].some((name) => environmentVariables[name] !== undefined);
    return yield* select({
      text: read._tag === "Success" ? read.success : null,
      legacyActivationPresent,
      home: config.baseDir,
      database: config.dbPath,
      environmentId: yield* environment.getEnvironmentId,
    });
  }),
);

/** Startup owns the branch before either transport factory can be constructed. */
export const chooseLayer = <L, LE, LR, H, HE, HR, U, UE, UR>(factories: {
  readonly legacy: () => Layer.Layer<L, LE, LR>;
  readonly home: (
    selection: Selection & { readonly home: NativeHome["Service"] },
  ) => Layer.Layer<H, HE, HR>;
  readonly unavailable: (selection: Selection) => Layer.Layer<U, UE, UR>;
}) =>
  Layer.unwrap(
    Effect.map(Startup, (selection): Layer.Layer<L & H & U, LE | HE | UE, LR | HR | UR> =>
      selection.mode === "legacy" || (selection.mode === "disabled" && selection.config === null)
        ? factories.legacy()
        : selection.mode === "native-home" && selection.home
          ? factories.home({ ...selection, home: selection.home })
          : factories.unavailable(selection),
    ),
  );

import * as NativeIntegration from "../integration.ts";
import * as NativeEntry from "./entry.ts";
import * as NativeReadRoutes from "../read/routes.ts";
import * as NativeReadExport from "../read/export.ts";
import * as NativeAdmission from "./admission.ts";
import * as ReferenceResolver from "../providers/referenceResolver.ts";
import * as GitHub from "../providers/githubPrObserver.ts";
import * as HomeExecutor from "../operations/homeExecutor.ts";
import * as Placements from "../registry/placements.ts";
import * as NativeAuthority from "../../../../environment/NativeStoreAuthority.ts";
import * as DurableEntries from "../operations/durableEntries.ts";
import * as Operations from "../../operations/service.ts";
import * as OperationsLive from "../../operations/live.ts";
import { Activation } from "../../operations/ports.ts";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as Registration from "../../registrationContext/service.ts";
import { WORKSTREAM_OPERATIONS_CONTRACT } from "@t3tools/contracts/jones/workstreamOperations";
import * as DateTime from "effect/DateTime";
import * as Recorded from "./recordedCaller.ts";
import * as RecordedPreparation from "./recordedCallerPreparation.ts";
import * as NativeRecovery from "./recovery.ts";
import * as NativeRollupWorker from "./rollupWorker.ts";
import * as NativeEffects from "../registry/effectRunner.ts";
import * as NativeSourceEnrollments from "./sourceEnrollment.ts";
import { NativeHome as Home } from "../mode/authorityMode.ts";

/** Lookup-only adapter; setup/reservation authority is deliberately not exposed. */
export const recordedSourceEnrollmentLayer = (
  source: Pick<
    NativeSourceEnrollments.NativeSourceEnrollments["Service"],
    "getValidatedBySessionId"
  >,
) =>
  Layer.succeed(NativeAdmission.RecordedSourceEnrollment, {
    lookup: Effect.fn("NativeRecovery.validatedSourceEnrollment")(function* (sessionId: string) {
      const found = yield* source
        .getValidatedBySessionId(sessionId)
        .pipe(Effect.mapError(() => new Recorded.RecordedCallerFence({ code: "not_granted" })));
      if (Option.isNone(found)) return null;
      const proof = found.value,
        request = proof.request,
        context = request.enrollment.context,
        expected = request.expected,
        session = request.enrollment.session;
      return {
        source: {
          environment_id: context.source_instance_id,
          authority_namespace: context.authority_namespace,
          store_generation: context.store_generation,
          binding_version: proof.receipt.source_binding_version,
          enrollment_id: request.enrollment.enrollment_id,
          enrollment_session_id: session.session_id,
        },
        destination: {
          registry_instance_id: expected.registry_instance_id,
          owner_id: expected.owner_id,
          home_environment_id: expected.home_environment_id,
          home_generation: expected.home_generation,
          principal_id: context.principal_id,
          binding_version: expected.singleton_binding_version,
        },
        issued_at: Date.parse(session.issued_at),
        expires_at: Date.parse(session.expires_at),
        active: true,
      };
    }),
  });

/** One home, SQL resource and operations instance. Authorization remains request-local. */
export const homeServicesLayer = (
  selection: Selection & { readonly home: NativeHome["Service"] },
  rollupPorts: Layer.Layer<
    | RollupEvidence.RollupEvidenceProvider
    | RollupGeneration.RollupGenerationProvider
    | RollupSummary.SummaryAuthorizer
    | RollupApplication.RollupAdmission
  > = Layer.mergeAll(
    RollupEvidence.unconfiguredLayer,
    RollupGeneration.unconfiguredLayer,
    RollupSummary.unconfiguredAuthorizerLayer,
    RollupApplication.admissionLayer,
  ),
) => {
  const admission = NativeAdmission.layer.pipe(
    Layer.provideMerge(NativeIntegration.nativeSessionLayer),
  );
  const enrolledSource = Layer.unwrap(
    Effect.map(Effect.serviceOption(NativeSourceEnrollments.NativeSourceEnrollments), (source) =>
      Option.isSome(source)
        ? recordedSourceEnrollmentLayer(source.value)
        : Layer.succeed(NativeAdmission.RecordedSourceEnrollment, {
            lookup: () => Effect.fail(new Recorded.RecordedCallerFence({ code: "unavailable" })),
          }),
    ),
  );
  const recording = Recorded.layer.pipe(
    Layer.provideMerge(
      NativeAdmission.recordedAuthorityLayer.pipe(
        Layer.provide(enrolledSource),
        Layer.provideMerge(admission),
      ),
    ),
  );
  const source = NativeIntegration.nativeSourceProvidersLayer.pipe(
    Layer.provideMerge(NativeAdmission.sourceAdmissionLayer),
  );
  const providers = ReferenceResolver.layer.pipe(
    Layer.provideMerge(NativeEntry.referenceSourcesLayer.pipe(Layer.provideMerge(source))),
    Layer.provideMerge(GitHub.unconfiguredLayer),
  );
  const durable = DurableEntries.layer.pipe(
    Layer.provideMerge(
      RecordedPreparation.layer.pipe(
        Layer.provideMerge(NativeIntegration.nativeSessionLayer),
        Layer.provideMerge(NativeIntegration.nativePersistenceLayer),
      ),
    ),
  );
  const entry = NativeEntry.layer.pipe(
    Layer.provideMerge(
      NativeIntegration.nativeHomeRuntimeLayer.pipe(
        Layer.provideMerge(NativeIntegration.nativeRollupRuntimeLayer),
        Layer.provideMerge(providers),
      ),
    ),
    Layer.provide(
      Layer.effect(
        Placements.PlacementTrust,
        Effect.map(NativeAuthority.NativeStoreAuthority, (authority) => authority.trustProvider),
      ),
    ),
    Layer.provide(NativeAdmission.callerBoundaryLayer),
    Layer.provideMerge(recording),
    Layer.provideMerge(durable),
    Layer.provideMerge(NativeIntegration.nativePersistenceLayer),
  );
  const execution = NativeEntry.executionLayer.pipe(
    Layer.provideMerge(HomeExecutor.layer.pipe(Layer.provide(NativeEntry.homeTransportLayer))),
    Layer.provideMerge(durable),
  );
  const operations = NativeEntry.operationsLayer.pipe(
    Layer.provide(
      OperationsLive.layer.pipe(
        Layer.provide(execution),
        Layer.provide(NativeEntry.gatewayLayer),
        Layer.provide(NativeEntry.registrationLayer),
        Layer.provide(
          Layer.succeed(Activation, {
            registryId: selection.home.registry_instance_id,
            status: { state: "enabled", missing_keys: [] },
          }),
        ),
      ),
    ),
  );
  const recovery = NativeRecovery.layer.pipe(
    Layer.provideMerge(operations),
    Layer.provide(NativeEffects.rawRecoveryLayer),
  );
  const reads = Layer.mergeAll(NativeReadRoutes.authorizedLayer, NativeReadExport.layer).pipe(
    Layer.provide(NativeAdmission.callerBoundaryLayer),
  );
  const peerBoundary = Layer.unwrap(
    Effect.map(Effect.serviceOption(PeerPins.NativePeerSessions), (native) =>
      Option.isNone(native)
        ? Layer.empty
        : NativeIntegration.nativePeerReadBoundaryLayer.pipe(
            Layer.provide(Layer.succeed(PeerPins.NativePeerSessions, native.value)),
          ),
    ),
  );
  const peerReads = Layer.unwrap(
    Effect.map(Effect.serviceOption(PeerReadBoundary.PeerReadBoundary), (boundary) =>
      Option.isNone(boundary)
        ? Layer.empty
        : HomeReadProtocol.layer.pipe(
            Layer.provide(PeerReadBoundary.homeAdmissionLayer),
            Layer.provide(Layer.succeed(PeerReadBoundary.PeerReadBoundary, boundary.value)),
          ),
    ),
  );
  return Layer.mergeAll(
    peerReads,
    NativeEntry.gatewayLayer,
    NativeEntry.registrationLayer,
    operations,
    recovery,
    NativeRollupWorker.layer,
    reads,
  ).pipe(
    Layer.provideMerge(entry),
    Layer.provideMerge(peerBoundary),
    Layer.provide(Layer.succeed(Home, selection.home)),
    Layer.provide(Layer.succeed(Startup, selection)),
    Layer.provide(rollupPorts),
  );
};

/** Explicitly disabled/fenced native configuration gets no Control Plane transport or requests. */
export const unavailableServicesLayer = (selection: Selection) => {
  const refuse = () =>
    Effect.fail(
      new Gateway.WorkstreamGatewayError({
        reason: "not-configured",
        detail:
          selection.problem?.message ??
          "Native Workstreams are disabled. See docs/operations/workstreams-native.md.",
      }),
    );
  const gateway: Gateway.WorkstreamGateway["Service"] = {
    readAppearance: refuse,
    saveAppearance: refuse,
    readThreadPlacements: refuse,
    readRegistryCounts: refuse,
    readSession: refuse,
    readMetadata: refuse,
    readDetail: refuse,
    readReferences: refuse,
    readReference: refuse,
    readMemberships: refuse,
    readDeclarations: refuse,
    readEdges: refuse,
    readHistory: refuse,
    pollCommand: refuse,
    readReceipt: refuse,
    resubmit: refuse,
    submit: refuse,
    purgeAuthorization: () => {},
  };
  const registration = Registration.WorkstreamsRegistrationContext.of({
    read: () => Effect.fail(new Registration.RegistrationContextError({ reason: "unconfigured" })),
  });
  const operations = Layer.effect(
    Operations.WorkstreamOperations,
    Effect.gen(function* () {
      const environment = yield* Environment.ServerEnvironmentIdentity;
      const environmentId = yield* environment.getEnvironmentId;
      return Operations.WorkstreamOperations.of({
        resumeRecorded: () => Effect.fail(new Recorded.RecordedCallerFence({ code: "missing" })),
        submit: () => Effect.fail(new Operations.OperationInternalError({})),
        read: (input) =>
          Effect.succeed(
            "operation_id" in input
              ? { operation: null }
              : "open" in input
                ? { operations: [] }
                : { assignment: { kind: "unverified" }, open_operation: null },
          ),
        setupStatus: Effect.fn("NativeUnavailable.setupStatus")(function* () {
          return {
            contract: WORKSTREAM_OPERATIONS_CONTRACT,
            environment_id: environmentId,
            observed_at: DateTime.formatIso(yield* DateTime.now),
            authority_effect: "none" as const,
            activation: {
              state: selection.mode === "disabled" ? ("disabled" as const) : ("invalid" as const),
              missing_keys: [],
            },
            registry: {
              state: "unavailable" as const,
              registry_id: selection.config?.registry?.registry_instance_id ?? null,
              owner_id: null,
              can_write: false as const,
            },
            t3_source: { state: "unavailable" as const, reason: "authority_fenced" as const },
            placements: { readiness: "unavailable" as const },
            operations: { open: 0 },
            next_steps: [],
          };
        }),
      });
    }),
  );
  return Layer.mergeAll(
    Layer.succeed(Gateway.WorkstreamGateway, gateway),
    Layer.succeed(Registration.WorkstreamsRegistrationContext, registration),
    operations,
  );
};
