// @effect-diagnostics nodeBuiltinImport:off - Explicit composition ports keep native adapters in production.
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesUpdateProvenance } from "@t3tools/contracts/jones/jonesUpdates";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import type * as Duration from "effect/Duration";
import { DesktopUpdates, makeDesktopUpdateStateCell } from "../../updates/DesktopUpdatesService.ts";
import { createInstallPlanStore } from "../installPlan/store.ts";
import { createInstallPlanCoordinator } from "../installPlan/coordinator.ts";
import { createDesktopFleetStore } from "../fleetUpdates/store.ts";
import { createJonesDesktopPreparation } from "./desktopPreparationRuntime.ts";
import {
  type JonesDesktopUpdateController,
  type JonesDesktopUpdateOptions,
} from "./JonesDesktopUpdatesCore.ts";

type NativePorts = Pick<
  JonesDesktopUpdateOptions,
  "prepareNative" | "listener" | "processProofs" | "ordinaryRecoveryReadiness"
>;
export const makeJonesDesktopUpdates = Effect.fn("DesktopUpdates.jones")(function* (input: {
  readonly home: string;
  readonly appRoot: string;
  readonly appPath: string;
  readonly executablePath: string;
  readonly profile: string | undefined;
  readonly bundleDirectory: string;
  readonly stateDirectory: string;
  readonly activeGeneration: string | undefined;
  readonly trialDescriptorPath?: string | undefined;
  readonly architecture: "arm64" | "x64";
  readonly disabledByEnv: boolean;
  readonly cell: Effect.Success<ReturnType<typeof makeDesktopUpdateStateCell>>;
  readonly native: NativePorts;
  readonly createController: (options: JonesDesktopUpdateOptions) => JonesDesktopUpdateController;
  readonly client?: JonesDesktopUpdateOptions["client"];
  readonly preparation?: ReturnType<typeof createJonesDesktopPreparation>;
  readonly timestamp: () => Promise<string>;
  readonly now: () => number;
  readonly uuid: () => string;
  readonly broadcastPlan: (state: JonesInstallPlanState) => Promise<void>;
  readonly schedule:
    | "none"
    | { readonly startupDelay: Duration.Input; readonly pollInterval: Duration.Input };
}) {
  const context = yield* Effect.context<never>();
  const { updateStateRef, stateChanges, stateMutex, emitState, setState } = input.cell;
  const profile = input.profile;
  const executablePath = input.executablePath;
  let coordinator: ReturnType<typeof createInstallPlanCoordinator> | undefined;
  const planStore = createInstallPlanStore({ home: input.home, profile });
  const fleet = createDesktopFleetStore({
    home: input.home,
    profile,
    isPlanManaged: (source) =>
      coordinator === undefined ? Promise.resolve(true) : coordinator.isPlanManaged(source),
    withUnmanagedTarget: (source, work) => {
      if (coordinator === undefined)
        return Promise.reject(new Error("Native plan persistence is unavailable."));
      return coordinator.withUnmanagedTarget(source, work);
    },
  });
  const preparation =
    input.preparation ??
    createJonesDesktopPreparation({
      bundleDirectory: input.bundleDirectory,
      stateDirectory: input.stateDirectory,
      platform: "darwin",
    });
  const controller = input.createController({
    ...preparation.controllerOptions,
    home: input.home,
    appRoot: input.appRoot,
    appPath: input.appPath,
    executablePath,
    profile,
    fleet,
    plan: {
      bindLaptopInstall: (binding) => {
        if (coordinator === undefined)
          return Promise.reject(new Error("Native plan persistence is unavailable."));
        return coordinator.bindLaptopInstall(binding);
      },
      recordLaptopOutcome: (outcome) =>
        coordinator?.recordLaptopOutcome(outcome) ?? Promise.resolve(),
      preparationHeld: () =>
        coordinator === undefined ? Promise.resolve(true) : coordinator.preparationHeld(),
      recordLaptopAdmission: (operationId) => {
        if (coordinator === undefined)
          return Promise.reject(new Error("Native plan persistence is unavailable."));
        return coordinator.recordLaptopAdmission(operationId);
      },
      recordLaptopWorker: (input) => {
        if (coordinator === undefined)
          return Promise.reject(new Error("Native plan persistence is unavailable."));
        return coordinator.recordLaptopWorker(input);
      },
    },
    activeGeneration: input.activeGeneration,
    trialDescriptorPath: input.trialDescriptorPath,
    architecture: input.architecture,
    platform: "darwin",
    initialState: yield* Ref.get(updateStateRef),
    disabledByEnv: input.disabledByEnv,
    onState: async (state) => {
      await Effect.runPromiseWith(context)(setState(state));
      // Native state changes wake all windows; no renderer observation is accepted as Laptop proof.
      void coordinator?.request({ type: "read" });
    },
    timestamp: input.timestamp,
    // oxlint-disable-next-line t3code/no-rpc-permission-bypass -- This is the Jones Actions catalog client, not an environment RPC client.
    ...(input.client === undefined ? {} : { client: input.client }),
    ...input.native,
  });
  const background = preparation.attach(controller, (source) =>
    coordinator === undefined ? Promise.resolve(true) : coordinator.backgroundSuppressed(source),
  );
  coordinator = createInstallPlanCoordinator({
    store: planStore,
    controller,
    localEnvironmentId: () => controller.state.jones?.environmentId,
    track: (work) => background.track(work),
    now: input.now,
    uuid: input.uuid,
    broadcast: (state) => {
      void input.broadcastPlan(state);
    },
  });
  const check = Effect.promise(async () => {
    await controller.refreshTrialActivation();
    return background.check();
  });
  const install = (handle?: string, campaignId?: string) =>
    Effect.promise(() => background.track(() => controller.install(handle, campaignId))).pipe(
      Effect.map((result) => ({ ...result, state: controller.state })),
    );
  return DesktopUpdates.of({
    getState: Ref.get(updateStateRef),
    isActionActive: Effect.sync(() => controller.busy !== null),
    isInstallActive: Effect.sync(() => controller.busy === "install"),
    subscribe: stateMutex.withPermits(1)(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(stateChanges);
        return {
          latest: yield* Ref.get(updateStateRef),
          changes: Stream.fromSubscription(subscription),
        };
      }),
    ),
    emitState,
    disabledReason: Ref.get(updateStateRef).pipe(
      Effect.map((state) =>
        state.jones?.capability.check
          ? Option.none()
          : Option.some(state.message ?? "Jones native launcher bootstrap is required."),
      ),
    ),
    configure: Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.promise(() => preparation.close()));
      yield* Effect.promise(() => controller.configure());
      yield* Effect.promise(() => coordinator!.request({ type: "read" }));
      yield* Effect.promise(() => background.start());
      if (input.schedule === "none") return;
      yield* Effect.sleep(input.schedule.startupDelay).pipe(
        Effect.andThen(check),
        Effect.asVoid,
        Effect.forkScoped,
      );
      yield* Effect.sleep(input.schedule.pollInterval).pipe(
        Effect.andThen(check),
        Effect.asVoid,
        Effect.forever,
        Effect.forkScoped,
      );
    }),
    setChannel: Effect.fn("DesktopUpdates.setChannel")(function () {
      return Ref.get(updateStateRef);
    }),
    check: Effect.fn("DesktopUpdates.check")(function () {
      return check.pipe(Effect.map((checked) => ({ checked, state: controller.state })));
    }),
    download: Effect.promise(() => background.track(() => controller.download())).pipe(
      Effect.map((result) => ({ ...result, state: controller.state })),
    ),
    listJonesBuilds: Effect.promise(() => controller.listBuilds()),
    selectJonesBuild: Effect.fn("DesktopUpdates.selectJonesBuild")((build: JonesUpdateProvenance) =>
      Effect.promise(() => controller.selectBuild(build)),
    ),
    downloadSelected: Effect.fn("DesktopUpdates.downloadSelected")(function (selection) {
      return Effect.promise(() => background.track(() => controller.download(selection))).pipe(
        Effect.map((result) => ({ ...result, state: controller.state })),
      );
    }),
    install: install().pipe(
      Effect.map(({ accepted, completed, state }) => ({ accepted, completed, state })),
    ),
    discardStaged: Effect.fn("DesktopUpdates.discardStaged")(function (handle) {
      return Effect.promise(() => background.discard(handle)).pipe(
        Effect.map((result) => ({ ...result, state: controller.state })),
      );
    }),
    setBackgroundPreparation: Effect.fn("DesktopUpdates.setBackgroundPreparation")(
      function (enabled) {
        return Effect.promise(() => background.setEnabled(enabled)).pipe(
          Effect.map(() => controller.state),
        );
      },
    ),
    installPlan: Effect.fn("DesktopUpdates.installPlan")(function (request) {
      return Effect.promise(async () => {
        if (request.type === "read") await controller.refreshTrialActivation();
        return coordinator!.request(request);
      });
    }),
    fleetUpdates: Effect.fn("DesktopUpdates.fleetUpdates")(function (request) {
      return Effect.promise(() => fleet.request(request));
    }),
    installStaged: Effect.fn("DesktopUpdates.installStaged")(function (handle, campaignId) {
      return install(handle, campaignId).pipe(
        Effect.map(({ accepted, completed, state }) => ({ accepted, completed, state })),
      );
    }),
    installPrepared: Effect.fn("DesktopUpdates.installPrepared")(function (version, handle) {
      return controller.state.downloadedVersion === version
        ? install(handle)
        : Effect.succeed({
            accepted: false,
            completed: false,
            failed: false,
            state: controller.state,
          });
    }),
  });
});
