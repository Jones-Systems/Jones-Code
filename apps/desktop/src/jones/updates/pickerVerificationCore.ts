import { JonesDesktopUpdateController } from "./JonesDesktopUpdatesCore.ts";
// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Disposable synthetic qualification state and explicit effect ports.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import type { DesktopUpdateState } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import { JonesActionsClient, type JonesActionsTransport } from "@t3tools/shared/jones/jonesActions";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import type { DesktopIpcRegistrationError } from "../../ipc/DesktopIpc.ts";
import { DesktopUpdates, makeDesktopUpdateStateCell } from "../../updates/DesktopUpdatesService.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";
import { createJonesDesktopPreparation } from "./desktopPreparationRuntime.ts";
import { makeJonesDesktopUpdates } from "./jonesDesktopUpdatesService.ts";
import { requireStableMacAppPath } from "./jonesMacStagingChecks.ts";
import type { JonesStagingWorker } from "./stagingWorkerClient.ts";

export interface PickerVerificationInput {
  readonly root: string;
  readonly nonce: string;
  readonly home: string;
  readonly profile: string;
  readonly appRoot: string;
  readonly appPath: string;
  readonly executablePath: string;
  readonly version: string;
  readonly phase: "catalog" | "persisted" | "inflight" | "held";
  readonly transport: JonesActionsTransport;
  readonly stagingWorker: JonesStagingWorker;
  readonly broadcastState: (state: DesktopUpdateState) => Promise<void>;
  readonly broadcastPlan: (state: JonesInstallPlanState) => Promise<void>;
}

async function assertPickerOwnedState(
  input: Pick<PickerVerificationInput, "root" | "nonce" | "home" | "profile" | "phase">,
) {
  const root = await NodeFSP.realpath(input.root);
  if (
    root !== NodePath.resolve(input.root) ||
    !/^[a-f0-9]{32}$/.test(input.nonce) ||
    (await NodeFSP.readFile(NodePath.join(root, ".picker-owner"), "utf8")) !== input.nonce
  )
    throw new Error("Picker root ownership is unavailable.");
  for (const target of [input.home, input.profile]) {
    const resolved = await NodeFSP.realpath(target);
    if (resolved !== NodePath.resolve(target) || !resolved.startsWith(root + NodePath.sep))
      throw new Error("Picker state must be inside the owned root.");
  }
  const userdata = NodePath.join(input.home, "userdata");
  if (
    (await NodeFSP.stat(NodePath.join(userdata, "statev2.sqlite"))).size !== 0 ||
    (await NodeFSP.readFile(NodePath.join(userdata, "environment-id"), "utf8")).trim() !==
      "picker-synthetic"
  )
    throw new Error("Picker requires the empty synthetic database and environment identity.");
  if (input.phase === "catalog") {
    const names = (await NodeFSP.readdir(input.home)).sort();
    if (JSON.stringify(names) !== JSON.stringify(["profile", "userdata"]))
      throw new Error("Initial picker home is not fresh.");
  }
  try {
    await NodeFSP.lstat(NodePath.join(input.home, "jones-desktop-updates.json"));
    throw new Error("Background preparation settings must be absent.");
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
  }
}

export async function createPickerVerificationCore(input: PickerVerificationInput) {
  await assertPickerOwnedState(input);
  const scope = Effect.runSync(Scope.make());
  const nativeCalls: string[] = [];
  const forbidden = async (name: string): Promise<never> => {
    nativeCalls.push(name);
    throw new Error(`Forbidden picker native port: ${name}`);
  };
  const cell = await Effect.runPromise(
    makeDesktopUpdateStateCell(
      createInitialDesktopUpdateState(
        input.version,
        { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
        "latest",
      ),
      (state) => Effect.promise(() => input.broadcastState(state)),
    ),
  );
  const service = await Effect.runPromise(
    makeJonesDesktopUpdates({
      createController: (options) =>
        new JonesDesktopUpdateController(options, {
          prepareJonesStage: () => forbidden("prepareJonesStage"),
          stageJonesMacApp: () => forbidden("stageJonesMacApp"),
        }),
      home: input.home,
      profile: input.profile,
      appRoot: input.appRoot,
      appPath: input.appPath,
      executablePath: input.executablePath,
      bundleDirectory: NodePath.dirname(input.appRoot),
      stateDirectory: input.home,
      activeGeneration: undefined,
      architecture: "arm64",
      disabledByEnv: false,
      cell,
      schedule: "none",
      now: Date.now,
      uuid: NodeCrypto.randomUUID,
      timestamp: async () => new Date().toISOString(),
      broadcastPlan: input.broadcastPlan,
      client: new JonesActionsClient({ transport: input.transport }),
      preparation: createJonesDesktopPreparation({
        bundleDirectory: NodePath.dirname(input.appRoot),
        stateDirectory: input.home,
        platform: "darwin",
        worker: input.stagingWorker,
      }),
      native: {
        prepareNative: () => forbidden("prepareNative"),
        listener: () => forbidden("listener"),
        processProofs: () => forbidden("processProofs"),
        ordinaryRecoveryReadiness: () => forbidden("ordinaryRecoveryReadiness"),
      },
    }),
  );
  const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | DesktopUpdates>) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(DesktopUpdates, service),
      ),
    );
  return {
    nativeCalls,
    // The host supplies Electron's ipcMain and the trusted window. The core
    // runs the registration in its own scope with its update service.
    register: (
      registration: Effect.Effect<void, DesktopIpcRegistrationError, Scope.Scope | DesktopUpdates>,
    ) => run(registration),
    preflightStableAppPath: () => requireStableMacAppPath(input.appPath),
    start: async () => {
      await assertPickerOwnedState(input);
      await run(service.configure);
    },
    state: () => Effect.runPromise(service.getState),
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  };
}
