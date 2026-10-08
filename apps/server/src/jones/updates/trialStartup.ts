import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import type * as NetAddress from "effect/net/NetAddress";
import packageJson from "../../../package.json" with { type: "json" };
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as ServiceLauncherClient from "../../cloud/serviceLauncherClient.ts";
import type { QualifiedTrialRuntimeWitness } from "../cloud/qualifiedStartup.ts";
import { awaitJonesTrialCommit, JonesTrialGateError } from "./trialGate.ts";

export const readJonesStartupGateProtocol = (): 1 => 1;

export const hasJonesTrialDescriptor = Effect.map(
  HostProcessEnvironment,
  (environment) => environment.T3CODE_JONES_TRIAL_DESCRIPTOR !== undefined,
);

export const hasJonesTrialAuthority = Effect.gen(function* () {
  const descriptor = yield* hasJonesTrialDescriptor;
  const launcher = yield* ServiceLauncherClient.ServiceLauncherClient;
  return descriptor || launcher.requiresQualifiedTrialGate === true;
});

export const awaitJonesTrialStartup = <E, R>(input: {
  readonly waitUntilParked: Effect.Effect<void, E, R>;
  readonly observedListener: Effect.Effect<NetAddress.SocketAddress, E, R>;
}) =>
  Effect.gen(function* () {
    const environment = yield* HostProcessEnvironment;
    const descriptorPath = environment.T3CODE_JONES_TRIAL_DESCRIPTOR;
    if (descriptorPath === undefined) return;
    const config = yield* ServerConfig.ServerConfig;
    const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
    yield* input.waitUntilParked;
    const observedListener = yield* input.observedListener;
    const environmentId = yield* serverEnvironment.getEnvironmentId;
    let pending: Promise<void> | undefined;
    yield* Effect.acquireUseRelease(
      Effect.sync(() => new AbortController()),
      (controller) =>
        Effect.tryPromise({
          try: () => {
            pending = awaitJonesTrialCommit({
              descriptorPath,
              home: config.baseDir,
              databasePath: config.dbPath,
              profile: environment.T3CODE_DESKTOP_USER_DATA_DIR,
              environmentId,
              version: packageJson.version,
              buildMetadata: packageJson,
              observedListener,
              signal: controller.signal,
            });
            return pending;
          },
          catch: (cause) =>
            Schema.is(JonesTrialGateError)(cause)
              ? cause
              : new JonesTrialGateError({ step: "identity", uncertain: false, cause }),
        }),
      (controller) =>
        Effect.tryPromise({
          try: async () => {
            controller.abort();
            // Scoped interruption drains native I/O before startup can close its resources.
            await pending;
          },
          catch: (cause) =>
            Schema.is(JonesTrialGateError)(cause)
              ? cause
              : new JonesTrialGateError({ step: "cancel", uncertain: false, cause }),
        }).pipe(Effect.catch((error) => (error.uncertain ? Effect.fail(error) : Effect.void))),
    );
  });

export const awaitSelectedJonesTrialStartup = <E, R>(input: {
  readonly waitUntilParked: Effect.Effect<void, E, R>;
  readonly observedListener: Effect.Effect<NetAddress.SocketAddress, E, R>;
}) =>
  Effect.gen(function* () {
    const descriptor = yield* hasJonesTrialDescriptor;
    const launcher = yield* ServiceLauncherClient.ServiceLauncherClient;
    const qualified = launcher.requiresQualifiedTrialGate === true;
    if (descriptor && qualified) {
      return yield* Effect.fail(
        new JonesTrialGateError({
          step: "identity",
          uncertain: false,
          cause: new Error(
            "A native descriptor and a qualified launcher trial cannot share startup authority.",
          ),
        }),
      );
    }
    if (descriptor) return yield* awaitJonesTrialStartup(input);
    if (!qualified) return;
    yield* input.waitUntilParked;
    const config = yield* ServerConfig.ServerConfig;
    const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
    const fs = yield* FileSystem.FileSystem;
    const [home, databasePath, serviceUserdata] = yield* Effect.all([
      fs.realPath(config.baseDir),
      fs.realPath(config.dbPath),
      fs.realPath(config.stateDir),
    ]);
    const witness: QualifiedTrialRuntimeWitness = {
      home,
      databasePath,
      serviceUserdata,
      environmentId: yield* serverEnvironment.getEnvironmentId,
      version: packageJson.version,
      buildMetadata: packageJson,
      listener: yield* input.observedListener,
      processId: process.pid,
    };
    yield* launcher.prepareQualifiedTrial(witness);
  });
