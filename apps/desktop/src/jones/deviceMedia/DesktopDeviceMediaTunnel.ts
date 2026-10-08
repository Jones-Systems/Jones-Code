import {
  DesktopDeviceMediaTunnelInputSchema,
  type DesktopDeviceMediaTunnel as DeviceMediaTunnel,
  type DesktopDeviceMediaTunnelInput,
} from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import { baseSshArgs, resolveSshCommand } from "@t3tools/ssh/command";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as DesktopShutdown from "../../app/DesktopShutdown.ts";

export class DesktopDeviceMediaTunnelError extends Schema.TaggedError<DesktopDeviceMediaTunnelError>()(
  "DesktopDeviceMediaTunnelError",
  { message: Schema.String },
) {}

export interface DeviceMediaRendererLifetime {
  readonly isDisposed: () => boolean;
  readonly subscribe: (dispose: () => void) => () => void;
}

export class DesktopDeviceMediaTunnel extends Context.Service<
  DesktopDeviceMediaTunnel,
  {
    readonly open: (
      input: DesktopDeviceMediaTunnelInput,
      rendererId: number,
      lifetime?: DeviceMediaRendererLifetime,
    ) => Effect.Effect<DeviceMediaTunnel, DesktopDeviceMediaTunnelError | Schema.SchemaError>;
    readonly close: (id: string, rendererId: number) => Effect.Effect<void>;
  }
>()("@t3tools/desktop/jones/deviceMedia/DesktopDeviceMediaTunnel") {}

const decodeInput = Schema.decodeUnknownEffect(DesktopDeviceMediaTunnelInputSchema);

const fail = (message: string) => Effect.fail(new DesktopDeviceMediaTunnelError({ message }));

const waitUntil = <E, R>(check: Effect.Effect<boolean, E, R>) =>
  check.pipe(
    Effect.repeat({ until: (ready) => ready, schedule: Schedule.spaced(Duration.millis(50)) }),
    Effect.timeout(Duration.seconds(12)),
  );

const make = Effect.gen(function* () {
  const managerScope = yield* Scope.Scope;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const net = yield* NetService.NetService;
  const shutdown = yield* DesktopShutdown.DesktopShutdown;
  const sshCommand = yield* resolveSshCommand;
  const entries = new Map<string, { readonly scope: Scope.Scope; readonly rendererId: number }>();
  let sequence = 0;
  let stopped = false;

  const close = Effect.fn("desktop.deviceMediaTunnel.close")(function* (
    id: string,
    rendererId: number,
  ) {
    const entry = entries.get(id);
    if (entry === undefined || entry.rendererId !== rendererId) return;
    entries.delete(id);
    yield* Scope.close(entry.scope, Exit.void);
  });

  const closeAll = Effect.gen(function* () {
    stopped = true;
    const owned = [...entries.values()];
    entries.clear();
    yield* Effect.forEach(owned, (entry) => Scope.close(entry.scope, Exit.void), { discard: true });
  });

  yield* Scope.addFinalizer(managerScope, closeAll);
  yield* Effect.forkIn(shutdown.awaitRequest.pipe(Effect.andThen(closeAll)), managerScope);

  const open = Effect.fn("desktop.deviceMediaTunnel.open")(function* (
    raw: DesktopDeviceMediaTunnelInput,
    rendererId: number,
    lifetime?: DeviceMediaRendererLifetime,
  ) {
    const input = yield* decodeInput(raw);
    if (input.gatewayPort === 58470 || input.gatewayPort === 58615) {
      return yield* fail("Device media must use an authenticated gateway port.");
    }
    if (stopped || lifetime?.isDisposed()) {
      return yield* fail("Device media renderer is unavailable.");
    }

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const entryScope = yield* Scope.make("sequential");
        const id = "device-media-" + ++sequence;
        entries.set(id, { scope: entryScope, rendererId });
        yield* Scope.addFinalizer(
          entryScope,
          Effect.sync(() => entries.delete(id)),
        );

        return yield* restore(
          Effect.gen(function* () {
            if (lifetime !== undefined) {
              yield* Effect.acquireRelease(
                Effect.sync(() =>
                  lifetime.subscribe(() => {
                    Effect.runFork(close(id, rendererId));
                  }),
                ),
                (unsubscribe) => Effect.sync(unsubscribe),
              );
              if (lifetime.isDisposed() || !entries.has(id)) {
                return yield* fail("Device media renderer is unavailable.");
              }
            }

            const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-device-media-" });
            yield* fs.chmod(directory, 0o700);
            const socket = path.join(directory, "control");
            const localPort = yield* net.reserveLoopbackPort("127.0.0.1");
            const target = {
              alias: input.target,
              hostname: input.target,
              username: null,
              port: null,
            };
            const masterArgs = [
              ...baseSshArgs(target, { batchMode: "yes" }),
              "-T",
              "-n",
              "-N",
              "-a",
              "-x",
              "-o",
              "StrictHostKeyChecking=yes",
              "-o",
              "ClearAllForwardings=yes",
              "-o",
              "PermitLocalCommand=no",
              "-o",
              "RemoteCommand=none",
              "-o",
              "Tunnel=no",
              "-o",
              "ControlMaster=yes",
              "-o",
              "ControlPersist=no",
              "-o",
              "ServerAliveInterval=15",
              "-o",
              "ServerAliveCountMax=3",
              "-S",
              socket,
              input.target,
            ];
            const master = yield* Effect.acquireRelease(
              spawner.spawn(
                ChildProcess.make(sshCommand, masterArgs, {
                  stdin: "ignore",
                  stdout: "ignore",
                  stderr: "ignore",
                }),
              ),
              (process) =>
                process
                  .kill({
                    killSignal: "SIGTERM",
                    forceKillAfter: Duration.seconds(2),
                  })
                  .pipe(Effect.ignore),
            );

            const masterExited = master.exitCode.pipe(
              Effect.andThen(fail("Device media SSH transport exited.")),
              Effect.catch(() => fail("Device media SSH transport exited.")),
            );
            yield* Effect.forkIn(
              master.exitCode.pipe(Effect.exit, Effect.andThen(close(id, rendererId))),
              managerScope,
            );

            // ClearAllForwardings also clears command-line -L; add only our forward through the owned master.
            yield* Effect.raceFirst(waitUntil(fs.exists(socket)), masterExited);
            yield* Effect.scoped(
              Effect.gen(function* () {
                const forwarding = yield* spawner.spawn(
                  ChildProcess.make(
                    sshCommand,
                    [
                      "-F",
                      "none",
                      "-S",
                      socket,
                      "-O",
                      "forward",
                      "-o",
                      "BatchMode=yes",
                      "-o",
                      "StrictHostKeyChecking=yes",
                      "-o",
                      "ControlMaster=no",
                      "-o",
                      "ExitOnForwardFailure=yes",
                      "-o",
                      "PermitLocalCommand=no",
                      "-L",
                      "127.0.0.1:" + localPort + ":127.0.0.1:" + input.gatewayPort,
                      input.target,
                    ],
                    { stdin: "ignore", stdout: "ignore", stderr: "ignore" },
                  ),
                );
                const exitCode = yield* forwarding.exitCode;
                if (Number(exitCode) !== 0)
                  return yield* fail("Device media SSH forwarding failed.");
              }).pipe(Effect.timeout(Duration.seconds(12))),
            );
            yield* Effect.raceFirst(
              waitUntil(net.hasListenerOnHost(localPort, "127.0.0.1")),
              masterExited,
            );
            if (
              stopped ||
              !entries.has(id) ||
              lifetime?.isDisposed() ||
              !(yield* master.isRunning)
            ) {
              return yield* fail("Device media SSH transport closed before it was ready.");
            }
            return { id, httpBase: "http://127.0.0.1:" + localPort + "/api/device-hub" };
          }).pipe(
            Effect.provideService(Scope.Scope, entryScope),
            Effect.mapError(
              () =>
                new DesktopDeviceMediaTunnelError({
                  message: "Device media SSH tunnel could not be established.",
                }),
            ),
          ),
        ).pipe(
          Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : close(id, rendererId))),
        );
      }),
    );
  });

  return DesktopDeviceMediaTunnel.of({ open, close });
});

export const layer = Layer.effect(DesktopDeviceMediaTunnel, make);
