import { assert, describe, it } from "@effect/vitest";
import * as NetService from "@t3tools/shared/Net";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as DesktopShutdown from "../../app/DesktopShutdown.ts";
import * as DeviceMedia from "./DesktopDeviceMediaTunnel.ts";

const input = { target: "mini", gatewayPort: 43123, owner: "owner-1", generation: "generation-1" };

function fixture(options?: {
  forwardExit?: number;
  holdForward?: boolean;
  socketReady?: boolean;
  failMaster?: boolean;
  holdListener?: boolean;
}) {
  const commands: ChildProcess.StandardCommand[] = [];
  const cleaned: string[] = [];
  const cleanupReceipts = new Map<string, Deferred.Deferred<void>>();
  const directoryCreated = Deferred.makeUnsafe<void>();
  const masterSpawned = Deferred.makeUnsafe<void>();
  const forwardStarted = Deferred.makeUnsafe<void>();
  const forwardFinished = Deferred.makeUnsafe<ChildProcessSpawner.ExitCode>();
  const listenerStarted = Deferred.makeUnsafe<void>();
  const listenerReady = Deferred.makeUnsafe<boolean>();
  const masters: Array<{
    readonly handle: ChildProcessSpawner.ChildProcessHandle;
    readonly exited: Deferred.Deferred<ChildProcessSpawner.ExitCode>;
    readonly killed: () => number;
  }> = [];
  let directoryCount = 0;
  let nextPort = 41000;
  const fs = FileSystem.makeNoop({
    makeTempDirectoryScoped: () =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const directory = "/tmp/t3-device-media-test-" + ++directoryCount;
          cleanupReceipts.set(directory, Deferred.makeUnsafe<void>());
          return directory;
        }).pipe(Effect.tap(() => Deferred.succeed(directoryCreated, undefined))),
        (directory) =>
          Effect.sync(() => cleaned.push(directory)).pipe(
            Effect.andThen(Deferred.succeed(cleanupReceipts.get(directory)!, undefined)),
          ),
      ),
    chmod: () => Effect.void,
    exists: () => Effect.succeed(options?.socketReady !== false),
  });
  const net = NetService.NetService.of({
    reserveLoopbackPort: () => Effect.sync(() => ++nextPort),
    hasListenerOnHost: () =>
      options?.holdListener
        ? Deferred.succeed(listenerStarted, undefined).pipe(
            Effect.andThen(Deferred.await(listenerReady)),
          )
        : Effect.succeed(true),
    canListenOnHost: () => Effect.succeed(true),
    isPortAvailableOnLoopback: () => Effect.succeed(true),
    findAvailablePort: () => Effect.sync(() => ++nextPort),
  });
  const process = (
    exited: Deferred.Deferred<ChildProcessSpawner.ExitCode>,
    running: () => boolean,
    kill: () => Effect.Effect<void>,
  ) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(100 + commands.length),
      stdout: Stream.empty,
      stderr: Stream.empty,
      all: Stream.empty,
      exitCode: Deferred.await(exited),
      isRunning: Effect.sync(running),
      kill,
      stdin: Sink.drain,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
      unref: Effect.succeed(Effect.void),
    });
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      assert.equal(command._tag, "StandardCommand");
      if (command._tag !== "StandardCommand") return yield* Effect.die("unexpected command");
      commands.push(command);
      if (command.args.includes("forward")) {
        yield* Deferred.succeed(forwardStarted, undefined);
        const exit = options?.holdForward
          ? forwardFinished
          : Deferred.makeUnsafe<ChildProcessSpawner.ExitCode>();
        if (!options?.holdForward) {
          yield* Deferred.succeed(exit, ChildProcessSpawner.ExitCode(options?.forwardExit ?? 0));
        }
        return process(
          exit,
          () => false,
          () => Effect.void,
        );
      }
      if (options?.failMaster) {
        return yield* Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
            description: "ssh unavailable",
          }),
        );
      }
      const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      let running = true;
      let kills = 0;
      const handle = process(
        exited,
        () => running,
        () =>
          Effect.sync(() => {
            if (running) kills++;
            running = false;
          }).pipe(
            Effect.andThen(Deferred.succeed(exited, ChildProcessSpawner.ExitCode(143))),
            Effect.asVoid,
          ),
      );
      masters.push({ handle, exited, killed: () => kills });
      yield* Deferred.succeed(masterSpawned, undefined);
      return handle;
    }),
  );
  const layer = DeviceMedia.layer.pipe(
    Layer.provideMerge(DesktopShutdown.layer),
    Layer.provideMerge(Layer.succeed(FileSystem.FileSystem, fs)),
    Layer.provideMerge(Layer.succeed(NetService.NetService, net)),
    Layer.provideMerge(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
    Layer.provideMerge(Path.layer),
  );
  return {
    layer,
    commands,
    cleaned,
    cleanupReceipts,
    masters,
    directoryCreated,
    masterSpawned,
    forwardStarted,
    forwardFinished,
    listenerStarted,
    listenerReady,
  };
}

describe("desktop device media tunnel", () => {
  it.effect(
    "starts only a dedicated authenticated gateway forward without provisioning an environment",
    () => {
      const f = fixture();
      return Effect.gen(function* () {
        const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
        const result = yield* tunnels.open(input, 7);
        assert.equal(result.httpBase, "http://127.0.0.1:41001/api/device-hub");
        assert.equal(f.commands.length, 2);
        const master = f.commands[0]!;
        const forwarding = f.commands[1]!;
        assert.equal(master.command, "ssh");
        assert.include(master.args, "BatchMode=yes");
        assert.include(master.args, "StrictHostKeyChecking=yes");
        assert.include(master.args, "ClearAllForwardings=yes");
        assert.include(master.args, "ControlPersist=no");
        assert.include(master.args, "PermitLocalCommand=no");
        assert.include(master.args, "-N");
        assert.notInclude(master.args, "-L");
        assert.equal(master.args.at(-1), "mini");
        const socket = master.args[master.args.indexOf("-S") + 1];
        assert.deepEqual(forwarding.args.slice(0, 6), [
          "-F",
          "none",
          "-S",
          socket,
          "-O",
          "forward",
        ]);
        assert.include(forwarding.args, "127.0.0.1:41001:127.0.0.1:43123");
        assert.notInclude(forwarding.args, "ClearAllForwardings=yes");
        yield* tunnels.close(result.id, 7);
        assert.equal(f.masters[0]!.killed(), 1);
        assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
    },
  );

  it.effect(
    "rejects invalid targets, unbounded ports, empty identities, and raw hub ports before spawning",
    () => {
      const f = fixture();
      return Effect.gen(function* () {
        const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
        for (const patch of [
          { target: "-oProxyCommand=bad" },
          { target: "mini; touch /tmp/unsafe" },
          { target: "mini\n-oProxyCommand=bad" },
          { gatewayPort: 0 },
          { gatewayPort: 65536 },
          { gatewayPort: 43123.5 },
          { gatewayPort: 58470 },
          { gatewayPort: 58615 },
          { owner: "" },
          { generation: "" },
        ]) {
          const exit = yield* Effect.exit(tunnels.open({ ...input, ...patch }, 7));
          assert(Exit.isFailure(exit));
        }
        assert.deepEqual(f.commands, []);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
    },
  );

  it.effect("gives concurrent panels independent leases even for the same generation", () => {
    const f = fixture();
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const [first, second] = yield* Effect.all([tunnels.open(input, 7), tunnels.open(input, 7)], {
        concurrency: "unbounded",
      });
      assert.notEqual(first.id, second.id);
      assert.notEqual(first.httpBase, second.httpBase);
      const sockets = f.commands
        .filter((command) => command.args.includes("-N"))
        .map((command) => command.args[command.args.indexOf("-S") + 1]);
      assert.notEqual(sockets[0], sockets[1]);
      yield* tunnels.close(first.id, 8);
      assert.deepEqual(
        f.masters.map((master) => master.killed()),
        [0, 0],
      );
      yield* tunnels.close(first.id, 7);
      assert.equal(
        f.masters.reduce((sum, master) => sum + master.killed(), 0),
        1,
      );
      yield* tunnels.close(second.id, 7);
      assert.deepEqual(
        f.masters.map((master) => master.killed()),
        [1, 1],
      );
      assert.equal(f.cleaned.length, 2);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("does not return a lease before the forwarding receipt succeeds", () => {
    const f = fixture({ holdForward: true });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      let returned = false;
      const fiber = yield* tunnels.open(input, 7).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            returned = true;
          }),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(f.forwardStarted);
      assert.equal(returned, false);
      yield* Deferred.succeed(f.forwardFinished, ChildProcessSpawner.ExitCode(0));
      const result = yield* Fiber.join(fiber);
      assert.equal(returned, true);
      yield* tunnels.close(result.id, 7);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("does not return before the owned loopback forward accepts connections", () => {
    const f = fixture({ holdListener: true });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      let returned = false;
      const fiber = yield* tunnels.open(input, 7).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            returned = true;
          }),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(f.listenerStarted);
      assert.equal(returned, false);
      yield* Deferred.succeed(f.listenerReady, true);
      const result = yield* Fiber.join(fiber);
      yield* tunnels.close(result.id, 7);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("cleans its socket directory when the SSH executable cannot start", () => {
    const f = fixture({ failMaster: true });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const exit = yield* Effect.exit(tunnels.open(input, 7));
      assert(Exit.isFailure(exit));
      assert.equal(f.commands.length, 1);
      assert.equal(f.masters.length, 0);
      assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("does not issue a forward after the master fails during startup", () => {
    const f = fixture({ socketReady: false });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const fiber = yield* tunnels.open(input, 7).pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(f.masterSpawned);
      yield* Deferred.succeed(f.masters[0]!.exited, ChildProcessSpawner.ExitCode(255));
      const exit = yield* Fiber.join(fiber);
      assert(Exit.isFailure(exit));
      assert.equal(f.commands.length, 1);
      assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("closes only its owned process and scratch when forwarding fails", () => {
    const f = fixture({ forwardExit: 255 });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const exit = yield* Effect.exit(tunnels.open(input, 7));
      assert(Exit.isFailure(exit));
      assert.equal(f.commands.length, 2);
      assert.equal(f.masters[0]!.killed(), 1);
      assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("cleans a pending open when its caller is interrupted", () => {
    const f = fixture({ socketReady: false });
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const fiber = yield* tunnels.open(input, 7).pipe(Effect.forkChild);
      yield* Deferred.await(f.masterSpawned);
      yield* Fiber.interrupt(fiber);
      assert.equal(f.masters[0]!.killed(), 1);
      assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
      assert.equal(f.commands.length, 1);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("cleans transport failure and removes its renderer lifetime subscription", () => {
    const f = fixture();
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const removed = yield* Deferred.make<void>();
      yield* tunnels.open(input, 7, {
        isDisposed: () => false,
        subscribe: () => () => {
          Deferred.doneUnsafe(removed, Effect.void);
        },
      });
      yield* Deferred.succeed(f.masters[0]!.exited, ChildProcessSpawner.ExitCode(255));
      yield* Deferred.await(f.cleanupReceipts.get("/tmp/t3-device-media-test-1")!);
      yield* Deferred.await(removed);
      assert.equal(f.masters[0]!.killed(), 1);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("closes on renderer disposal without closing another panel", () => {
    const f = fixture();
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      let dispose = () => {};
      yield* tunnels.open(input, 7, {
        isDisposed: () => false,
        subscribe: (listener) => {
          dispose = listener;
          return () => {};
        },
      });
      const second = yield* tunnels.open(input, 7);
      dispose();
      yield* Deferred.await(f.cleanupReceipts.get("/tmp/t3-device-media-test-1")!);
      assert.deepEqual(
        f.masters.map((master) => master.killed()),
        [1, 0],
      );
      yield* tunnels.close(second.id, 7);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("app shutdown releases all owned tunnels and prevents new opens", () => {
    const f = fixture();
    return Effect.gen(function* () {
      const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
      const shutdown = yield* DesktopShutdown.DesktopShutdown;
      yield* tunnels.open(input, 7);
      yield* tunnels.open({ ...input, generation: "generation-2" }, 8);
      yield* shutdown.request;
      yield* Deferred.await(f.cleanupReceipts.get("/tmp/t3-device-media-test-1")!);
      yield* Deferred.await(f.cleanupReceipts.get("/tmp/t3-device-media-test-2")!);
      assert.deepEqual(
        f.masters.map((master) => master.killed()),
        [1, 1],
      );
      const exit = yield* Effect.exit(tunnels.open(input, 7));
      assert(Exit.isFailure(exit));
      assert.equal(f.commands.length, 4);
    }).pipe(Effect.provide(f.layer), Effect.scoped);
  });

  it.effect("releases an open tunnel when the service scope ends", () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const tunnels = yield* DeviceMedia.DesktopDeviceMediaTunnel;
        yield* tunnels.open(input, 7);
      }).pipe(Effect.provide(f.layer), Effect.scoped);
      assert.equal(f.masters[0]!.killed(), 1);
      assert.deepEqual(f.cleaned, ["/tmp/t3-device-media-test-1"]);
    });
  });
});
