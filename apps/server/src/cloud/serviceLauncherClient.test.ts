import * as NetAddress from "effect/net/NetAddress";
import type {
  QualifiedTrialReceipt,
  QualifiedTrialRuntimeWitness,
} from "../jones/cloud/qualifiedStartup.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Logger from "effect/Logger";
import * as PlatformError from "effect/PlatformError";
import { assert, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
  type ServiceLauncherChildMessage,
  type ServiceLauncherParentMessage,
} from "./serviceProtocol.ts";
import { posixLauncherScript } from "./pinnedRuntime.ts";
import * as ServiceLauncherClient from "./serviceLauncherClient.ts";
import * as UpdateAdmission from "../jones/updates/UpdateAdmissionFence.ts";

function qualifiedFixture(protocol: 3 | 4 = 4, startupGateProtocol?: 1) {
  const fromVersion = "0.0.0-preview.20261002.100";
  const targetVersion = "0.0.0-preview.20261002.101.1";
  const receipt: QualifiedTrialReceipt = {
    protocol: 4,
    startupGateProtocol: 1,
    updateId: "qualified-trial",
    stagedHandle: "11111111-1111-4111-8111-111111111111",
    home: "/fixture",
    databasePath: "/fixture/userdata/statev2.sqlite",
    serviceUserdata: "/fixture/userdata",
    environmentId: "native-fixture",
    version: targetVersion,
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    listener: { family: "IPv4", address: "127.0.0.1", port: 43123, scopeId: 0 },
    processId: 123,
    resumeHeld: true,
  };
  const qualified = {
    protocol: 1,
    stagedHandle: receipt.stagedHandle,
    binding: {
      baseDir: receipt.home,
      dbPath: receipt.databasePath,
      environmentId: receipt.environmentId,
      activeVersion: fromVersion,
      activeSourceSha: "c".repeat(40),
    },
    receipt: {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: targetVersion,
      sourceSha: receipt.sourceSha,
      sourceTree: receipt.sourceTree,
      installedSourceSha: "c".repeat(40),
      runId: 101,
      runAttempt: 1,
      artifactId: 102,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      artifactDigest: `sha256:${"d".repeat(64)}`,
      archiveSha256: "e".repeat(64),
      payloadSha256: "f".repeat(64),
      platform: "linux",
      architecture: "x64",
    },
  };
  const pending = {
    id: receipt.updateId,
    fromVersion,
    targetVersion,
    dbPath: receipt.databasePath,
    status: "pending",
    phase: "trial-ready",
    qualified,
  };
  const context = {
    protocol,
    childVersion: targetVersion,
    qualifiedUpdatesProtocol: 1,
    ...(startupGateProtocol === undefined ? {} : { startupGateProtocol }),
    update: pending,
  };
  const host = new FakeLauncherProcess(context);
  const events: string[] = [];
  const operations = {
    receipt: async () => {
      events.push("receipt");
      return receipt;
    },
    assertUnreserved: async () => {
      events.push("unreserved");
    },
    reserve: async () => {
      events.push("reserved");
    },
  };
  const witness: QualifiedTrialRuntimeWitness = {
    home: receipt.home,
    databasePath: receipt.databasePath,
    serviceUserdata: receipt.serviceUserdata,
    environmentId: receipt.environmentId,
    version: targetVersion,
    processId: receipt.processId,
    buildMetadata: {
      jonesSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: receipt.sourceSha,
        tree: receipt.sourceTree,
      },
    },
    listener: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  };
  const make = () =>
    makeClient(host, targetVersion).pipe(
      Effect.provideService(ServiceLauncherClient.QualifiedTrialOperations, operations),
    );
  return { host, context, pending, receipt, events, operations, witness, make };
}

class FakeLauncherProcess {
  readonly connected: boolean;
  readonly env: Record<string, string | undefined>;
  readonly sent: ServiceLauncherChildMessage[] = [];
  readonly sentSignal = Promise.withResolvers<void>();
  readonly #listeners = new Map<string, Set<(...args: ReadonlyArray<unknown>) => void>>();

  constructor(context: unknown, connected = true) {
    this.connected = connected;
    this.env = { [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context) };
  }

  send = (message: ServiceLauncherChildMessage, callback?: (error: Error | null) => void) => {
    this.sent.push(message);
    this.sentSignal.resolve();
    callback?.(null);
    return true;
  };

  on = (event: "message" | "disconnect", listener: (...args: ReadonlyArray<unknown>) => void) => {
    const listeners = this.#listeners.get(event) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(event, listeners);
  };

  off = (event: "message" | "disconnect", listener: (...args: ReadonlyArray<unknown>) => void) => {
    this.#listeners.get(event)?.delete(listener);
  };

  listenerCount() {
    return [...this.#listeners.values()].reduce((count, listeners) => count + listeners.size, 0);
  }

  emit(message: ServiceLauncherParentMessage) {
    for (const listener of this.#listeners.get("message") ?? []) listener(message);
  }
}

const makeClient = (host: FakeLauncherProcess, currentVersion: string) =>
  Effect.gen(function* () {
    // requestUpdate hands off through the real fence owned by this test's scope.
    const fence = yield* UpdateAdmission.make;
    return yield* ServiceLauncherClient.make({ currentVersion }).pipe(
      Effect.provideService(UpdateAdmission.UpdateAdmissionFence, fence),
      Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, host),
      Effect.provideService(HostProcess.Environment, host.env),
      Effect.provide(NodeServices.layer),
    );
  });

// regression: #34502 a remote update never replaces the launcher, so an installed one stays protocol 3
it.effect.each([3, SERVICE_LAUNCHER_PROTOCOL])(
  "waits for a protocol-%i launcher to durably commit the trial update ID, then moves owned launchers",
  (protocol) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-trial-" });
      const baseDir = path.join(root, ".t3");
      const exe = (version: string) => path.join(baseDir, "runtime/versions", version, "t3");
      const installed = path.join(root, ".local/bin/t3");
      for (const file of [exe("1.0.0"), exe("1.1.0")]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "");
      }
      yield* fs.makeDirectory(path.dirname(installed), { recursive: true });
      yield* fs.symlink(exe("1.0.0"), installed);

      const pending = {
        id: "update-1",
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending" as const,
        ...(protocol === SERVICE_LAUNCHER_PROTOCOL ? { phase: "trial-ready" } : {}),
      };
      const host = new FakeLauncherProcess({ protocol, childVersion: "1.1.0", update: pending });
      host.env["HOME"] = root;
      const client = yield* makeClient(host, "1.1.0");
      const prepared = yield* Effect.forkChild(client.prepareTrial({ baseDir }), {
        startImmediately: true,
      });
      yield* Effect.yieldNow;
      expect(host.sent).toEqual([{ type: "prepared", updateId: "update-1" }]);
      assert.equal(yield* fs.readLink(installed), exe("1.0.0"));

      const committed = {
        id: pending.id,
        fromVersion: pending.fromVersion,
        targetVersion: pending.targetVersion,
        status: "committed" as const,
      };
      host.emit({ type: "committed", updateId: committed.id });
      expect(yield* Fiber.join(prepared)).toEqual(committed);
      assert.equal(yield* fs.readFileString(installed), posixLauncherScript(exe("1.1.0")));

      // A later start replays the stored outcome; user pins made since then stay put.
      yield* fs.remove(installed);
      yield* fs.symlink(exe("1.0.0"), installed);
      const restarted = new FakeLauncherProcess({
        protocol,
        childVersion: "1.1.0",
        update: committed,
      });
      restarted.env["HOME"] = root;
      const restartedClient = yield* makeClient(restarted, "1.1.0");
      expect(yield* restartedClient.prepareTrial({ baseDir })).toEqual(committed);
      assert.equal(yield* fs.readLink(installed), exe("1.0.0"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("returns the launcher-generated ID only after update acceptance", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const client = yield* makeClient(host, "1.0.0");
    const requested = yield* Effect.forkChild(
      client.requestUpdate({ targetVersion: "1.1.0", dbPath: "/tmp/state.sqlite" }),
      { startImmediately: true },
    );
    yield* Effect.yieldNow;
    host.emit({
      type: "update-accepted",
      updateId: "launcher-id",
    });
    expect(yield* Fiber.join(requested)).toBe("launcher-id");
  }),
);

it.effect("preserves a launcher rejection as a distinct error", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const client = yield* makeClient(host, "1.0.0");
    const requested = yield* Effect.forkChild(
      client.requestUpdate({ targetVersion: "1.1.0", dbPath: "/tmp/state.sqlite" }),
      { startImmediately: true },
    );
    yield* Effect.yieldNow;
    host.emit({ type: "update-rejected", reason: "requires local update" });
    expect(yield* Fiber.join(requested).pipe(Effect.flip)).toMatchObject({
      _tag: "ServiceLauncherRejectedError",
      targetVersion: "1.1.0",
      reason: "requires local update",
    });
  }),
);

it.effect("stops the server when the launcher asks over IPC", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const stopped = yield* Effect.forkChild(
      ServiceLauncherClient.awaitServiceLauncherStop({ currentVersion: "1.0.0" }).pipe(
        Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, host),
        Effect.provideService(HostProcess.Environment, host.env),
      ),
      { startImmediately: true },
    );
    host.emit({ type: "stop" });
    yield* Fiber.join(stopped);
  }),
);

it.effect("rejects contradictory trial context instead of leaving activation closed", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.1.0",
      update: {
        id: "update-1",
        fromVersion: "1.0.0",
        targetVersion: "1.2.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
      },
    });
    const error = yield* makeClient(host, "1.1.0").pipe(Effect.flip);
    expect(error.message).toBe("The service launcher supplied invalid startup context.");
  }),
);

it.effect("ignores launcher context inherited without an IPC channel", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess(
      { protocol: SERVICE_LAUNCHER_PROTOCOL, childVersion: "1.0.0" },
      false,
    );
    const client = yield* makeClient(host, "1.1.0");
    const error = yield* client
      .requestUpdate({ targetVersion: "1.2.0", dbPath: "/tmp/state.sqlite" })
      .pipe(Effect.flip);
    expect(error).toMatchObject({ _tag: "ServiceLauncherClientError", operation: "unmanaged" });
    expect(host.sent).toEqual([]);
  }),
);

it.layer(NodeServices.layer)("self-update launchers", (it) => {
  it.effect(
    "updates custom install and PATH launchers, including versions older than fromVersion",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
        const baseDir = path.join(root, ".t3");
        const exe = (version: string) => path.join(baseDir, "runtime/versions", version, "t3");
        const installed = path.join(root, "custom/bin/t3");
        const onPath = path.join(root, "bin/t3");
        for (const file of [exe("1.0.0"), exe("1.5.0"), exe("2.0.0")]) {
          yield* fs.makeDirectory(path.dirname(file), { recursive: true });
          yield* fs.writeFileString(file, "");
        }
        yield* fs.makeDirectory(path.dirname(installed), { recursive: true });
        yield* fs.writeFileString(installed, posixLauncherScript(exe("1.5.0")), { mode: 0o755 });
        yield* fs.makeDirectory(path.dirname(onPath), { recursive: true });
        yield* fs.symlink(exe("1.0.0"), onPath);

        yield* ServiceLauncherClient.repointLaunchers({ baseDir, targetVersion: "2.0.0" }).pipe(
          Effect.provideService(HostProcess.Environment, {
            HOME: root,
            T3CODE_INSTALL_BIN_DIR: path.dirname(installed),
            PATH: path.dirname(onPath),
          }),
        );

        // The script is rewritten and the legacy symlink becomes one.
        assert.equal(yield* fs.readFileString(installed), posixLauncherScript(exe("2.0.0")));
        assert.equal(yield* fs.readFileString(onPath), posixLauncherScript(exe("2.0.0")));
      }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
  );

  it.effect("still moves the PATH launcher when the install bin one cannot be rewritten", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
      const baseDir = path.join(root, ".t3");
      const exe = (version: string) => path.join(baseDir, "runtime/versions", version, "t3");
      const installed = path.join(root, ".local/bin/t3");
      const onPath = path.join(root, "bin/t3");
      for (const file of [exe("1.0.0"), exe("2.0.0")]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "");
      }
      for (const link of [installed, onPath]) {
        yield* fs.makeDirectory(path.dirname(link), { recursive: true });
        yield* fs.symlink(exe("1.0.0"), link);
      }
      const warnings: unknown[] = [];
      const logger = Logger.make(({ logLevel, message }) => {
        if (logLevel === "Warn") warnings.push(message);
      });
      const failingFs = FileSystem.FileSystem.of({
        ...fs,
        rename: (source, destination) =>
          destination === installed
            ? Effect.fail(
                new PlatformError.PlatformError(
                  new PlatformError.SystemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "rename",
                    pathOrDescriptor: destination,
                  }),
                ),
              )
            : fs.rename(source, destination),
      });

      yield* ServiceLauncherClient.repointLaunchers({ baseDir, targetVersion: "2.0.0" }).pipe(
        Effect.provideService(HostProcess.Environment, {
          HOME: root,
          PATH: path.dirname(onPath),
        }),
        Effect.provideService(FileSystem.FileSystem, failingFs),
        Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
      );

      assert.lengthOf(warnings, 1);
      assert.include(JSON.stringify(warnings), installed);
      assert.equal(yield* fs.readLink(installed), exe("1.0.0"));
      assert.equal(yield* fs.readFileString(onPath), posixLauncherScript(exe("2.0.0")));
    }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
  );

  it.effect("leaves unowned install files and PATH symlinks untouched after self-update", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
      const baseDir = path.join(root, ".t3");
      const installed = path.join(root, ".local/bin/t3");
      const onPath = path.join(root, "bin/t3");
      const foreign = path.join(root, "other-home/runtime/versions/1.0.0/t3");
      const target = path.join(baseDir, "runtime/versions/2.0.0/t3");
      for (const file of [installed, foreign, target]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "user-owned file");
      }
      yield* fs.makeDirectory(path.dirname(onPath), { recursive: true });
      yield* fs.symlink(foreign, onPath);

      yield* ServiceLauncherClient.repointLaunchers({ baseDir, targetVersion: "2.0.0" }).pipe(
        Effect.provideService(HostProcess.Environment, {
          HOME: root,
          PATH: path.dirname(onPath),
        }),
      );

      assert.equal(yield* fs.readFileString(installed), "user-owned file");
      assert.equal(yield* fs.readLink(onPath), foreign);
    }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
  );
});

it.effect("establishes the qualified outcome only after exact grant and durable reservation", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(4, 1);
    const client = yield* fixture.make();
    expect(client.requiresQualifiedTrialGate).toBe(true);
    yield* client.prepareTrial({ baseDir: fixture.receipt.home }).pipe(Effect.flip);
    expect(fixture.host.sent).toEqual([]);
    const prepared = yield* Effect.forkChild(client.prepareQualifiedTrial(fixture.witness), {
      startImmediately: true,
    });
    yield* Effect.promise(() => fixture.host.sentSignal.promise);
    expect(fixture.host.sent).toEqual([
      {
        type: "prepared",
        updateId: fixture.pending.id,
        startupGateProtocol: 1,
        qualified: fixture.receipt,
      },
    ]);
    expect(fixture.events).toEqual(["receipt", "unreserved"]);
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    fixture.host.emit({
      type: "committed",
      updateId: fixture.pending.id,
      startupGateProtocol: 1,
      qualified: { ...fixture.receipt, generation: fixture.pending.id },
    });
    const outcome = yield* Fiber.join(prepared);
    expect(fixture.events).toEqual(["receipt", "unreserved", "reserved"]);
    expect(client.qualifiedStartupOutcome).toEqual(outcome);
    expect(yield* client.prepareTrial({ baseDir: fixture.receipt.home })).toEqual(outcome);
    expect((yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation).toBe(
      "qualified-replay",
    );
    expect(fixture.host.sent).toHaveLength(1);
    expect(fixture.host.listenerCount()).toBe(0);
  }),
);
