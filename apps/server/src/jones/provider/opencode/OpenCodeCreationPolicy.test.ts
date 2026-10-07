import { it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expect } from "vite-plus/test";

import * as ProviderAdapter from "../../../orchestration-v2/ProviderAdapter.ts";
import * as OpenCodeServerLedger from "../../../provider/OpenCodeServerLedger.ts";
import * as OpenCodeServerOwner from "../../../provider/OpenCodeServerOwner.ts";
import * as OpenCodeRuntime from "../../../provider/opencodeRuntime.ts";
import * as OpenCode2Client from "../../../provider/opencode2/OpenCode2Client.ts";
import * as OpenCode2Server from "../../../provider/opencode2/OpenCode2Server.ts";
import * as OpenCodeCreationPolicy from "./OpenCodeCreationPolicy.ts";

const unusedRuntimeMethod = () =>
  Effect.fail(new OpenCodeRuntime.OpenCodeRuntimeError({ operation: "unused", detail: "unused" }));
const deniedReservation = () =>
  new ProviderAdapter.ProviderRuntimeBindingError({
    driver: ProviderDriverKind.make("opencode"),
    detail: "synthetic reservation denied",
  });
const deniedAuthorization = () =>
  new OpenCodeRuntime.OpenCodeRuntimeError({
    operation: "authorize",
    detail: "synthetic authorization denied",
  });

const makeHooks = (
  trace: string[],
  generations: string[],
): OpenCodeCreationPolicy.OpenCodeCreationHooks => ({
  reserveGeneration: Effect.sync(() => {
    trace.push("reserve");
    const generation = `existing-owner-generation-${generations.length + 1}`;
    generations.push(generation);
    return generation;
  }),
  authorize: (capture) =>
    Effect.sync(() => {
      expect(Object.isFrozen(capture)).toBe(true);
      expect(capture.runtimeGeneration).toBe(generations.at(-1));
      trace.push(`authorize:${capture.directory}`);
    }),
  abandonGeneration: (generation) =>
    Effect.sync(() => {
      trace.push(`abandon:${generation}`);
    }),
});

const syntheticRuntime = (
  start: OpenCodeRuntime.OpenCodeRuntimeShape["startOpenCodeServerProcess"],
): OpenCodeRuntime.OpenCodeRuntimeShape => ({
  startOpenCodeServerProcess: (input) =>
    Effect.gen(function* () {
      const capture =
        input.creationHooks === undefined
          ? undefined
          : yield* OpenCodeCreationPolicy.prepareGeneration(input.directory, input.creationHooks);
      const process = yield* start(input);
      return {
        ...process,
        ...(capture === undefined ? {} : { runtimeGeneration: capture.runtimeGeneration }),
      };
    }),
  connectToOpenCodeServer: unusedRuntimeMethod,
  runOpenCodeCommand: unusedRuntimeMethod,
  createOpenCodeSdkClient: () => ({}) as never,
  loadOpenCodeInventory: unusedRuntimeMethod,
  loadOpenCodeSkills: unusedRuntimeMethod,
  loadInventoryFromCli: unusedRuntimeMethod,
  loadSkillsFromCli: unusedRuntimeMethod,
});
const processSnapshot = (index: number): OpenCodeRuntime.OpenCodeServerProcess => ({
  url: `http://127.0.0.1:${index}`,
  version: "2.0.0",
  isRunning: Effect.succeed(true),
  exitCode: Effect.never,
});
const ownerInput = { binaryPath: "opencode", directory: "/qualified/project" };

it.effect(
  "reserves the existing owner once and authorizes before starting concurrent borrowers",
  () =>
    Effect.gen(function* () {
      const trace: string[] = [];
      const generations: string[] = [];
      const hooks = makeHooks(trace, generations);
      const release = yield* Deferred.make<void>();
      const borrowed = yield* Deferred.make<void>();
      const runtime = syntheticRuntime((input) =>
        Effect.sync(() => {
          expect(input.creationHooks).toBe(hooks);
          trace.push(`start:${input.directory}`);
          return processSnapshot(1);
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* OpenCodeServerOwner.make(ownerInput);
          const firstBorrower = yield* owner
            .withServer(
              (server) =>
                Deferred.succeed(borrowed, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(server),
                ),
              hooks,
            )
            .pipe(Effect.forkChild);
          yield* Deferred.await(borrowed);
          const second = yield* owner.withServer(Effect.succeed, {
            ...hooks,
            reserveGeneration: Effect.fail(deniedReservation()),
          });
          yield* Deferred.succeed(release, undefined);
          expect(second).toBe(yield* Fiber.join(firstBorrower));
          expect(second.runtimeGeneration).toBe("existing-owner-generation-1");
          expect(generations).toHaveLength(1);
          expect(trace).toEqual([
            "reserve",
            "authorize:/qualified/project",
            "start:/qualified/project",
          ]);
        }),
      ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    }),
);

it.effect.each(["reservation", "authorization"] as const)(
  "does not start when %s fails",
  (denied) =>
    Effect.gen(function* () {
      const trace: string[] = [];
      const generations: string[] = [];
      const hooks = makeHooks(trace, generations);
      const runtime = syntheticRuntime(() =>
        Effect.sync(() => {
          trace.push("start");
          return processSnapshot(1);
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* OpenCodeServerOwner.make(ownerInput);
          const result = yield* owner
            .withServer(Effect.succeed, {
              ...hooks,
              ...(denied === "reservation"
                ? {
                    reserveGeneration: Effect.sync(() => {
                      trace.push("reserve");
                    }).pipe(Effect.andThen(Effect.fail(deniedReservation()))),
                  }
                : {
                    authorize: () => {
                      trace.push("authorize");
                      return Effect.fail(deniedAuthorization());
                    },
                  }),
            })
            .pipe(Effect.flip);
          expect(result.operation).toBe(
            denied === "reservation" ? "reserveGeneration" : "authorize",
          );
          expect(trace).toEqual(
            denied === "reservation"
              ? ["reserve"]
              : ["reserve", "authorize", "abandon:existing-owner-generation-1"],
          );
        }),
      ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    }),
);

it.effect("abandons the exact reservation when authorization is interrupted before spawn", () =>
  Effect.gen(function* () {
    const trace: string[] = [];
    const generations: string[] = [];
    const entered = yield* Deferred.make<void>();
    const hooks = makeHooks(trace, generations);
    const runtime = syntheticRuntime(() =>
      Effect.sync(() => {
        trace.push("start");
        return processSnapshot(1);
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make(ownerInput);
        const borrower = yield* owner
          .withServer(Effect.succeed, {
            ...hooks,
            authorize: () =>
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(borrower);
        expect(trace).toEqual(["reserve", "abandon:existing-owner-generation-1"]);
        const retry = yield* owner.withServer(Effect.succeed, hooks);
        expect(retry.runtimeGeneration).toBe("existing-owner-generation-2");
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }),
);

it.effect(
  "reports failed abandonment rather than claiming the denied reservation was released",
  () =>
    Effect.gen(function* () {
      const error = yield* OpenCodeCreationPolicy.prepareGeneration("/qualified/project", {
        reserveGeneration: Effect.succeed("reserved-generation"),
        authorize: () => Effect.fail(deniedAuthorization()),
        abandonGeneration: () => Effect.fail(deniedReservation()),
      }).pipe(Effect.flip);
      expect(error.operation).toBe("abandonGeneration");
    }),
);

it.effect(
  "reserves a new generation on start failure without treating the attempted spawn as safely abandoned",
  () =>
    Effect.gen(function* () {
      const trace: string[] = [];
      const generations: string[] = [];
      const hooks = makeHooks(trace, generations);
      const attempts = yield* Ref.make(0);
      const runtime = syntheticRuntime(() =>
        Effect.gen(function* () {
          trace.push("start");
          const index = yield* Ref.updateAndGet(attempts, (value) => value + 1);
          if (index === 1)
            return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
              operation: "start",
              detail: "failed",
            });
          return processSnapshot(index);
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* OpenCodeServerOwner.make(ownerInput);
          expect((yield* owner.withServer(Effect.succeed, hooks).pipe(Effect.exit))._tag).toBe(
            "Failure",
          );
          const second = yield* owner.withServer(Effect.succeed, hooks);
          expect(second.runtimeGeneration).toBe("existing-owner-generation-2");
          expect(trace).toEqual([
            "reserve",
            "authorize:/qualified/project",
            "start",
            "reserve",
            "authorize:/qualified/project",
            "start",
          ]);
        }),
      ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    }),
);

it.effect("reserves another generation after idle close", () =>
  Effect.gen(function* () {
    const trace: string[] = [];
    const generations: string[] = [];
    const hooks = makeHooks(trace, generations);
    const closed = yield* Deferred.make<void>();
    const starts = yield* Ref.make(0);
    const runtime = syntheticRuntime(() =>
      Effect.gen(function* () {
        const index = yield* Ref.updateAndGet(starts, (value) => value + 1);
        yield* Effect.addFinalizer(() => Deferred.succeed(closed, undefined).pipe(Effect.ignore));
        return processSnapshot(index);
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make(ownerInput);
        yield* owner.withServer(Effect.succeed, hooks);
        yield* TestClock.adjust("31 seconds");
        yield* Deferred.await(closed);
        const second = yield* owner.withServer(Effect.succeed, hooks);
        expect(second.runtimeGeneration).toBe("existing-owner-generation-2");
        expect(generations).toHaveLength(2);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("blocks the actual local spawn boundary and abandons on authorization denial", () =>
  Effect.gen(function* () {
    const trace: string[] = [];
    const generations: string[] = [];
    const hooks = makeHooks(trace, generations);
    const spawner = ChildProcessSpawner.make(() =>
      Effect.sync(() => {
        trace.push("spawn-attempt");
      }).pipe(Effect.andThen(Effect.die("unexpected synthetic spawn"))),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
        const error = yield* runtime
          .startOpenCodeServerProcess({
            binaryPath: "/synthetic/opencode",
            directory: "/qualified/project",
            port: 4200,
            creationHooks: { ...hooks, authorize: () => Effect.fail(deniedAuthorization()) },
          })
          .pipe(Effect.flip);
        expect(error.operation).toBe("authorize");
        expect(trace).toEqual(["reserve", "abandon:existing-owner-generation-1"]);
      }),
    ).pipe(
      Effect.provide(
        OpenCodeRuntime.OpenCodeRuntimeLive.pipe(
          Layer.provide(OpenCodeServerLedger.layerTest),
          Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
        ),
      ),
      Effect.provideService(HostProcessPlatform, "linux"),
    );
  }),
);

it.effect.each(["start", "connect"] as const)(
  "passes creation hooks through the local %s route and constructs the authorized cwd",
  (route) =>
    Effect.gen(function* () {
      const commands: ChildProcess.Command[] = [];
      const trace: string[] = [];
      const generations: string[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          trace.push("spawn-attempt");
          commands.push(command);
        }).pipe(Effect.andThen(Effect.die("synthetic spawn boundary"))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
          const input = {
            binaryPath: "/synthetic/opencode",
            directory: "/qualified/project",
            port: 4200,
            environment: { OPENCODE_CONFIG_CONTENT: "{}" },
            creationHooks: makeHooks(trace, generations),
          };
          if (route === "start") {
            yield* runtime.startOpenCodeServerProcess(input).pipe(Effect.exit);
          } else {
            yield* runtime.connectToOpenCodeServer(input).pipe(Effect.exit);
          }
          expect(trace).toEqual(["reserve", "authorize:/qualified/project", "spawn-attempt"]);
          expect(commands).toHaveLength(1);
          const command = commands[0]!;
          expect(command._tag).toBe("StandardCommand");
          if (command._tag !== "StandardCommand") return;
          expect(command.options.cwd).toBe("/qualified/project");
          expect(command.command).toBe("/synthetic/opencode");
          expect(command.args).toEqual(["serve", "--hostname=127.0.0.1", "--port=4200"]);
          expect(command.options.detached).toBe(true);
          expect(command.options.shell).toBe(false);
          expect(command.options.extendEnv).toBe(false);
          expect(command.options.env?.OPENCODE_CONFIG_CONTENT).toBe("{}");
        }),
      ).pipe(
        Effect.provide(
          OpenCodeRuntime.OpenCodeRuntimeLive.pipe(
            Layer.provide(OpenCodeServerLedger.layerTest),
            Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
          ),
        ),
        Effect.provideService(HostProcessPlatform, "linux"),
      );
    }),
);

const syntheticClient = OpenCode2Client.layer.pipe(
  Layer.provide(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response('{"version":"2.0.18","pid":4242,"urls":[],"paths":{"tmp":"/synthetic"}}', {
              status: 200,
              headers: { "content-type": "application/json" },
            }),
          ),
        ),
      ),
    ),
  ),
);
const syntheticCrypto = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, bytes) => Effect.succeed(bytes),
  }),
);
const serverInput = { ...ownerInput, serverUrl: "", serverPassword: "", environment: {} };

it.effect("passes the hook bundle and exposes the immutable owned generation", () =>
  Effect.gen(function* () {
    const trace: string[] = [];
    const generations: string[] = [];
    const hooks = makeHooks(trace, generations);
    const runtime = syntheticRuntime((input) =>
      Effect.gen(function* () {
        trace.push("start");
        const process = processSnapshot(1);
        const version = yield* input.verify!(process.url);
        return { ...process, version };
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const server = yield* OpenCode2Server.make(serverInput);
        const first = yield* server.withConnection(Effect.succeed, hooks);
        const second = yield* server.withConnection(Effect.succeed, {
          ...hooks,
          reserveGeneration: Effect.fail(deniedReservation()),
        });
        expect(second).toBe(first);
        expect(first.ownedProcess?.runtimeGeneration).toBe("existing-owner-generation-1");
        expect(Object.isFrozen(first.ownedProcess)).toBe(true);
        expect(yield* first.ownedProcess!.isRunning).toBe(true);
        expect(trace).toEqual(["reserve", "authorize:/qualified/project", "start"]);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }).pipe(Effect.provide([syntheticClient, syntheticCrypto])),
);

it.effect("external connections ignore local creation hooks and expose no owned snapshot", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const trace: string[] = [];
      const server = yield* OpenCode2Server.make({
        ...serverInput,
        serverUrl: "http://synthetic:4200",
      });
      const connection = yield* server.withConnection(Effect.succeed, {
        ...makeHooks(trace, []),
        reserveGeneration: Effect.fail(deniedReservation()),
      });
      expect(connection.external).toBe(true);
      expect(connection.ownedProcess).toBeUndefined();
      expect(trace).toEqual([]);
    }),
  ).pipe(
    Effect.provide([syntheticClient, syntheticCrypto]),
    Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, syntheticRuntime(unusedRuntimeMethod)),
  ),
);
