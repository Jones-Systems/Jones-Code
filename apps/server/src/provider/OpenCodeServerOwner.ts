import * as NodeCrypto from "node:crypto";

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import * as OpenCodeRuntime from "./opencodeRuntime.ts";

const OPENCODE_SERVER_IDLE_TTL = "30 seconds";

export interface OpenCodeOwnedServerProcess extends OpenCodeRuntime.OpenCodeServerProcess {
  readonly runtimeGeneration?: string;
  readonly isCurrentAndRunning?: Effect.Effect<boolean>;
}

interface OpenCodeServerOwnerState {
  server: OpenCodeOwnedServerProcess | null;
  serverScope: Scope.Closeable | null;
  borrowers: number;
  idleCloseFiber: Fiber.Fiber<void, never> | null;
}

export class OpenCodeServerOwner extends Context.Service<
  OpenCodeServerOwner,
  {
    readonly subscribeBeforeRuntimeReplacement?: <E>(
      listener: (generation: string) => Effect.Effect<void, E>,
    ) => Effect.Effect<void, never, Scope.Scope>;
    readonly withServer: <A, E, R, CreationError = never>(
      use: (server: OpenCodeOwnedServerProcess) => Effect.Effect<A, E, R>,
      beforeNativeCreation?: (actualDirectory: string) => Effect.Effect<void, CreationError>,
    ) => Effect.Effect<A, E | OpenCodeRuntime.OpenCodeRuntimeError, R>;
  }
>()("t3/provider/OpenCodeServerOwner") {}

/** Owns the lazy local OpenCode server shared by one provider instance. */
export const make = Effect.fn("OpenCodeServerOwner.make")(function* (input: {
  readonly binaryPath: string;
  readonly directory: string;
  readonly serverPassword?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly verify?: (url: string) => Effect.Effect<string, OpenCodeRuntime.OpenCodeRuntimeError>;
}) {
  const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
  const ownerScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void),
  );
  const mutex = yield* Semaphore.make(1);
  const replacementListeners = new Set<{
    readonly listener: (
      generation: string,
    ) => Effect.Effect<void, OpenCodeRuntime.OpenCodeRuntimeError>;
  }>();
  const state: OpenCodeServerOwnerState = {
    server: null,
    serverScope: null,
    borrowers: 0,
    idleCloseFiber: null,
  };

  const cancelIdleClose = Effect.fn("OpenCodeServerOwner.cancelIdleClose")(function* () {
    const fiber = state.idleCloseFiber;
    state.idleCloseFiber = null;
    if (fiber !== null) {
      yield* Fiber.interrupt(fiber).pipe(Effect.ignore);
    }
  });

  const closeServer = Effect.fn("OpenCodeServerOwner.closeServer")(function* (
    expected?: OpenCodeOwnedServerProcess,
  ) {
    if (expected !== undefined && state.server !== expected) {
      return;
    }
    const scope = state.serverScope;
    state.server = null;
    state.serverScope = null;
    if (scope !== null) {
      yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
    }
  });

  const watchServerExit = Effect.fn("OpenCodeServerOwner.watchServerExit")(function* (
    server: OpenCodeOwnedServerProcess,
  ) {
    yield* server.exitCode;
    yield* mutex.withPermit(
      Effect.gen(function* () {
        if (state.server !== server) {
          return;
        }
        yield* cancelIdleClose();
        yield* closeServer(server);
      }),
    );
  });

  const acquireServer = <CreationError>(
    beforeNativeCreation?: (actualDirectory: string) => Effect.Effect<void, CreationError>,
  ) =>
    mutex.withPermit(
      Effect.gen(function* () {
        yield* cancelIdleClose();
        if (state.server !== null) {
          if (yield* state.server.isRunning) {
            state.borrowers += 1;
            return state.server;
          }
        }

        const runtimeGeneration = yield* Effect.sync(() => NodeCrypto.randomUUID());
        for (const { listener } of replacementListeners) {
          yield* listener(runtimeGeneration);
        }
        if (beforeNativeCreation !== undefined) {
          yield* beforeNativeCreation(input.directory).pipe(
            Effect.mapError(
              (cause) =>
                new OpenCodeRuntime.OpenCodeRuntimeError({
                  operation: "beforeNativeCreation",
                  detail: "Could not authorize creation of the owned OpenCode runtime.",
                  cause,
                }),
            ),
          );
        }
        if (state.server !== null) {
          yield* closeServer(state.server);
        }

        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const serverScope = yield* Scope.make();
            const started = yield* Effect.exit(
              restore(
                runtime
                  .startOpenCodeServerProcess({
                    binaryPath: input.binaryPath,
                    directory: input.directory,
                    ...(input.serverPassword !== undefined
                      ? { serverPassword: input.serverPassword }
                      : {}),
                    ...(input.environment ? { environment: input.environment } : {}),
                    ...(input.verify ? { verify: input.verify } : {}),
                  })
                  .pipe(Effect.provideService(Scope.Scope, serverScope)),
              ),
            );
            if (Exit.isFailure(started)) {
              yield* Scope.close(serverScope, Exit.void).pipe(Effect.ignore);
              return yield* Effect.failCause(started.cause);
            }

            const server: OpenCodeOwnedServerProcess = Object.freeze({
              ...started.value,
              runtimeGeneration,
              isCurrentAndRunning: mutex.withPermit(
                Effect.suspend(() =>
                  state.server === server ? started.value.isRunning : Effect.succeed(false),
                ),
              ),
            });
            state.server = server;
            state.serverScope = serverScope;
            state.borrowers = 1;
            yield* watchServerExit(server).pipe(Effect.forkIn(ownerScope));
            return server;
          }),
        );
      }),
    );

  const releaseServer = (server: OpenCodeOwnedServerProcess) =>
    mutex.withPermit(
      Effect.gen(function* () {
        if (state.server !== server) {
          return;
        }
        state.borrowers = Math.max(0, state.borrowers - 1);
        if (state.borrowers > 0) {
          return;
        }
        yield* cancelIdleClose();
        state.idleCloseFiber = yield* Effect.sleep(OPENCODE_SERVER_IDLE_TTL).pipe(
          Effect.andThen(
            mutex.withPermit(
              Effect.gen(function* () {
                if (state.server !== server || state.borrowers > 0) {
                  return;
                }
                state.idleCloseFiber = null;
                yield* closeServer(server);
              }),
            ),
          ),
          Effect.forkIn(ownerScope),
        );
      }),
    );

  yield* Effect.addFinalizer(() =>
    mutex.withPermit(
      Effect.gen(function* () {
        yield* cancelIdleClose();
        state.borrowers = 0;
        yield* closeServer();
      }),
    ),
  );

  return OpenCodeServerOwner.of({
    subscribeBeforeRuntimeReplacement: (listener) => {
      const entry = {
        listener: (generation: string) =>
          listener(generation).pipe(
            Effect.mapError(
              (cause) =>
                new OpenCodeRuntime.OpenCodeRuntimeError({
                  operation: "beforeRuntimeReplacement",
                  detail: "Could not register the replacement OpenCode runtime.",
                  cause,
                }),
            ),
          ),
      };
      return Effect.acquireRelease(
        mutex.withPermit(
          Effect.sync(() => {
            replacementListeners.add(entry);
          }),
        ),
        () =>
          mutex.withPermit(
            Effect.sync(() => {
              replacementListeners.delete(entry);
            }),
          ),
      );
    },
    withServer: (use, beforeNativeCreation) =>
      Effect.uninterruptibleMask((restore) =>
        restore(acquireServer(beforeNativeCreation)).pipe(
          Effect.flatMap((server) =>
            restore(use(server)).pipe(Effect.ensuring(releaseServer(server))),
          ),
        ),
      ),
  });
});

/** @public Service construction is part of the canonical Effect module API. */
export const layer = (input: {
  readonly binaryPath: string;
  readonly directory: string;
  readonly serverPassword?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly verify?: (url: string) => Effect.Effect<string, OpenCodeRuntime.OpenCodeRuntimeError>;
}) => Layer.effect(OpenCodeServerOwner, make(input));
