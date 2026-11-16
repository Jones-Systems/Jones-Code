// @effect-diagnostics nodeBuiltinImport:off -- Verify release of the exact fixture listener, never scan ports.
import * as NodeNet from "node:net";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import { makeHome, startServer } from "./serverFixture.ts";

class SetupProbeFailure extends Schema.TaggedError<SetupProbeFailure>()("SetupProbeFailure", {}) {
  override get message() {
    return "JE-10 deliberate setup failure after listener acquisition";
  }
}

const descriptor = (url: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* HttpClient.filterStatusOk(client).get(
      `${url}/.well-known/t3/environment`,
    );
    return yield* response.json;
  }).pipe(Effect.timeout("5 seconds"), Effect.provide(FetchHttpClient.layer));

const assertListenerClosed = (url: string) =>
  Effect.callback<void>((resume) => {
    const socket = NodeNet.connect({ host: "127.0.0.1", port: Number(new URL(url).port) });
    socket.setTimeout(5_000);
    socket.once("connect", () => {
      socket.destroy();
      resume(Effect.die("Owned listener remained reachable after scope closed"));
    });
    socket.once("timeout", () => {
      socket.destroy();
      resume(Effect.die("Listener cleanup readback timed out"));
    });
    socket.once("error", (error: NodeJS.ErrnoException) => {
      socket.destroy();
      resume(error.code === "ECONNREFUSED" ? Effect.void : Effect.die(error));
    });
    return Effect.sync(() => {
      socket.destroy();
    });
  });

it.effect("JE-10: same-home startup refuses without changing the owner; fresh home works", () =>
  Effect.gen(function* () {
    const roots = yield* Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const firstHome = yield* makeHome("owner");
        const freshHome = yield* makeHome("fresh");
        const first = yield* startServer(firstHome);
        const state = yield* fs.readFileString(first.config.serverRuntimeStatePath);
        const before = yield* descriptor(first.url);
        const refused = yield* Effect.scoped(startServer(firstHome)).pipe(Effect.result);
        expect(refused._tag).toBe("Failure");
        if (refused._tag === "Failure") {
          expect(refused.failure).toMatchObject({ _tag: "ServerAlreadyRunningError" });
        }
        expect(yield* fs.readFileString(first.config.serverRuntimeStatePath)).toBe(state);
        expect(yield* descriptor(first.url)).toEqual(before);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const fresh = yield* startServer(freshHome);
            expect(fresh.url).not.toBe(first.url);
            expect(fresh.environmentId).not.toBe(first.environmentId);
            expect(yield* descriptor(fresh.url)).toMatchObject({
              environmentId: fresh.environmentId,
            });
          }),
        );
        expect(yield* fs.readFileString(first.config.serverRuntimeStatePath)).toBe(state);
        expect(yield* descriptor(first.url)).toEqual(before);
        // Closing only the fresh server releases its lock without releasing the first's.
        yield* Effect.scoped(startServer(freshHome));
        const stillOwned = yield* Effect.scoped(startServer(firstHome)).pipe(Effect.result);
        expect(stillOwned._tag).toBe("Failure");
        if (stillOwned._tag === "Failure") {
          expect(stillOwned.failure).toMatchObject({ _tag: "ServerAlreadyRunningError" });
        }
        return [firstHome, freshHome];
      }),
    );
    const fs = yield* FileSystem.FileSystem;
    for (const root of roots) expect(yield* fs.exists(root)).toBe(false);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "JE-10: cancelled and failed setup closes only its owned listener and removes its home",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const sentinelHome = yield* makeHome("sentinel");
        const sentinel = yield* startServer(sentinelHome);
        const before = yield* descriptor(sentinel.url);
        const state = yield* fs.readFileString(sentinel.config.serverRuntimeStatePath);
        const acquired = yield* Deferred.make<{ home: string; url: string }>();
        const setup = yield* Effect.scoped(
          Effect.gen(function* () {
            const home = yield* makeHome("cancelled");
            yield* startServer(home, (url) =>
              Deferred.succeed(acquired, { home, url }).pipe(Effect.andThen(Effect.never)),
            );
          }),
        ).pipe(Effect.forkScoped);
        const cancelled = yield* Deferred.await(acquired).pipe(
          Effect.raceFirst(
            Fiber.join(setup).pipe(
              Effect.andThen(Effect.die("Fixture setup ended before the cancellation gate")),
            ),
          ),
        );
        expect(yield* descriptor(cancelled.url)).toBeDefined();
        yield* Fiber.interrupt(setup);
        expect(yield* fs.exists(cancelled.home)).toBe(false);
        yield* assertListenerClosed(cancelled.url);

        const failedLocation = yield* Ref.make<{ home: string; url: string } | undefined>(
          undefined,
        );
        const injected = new SetupProbeFailure();
        const failed = yield* Effect.scoped(
          Effect.gen(function* () {
            const home = yield* makeHome("failed");
            yield* startServer(home, (url) =>
              Ref.set(failedLocation, { home, url }).pipe(Effect.andThen(Effect.fail(injected))),
            );
          }),
        ).pipe(Effect.result);
        expect(failed._tag).toBe("Failure");
        if (failed._tag === "Failure") expect(failed.failure).toBe(injected);
        const location = yield* Ref.get(failedLocation);
        expect(location).toBeDefined();
        if (location === undefined)
          return yield* Effect.die("setup never reached the injected failure");
        expect(yield* fs.exists(location.home)).toBe(false);
        yield* assertListenerClosed(location.url);
        expect(yield* fs.exists(sentinelHome)).toBe(true);
        expect(yield* fs.readFileString(sentinel.config.serverRuntimeStatePath)).toBe(state);
        expect(yield* descriptor(sentinel.url)).toEqual(before);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
