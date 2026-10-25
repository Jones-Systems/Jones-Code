// @effect-diagnostics nodeBuiltinImport:off -- Each fixture owns and closes its loopback HTTP server.
import * as NodeHttp from "node:http";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";
import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import { prepareAndStopAllPoolInstances } from "../../app/DesktopApp.ts";
import { prepareLocalBackendsForQuit } from "./jonesQuitPreparation.ts";

function makeInstance(input: {
  readonly id: string;
  readonly listener: string;
  readonly ready?: boolean;
  readonly hasConfig?: boolean;
  readonly stop?: DesktopBackendPool.DesktopBackendInstance["stop"];
}): DesktopBackendPool.DesktopBackendInstance {
  return {
    id: DesktopBackendPool.BackendInstanceId(input.id),
    label: Effect.succeed(input.id),
    start: Effect.void,
    stop: input.stop ?? (() => Effect.void),
    snapshot: Effect.succeed({
      desiredRunning: true,
      ready: input.ready ?? true,
      activePid: Option.none(),
      restartAttempt: 0,
      restartScheduled: false,
    }),
    currentConfig: Effect.succeed(
      input.hasConfig === false
        ? Option.none()
        : Option.some({
            executablePath: "/fixture/electron",
            entryPath: "/fixture/server.mjs",
            args: [],
            cwd: "/fixture",
            env: {},
            extendEnv: false,
            bootstrap: {
              mode: "desktop",
              noBrowser: true,
              port: 3773,
              t3Home: "/fixture",
              host: "127.0.0.1",
              desktopBootstrapToken: "fixture-bootstrap",
              tailscaleServeEnabled: false,
              tailscaleServePort: 443,
            },
            bootstrapDelivery: "stdin",
            httpBaseUrl: new URL(input.listener),
            captureOutput: false,
            preflightFailure: Option.none(),
          }),
    ),
    waitForReady: () => Effect.succeed(input.ready ?? true),
  };
}

async function withBackend(
  status: number,
  body: (listener: string, paths: string[]) => Promise<void>,
) {
  const paths: string[] = [];
  const server = NodeHttp.createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      paths.push(request.url ?? "");
      response.setHeader("content-type", "application/json");
      if (request.url === "/oauth/token") {
        response.end(JSON.stringify({ access_token: "fixture-access" }));
      } else {
        response.statusCode = status;
        response.end(JSON.stringify({ prepared: true, threadCount: 1 }));
      }
    });
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("Missing fixture listener.");
    await body(`http://127.0.0.1:${address.port}`, paths);
  } finally {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    });
  }
}

it("prepares every ready owned loopback backend without requiring installer state", async () =>
  withBackend(200, async (listener, paths) => {
    const instances = ["primary", "wsl"].map((id) => makeInstance({ id, listener }));
    await Effect.runPromise(
      prepareLocalBackendsForQuit().pipe(Effect.provide(DesktopBackendPool.layerTest(instances))),
    );
    expect(paths.filter((path) => path === "/api/jones-updates/prepare-quit")).toHaveLength(2);
  }));

it("skips not-ready, unconfigured, non-loopback and non-http instances", async () =>
  withBackend(200, async (listener, paths) => {
    const instances = [
      makeInstance({ id: "not-ready", listener, ready: false }),
      makeInstance({ id: "unconfigured", listener, hasConfig: false }),
      makeInstance({ id: "remote", listener: listener.replace("127.0.0.1", "192.0.2.1") }),
      makeInstance({ id: "https", listener: listener.replace("http:", "https:") }),
    ];
    await Effect.runPromise(
      prepareLocalBackendsForQuit().pipe(Effect.provide(DesktopBackendPool.layerTest(instances))),
    );
    expect(paths).toEqual([]);
  }));

it.each([200, 404, 500])("prepares before stop and still stops after HTTP %s", async (status) =>
  withBackend(status, async (listener, paths) => {
    const instance = makeInstance({
      id: "primary",
      listener,
      stop: (options) =>
        Effect.sync(() => {
          expect(options).toBeUndefined();
          expect(paths).toEqual(["/oauth/token", "/api/jones-updates/prepare-quit"]);
          paths.push("stop");
        }),
    });
    // Exercise the actual app hook in an uninterruptible finalizer.
    await Effect.runPromise(
      Effect.scoped(Effect.addFinalizer(() => prepareAndStopAllPoolInstances())).pipe(
        Effect.provide(DesktopBackendPool.layerTest([instance])),
      ),
    );
    expect(paths.at(-1)).toBe("stop");
  }));

it("tolerates a refused connection and still stops", async () => {
  let listener = "";
  await withBackend(200, async (value) => {
    listener = value;
  });
  let stopped = false;
  const instance = makeInstance({
    id: "primary",
    listener,
    stop: () => Effect.sync(() => {
      stopped = true;
    }),
  });
  await Effect.runPromise(
    prepareAndStopAllPoolInstances().pipe(Effect.provide(DesktopBackendPool.layerTest([instance]))),
  );
  expect(stopped).toBe(true);
});

it.effect("bounds even an uninterruptible finalizer's preparation wait at three seconds", () =>
  Effect.gen(function* () {
    let stopped = false;
    const instance = makeInstance({
      id: "pending",
      listener: "http://127.0.0.1:3773",
      stop: () => Effect.sync(() => {
        stopped = true;
      }),
    });
    const fiber = yield* Effect.scoped(
      Effect.addFinalizer(() => prepareAndStopAllPoolInstances()),
    ).pipe(
      Effect.provide(DesktopBackendPool.layerTest([{ ...instance, snapshot: Effect.never }])),
      Effect.forkChild,
    );
    yield* TestClock.adjust("2999 millis");
    expect(stopped).toBe(false);
    yield* TestClock.adjust("1 millis");
    yield* Fiber.join(fiber);
    expect(stopped).toBe(true);
  }));
