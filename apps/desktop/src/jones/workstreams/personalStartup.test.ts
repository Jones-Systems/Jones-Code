import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import { runBackendProcess } from "../../backend/DesktopBackendManager.ts";

it.effect(
  "starts a personal backend without consulting legacy owner activation or provisioning descriptors",
  () =>
    Effect.gen(function* () {
      let spawned = false;
      const spawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make((command) => {
          assert.equal(command._tag, "StandardCommand");
          if (command._tag === "StandardCommand") {
            assert.equal("fd8" in command.options.additionalFds!, false);
            assert.equal("fd9" in command.options.additionalFds!, false);
          }
          spawned = true;
          return Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(123),
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              stdin: Sink.drain,
              getInputFd: () => Sink.drain,
              getOutputFd: () => {
                throw new Error("Legacy owner output must not be opened");
              },
              unref: Effect.succeed(Effect.void),
            }),
          );
        }),
      );
      const result = yield* runBackendProcess({
        executablePath: "/synthetic/electron",
        args: [],
        entryPath: "/synthetic/server",
        cwd: "/synthetic",
        env: {
          T3_WORKSTREAM_CONTROL_PLANE_URL: "https://unreachable.invalid",
          T3_WORKSTREAM_OWNER_ID: "legacy",
        },
        bootstrap: {
          mode: "desktop",
          noBrowser: true,
          port: 3773,
          t3Home: "/synthetic",
          host: "127.0.0.1",
          desktopBootstrapToken: "synthetic",
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
        },
        bootstrapDelivery: "fd3",
        extendEnv: false,
        captureOutput: false,
        httpBaseUrl: new URL("http://127.0.0.1:3773"),
        preflightFailure: Option.none(),
        desktopTelemetryStream: Stream.empty,
        jonesDesktopOwner: () => {
          throw new Error("Unreachable legacy owner must not gate startup");
        },
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            spawner,
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make(() => Effect.never),
            ),
          ),
        ),
      );
      assert.equal(spawned, true);
      assert.equal(Option.getOrNull(result.code), 0);
    }),
);
