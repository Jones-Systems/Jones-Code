import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Net from "@t3tools/shared/Net";
import { resolveServerConfig } from "../../cli/config.ts";

it.effect(
  "normal CLI configuration supplies only the explicit fixed checkpoint command and remains absent by default",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-checkpoint-config-" });
      const flags = {
        mode: Option.some("web" as const),
        port: Option.some(4888),
        host: Option.some("127.0.0.1"),
        baseDir: Option.some(root),
        cwd: Option.some(root),
        devUrl: Option.none<URL>(),
        noBrowser: Option.some(true),
        bootstrapFd: Option.none<number>(),
        autoBootstrapProjectFromCwd: Option.none<boolean>(),
        logWebSocketEvents: Option.none<boolean>(),
        tailscaleServeEnabled: Option.some(false),
        tailscaleServePort: Option.none<number>(),
      };
      const config = {
        executable: "/qualified/host-tooling",
        arguments: ["fleet-controller"],
        binding: "/qualified/client.json",
        cwd: "/qualified",
        controllerId: "controller",
      };
      const resolve = (env: Record<string, string>) =>
        resolveServerConfig(flags, Option.none(), { rejectRunningServer: false }).pipe(
          Effect.provide([ConfigProvider.layer(ConfigProvider.fromEnv({ env })), Net.layer]),
        );
      expect((yield* resolve({})).fleetCheckpoint).toBeUndefined();
      expect(
        (yield* resolve({ JONES_FLEET_CHECKPOINT_COMMAND: JSON.stringify(config) }))
          .fleetCheckpoint,
      ).toEqual(config);
      expect(
        Exit.isFailure(yield* Effect.exit(resolve({ JONES_FLEET_CHECKPOINT_COMMAND: "not JSON" }))),
      ).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
);
