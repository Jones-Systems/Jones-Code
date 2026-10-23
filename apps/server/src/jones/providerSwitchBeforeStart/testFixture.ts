import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ServerConfig from "../../config.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

export const layer = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-switch-before-start-" });
    return yield* ProviderReplayHarness.makeReplayServerConfig("provider-switch-before-start").pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        makeTempDirectory: (options) => fs.makeTempDirectory({ ...options, directory: root }),
      }),
    );
  }),
).pipe(Layer.provide(NodeServices.layer));
