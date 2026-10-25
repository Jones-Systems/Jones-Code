import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { HttpClient } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import { expect } from "vite-plus/test";

import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ClaudeAdapterV2 from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as ModelManifest from "../ModelManifest.ts";
import * as ProviderHostLive from "../ProviderHostLive.ts";
import * as ResetCreditCoordinator from "../resetCreditCoordinator.ts";
import { ClaudeDriver } from "./ClaudeDriver.ts";

const layerDeps = ServerConfig.layerTest(process.cwd(), {
  prefix: "claude-driver-evaluator-capability-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(McpProviderSessions.layer),
  Layer.provideMerge(Layer.mock(ClaudeAdapterV2.ClaudeAgentSdkQueryRunner)({})),
  Layer.provideMerge(Layer.mock(ServerSecretStore.ServerSecretStore)({})),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(ModelManifest.layerTest),
  Layer.provideMerge(ResetCreditCoordinator.layerTest),
  Layer.provideMerge(ProviderLatestVersions.layer),
  Layer.provideMerge(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Capability classification must not make HTTP requests")),
    ),
  ),
);
const layerTest = ProviderHostLive.layer.pipe(Layer.provideMerge(layerDeps));

it.layer(layerTest)("ClaudeDriver evaluator capability", (it) => {
  it.effect.each([
    { launchArgs: "", supported: true },
    { launchArgs: "--tools Read Bash --setting-sources user,local", supported: true },
    { launchArgs: "--settings=private-settings-value", supported: false },
    { launchArgs: "--unknown-account-flag private-account-value", supported: false },
  ])(
    "reports evaluator support=$supported without rejecting the provider (case %#)",
    ({ launchArgs, supported }) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const homePath = yield* fs.makeTempDirectoryScoped({ prefix: "claude-evaluator-home-" });
        const instance = yield* ClaudeDriver.create({
          instanceId: ProviderInstanceId.make("synthetic-claude-evaluator"),
          displayName: "Synthetic Claude evaluator",
          environment: [],
          enabled: false,
          config: { ...ClaudeDriver.defaultConfig(), enabled: false, homePath, launchArgs },
        }).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() => Effect.die("Capability classification must not spawn")),
          ),
        );
        const snapshot = yield* instance.snapshot.getSnapshot;
        expect(snapshot.supportsRemoteRequestEvaluation).toBe(supported);
        expect(snapshot.status).toBe("disabled");
        expect(instance.orchestrationAdapter).toBeDefined();
        expect(instance.textGeneration).toBeDefined();
      }).pipe(Effect.scoped),
  );
});
