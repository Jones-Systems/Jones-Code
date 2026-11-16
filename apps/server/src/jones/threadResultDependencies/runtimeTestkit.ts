import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ServerConfig from "../../config.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Changes from "./DependencyChanges.ts";

const adapter = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Result dependency tests never launch providers"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
// This test owns its scratch scope; cleanup happens on success, failure and interruption.
const config = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-result-dependency-" });
    return ServerConfig.layerTest(process.cwd(), directory);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
export const runtime = Replay.layerWithRegistryAndServices(
  { name: "result-dependencies" },
  Registry.layerFromAdapters([adapter]),
  {
    databaseLayer: Sqlite.layerMemory,
    serverConfigLayer: config,
    runEffectWorker: false,
    runContinuationWorker: false,
  },
).pipe(Layer.provideMerge(Changes.layer));
