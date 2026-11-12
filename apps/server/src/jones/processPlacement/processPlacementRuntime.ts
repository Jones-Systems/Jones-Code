import { AgentScope } from "@t3tools/shared/AgentScope";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcessSpawner } from "effect/process";
import * as AgentScopeLive from "../../process/agentScope.ts";
import * as ProcessRunner from "../../processRunner.ts";
import { readProcessPlacementBinding } from "./processPlacement.ts";
import * as PlacementAcp from "./processPlacementAcp.ts";
import * as PlacementAgentScope from "./processPlacementAgentScope.ts";
import * as PlacementSpawner from "./processPlacementSpawner.ts";

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const binding = yield* Effect.sync(() => readProcessPlacementBinding());
    const raw = yield* ChildProcessSpawner.ChildProcessSpawner;
    // Construct a distinct management runner before publishing the placing spawner.
    const management = yield* ProcessRunner.make().pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, raw),
    );
    const liveScope = Layer.effect(
      AgentScope,
      AgentScopeLive.make.pipe(Effect.provideService(ProcessRunner.ProcessRunner, management)),
    );
    const scope = PlacementAgentScope.layer(binding).pipe(Layer.provide(liveScope));
    return PlacementSpawner.layer(binding).pipe(
      Layer.provideMerge(scope),
      Layer.provideMerge(PlacementAcp.layer(binding)),
    );
  }),
);
