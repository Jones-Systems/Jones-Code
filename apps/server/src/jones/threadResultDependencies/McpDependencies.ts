import * as Layer from "effect/Layer";
import * as DependencyChanges from "./DependencyChanges.ts";
import * as RequestWatch from "./RequestWatch.ts";

// Reuse these layer objects in the server graph so MCP, the reactor and
// event infrastructure share one memoized watcher and notification bus.
// Orchestrator and SQL remain inputs; neither is constructed through MCP.
export const layer = RequestWatch.layer.pipe(Layer.provideMerge(DependencyChanges.layer));
