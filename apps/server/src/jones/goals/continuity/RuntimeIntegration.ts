import * as Layer from "effect/Layer";
import * as GoalContinuity from "./GoalContinuity.ts";
import * as GoalContinuityStore from "./GoalContinuityStore.ts";

/** One layer identity shares captured producer and once-only permit authority
 * across MCP requests, ordinary turns and native goal successors. */
export const layer = GoalContinuity.layer.pipe(Layer.provide(GoalContinuityStore.layer));
