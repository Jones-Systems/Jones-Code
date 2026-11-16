import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { FollowUpCommandOwner } from "../remoteDelegation/FollowUp.ts";

/** Construct after Orchestrator and provide before remote delegation captures its services. */
export const layer = Layer.effect(FollowUpCommandOwner,
  Effect.map(Orchestrator.OrchestratorV2, (orchestrator) => orchestrator.followUpCommandOwner));
