import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as Settings from "../../serverSettings.ts";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import type { DeletionWorktreeRemovalStartV1 } from "../cleanup/DeletionWorktreeRemovalTypes.ts";
import { RetirementError } from "./Receipts.ts";
import { evaluate } from "./Runtime.ts";

export class RemovalGate extends Context.Service<RemovalGate, {
  readonly remove: (start:DeletionWorktreeRemovalStartV1,revalidate:Effect.Effect<void,RetirementError>) => Effect.Effect<{exitCode:number|null;completion:"exited"|"unknown"},RetirementError>;
}>()("jones/workspaceRetirement/RemovalGate") {}
export const layer = Layer.effect(RemovalGate,Effect.gen(function* () {
  const context=yield* Effect.context<Settings.ServerSettingsService | Config.ServerConfig | Projections.ProjectionStoreV2 | SqlClient.SqlClient>();
  const settings=yield* Settings.ServerSettingsService;
  return RemovalGate.of({remove:Effect.fn("WorkspaceRetirement.removeDeletion")(function* (start,revalidate) {
    const rules=resolveWorktreeCleanup(yield* settings.getSettings,start.target.projectId);
    const result=yield* evaluate({cause:"deletion",worktree:start.target.path,projectRoot:start.target.projectRoot,threadId:null,rules,execute:true,admit:revalidate,revalidate}).pipe(Effect.provide(context));
    if(result.state!=="retired") return yield* new RetirementError({target:start.target.path,reason:result.reasons[0] ?? "retirement_hold"});
    return {exitCode:0,completion:"exited" as const};
  },Effect.mapError(cause=>cause instanceof RetirementError ? cause : new RetirementError({target:"deletion",reason:"retirement_unavailable",cause}))) });
}));
