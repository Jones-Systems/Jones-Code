import { WorkspaceRetirementOutcome } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export const lookup = Effect.gen(function* () {
  const sql=yield* Effect.serviceOption(SqlClient.SqlClient);
  return Effect.fn("WorkspaceRetirement.lookupOutcome")(function* (threadId:string) {
    if(Option.isNone(sql)) return {retirementObservation:"unavailable" as const};
    const rows=yield* sql.value<{outcome_json:string|null}>`SELECT outcome_json FROM jones_workspace_retirement_intents WHERE thread_id=${threadId} ORDER BY rowid DESC LIMIT 1`;
    if(!rows[0]?.outcome_json) return {retirementObservation:"none" as const};
    const retirement=yield* Schema.decodeEffect(Schema.fromJsonString(WorkspaceRetirementOutcome))(rows[0].outcome_json);
    return {retirementObservation:"recorded" as const,retirement};
  },Effect.catch(()=>Effect.succeed({retirementObservation:"unavailable" as const})));
});
