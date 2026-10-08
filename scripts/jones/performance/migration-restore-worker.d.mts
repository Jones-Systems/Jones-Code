import type { ThreadId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { SqlError } from "effect/sql/SqlError";
import type * as SqlClient from "effect/sql/SqlClient";
import type { readApplicationBirthRecord } from "../../../apps/server/src/jones/importedHistory/ApplicationBirth.ts";

type Row = Readonly<Record<string, unknown>>;

type Modules = {
  readonly Effect: typeof Effect;
  readonly Contracts: { readonly ThreadId: typeof ThreadId };
  readonly Birth: { readonly readApplicationBirthRecord: typeof readApplicationBirthRecord };
};

type LookupEvidence = {
  readonly owner: "055_OrchestrationV2/RecoveryIndexes";
  readonly index: "orchestration_events_v2_created_threads_idx";
  readonly plan: ReadonlyArray<Row>;
  readonly actualLookupExecuted: true;
  readonly foreignBirthExcluded: true;
  readonly changedProjectionRejected: true;
  readonly missingIndexRejected: true;
};

export function receivingCreationLookupProgram<E>(scope: {
  readonly modules: Modules;
  readonly queryEffect: (
    text: string,
    values?: ReadonlyArray<string | number | null>,
  ) => Effect.Effect<ReadonlyArray<Row>, E, SqlClient.SqlClient>;
}): Effect.Effect<LookupEvidence, E | SqlError, SqlClient.SqlClient>;

export function probeReceivingCreationLookup(scope: {
  readonly modules: Modules;
  readonly run: <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) => Promise<A>;
  readonly query: (
    text: string,
    values?: ReadonlyArray<string | number | null>,
  ) => Promise<ReadonlyArray<Row>>;
}): Promise<LookupEvidence>;
