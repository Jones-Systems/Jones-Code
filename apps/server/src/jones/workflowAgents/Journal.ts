import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

type Kind = "step" | "write" | "turn" | "unconfirmed_stop";

export const make = (sql: SqlClient.SqlClient) => {
  const available = makeJonesTablePresence(sql)("jones_workflow_agent_journal");

  const read = Effect.fn("WorkflowAgentJournal.read")(function* (
    runId: string,
    kind: Kind,
    name: string,
  ) {
    if (!(yield* available)) return undefined;
    const [row] = yield* sql<{ binding: string | null }>`
      SELECT binding FROM jones_workflow_agent_journal
      WHERE run_id = ${runId} AND kind = ${kind} AND name = ${name}
        AND EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = ${runId})
    `;
    return row?.binding;
  });

  const insert = Effect.fn("WorkflowAgentJournal.insert")(function* (
    runId: string,
    kind: Exclude<Kind, "turn">,
    name: string,
    binding: string | null = null,
  ) {
    const rows = yield* sql`
      INSERT INTO jones_workflow_agent_journal (run_id, kind, name, binding)
      SELECT ${runId}, ${kind}, ${name}, ${binding}
      WHERE EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = ${runId})
      ON CONFLICT DO NOTHING RETURNING 1
    `;
    return rows.length === 1;
  });

  const registerStep = Effect.fn("WorkflowAgentJournal.registerStep")(function* (
    runId: string,
    name: string,
  ) {
    if (!(yield* available)) return;
    // A step from an older build may have issued a write without recording a receipt.
    yield* sql`
      INSERT INTO jones_workflow_agent_journal (run_id, kind, name, binding)
      SELECT ${runId}, 'step', ${name}, NULL
      WHERE EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = ${runId})
        AND NOT EXISTS (SELECT 1 FROM workflow_steps WHERE run_id = ${runId} AND name = ${name})
      ON CONFLICT DO NOTHING
    `;
  });

  const acceptTurn = Effect.fn("WorkflowAgentJournal.acceptTurn")(function* (
    runId: string,
    key: string,
    generation: string,
  ) {
    // A conflicting pairing permanently clears proof of ownership for this turn.
    const [row] = yield* sql<{ binding: string | null }>`
      INSERT INTO jones_workflow_agent_journal (run_id, kind, name, binding)
      SELECT ${runId}, 'turn', ${key}, ${generation}
      WHERE EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = ${runId})
      ON CONFLICT DO UPDATE SET binding = CASE
        WHEN jones_workflow_agent_journal.binding = excluded.binding THEN excluded.binding
        ELSE NULL
      END
      RETURNING binding
    `;
    return row?.binding === generation;
  });

  const hasUnconfirmedStop = Effect.fn("WorkflowAgentJournal.hasUnconfirmedStop")(function* (
    runId: string,
  ) {
    if (!(yield* available)) return false;
    const rows = yield* sql`
      SELECT 1 FROM jones_workflow_agent_journal WHERE run_id = ${runId} AND kind = 'unconfirmed_stop'
        AND EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = ${runId})
      LIMIT 1
    `;
    return rows.length > 0;
  });

  const removeRuns = Effect.fn("WorkflowAgentJournal.removeRuns")(function* (
    runIds: ReadonlyArray<string>,
  ) {
    if (yield* available)
      yield* sql`DELETE FROM jones_workflow_agent_journal WHERE ${sql.in("run_id", runIds)}`;
  });

  return { read, insert, registerStep, acceptTurn, hasUnconfirmedStop, removeRuns };
};
