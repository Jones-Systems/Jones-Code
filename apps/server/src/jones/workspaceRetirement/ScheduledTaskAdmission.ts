import { ScheduledTaskError, type ScheduledTask } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { RetirementError } from "./Receipts.ts";

/** The scheduler owns the surrounding SQL write transaction. This check is
 * about a real explicit binding, never optional provenance or workspace health. */
export const assertScheduledTarget = (
  sql: SqlClient.SqlClient["Service"],
  task: Pick<ScheduledTask, "id" | "workspaceStrategy">,
) => Effect.gen(function* () {
  if (task.workspaceStrategy.type !== "existing_worktree") return;
  const target = task.workspaceStrategy.worktreePath;
  const rows = yield* sql<{ state: string }>`SELECT state FROM jones_workspace_retirement_state WHERE worktree=${target}`;
  if (rows[0]) return yield* new ScheduledTaskError({
    taskId: task.id,
    message: "The scheduled task's checkout is retiring or retired.",
    cause: new RetirementError({ target, reason: rows[0].state }),
  });
});
