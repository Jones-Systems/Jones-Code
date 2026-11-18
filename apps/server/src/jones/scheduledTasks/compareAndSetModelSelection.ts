import type { ModelSelection, ScheduledTask } from "@t3tools/contracts";
import type * as SqlClient from "effect/sql/SqlClient";

export function compareAndSetModelSelection(
  sql: SqlClient.SqlClient,
  expected: ScheduledTask,
  modelSelection: ModelSelection,
  updatedAt: ScheduledTask["updatedAt"],
) {
  return sql<{ task_id: string }>`
    UPDATE scheduled_tasks
    SET model_selection_json = ${JSON.stringify(modelSelection)}, updated_at = ${updatedAt}
    WHERE task_id = ${expected.id}
      AND created_at = ${expected.createdAt}
      AND updated_at = ${expected.updatedAt}
      AND project_id = ${expected.projectId}
      AND thread_id IS NULL
      AND COALESCE(thread_mode, 'new_thread') = 'new_thread'
    RETURNING task_id
  `;
}
