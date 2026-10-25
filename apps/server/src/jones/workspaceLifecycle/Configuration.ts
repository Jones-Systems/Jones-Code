import * as Schema from "effect/Schema";
import { Binding } from "./Protocol.ts";

export const Configuration = Schema.Struct({
  environmentId: Binding.fields.environment_id,
  projects: Schema.Array(
    Schema.Struct({
      projectId: Binding.fields.thread_id,
      repository: Binding.fields.repository,
      projectRoot: Binding.fields.worktree,
    }),
  ).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(128),
    Schema.makeFilter(
      (projects) => new Set(projects.map((p) => p.projectId)).size === projects.length,
    ),
  ),
  registry: Schema.Struct({
    executable: Binding.fields.worktree,
    arguments: Schema.Array(
      Schema.String.check(Schema.isMaxLength(4096), Schema.isPattern(/^[^\0]*$/u)),
    ).check(Schema.isMaxLength(32)),
    database: Binding.fields.worktree,
    cwd: Binding.fields.worktree,
  }),
});
export type Configuration = typeof Configuration.Type;
