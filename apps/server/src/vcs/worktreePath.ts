// Preserve the Git driver's native platform path semantics without filesystem work.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodePath from "node:path";

export const nativeWorktreePath = (input: {
  readonly worktreesDir: string;
  readonly cwd: string;
  readonly branch: string;
  readonly path?: string | null;
}): string =>
  input.path ??
  NodePath.join(input.worktreesDir, NodePath.basename(input.cwd), input.branch.replace(/\//g, "-"));
