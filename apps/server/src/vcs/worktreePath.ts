// Preserve the Git driver's native platform path semantics without filesystem work.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as path from "node:path";

export const nativeWorktreePath = (input: {
  readonly worktreesDir: string;
  readonly cwd: string;
  readonly branch: string;
  readonly path?: string | null;
}): string =>
  input.path ??
  path.join(input.worktreesDir, path.basename(input.cwd), input.branch.replace(/\//g, "-"));
