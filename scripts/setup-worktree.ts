// @effect-diagnostics nodeBuiltinImport:off - runs before `vp i`, so only Node built-ins exist.
import * as NodePath from "node:path";
import { setupWorktree } from "./jones/worktree-setup/setup.ts";

try {
  process.exitCode = setupWorktree(
    process.argv.slice(2),
    process.env.T3CODE_PROJECT_ROOT,
    NodePath.dirname(import.meta.dirname),
  );
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : "Worktree setup failed"}\n`);
  process.exitCode = 1;
}
