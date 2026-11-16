#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off -- Explicit standalone adapter; reconciliation stays in the producer owner.
import { resolveProductions } from "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts";

export async function resolveCommand(args: readonly string[]) {
  if (
    args.length < 2 ||
    args[0] !== "--worktree" ||
    !args[1] ||
    args.length > 3 ||
    (args[2] !== undefined && args[2] !== "--json")
  )
    throw new Error("usage: resolve.ts --worktree <path> [--json]");
  const records = await resolveProductions(args[1]);
  return {
    schema: "workspace-production-resolution/v1",
    worktree: args[1],
    blocked: records.some((record) => record.reasons.length > 0 || record.outcome === "unresolved"),
    records,
  };
}

if (import.meta.main) {
  try {
    const result = await resolveCommand(process.argv.slice(2));
    process.stdout.write(JSON.stringify(result) + "\n");
    if (result.blocked) process.exitCode = 1;
  } catch (cause) {
    process.stderr.write(
      JSON.stringify({
        schema: "workspace-production-resolution/v1",
        error: cause instanceof Error ? cause.message : "resolution_unavailable",
      }) + "\n",
    );
    process.exitCode = 1;
  }
}
