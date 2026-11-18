import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { inside } from "./Filesystem.ts";
import { RetirementError, type RetirementReceipt } from "./Receipts.ts";

const isRetirementError = Schema.is(RetirementError);

/**
 * Forgets a retired checkout's active, non-evidence output entries. Both ways of settling an
 * interrupted retirement use this, so they leave the same registry state. Entries may be recorded
 * under the requested or the canonical path, so both are read; evidence and other checkouts' entries
 * are left alone.
 */
export const forgetWorktreeOutputs = Effect.fn("WorkspaceRetirement.forgetWorktreeOutputs")(
  function* (
    registry: Registry.LifecycleRegistry["Service"] | null,
    worktrees: readonly string[],
    settled: RetirementReceipt,
  ) {
    if (!registry?.outputs) return;
    const roots = [...new Set([...worktrees, settled.binding.path])];
    const internal = new Map<string, Registry.OutputRecord>();
    for (const root of roots)
      for (const record of yield* registry.outputs(root))
        if (
          roots.some((spelling) => inside(record.path, spelling)) &&
          record.state === "active" &&
          record.class !== "evidence"
        )
          internal.set(record.id, record);
    if (!internal.size) return;
    if (!registry.forgetOutputs)
      return yield* new RetirementError({
        target: settled.binding.path,
        reason: "output_finalization_unavailable",
      });
    yield* registry.forgetOutputs([...internal.values()]);
  },
  Effect.mapError((cause) =>
    isRetirementError(cause)
      ? cause
      : new RetirementError({ target: "workspace", reason: "observation_failed", cause }),
  ),
);
