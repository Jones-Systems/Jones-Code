import type {
  WorkspaceRetirementPreserve,
  WorkspaceRetirementRecoverability,
} from "@t3tools/contracts";
import type { PreservePlan, Recoverability } from "./Recoverability.ts";
import type { RetirementPreserve } from "./Receipts.ts";

/**
 * The owner-facing facts of a qualification: what removal would lose and what a preserve branch
 * would carry. `remote` is what a push would target (null when none), when the caller resolved it.
 * A qualification observed before recoverability existed yields nothing.
 */
export function recoverabilityOutcome(
  qualification:
    | {
        readonly recoverability?: Recoverability | undefined;
        readonly preservePlan?: PreservePlan | undefined;
      }
    | null
    | undefined,
  remote?: string | null,
): WorkspaceRetirementRecoverability | undefined {
  const recoverability = qualification?.recoverability;
  if (!recoverability) return undefined;
  const plan = qualification?.preservePlan;
  return {
    verdict: recoverability.verdict,
    regenerable: [...recoverability.regenerable],
    ...(plan
      ? {
          preserve: {
            files: plan.selected.length,
            deletions: plan.deletions.length,
            bytes: plan.selectedBytes,
            ...(remote === undefined ? {} : { remote }),
            excluded: plan.excluded.map(({ path, reason }) => ({ path, reason })),
            excludedBytes: plan.excludedBytes,
          },
        }
      : {}),
  };
}

/** The receipt's preserve branch, as the wire carries it. */
export function preserveOutcome(
  preserve: RetirementPreserve | undefined,
): WorkspaceRetirementPreserve | undefined {
  return preserve
    ? {
        branch: preserve.branch,
        sha: preserve.sha,
        remote: preserve.remote,
        verification: preserve.verification,
        excluded: preserve.excluded.map(({ path, reason }) => ({ path, reason })),
        restore: preserve.restore,
        forced: preserve.forced,
        ...(preserve.readback ? { readback: { ...preserve.readback } } : {}),
      }
    : undefined;
}

/**
 * Untracked paths a preserve branch will carry are not disposal selections, so the covered paths
 * the owner confirms omit them. Other verdicts keep every path.
 */
export const confirmedPaths = (
  verdict: WorkspaceRetirementRecoverability["verdict"] | undefined,
  ignored: readonly string[],
  untracked: readonly string[],
) => (verdict === "retirable_after_preserve" ? [...ignored] : [...ignored, ...untracked]);
