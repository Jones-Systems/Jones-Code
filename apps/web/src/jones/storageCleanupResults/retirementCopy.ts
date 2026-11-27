import type { WorkspaceRetirementOutcome } from "@t3tools/contracts";
import { formatBytes } from "../../lib/formatBytes.ts";

/** What the owner reads about a retirement before pressing Retire, or about the branch that kept it. */

const REASON_LABELS: Readonly<Record<string, string>> = {
  preserve_commit_failed:
    "Saving the checkout's content to a branch failed, so nothing was removed.",
  preserve_index_isolation_unverified:
    "Could not confirm the checkout's own Git index was left untouched while saving, so nothing was removed.",
  preserve_branch_unavailable:
    "No free preserve branch name could be created, so nothing was removed.",
  preserve_remote_unavailable:
    "The remote could not be read, so the branch could not be pushed or checked. Nothing was removed.",
  preserve_store_not_durable:
    "The saved branch lives in a repository that is itself being removed or is temporary, so it would not keep the content. Nothing was removed.",
  preserve_exclusions_changed:
    "Files that cannot be saved changed after the proposal. Propose again; nothing was removed.",
  preserve_selection_changed:
    "The files to save changed after the proposal. Propose again; nothing was removed.",
  preserve_tree_mismatch:
    "The checkout no longer matches the saved branch, so removal was not forced. Propose again.",
  preserve_readback_failed:
    "The saved branch could not be read back from this machine's repository. Check it before relying on it.",
  preserve_remote_readback_failed:
    "The saved branch could not be read back from the remote. Check it before relying on it.",
  preserve_path_unrepresentable:
    "A file name cannot be recorded exactly in a branch, so the checkout stays.",
  interrupted_preserved_path_missing:
    "An interrupted retirement removed a saved file that the preserve branch should still hold. Restore from the branch before retrying.",
};

/** A human sentence for a reason code introduced with preserve branches, or null for any other code. */
export function retirementReasonLabel(code: string): string | null {
  return REASON_LABELS[code] ?? null;
}

const EXCLUSION_LABELS: Readonly<Record<string, string>> = {
  sensitive_name: "Name looks like a secret or credential file",
  oversized: "Too large to save (over 50 MiB, or the set is over 500 MiB)",
  nested_repository: "Contains its own Git repository",
};

/** A human reason for a path no preserve branch may carry. Unknown codes read as the code itself. */
export function exclusionLabel(reason: string): string {
  if (reason.startsWith("secret_pattern:"))
    return `Content matches a secret pattern (${reason.slice("secret_pattern:".length)})`;
  return EXCLUSION_LABELS[reason] ?? reason;
}

const plural = (count: number, one: string, other = `${one}s`) =>
  `${count.toLocaleString()} ${count === 1 ? one : other}`;

type Recoverability = NonNullable<WorkspaceRetirementOutcome["recoverability"]>;

export type ProposalCopy =
  | { kind: "recreatable"; lines: string[] }
  | { kind: "preserve"; lines: string[] }
  | { kind: "excluded"; lines: string[]; excluded: { path: string; label: string }[] };

/**
 * The plain statement of what Retire will do. `null` when the server sent nothing to state, in which
 * case the row keeps its existing reason-code behavior.
 */
export function proposalCopy(recoverability: Recoverability | undefined): ProposalCopy | null {
  if (!recoverability) return null;
  const plan = recoverability.preserve;
  const excluded = (plan?.excluded ?? []).map(({ path, reason }) => ({
    path,
    label: exclusionLabel(reason),
  }));
  if (excluded.length > 0)
    return {
      kind: "excluded",
      lines: [
        `Held: ${plural(excluded.length, "file")} cannot be saved to a branch, so Retire would lose ${excluded.length === 1 ? "it" : "them"}. Move or remove ${excluded.length === 1 ? "it" : "them"} yourself, then propose again.`,
      ],
      excluded,
    };
  if (recoverability.verdict === "recreatable")
    return {
      kind: "recreatable",
      lines: [
        "Safe to retire: everything in this checkout is already kept in a branch or can be regenerated. Nothing is lost.",
        ...(recoverability.regenerable.length > 0
          ? [
              `Regenerable output (${plural(recoverability.regenerable.length, "folder")}) is deleted with the checkout.`,
            ]
          : []),
      ],
    };
  if (recoverability.verdict !== "retirable_after_preserve" || !plan) return null;
  const saved = `${plural(plan.files, "file")} (${formatBytes(plan.bytes)})`;
  const remote =
    plan.remote === undefined
      ? "The branch is pushed to this repository's remote when it has one; otherwise it stays local-only, in this machine's repository."
      : plan.remote === null
        ? "This repository has no remote, so the branch stays local-only, in this machine's repository."
        : `The branch is pushed to the remote "${plan.remote}" and read back to confirm. If the push is refused, it stays local-only, in this machine's repository.`;
  return {
    kind: "preserve",
    lines: [
      `Retire saves ${saved} to a new preserve/… branch before removing this checkout${plan.deletions > 0 ? `, and records ${plural(plan.deletions, "deleted file")}` : ""}.`,
      remote,
      "Removal uses --force only after the saved content is verified to match this checkout; if anything changed since, nothing is removed.",
      "Preserve branches are never deleted automatically.",
    ],
  };
}

/**
 * Whether the recoverability verdict alone settles Retire: `true` when a preserve branch or nothing
 * at all covers the checkout, `false` when a path no branch may carry holds it, and `null` when the
 * verdict says nothing either way (none sent, held or unknown), so the reason codes decide.
 */
export function recoverabilityAllowsRetire(recoverability: Recoverability | undefined) {
  if (!recoverability) return null;
  if ((recoverability.preserve?.excluded.length ?? 0) > 0) return false;
  return recoverability.verdict === "retirable_after_preserve" ||
    recoverability.verdict === "recreatable"
    ? true
    : null;
}

/** Short sha for display; the full sha stays in the restore command's branch. */
export const shortSha = (sha: string) => sha.slice(0, 10);

type Preserve = NonNullable<WorkspaceRetirementOutcome["preserve"]>;

export function verificationLabel(verification: Preserve["verification"]) {
  return verification === "remote_verified" ? "Remote-verified" : "Local-only";
}

export function readbackLabel(readback: Preserve["readback"]) {
  if (!readback) return "Not read back yet";
  const local = readback.local === "verified" ? "verified" : "FAILED";
  const remote =
    readback.remote === "not_applicable"
      ? "not applicable (not pushed)"
      : readback.remote === "verified"
        ? "verified"
        : "FAILED";
  return `Local branch ${local} · remote ${remote}`;
}

type RetirementSelection = {
  path: string;
  coveredPaths: readonly string[];
  recoverability: Recoverability | undefined;
};

/** The sentence the Retire confirmation shows. Falls back to the original text without a preserve plan. */
export function retireConfirmation(input: RetirementSelection) {
  const copy = proposalCopy(input.recoverability);
  const covered = `${input.coveredPaths.length ? input.coveredPaths.join("\n") : "No local content selected."}`;
  if (copy?.kind === "preserve")
    return `Retire ${input.path}?
${copy.lines.join("\n")}
Branches and recovery references are kept. Retire also authorizes disposal of these exact selections, including additions within the listed roots:
${covered}
Live consumers, nested repositories, secrets and other protections still hold.`;
  return `Retire ${input.path}? Branches and recovery references are kept. This authorizes disposal only of these exact selections, including additions within the listed roots:
${covered}
Live consumers, tracked edits, nested repositories and other protections still hold.`;
}

export function retireAllConfirmation(
  selections: readonly RetirementSelection[],
  strings: { title: string; description: string; noSelection: string },
) {
  const checkouts = selections.map((selection) => {
    const copy = proposalCopy(selection.recoverability);
    return [
      selection.path,
      ...(copy?.kind === "preserve" ? copy.lines : []),
      selection.coveredPaths.length ? selection.coveredPaths.join("\n") : strings.noSelection,
    ].join("\n");
  });
  return [strings.title, strings.description, ...checkouts].join("\n\n");
}
