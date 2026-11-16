// @effect-diagnostics nodeBuiltinImport:off -- Bounded read-only no-follow metadata and Git observations.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import type { WorkspaceRetirementOutcome } from "@t3tools/contracts";
import { fdPath, git, openAbsolute } from "./Filesystem.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

export type TargetPresence =
  | { status: "present" }
  | { status: "absent_unregistered" }
  | { status: "absent_registered" }
  | { status: "replaced"; reason: string }
  | { status: "unobservable"; reason: string };

export async function observeTarget(
  worktree: string,
  projectRoot: string,
  prior?: DutyIdentity | null,
): Promise<TargetPresence> {
  try {
    if (!NodePath.isAbsolute(worktree) || NodePath.normalize(worktree) !== worktree)
      return { status: "unobservable", reason: "target_path_invalid" };
    const parentPath = NodePath.dirname(worktree);
    const parent = await openAbsolute(parentPath, true);
    try {
      const before = await parent.stat({ bigint: true });
      const leaf = `${fdPath(parent.fd)}/${NodePath.basename(worktree)}`;
      try {
        const stat = await NodeFSP.lstat(leaf, { bigint: true });
        if (stat.isSymbolicLink()) return { status: "replaced", reason: "target_symlink" };
        if (!stat.isDirectory()) return { status: "replaced", reason: "target_not_directory" };
        if (
          prior?.status === "bound" &&
          (String(stat.dev) !== prior.binding.dev || String(stat.ino) !== prior.binding.ino)
        )
          return { status: "replaced", reason: "target_incarnation_changed" };
        return { status: "present" };
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
      }
      const project = await openAbsolute(projectRoot, true);
      try {
        const rootBefore = await project.stat({ bigint: true });
        if (prior?.status === "bound") {
          const common = await NodeFSP.realpath(
            (
              await git(
                projectRoot,
                ["rev-parse", "--path-format=absolute", "--git-common-dir"],
                1500,
              )
            )
              .toString()
              .trim(),
          );
          if (common !== prior.binding.commonDirectory)
            return { status: "unobservable", reason: "project_repository_changed" };
        }
        const listing = (
          await git(projectRoot, ["worktree", "list", "--porcelain", "-z"], 1500)
        ).toString();
        const rootAfter = await NodeFSP.lstat(projectRoot, { bigint: true });
        if (
          !rootAfter.isDirectory() ||
          rootBefore.dev !== rootAfter.dev ||
          rootBefore.ino !== rootAfter.ino
        )
          return { status: "unobservable", reason: "project_root_changed" };
        const registered = listing.split("\0").some((field) => field === `worktree ${worktree}`);
        // Recheck both lexical parent identity and leaf after the Git observation.
        const after = await NodeFSP.lstat(parentPath, { bigint: true });
        if (!after.isDirectory() || before.dev !== after.dev || before.ino !== after.ino)
          return { status: "unobservable", reason: "target_parent_changed" };
        try {
          await NodeFSP.lstat(leaf);
          return { status: "replaced", reason: "target_reappeared" };
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
        }
        return { status: registered ? "absent_registered" : "absent_unregistered" };
      } finally {
        await project.close();
      }
    } finally {
      await parent.close();
    }
  } catch (cause) {
    const code = (cause as NodeJS.ErrnoException).code;
    return {
      status: "unobservable",
      reason: ["EACCES", "EPERM"].includes(code ?? "")
        ? "target_parent_unreadable"
        : "target_presence_unavailable",
    };
  }
}

export class TargetObservation extends Context.Reference<typeof observeTarget>(
  "jones/workspaceRetirement/TargetObservation",
  { defaultValue: () => observeTarget },
) {}

export function presenceOutcome(
  worktree: string,
  presence: Exclude<TargetPresence, { status: "present" }>,
): WorkspaceRetirementOutcome {
  const absent = presence.status === "absent_unregistered";
  const reason = absent
    ? "worktree_already_absent"
    : presence.status === "absent_registered"
      ? "worktree_registration_orphaned"
      : presence.reason;
  return {
    state: absent ? "proposed" : "held",
    reasonCodes: [reason],
    receiptId: null,
    fingerprint: null,
    paths: [],
    outputObligations: [],
    details: [
      {
        reason,
        target: worktree,
        root: worktree,
        fact: absent
          ? "The checkout is absent and has no Git worktree registration. No removal or script completion is claimed."
          : "The checkout is registered, replaced, or cannot be safely observed.",
        missingFact: absent ? null : reason,
        lineage: [],
        nextAction: absent
          ? "No cleanup is needed. Keep the branch and path for future recreation."
          : "Reobserve the exact checkout through its owner. Do not prune registration or replay an old script.",
      },
    ],
  };
}
