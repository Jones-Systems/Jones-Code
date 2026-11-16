// @effect-diagnostics nodeBuiltinImport:off -- Synthetic Git fixtures own and remove their exact root.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { fixture, command } from "./Conformance.testkit.ts";
import { dutyIdentity } from "./DutyIdentity.ts";
import { observeTarget, presenceOutcome } from "./TargetPresence.ts";
import { legacySettlementAdmission } from "./SettlementAdmission.ts";

export const presenceCases = {
  "missing leaf needs a successful listing and no registration; observation preserves branch": () =>
    fixture(async (f) => {
      const prior = await dutyIdentity(f.worktree);
      NodeAssert.equal((await observeTarget(f.worktree, f.repo, prior)).status, "present");
      await NodeFSP.rm(f.worktree, { recursive: true });
      NodeAssert.equal(
        (await observeTarget(f.worktree, f.repo, prior)).status,
        "absent_registered",
      );
      // Fixture-only normal Git removal of its exact synthetic registration, never production prune.
      await command(f.repo, ["worktree", "remove", f.worktree]);
      for (let pass = 0; pass < 2; pass++) {
        const presence = await observeTarget(f.worktree, f.repo, prior);
        NodeAssert.equal(presence.status, "absent_unregistered");
        const outcome = presenceOutcome(f.worktree, presence);
        NodeAssert.deepEqual(outcome.reasonCodes, ["worktree_already_absent"]);
        NodeAssert.equal(outcome.receiptId, null);
        NodeAssert.equal(outcome.fingerprint, null);
      }
      NodeAssert.equal(await command(f.repo, ["branch", "--list", "task"]), "task");
      const other = NodePath.join(f.base, "other-repository");
      await NodeFSP.mkdir(other);
      await command(other, ["init", "-b", "main"]);
      NodeAssert.deepEqual(await observeTarget(f.worktree, other, prior), {
        status: "unobservable",
        reason: "project_repository_changed",
      });
      NodeAssert.equal(
        (await observeTarget(f.worktree, NodePath.join(f.base, "missing-project"))).status,
        "unobservable",
      );
    }),
  "symlink, file, replaced inode and non-directory parent are distinct from absence": () =>
    fixture(async (f) => {
      const prior = await dutyIdentity(f.worktree);
      await NodeFSP.rename(f.worktree, NodePath.join(f.base, "original"));
      await NodeFSP.mkdir(f.worktree);
      NodeAssert.deepEqual(await observeTarget(f.worktree, f.repo, prior), {
        status: "replaced",
        reason: "target_incarnation_changed",
      });
      await NodeFSP.rmdir(f.worktree);
      await NodeFSP.symlink(NodePath.join(f.base, "original"), f.worktree);
      NodeAssert.deepEqual(await observeTarget(f.worktree, f.repo), {
        status: "replaced",
        reason: "target_symlink",
      });
      await NodeFSP.unlink(f.worktree);
      await NodeFSP.writeFile(f.worktree, "file");
      NodeAssert.deepEqual(await observeTarget(f.worktree, f.repo), {
        status: "replaced",
        reason: "target_not_directory",
      });
      NodeAssert.equal(
        (await observeTarget(NodePath.join(f.worktree, "leaf"), f.repo)).status,
        "unobservable",
      );
      await NodeFSP.unlink(f.worktree);
      await NodeFSP.mkdir(f.worktree, { mode: 0o700 });
      await NodeFSP.chmod(f.worktree, 0);
      try {
        NodeAssert.equal(
          (await observeTarget(NodePath.join(f.worktree, "leaf"), f.repo)).status,
          "unobservable",
        );
      } finally {
        await NodeFSP.chmod(f.worktree, 0o700);
      }
    }),
  "legacy metadata ages into qualification only; automatic removal still requires owner confirmation":
    async () => {
      const input = { settledAt: 1000, now: 601000, execute: false, ownerConfirmed: false };
      NodeAssert.equal(legacySettlementAdmission(input), null);
      NodeAssert.equal(
        legacySettlementAdmission({ ...input, now: 600999 }),
        "settlement_intent_missing",
      );
      NodeAssert.equal(
        legacySettlementAdmission({ ...input, execute: true }),
        "legacy_settlement_confirmation_required",
      );
      NodeAssert.equal(
        legacySettlementAdmission({ ...input, execute: true, ownerConfirmed: true }),
        null,
      );
    },
};
