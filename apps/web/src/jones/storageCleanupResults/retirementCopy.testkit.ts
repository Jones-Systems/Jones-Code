// @effect-diagnostics nodeBuiltinImport:off -- Pure retirement copy cases also run without installed dependencies.
import * as NodeAssert from "node:assert/strict";
import type { WorkspaceRetirementOutcome } from "@t3tools/contracts";
import {
  exclusionLabel,
  proposalCopy,
  readbackLabel,
  recoverabilityAllowsRetire,
  retireConfirmation,
  retirementReasonLabel,
  shortSha,
  verificationLabel,
} from "./retirementCopy.ts";

type Recoverability = NonNullable<WorkspaceRetirementOutcome["recoverability"]>;

const NEW_REASON_CODES = [
  "preserve_commit_failed",
  "preserve_index_isolation_unverified",
  "preserve_branch_unavailable",
  "preserve_remote_unavailable",
  "preserve_store_not_durable",
  "preserve_exclusions_changed",
  "preserve_selection_changed",
  "preserve_tree_mismatch",
  "preserve_readback_failed",
  "preserve_remote_readback_failed",
  "preserve_path_unrepresentable",
  "interrupted_preserved_path_missing",
] as const;

const preservable = (
  plan: Partial<NonNullable<Recoverability["preserve"]>> = {},
): Recoverability => ({
  verdict: "retirable_after_preserve",
  regenerable: [],
  preserve: {
    files: 12,
    deletions: 0,
    bytes: 3 * 1024 ** 2,
    excluded: [],
    excludedBytes: 0,
    ...plan,
  },
});

export const retirementCopyCases = {
  "every preserve reason code has its own human label, never the raw code": () => {
    const labels = NEW_REASON_CODES.map((code) => retirementReasonLabel(code));
    for (const [index, label] of labels.entries()) {
      NodeAssert.ok(label, NEW_REASON_CODES[index]);
      NodeAssert.doesNotMatch(label, /preserve_|interrupted_/);
    }
    NodeAssert.equal(new Set(labels).size, NEW_REASON_CODES.length);
  },
  "reason codes this module does not own have no label": () => {
    NodeAssert.equal(retirementReasonLabel("live_consumer"), null);
    NodeAssert.equal(retirementReasonLabel(""), null);
  },
  "exclusion reasons read as sentences, with the secret pattern named but never its text": () => {
    NodeAssert.match(exclusionLabel("sensitive_name"), /secret or credential/);
    NodeAssert.match(exclusionLabel("oversized"), /Too large/);
    NodeAssert.match(exclusionLabel("nested_repository"), /own Git repository/);
    NodeAssert.equal(
      exclusionLabel("secret_pattern:aws_access_key"),
      "Content matches a secret pattern (aws_access_key)",
    );
    NodeAssert.equal(exclusionLabel("future_reason"), "future_reason");
  },
  "a preservable proposal with a remote states the file count, size, branch, remote and forced removal":
    () => {
      const copy = proposalCopy(preservable({ remote: "origin" }));
      NodeAssert.equal(copy?.kind, "preserve");
      const text = copy!.lines.join("\n");
      NodeAssert.match(text, /12 files \(3\.00 MB\) to a new preserve\/… branch/);
      NodeAssert.match(text, /pushed to the remote "origin" and read back/);
      NodeAssert.match(text, /--force only after the saved content is verified to match/);
      NodeAssert.match(text, /never deleted automatically/);
    },
  "a preservable proposal without a remote says the branch stays local-only": () => {
    const copy = proposalCopy(preservable({ remote: null, files: 1, bytes: 512 }));
    const text = copy!.lines.join("\n");
    NodeAssert.match(text, /1 file \(512 B\)/);
    NodeAssert.match(text, /no remote, so the branch stays local-only/);
    NodeAssert.match(text, /--force only after the saved content is verified/);
  },
  "a preservable proposal whose remote is unresolved does not name one": () => {
    const text = proposalCopy(preservable())!.lines.join("\n");
    NodeAssert.match(text, /otherwise it stays local-only/);
    NodeAssert.doesNotMatch(text, /remote "/);
  },
  "deleted tracked files are stated as recorded by the branch": () => {
    const text = proposalCopy(preservable({ remote: "origin", deletions: 2 }))!.lines.join("\n");
    NodeAssert.match(text, /records 2 deleted files/);
  },
  "excluded paths are listed with human reasons and hold the target": () => {
    const copy = proposalCopy({
      verdict: "held",
      regenerable: [],
      preserve: {
        files: 3,
        deletions: 0,
        bytes: 10,
        excluded: [
          { path: ".env.local", reason: "sensitive_name" },
          { path: "big.bin", reason: "oversized" },
        ],
        excludedBytes: 100,
      },
    });
    NodeAssert.equal(copy?.kind, "excluded");
    NodeAssert.match(copy!.lines.join(" "), /Held: 2 files cannot be saved to a branch/);
    NodeAssert.deepEqual(copy && "excluded" in copy ? copy.excluded.map((item) => item.path) : [], [
      ".env.local",
      "big.bin",
    ]);
  },
  "a recreatable target is safe to retire with nothing lost": () => {
    const copy = proposalCopy({
      verdict: "recreatable",
      regenerable: ["/w/node_modules", "/w/dist"],
    });
    NodeAssert.equal(copy?.kind, "recreatable");
    const text = copy!.lines.join("\n");
    NodeAssert.match(text, /Safe to retire/);
    NodeAssert.match(text, /Nothing is lost/);
    NodeAssert.match(text, /2 folders/);
  },
  "a held or unknown verdict without excluded paths states nothing": () => {
    NodeAssert.equal(proposalCopy(undefined), null);
    NodeAssert.equal(proposalCopy({ verdict: "held", regenerable: [] }), null);
    NodeAssert.equal(proposalCopy({ verdict: "unknown", regenerable: [] }), null);
  },
  "the recoverability verdict makes preservable and recreatable targets actionable, never excluded ones":
    () => {
      NodeAssert.equal(recoverabilityAllowsRetire(undefined), null);
      NodeAssert.equal(recoverabilityAllowsRetire(preservable()), true);
      NodeAssert.equal(
        recoverabilityAllowsRetire({ verdict: "recreatable", regenerable: [] }),
        true,
      );
      NodeAssert.equal(recoverabilityAllowsRetire({ verdict: "held", regenerable: [] }), null);
      NodeAssert.equal(recoverabilityAllowsRetire({ verdict: "unknown", regenerable: [] }), null);
      NodeAssert.equal(
        recoverabilityAllowsRetire(
          preservable({ excluded: [{ path: ".env", reason: "sensitive_name" }] }),
        ),
        false,
      );
    },
  "receipt labels name the verification, the short sha and the readback": () => {
    NodeAssert.equal(verificationLabel("remote_verified"), "Remote-verified");
    NodeAssert.equal(verificationLabel("local_only"), "Local-only");
    NodeAssert.equal(shortSha("0123456789abcdef0123"), "0123456789");
    NodeAssert.equal(readbackLabel(undefined), "Not read back yet");
    NodeAssert.equal(
      readbackLabel({ local: "verified", remote: "verified" }),
      "Local branch verified · remote verified",
    );
    NodeAssert.equal(
      readbackLabel({ local: "verified", remote: "not_applicable" }),
      "Local branch verified · remote not applicable (not pushed)",
    );
    NodeAssert.match(readbackLabel({ local: "failed", remote: "failed" }), /FAILED.*FAILED/);
  },
  "the confirmation states the preserve plan before the click and keeps the original text otherwise":
    () => {
      const preserve = retireConfirmation({
        path: "/w/feature",
        coveredPaths: ["/w/feature/dist"],
        recoverability: preservable({ remote: "origin" }),
      });
      NodeAssert.match(preserve, /^Retire \/w\/feature\?\nRetire saves 12 files \(3\.00 MB\)/);
      NodeAssert.match(preserve, /remote "origin"/);
      NodeAssert.match(preserve, /--force only after the saved content is verified/);
      NodeAssert.match(preserve, /\/w\/feature\/dist/);
      const plain = retireConfirmation({
        path: "/w/old",
        coveredPaths: [],
        recoverability: undefined,
      });
      NodeAssert.equal(
        plain,
        `Retire /w/old? Branches and recovery references are kept. This authorizes disposal only of these exact selections, including additions within the listed roots:
No local content selected.
Live consumers, tracked edits, nested repositories and other protections still hold.`,
      );
    },
};
