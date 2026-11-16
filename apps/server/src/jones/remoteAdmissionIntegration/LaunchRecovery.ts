// @effect-diagnostics nodeBuiltinImport:off -- Synchronous receiver identity hashes and opaque IDs preserve retained command/fence bytes.
import * as Crypto from "node:crypto";
import type {
  RemoteAdmissionLaunchResult,
  RemoteAdmissionRecoveryEvidence,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import { canonicalJsonStrict } from "../remoteDelegation/shared.ts";

export const recoveryIdentities = (sessionId: string, clientRequestId: string) => {
  return recoveryNamespaceIdentities(`client:${sessionId}`, clientRequestId);
};
export const recoveryNamespaceIdentities = (namespace: string, clientRequestId: string) => {
  const hashed = `mcp:launch:${Crypto.createHash("sha256")
    .update(JSON.stringify([namespace, clientRequestId]))
    .digest("hex")}`;
  const legacy = `mcp:launch:${encodeURIComponent(namespace)}:${encodeURIComponent(clientRequestId)}`;
  return { hashed, legacy };
};
export interface RecoveryReceipt {
  readonly commandId: string;
  readonly threadId: string;
  readonly commandType: string;
  readonly status: "accepted" | "rejected";
}
export interface RecoveryPath {
  readonly commandId: string;
  readonly creation: RecoveryReceipt | null;
  /** Exact immutable thread.created project, never the mutable shell's assertion. */
  readonly creationProjectId: string | null;
  readonly message: RecoveryReceipt | null;
  readonly shell: {
    readonly id: string;
    readonly projectId: string;
    readonly deletedAt: string | null;
  } | null;
  /** Reader verifies durable linked launch claim against this admission, never sender JSON. */
  readonly claimMatches: boolean;
}
export interface LaunchEvidenceInput {
  readonly keyed: boolean;
  readonly abandonedClaim: boolean;
  readonly readsComplete: boolean;
  readonly liveClaim: boolean;
  readonly expectedProjectId: string | null;
  readonly initialMessageRequested: boolean;
  readonly observedAt: string;
  readonly hashed: RecoveryPath;
  readonly legacy: RecoveryPath;
}
export const evidenceDigest = (value: unknown) =>
  Crypto.createHash("sha256").update(canonicalJsonStrict(value)).digest("hex");

/** Pure classification: call only after prior immutable result and local-live-claim precedence. */
export function classifyLaunchEvidence(input: LaunchEvidenceInput): {
  readonly result: RemoteAdmissionLaunchResult;
  readonly evidence: RemoteAdmissionRecoveryEvidence;
} {
  const contradictions: Array<RemoteAdmissionRecoveryEvidence["contradictions"][number]> = [];
  const paths = [input.hashed, input.legacy];
  const present = (p: RecoveryPath) =>
    p.creation !== null || p.message !== null || p.shell !== null;
  if (paths.every(present)) contradictions.push("both_identities");
  for (const p of paths) {
    if (
      (p.creation &&
        (p.creation.commandId !== p.commandId ||
          p.creation.threadId !== p.commandId ||
          p.creation.commandType !== "thread.create" ||
          !p.claimMatches)) ||
      (p.message &&
        (p.message.commandId !== `${p.commandId}:initial-message` ||
          p.message.threadId !== p.commandId ||
          p.message.commandType !== "message.dispatch")) ||
      (p.shell && p.shell.id !== p.commandId)
    )
      contradictions.push("invalid_receipt");
    if (p.shell && p.creation?.status !== "accepted") contradictions.push("shell_without_creation");
    const projectId = input.expectedProjectId ?? p.creationProjectId;
    if (
      (p.creation?.status === "accepted" &&
        (typeof p.creationProjectId !== "string" ||
          p.creationProjectId.length === 0 ||
          p.creationProjectId !== projectId)) ||
      (p.shell && p.shell.projectId !== projectId)
    )
      contradictions.push("project_mismatch");
    if (p.message && p.creation?.status !== "accepted")
      contradictions.push("message_without_creation");
    if (p.creation?.status === "rejected" && (p.shell || p.message))
      contradictions.push("rejected_creation_with_effect");
  }
  const selected = paths.find(present);
  const evidence: RemoteAdmissionRecoveryEvidence = {
    evidenceDigest: evidenceDigest(input),
    observedAt: input.observedAt,
    identityPath: contradictions.length
      ? "contradictory"
      : selected === input.hashed
        ? "hashed"
        : selected
          ? "legacy"
          : "neither",
    creationCommandId: selected?.commandId ?? null,
    initialMessageCommandId: selected ? `${selected.commandId}:initial-message` : null,
    abandonedClaim: input.abandonedClaim,
    readsComplete: input.readsComplete,
    contradictions: [...new Set(contradictions)],
  };
  let outcome: RemoteAdmissionLaunchResult["outcome"] = "unknown";
  let reasonCode = "evidence_incomplete";
  let availability: RemoteAdmissionLaunchResult["stages"]["availability"] = "unknown";
  if (input.liveClaim) reasonCode = "claim_live";
  else if (!input.readsComplete) reasonCode = "read_deferred";
  else if (!input.keyed) reasonCode = "unkeyed";
  else if (contradictions.length) reasonCode = "contradictory_evidence";
  else if (selected?.creation?.status === "accepted") {
    outcome = "created";
    reasonCode = "creation_receipt";
    availability = selected.shell
      ? selected.shell.deletedAt === null
        ? "available"
        : "deleted"
      : "unknown";
  } else if (selected?.creation?.status === "rejected") {
    outcome = "refused";
    reasonCode = "creation_rejected";
  } else if (!selected && input.abandonedClaim) {
    outcome = "not_created";
    reasonCode = "abandoned_claim_no_thread";
  }
  return {
    evidence,
    result: {
      outcome,
      reasonCode,
      recordedAt: input.observedAt,
      threadId: outcome === "created" ? selected!.commandId : null,
      stages: {
        creation:
          outcome === "created"
            ? "confirmed"
            : outcome === "not_created"
              ? "not_created"
              : outcome === "refused"
                ? "refused"
                : "unknown",
        availability,
        initialMessage: !input.initialMessageRequested
          ? "none"
          : outcome === "created"
            ? selected?.message?.status === "accepted"
              ? "accepted"
              : selected?.message?.status === "rejected"
                ? "refused"
                : "unconfirmed"
            : "unconfirmed",
        preparation: "unknown",
        providerProgress: "unknown",
      },
    },
  };
}
