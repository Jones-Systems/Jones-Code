// @effect-diagnostics nodeBuiltinImport:off -- Read-only Git/filesystem qualification adapter; exclusion and removal belong to J3.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { canonical, exists, git, inside, readBounded, sha256 } from "./Filesystem.ts";
import { binding, incarnation, type Binding } from "./OutputAttestation.ts";
import {
  classifyIgnored,
  verifyPreservation,
  type Coverage,
  type IgnoredResult,
  type Preservation,
} from "./IgnoredClassifier.ts";
import { consumerDecision, type ConsumerEvidence } from "./ConsumerProbe.ts";
import { outputObligations } from "./ProducerFinalizer.ts";
import { completionLineage } from "./CompletionObservation.ts";

export interface QualificationInput {
  worktree: string;
  expected: Binding;
  projectRoots: readonly string[];
  durableRepositories: readonly string[];
  temporaryRoots: readonly string[];
  nativeRecreation: "supported" | "unsupported" | "unknown";
  consumers: ConsumerEvidence;
  coverage?: Coverage;
  attestations?: Readonly<Record<string, string>>;
  preservations?: readonly Preservation[];
  registeredStores?: readonly { path: string; common: string }[];
  lineage?: Readonly<Record<string, string>>;
  dutyDetails?: readonly ReasonDetail[];
}
export interface ReasonDetail {
  reason: string;
  target: string;
  root: string;
  category:
    | "active_use"
    | "unique_work"
    | "missing_proof"
    | "unavailable_observation"
    | "protection";
  fact: string;
  missingFact: string | null;
  lineage: readonly Readonly<Record<string, string>>[];
  nextAction: string;
}
type ObservationStage =
  | "identity"
  | "registration"
  | "head"
  | "tracked_status"
  | "index_flags"
  | "operation_state"
  | "ref_containment"
  | "candidate_reflog"
  | "reflog_containment"
  | "object_dependencies"
  | "submodules"
  | "worktree_config"
  | "ignored_content"
  | "preservation"
  | "consumers"
  | "identity_recheck";
type ObservationErrorClass =
  | "timeout"
  | "bounds"
  | "malformed"
  | "permission_denied"
  | "not_found"
  | "native_read"
  | "native_command"
  | "unexpected";
class ObservationError extends Error {
  readonly errorClass: ObservationErrorClass;
  constructor(errorClass: ObservationErrorClass) {
    super(errorClass);
    this.errorClass = errorClass;
  }
}
function errorClass(cause: unknown): ObservationErrorClass {
  if (cause instanceof ObservationError) return cause.errorClass;
  const error = cause as NodeJS.ErrnoException & { killed?: boolean; signal?: string };
  if (error?.code === "ETIMEDOUT" || (error?.killed && error.signal === "SIGTERM"))
    return "timeout";
  if (
    error?.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER" ||
    (cause instanceof Error && cause.message === "bound_exceeded")
  )
    return "bounds";
  if (["EACCES", "EPERM"].includes(error?.code ?? "")) return "permission_denied";
  if (error?.code === "ENOENT") return "not_found";
  if (typeof error?.code === "number") return "native_command";
  if (typeof error?.code === "string" && /^E[A-Z]+$/.test(error.code)) return "native_read";
  return "unexpected";
}

// Read only the carrier that Git removes with this checkout. Common branch and
// other-checkout reflogs survive removal. Keep both ends of every record: an old
// tip may have no later entry after expiry or a rewritten reflog.
async function candidateReflog(admin: string): Promise<string[]> {
  let bytes: Buffer;
  try {
    bytes = await readBounded(NodePath.join(admin, "logs/HEAD"), 1024 * 1024);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  }
  const tips = new Set<string>();
  for (const line of bytes.toString().split("\n")) {
    if (!line) continue;
    const record =
      /^(?<old>[a-f0-9]{40}|[a-f0-9]{64}) (?<next>[a-f0-9]{40}|[a-f0-9]{64}) [^\0\n\t]+ -?\d+ [+-]\d{4}(?:\t[^\0\n]*)?$/.exec(
        line,
      )?.groups;
    if (!record || record.old!.length !== record.next!.length)
      throw new ObservationError("malformed");
    for (const tip of [record.old!, record.next!]) if (!/^0+$/.test(tip)) tips.add(tip);
    if (tips.size > 1000) throw new ObservationError("bounds");
  }
  return [...tips].sort();
}

export interface QualificationResult {
  schema: "workspace-qualification/v1";
  authority_effect: "none";
  outcome: "retirable" | "hold" | "unknown";
  level: "contained" | "branch-retained" | null;
  reasons: string[];
  decision: "propose" | "hold" | "unknown";
  binding?: Binding;
  head?: string;
  branch?: string | null;
  containingRefs?: string[];
  preservationTips?: string[];
  untrackedPaths?: string[];
  ignored?: IgnoredResult[];
  consumers?: ReturnType<typeof consumerDecision>;
  capturedConfig?: Record<string, string>;
  sparsePatterns?: string;
  fingerprint?: string;
  observationMs: number;
  observationFailure?: { stage: ObservationStage; errorClass: ObservationErrorClass };
  details?: ReasonDetail[];
  lineageObservation?: Awaited<ReturnType<typeof completionLineage>>;
}
export async function qualify(input: QualificationInput): Promise<QualificationResult> {
  const started = performance.now(),
    reasons = new Set<string>(),
    unknown = new Set<string>();
  const result: QualificationResult = {
    schema: "workspace-qualification/v1",
    authority_effect: "none",
    outcome: "unknown",
    level: null,
    reasons: [],
    decision: "unknown",
    observationMs: 0,
  };
  let stage: ObservationStage = "identity";
  const g = async (args: string[]) => {
    const remaining = 60000 - (performance.now() - started);
    if (remaining <= 0) throw new ObservationError("timeout");
    return git(input.worktree, args, Math.min(10000, remaining));
  };
  const finish = () => {
    if (performance.now() - started > 60000) {
      unknown.add("bound_exceeded");
      unknown.add("observation_failed");
      result.observationFailure ??= { stage, errorClass: "timeout" };
    }
    result.reasons = [...new Set([...reasons, ...unknown])].sort();
    result.outcome = reasons.size ? "hold" : unknown.size ? "unknown" : "retirable";
    if (result.outcome !== "retirable") result.level = null;
    result.decision = result.outcome === "retirable" ? "propose" : result.outcome;
    const lineage = [
      ...(input.lineage ? [input.lineage] : []),
      ...(result.lineageObservation?.lineage ?? []),
    ];
    result.details = result.reasons.flatMap((reason) => {
      if (reason === "active_consumer" && input.dutyDetails?.length) return [...input.dutyDetails];
      if (reason === "consumer_evidence_unavailable") {
        return Object.entries(result.consumers?.coverage ?? {})
          .filter(([, probe]) => probe.reason !== null)
          .map(([name, probe]) => ({
            reason,
            target: input.worktree,
            root: input.worktree,
            category: "unavailable_observation" as const,
            fact: `${name}: ${probe.reason}. ${probe.limits.join("; ")}`,
            missingFact: `fresh_complete_${name}_observation`,
            lineage,
            nextAction: `Obtain only this target's ${name} readback from its named native owner; ${probe.limits.join("; ")}`,
          }));
      }
      const roots =
        result.ignored?.filter((item) => item.reason === reason).map((item) => item.path) ?? [];
      return (roots.length ? roots : [input.worktree]).map((root) => {
        const missing = [
          "unattested_ignored",
          "attestation_unverifiable",
          "repository_durability_unproven",
          "native_evidence_unavailable",
        ].includes(reason);
        const unique = [
          "dirty_tracked",
          "untracked",
          "preservation_unverified",
          "attestation_mismatch",
        ].includes(reason);
        const active = ["active_consumer", "obligation", "setup_unfinalized"].includes(reason);
        const unavailable = unknown.has(reason);
        return {
          reason,
          target: input.worktree,
          root,
          category: active
            ? "active_use"
            : unique
              ? "unique_work"
              : unavailable
                ? "unavailable_observation"
                : missing
                  ? "missing_proof"
                  : "protection",
          fact:
            reason === "observation_failed" && result.observationFailure
              ? `Qualification observation failed at ${result.observationFailure.stage} (${result.observationFailure.errorClass}).`
              : reason === "unattested_ignored"
                ? "Ignored content has no verified producer baseline or applicable coverage."
                : reason === "attestation_mismatch"
                  ? "Current content differs from the existing producer baseline."
                  : reason,
          missingFact:
            reason === "observation_failed" && result.observationFailure
              ? `fresh_complete_${result.observationFailure.stage}_observation`
              : missing
                ? reason === "unattested_ignored"
                  ? "producer_content_baseline_or_exact_coverage_or_preservation"
                  : reason
                : unavailable
                  ? "fresh_complete_target_observation"
                  : null,
          lineage,
          nextAction: active
            ? "Resolve the named live consumer or actual producer obligation, then re-observe this target."
            : unique || missing
              ? "Preserve this exact root or use independently valid owner coverage; cleanup must not manufacture a producer baseline."
              : unavailable
                ? "Repeat only the unavailable target observation with its owning adapter."
                : "Resolve this named protection before evaluating retirement.",
        } as ReasonDetail;
      });
    });
    result.observationMs = performance.now() - started;
    result.fingerprint = sha256(
      canonical({
        binding: result.binding,
        head: result.head,
        branch: result.branch,
        containingRefs: result.containingRefs,
        preservationTips: result.preservationTips,
        untrackedPaths: result.untrackedPaths,
        ignored: result.ignored,
        capturedConfig: result.capturedConfig,
        sparsePatterns: result.sparsePatterns,
        reasons: result.reasons,
        level: result.level,
      }),
    );
    return result;
  };
  try {
    let current: Binding;
    try {
      current = await binding(input.worktree);
    } catch {
      reasons.add("identity_mismatch");
      return finish();
    }
    result.binding = current;
    if (canonical(current) !== canonical(input.expected)) {
      reasons.add("identity_mismatch");
      return finish();
    }
    result.lineageObservation = await completionLineage(current.git_dir, current);
    if (current.git_dir === current.git_common_dir) {
      reasons.add("primary_checkout");
      return finish();
    }
    stage = "registration";
    const registrations = (await g(["worktree", "list", "--porcelain", "-z"]))
      .toString()
      .split("\0\0")
      .filter(Boolean)
      .map((block) =>
        Object.fromEntries(
          block
            .split("\0")
            .filter(Boolean)
            .map((field) => {
              const index = field.indexOf(" ");
              return index < 0 ? [field, true] : [field.slice(0, index), field.slice(index + 1)];
            }),
        ),
      );
    const own = registrations.find((row) => row.worktree === current.path);
    if (!own) reasons.add("identity_mismatch");
    if (own?.locked) reasons.add("locked");
    if (input.projectRoots.some((root) => inside(NodePath.resolve(root), current.path)))
      reasons.add("project_root");
    stage = "head";
    result.head = (await g(["rev-parse", "HEAD"])).toString().trim();
    result.branch =
      (
        await g(["symbolic-ref", "-q", "HEAD"]).catch((cause: unknown) => {
          if ((cause as { code?: number }).code === 1) return Buffer.alloc(0);
          throw cause;
        })
      )
        .toString()
        .trim() || null;
    stage = "tracked_status";
    const status = (
      await g([
        "-c",
        "status.showUntrackedFiles=all",
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignore-submodules=none",
      ])
    )
      .toString()
      .split("\0");
    const untracked: string[] = [];
    for (let i = 0; i < status.length; i++) {
      const item = status[i]!;
      if (!item) continue;
      const code = item.slice(0, 2),
        path = item.slice(3);
      if (code === "??") untracked.push(path);
      else {
        reasons.add("dirty_tracked");
        if (code.includes("R") || code.includes("C")) i++;
      }
    }
    result.untrackedPaths = untracked;
    if (untracked.length > 10000) reasons.add("untracked_paths_bound");
    if (
      untracked.some(
        (path) =>
          input.coverage?.keepMode !== "tracked-changes" ||
          !input.coverage?.exactRoots?.includes(NodePath.join(current.path, path)),
      )
    )
      reasons.add("untracked");
    stage = "index_flags";
    const flags = (await g(["ls-files", "-v", "-z"]))
      .toString()
      .split("\0")
      .filter((line) => line && (line[0] === line[0]!.toLowerCase() || line[0] === "S"));
    if (flags.length > 2000) reasons.add("index_flags");
    else
      for (const entry of flags) {
        const path = entry.slice(2);
        if (!(await exists(NodePath.join(current.path, path)))) {
          if (entry[0] !== "S") reasons.add("index_flags");
          continue;
        }
        const object = (await g(["ls-files", "-s", "--", path]))
          .toString()
          .match(/^\d+ ([a-f0-9]+) 0\t/);
        if (!object) {
          reasons.add("index_flags");
          continue;
        }
        const content = await readBounded(NodePath.join(current.path, path), 16 * 1024 * 1024);
        if (!content.equals(await g(["cat-file", "blob", object[1]!]))) reasons.add("index_flags");
      }
    stage = "operation_state";
    for (const file of [
      "MERGE_HEAD",
      "CHERRY_PICK_HEAD",
      "REVERT_HEAD",
      "REBASE_HEAD",
      "rebase-apply",
      "rebase-merge",
      "sequencer",
      "BISECT_LOG",
    ])
      if (await exists(NodePath.join(current.git_dir, file))) reasons.add("operation_in_progress");
    if ((await g(["for-each-ref", "--format=%(refname)", "refs/worktree/", "refs/bisect/"])).length)
      reasons.add("per_worktree_refs");
    const anchored =
      input.durableRepositories.includes(current.git_common_dir) &&
      !inside(current.git_common_dir, current.path) &&
      !input.temporaryRoots.some((root) => inside(current.git_common_dir, root));
    stage = "ref_containment";
    const containing = (await g(["for-each-ref", "--contains", result.head, "--format=%(refname)"]))
      .toString()
      .trim()
      .split("\n")
      .filter(Boolean);
    // The executor creates these after final qualification. They preserve objects
    // but must not change this candidate's containment comparison during removal.
    const ownRecovery = `refs/retired-worktrees/${incarnation(current)}/`;
    const other = containing.filter(
      (ref) =>
        ref !== result.branch &&
        !ref.startsWith(ownRecovery) &&
        !ref.startsWith("refs/worktree/") &&
        !ref.startsWith("refs/bisect/"),
    );
    result.containingRefs = other;
    if (!anchored && !other.some((ref) => ref.startsWith("refs/remotes/"))) {
      // Unanchored object stores never establish durable restoration, even with a ref.
      reasons.add("repository_durability_unproven");
    }
    result.level = other.length ? "contained" : "branch-retained";
    stage = "candidate_reflog";
    const tips = await candidateReflog(current.git_dir);
    stage = "reflog_containment";
    const orphan: string[] = [];
    for (const tip of tips)
      if (!(await g(["for-each-ref", "--contains", tip, "--format=%(refname)"])).length) {
        orphan.push(tip);
        if (orphan.length > 50) break;
      }
    if (orphan.length > 50) reasons.add("orphan_commits_bound");
    result.preservationTips = [
      ...new Set([...orphan, ...(!result.branch && !other.length ? [result.head] : [])]),
    ];
    stage = "object_dependencies";
    const alternates = NodePath.join(current.git_common_dir, "objects/info/alternates");
    if (await exists(alternates))
      for (const alternate of (await readBounded(alternates, 65536))
        .toString()
        .split("\n")
        .filter(Boolean)) {
        const objectRoot = NodePath.resolve(current.git_common_dir, "objects", alternate);
        if (
          !input.durableRepositories.some((root) => inside(objectRoot, root)) ||
          input.temporaryRoots.some((root) => inside(objectRoot, root))
        )
          reasons.add("object_dependency");
      }
    stage = "submodules";
    const index = (await g(["ls-files", "--stage", "-z"])).toString().split("\0").filter(Boolean);
    if (index.length > 1000000) unknown.add("bound_exceeded");
    for (const line of index)
      if (line.startsWith("160000 ")) {
        const path = line.slice(line.indexOf("\t") + 1);
        if (
          (await exists(NodePath.join(current.path, path))) &&
          (await NodeFSP.readdir(NodePath.join(current.path, path))).length
        )
          reasons.add("populated_submodules");
      }
    for (const modules of [
      NodePath.join(current.git_dir, "modules"),
      NodePath.join(current.git_common_dir, "modules"),
    ])
      if ((await exists(modules)) && (await NodeFSP.readdir(modules)).length)
        reasons.add("submodule_gitdirs");
    stage = "worktree_config";
    result.capturedConfig = {};
    const config = NodePath.join(current.git_dir, "config.worktree");
    if (await exists(config)) {
      const pairs = (await g(["config", "--null", "--file", config, "--list"]))
        .toString()
        .split("\0")
        .filter(Boolean);
      for (const pair of pairs) {
        const [key, ...rest] = pair.split("\n");
        if (
          !["core.sparsecheckout", "core.sparsecheckoutcone", "index.sparse"].includes(
            key!.toLowerCase(),
          )
        )
          reasons.add("worktree_config_unpreservable");
        else result.capturedConfig[key!] = rest.join("\n");
      }
      const patterns = NodePath.join(current.git_dir, "info/sparse-checkout");
      if (await exists(patterns))
        result.sparsePatterns = (await readBounded(patterns, 1024 * 1024)).toString();
    }
    if (input.nativeRecreation === "unsupported") reasons.add("native_recreation_unsupported");
    if (input.nativeRecreation === "unknown") unknown.add("native_evidence_unavailable");
    stage = "ignored_content";
    const ignoredRaw = (
      await g(["ls-files", "--others", "--ignored", "--exclude-standard", "--directory", "-z"])
    )
      .toString()
      .split("\0")
      .filter(Boolean)
      .map((p) => NodePath.join(current.path, p.replace(/\/$/, "")));
    if (ignoredRaw.length > 1000000) throw new Error("bound_exceeded");
    result.ignored = [];
    for (const root of ignoredRaw) {
      const preserved = input.preservations?.find((p) => p.source === root);
      const classified = await classifyIgnored({
        worktree: current.path,
        root,
        common: current.git_common_dir,
        coverage: input.coverage,
        sidecar: input.attestations?.[root],
        preservation: preserved,
        targets: [current.path],
        temporaryRoots: input.temporaryRoots,
        registered: input.registeredStores,
      });
      result.ignored.push(classified);
      if (classified.reason)
        (classified.outcome === "unknown" ? unknown : reasons).add(classified.reason);
    }
    stage = "preservation";
    for (const record of input.preservations ?? [])
      if (
        !(await verifyPreservation(
          record,
          [current.path],
          current.git_common_dir,
          input.temporaryRoots,
        ))
      )
        reasons.add("preservation_unverified");
    stage = "consumers";
    const running = NodePath.join(current.path, ".t3/setup-running");
    if (await exists(running)) reasons.add("setup_unfinalized");
    if (await exists(NodePath.join(current.path, ".t3/setup-unfinalized")))
      reasons.add("setup_unfinalized");
    if ((await outputObligations(current.path)).length) reasons.add("obligation");
    result.consumers = consumerDecision(current.path, input.consumers);
    result.consumers.holds.forEach((reason) => reasons.add(reason));
    result.consumers.unknown.forEach((reason) => unknown.add(reason));
    stage = "identity_recheck";
    if (canonical(await binding(current.path)) !== canonical(current))
      reasons.add("identity_mismatch");
    return finish();
  } catch (cause) {
    result.observationFailure = { stage, errorClass: errorClass(cause) };
    unknown.add("observation_failed");
    unknown.add(
      cause instanceof Error && cause.message === "bound_exceeded"
        ? "bound_exceeded"
        : "observation_failed",
    );
    return finish();
  }
}

// J3 must invoke this while holding the reservation; this helper supplies no exclusion or deletion authority.
export async function finalCheck(input: QualificationInput, previous: QualificationResult) {
  const fresh = await qualify(input);
  const fact = (r: QualificationResult) =>
    canonical({
      binding: r.binding,
      head: r.head,
      branch: r.branch,
      containingRefs: r.containingRefs,
      preservationTips: r.preservationTips,
      untrackedPaths: r.untrackedPaths,
      ignored: r.ignored,
      capturedConfig: r.capturedConfig,
      sparsePatterns: r.sparsePatterns,
    });
  return {
    result: fresh,
    conflict: fresh.outcome !== "retirable" || fact(fresh) !== fact(previous),
  };
}
