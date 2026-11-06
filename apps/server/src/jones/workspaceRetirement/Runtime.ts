// @effect-diagnostics nodeBuiltinImport:off -- Read-only target observations; all effects flow through the common executor.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as EffectPath from "effect/Path";
import * as ProcessRunner from "../../processRunner.ts";
import * as Provenance from "../workspaceLifecycle/Provenance.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import {
  ThreadId,
  type WorkspaceRetirementOutcome,
  type WorktreeCleanupRules,
} from "@t3tools/contracts";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { Configuration } from "../workspaceLifecycle/Configuration.ts";
import * as Schema from "effect/Schema";
import { exists, git, inside, readBounded } from "./Filesystem.ts";
import { binding } from "./OutputAttestation.ts";
import { Receipts, layer, RetirementError, type RetirementReceipt } from "./Receipts.ts";
import { qualify, type QualificationInput } from "./Qualification.ts";
import type { Probe } from "./ConsumerProbe.ts";
import { NativeObservations } from "./NativeObservations.ts";
import * as GitDriver from "../../vcs/GitVcsDriver.ts";
import { settlementAdmission } from "./SettlementAdmission.ts";
import { retire } from "./RetirementExecutor.ts";

const unavailable = (limit: string): Probe => ({
  observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
  status: "unavailable",
  paths: [],
  limits: [limit],
});
const observed = (paths: readonly string[]): Probe => ({
  observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
  status: "complete",
  paths,
  limits: [],
});
const io = <A>(target: string, action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) => new RetirementError({ target, reason: "observation_failed", cause }),
  });
export interface RuntimeRequest {
  worktree: string;
  projectRoot: string;
  threadId: string | null;
  rules: WorktreeCleanupRules;
  cause: RetirementReceipt["cause"];
  execute: boolean;
  ownerCoverage?: { fingerprint: string; paths: readonly string[] };
  admit: Effect.Effect<void, RetirementError>;
  revalidate: Effect.Effect<void, RetirementError>;
}
export const evaluate = Effect.fn("WorkspaceRetirement.evaluate")(function* (
  request: RuntimeRequest,
) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Receipts.pipe(Effect.provide(layer));
  const projections = yield* Projections.ProjectionStoreV2;
  const config = yield* Config.ServerConfig;
  const nativeObservations = yield* NativeObservations;
  const gitDriver = yield* Effect.serviceOption(GitDriver.GitVcsDriver);
  const selected =
    config.workspaceLifecycle === undefined
      ? null
      : yield* Schema.decodeUnknownEffect(Configuration)(config.workspaceLifecycle).pipe(
          Effect.mapError(
            (cause) =>
              new RetirementError({
                target: request.worktree,
                reason: "configuration_unavailable",
                cause,
              }),
          ),
        );
  const runner = yield* Effect.serviceOption(ProcessRunner.ProcessRunner);
  const effectPath = yield* Effect.serviceOption(EffectPath.Path);
  const suppliedRegistry = yield* Effect.serviceOption(Registry.LifecycleRegistry);
  const registry = Option.isSome(suppliedRegistry)
    ? suppliedRegistry.value
    : selected === null || Option.isNone(runner) || Option.isNone(effectPath)
      ? null
      : yield* Registry.LifecycleRegistry.pipe(
          Effect.provide(Registry.layer(selected.registry, selected.outputs)),
          Effect.provideService(ProcessRunner.ProcessRunner, runner.value),
          Effect.provideService(EffectPath.Path, effectPath.value),
        );
  const bound = yield* io(request.worktree, () => binding(request.worktree));
  const projectCommon = yield* io(request.worktree, async () =>
    NodeFSP.realpath(
      (await git(request.projectRoot, ["rev-parse", "--path-format=absolute", "--git-common-dir"]))
        .toString()
        .trim(),
    ),
  );
  if (projectCommon !== bound.git_common_dir)
    return yield* new RetirementError({
      target: request.worktree,
      reason: "repository_identity_changed",
    });
  let firstGeneration: string | null | undefined;
  let outputs: readonly Registry.OutputRecord[] = [];
  const mapError = Effect.mapError((cause: unknown) =>
    cause instanceof RetirementError
      ? cause
      : new RetirementError({ target: request.worktree, reason: "observation_failed", cause }),
  );
  const scriptGate = Effect.gen(function* () {
    const pending = yield* sql<{
      script_state: string;
      script_started_at: string | null;
      settled_at: string;
    }>`SELECT i.script_state,i.script_started_at,i.settled_at FROM jones_workspace_retirement_intents i WHERE i.worktree=${request.worktree} AND i.rowid IN (SELECT MAX(rowid) FROM jones_workspace_retirement_intents WHERE worktree=${request.worktree} GROUP BY thread_id) AND i.script_state!='completed'`;
    if (pending.length) {
      const timedOut = pending.some(
        (i) =>
          settlementAdmission({
            script: i.script_state as "pending" | "running" | "failed" | "cancelled",
            startedAt: Date.parse(i.script_started_at ?? i.settled_at),
            now: DateTime.toEpochMillis(DateTime.nowUnsafe()),
            checkpoint: "pending",
            enabled: true,
          }).reason === "settle_script_unfinished",
      );
      return yield* new RetirementError({
        target: request.worktree,
        reason: timedOut ? "settle_script_unfinished" : "settle_script_pending",
      });
    }
    const threads = yield* sql<{
      thread_id: string;
    }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.worktreePath')=${request.worktree} LIMIT 201`;
    if (threads.length > 200)
      return yield* new RetirementError({
        target: request.worktree,
        reason: "consumer_bound_exceeded",
      });
    for (const row of threads) {
      const thread = yield* projections.getThread(ThreadId.make(row.thread_id));
      if (thread.worktreePath !== request.worktree)
        return yield* new RetirementError({ target: request.worktree, reason: "binding_changed" });
      const checkpoints = yield* projections.getCheckpointContext(thread.id);
      const last = [...checkpoints.runs].sort((a, b) => b.ordinal - a.ordinal)[0];
      const ready =
        last === undefined ||
        (["completed", "cancelled", "interrupted"].includes(last.status) &&
          checkpoints.checkpoints.some((c) => c.runId === last.id && c.status === "ready"));
      if (!ready)
        return yield* new RetirementError({
          target: request.worktree,
          reason: "final_checkpoint_unfinished",
        });
      if (thread.settledAt === null) continue;
      const intent = yield* store.settlement(
        row.thread_id,
        request.worktree,
        DateTime.formatIso(thread.settledAt),
      );
      if (intent === null)
        return yield* new RetirementError({
          target: request.worktree,
          reason: "settlement_intent_missing",
        });
      const admission = settlementAdmission({
        script: intent.script_state,
        startedAt: Date.parse(intent.script_started_at ?? intent.settled_at),
        now: DateTime.toEpochMillis(DateTime.nowUnsafe()),
        checkpoint: "ready",
        enabled: request.rules.worktreeSettledAfterDays !== null,
      });
      if (admission.decision === "hold")
        return yield* new RetirementError({ target: request.worktree, reason: admission.reason });
    }
  }).pipe(mapError);
  let ownerPaths: readonly string[] = [];
  const observe = Effect.gen(function* () {
    const claims = yield* store.claims(request.worktree);
    const dutyDetails = claims.map((claim) => {
      let missingFact: string | null = null;
      try {
        const physical = claim.identity_json === null ? null : JSON.parse(claim.identity_json);
        const owner = claim.native_owner_json === null ? null : JSON.parse(claim.native_owner_json);
        missingFact =
          physical?.status !== "bound"
            ? (physical?.reason ?? "native_worktree_identity_unavailable")
            : owner?.type === "unresolved"
              ? owner.reason
              : owner?.type === "in-process"
                ? "native_child_or_scope_drain_not_recorded"
                : owner?.type === "native-session"
                  ? "native_session_drain_not_recorded"
                  : "native_owner_observation_unavailable";
      } catch {
        missingFact = "durable_duty_identity_unverifiable";
      }
      return {
        reason: "active_consumer",
        target: request.worktree,
        root: request.worktree,
        category: "active_use" as const,
        fact: `${claim.kind} duty ${claim.claim_id} remains owned by ${claim.owner}${missingFact ? `; ${missingFact}` : ""}.`,
        missingFact,
        lineage: [
          {
            ...(request.threadId ? { threadId: request.threadId } : {}),
            producer: claim.kind,
            claimId: claim.claim_id,
            owner: claim.owner,
            incarnation: claim.incarnation_id ?? "unbound_native_identity",
            ...(claim.kind === "turn"
              ? { runId: claim.owner }
              : claim.kind === "queued-turn"
                ? { runId: claim.claim_id.slice("queued-turn:".length) }
                : {}),
          },
        ],
        nextAction: `Observe only ${claim.owner}'s actual ${claim.kind} holder and target incarnation, or use its existing completion/drain acknowledgement; then reevaluate this checkout.`,
      };
    });
    const roots = yield* sql<{
      workspace_root: string;
    }>`SELECT workspace_root FROM projection_projects WHERE deleted_at IS NULL`;
    const native = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.worktreePath')=${request.worktree} AND deleted_at IS NULL`;
    const active = native.some((r) => {
      const t = JSON.parse(r.payload_json);
      return (
        t.status === "running" ||
        t.status === "waiting" ||
        (t.latestRunId !== null && t.latestRunCompletedAt === null)
      );
    });
    const services = yield* io(request.worktree, () =>
      nativeObservations.services(request.worktree),
    );
    const workloads = yield* nativeObservations.workloads(sql, request.worktree);
    let registryProbe = unavailable("build-output registry is not configured"),
      obligations = unavailable("lifecycle registry is not configured");
    if (registry) {
      const current = yield* registry.show(request.worktree);
      if (firstGeneration === undefined) firstGeneration = current.generation;
      else if (firstGeneration !== current.generation)
        return yield* new RetirementError({
          target: request.worktree,
          reason: "lifecycle_generation_changed",
        });
      const assessment = current.assessment?.assessment;
      obligations = observed(
        assessment &&
          (assessment.remaining_work.length ||
            assessment.unresolved_obligations.length ||
            ["active", "preserve", "needs_handoff"].includes(assessment.disposition))
          ? [request.worktree]
          : [],
      );
      if (registry.outputs) {
        outputs = yield* registry.outputs(request.worktree);
        const own = outputs.filter(
          (r) =>
            r.worktree?.dev === bound.dev &&
            r.worktree?.ino === bound.ino &&
            r.worktree?.git_dir === bound.git_dir,
        );
        if (own.length !== outputs.length)
          return yield* new RetirementError({
            target: request.worktree,
            reason: "output_incarnation_mismatch",
          });
        registryProbe = observed(
          own
            .filter(
              (r) =>
                inside(r.path, request.worktree) &&
                (r.state !== "active" || r.class === "evidence"),
            )
            .map((r) => r.path),
        );
      }
    }
    const untracked = (yield* io(request.worktree, () =>
      git(request.worktree, ["ls-files", "--others", "--exclude-standard", "-z"]),
    ))
      .toString()
      .split("\0")
      .filter(Boolean);
    const ignored = (yield* io(request.worktree, () =>
      git(request.worktree, [
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "--directory",
        "-z",
      ]),
    ))
      .toString()
      .split("\0")
      .filter(Boolean);
    const exactRoots = [
      ...new Set([
        ...ownerPaths,
        ...(request.rules.worktreeKeepWhen === "tracked-changes" && untracked.length <= 10000
          ? [...untracked, ...ignored].map((p) =>
              NodePath.resolve(request.worktree, p.replace(/\/$/, "")),
            )
          : []),
      ]),
    ];
    const attestations: Record<string, string> = {};
    yield* io(request.worktree, async () => {
      const directory = NodePath.join(bound.git_dir, "workspace-outputs");
      if (!(await exists(directory))) return;
      const names = await NodeFSP.readdir(directory);
      if (names.length > 10000) throw new Error("bound_exceeded");
      for (const name of names.filter((n) => n.endsWith(".json")).sort()) {
        const path = NodePath.join(directory, name),
          sidecar = JSON.parse((await readBounded(path, 65536)).toString());
        if (sidecar.schema === "output-attestation/v1")
          for (const root of sidecar.roots ?? [])
            if (typeof root.path === "string") attestations[root.path] ??= path;
      }
    });
    const nativeGuard = yield* sql<{
      worktree_path: string;
    }>`SELECT worktree_path FROM native_creation_intents WHERE worktree_path=${request.worktree} UNION SELECT worktree_path FROM jones_native_workspace_admissions WHERE worktree_path=${request.worktree}`;
    return {
      worktree: request.worktree,
      expected: bound,
      projectRoots: roots.map((r) => r.workspace_root),
      durableRepositories: [projectCommon],
      temporaryRoots: nativeObservations.temporaryRoots,
      nativeRecreation: nativeGuard.length ? "unsupported" : "supported",
      coverage: {
        keepMode: request.rules.worktreeKeepWhen,
        classes: request.rules.worktreeCoveredDiscard,
        exactRoots,
        sensitivePaths: ownerPaths,
      },
      attestations,
      lineage: request.threadId ? { threadId: request.threadId } : {},
      dutyDetails,
      consumers: {
        native: observed(claims.length || active ? [request.worktree] : []),
        services,
        workloads,
        registry: registryProbe,
        obligations,
        process: yield* io(request.worktree, () => nativeObservations.processes(request.worktree)),
      },
    } satisfies QualificationInput;
  }).pipe(mapError);
  // Automatic assessment may only replace an obligation-free current-incarnation record, and always precedes acquisition.
  const assess = Effect.gen(function* () {
    if (!registry?.assess) return;
    const current = yield* registry.show(request.worktree);
    const a = current.assessment?.assessment;
    if (
      !current.binding ||
      current.binding.git_dir !== bound.git_dir ||
      current.binding.git_common_dir !== bound.git_common_dir ||
      (a &&
        (a.remaining_work.length ||
          a.unresolved_obligations.length ||
          ["preserve", "needs_handoff"].includes(a.disposition)))
    )
      return;
    const claims = yield* store.claims(request.worktree);
    if (claims.length) return;
    firstGeneration = yield* registry.assess(
      {
        schema: "workspace-assessment/v1",
        binding: current.binding,
        role: "authoring",
        disposition: "cleanup_candidate",
        remaining_work: [],
        preservation_refs: a?.preservation_refs ?? [],
        handoff_refs: a?.handoff_refs ?? [],
        artifact_destinations: a?.artifact_destinations ?? [],
        unresolved_obligations: [],
      },
      current.generation,
    );
  }).pipe(mapError);
  const result = yield* Effect.gen(function* () {
    if (!request.execute) {
      yield* scriptGate;
      const input = yield* observe;
      const qualification = yield* io(request.worktree, () => qualify(input));
      return {
        state: qualification.outcome === "retirable" ? ("proposed" as const) : ("held" as const),
        qualification,
        reasons: qualification.reasons,
        outputs,
      };
    }
    if (request.ownerCoverage) {
      const baseInput = yield* observe;
      const base = yield* io(request.worktree, () => qualify(baseInput));
      if (base.fingerprint !== request.ownerCoverage.fingerprint)
        return yield* new RetirementError({ target: request.worktree, reason: "proposal_changed" });
      const allowed = new Set([
        ...(base.ignored ?? []).flatMap((r) => [r.path, ...(r.paths ?? [])]),
        ...(base.untrackedPaths ?? []).map((p) => NodePath.resolve(request.worktree, p)),
      ]);
      if (
        request.ownerCoverage.paths.length > 10000 ||
        request.ownerCoverage.paths.some((p) => !allowed.has(p))
      )
        return yield* new RetirementError({
          target: request.worktree,
          reason: "coverage_path_changed",
        });
      ownerPaths = request.ownerCoverage.paths;
    }
    yield* scriptGate;
    const provenance = yield* Effect.serviceOption(Provenance.Provenance);
    if (Option.isSome(provenance) && provenance.value.production && request.threadId) {
      // This explicit lifecycle boundary can link existing production facts. Ordinary work
      // keeps J1's unbounded-sink skip; this is never called while holding our reservation.
      const references = yield* io(request.worktree, async () => {
        const directory = NodePath.join(bound.git_dir, "workspace-completions");
        if (!(await exists(directory))) return [];
        const names = await NodeFSP.readdir(directory);
        if (names.length > 20) return [];
        const matching: string[] = [];
        let bytes = 65536;
        for (const name of names) {
          if (!/^[a-f0-9]{32}\.json$/.test(name)) continue;
          const reference = NodePath.join(directory, name),
            content = await readBounded(reference, bytes);
          bytes -= content.length;
          const value = JSON.parse(content.toString());
          if (
            value.schema === "workspace-output-observation/v1" &&
            value.proofStatus === "deferred" &&
            value.lineage?.threadId === request.threadId &&
            value.lineage?.gitDirectory === bound.git_dir &&
            value.lineage?.rootDevice === String(bound.dev) &&
            value.lineage?.rootInode === String(bound.ino)
          )
            matching.push(reference);
        }
        return matching;
      }).pipe(Effect.catch(() => Effect.succeed([] as string[])));
      for (const reference of references)
        yield* provenance.value
          .production(request.threadId, reference, request.worktree)
          .pipe(
            Effect.catch(() =>
              Effect.logDebug("Optional producer fact unavailable", { threadId: request.threadId }),
            ),
          );
    }
    yield* assess;
    return yield* retire({
      cause: request.cause,
      worktree: request.worktree,
      projectRoot: request.projectRoot,
      maxLevel: request.rules.worktreeSettledScope,
      admit: request.admit,
      observe,
      revalidate: scriptGate.pipe(Effect.andThen(request.revalidate)),
      ...(Option.isSome(gitDriver)
        ? {
            remove: gitDriver.value
              .execute({
                operation: "WorkspaceRetirement.remove",
                cwd: request.projectRoot,
                args: ["worktree", "remove", "--", request.worktree],
                allowNonZeroExit: true,
              })
              .pipe(
                Effect.flatMap((result) =>
                  result.exitCode === 0
                    ? Effect.void
                    : Effect.fail(
                        new RetirementError({
                          target: request.worktree,
                          reason: "git_removal_refused",
                          cause: `git worktree remove: ${result.stderr.trim().split(/\r?\n/)[0] ?? "native removal refused"}`,
                        }),
                      ),
                ),
                mapError,
              ),
          }
        : {}),
      finalizeOutputs: (receipt) => {
        const internal = outputs.filter(
          (r) => inside(r.path, request.worktree) && r.state === "active" && r.class !== "evidence",
        );
        return registry?.forgetOutputs
          ? registry.forgetOutputs(internal).pipe(mapError)
          : internal.length
            ? Effect.fail(
                new RetirementError({
                  target: receipt.binding.path,
                  reason: "output_finalization_unavailable",
                }),
              )
            : Effect.void;
      },
    }).pipe(Effect.provideService(Receipts, store));
  }).pipe(
    mapError,
    Effect.catch((error) =>
      Effect.succeed({
        state: "held" as const,
        reasons: [error.reason],
        target: error.target,
        outputs,
      }),
    ),
  );
  const qualification = "qualification" in result ? result.qualification : null;
  const outcome: WorkspaceRetirementOutcome = {
    state: result.state,
    reasonCodes: result.reasons,
    receiptId: "id" in result ? result.id : null,
    fingerprint: qualification?.fingerprint ?? null,
    paths: qualification
      ? [
          ...new Set([
            ...(qualification.ignored ?? []).flatMap((r) => [r.path, ...(r.paths ?? [])]),
            ...(qualification.untrackedPaths ?? []).map((p) =>
              NodePath.resolve(request.worktree, p),
            ),
          ]),
        ]
      : [],
    details: [
      ...(qualification?.details ?? []),
      ...result.reasons
        .filter(
          (reason) => !(qualification?.details ?? []).some((detail) => detail.reason === reason),
        )
        .map((reason) => ({
          reason,
          target: request.worktree,
          root: request.worktree,
          fact:
            "removalDiagnostic" in result &&
            typeof result.removalDiagnostic === "string" &&
            reason === "git_removal_refused"
              ? result.removalDiagnostic
              : reason === "settle_script_unfinished"
                ? "Durable settle-script completion has not been recorded."
                : reason === "final_checkpoint_unfinished"
                  ? "The latest run has no ready final checkpoint."
                  : reason,
          missingFact: reason,
          lineage: request.threadId ? [{ threadId: request.threadId }] : [],
          nextAction:
            reason === "settle_script_unfinished"
              ? "Resolve the settle script's actual holder and record its completion before reevaluating this checkout."
              : "Resolve this named target fact, then request a fresh proposal.",
        })),
    ],
    outputObligations: outputs.map((r) => ({
      id: r.id,
      path: r.path,
      owner: r.owner,
      state: r.state,
    })),
  };
  if (request.threadId) {
    const t = yield* projections.getThread(ThreadId.make(request.threadId));
    if (t.settledAt) {
      const intent = yield* store.settlement(
        request.threadId,
        request.worktree,
        DateTime.formatIso(t.settledAt),
      );
      if (intent) yield* store.outcome(intent.event_id, outcome);
    }
  }
  return { ...result, outcome };
});
