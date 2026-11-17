import * as Schema from "effect/Schema";
import {
  ServerSettingsError,
  type WorkspaceRetirementAction,
  type StorageCleanupReport,
  type StorageCleanupReportEntry,
} from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import { beforeCleanup } from "../workspaceLifecycle/NativeBoundary.ts";
import { withOperation } from "../workspaceLifecycle/NativeOperation.ts";
import { RetirementError } from "./Receipts.ts";
import { TargetObservation, presenceOutcome } from "./TargetPresence.ts";
import { evaluate } from "./Runtime.ts";

const compiledMain3acaCodec22 = Schema.is(ServerSettingsError);

const compiledMain3acaCodec21 = Schema.is(RetirementError);

const compiledMain3acaCodec20 = Schema.is(RetirementError);

export interface OwnerActionOptions {
  /** Bytes a checkout occupies, or null when it cannot be measured. */
  readonly measure?: (worktreePath: string) => Effect.Effect<number | null>;
}

const heldEntry = (input: {
  readonly reason: string;
  readonly nextAction: string;
  readonly outcome: StorageCleanupReportEntry["outcome"];
  readonly path: string | null;
  readonly projectPath: string | null;
  readonly threadId: StorageCleanupReportEntry["threadId"];
  readonly threadTitle: string | null;
  readonly bytes: number | null;
  readonly summary: string;
}) =>
  ({
    kind: "worktree",
    outcome: input.outcome,
    reason: input.summary,
    path: input.path,
    projectPath: input.projectPath,
    threadId: input.threadId,
    threadTitle: input.threadTitle,
    bytes: input.bytes,
    files: null,
    retirement: {
      state: "held",
      reasonCodes: [input.reason],
      receiptId: null,
      fingerprint: null,
      paths: [],
      details: [
        {
          reason: input.reason,
          target: input.path ?? "checkout",
          root: input.path ?? "checkout",
          fact: input.reason,
          missingFact: input.reason,
          lineage: input.threadId === null ? [] : [{ threadId: input.threadId }],
          nextAction: input.nextAction,
        },
      ],
      outputObligations: [],
    },
  }) satisfies StorageCleanupReportEntry;

export const make = Effect.gen(function* () {
  const context = yield* Effect.context<
    SqlClient.SqlClient | Config.ServerConfig | Projections.ProjectionStoreV2
  >();
  const observeTarget = yield* TargetObservation;
  const projections = yield* Projections.ProjectionStoreV2,
    projects = yield* Projects.ProjectStoreV2;
  const settings = yield* Settings.ServerSettingsService,
    config = yield* Config.ServerConfig;
  return Effect.fn("WorkspaceRetirement.ownerAction")(
    function* (input: WorkspaceRetirementAction, options?: OwnerActionOptions) {
      const now = yield* DateTime.now,
        entries: StorageCleanupReportEntry[] = [];
      let omitted = 0;
      const finish = Effect.fn("WorkspaceRetirement.ownerAction.finish")(function* () {
        return {
          operation: input.action,
          trigger: "manual",
          startedAt: DateTime.formatIso(now),
          finishedAt: DateTime.formatIso(yield* DateTime.now),
          entries,
          counts: {
            removed: entries.filter((e) => e.outcome === "removed").length,
            kept: entries.filter((e) => e.outcome === "kept").length,
            failed: entries.filter((e) => e.outcome === "failed").length,
          },
          omittedCount: omitted,
          bytesFreed: entries.reduce(
            (total, e) => total + (e.outcome === "removed" ? (e.bytes ?? 0) : 0),
            0,
          ),
        } satisfies StorageCleanupReport;
      });
      // A stale or incomplete retire is a result for its checkout, not a settings failure.
      const refused = (reason: "exact_proposal_required" | "binding_changed") => {
        entries.push(
          heldEntry({
            reason,
            nextAction:
              reason === "binding_changed"
                ? "This checkout's thread binding changed after the proposal. Propose again, then retire."
                : "Retire needs the thread, checkout path and fingerprint from a current proposal. Propose again, then retire.",
            outcome: "kept",
            path: input.worktreePath ?? null,
            projectPath: null,
            threadId: input.threadId ?? null,
            threadTitle: null,
            bytes: null,
            summary: reason,
          }),
        );
        return finish();
      };
      if (
        input.action === "retire" &&
        (!input.threadId || !input.worktreePath || !input.expectedFingerprint)
      )
        return yield* refused("exact_proposal_required");
      const candidates = yield* projections.getThreadWorktreePaths({ includeArchived: true });
      const selected = [
        ...new Map(
          candidates
            .filter((c) => input.threadId === undefined || c.threadId === input.threadId)
            .map((c) => [c.worktreePath, c]),
        ).values(),
      ];
      if (
        input.action === "retire" &&
        (selected.length !== 1 || selected[0]?.worktreePath !== input.worktreePath)
      )
        return yield* refused("binding_changed");
      const serverSettings = yield* settings.getSettings;
      let evaluated = 0;
      for (const item of selected) {
        let threadTitle: string | null = null;
        let projectPath: string | null = null;
        let failureReason = "thread_metadata_unavailable";
        let nextAction = "Restore this thread's metadata lookup, then repeat the proposal.";
        let bytes: number | null = null;
        const entry = yield* Effect.gen(function* () {
          const thread = yield* projections.getThread(item.threadId);
          threadTitle = thread.title;
          failureReason = "project_metadata_unavailable";
          nextAction = "Restore this thread's project metadata lookup, then repeat the proposal.";
          const project = yield* projects.get(thread.projectId);
          if (Option.isNone(project)) {
            failureReason = "project_metadata_missing";
            nextAction =
              "Restore or reconcile this thread's missing project, then repeat the proposal.";
            return yield* new RetirementError({
              target: item.worktreePath,
              reason: failureReason,
            });
          }
          projectPath = project.value.workspaceRoot;
          const presence = yield* Effect.promise(() =>
            observeTarget(item.worktreePath, project.value.workspaceRoot),
          );
          if (presence.status !== "present") {
            return {
              kind: "worktree",
              outcome: "kept",
              reason: presenceOutcome(item.worktreePath, presence).reasonCodes.join(", "),
              path: item.worktreePath,
              projectPath,
              threadId: item.threadId,
              threadTitle,
              bytes: null,
              files: null,
              retirement: presenceOutcome(item.worktreePath, presence),
            } satisfies StorageCleanupReportEntry;
          }
          if (evaluated >= 200) {
            omitted++;
            return null;
          }
          evaluated++;
          failureReason = "candidate_evaluation_unavailable";
          nextAction = "Restore this checkout's retirement observations, then repeat the proposal.";
          const rules = resolveWorktreeCleanup(serverSettings, thread.projectId);
          // Measure before removal; a failed measurement leaves bytes null and never blocks it.
          if (input.action === "retire" && options?.measure)
            bytes = yield* options
              .measure(item.worktreePath)
              .pipe(Effect.orElseSucceed(() => null));
          const revalidate = Effect.gen(function* () {
            const fresh = yield* projections.getThread(item.threadId);
            if (
              fresh.worktreePath !== thread.worktreePath ||
              fresh.branch !== thread.branch ||
              DateTime.toEpochMillis(fresh.updatedAt) !== DateTime.toEpochMillis(thread.updatedAt)
            )
              return yield* new RetirementError({
                target: item.worktreePath,
                reason: "binding_changed",
              });
          }).pipe(
            Effect.mapError((cause) =>
              compiledMain3acaCodec20(cause)
                ? cause
                : new RetirementError({
                    target: item.worktreePath,
                    reason: "observation_failed",
                    cause,
                  }),
            ),
          );
          const result = yield* evaluate({
            worktree: item.worktreePath,
            projectRoot: project.value.workspaceRoot,
            threadId: item.threadId,
            rules,
            cause: "owner_action",
            execute: input.action === "retire",
            ...(input.action === "retire"
              ? {
                  ownerCoverage: {
                    fingerprint: input.expectedFingerprint!,
                    paths: input.coveredPaths ?? [],
                  },
                }
              : {}),
            admit: withOperation(
              {
                id: `retire:${item.threadId}`,
                kind: "preservation",
                threadId: item.threadId,
                worktree: item.worktreePath,
                boundaries: ["cleanup"],
                inherit: false,
                check: revalidate,
              },
              beforeCleanup({
                id: `retire:${item.threadId}`,
                threadId: item.threadId,
                boundary: "cleanup",
              }),
            ).pipe(
              Effect.mapError(
                (cause) =>
                  new RetirementError({
                    target: item.worktreePath,
                    reason: "workspace_preservation_hold",
                    cause,
                  }),
              ),
            ),
            revalidate,
          }).pipe(
            Effect.provide(context),
            Effect.catch((error) => {
              const reason = compiledMain3acaCodec21(error)
                ? error.reason
                : "target_observation_unavailable";
              return Effect.succeed({
                state: "held" as const,
                reasons: [reason],
                outcome: {
                  state: "held" as const,
                  reasonCodes: [reason],
                  receiptId: null,
                  fingerprint: null,
                  paths: [],
                  details: [
                    {
                      reason,
                      target: item.worktreePath,
                      root: item.worktreePath,
                      fact: reason,
                      missingFact: reason,
                      lineage: [{ threadId: item.threadId }],
                      nextAction:
                        "Repeat the named observation for this checkout with its owning adapter.",
                    },
                  ],
                  outputObligations: [],
                },
              });
            }),
          );
          return {
            kind: "worktree",
            outcome:
              result.state === "retired"
                ? "removed"
                : result.state === "partial" || result.state === "unknown"
                  ? "failed"
                  : "kept",
            reason:
              result.reasons.join(", ") ||
              (input.action === "propose"
                ? "Retirement proposed; confirmation is required."
                : result.state),
            path: item.worktreePath,
            projectPath: project.value.workspaceRoot,
            threadId: item.threadId,
            threadTitle: thread.title,
            bytes,
            files: null,
            retirement: result.outcome,
          } satisfies StorageCleanupReportEntry;
        }).pipe(
          Effect.orElseSucceed(() =>
            heldEntry({
              reason: failureReason,
              nextAction,
              outcome: "failed",
              path: item.worktreePath,
              projectPath,
              threadId: item.threadId,
              threadTitle,
              bytes,
              summary: `${failureReason}: ${nextAction}`,
            }),
          ),
        );
        if (entry !== null) entries.push(entry);
      }
      return yield* finish();
    },
    Effect.mapError((cause) =>
      compiledMain3acaCodec22(cause)
        ? cause
        : new ServerSettingsError({
            settingsPath: config.settingsPath,
            operation: "read-file",
            cause,
          }),
    ),
  );
});
