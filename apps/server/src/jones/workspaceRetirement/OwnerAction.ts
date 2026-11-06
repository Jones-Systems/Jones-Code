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
import { evaluate } from "./Runtime.ts";

export const make = Effect.gen(function* () {
  const context = yield* Effect.context<
    SqlClient.SqlClient | Config.ServerConfig | Projections.ProjectionStoreV2
  >();
  const projections = yield* Projections.ProjectionStoreV2,
    projects = yield* Projects.ProjectStoreV2;
  const settings = yield* Settings.ServerSettingsService,
    config = yield* Config.ServerConfig;
  return Effect.fn("WorkspaceRetirement.ownerAction")(
    function* (input: WorkspaceRetirementAction) {
      const now = yield* DateTime.now,
        entries: StorageCleanupReportEntry[] = [];
      if (
        input.action === "retire" &&
        (!input.threadId || !input.worktreePath || !input.expectedFingerprint)
      )
        return yield* new RetirementError({
          target: input.worktreePath ?? "checkout",
          reason: "exact_proposal_required",
        });
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
        return yield* new RetirementError({
          target: input.worktreePath!,
          reason: "binding_changed",
        });
      for (const item of selected.slice(0, 200)) {
        const thread = yield* projections.getThread(item.threadId),
          project = yield* projects.get(thread.projectId);
        if (Option.isNone(project)) continue;
        const rules = resolveWorktreeCleanup(yield* settings.getSettings, thread.projectId);
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
            Schema.is(RetirementError)(cause)
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
            const reason =
              Schema.is(RetirementError)(error) ? error.reason : "target_observation_unavailable";
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
        entries.push({
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
          bytes: null,
          files: null,
          retirement: result.outcome,
        });
      }
      return {
        trigger: "manual",
        startedAt: DateTime.formatIso(now),
        finishedAt: DateTime.formatIso(yield* DateTime.now),
        entries,
        counts: {
          removed: entries.filter((e) => e.outcome === "removed").length,
          kept: entries.filter((e) => e.outcome === "kept").length,
          failed: entries.filter((e) => e.outcome === "failed").length,
        },
        omittedCount: Math.max(0, selected.length - 200),
        bytesFreed: 0,
      } satisfies StorageCleanupReport;
    },
    Effect.mapError((cause) =>
      Schema.is(ServerSettingsError)(cause)
        ? cause
        : new ServerSettingsError({
            settingsPath: config.settingsPath,
            operation: "read-file",
            cause,
          }),
    ),
  );
});
