import type { ServerSettings } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { isWorkModeKeepWarm } from "../provider/workModePrompt.ts";

const DAY_MS = 24 * 60 * 60 * 1_000;

type ThreadActivityRecords = ProjectionStore.ProjectionRecords<"runs" | "messages">;

function genuineActivity<T extends ProjectionStore.ProjectionSettlementCandidate>(
  candidate: T,
  records: ThreadActivityRecords,
): T {
  const userMessages = records.messages.filter((message) => message.role === "user");
  const refreshIds = new Set(
    userMessages.filter((message) => isWorkModeKeepWarm(message)).map((message) => message.id),
  );
  const runs = records.runs.toSorted((left, right) => right.ordinal - left.ordinal);
  if (runs[0] === undefined || !refreshIds.has(runs[0].userMessageId)) return candidate;
  const genuineRun = runs.find((run) => !refreshIds.has(run.userMessageId));
  if (genuineRun === undefined) return candidate;
  const genuineMessageAt = userMessages
    .filter((message) => !refreshIds.has(message.id))
    .reduce<DateTime.Utc | null>(
      (latest, message) =>
        latest === null ||
        DateTime.toEpochMillis(message.updatedAt) > DateTime.toEpochMillis(latest)
          ? message.updatedAt
          : latest,
      null,
    );
  return {
    ...candidate,
    latestUserMessageAt: genuineMessageAt,
    latestRunRequestedAt: genuineRun.requestedAt,
    latestRunStartedAt: genuineRun.startedAt,
    latestRunCompletedAt: genuineRun.completedAt,
  };
}

/**
 * Each keep-warm refresh is a run and a user message, so upstream inactivity
 * settlement would treat every refreshed thread as active and Work would keep
 * it warm forever. While Work is on, measure inactivity from the latest
 * exchange that is not a refresh. The returned view checks the settings current
 * when each thread is decided, so turning Work off during the history read
 * restores upstream anchors. Identity, status and snapshot fields stay as read,
 * so pull-request settlement and the settle command's snapshot guard are
 * unchanged, and a thread whose history cannot be read keeps upstream anchors.
 */
export function workModeSettlementActivity<T extends ProjectionStore.ProjectionSettlementCandidate>(
  projections: Pick<ProjectionStore.ProjectionStoreV2["Service"], "getThreadRecords">,
  settings: ServerSettings,
  candidates: ReadonlyArray<T>,
  nowMs: number,
): Effect.Effect<(thread: T, currentSettings: ServerSettings) => T> {
  if (!settings.workModeEnabled) return Effect.succeed((thread) => thread);
  return Effect.forEach(
    candidates,
    (candidate) => {
      const afterDays = resolveProjectSettings(settings, candidate.projectId).settings
        .sidebarAutoSettleAfterDays;
      const latestUserMs =
        candidate.latestUserMessageAt == null
          ? null
          : DateTime.toEpochMillis(candidate.latestUserMessageAt);
      const userAuthoredMs =
        candidate.latestUserAuthoredMessageAt === null
          ? null
          : DateTime.toEpochMillis(candidate.latestUserAuthoredMessageAt);
      // Refreshes are never user-authored, and genuine activity is never older
      // than the thread or its latest user-authored message, so most threads
      // can be decided without reading their history.
      if (
        afterDays === null ||
        latestUserMs === null ||
        (userAuthoredMs !== null && userAuthoredMs >= latestUserMs) ||
        Math.max(DateTime.toEpochMillis(candidate.createdAt), userAuthoredMs ?? 0) >=
          nowMs - afterDays * DAY_MS
      )
        return Effect.succeed(candidate);
      return projections
        .getThreadRecords(candidate.id, ["runs", "messages"], { messageRoles: ["user"] })
        .pipe(
          Effect.map((records) => genuineActivity(candidate, records)),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : Effect.logWarning("Work Mode settlement activity unavailable", {
                  threadId: candidate.id,
                  cause: Cause.pretty(cause),
                }).pipe(Effect.as(candidate)),
          ),
        );
    },
    { concurrency: 8 },
  ).pipe(
    Effect.map((discounted) => {
      const genuine = new Map(discounted.map((thread) => [thread.id, thread]));
      return (thread, currentSettings) =>
        currentSettings.workModeEnabled ? (genuine.get(thread.id) ?? thread) : thread;
    }),
  );
}
