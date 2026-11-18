import { isPersonalWorkstreamSnoozed } from "@t3tools/contracts/jones/workstreamsPersonal";
import {
  openPersonalWorkstreams,
  personalThreadKey,
  type PersonalWorkstreamView,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { effectiveSnoozed, threadWokeAt } from "@t3tools/client-runtime/state/thread-settled";

export function projectWorkstreamSnoozes(state: PersonalWorkstreamsState, now: string) {
  const nowMs = Date.parse(now);
  const snoozed = openPersonalWorkstreams(state).filter((workstream) =>
    isPersonalWorkstreamSnoozed(workstream, nowMs),
  );
  const snoozedById = new Map(snoozed.map((workstream) => [workstream.id, workstream]));
  const memberOfSnoozedWorkstream = (thread: EnvironmentThreadShell) => {
    const groupId = state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId;
    return groupId == null ? undefined : snoozedById.get(groupId);
  };
  return {
    snoozed,
    visibleState:
      snoozed.length === 0
        ? state
        : {
            ...state,
            workstreams: state.workstreams.filter((workstream) => !snoozedById.has(workstream.id)),
          },
    hidesThread(thread: EnvironmentThreadShell) {
      const workstream = memberOfSnoozedWorkstream(thread);
      return workstream !== undefined && isHiddenBySnooze(thread, workstream, now);
    },
    keepsAwakeMemberUnassigned(thread: EnvironmentThreadShell) {
      return memberOfSnoozedWorkstream(thread) !== undefined;
    },
  };
}

function isHiddenBySnooze(
  thread: EnvironmentThreadShell,
  workstream: PersonalWorkstreamView,
  now: string,
): boolean {
  if (thread.settledOverride === "settled") return false;
  // Individually snoozed threads keep their own shelf entry and wake time.
  if (effectiveSnoozed(thread, { now })) return false;
  const startedAt = workstream.snoozedAt;
  if (
    startedAt != null &&
    [
      thread.lastSnoozeWakeAt,
      threadWokeAt(thread, { now }),
      thread.latestUserAuthoredMessageAt,
    ].some((timestamp) => timestamp != null && Date.parse(timestamp) > Date.parse(startedAt))
  )
    return false;
  return effectiveSnoozed(
    { ...thread, snoozedAt: startedAt ?? null, snoozedUntil: workstream.snoozedUntil ?? null },
    { now },
  );
}
