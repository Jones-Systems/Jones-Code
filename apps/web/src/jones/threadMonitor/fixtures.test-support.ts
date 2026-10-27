import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  ThreadMonitorEnvironment,
  ThreadMonitorRow,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
const availability = { available: true, reason: null };
export const testEnvironment: ThreadMonitorEnvironment = {
  publisherGeneration: "generation",
  environmentId: EnvironmentId.make("environment"),
  environmentEpoch: 0,
  label: "Synthetic environment",
  transport: "connected",
  freshness: "live",
  reason: null,
};
export function testRow(patch: Partial<ThreadMonitorRow> = {}): ThreadMonitorRow {
  return {
    publisherGeneration: testEnvironment.publisherGeneration,
    environmentId: testEnvironment.environmentId,
    environmentEpoch: 0,
    threadId: ThreadId.make("thread"),
    title: "Synthetic thread",
    projectLabel: "Synthetic project",
    group: "waiting",
    status: "idle",
    statusLabel: "Idle",
    updatedAt: "2026-10-26T07:00:00.000Z",
    completedAt: null,
    completionTimeUnavailable: false,
    pendingChildAttention: null,
    actions: {
      reply: availability,
      settle: availability,
      respondToApproval: availability,
      respondToUserInput: availability,
      usePlan: availability,
      restore: availability,
    },
    ...patch,
  };
}
