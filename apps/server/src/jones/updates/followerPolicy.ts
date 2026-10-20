import type { FleetFollowerBlocker } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesUpdatePolicy } from "@t3tools/contracts/jones/jonesUpdates";

/**
 * Provisional owner default. Input history is not persisted, so the window also starts no earlier
 * than this server's own start.
 */
export const QUIET_INPUT_IDLE_MS = 30 * 60_000;
/** Spacing between automatic activations. A manual Install is explicit intent and never waits. */
export const AUTOMATIC_ACTIVATION_SPACING_MS = 15 * 60_000;

/** Durable work a restart would cut, counted from the host's projections and outbox. */
export interface HostWorkSnapshot {
  /** Runs preparing, starting, running, or draining delegated or background work. */
  readonly activeRuns: number;
  /** Queued runs that are not held, so they are about to start. */
  readonly unheldQueuedRuns: number;
  /** Pending provider process effects such as a turn start or an approval reply. */
  readonly processEffects: number;
  /** Provider threads reporting native background tasks (subagents, monitors, shells). */
  readonly backgroundTasks: number;
  readonly pendingRequests: ReadonlyArray<{
    readonly kind: string;
    /** The request's `responseCapability.type`; null when it could not be read. */
    readonly capability: string | null;
  }>;
}

/**
 * What a restart does to a pending request. A message-capable question stays answerable, so it
 * does not hold an update. An approval or question answered through a live provider callback is
 * cancelled by a restart, and so is a dynamic tool call or auth refresh in progress. Surviving
 * those would need the provider adapter to declare that a pending approval can be rebound to the
 * resumed session; no adapter declares it, so they hold automatic activation.
 */
export function pendingRequestObligation(request: HostWorkSnapshot["pendingRequests"][number]) {
  if (request.kind === "dynamic_tool_call" || request.kind === "auth_refresh")
    return "tool-call" as const;
  switch (request.capability) {
    case "message":
      return "survives" as const;
    case "live":
    case "not_resumable":
      return "approval" as const;
    default:
      return "unknown" as const;
  }
}

/** Blockers in a stable order; scheduled registrations and held queues never appear here. */
export function workBlockers(
  work: HostWorkSnapshot,
  runningTerminals: number,
): ReadonlyArray<FleetFollowerBlocker> {
  const requests = { approval: 0, "tool-call": 0, unknown: 0 };
  for (const request of work.pendingRequests) {
    const obligation = pendingRequestObligation(request);
    if (obligation !== "survives") requests[obligation] += 1;
  }
  const counts: ReadonlyArray<FleetFollowerBlocker> = [
    { kind: "run", count: work.activeRuns },
    { kind: "queued-run", count: work.unheldQueuedRuns },
    { kind: "terminal", count: runningTerminals },
    { kind: "background-task", count: work.backgroundTasks },
    { kind: "process-effect", count: work.processEffects },
    { kind: "approval", count: requests.approval },
    { kind: "tool-call", count: requests["tool-call"] },
    { kind: "unknown", count: requests.unknown },
  ];
  return counts.filter((blocker) => blocker.count > 0);
}

const BLOCKER_TEXT: Record<FleetFollowerBlocker["kind"], string> = {
  run: "running turns or delegated tasks",
  "queued-run": "queued messages about to start",
  terminal: "terminal commands",
  "background-task": "provider background tasks",
  "process-effect": "provider requests in flight",
  approval: "approvals waiting in a live provider session",
  "tool-call": "tool calls in progress",
  unknown: "requests whose restart behaviour is unknown",
};

export function describeBlockers(blockers: ReadonlyArray<FleetFollowerBlocker>): string {
  return blockers.map((blocker) => `${BLOCKER_TEXT[blocker.kind]} (${blocker.count})`).join(", ");
}

const minutes = (ms: number) => {
  const count = Math.max(1, Math.ceil(ms / 60_000));
  return `${count} minute${count === 1 ? "" : "s"}`;
};

export type AutomaticActivationDecision =
  | {
      readonly ready: true;
      readonly inputIdleMs: number;
      readonly blockers: ReadonlyArray<FleetFollowerBlocker>;
    }
  | {
      readonly ready: false;
      readonly inputIdleMs: number;
      readonly blockers: ReadonlyArray<FleetFollowerBlocker>;
      readonly reason: string;
      /** Epoch milliseconds before which no automatic activation may start, when known. */
      readonly notBefore?: number;
    };

/**
 * Whether an automatic activation may start now. Cheap signals are judged first; `work` is the
 * durable snapshot, absent when the caller has not read it because an earlier gate is closed.
 * A failed read is `"unavailable"` and holds activation.
 */
export function automaticActivationDecision(input: {
  readonly now: number;
  readonly lastInputAt: number;
  readonly runningTerminals: number;
  readonly lastAutomaticActivationAt: number | null;
  readonly work?: HostWorkSnapshot | "unavailable" | undefined;
  readonly requiredInputIdleMs?: number;
  readonly spacingMs?: number;
}): AutomaticActivationDecision {
  const required = input.requiredInputIdleMs ?? QUIET_INPUT_IDLE_MS;
  const spacing = input.spacingMs ?? AUTOMATIC_ACTIVATION_SPACING_MS;
  const inputIdleMs = Math.max(0, input.now - input.lastInputAt);
  const spacedUntil =
    input.lastAutomaticActivationAt === null ? 0 : input.lastAutomaticActivationAt + spacing;
  const idleUntil = input.lastInputAt + required;
  const notBefore = Math.max(spacedUntil, idleUntil);
  if (input.now < spacedUntil)
    return {
      ready: false,
      inputIdleMs,
      blockers: [],
      notBefore,
      reason: "The previous automatic update was too recent.",
    };
  if (input.now < idleUntil)
    return {
      ready: false,
      inputIdleMs,
      blockers: [],
      notBefore,
      reason: `Waiting until no client has had input for ${Math.round(required / 60_000)} minutes (${minutes(idleUntil - input.now)} left).`,
    };
  if (input.work === "unavailable")
    return {
      ready: false,
      inputIdleMs,
      blockers: [{ kind: "unknown", count: 1 }],
      reason: "Running work on this host could not be read.",
    };
  const blockers =
    input.work === undefined
      ? input.runningTerminals > 0
        ? [{ kind: "terminal" as const, count: input.runningTerminals }]
        : []
      : workBlockers(input.work, input.runningTerminals);
  if (blockers.length > 0)
    return {
      ready: false,
      inputIdleMs,
      blockers,
      reason: `Waiting for ${describeBlockers(blockers)} to finish.`,
    };
  if (input.work === undefined)
    return {
      ready: false,
      inputIdleMs,
      blockers,
      reason: "Running work on this host has not been checked yet.",
    };
  return { ready: true, inputIdleMs, blockers };
}

export type FollowerOrigin = "manual" | "fleet";

/**
 * Why a follower may not prepare or install `target`, or undefined when it may. A host without a
 * policy chooses its own builds. Every entrypoint applies it, a laptop campaign included: an older
 * laptop's commit proof stays in its own journal and never reaches the host, so until the laptop
 * publishes its committed source the host holds.
 */
export function followerTargetRefusal(
  policy: JonesUpdatePolicy | undefined,
  target: string | undefined,
  /** The target commit's tree as the repository records it, once a check has read it. */
  tree?: string,
): string | undefined {
  if (policy === undefined) return undefined;
  if (policy.leaderSource === null || policy.leaderTree === undefined)
    return "This host follows its laptop and waits for the laptop to report its installed build. A laptop app that predates following cannot report it; update the laptop app, or turn off fleet updates for this host to choose builds here.";
  if (target !== policy.leaderSource)
    return `This host follows its laptop and installs only the laptop's installed source ${policy.leaderSource.slice(0, 7)}.`;
  if (tree !== undefined && tree !== policy.leaderTree)
    return "The laptop reported a source tree that the repository does not record for that commit, so this host does not install it.";
  return undefined;
}

/** Ends an automatic activation before the launcher accepts it; the stage stays ready. */
export class AutomaticActivationWithdrawn extends Error {
  constructor(reason: string) {
    super(reason);
    this.name = "AutomaticActivationWithdrawn";
  }
}

/** The withdrawal anywhere in an error's cause chain. */
export function findActivationWithdrawal(error: unknown): AutomaticActivationWithdrawn | undefined {
  let current = error;
  for (let depth = 0; depth < 6 && current !== null && typeof current === "object"; depth++) {
    if (current instanceof AutomaticActivationWithdrawn) return current;
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}
