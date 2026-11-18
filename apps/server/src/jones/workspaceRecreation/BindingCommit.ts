import type { OrchestrationV2AppThread, OrchestrationV2DomainEvent } from "@t3tools/contracts";

export interface ExpectedCheckout {
  readonly worktreePath: string;
  readonly branch: string | null;
}

// Called inside EventSink's write transaction. Concurrent owner metadata survives; another binding wins.
export function bindRecreatedCheckout(
  current: OrchestrationV2AppThread,
  expected: ExpectedCheckout,
  events: readonly OrchestrationV2DomainEvent[],
): readonly OrchestrationV2DomainEvent[] | null {
  const [event, recoveryEvent] = events;
  if (
    current.deletedAt !== null ||
    current.worktreePath !== expected.worktreePath ||
    current.branch !== expected.branch ||
    events.length > 2 ||
    event?.type !== "thread.metadata-updated" ||
    event.threadId !== current.id
  )
    return null;
  if (recoveryEvent !== undefined) {
    if (
      recoveryEvent.type !== "runtime-request.updated" ||
      recoveryEvent.threadId !== current.id ||
      recoveryEvent.payload.kind !== "user_input" ||
      recoveryEvent.payload.status !== "resolved" ||
      recoveryEvent.payload.responseCapability.type !== "message"
    )
      return null;
    const recovery = recoveryEvent.payload.responseCapability.checkoutRecovery;
    if (
      recovery?.worktreePath !== expected.worktreePath ||
      recovery.branch !== expected.branch ||
      recovery.applied === undefined ||
      recovery.applied.runId !== event.runId ||
      recovery.applied.path !== event.payload.worktreePath ||
      recovery.applied.branch !== event.payload.branch
    )
      return null;
  }
  return [
    {
      ...event,
      payload: {
        ...current,
        worktreePath: event.payload.worktreePath,
        branch: event.payload.branch,
        updatedAt: event.payload.updatedAt,
      },
    },
    ...(recoveryEvent === undefined ? [] : [recoveryEvent]),
  ];
}
