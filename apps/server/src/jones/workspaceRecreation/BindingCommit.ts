import type { OrchestrationV2AppThread, OrchestrationV2DomainEvent } from "@t3tools/contracts";

export interface ExpectedCheckout {
  readonly worktreePath: string;
  readonly branch: string | null;
}

// Called inside EventSink's write transaction. Concurrent owner metadata survives; another binding wins.
export function bindRecreatedCheckout(current: OrchestrationV2AppThread, expected: ExpectedCheckout,
  events: readonly OrchestrationV2DomainEvent[]): readonly OrchestrationV2DomainEvent[] | null {
  if (current.deletedAt !== null || current.worktreePath !== expected.worktreePath || current.branch !== expected.branch ||
    events.length !== 1 || events[0]?.type !== "thread.metadata-updated" || events[0].threadId !== current.id) return null;
  const event = events[0];
  return [{ ...event, payload: { ...current, worktreePath: event.payload.worktreePath,
    branch: event.payload.branch, updatedAt: event.payload.updatedAt } }];
}
