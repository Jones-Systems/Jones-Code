import { OrchestratorMcpFailure, type OrchestrationV2DomainEvent, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { makeDependencyStore } from "./DependencyStore.ts";

/** Runs after projections, inside the accepting upstream transaction. Promotion is not withdrawal. */
export const acceptEvents = Effect.fn("DependencyCommit.acceptEvents")(function* (
  store: ReturnType<typeof makeDependencyStore>,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
  command?: { readonly commandType: string; readonly threadId: ThreadId },
  includeLifecycle = true,
) {
  if (!(yield* store.available)) return;
  if (command?.commandType === "thread.stop" || command?.commandType === "subagent.stop") {
    const affected = new Set(command.commandType === "thread.stop" ? [command.threadId, ...events.map((event) => event.threadId)] : events.map((event) => event.threadId).filter((id) => id !== command.threadId));
    for (const event of events) if (event.type === "subagent.updated" && event.payload.origin === "app_owned" && event.payload.remoteChild === undefined && event.payload.childThreadId !== null)
      affected.add(event.payload.childThreadId);
    for (const threadId of affected) yield* store.release(threadId);
  }
  if (command?.commandType === "queued-run.cancel") {
    for (const event of events) if (event.type === "run.updated" && event.payload.status === "cancelled")
      yield* store.disposeRunRequests(event.threadId, event.payload.id, "withdrawn");
  }
  if (!includeLifecycle) return;
  for (const event of events) {
    // Planning can race a send targeting another thread. The accepting SQL
    // snapshot must still reject settlement rather than silently release it.
    if (event.type === "thread.settled" && (yield* store.duties(event.threadId)).outgoing > 0)
      return yield* new OrchestratorMcpFailure({ code: "request_conflict", message: "Waiting on explicit results; Stop or release them before settling." });
    if (event.type === "thread.deleted" || event.type === "thread.archived" || event.type === "thread.settled") {
      yield* store.release(event.threadId);
      yield* store.disposeRequests(event.threadId, "dropped");
    }
    if (event.type === "run.updated" && event.payload.status === "rolled_back")
      yield* store.disposeRunRequests(event.threadId, event.payload.id, "dropped");
  }
});
