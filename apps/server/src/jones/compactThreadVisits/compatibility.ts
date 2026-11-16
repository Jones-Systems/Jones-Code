import {
  applyOrchestrationV2ThreadVisit,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import { applyThreadPullRequestLinkUpdate } from "../../sourceControl/threadPullRequests.ts";

/** One instance per subscriber/generation. Never store expanded events in wire caches. */
export function makeCompactVisitCompatibility(acceptCompact: boolean) {
  let baseline: OrchestrationV2AppThread | undefined;
  return {
    seed(thread: OrchestrationV2AppThread) { baseline = thread; },
    project(event: OrchestrationV2DomainEvent): OrchestrationV2DomainEvent {
      if (event.type === "thread.visit-recorded") {
        if (baseline !== undefined) baseline = applyOrchestrationV2ThreadVisit(baseline, event.payload);
        if (acceptCompact) return event;
        if (baseline === undefined) throw new Error("Compact visit requires a delivered thread baseline");
        return { ...event, type: "thread.visited", payload: baseline };
      }
      if (event.type === "thread.pull-request-link-updated") {
        if (baseline !== undefined) baseline = applyThreadPullRequestLinkUpdate(baseline, event.payload);
      } else if (event.type.startsWith("thread.") && "id" in event.payload &&
                 "lastVisitedAt" in event.payload) {
        // Full replacement events follow the public detail reducer's semantics.
        baseline = event.payload as OrchestrationV2AppThread;
      }
      return event;
    },
  };
}
