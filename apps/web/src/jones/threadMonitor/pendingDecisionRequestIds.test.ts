import {
  NodeId,
  ProviderSessionId,
  RuntimeRequestId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { monitorDraftKey } from "@t3tools/client-runtime/jones/thread-monitor";
import { describe, expect, it } from "vite-plus/test";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import { projectMonitorDetail } from "./detail";
import { testEnvironment, testRow } from "./fixtures.test-support";
import { pendingDecisionRequestIds } from "./pendingDecisionRequestIds";
import { MonitorPeerStore } from "./peerStore";
import { monitorSnapshot } from "./snapshots";
import { emptyCounts, MonitorStore, refOf } from "./store";

const input = RuntimeRequestId.make("input-a");
const approval = RuntimeRequestId.make("approval-b");
function boundedProjection(): OrchestrationV2ThreadProjection {
  const projection = makeThreadProjectionFixture();
  const request = {
    nodeId: NodeId.make("node"),
    providerTurnId: null,
    nativeRequestRef: null,
    status: "pending" as const,
    responseCapability: {
      type: "live" as const,
      providerSessionId: ProviderSessionId.make("synthetic-session"),
    },
    createdAt: projection.updatedAt,
    resolvedAt: null,
  };
  return {
    ...projection,
    runtimeRequests: [
      { ...request, id: input, kind: "user_input" },
      { ...request, id: approval, kind: "command" },
    ],
    turnItems: [],
  };
}

describe("monitor pending decision retention from bounded live projections", () => {
  it.each(["resolved", "removed"] as const)(
    "retains a paged-out input through approval selection and peer replay, then fences it when %s",
    async (resolution) => {
      const store = new MonitorStore("generation");
      store.register(testEnvironment.environmentId);
      const row = testRow();
      store.setCohort([testEnvironment], [row], { ...emptyCounts(), waiting: 1 });
      const selection = { ...refOf(row), selectionId: "selected" };
      store.select(selection);
      const draft = { ...store.draft(row, input), revision: 1, answers: { note: "unsent A" } };
      expect(store.edit(draft)).toBe(true);
      const publish = (projection: OrchestrationV2ThreadProjection) => {
        const detail = projectMonitorDetail({
          selection,
          projection,
          revision: store.nextDetailRevision(),
          unavailableReason: null,
          planUnavailableReason: null,
          childAttention: false,
        });
        store.setDetail(detail, pendingDecisionRequestIds(projection));
        return detail;
      };
      const projection = boundedProjection();
      expect(projection.turnItems).toEqual([]);
      expect(publish(projection).request?.requestId).toBe(approval);
      expect(store.draft(row, input).answers).toEqual(draft.answers);
      const edited = { ...draft, revision: 2, answers: { note: "edited while A is paged out" } };
      expect(store.edit(edited)).toBe(true);
      const peer = new MonitorPeerStore(async () => undefined);
      peer.receive({ type: "reset", publisherGeneration: "generation" });
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision: 1,
        environments: store.environments,
        rows: store.visibleRows(),
        counts: store.counts,
      }))
        peer.receive(frame);
      for (const retained of store.drafts.values())
        peer.receive({ type: "draft", draft: retained });
      expect(peer.draft(row, input).answers).toEqual(edited.answers);
      publish({
        ...projection,
        runtimeRequests: projection.runtimeRequests.filter((request) => request.id === input),
      });
      expect(store.draft(row, input).answers).toEqual(edited.answers);
      publish({
        ...projection,
        runtimeRequests:
          resolution === "removed"
            ? []
            : projection.runtimeRequests.map((request) => ({
                ...request,
                status: "resolved",
                resolvedAt: projection.updatedAt,
              })),
      });
      expect(store.drafts.has(monitorDraftKey(row, input))).toBe(false);
      expect(store.edit({ ...edited, revision: 3 })).toBe(false);
      await store.retire(row.environmentId);
      publish(projection);
      expect(store.edit({ ...edited, revision: 4 })).toBe(false);
      expect(store.drafts.size).toBe(0);
    },
  );
});
