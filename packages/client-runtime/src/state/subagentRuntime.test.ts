import {
  EventId,
  NodeId,
  OrchestrationV2DomainEventJson,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { applyOrchestrationV2ProjectionEvent } from "./orchestrationV2Projection.ts";
import { v2Now, v2Projection, v2ThreadId } from "./orchestrationV2TestFixtures.ts";
import { projectedSubagentsToRuntime } from "./subagentRuntime.ts";

const child = {
  id: NodeId.make("observed-child"),
  threadId: v2ThreadId,
  runId: RunId.make("parent-run"),
  parentNodeId: NodeId.make("parent-node"),
  origin: "provider_native",
  createdBy: "agent",
  driver: ProviderDriverKind.make("codex"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerThreadId: null,
  childThreadId: null,
  nativeTaskRef: null,
  prompt: "Inspect the source",
  title: "Observed child",
  model: "gpt-5.4",
  status: "running",
  result: null,
  startedAt: v2Now,
  completedAt: null,
  updatedAt: v2Now,
} satisfies OrchestrationV2Subagent;

describe("projectedSubagentsToRuntime observed reasoning effort", () => {
  it.each([
    ["omitted", undefined, null],
    ["explicit null", null, null],
    ["observed", "xhigh", "xhigh"],
    ["provider-specific observed", "future-provider-effort", "future-provider-effort"],
  ] as const)(
    "presents %s child effort through the V2 update projection",
    (_name, reasoningEffort, expectedEffort) => {
      const event = {
        id: EventId.make("child-effort-update"),
        type: "subagent.updated",
        threadId: v2ThreadId,
        occurredAt: v2Now,
        payload: {
          ...child,
          ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
        },
      } satisfies OrchestrationV2DomainEvent;
      const wire = Schema.encodeSync(OrchestrationV2DomainEventJson)(event);
      const decoded = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson)(wire);
      const projection = applyOrchestrationV2ProjectionEvent(v2Projection, decoded)!;
      const rows = projectedSubagentsToRuntime(projection.subagents);

      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: child.id,
        title: child.title,
        model: child.model,
        effort: expectedEffort,
        status: "running",
      });
    },
  );

  it("keeps a sibling's unknown effort independent of another child's observed value", () => {
    const sibling = { ...child, id: NodeId.make("unknown-child") };
    const rows = projectedSubagentsToRuntime([
      { ...child, reasoningEffort: "high" },
      sibling,
    ]);

    expect(rows.map(({ id, effort }) => ({ id, effort }))).toEqual([
      { id: child.id, effort: "high" },
      { id: sibling.id, effort: null },
    ]);
  });
});
