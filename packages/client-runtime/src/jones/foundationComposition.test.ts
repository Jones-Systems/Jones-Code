import { describe, expect, it } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import {
  EventId,
  CommandId,
  OrchestrationV2DomainEvent,
  OrchestrationV2DomainEventJson,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { applyOrchestrationV2ProjectionEvent } from "../state/orchestrationV2Projection.ts";
import { v2Now, v2Projection } from "../state/orchestrationV2TestFixtures.ts";

const decode = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
const encode = Schema.encodeSync(OrchestrationV2DomainEventJson);

describe("foundation compact visit composition", () => {
  it("replays a compact read mark without erasing newer Jones or upstream thread facts", () => {
    const thread = {
      ...v2Projection.thread,
      title: "newer title",
      worktreePath: "/synthetic/newer",
      threadMessagesBlocked: true,
      opens: { openedAt: 1, score: 2, picks: [] },
      lastTitleRegeneration: {
        requestId: CommandId.make("title-request"),
        completedAt: v2Now,
        status: "renamed" as const,
      },
      updatedAt: DateTime.add(v2Now, { days: 1 }),
    };
    // Unknown future fields must also survive the reducer, independently of codec additions.
    const projection = { ...v2Projection, thread } satisfies OrchestrationV2ThreadProjection;
    const event = decode({
      id: "event:compact-visit",
      type: "thread.visit-recorded",
      threadId: thread.id,
      occurredAt: DateTime.formatIso(v2Now),
      payload: { lastVisitedAt: DateTime.formatIso(v2Now) },
    });
    expect(Schema.is(OrchestrationV2DomainEvent)(event)).toBe(true);
    expect(decode(encode(event))).toEqual(event);
    const next = applyOrchestrationV2ProjectionEvent(projection, event)!;
    expect(next.thread).toEqual({ ...thread, lastVisitedAt: v2Now });
    expect(next.updatedAt).toBe(projection.updatedAt);
    expect(next.runs).toBe(projection.runs);
    expect(next.thread.lastTitleRegeneration).toBe(thread.lastTitleRegeneration);
  });

  it("distinguishes omitted opens from an explicit clear and still decodes full legacy visits", () => {
    const projection = { ...v2Projection, thread: { ...v2Projection.thread, opens: { openedAt: 1, score: 2, picks: [] } } };
    const compact = {
      id: EventId.make("event:visit-opens"),
      type: "thread.visit-recorded" as const,
      threadId: projection.thread.id,
      occurredAt: v2Now,
      payload: { lastVisitedAt: v2Now, opens: null },
    };
    expect(applyOrchestrationV2ProjectionEvent(projection, decode(encode(compact)))?.thread.opens).toBeNull();
    const legacy = { ...compact, type: "thread.visited" as const, payload: projection.thread };
    expect(decode(encode(legacy)).type).toBe("thread.visited");
    expect(applyOrchestrationV2ProjectionEvent(projection, decode(encode(legacy)))?.thread).toEqual(projection.thread);
  });
});
