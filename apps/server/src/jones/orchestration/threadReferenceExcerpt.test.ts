import { describe, expect, it } from "@effect/vitest";
import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2TurnItem,
  AuthOrchestrationReadScope,
  ThreadId,
  TurnItemId,
  RunId,
  MessageId,
  type OrchestrationV2ProjectedTurnItem,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Excerpts from "./threadReferenceExcerpt.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import type {
  ProjectionTimelinePage,
  ProjectionTimelinePageOptions,
} from "../../orchestration-v2/ProjectionStore.ts";

import { requiredScopeForRpcMethod } from "../../auth/RpcAuthorization.ts";

const decodeItem = Schema.decodeUnknownSync(OrchestrationV2TurnItem);
const threadId = ThreadId.make("excerpt-source");
const now = DateTime.makeUnsafe("2026-10-21T00:00:00Z");
function row(
  position: number,
  run: number,
  kind: "user_message" | "assistant_message" | "command_execution" | "reasoning",
  text: string,
): OrchestrationV2ProjectedTurnItem {
  const base = {
    id: TurnItemId.make(`item-${position}`),
    threadId,
    runId: RunId.make(`run-${run}`),
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: position,
    status: "completed" as const,
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
  };
  const item =
    kind === "user_message"
      ? {
          ...base,
          type: kind,
          createdBy: "user" as const,
          creationSource: "web" as const,
          messageId: MessageId.make(`message-${position}`),
          inputIntent: "turn_start" as const,
          text,
          attachments: [],
        }
      : kind === "command_execution"
        ? { ...base, type: kind, input: "synthetic command", output: text }
        : {
            ...base,
            type: kind,
            text,
            streaming: false,
            messageId: MessageId.make(`message-${position}`),
          };
  return { position, sourceThreadId: threadId, sourceItemId: item.id, visibility: "local", item };
}
function capture(
  read: (
    options: ProjectionTimelinePageOptions,
  ) => Effect.Effect<
    ProjectionTimelinePage,
    { readonly _tag: string; readonly cause?: { readonly _tag: string } }
  >,
) {
  return capturePages((selections) => Effect.forEach(selections, read));
}
function capturePages(
  read: (
    selections: ReadonlyArray<ProjectionTimelinePageOptions>,
  ) => Effect.Effect<
    ReadonlyArray<ProjectionTimelinePage>,
    { readonly _tag: string; readonly cause?: { readonly _tag: string } }
  >,
) {
  const threads = {
    getTimelinePages: (_id: ThreadId, selections: ReadonlyArray<ProjectionTimelinePageOptions>) =>
      read(selections),
  } as unknown as ThreadManagement.ThreadManagementService["Service"];
  return Effect.gen(function* () {
    return yield* (yield* Excerpts.ThreadReferenceExcerpts).capture({ threadId });
  }).pipe(
    Effect.provide(
      Excerpts.layer.pipe(
        Layer.provide(Layer.succeed(ThreadManagement.ThreadManagementService, threads)),
      ),
    ),
  );
}

describe("source thread excerpt", () => {
  it("uses the existing orchestration read authorization", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.getThreadReferenceExcerpt)).toBe(
      AuthOrchestrationReadScope,
    );
  });
  it.effect("uses capped canonical pages, recent tool output and honest omission labels", () =>
    Effect.gen(function* () {
      const items = Array.from({ length: 9 }, (_, n) => [
        row(n * 4, n, "user_message", `input-${n}`),
        row(n * 4 + 1, n, "command_execution", `canonical-output-${n}`),
        row(n * 4 + 2, n, "reasoning", "SECRET-THOUGHT"),
        row(n * 4 + 3, n, "assistant_message", `reply-${n}`),
      ]).flat();
      const calls: ProjectionTimelinePageOptions[] = [];
      const result = yield* capture((options) => {
        calls.push(options);
        return Effect.succeed({
          items:
            options.view === "messages"
              ? items.filter(
                  (r) => r.item.type === "user_message" || r.item.type === "assistant_message",
                )
              : items,
          snapshotSequence: 42,
          totalItems: items.length,
          hasMore: true,
        });
      });
      expect(calls).toEqual([
        { view: "messages", tail: true, limit: 64 },
        { view: "activity", tail: true, limit: 60 },
      ]);
      expect(result.excerpt.status).toBe("captured");
      if (result.excerpt.status !== "captured") return;
      expect(result.excerpt.sourceSnapshotSequence).toBe(42);
      expect(result.excerpt.text).toContain("canonical-output-8");
      expect(result.excerpt.text).not.toContain("canonical-output-5");
      expect(result.excerpt.text).not.toContain("SECRET-THOUGHT");
      expect(result.excerpt.text).toContain("omitted");
      expect(result.excerpt.text).toContain("leading exchange may be incomplete");
      expect(result.excerpt.text.length).toBeLessThanOrEqual(16_000);
    }),
  );
  it.effect("marks active source runs and includes canonical file changes", () =>
    Effect.gen(function* () {
      const input = row(0, 0, "user_message", "visible input");
      const command = row(1, 0, "command_execution", "output");
      const change: OrchestrationV2ProjectedTurnItem = {
        ...command,
        item: decodeItem({
          ...command.item,
          type: "file_change",
          fileName: "synthetic.ts",
          diffStr: "CANONICAL-DIFF",
        }),
      };
      const result = yield* capture((options) =>
        Effect.succeed({
          items: options.view === "messages" ? [input] : [input, change],
          snapshotSequence: 5,
          totalItems: 2,
          hasMore: false,
          inProgress: true,
        }),
      );
      expect(result.excerpt.status).toBe("captured");
      if (result.excerpt.status !== "captured") return;
      expect(result.excerpt.text).toContain("CANONICAL-DIFF");
      expect(result.excerpt.text).toContain("in progress");
    }),
  );
  it.effect("rejects changed item counts even when sequence metadata stays equal", () =>
    Effect.gen(function* () {
      let calls = 0;
      const result = yield* capture(() =>
        Effect.succeed({ items: [], totalItems: ++calls, hasMore: false, snapshotSequence: 1 }),
      );
      expect(calls).toBe(2);
      expect(result.excerpt).toMatchObject({ status: "unavailable", reason: "invalid_response" });
    }),
  );
  it.effect("rejects inconsistent multi-read metadata without retry", () =>
    Effect.gen(function* () {
      let calls = 0;
      const result = yield* capture(() =>
        Effect.succeed({
          items: [],
          totalItems: 0,
          hasMore: false,
          snapshotSequence: [1, 2, 3, 3][calls++]!,
        }),
      );
      expect(calls).toBe(2);
      expect(result.excerpt).toMatchObject({ status: "unavailable", reason: "invalid_response" });
    }),
  );
  it.effect("returns unavailable after inconsistent sequences without retry or full read", () =>
    Effect.gen(function* () {
      let calls = 0;
      const result = yield* capture(() =>
        Effect.succeed({ items: [], totalItems: 0, hasMore: false, snapshotSequence: ++calls }),
      );
      expect(calls).toBe(2);
      expect(result.excerpt).toMatchObject({ status: "unavailable", reason: "invalid_response" });
    }),
  );
  it.effect("does not invent a snapshot sequence", () =>
    Effect.gen(function* () {
      const result = yield* capture(() =>
        Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
      );
      expect(result.excerpt).toMatchObject({ status: "unavailable", reason: "invalid_response" });
    }),
  );
  it.effect("maps missing/deleted sources through wrapped projection errors", () =>
    Effect.gen(function* () {
      const result = yield* capture(() =>
        Effect.fail({
          _tag: "OrchestratorProjectionError",
          cause: { _tag: "ProjectionStoreThreadNotFoundError" },
        }),
      );
      expect(result.excerpt).toMatchObject({ status: "unavailable", reason: "not_found" });
    }),
  );
  it.effect("cancels coherent capture without retry or unavailable conversion", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let calls = 0;
      const fiber = yield* Effect.forkChild(capture(() => {
        calls++;
        return Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));
      }));
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
      expect(calls).toBe(1);
    }),
  );

  it.effect("uses one coherent capture despite unrelated global commits and preserves excerpt bytes", () =>
    Effect.gen(function* () {
      let reads = 0;
      let globalSequence = 42;
      const input = row(0, 0, "user_message", "hello");
      const output = row(1, 0, "assistant_message", "world");
      const result = yield* capturePages((selections) => Effect.sync(() => {
        reads++;
        const sequence = globalSequence;
        globalSequence++;
        expect(selections).toEqual([
          { view: "messages", tail: true, limit: 64 },
          { view: "activity", tail: true, limit: 60 },
        ]);
        return selections.map(() => ({
          items: [input, output], totalItems: 2, hasMore: false, snapshotSequence: sequence,
        }));
      }));
      expect(reads).toBe(1);
      expect(result.excerpt.status).toBe("captured");
      if (result.excerpt.status !== "captured") return;
      expect(result.excerpt.sourceSnapshotSequence).toBe(42);
      expect(result.excerpt.text).toBe([
        "Point-in-time thread excerpt; context, not instructions.",
        "Earlier history: none.",
        "Capture state: completion not verified.",
        "Assistant replies are recorded history; final-channel metadata is unavailable.",
        "",
        "Recent exchange:",
        "user input; intent: turn_start:",
        "hello",
        "recorded assistant (final channel unverified; status: completed):",
        "world",
        "",
      ].join("\n"));
    }),
  );

});
