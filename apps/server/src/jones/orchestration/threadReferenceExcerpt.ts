import type { ThreadId, OrchestrationV2TurnItem } from "@t3tools/contracts";
import type { ThreadReferenceExcerpt } from "@t3tools/contracts/jones/threadReferenceExcerpt";
import {
  buildThreadReferenceExcerpt,
  type ThreadReferenceExcerptItem,
} from "@t3tools/shared/jones/threadReferenceExcerpt";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import { projectTurnItemForDetail } from "../../orchestration-v2/WireProjection.ts";

export class ThreadReferenceExcerpts extends Context.Service<
  ThreadReferenceExcerpts,
  {
    readonly capture: (input: { readonly threadId: ThreadId }) => Effect.Effect<{
      readonly threadId: ThreadId;
      readonly excerpt: ThreadReferenceExcerpt;
    }>;
  }
>()("t3/jones/orchestration/threadReferenceExcerpt/ThreadReferenceExcerpts") {}

function excerptItem(raw: OrchestrationV2TurnItem): ThreadReferenceExcerptItem {
  const item =
    raw.type === "file_change"
      ? {
          ...projectTurnItemForDetail(raw),
          type: "file_change" as const,
          fileName: raw.fileName,
          additions: raw.additions,
          deletions: raw.deletions,
          diffStr:
            raw.diffStr === undefined
              ? undefined
              : raw.diffStr.slice(0, 4_000) +
                (raw.diffStr.length > 4_000 ? "\n[diff truncated]" : ""),
        }
      : projectTurnItemForDetail(raw);
  const base = { type: item.type, runId: item.runId, status: item.status, title: item.title };
  switch (item.type) {
    case "user_message":
      return {
        ...base,
        text: item.text,
        inputIntent: item.inputIntent,
        createdBy: item.createdBy,
        ...(item.senderThreadId ? { senderThreadId: item.senderThreadId } : {}),
        ...(item.scheduledTaskId ? { scheduledTaskId: item.scheduledTaskId } : {}),
      };
    case "assistant_message":
      return { ...base, text: item.text, streaming: item.streaming };
    case "command_execution":
      return { ...base, input: item.input, output: item.output };
    case "dynamic_tool":
      return { ...base, toolName: item.toolName, input: item.input, output: item.output };
    case "file_change":
      return {
        ...base,
        input: { fileName: item.fileName },
        output: { diff: item.diffStr, additions: item.additions, deletions: item.deletions },
      };
    case "file_search":
      return { ...base, input: item.pattern, output: item.results };
    case "web_search":
      return { ...base, input: item.patterns, output: item.results };
    case "subagent":
      return {
        ...base,
        input: item.prompt,
        output: { progress: item.progress, result: item.result },
      };
    case "approval_request":
      return { ...base, input: { requestKind: item.requestKind, prompt: item.prompt } };
    case "user_input_request":
      return { ...base, input: item.questions };
    case "error":
      return { ...base, output: item.failure };
    case "proposed_plan":
      return { ...base, title: item.markdown.slice(0, 300) };
    case "todo_list":
      return { ...base, title: item.explanation?.slice(0, 300) ?? item.title };
    default:
      return base;
  }
}

function isNotFound(error: unknown): boolean {
  let current = error;
  for (let depth = 0; depth < 4 && current !== null && typeof current === "object"; depth++) {
    if (
      "_tag" in current &&
      typeof current._tag === "string" &&
      current._tag.endsWith("NotFoundError")
    )
      return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
}

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const capture = (input: { readonly threadId: ThreadId }) =>
    Effect.gen(function* () {
      const capturedAt = DateTime.formatIso(yield* DateTime.now);
      const unavailable = (reason: "not_found" | "invalid_response") => ({
        threadId: input.threadId,
        excerpt: { status: "unavailable" as const, capturedAt, reason },
      });
      for (let attempt = 0; attempt < 2; attempt++) {
        const messages = yield* threads.getTimelinePage(input.threadId, {
          view: "messages",
          tail: true,
          limit: 64,
        });
        const activity = yield* threads.getTimelinePage(input.threadId, {
          view: "activity",
          tail: true,
          limit: 60,
        });
        if (messages.snapshotSequence === undefined || activity.snapshotSequence === undefined)
          return unavailable("invalid_response");
        if (
          messages.snapshotSequence !== activity.snapshotSequence ||
          messages.totalItems !== activity.totalItems
        )
          continue;
        const groups: Array<{ runId: string | null; position: number }> = [];
        const seenRuns = new Set<string>();
        for (const row of messages.items) {
          if (row.item.type !== "user_message") continue;
          if (row.item.runId && seenRuns.has(row.item.runId)) continue;
          if (row.item.runId) seenRuns.add(row.item.runId);
          groups.push({ runId: row.item.runId, position: row.position });
        }
        const recent = new Set(
          groups.slice(-3).flatMap((group) => (group.runId ? [group.runId] : [])),
        );
        const legacyPositions = groups
          .slice(-3)
          .filter((group) => !group.runId)
          .map((group) => group.position);
        const merged = new Map(messages.items.map((row) => [row.sourceItemId, row]));
        for (const row of activity.items) {
          if (row.item.type === "reasoning") continue;
          if (
            row.item.runId
              ? recent.has(row.item.runId)
              : legacyPositions.some(
                  (start) =>
                    row.position >= start &&
                    !groups.some(
                      (group) => group.position > start && group.position <= row.position,
                    ),
                )
          )
            merged.set(row.sourceItemId, row);
        }
        const rows = [...merged.values()].sort((a, b) => a.position - b.position);
        const excerpt = buildThreadReferenceExcerpt({
          items: rows.map((row) => excerptItem(row.item)),
          capturedAt,
          sourceSnapshotSequence: messages.snapshotSequence,
          earlierHistory: messages.hasMore ? "omitted" : "none",
          inProgress:
            messages.inProgress ||
            activity.inProgress ||
            rows.some(
              ({ item }) =>
                ("streaming" in item && item.streaming) ||
                ["running", "pending", "waiting"].includes(item.status),
            ),
          leadingPartial: messages.hasMore,
          toolActivityOmitted: activity.hasMore,
        });
        return { threadId: input.threadId, excerpt };
      }
      return unavailable("invalid_response");
    }).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          return {
            threadId: input.threadId,
            excerpt: {
              status: "unavailable" as const,
              capturedAt: DateTime.formatIso(yield* DateTime.now),
              reason: isNotFound(error) ? ("not_found" as const) : ("invalid_response" as const),
            },
          };
        }),
      ),
    );
  return ThreadReferenceExcerpts.of({ capture });
});

export const layer = Layer.effect(ThreadReferenceExcerpts, make);
