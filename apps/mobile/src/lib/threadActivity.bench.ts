import {
  EventId,
  MessageId,
  TurnId,
  type OrchestrationThread,
  type OrchestrationThreadActivity,
} from "@t3tools/contracts";
import { bench, describe } from "vite-plus/test";

import {
  buildThreadFeed,
  deriveThreadFeedPresentation,
  type ThreadFeedLatestTurn,
} from "./threadActivity";

type FeedThread = Pick<OrchestrationThread, "messages" | "activities">;
type FeedMessage = OrchestrationThread["messages"][number];
type FeedInput = {
  readonly thread: FeedThread;
  readonly options?: Parameters<typeof buildThreadFeed>[1];
};

const epoch = Date.parse("2026-09-01T00:00:00.000Z");
const measurementOptions = {
  warmupTime: 100,
  time: 400,
  warmupIterations: 2,
  iterations: 10,
};

function timestamp(turnIndex: number, second: number): string {
  return new Date(epoch + turnIndex * 10_000 + second * 1_000).toISOString();
}

function makeTurn(index: number) {
  const turnId = TurnId.make(`bench-turn-${index}`);
  const messages: FeedMessage[] = [
    {
      id: MessageId.make(`bench-user-${index}`),
      role: "user",
      text: "Review the synthetic input.",
      turnId,
      streaming: false,
      createdAt: timestamp(index, 0),
      updatedAt: timestamp(index, 0),
    },
    {
      id: MessageId.make(`bench-assistant-first-${index}`),
      role: "assistant",
      text: "Inspecting the synthetic input.",
      turnId,
      streaming: false,
      createdAt: timestamp(index, 3),
      updatedAt: timestamp(index, 3),
    },
    {
      id: MessageId.make(`bench-assistant-final-${index}`),
      role: "assistant",
      text: "Synthetic response. ".repeat(8),
      turnId,
      streaming: false,
      createdAt: timestamp(index, 6),
      updatedAt: timestamp(index, 6),
    },
  ];
  const activities: OrchestrationThreadActivity[] = [
    {
      id: EventId.make(`bench-read-started-${index}`),
      kind: "tool.started",
      tone: "tool",
      summary: "Read synthetic input",
      createdAt: timestamp(index, 1),
      turnId,
      payload: {
        toolCallId: `bench-read-${index}`,
        itemType: "file_read",
        status: "inProgress",
      },
    },
    {
      id: EventId.make(`bench-read-completed-${index}`),
      kind: "tool.completed",
      tone: "tool",
      summary: "Read synthetic input",
      createdAt: timestamp(index, 2),
      turnId,
      payload: {
        toolCallId: `bench-read-${index}`,
        itemType: "file_read",
        status: "completed",
        detail: "Bounded synthetic tool output. ".repeat(4),
      },
    },
    {
      id: EventId.make(`bench-command-${index}`),
      kind: "tool.completed",
      tone: "tool",
      summary: "Check synthetic input",
      createdAt: timestamp(index, 4),
      turnId,
      payload: {
        toolCallId: `bench-command-${index}`,
        itemType: "command_execution",
        command: "synthetic-check --fixture",
        status: "completed",
        detail: "Synthetic check completed.",
      },
    },
    {
      id: EventId.make(`bench-warning-${index}`),
      kind: "runtime.warning",
      tone: "info",
      summary: "Synthetic history notice",
      createdAt: timestamp(index, 5),
      turnId,
      payload: {},
    },
  ];
  return { turnId, messages, activities };
}

function benchmarkBase(name: string, prepare: () => FeedInput) {
  let input = prepare();
  bench(
    `base: ${name}`,
    () => {
      buildThreadFeed(input.thread, input.options);
    },
    {
      ...measurementOptions,
      setup(task) {
        // Tinybench runs this hook before the sample clock. Fresh changed arrays
        // must not become warm WeakMap keys by being reused across iterations.
        task.opts.beforeEach = () => {
          input = prepare();
        };
      },
    },
  );
}

for (const turnCount of [10, 100, 1_000]) {
  describe(`mobile timeline: ${turnCount} turns`, () => {
    const turns = Array.from({ length: turnCount }, (_, index) => makeTurn(index));
    const thread: FeedThread = {
      messages: turns.flatMap((turn) => turn.messages),
      activities: turns.flatMap((turn) => turn.activities),
    };
    const tail = thread.messages.at(-1)!;
    const middleIndex = Math.floor(thread.messages.length / 2);
    const olderTurn = makeTurn(-1);
    const appendedTurn = makeTurn(turnCount);
    const localMessage: FeedMessage = {
      ...appendedTurn.messages[0]!,
      id: MessageId.make("bench-local-user"),
      turnId: null,
    };
    const expandedTurns = new Set(turns.map((turn) => turn.turnId));
    const expandedWorkGroups = new Set(
      turns.flatMap((turn, index) => [
        `work-group:tool:${turn.turnId}:bench-read-${index}`,
        `work-group:tool:${turn.turnId}:bench-command-${index}`,
        `work-group:bench-warning-${index}`,
      ]),
    );
    const collapsedTurns = new Set<TurnId>();
    const collapsedWorkGroups = new Set<string>();
    const latestTurn: ThreadFeedLatestTurn = {
      turnId: turns.at(-1)!.turnId,
      state: "completed",
      startedAt: timestamp(turnCount - 1, 0),
      completedAt: timestamp(turnCount - 1, 6),
    };
    const activeTurn: ThreadFeedLatestTurn = {
      ...latestTurn,
      state: "running",
      completedAt: null,
    };
    const activeActivity: OrchestrationThreadActivity = {
      id: EventId.make("bench-active-tool"),
      kind: "tool.updated",
      tone: "tool",
      summary: "Check synthetic tail",
      createdAt: timestamp(turnCount - 1, 7),
      turnId: activeTurn.turnId,
      payload: {
        toolCallId: "bench-active-tool",
        itemType: "command_execution",
        command: "synthetic-check --tail",
        status: "inProgress",
      },
    };
    // Prime the existing message and activity caches before measuring repeated inputs.
    const feed = buildThreadFeed(thread);
    const activeFeed = buildThreadFeed({
      ...thread,
      activities: [...thread.activities, activeActivity],
    });

    benchmarkBase("unchanged input (warm)", () => ({ thread }));
    benchmarkBase("new message array, same objects", () => ({
      thread: { ...thread, messages: [...thread.messages] },
    }));
    benchmarkBase("new activity array, same objects", () => ({
      thread: { ...thread, activities: [...thread.activities] },
    }));
    benchmarkBase("new message and activity arrays, same objects", () => ({
      thread: { messages: [...thread.messages], activities: [...thread.activities] },
    }));
    benchmarkBase("streamed assistant text replacement", () => ({
      thread: {
        ...thread,
        messages: [
          ...thread.messages.slice(0, -1),
          {
            ...tail,
            text: `${tail.text}Streamed suffix.`,
            streaming: true,
            updatedAt: timestamp(turnCount - 1, 7),
          },
        ],
      },
    }));
    benchmarkBase("append one turn", () => ({
      thread: {
        messages: [...thread.messages, ...appendedTurn.messages],
        activities: [...thread.activities, ...appendedTurn.activities],
      },
    }));
    benchmarkBase("prepend one loaded turn", () => ({
      thread: {
        ...thread,
        activities: [...olderTurn.activities, ...thread.activities],
      },
      options: { loadedMessages: [...olderTurn.messages, ...thread.messages] },
    }));
    benchmarkBase("local user message", () => ({
      thread,
      options: { loadedMessages: thread.messages, localMessages: [localMessage] },
    }));
    benchmarkBase("middle message timestamp correction", () => ({
      thread: {
        ...thread,
        messages: thread.messages.map((message, index) =>
          index === middleIndex ? { ...message, createdAt: timestamp(-1, 0) } : message,
        ),
      },
    }));
    benchmarkBase("revert the last turn", () => ({
      thread: {
        messages: thread.messages.slice(0, -3),
        activities: thread.activities.slice(0, -4),
      },
    }));

    bench(
      "presentation: settled turns collapsed",
      () => {
        deriveThreadFeedPresentation(feed, latestTurn, collapsedTurns, collapsedWorkGroups);
      },
      measurementOptions,
    );
    bench(
      "presentation: turns and work groups expanded",
      () => {
        deriveThreadFeedPresentation(feed, latestTurn, expandedTurns, expandedWorkGroups);
      },
      measurementOptions,
    );
    let expanded = false;
    bench(
      "presentation: alternating fold and work disclosure",
      () => {
        expanded = !expanded;
        deriveThreadFeedPresentation(
          feed,
          latestTurn,
          expanded ? expandedTurns : collapsedTurns,
          expanded ? expandedWorkGroups : collapsedWorkGroups,
        );
      },
      measurementOptions,
    );
    bench(
      "presentation: active tool tail",
      () => {
        deriveThreadFeedPresentation(
          activeFeed,
          activeTurn,
          collapsedTurns,
          collapsedWorkGroups,
          activeTurn.startedAt,
        );
      },
      measurementOptions,
    );
  });
}
