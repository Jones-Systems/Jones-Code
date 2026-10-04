import { describe, expect, it } from "vite-plus/test";
import { MessageId, NodeId, ProviderDriverKind, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { makeAssistantStreamingFilter, splitBufferedAssistantText } from "./assistantStreaming.ts";
import type { ProviderAdapterV2Event } from "./ProviderAdapter.ts";

const message = (text: string, streaming = true): ProviderAdapterV2Event => ({
  type: "message.updated",
  driver: ProviderDriverKind.make("codex"),
  message: {
    id: MessageId.make("message"),
    threadId: ThreadId.make("thread"),
    runId: null,
    nodeId: null,
    createdBy: "agent",
    creationSource: "provider",
    updatedAt: DateTime.makeUnsafe("2026-09-14T00:00:00Z"),
    role: "assistant",
    text,
    attachments: [],
    createdAt: DateTime.makeUnsafe("2026-09-14T00:00:00Z"),
    streaming,
  },
});

const turnItem = (
  text: string,
  streaming = true,
  type: "assistant_message" | "reasoning" = "assistant_message",
): ProviderAdapterV2Event => ({
  type: "turn_item.updated",
  driver: ProviderDriverKind.make("codex"),
  turnItem: {
    id: TurnItemId.make("item"),
    threadId: ThreadId.make("thread"),
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: streaming ? "running" : "completed",
    title: null,
    startedAt: null,
    completedAt: null,
    updatedAt: DateTime.makeUnsafe("2026-09-14T00:00:00Z"),
    ...(type === "assistant_message" ? { type, messageId: MessageId.make("message") } : { type }),
    text,
    streaming,
  },
});

describe("V2 assistant streaming", () => {
  it("delivers completed paragraphs, coalesces rapid updates, and flushes final text", () => {
    const filter = makeAssistantStreamingFilter("paragraph");
    expect(filter(message("First"), 0)).toBeNull();
    expect(filter(message("First\n\nSec"), 10)).toMatchObject({ message: { text: "First\n\n" } });
    expect(filter(message("First\n\nSecond\n\nThi"), 100)).toBeNull();
    expect(filter(message("First\n\nSecond\n\nThird"), 410)).toMatchObject({
      message: { text: "First\n\nSecond\n\n" },
    });
    const final = message("First\n\nSecond\n\nThird", false);
    expect(filter(final, 420)).toBe(final);
  });
  it("keeps code fences intact", () => {
    expect(splitBufferedAssistantText("Intro\n\n```ts\nx()\n\n")).toEqual({
      ready: "Intro\n\n",
      rest: "```ts\nx()\n\n",
    });
    expect(splitBufferedAssistantText("```ts\nx()\n```\nrest")).toEqual({
      ready: "```ts\nx()\n```\n",
      rest: "rest",
    });
  });
  it("holds streaming text until the full response completes", () => {
    const running = message("partial");
    const final = message("complete", false);
    const buffered = makeAssistantStreamingFilter("turn");
    expect(buffered(running, 0)).toBeNull();
    expect(buffered(message("First\n\nSecond\n\n"), 500)).toBeNull();
    expect(buffered(final, 501)).toBe(final);
  });

  it.each(["assistant_message", "reasoning"] as const)(
    "buffers %s at paragraph boundaries and flushes the final item",
    (type) => {
      const item = (text: string, streaming = true) => turnItem(text, streaming, type);
      const filter = makeAssistantStreamingFilter("paragraph");
      expect(filter(item("First"), 0)).toBeNull();
      expect(filter(item("First\n\nSec"), 10)).toMatchObject({
        turnItem: { text: "First\n\n", streaming: true, type },
      });
      expect(filter(item("First\n\nSecond\n\nThi"), 100)).toBeNull();
      expect(filter(item("First\n\nSecond\n\nThi"), 410)).toMatchObject({
        turnItem: { text: "First\n\nSecond\n\n", streaming: true },
      });
      const final = item("First\n\nSecond\n\nThird", false);
      expect(filter(final, 420)).toBe(final);

      const buffered = makeAssistantStreamingFilter("turn");
      expect(buffered(item("First\n\nSecond"), 0)).toBeNull();
      expect(buffered(final, 1)).toBe(final);
    },
  );

  it.each(["turn", "paragraph"] as const)(
    "suppresses running assistant nodes in %s mode while delivering tool and completed nodes",
    (mode) => {
      const filter = makeAssistantStreamingFilter(mode);
      const running: Extract<ProviderAdapterV2Event, { type: "node.updated" }> = {
        type: "node.updated",
        driver: ProviderDriverKind.make("codex"),
        node: {
          id: NodeId.make("node"),
          threadId: ThreadId.make("thread"),
          runId: null,
          parentNodeId: null,
          rootNodeId: NodeId.make("root"),
          kind: "assistant_message",
          status: "running",
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: null,
          completedAt: null,
        },
      };
      expect(filter(running, 0)).toBeNull();
      const completed = { ...running, node: { ...running.node, status: "completed" as const } };
      expect(filter(completed, 1)).toBe(completed);
      const tool = { ...running, node: { ...running.node, kind: "tool_call" as const } };
      expect(filter(tool, 2)).toBe(tool);
    },
  );
});

it("holds a streamed section heading until its content has a boundary", () => {
  expect(splitBufferedAssistantText("Intro\n\n## Results\n\n")).toEqual({
    ready: "Intro\n\n",
    rest: "## Results\n\n",
  });
  expect(splitBufferedAssistantText("**Results**\n\nBody\n\nNext")).toEqual({
    ready: "**Results**\n\nBody\n\n",
    rest: "Next",
  });
});

describe("splitBufferedAssistantText", () => {
  it("keeps a partial trailing line buffered", () => {
    expect(splitBufferedAssistantText("one\n\ntwo")).toEqual({ ready: "one\n\n", rest: "two" });
    expect(splitBufferedAssistantText("one\ntwo")).toEqual({ ready: "", rest: "one\ntwo" });
  });

  it("does not split inside an open fence and delivers the block at its closing fence", () => {
    const open = "intro\n\n```\ncode\n\nmore\n";
    expect(splitBufferedAssistantText(open)).toEqual({
      ready: "intro\n\n",
      rest: "```\ncode\n\nmore\n",
    });
    expect(splitBufferedAssistantText(`${open}\`\`\`\nafter`)).toEqual({
      ready: `${open}\`\`\`\n`,
      rest: "after",
    });
  });

  it("does not treat a fence with an info string as a closing fence", () => {
    const text = "```\n```javascript\nstill code\n\nmore\n";
    expect(splitBufferedAssistantText(text)).toEqual({ ready: "", rest: text });
  });

  it("treats a fence indented four or more spaces as code, not a closing fence", () => {
    const text = "```\n    ```\n\nstill code\n";
    expect(splitBufferedAssistantText(text)).toEqual({ ready: "", rest: text });
    expect(splitBufferedAssistantText("```\n   ```\nafter")).toEqual({
      ready: "```\n   ```\n",
      rest: "after",
    });
  });

  it("keeps a fence nested under a list item open across its blank lines", () => {
    const text = "- step\n\n    ```ts\n    a\n\n    b\n    ```\n\nafter\n";
    expect(splitBufferedAssistantText(text)).toEqual({
      ready: "- step\n\n    ```ts\n    a\n\n    b\n    ```\n\n",
      rest: "after\n",
    });
  });

  it("does not treat a no-break-space line as blank", () => {
    expect(splitBufferedAssistantText("para\n\u00a0\ncont\n\nnext")).toEqual({
      ready: "para\n\u00a0\ncont\n\n",
      rest: "next",
    });
  });

  it("treats CRLF blank lines as boundaries", () => {
    expect(splitBufferedAssistantText("one\r\n\r\ntwo")).toEqual({
      ready: "one\r\n\r\n",
      rest: "two",
    });
  });

  it("only closes a fence with the same marker of equal or greater length", () => {
    const text = "````\n```\nstill code\n\n````\n\nout\n";
    expect(splitBufferedAssistantText(text)).toEqual({
      ready: "````\n```\nstill code\n\n````\n\n",
      rest: "out\n",
    });
    expect(splitBufferedAssistantText("~~~\n```\n\nx\n")).toEqual({
      ready: "",
      rest: "~~~\n```\n\nx\n",
    });
  });

  it("delivers tight list items one at a time", () => {
    expect(splitBufferedAssistantText("## Steps\n\n- one\n- two\n- thr")).toEqual({
      ready: "## Steps\n\n- one\n- two\n",
      rest: "- thr",
    });
    expect(splitBufferedAssistantText("1. one\n2. two\n   more\n3. t")).toEqual({
      ready: "1. one\n2. two\n   more\n",
      rest: "3. t",
    });
  });

  it("keeps a partial list marker and list-like code buffered", () => {
    expect(splitBufferedAssistantText("intro\n-")).toEqual({ ready: "", rest: "intro\n-" });
    expect(splitBufferedAssistantText("intro\n1.")).toEqual({ ready: "", rest: "intro\n1." });
    // `intro\n- \n` would parse as a setext heading, so a bare marker with only
    // trailing whitespace is not a boundary on the partial line either.
    expect(splitBufferedAssistantText("intro\n- ")).toEqual({ ready: "", rest: "intro\n- " });
    expect(splitBufferedAssistantText("- one\n")).toEqual({ ready: "", rest: "- one\n" });
    expect(splitBufferedAssistantText("```\n- one\n- two\n")).toEqual({
      ready: "",
      rest: "```\n- one\n- two\n",
    });
  });

  it("holds a heading until the block under it is done", () => {
    expect(splitBufferedAssistantText("intro\n\n## Setup\n\nInstall it")).toEqual({
      ready: "intro\n\n",
      rest: "## Setup\n\nInstall it",
    });
    expect(
      splitBufferedAssistantText("intro\n\n# Plan\n\n## Setup\n\nInstall it.\n\nNext"),
    ).toEqual({
      ready: "intro\n\n# Plan\n\n## Setup\n\nInstall it.\n\n",
      rest: "Next",
    });
  });

  it("delivers the paragraph above a heading with no blank line between them", () => {
    expect(splitBufferedAssistantText("para\n## Setup\n\nInstall")).toEqual({
      ready: "para\n",
      rest: "## Setup\n\nInstall",
    });
    // A bold line there continues the paragraph, so both stay buffered.
    expect(splitBufferedAssistantText("para\n**Setup**\n\nInstall")).toEqual({
      ready: "",
      rest: "para\n**Setup**\n\nInstall",
    });
  });

  it("holds a line of only bold text like a heading", () => {
    expect(splitBufferedAssistantText("**Risk by area:**\n\n| a |\n|---|\n")).toEqual({
      ready: "",
      rest: "**Risk by area:**\n\n| a |\n|---|\n",
    });
    expect(splitBufferedAssistantText("**Use *npm* now**\n\nInstall it")).toEqual({
      ready: "",
      rest: "**Use *npm* now**\n\nInstall it",
    });
    expect(splitBufferedAssistantText("**Note:** read this.\n\nNext")).toEqual({
      ready: "**Note:** read this.\n\n",
      rest: "Next",
    });
  });

  it("delivers a held heading with its first list item or its whole code block", () => {
    expect(splitBufferedAssistantText("## Steps\n\n- one\n- tw")).toEqual({
      ready: "## Steps\n\n- one\n",
      rest: "- tw",
    });
    expect(splitBufferedAssistantText("## Code\n\n```ts\na\n\nb\n")).toEqual({
      ready: "",
      rest: "## Code\n\n```ts\na\n\nb\n",
    });
    expect(splitBufferedAssistantText("## Code\n\n```ts\na\n```\nafter")).toEqual({
      ready: "## Code\n\n```ts\na\n```\n",
      rest: "after",
    });
  });
});
