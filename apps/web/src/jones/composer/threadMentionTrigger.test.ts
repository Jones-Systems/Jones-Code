import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  detectComposerTrigger as detectMobileTrigger,
  replaceTextRange,
} from "@t3tools/shared/composerTrigger";
import { matchComposerThreadItems } from "@t3tools/client-runtime/composerThreadItems";
import { detectComposerTrigger as detectWebTrigger } from "../../composer-logic";
import {
  collectComposerPromptInlineTokens,
  splitPromptIntoComposerSegments,
} from "../../composer-editor-mentions";

it.each([
  "@Bones ",
  "@Bones R",
  "@Codex Components",
  "@code server thread",
  "@code server thread ",
  "Please inspect @code server thread ",
])("keeps pasted and restored query %s editable instead of attaching a file", (text) => {
  expect(splitPromptIntoComposerSegments(text)).toEqual([{ type: "text", text }]);
  expect(collectComposerPromptInlineTokens(`${text}\n`, text.length)).toEqual([]);
});

describe.each([detectWebTrigger, detectMobileTrigger])(
  "single-spaced thread queries (%#)",
  (detect) => {
    it.each([
      "@Bones ",
      "@Bones R",
      "@Bones repo",
      "@Bones repo ",
      "@Codex Components",
      "@骨 組",
      "@code server thread",
      "@code server thread ",
      "@one two three four five",
    ])("keeps %s active with its entire replacement range", (query) => {
      const text = `Please inspect ${query}`;
      expect(detect(text, text.length)).toEqual({
        kind: "path",
        query: query.slice(1),
        rangeStart: 15,
        rangeEnd: text.length,
      });
    });
    it.each([
      "@Bones  ",
      "@Bones repo  ",
      "@code server  thread",
      "@code server thread  afterwards",
      "@Bones\tR",
      "@Bones\nR",
      "@Bones\rR",
      "@Bones\uFFFC R",
    ])("exits at consecutive spaces or a nonspace boundary: %s", (text) => {
      expect(detect(text, text.length)).toBeNull();
    });
    it("tracks typing, caret reentry and backspace without consuming surrounding text", () => {
      const text = "Use @code server thread  afterwards";
      const active = "Use @code server thread ";
      for (let cursor = "Use @".length; cursor <= active.length; cursor += 1) {
        for (const value of [text, text.slice(0, cursor)]) {
          const trigger = detect(value, cursor)!;
          expect(trigger).toEqual({
            kind: "path",
            query: text.slice(5, cursor),
            rangeStart: 4,
            rangeEnd: cursor,
          });
          expect(
            replaceTextRange(value, trigger.rangeStart, trigger.rangeEnd, "[thread] "),
          ).toEqual({
            text: `Use [thread] ${value.slice(cursor)}`,
            cursor: 13,
          });
        }
      }
      const closed = `${active} `;
      expect(detect(closed, closed.length)).toBeNull();
      expect(detect(text, text.length)).toBeNull();
      const reopened = closed.slice(0, -1);
      expect(detect(reopened, reopened.length)?.query).toBe("code server thread ");
    });
    it.each([
      "",
      "Earlier prose has several words ",
      "Earlier @old query  ",
      "Earlier\n",
      "\uFFFC",
    ])("starts at the nearest valid mention after %j", (prefix) => {
      const text = `${prefix}@code server thread `;
      expect(detect(text, text.length)).toEqual({
        kind: "path",
        query: "code server thread ",
        rangeStart: prefix.length,
        rangeEnd: text.length,
      });
    });
    it.each([
      "email@code server thread",
      "Use email@code server thread",
      "@src/file.ts server thread",
      "@src\\file.ts server thread",
      "@file.ts server thread",
      '@"file name" server thread',
      "@ server thread",
    ])("preserves email and first-word file boundaries: %s", (text) => {
      expect(detect(text, text.length)).toBeNull();
    });
    it("preserves other sigils, email boundaries and path queries", () => {
      expect(detect("email@Bones R", 13)).toBeNull();
      expect(detect("@src/file.ts", 12)?.query).toBe("src/file.ts");
      expect(detect("$review ", 8)).toBeNull();
      expect(detect("#123 ", 5)).toBeNull();
      expect(detect("/plan ", 6)).toBeNull();
      expect(detect("@Bones #12", 10)?.kind).toBe("pull-request");
      expect(detect("@Bones $review", 14)?.kind).toBe("skill");
    });
  },
);

it("matches partial words anywhere in titles without confusing duplicate or foreign identities", () => {
  const environmentId = EnvironmentId.make("one-space-test");
  const shell = (id: string, title: string) => ({
    environmentId,
    id: ThreadId.make(id),
    title,
    updatedAt: "2026-10-09T00:00:00Z",
    archivedAt: null,
  });
  const shells = [
    shell("first", "Tommy Bones repo"),
    shell("duplicate", "Tommy Bones repo"),
    shell("codex", "Work on Codex Components"),
    { ...shell("foreign", "Tommy Bones repo"), environmentId: EnvironmentId.make("other") },
    { ...shell("archived", "Tommy Bones repo"), archivedAt: "2026-10-09T00:00:00Z" },
  ];
  for (const query of ["Bones repo", "bONES r", "Bones "]) {
    expect(
      matchComposerThreadItems({
        shells,
        environmentId,
        excludeThreadId: ThreadId.make("first"),
        query,
      }).map((item) => item.thread.threadId),
    ).toEqual(["duplicate"]);
  }
  expect(
    matchComposerThreadItems({
      shells,
      environmentId,
      excludeThreadId: null,
      query: "codex components",
    })[0]?.thread.threadId,
  ).toBe("codex");
});

it.each(["code server thread", "code server thread ", "CODE SERVER THREAD"])(
  "matches multiword thread title query %s",
  (query) => {
    const environmentId = EnvironmentId.make("multiword-test");
    expect(
      matchComposerThreadItems({
        shells: [
          {
            environmentId,
            id: ThreadId.make("server"),
            title: "Work on code server thread",
            updatedAt: "2026-10-09T00:00:00Z",
            archivedAt: null,
          },
        ],
        environmentId,
        excludeThreadId: null,
        query,
      }).map((item) => item.thread.threadId),
    ).toEqual(["server"]);
  },
);
