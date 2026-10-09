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

it.each(["@Bones ", "@Bones R", "@Codex Components"])(
  "keeps pasted and restored query %s editable instead of attaching a file",
  (text) => {
    expect(splitPromptIntoComposerSegments(text)).toEqual([{ type: "text", text }]);
    expect(collectComposerPromptInlineTokens(`${text}\n`, text.length)).toEqual([]);
  },
);

describe.each([detectWebTrigger, detectMobileTrigger])(
  "one-space thread queries (%#)",
  (detect) => {
    it.each(["@Bones ", "@Bones R", "@Bones repo", "@Codex Components", "@骨 組"])(
      "keeps %s active with its entire replacement range",
      (query) => {
        const text = `Please inspect ${query}`;
        expect(detect(text, text.length)).toEqual({
          kind: "path",
          query: query.slice(1),
          rangeStart: 15,
          rangeEnd: text.length,
        });
      },
    );
    it.each(["@Bones repo ", "@Bones  ", "@Bones\tR", "@Bones\nR", "@Bones\rR"])(
      "exits at the second space or a line boundary: %s",
      (text) => {
        expect(detect(text, text.length)).toBeNull();
      },
    );
    it("tracks caret movement and backspace through the first space without consuming surrounding text", () => {
      const text = "Use @Bones repo afterwards";
      for (const prefix of ["Use @Bones", "Use @Bones ", "Use @Bones r", "Use @Bones repo"]) {
        const trigger = detect(text, prefix.length)!;
        expect(trigger.query).toBe(prefix.slice(5));
        expect(replaceTextRange(text, trigger.rangeStart, trigger.rangeEnd, "[thread] ")).toEqual({
          text: `[thread] ${text.slice(prefix.length)}`.replace(/^/, "Use "),
          cursor: 13,
        });
      }
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
