import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { matchComposerThreadItems } from "../composerThreadItems.ts";

const environmentId = EnvironmentId.make("env-1");
const shell = (
  id: string,
  title: string,
  overrides: Partial<Parameters<typeof matchComposerThreadItems>[0]["shells"][number]> = {},
) => ({
  environmentId,
  id: ThreadId.make(id),
  title,
  updatedAt: "2026-01-01T00:00:00.000Z",
  archivedAt: null,
  ...overrides,
});

function matches(
  query: string,
  shells: Parameters<typeof matchComposerThreadItems>[0]["shells"],
  excludeThreadId: ThreadId | null = null,
) {
  return matchComposerThreadItems({ shells, environmentId, excludeThreadId, query }).map(
    (item) => item.thread.threadId,
  );
}

describe("Jones composer thread matching", () => {
  it("finds a conversation when query words appear in a different order", () => {
    const items = matchComposerThreadItems({
      shells: [
        {
          environmentId,
          id: ThreadId.make("space-handling"),
          title: "fix @mention space handling",
          updatedAt: "2026-10-17T00:00:00.000Z",
          archivedAt: null,
        },
      ],
      environmentId,
      excludeThreadId: null,
      query: "fix space mention",
    });

    expect(items.map((item) => item.thread.threadId)).toEqual(["space-handling"]);
  });

  it("normalizes case, punctuation and whitespace on both sides", () => {
    expect(
      matches("  SPACE / FIX @MENTION  ", [shell("punctuation", "Fix: @mention-space handling")]),
    ).toEqual(["punctuation"]);
  });

  it("keeps Unicode title words searchable", () => {
    expect(matches("révision CAFÉ", [shell("unicode", "Café — révision")])).toEqual(["unicode"]);
  });

  it("accepts reordered partial words and legacy single-word substrings", () => {
    expect(matches("men spa", [shell("partial", "Space @mention handling")])).toEqual(["partial"]);
    expect(matches("gin", [shell("substring", "Login flow")])).toEqual(["substring"]);
  });

  it("requires every query term and does not guess misspelled words", () => {
    const shells = [shell("title", "Fix @mention space handling")];
    expect(matches("fix space missing", shells)).toEqual([]);
    expect(matches("fiz space mention", shells)).toEqual([]);
  });

  it.each(["", "   ", "@ / - !!!"])("offers nothing for an empty word query %j", (query) => {
    expect(matches(query, [shell("title", "Fix @mention space handling")])).toEqual([]);
  });

  it("ranks stronger title matches ahead of more recently active weaker matches", () => {
    expect(
      matches("fix space mention", [
        shell("substring", "prefix aerospace aforementioned", {
          updatedAt: "2026-05-01T00:00:00.000Z",
        }),
        shell("prefix", "fixing spaced mentioning", { updatedAt: "2026-04-01T00:00:00.000Z" }),
        shell("words", "fix @mention space handling", { updatedAt: "2026-03-01T00:00:00.000Z" }),
        shell("phrase", "Please fix space mention now", { updatedAt: "2026-02-01T00:00:00.000Z" }),
        shell("exact", "fix space mention"),
      ]),
    ).toEqual(["exact", "phrase", "words", "prefix", "substring"]);
  });

  it("ranks complete unordered words above an incomplete phrase", () => {
    expect(
      matches("fix space mention", [
        shell("partial-phrase", "fix space mentioning", {
          updatedAt: "2026-02-01T00:00:00.000Z",
        }),
        shell("complete-words", "fix @mention space handling"),
      ]),
    ).toEqual(["complete-words", "partial-phrase"]);
  });

  it("uses recent activity within equal relevance and preserves equal-activity order", () => {
    expect(
      matches("fix space mention", [
        shell("old", "Fix @mention space handling"),
        shell("new-first", "Space handling fix mention", {
          updatedAt: "2026-02-01T00:00:00.000Z",
        }),
        shell("new-second", "Mention handling space fix", {
          updatedAt: "2026-02-01T00:00:00.000Z",
        }),
      ]),
    ).toEqual(["new-first", "new-second", "old"]);
  });

  it("excludes self, archived and other environments before limiting matches", () => {
    expect(
      matches(
        "fix space mention",
        [
          shell("self", "fix space mention"),
          shell("archived", "fix space mention", { archivedAt: "2026-02-01T00:00:00.000Z" }),
          shell("foreign", "fix space mention", { environmentId: EnvironmentId.make("env-2") }),
          shell("eligible", "fix @mention space handling"),
        ],
        ThreadId.make("self"),
      ),
    ).toEqual(["eligible"]);
  });

  it("returns five matches after ranking, including a stronger late candidate", () => {
    expect(
      matches("fix space mention", [
        ...Array.from({ length: 6 }, (_, index) =>
          shell(`partial-${index}`, "fixing spaced mentioning"),
        ),
        shell("exact", "fix space mention"),
      ]),
    ).toEqual(["exact", "partial-0", "partial-1", "partial-2", "partial-3"]);
  });
});
