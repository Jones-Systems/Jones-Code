import { describe, expect, it } from "@effect/vitest";

import { composeAgentMessage } from "./agentMessageHeader.ts";

const acceptedAt = Date.parse("2026-10-20T14:50:00Z");
const sender = { id: "thread:sender%3A1", title: "Source thread" };

describe("native agent message header", () => {
  it("adds a distinct header and exact thread reference without changing or truncating the body", () => {
    const body = `Summary ready.\n- First point\n- Second point\n\n${"Full detail. ".repeat(200)}`;
    expect(composeAgentMessage({ body, sender, acceptedAt })).toBe(
      `**T3M** · From [Source thread](t3-thread://v1/thread:sender%3A1) · Sent: 10/20 10:50:00\n\n${body}`,
    );
  });

  it("escapes Markdown and HTML in the sender title and collapses title newlines", () => {
    expect(
      composeAgentMessage({
        body: "Body <unchanged> & **bold**",
        sender: { ...sender, title: "[Plan](bad) <script> & *go* `code` \\ path\r\nnext" },
        acceptedAt,
      }),
    ).toBe(
      "**T3M** · From [\\[Plan\\]\\(bad\\) &lt;script&gt; &amp; \\*go\\* \\`code\\` \\\\ path next](t3-thread://v1/thread:sender%3A1) · Sent: 10/20 10:50:00\n\nBody <unchanged> & **bold**",
    );
  });

  it.each([
    ["2026-07-04T16:05:06Z", "07/04 12:05:06"],
    ["2026-01-04T17:05:06Z", "01/04 12:05:06"],
    ["2026-03-08T06:59:59Z", "03/08 01:59:59"],
    ["2026-03-08T07:00:00Z", "03/08 03:00:00"],
    ["2026-11-01T05:59:59Z", "11/01 01:59:59"],
    ["2026-11-01T06:00:00Z", "11/01 01:00:00"],
    ["2026-12-02T05:00:00Z", "12/02 00:00:00"],
  ])("formats server time %s in America/New_York", (instant, expected) => {
    expect(
      composeAgentMessage({ body: "Summary", sender, acceptedAt: Date.parse(instant) }),
    ).toContain(` · Sent: ${expected}\n\nSummary`);
  });

  it("reports an absent sender without inventing a title or thread reference", () => {
    expect(composeAgentMessage({ body: "Summary", acceptedAt })).toBe(
      "**T3M** · From unavailable · Sent: 10/20 10:50:00\n\nSummary",
    );
  });

  it.each([undefined, null, "", "\r\n  "])(
    "keeps the real sender reference when its title is %s",
    (title) => {
      expect(
        composeAgentMessage({
          body: "Summary",
          sender: title === undefined ? { id: sender.id } : { ...sender, title },
          acceptedAt,
        }),
      ).toContain("From [Untitled thread](t3-thread://v1/thread:sender%3A1)");
    },
  );

  it.each(["/compact", "  /compact  ", "/model next", "\n/command detail\nmore"])(
    "preserves leading-slash command bytes: %s",
    (body) => {
      expect(composeAgentMessage({ body, sender, acceptedAt })).toBe(body);
    },
  );
});
