import { describe, expect, it } from "vite-plus/test";
import { messageFindText } from "./messageFindText";

describe("message find text", () => {
  it("indexes rendered words across inline markup and excludes link destinations", () => {
    expect(
      messageFindText({
        text: "A work**tree** and [setting](https://hidden.example) with `code`.",
      }),
    ).toBe("A worktree and setting with code.");
  });
  it("refreshes cached text when a streamed message changes", () => {
    const message = { text: "First **part**" };
    expect(messageFindText(message)).toBe("First part");
    message.text += " and more";
    expect(messageFindText(message)).toBe("First part and more");
  });
});
