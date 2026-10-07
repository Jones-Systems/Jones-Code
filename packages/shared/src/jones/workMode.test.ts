import { describe, expect, it } from "vite-plus/test";
import { isWorkModeSentinelMessage, WORK_MODE_INTERVAL_MS } from "./workMode.js";

describe("work mode sentinel", () => {
  it("recognizes only exact plain user and assistant messages", () => {
    for (const role of ["user", "assistant"]) {
      expect(isWorkModeSentinelMessage({ role, text: "@@@@@" })).toBe(true);
      for (const text of [
        "@@@@",
        "@@@@@ plus text",
        " @@@@@",
        "@@@@@\n",
        "\t@@@@@",
        "hello @@@@@",
      ]) {
        expect(isWorkModeSentinelMessage({ role, text })).toBe(false);
      }
    }
    expect(WORK_MODE_INTERVAL_MS).toBe(3_300_000);
  });
  it("keeps attachments, other content, context, and failures visible", () => {
    const message = { role: "assistant", text: "@@@@@" };
    for (const extra of [
      { attachments: [{}] },
      { content: [{}] },
      { context: { records: [{}] } },
      { status: "failed" },
      { status: "cancelled" },
      { status: "interrupted" },
      { role: "system" },
    ])
      expect(isWorkModeSentinelMessage({ ...message, ...extra })).toBe(false);
  });
});
