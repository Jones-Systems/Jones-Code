import { describe, expect, it } from "vite-plus/test";
import {
  ChatAttachmentId,
  QUEUED_STEER_BATCH_LIMITS,
  ThreadId,
  type ChatAttachment,
} from "@t3tools/contracts";
import { formatQueuedSteerBatch, planQueuedSteerBatch } from "./queuedSteerBatch.ts";
const row = (text: string, attachments: ReadonlyArray<ChatAttachment> = []) => ({
  eligible: true,
  message: {
    text,
    attachments,
    createdBy: "agent",
    creationSource: "mcp",
    senderThreadId: ThreadId.make("peer"),
  },
});
describe("queued steer batch budget", () => {
  it("retains 22 messages in order with readable provenance and exact payload accounting", () => {
    const rows = Array.from({ length: 22 }, (_, index) => row(`original ${index}`));
    const plan = planQueuedSteerBatch(rows);
    expect(plan.count).toBe(22);
    expect(plan.text).toBe(formatQueuedSteerBatch(rows.map((entry) => entry.message)).text);
    expect(plan.text.indexOf("original 20")).toBeLessThan(plan.text.indexOf("original 21"));
    expect(plan.text).toContain("sender: thread peer; source: mcp");
  });
  it("includes every wrapper in the boundary and never truncates", () => {
    const rows = [row("first"), row("second")];
    const exact = formatQueuedSteerBatch(rows.map((entry) => entry.message)).text.length;
    expect(
      planQueuedSteerBatch(rows, { ...QUEUED_STEER_BATCH_LIMITS, maxInputChars: exact }).count,
    ).toBe(2);
    expect(
      planQueuedSteerBatch(rows, { ...QUEUED_STEER_BATCH_LIMITS, maxInputChars: exact - 1 }).count,
    ).toBe(1);
    const oversized = planQueuedSteerBatch([row("x".repeat(120_000))]);
    expect(oversized.reason).toBe("first_member_over_budget");
    expect(oversized.count).toBe(0);
    expect(oversized.text).toBe("");
  });
  it("enforces attachment and image budgets across original messages", () => {
    const attachment: ChatAttachment = {
      id: ChatAttachmentId.make("image"),
      type: "image",
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 10 * 1024 * 1024,
    };
    expect(
      planQueuedSteerBatch(Array.from({ length: 9 }, () => row("image", [attachment]))).count,
    ).toBe(8);
    const file: ChatAttachment = {
      ...attachment,
      type: "file",
      mimeType: "text/plain",
      sizeBytes: 1,
    };
    const plan = planQueuedSteerBatch([
      row(
        "first",
        Array.from({ length: 100 }, () => file),
      ),
      row("second", [file]),
    ]);
    expect(plan.count).toBe(1);
    expect(plan.attachments).toHaveLength(100);
  });
  it("stops at eligibility and slash-command boundaries without skipping originals", () => {
    expect(
      planQueuedSteerBatch([row("first"), { ...row("ineligible"), eligible: false }, row("third")])
        .count,
    ).toBe(1);
    expect(planQueuedSteerBatch([row("first"), row("/skill")]).count).toBe(1);
    const slash = planQueuedSteerBatch([row("/skill"), row("later")]);
    expect(slash.count).toBe(1);
    expect(slash.text).toBe("/skill");
  });
});
