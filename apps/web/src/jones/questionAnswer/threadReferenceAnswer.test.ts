import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { threadContextRecord, threadContextReference } from "../../lib/composerContextRecords";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { projectPendingUserInputAnswer } from "./threadReferenceAnswer";

describe("thread reference answers", () => {
  const local = EnvironmentId.make("local");
  const foreign = threadContextRecord(
    { environmentId: EnvironmentId.make("remote"), threadId: ThreadId.make("same-id") },
    "Foreign title",
  );
  const record = threadContextRecord(
    { environmentId: local, threadId: ThreadId.make("other-id") },
    "Local title",
  );
  it("preserves foreign identity without linking to a local thread or including history", () => {
    const answer = `Compare ${formatInlineContextReference(threadContextReference(foreign))} and ${formatInlineContextReference(threadContextReference(record))}`;
    expect(projectPendingUserInputAnswer(answer, [foreign, record], local)).toBe(
      "Compare Foreign title (threadId: same-id; environmentId: remote; reference only, no history attached; unavailable through this environment's t3_thread_read). and [Local title](t3-thread://v1/other-id)",
    );
  });
  it("leaves missing references and literal choices unchanged", () => {
    const text = formatInlineContextReference(threadContextReference(foreign));
    expect(projectPendingUserInputAnswer(text, [record], local)).toBe(text);
    const choices = [" first ", "second"];
    expect(projectPendingUserInputAnswer(choices, [foreign], local)).toBe(choices);
  });
});
