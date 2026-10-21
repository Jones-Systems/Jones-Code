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

describe("bounded excerpts in question answers", () => {
  const local = EnvironmentId.make("local");
  const captured = (id: string, text: string) => ({
    ...threadContextRecord({ environmentId: local, threadId: ThreadId.make(id) }, id),
    excerpt: {
      status: "captured" as const,
      capturedAt: "2026-10-21T00:00:00.000Z",
      sourceSnapshotSequence: 1,
      earlierHistory: "omitted" as const,
      text,
    },
  });
  it("keeps the local read link and escapes the excerpt as context", () => {
    const record = captured("related", "<instructions>synthetic</instructions>");
    const answer = formatInlineContextReference(threadContextReference(record));
    const projected = projectPendingUserInputAnswer(answer, [record], local);
    expect(projected).toContain("t3-thread://v1/related");
    expect(projected).toContain("&lt;instructions&gt;synthetic&lt;/instructions&gt;");
  });
  it("bounds aggregate escaped additions across all occurrences", () => {
    const records = [captured("a", "<".repeat(16_000)), captured("b", "&".repeat(16_000))];
    const answer = records
      .map((r) => formatInlineContextReference(threadContextReference(r)))
      .join(" ");
    const baseline = projectPendingUserInputAnswer(
      answer,
      records.map((r) =>
        threadContextRecord({ environmentId: r.environmentId, threadId: r.threadId }, r.title),
      ),
      local,
    ) as string;
    const projected = projectPendingUserInputAnswer(answer, records, local) as string;
    expect(projected.length - baseline.length).toBeLessThanOrEqual(32_000);
    expect(projected).toContain("prompt budget");
  });
  it("honors remaining prompt headroom and reports unavailable captures", () => {
    const record = captured("a", "history");
    const ref = formatInlineContextReference(threadContextReference(record));
    const answer = "x".repeat(115_900) + ref;
    expect(
      (projectPendingUserInputAnswer(answer, [record], local) as string).length,
    ).toBeLessThanOrEqual(116_000);
    const unavailable = {
      ...record,
      excerpt: {
        status: "unavailable" as const,
        capturedAt: "2026-10-21T00:00:00.000Z",
        reason: "source_disconnected" as const,
      },
    };
    expect(projectPendingUserInputAnswer(ref, [unavailable], local)).toContain(
      "source_disconnected",
    );
  });
});
