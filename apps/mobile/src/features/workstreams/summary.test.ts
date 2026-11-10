import { expect, it } from "vite-plus/test";
import { loadSummary, projectSummary } from "./summary";
const result = {
  outcome: {
    text: "Summary outcome",
    sources: [{ source_set_id: "source:test", member_ordinal: 0 }],
  },
  semantic_status: "active",
  blockers: [],
  decisions: [],
  next_steps: [
    { text: "Continue", sources: [{ source_set_id: "source:test", member_ordinal: 0 }] },
  ],
  follow_up_candidates: [],
  confidence: 0.8,
};
const read = {
  contract: "workstreams-rollups/1.0.0",
  state: "ready",
  freshness: "stale",
  summary: {
    workstream_id: "ws:test",
    rollup_version: 1,
    manifest_sha256: "a".repeat(64),
    generated_at: "2026-01-01T00:00:00.000Z",
    output_sha256: "b".repeat(64),
    result,
  },
};
it("shows truthful stale summary sections and suppresses foreign or inaccessible content", () => {
  expect(projectSummary("ws:test", read)).toMatchObject({
    state: "ready",
    notice: "This summary may be out of date.",
    sections: [{ title: "Next steps", items: ["Continue"] }],
  });
  for (const value of [
    null,
    { ...read, freshness: "inaccessible" },
    { ...read, state: "provider_unconfigured", summary: null },
    { ...read, contract: "future" },
  ])
    expect(projectSummary("ws:test", value).state).toBe("unavailable");
  expect(projectSummary("ws:other", read).state).toBe("unavailable");
});
it("drops late reads after cancellation and reports unavailable without retaining a last payload", async () => {
  const controller = new AbortController();
  const loaded = await loadSummary(
    "ws:test",
    async () => {
      controller.abort();
      return read;
    },
    controller.signal,
  );
  expect(loaded).toEqual({ state: "unavailable", message: "Summary unavailable." });
  expect(
    (
      await loadSummary(
        "ws:test",
        async () => {
          throw new Error("synthetic");
        },
        new AbortController().signal,
      )
    ).state,
  ).toBe("unavailable");
});
