import { expect, it } from "vite-plus/test";
import { render } from "../../../test/render";
import type { WorkstreamsRollupRead } from "@t3tools/contracts/jones/workstreamsRollups";
import { WorkstreamSummaryPanel } from "./WorkstreamSummaryPanel";
const read: typeof WorkstreamsRollupRead.Type = {
  contract: "workstreams-rollups/1.0.0",
  state: "ready",
  freshness: "stale",
  summary: {
    workstream_id: "ws:test",
    rollup_version: 1,
    manifest_sha256: "a".repeat(64),
    generated_at: "2026-01-01T00:00:00.000Z",
    output_sha256: "b".repeat(64),
    result: {
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
    },
  },
};
it("presents summary, next steps, freshness and the observation time", async () => {
  await using _screen = await render(<WorkstreamSummaryPanel workstreamId="ws:test" read={read} />);
  expect(document.body.textContent).toContain("Summary outcome");
  expect(document.body.textContent).toContain("This summary may be out of date.");
  expect(document.body.textContent).toContain("Next steps");
  expect(document.body.textContent).toContain("EST");
});
it("does not present content for another Workstream or inaccessible reads", async () => {
  await using _screen = await render(
    <WorkstreamSummaryPanel workstreamId="ws:other" read={read} />,
  );
  expect(document.body.textContent).not.toContain("Summary outcome");
  expect(document.body.textContent).toContain("Summary unavailable.");
});
