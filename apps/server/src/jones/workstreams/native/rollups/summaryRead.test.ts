import { expect, it } from "vite-plus/test";
import { validateAuthorizations } from "./summaryRead.ts";
const m = {
  owner_id: "owner:test",
  workstream_id: "ws:test",
  rollup_version: 1,
  manifest_sha256: "a".repeat(64),
  output_sha256: "b".repeat(64),
  account_ids: ["account:a", "account:b"],
  coverage: "complete" as const,
  freshness: "current" as const,
};
const decisions = m.account_ids.map((account_id) => ({
  principal_id: "principal:test",
  owner_id: m.owner_id,
  workstream_id: m.workstream_id,
  account_id,
  decision_id: "decision:" + account_id,
  authorization_revision: 1,
  allowed: true,
}));
it("pins every account decision and revision across the private payload read", () => {
  validateAuthorizations("principal:test", m, decisions, decisions);
  for (const changed of [
    decisions.toReversed(),
    decisions.slice(0, 1),
    [decisions[0]!, { ...decisions[1]!, decision_id: "different" }],
    [decisions[0]!, { ...decisions[1]!, authorization_revision: 2 }],
    [decisions[0]!, { ...decisions[1]!, principal_id: "other" }],
    [decisions[0]!, { ...decisions[1]!, owner_id: "other" }],
    [decisions[0]!, { ...decisions[1]!, allowed: false }],
  ])
    expect(() => validateAuthorizations("principal:test", m, changed, decisions)).toThrow();
});
