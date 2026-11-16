import { ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { codexFailoverSelections, type CodexFailoverCandidate } from "./CodexFailoverSelection.ts";

const selection: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex_A"),
  model: "gpt-6.1-sol",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "priority" },
  ],
};
const source = { driver: "codex", continuationKey: "codex:home:/synthetic/shared" };
const candidate = (
  id: string,
  overrides: Partial<CodexFailoverCandidate> = {},
): CodexFailoverCandidate => ({
  instanceId: ProviderInstanceId.make(id),
  ...source,
  enabled: true,
  projectEnabled: true,
  ...overrides,
});
const select = (
  candidates: ReadonlyArray<CodexFailoverCandidate>,
  visited: ReadonlySet<ProviderInstanceId> = new Set(),
) => codexFailoverSelections({ selection, source, candidates, visited });

describe("Codex failover proposals", () => {
  it("preserves model and explicit options while ordering every eligible account deterministically", () => {
    const accounts = ["codex_E", "codex_C", "codex_A", "codex_D", "codex_B"].map((id) =>
      candidate(id),
    );
    const expected = ["codex_B", "codex_C", "codex_D", "codex_E"].map((instanceId) => ({
      ...selection,
      instanceId,
    }));
    expect(select(accounts)).toEqual(expected);
    expect(select(accounts.toReversed())).toEqual(expected);
    expect(selection.instanceId).toBe("codex_A");
  });

  it("excludes previously tried accounts and duplicate proposals without changing the durable visited set", () => {
    const visited = new Set([ProviderInstanceId.make("codex_B")]);
    expect(
      select([candidate("codex_B"), candidate("codex_C"), candidate("codex_C")], visited),
    ).toEqual([{ ...selection, instanceId: "codex_C" }]);
    expect([...visited]).toEqual(["codex_B"]);
  });

  it("requires current server and project enablement plus a compatible native home", () => {
    expect(
      select([
        candidate("disabled", { enabled: false }),
        candidate("project_disabled", { projectEnabled: false }),
        candidate("claude", { driver: "claudeAgent" }),
        candidate("unknown_home", { continuationKey: null }),
        candidate("separate_home", { continuationKey: "codex:home:/synthetic/independent" }),
        candidate("codex_B"),
      ]),
    ).toEqual([{ ...selection, instanceId: "codex_B" }]);
  });

  it.each([null, "", "   "])(
    "does not infer compatibility from missing source identity: %j",
    (key) => {
      expect(
        codexFailoverSelections({
          selection,
          source: { ...source, continuationKey: key },
          candidates: [candidate("codex_B")],
          visited: new Set(),
        }),
      ).toEqual([]);
    },
  );

  it("leaves Claude routing alone even when a continuation key happens to match", () => {
    expect(
      codexFailoverSelections({
        selection,
        source: { ...source, driver: "claudeAgent" },
        candidates: [candidate("codex_B")],
        visited: new Set(),
      }),
    ).toEqual([]);
  });

  it("retains omitted options and terminates when all compatible accounts have been tried", () => {
    const candidates = [candidate("codex_A"), candidate("codex_B")];
    const inherited = { instanceId: selection.instanceId, model: selection.model };
    const proposed = codexFailoverSelections({
      selection: inherited,
      source,
      candidates,
      visited: new Set(),
    });
    expect(proposed).toEqual([{ instanceId: "codex_B", model: selection.model }]);
    expect(Object.hasOwn(proposed[0]!, "options")).toBe(false);
    expect(select(candidates, new Set(candidates.map((entry) => entry.instanceId)))).toEqual([]);
  });
});
