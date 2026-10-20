import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { reorderProviderInstance } from "./providerOrder";

const codex = ProviderDriverKind.make("codex");
const claude = ProviderDriverKind.make("claudeAgent");
const defaultId = defaultInstanceIdForDriver(codex);
const instances: Readonly<Record<string, ProviderInstanceConfig>> = {
  [defaultId]: { driver: codex, displayName: "Default", enabled: true },
  malcolm: { driver: codex, displayName: "Malcolm", config: { binaryPath: "/synthetic/codex" } },
  claude: { driver: claude, displayName: "Claude", enabled: false },
  jace: { driver: codex, displayName: "Jace" },
  steven: { driver: codex, displayName: "Steven" },
  umich: { driver: codex, displayName: "UMICH" },
  wallace: { driver: codex, displayName: "Wallace" },
};

describe("provider order", () => {
  it("reaches the requested custom order while preserving default, other drivers, and values", () => {
    const next = reorderProviderInstance(instances, "umich", "steven");
    expect(Object.keys(next)).toEqual([
      defaultId,
      "malcolm",
      "claude",
      "jace",
      "umich",
      "steven",
      "wallace",
    ]);
    expect(
      Object.values(next)
        .filter((instance) => instance.driver === codex)
        .map((instance) => instance.displayName),
    ).toEqual(["Default", "Malcolm", "Jace", "UMICH", "Steven", "Wallace"]);
    for (const [id, instance] of Object.entries(instances)) expect(next[id]).toBe(instance);
    expect(Object.keys(instances)).toEqual([
      defaultId,
      "malcolm",
      "claude",
      "jace",
      "steven",
      "umich",
      "wallace",
    ]);
  });

  it("moves across other-driver slots without moving those slots and can reverse the change", () => {
    const next = reorderProviderInstance(instances, "jace", "malcolm");
    expect(Object.keys(next)).toEqual([
      defaultId,
      "jace",
      "claude",
      "malcolm",
      "steven",
      "umich",
      "wallace",
    ]);
    expect(Object.entries(reorderProviderInstance(next, "jace", "malcolm"))).toEqual(
      Object.entries(instances),
    );
  });

  it("moves multiple positions by insertion and reverses without disturbing other drivers", () => {
    const next = reorderProviderInstance(instances, "malcolm", "wallace");
    expect(Object.keys(next)).toEqual([
      defaultId,
      "jace",
      "claude",
      "steven",
      "umich",
      "wallace",
      "malcolm",
    ]);
    expect(Object.entries(reorderProviderInstance(next, "malcolm", "jace"))).toEqual(
      Object.entries(instances),
    );
    for (const [id, instance] of Object.entries(instances)) expect(next[id]).toBe(instance);
  });

  it("keeps defaults pinned and rejects unknown, cross-driver and same-row drops", () => {
    for (const [from, to] of [
      [defaultId, "malcolm"],
      ["malcolm", defaultId],
      ["malcolm", "claude"],
      ["missing", "malcolm"],
      ["malcolm", "missing"],
      ["malcolm", "malcolm"],
    ]) {
      expect(reorderProviderInstance(instances, from!, to!)).toBe(instances);
    }
    const empty = {};
    expect(reorderProviderInstance(empty, "missing", "missing")).toBe(empty);
  });

  it("does not move an explicitly stored default even between custom slots", () => {
    const interleaved = {
      first: instances.malcolm!,
      [defaultId]: instances[defaultId]!,
      last: instances.jace!,
    };
    expect(Object.keys(reorderProviderInstance(interleaved, "last", "first"))).toEqual([
      "last",
      defaultId,
      "first",
    ]);
  });
});
