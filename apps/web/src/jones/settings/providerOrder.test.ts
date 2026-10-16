import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { moveProviderInstance } from "./providerOrder";

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
    const next = moveProviderInstance(instances, "umich", "up");
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
    const next = moveProviderInstance(instances, "jace", "up");
    expect(Object.keys(next)).toEqual([
      defaultId,
      "jace",
      "claude",
      "malcolm",
      "steven",
      "umich",
      "wallace",
    ]);
    expect(Object.entries(moveProviderInstance(next, "jace", "down"))).toEqual(
      Object.entries(instances),
    );
  });

  it("keeps the default pinned and refuses moves beyond the custom bounds", () => {
    expect(moveProviderInstance(instances, defaultId, "down")).toBe(instances);
    expect(moveProviderInstance(instances, "malcolm", "up")).toBe(instances);
    expect(moveProviderInstance(instances, "wallace", "down")).toBe(instances);
    expect(moveProviderInstance(instances, "claude", "up")).toBe(instances);
    expect(moveProviderInstance(instances, "missing", "down")).toBe(instances);
    expect(moveProviderInstance({}, "missing", "up")).toEqual({});
  });

  it("does not move an explicitly stored default even between custom slots", () => {
    const interleaved = {
      first: instances.malcolm!,
      [defaultId]: instances[defaultId]!,
      last: instances.jace!,
    };
    expect(Object.keys(moveProviderInstance(interleaved, "last", "up"))).toEqual([
      "last",
      defaultId,
      "first",
    ]);
  });
});
