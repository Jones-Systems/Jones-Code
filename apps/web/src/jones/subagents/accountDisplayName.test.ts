import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { subagentAccountDisplayName } from "./accountDisplayName";

const instanceId = ProviderInstanceId.make("work-account");
const codex = ProviderDriverKind.make("codex");
const claude = ProviderDriverKind.make("claudeAgent");
const provider = { instanceId, driver: codex, displayName: "  Work Account  " };

describe("subagent account identity", () => {
  it.each([codex, claude])("resolves a matching %s account", (driver) => {
    expect(subagentAccountDisplayName([{ ...provider, driver }], instanceId, driver)).toBe(
      "Work Account",
    );
  });

  it("uses the same fallback account name as the composer", () => {
    expect(
      subagentAccountDisplayName([{ ...provider, displayName: "Codex" }], instanceId, codex),
    ).toBe("Work Account");
  });

  it("does not identify a mismatched driver as the catalog account", () => {
    expect(subagentAccountDisplayName([provider], instanceId, claude)).toBeUndefined();
  });

  it("does not label a remote child with its parent's account", () => {
    const remote = ProviderDriverKind.make("remote");
    expect(subagentAccountDisplayName([provider], instanceId, remote)).toBeUndefined();
    expect(
      subagentAccountDisplayName([{ ...provider, driver: remote }], instanceId, remote),
    ).toBeUndefined();
  });

  it("preserves fallback avatars for missing driver, instance, or catalog", () => {
    expect(subagentAccountDisplayName([provider], instanceId, undefined)).toBeUndefined();
    expect(
      subagentAccountDisplayName([provider], ProviderInstanceId.make("unknown"), codex),
    ).toBeUndefined();
    expect(subagentAccountDisplayName([], instanceId, codex)).toBeUndefined();
    expect(subagentAccountDisplayName(null, instanceId, codex)).toBeUndefined();
    expect(subagentAccountDisplayName(undefined, instanceId, codex)).toBeUndefined();
  });
});
