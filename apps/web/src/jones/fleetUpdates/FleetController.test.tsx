import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { readFleetPublication, readFleetPublicationSource } from "./FleetController";

afterEach(() => vi.unstubAllGlobals());
describe("native fleet publication grant", () => {
  const source = {
    repository: "Jones-Systems/Jones-Code",
    sha: "a".repeat(40),
    tree: "b".repeat(40),
  } as const;
  it.each(["plan-managed", "unproved", "blocked", "unavailable"] as const)(
    "withholds %s without falling back to installedSource",
    async (code) => {
      const getUpdateState = vi.fn(async () => ({ jones: { installedSource: source } }));
      const installPlan = vi.fn(async () => ({
        publication: { kind: "withheld", code, message: "Native proof is required." },
      }));
      vi.stubGlobal("window", { desktopBridge: { installPlan, getUpdateState } });
      expect(await readFleetPublication()).toEqual({ kind: "withheld", code, message: "Native proof is required." });
      expect(await readFleetPublicationSource()).toBeUndefined();
      expect(installPlan).toHaveBeenCalledWith({
        type: "publication",
        acceptOrdinaryRecovery: 1,
      });
      expect(getUpdateState).not.toHaveBeenCalled();
    },
  );
  it.each(["committed", "ordinary-verified"])("relays the native %s grant", async (kind) => {
    vi.stubGlobal("window", {
      desktopBridge: { installPlan: async () => ({ publication: { kind, source } }) },
    });
    expect(await readFleetPublication()).toEqual({ kind, source });
    expect(await readFleetPublicationSource()).toEqual(source);
  });
  it("keeps an old bridge's installed source as display evidence only", async () => {
    const getUpdateState = vi.fn(async () => ({ jones: { installedSource: source } }));
    vi.stubGlobal("window", { desktopBridge: { getUpdateState } });
    expect(await readFleetPublication()).toMatchObject({ kind: "withheld", code: "unavailable" });
    expect(await readFleetPublicationSource()).toBeUndefined();
    expect(getUpdateState).not.toHaveBeenCalled();
  });
  it("retains unavailable diagnostics for absent and failed native answers", async () => {
    for (const installPlan of [async () => ({}), async () => { throw new Error("unavailable"); }]) {
      vi.stubGlobal("window", { desktopBridge: { installPlan } });
      expect(await readFleetPublication()).toMatchObject({ kind: "withheld", code: "unavailable" });
    }
  });
});
