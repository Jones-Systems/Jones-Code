import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { readFleetPublicationSource } from "./FleetController";

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
      expect(await readFleetPublicationSource()).toBeUndefined();
      expect(installPlan).toHaveBeenCalledExactlyOnceWith({ type: "publication" });
      expect(getUpdateState).not.toHaveBeenCalled();
    },
  );
  it("relays only a committed native grant", async () => {
    vi.stubGlobal("window", {
      desktopBridge: { installPlan: async () => ({ publication: { kind: "committed", source } }) },
    });
    expect(await readFleetPublicationSource()).toEqual(source);
  });
  it("keeps the installed-source relay only for old desktop bridges", async () => {
    vi.stubGlobal("window", {
      desktopBridge: { getUpdateState: async () => ({ jones: { installedSource: source } }) },
    });
    expect(await readFleetPublicationSource()).toEqual(source);
  });
});
