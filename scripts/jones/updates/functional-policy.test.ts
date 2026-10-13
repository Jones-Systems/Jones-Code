import { describe, expect, it } from "vite-plus/test";
import {
  verifyFunctionalPreparationGraph,
  verifyPreparationSourceOverlay,
} from "./functional-policy.ts";

const identity = { sha: "a".repeat(40), tree: "b".repeat(40) };
const preview = "0.0.45-preview.20261013.1";
const original = { name: "fixture", version: "0.0.45", dependencies: { fixed: "1.2.3" } };
const source = { repository: "Jones-Systems/Jones-Code", ...identity };

describe("reviewed functional worker policy", () => {
  it.each([
    ["synthetic module", new TextEncoder().encode("export const qualified = true;")],
    ["same-sized substituted bytes", new Uint8Array(1883613)],
  ] as const)("refuses %s before reading source or trusting graph claims", (_name, entryBytes) => {
    let sourceRead = false;
    expect(() =>
      verifyFunctionalPreparationGraph({
        get repoRoot() {
          sourceRead = true;
          throw new Error("source must remain unopened");
        },
        entryBytes,
        reportBytes: new TextEncoder().encode(
          JSON.stringify({ qualification: "qualified", preparationCapable: true }),
        ),
      }),
    ).toThrow("outside the reviewed worker policy");
    expect(sourceRead).toBe(false);
  });
});

describe("committed release manifest overlays", () => {
  it.each(["apps/server/package.json", "apps/desktop/package.json"])(
    "allows only the exact stamp on %s",
    (file) => {
      expect(
        verifyPreparationSourceOverlay(
          file,
          original,
          {
            ...original,
            version: preview,
            jonesSource: source,
            startupGateProtocol: 1,
          },
          identity,
        ),
      ).toBe(preview);
    },
  );
  it.each(["apps/web/package.json", "packages/contracts/package.json"])(
    "allows only a version edit on %s",
    (file) => {
      expect(
        verifyPreparationSourceOverlay(file, original, { ...original, version: preview }, identity),
      ).toBe(preview);
      expect(() =>
        verifyPreparationSourceOverlay(
          file,
          original,
          {
            ...original,
            version: preview,
            jonesSource: source,
          },
          identity,
        ),
      ).toThrow("beyond release stamping");
    },
  );
  it.each([
    ["wrong source", { jonesSource: { ...source, sha: "c".repeat(40) } }],
    ["wrong tree", { jonesSource: { ...source, tree: "c".repeat(40) } }],
    ["extra source field", { jonesSource: { ...source, trusted: true } }],
    ["startup protocol", { startupGateProtocol: 2 }],
    ["dependency edit", { dependencies: { fixed: "9.9.9" } }],
    ["unrelated metadata", { approved: true }],
  ] as const)("rejects %s", (_name, changes) => {
    expect(() =>
      verifyPreparationSourceOverlay(
        "apps/server/package.json",
        original,
        {
          ...original,
          version: preview,
          ...changes,
        },
        identity,
      ),
    ).toThrow();
  });
  it("rejects an arbitrary manifest even when its bytes match", () => {
    expect(() =>
      verifyPreparationSourceOverlay("package.json", original, original, identity),
    ).toThrow("Unadmitted source overlay");
  });
});
