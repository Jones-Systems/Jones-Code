// @effect-diagnostics nodeBuiltinImport:off - Synchronous fixture hashing verifies exact boundary source bytes.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import {
  boundaryExit,
  checkBoundary,
  imports,
  parseFragment,
  parseInventory,
  parseResolutionCases,
  runCli,
  safePath,
} from "./check-boundary.ts";
import type { Fragment, Inventory } from "./check-boundary.ts";

const hook = "apps/web/src/entry.ts";
const implementation = "apps/web/src/jones/usage/view.ts";
function inventory(): Inventory {
  return parseInventory({
    schema: "jones.boundary-inventory/v1",
    upstream: {
      repository: "pingdotgg/t3code",
      tag: "v0.0.46-nightly.20261004.2648",
      commit: "5a96895a85b43c838da6c89cf21068b99c003d5a",
    },
    baseline: "612e95df23c30e9cb08f20475d501f76393e5457",
    paths: { [hook]: "accepted-shared" },
    jonesOwnedPaths: [],
    tombstones: [],
    allowedImports: [],
  });
}
function fragment(): Fragment {
  return parseFragment({
    schema: "jones.boundary-fragment/v1",
    feature: "usage",
    jonesOwnedPaths: ["apps/web/src/jones/usage/"],
    hookPaths: [hook],
    tombstones: [],
    allowedImports: [{ from: hook, to: implementation }],
  });
}
function check(source: string, fragments: Fragment[] = [fragment()], inv = inventory()) {
  return checkBoundary(
    inv,
    fragments,
    new Map([
      [hook, source],
      [implementation, "export const value = 1;"],
    ]),
    new Set([hook]),
    [hook, implementation],
  );
}
describe("Jones boundary", () => {
  it("classifies an explicit feature and verifies its integration edge", () => {
    const result = check('import {value} from "~/jones/usage/view";');
    expect(result.violations).toEqual([]);
    expect(result.classified).toEqual({ [hook]: "hook", [implementation]: "jones-owned" });
  });
  it("keeps advisory findings non-gating and strict findings failing", () => {
    const result = checkBoundary(
      inventory(),
      [],
      new Map([["apps/web/src/jones/unknown/view.ts", ""]]),
      new Set([hook]),
      ["apps/web/src/jones/unknown/view.ts"],
    );
    expect(result.violations.map((entry) => entry.code)).toEqual(["unclassified-path"]);
    expect(boundaryExit(false, result.violations)).toBe(0);
    expect(boundaryExit(true, result.violations)).toBe(1);
    expect(boundaryExit(true, [])).toBe(0);
  });
  it.each([
    'import {value} from "./jones/usage/view";',
    'export {value} from "./jones/usage/view";',
    'const value = import("./jones/usage/view");',
    'const value = require("./jones/usage/view");',
    'import value = require("./jones/usage/view");',
  ])("rejects unlisted literal boundary syntax: %s", (source) => {
    const f = fragment();
    f.allowedImports = [];
    expect(check(source, [f]).violations.map((entry) => entry.code)).toEqual([
      "unlisted-upstream-import",
    ]);
  });
  it("rejects computed calls and unresolved internal imports conservatively", () => {
    expect(check("const value = import(destination);").violations[0]?.code).toBe(
      "unresolved-import",
    );
    expect(check('import value from "@t3tools/missing";').violations[0]?.code).toBe(
      "unresolved-import",
    );
    expect(check('import value from "~/jones/usage/missing";').violations[0]?.code).toBe(
      "unresolved-import",
    );
  });
  it("does not mistake comments, strings or a member require call for imports", () => {
    expect(
      imports(
        hook,
        '// import("./jones/usage/view");\nconst text = "require(secret)"; loader.require("x");',
      ),
    ).toEqual([]);
  });
  it("resolves exact workspace package exports without installed package code", () => {
    const inv = inventory();
    inv.jonesOwnedPaths = ["packages/contracts/src/jones/usage/"];
    inv.allowedImports = [{ from: hook, to: "packages/contracts/src/jones/usage/schema.ts" }];
    const tree = new Map([
      [hook, 'export {value} from "@t3tools/contracts/usage";'],
      [
        "packages/contracts/package.json",
        JSON.stringify({
          name: "@t3tools/contracts",
          exports: {
            "./usage": { types: "./src/jones/usage/schema.ts", import: "./dist/usage.js" },
          },
        }),
      ],
      ["packages/contracts/src/jones/usage/schema.ts", 'import * as S from "effect/Schema";'],
    ]);
    expect(checkBoundary(inv, [], tree, new Set([hook]), [hook]).violations).toEqual([]);
  });
  it("rejects contract runtime imports while allowing erased type imports", () => {
    const contract = "packages/contracts/src/jones/usage/schema.ts";
    const runtime = "apps/server/src/runtime.ts";
    const inv = inventory();
    inv.jonesOwnedPaths = [contract];
    const tree = new Map([
      [contract, 'import { value } from "../../../../../apps/server/src/runtime.ts";'],
      [runtime, "export const value = 1;"],
    ]);
    expect(checkBoundary(inv, [], tree, new Set(), [contract]).violations[0]?.code).toBe(
      "contract-runtime-import",
    );
    tree.set(contract, 'import type { Value } from "../../../../../apps/server/src/runtime.ts";');
    expect(checkBoundary(inv, [], tree, new Set(), [contract]).violations).toEqual([]);
    tree.set(contract, 'import { type Value } from "../../../../../apps/server/src/runtime.ts";');
    expect(checkBoundary(inv, [], tree, new Set(), [contract]).violations).toEqual([]);
    tree.set(contract, 'import fs from "node:fs";');
    expect(checkBoundary(inv, [], tree, new Set(), [contract]).violations[0]?.code).toBe(
      "contract-runtime-import",
    );
  });
  it("rejects resurrection even when it is not a changed source file", () => {
    const inv = inventory();
    inv.tombstones = ["assets/retired.png"];
    const result = checkBoundary(inv, [], new Map([["assets/retired.png", ""]]), new Set(), []);
    expect(result.violations[0]?.code).toBe("tombstone-resurrected");
    inv.paths["assets/retired.png"] = "upstream-deleted";
    expect(
      checkBoundary(inv, [], new Map([["assets/retired.png", ""]]), new Set(), [
        "assets/retired.png",
      ]).violations.map((entry) => entry.code),
    ).toContain("upstream-deleted-resurrected");
  });
  it("rejects broad or overlapping exemptions and ownership of upstream files", () => {
    expect(() => parseFragment({ ...fragment(), jonesOwnedPaths: ["apps/web/src/"] })).toThrow(
      "too broad",
    );
    expect(() => check("", [fragment(), fragment()])).toThrow("Duplicate feature");
    expect(() => check("", [{ ...fragment(), jonesOwnedPaths: [hook] }])).toThrow(
      "overlaps upstream",
    );
    expect(() => check("", [{ ...fragment(), feature: "other" }, fragment()])).toThrow(
      "Overlapping ownership",
    );
  });
  it("rejects malformed provenance, unknown keys and unsafe option/path inputs", () => {
    expect(() => parseInventory({ ...inventory(), baseline: "HEAD" })).toThrow("full commit");
    expect(() => parseInventory({ ...inventory(), extra: true })).toThrow("exactly");
    expect(() => parseFragment({ ...fragment(), hookPaths: ["../outside"] })).toThrow("Unsafe");
    for (const name of [
      "/outside",
      "../outside",
      "a/../b",
      "-option",
      "apps/*jones*/",
      "a\\b",
      "a\nb",
    ])
      expect(() => safePath(name, true)).toThrow("Unsafe");
    expect(() => runCli(["--upstream", "HEAD;touch outside"])).toThrow("Unknown");
  });
  it("checks imports in unchanged upstream files too", () => {
    const f = fragment();
    f.allowedImports = [];
    expect(
      checkBoundary(
        inventory(),
        [f],
        new Map([
          [hook, 'import "./jones/usage/view";'],
          [implementation, ""],
        ]),
        new Set([hook]),
        [implementation],
      ).violations[0]?.code,
    ).toBe("unlisted-upstream-import");
  });
  it("resolves nested mobile module exports and retains the Jones edge check", () => {
    const from = "apps/mobile/src/view.ts";
    const target = "apps/mobile/modules/local/src/view.ts";
    const inv = inventory();
    inv.jonesOwnedPaths = [target];
    const tree = new Map([
      [from, 'import {value} from "@t3tools/local/view";'],
      [
        "apps/mobile/modules/local/package.json",
        JSON.stringify({ name: "@t3tools/local", exports: { "./view": "./src/view.ts" } }),
      ],
      [target, "export const value = 1;"],
    ]);
    expect(
      checkBoundary(inv, [], tree, new Set([from]), [target]).violations.map((entry) => entry.code),
    ).toEqual(["unlisted-upstream-import"]);
    inv.allowedImports = [{ from, to: target }];
    expect(checkBoundary(inv, [], tree, new Set([from]), [target]).violations).toEqual([]);
  });
  it.each(["?url", "?raw", "?inline", "?url&no-inline"])(
    "resolves Vite asset queries without exempting Jones targets: %s",
    (query) => {
      const target = "apps/web/src/jones/usage/asset.wasm";
      const result = checkBoundary(
        inventory(),
        [fragment()],
        new Map([
          [hook, `import data from "./jones/usage/asset.wasm${query}";`],
          [target, ""],
        ]),
        new Set([hook]),
        [target],
      );
      expect(result.violations.map((entry) => entry.code)).toEqual(["unlisted-upstream-import"]);
      expect(check('import data from "./missing.wasm?unknown";').violations[0]?.code).toBe(
        "unresolved-import",
      );
    },
  );
  it("binds explicit unsupported loaders to exact source and anchor bytes", () => {
    const text = "const value = import(destination);";
    const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
    const anchor = "apps/web/package.json";
    const anchorText = '{"name":"@t3tools/web"}';
    const cases = parseResolutionCases({
      schema: "jones.boundary-resolutions/v1",
      cases: [
        {
          from: hook,
          sourceSha256: hash(text),
          expression: "destination",
          reason: "Synthetic fixture uses an explicitly supplied loader URL.",
          anchors: [{ path: anchor, sha256: hash(anchorText) }],
          targets: [],
        },
      ],
    });
    const tree = new Map([
      [hook, text],
      [anchor, anchorText],
    ]);
    const run = () => checkBoundary(inventory(), [], tree, new Set([hook]), [], cases);
    expect(run().violations).toEqual([]);
    expect(run().acceptedUnsupported).toHaveLength(1);
    tree.set(hook, text + " import(otherDestination);");
    expect(run().violations.map((entry) => entry.code)).toContain("stale-resolution-disposition");
    expect(run().violations.filter((entry) => entry.code === "unresolved-import")).toHaveLength(2);
    tree.set(hook, text);
    tree.set(anchor, anchorText + " ");
    expect(run().violations.map((entry) => entry.code)).toContain("stale-resolution-disposition");
    expect(() =>
      parseResolutionCases({
        schema: "jones.boundary-resolutions/v1",
        cases: [{ ...cases[0], extra: true }],
      }),
    ).toThrow("exactly");
  });
  it("still checks known computed targets and forbids contract dispositions", () => {
    const text = "const value = import(destination);";
    const hash = NodeCrypto.createHash("sha256").update(text).digest("hex");
    const cases = parseResolutionCases({
      schema: "jones.boundary-resolutions/v1",
      cases: [
        {
          from: hook,
          sourceSha256: hash,
          expression: "destination",
          reason: "A fixed local target is known from the pinned loader source.",
          anchors: [],
          targets: [implementation],
        },
      ],
    });
    const f = fragment();
    f.allowedImports = [];
    const tree = new Map([
      [hook, text],
      [implementation, ""],
    ]);
    expect(
      checkBoundary(inventory(), [f], tree, new Set([hook]), [], cases).violations[0]?.code,
    ).toBe("unlisted-upstream-import");
    tree.delete(implementation);
    expect(
      checkBoundary(inventory(), [f], tree, new Set([hook]), [], cases).violations[0]?.code,
    ).toBe("unresolved-import");
    const contract = "packages/contracts/src/jones/usage/schema.ts";
    const inv = inventory();
    inv.jonesOwnedPaths = [contract];
    const contractCases = parseResolutionCases({
      schema: "jones.boundary-resolutions/v1",
      cases: [{ ...cases[0], from: contract, targets: [] }],
    });
    const result = checkBoundary(
      inv,
      [],
      new Map([[contract, text]]),
      new Set(),
      [contract],
      contractCases,
    );
    expect(result.violations[0]?.code).toBe("unresolved-import");
    expect(result.acceptedUnsupported).toEqual([]);
  });
});
