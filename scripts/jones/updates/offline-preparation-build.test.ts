// @effect-diagnostics-next-line nodeBuiltinImport:off -- These tests allocate and remove only one captured synthetic artifact directory per case.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths identify only the test-owned directory.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture digests bind exact synthetic bytes.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import {
  preparationSeaConfig,
  verifyNodeArchive,
  verifyPreparationArtifact,
  verifyPreparationClosure,
  verifyPreparationHeldOutput,
  verifyPreparationSeaConfig,
  verifyPreparationSourceOverlay,
} from "./offline-preparation-build.ts";

const sha = (bytes: string) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const record = (bytes: string) => ({ sha256: sha(bytes), size: Buffer.byteLength(bytes) });

function withArtifact(run: (root: string, manifest: ReturnType<typeof artifactManifest>) => void) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-preparation-build-"));
  let failure: unknown;
  try {
    const bundle = 'process.stdout.write("held\\n");\n';
    const config = `${JSON.stringify(preparationSeaConfig())}\n`;
    const binary = "synthetic bytes: never executable or native qualification";
    const manifest = artifactManifest(bundle, config, binary);
    for (const [name, value] of Object.entries({
      "offlinePreparationEntry.mjs": bundle,
      "sea-config.json": config,
      "jones-offline-preparation": binary,
      "provenance.json": JSON.stringify(manifest),
    }))
      NodeFS.writeFileSync(NodePath.join(root, name), value, { flag: "wx", mode: 0o600 });
    run(root, manifest);
  } catch (cause) {
    failure = cause;
    throw cause;
  } finally {
    try {
      NodeFS.rmSync(root, { recursive: true, force: true });
    } catch (cleanup) {
      throw new AggregateError(
        failure === undefined ? [cleanup] : [failure, cleanup],
        `Artifact fixture cleanup failed: ${root}`,
      );
    }
  }
}

function artifactManifest(bundle: string, config: string, binary: string) {
  return {
    schemaVersion: 1,
    observation: "complete",
    classification: "held-entry-only",
    preparationCapable: false,
    productionInvocation: false,
    source: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
      version: "fixture",
      hashes: Object.fromEntries(
        [
          "pnpm-lock.yaml",
          "pnpm-workspace.yaml",
          "package.json",
          "apps/server/package.json",
          "apps/desktop/package.json",
          "apps/web/package.json",
          "packages/contracts/package.json",
          "apps/server/vite.config.ts",
          "vite.config.ts",
          "scripts/jones/updates/offline-preparation-build.ts",
          "apps/server/src/jones/updates/offlinePreparationEntry.ts",
          "apps/server/src/jones/updates/offlinePreparation.ts",
        ].map((file) => [file, sha(file)]),
      ),
    },
    entry: "src/jones/updates/offlinePreparationEntry.ts",
    dependencyInventory: ["apps/server/src/jones/updates/offlinePreparationEntry.ts"],
    builtins: [],
    bundle: record(bundle),
    seaConfig: { ...record(config), value: preparationSeaConfig() },
    upstream: {
      archiveUrl: "https://nodejs.org/download/release/v26.8.2/node-v26.8.2-darwin-arm64.tar.gz",
      checksumsUrl: "https://nodejs.org/download/release/v26.8.2/SHASUMS256.txt",
      verification: "official-https-checksum",
      signedChecksumVerification: false,
      archive: record("archive fixture"),
      checksumsSha256: sha("checksum fixture"),
      runtime: {
        ...record("runtime fixture"),
        version: "26.8.2",
        platform: "darwin",
        arch: "arm64",
      },
    },
    executable: {
      ...record(binary),
      name: "jones-offline-preparation",
      signature: "ad-hoc-verified",
      heldOutputObserved: true,
      argumentRefusalObserved: true,
    },
  };
}

describe("closed preparation SEA build", () => {
  it("accepts only literal builtins in an otherwise closed module", () => {
    expect(
      verifyPreparationClosure(
        'import { isSea } from "node:sea"; await import("node:sqlite"); isSea();',
      ),
    ).toEqual(["node:sea", "node:sqlite"]);
  });

  it.each([
    'import "./sibling.mjs";',
    'export * from "/tmp/other.mjs";',
    'import "effect";',
    'import("file:///tmp/other.mjs");',
    'import("data:text/javascript,0");',
    "import(name);",
    '__require("external");',
    "import(`node:${name}`);",
    'require("node:fs");',
    'import { createRequire as loader } from "node:module";',
    'process["dl" + "open"]({}, path);',
    "const p = process; p[x](path);",
    "globalThis[loader](path);",
    'new Function("return import(path)")();',
    'import "./addon.node";',
    "function initializeFixture() {}",
    'const sql = "CREATE TABLE fixture (id TEXT)";',
    "import {",
  ])("rejects loader/initializer escape %s", (source) => {
    expect(() => verifyPreparationClosure(source)).toThrow();
  });

  it("refuses code cache, snapshot, extension, alternate entry and asset configuration", () => {
    const fixed = preparationSeaConfig();
    expect(() => verifyPreparationSeaConfig(fixed)).not.toThrow();
    for (const patch of [
      { useCodeCache: true },
      { useSnapshot: true },
      { execArgvExtension: "env" },
      { main: "bin.mjs" },
      { executable: "/some/node" },
      { mainFormat: "commonjs" },
      { assets: { addon: "addon.node" } },
    ])
      expect(() => verifyPreparationSeaConfig({ ...fixed, ...patch })).toThrow();
  });

  it("requires one exact official archive checksum and observed matching bytes", () => {
    const bytes = Buffer.from("owned archive fixture");
    const row = `${sha(bytes.toString())}  node-v26.8.2-darwin-arm64.tar.gz\n`;
    expect(verifyNodeArchive(row, bytes)).toBe(sha(bytes.toString()));
    for (const checksums of [
      "",
      row + row,
      row.replace("darwin-arm64", "linux-arm64"),
      row.replace(/^./, "z"),
      row.replace("  ", "  other  "),
    ])
      expect(() => verifyNodeArchive(checksums, bytes)).toThrow();
    expect(() => verifyNodeArchive(row, Buffer.from("changed"))).toThrow();
  });

  it("refuses a claimed successful, duplicate or incomplete entry response", () => {
    const output =
      JSON.stringify({
        status: "held",
        reason: "maintenance-owner-binding-unqualified",
        databaseOpened: false,
        productionInvocation: false,
        activation: false,
      }) + "\n";
    expect(() => verifyPreparationHeldOutput(output)).not.toThrow();
    for (const invalid of [
      output + output,
      output.replace('"held"', '"ready"'),
      output.replace('"databaseOpened":false', '"databaseOpened":true'),
      "{}\n",
    ])
      expect(() => verifyPreparationHeldOutput(invalid)).toThrow();
  });

  it("admits only observed release version/source stamping", () => {
    const identity = { sha: "a".repeat(40), tree: "b".repeat(40) };
    const original = { version: "0.0.45", dependencies: { effect: "fixed" } };
    const actual = {
      ...original,
      version: "0.0.45-preview.1",
      jonesSource: { repository: "Jones-Systems/Jones-Code", ...identity },
      startupGateProtocol: 1,
    };
    expect(
      verifyPreparationSourceOverlay("apps/server/package.json", original, actual, identity),
    ).toBe(actual.version);
    for (const changed of [
      { ...actual, dependencies: { effect: "changed" } },
      { ...actual, jonesSource: { ...actual.jonesSource, sha: "c".repeat(40) } },
    ])
      expect(() =>
        verifyPreparationSourceOverlay("apps/server/package.json", original, changed, identity),
      ).toThrow();
    expect(() =>
      verifyPreparationSourceOverlay("pnpm-lock.yaml", original, actual, identity),
    ).toThrow();
  });

  it("reobserves final bytes instead of trusting recorded digests", () =>
    withArtifact((root) => {
      expect(() => verifyPreparationArtifact(root)).not.toThrow();
      NodeFS.appendFileSync(
        NodePath.join(root, "jones-offline-preparation"),
        "changed after signing",
      );
      expect(() => verifyPreparationArtifact(root)).toThrow(/artifact changed/);
    }));

  it.each(["observation", "upstream", "source", "seaConfig", "dependencyInventory", "builtins"])(
    "refuses missing provenance %s",
    (key) =>
      withArtifact((root, manifest) => {
        const incomplete = { ...manifest } as Record<string, unknown>;
        delete incomplete[key];
        NodeFS.writeFileSync(NodePath.join(root, "provenance.json"), JSON.stringify(incomplete));
        expect(() => verifyPreparationArtifact(root)).toThrow();
      }),
  );

  it("rejects added assets and symlinked executable members", () =>
    withArtifact((root) => {
      const extra = NodePath.join(root, "addon.node");
      NodeFS.writeFileSync(extra, "extra");
      expect(() => verifyPreparationArtifact(root)).toThrow(/inventory/);
      NodeFS.unlinkSync(extra);
      const executable = NodePath.join(root, "jones-offline-preparation");
      NodeFS.unlinkSync(executable);
      NodeFS.symlinkSync("offlinePreparationEntry.mjs", executable);
      expect(() => verifyPreparationArtifact(root)).toThrow();
    }));
});
