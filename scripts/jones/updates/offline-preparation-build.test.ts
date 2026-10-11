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
  preparePreparationOutput,
  offlinePreparationPack,
  offlinePreparationCandidatePack,
  inspectPreparationCandidateJavaScript,
  inspectPreparationCandidateCode,
  inspectPreparationCandidateExternal,
  requireEmptyPreparationCandidateOutput,
  runPreparationLifecycleFixture,
  verifyNodeArchive,
  verifyPreparationArtifact,
  verifyPreparationClosure,
  verifyPreparationHeldOutput,
  verifyPreparationSeaConfig,
  verifyPreparationSourceOverlay,
  verifyPreparationSQLiteObservation,
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
      buildOnlyOverlays: [] as string[],
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
          "apps/server/src/jones/updates/offlinePreparationCore.ts",
          "apps/server/src/jones/updates/offlinePreparationPlan.ts",
          "apps/server/src/jones/updates/offlinePreparationWorker.ts",
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
        sqlite: sqliteObservation().sqlite,
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

function sqliteObservation() {
  return {
    nodeVersion: "26.8.2",
    processVersion: "v26.8.2",
    platform: "darwin",
    arch: "arm64",
    database: ":memory:",
    databaseClosed: true,
    runtimeSQLiteVersion: "3.50.4",
    // Synthetic values verify shape/equality only; native producer execution supplies actual SQLite identity.
    sqlite: {
      version: "3.50.4",
      sourceId: `2026-01-02 03:04:05 ${"a".repeat(64)}`,
      observation: "in-memory-closed",
    },
  };
}

describe("closed preparation SEA build", () => {
  it("verifies the closed in-memory observation without upgrading held capability", () => {
    expect(verifyPreparationSQLiteObservation(JSON.stringify(sqliteObservation()))).toEqual(
      sqliteObservation().sqlite,
    );
    withArtifact((root, manifest) => {
      const proof = verifyPreparationArtifact(root);
      expect(proof.classification).toBe("held-entry-only");
      expect(proof.preparationCapable).toBe(false);
      expect(proof.productionInvocation).toBe(false);
      expect(proof.provenanceSha256).toBe(
        sha(NodeFS.readFileSync(NodePath.join(root, "provenance.json"), "utf8")),
      );
      expect(proof.source.hashes).toEqual(manifest.source.hashes);
      expect(Object.keys(proof.source.hashes)).toHaveLength(15);
      expect(proof.sqlite).toEqual(sqliteObservation().sqlite);
      expect(proof.node.runtime.sqlite).toEqual(proof.sqlite);
      expect(Object.isFrozen(proof.source.hashes)).toBe(true);
    });
  });

  it("accepts the exact observed release-stamp overlay subset", () =>
    withArtifact((root, manifest) => {
      manifest.source.buildOnlyOverlays = [
        "apps/server/package.json",
        "apps/desktop/package.json",
        "apps/web/package.json",
        "packages/contracts/package.json",
      ];
      NodeFS.writeFileSync(NodePath.join(root, "provenance.json"), JSON.stringify(manifest));
      expect(verifyPreparationArtifact(root).source.buildOnlyOverlays).toEqual(
        manifest.source.buildOnlyOverlays,
      );
    }));

  it.each(
    [
      ["apps/server/package.json", "apps/server/package.json"],
      ["pnpm-lock.yaml"],
      ["apps/server/src/jones/updates/offlinePreparationCore.ts"],
      Array.from({ length: 5 }, () => "apps/server/package.json"),
      "apps/server/package.json",
      undefined,
    ].map((overlays) => [overlays] as const),
  )("rejects invalid producer overlay evidence %j", (overlays) =>
    withArtifact((root, manifest) => {
      const source = { ...manifest.source, buildOnlyOverlays: overlays };
      NodeFS.writeFileSync(
        NodePath.join(root, "provenance.json"),
        JSON.stringify({ ...manifest, source }),
      );
      expect(() => verifyPreparationArtifact(root)).toThrow();
    }),
  );

  it.each([
    { nodeVersion: "24.21.0" },
    { processVersion: "26.8.2" },
    { platform: "linux" },
    { arch: "x64" },
    { database: "/tmp/state.sqlite" },
    { databaseClosed: false },
    { runtimeSQLiteVersion: "3.50.5" },
    { extra: true },
    { sqlite: { ...sqliteObservation().sqlite, extra: true } },
    {
      sqlite: { ...sqliteObservation().sqlite, sourceId: `2026-02-30 03:04:05 ${"a".repeat(64)}` },
    },
    {
      sqlite: {
        ...sqliteObservation().sqlite,
        sourceId: sqliteObservation().sqlite.sourceId + "\n",
      },
    },
  ])("rejects mismatched or excess SQLite observation %j", (patch) => {
    expect(() =>
      verifyPreparationSQLiteObservation(JSON.stringify({ ...sqliteObservation(), ...patch })),
    ).toThrow();
  });

  it.each([
    "",
    "source",
    "source.hashes",
    "bundle",
    "seaConfig",
    "seaConfig.value",
    "upstream",
    "upstream.archive",
    "upstream.runtime",
    "upstream.runtime.sqlite",
    "executable",
  ])("rejects excess nested artifact proof fields at %s", (path) =>
    withArtifact((root, manifest) => {
      const changed = JSON.parse(JSON.stringify(manifest)) as Record<string, unknown>;
      let object = changed;
      for (const key of path === "" ? [] : path.split("."))
        object = object[key] as Record<string, unknown>;
      object.extra = true;
      NodeFS.writeFileSync(NodePath.join(root, "provenance.json"), JSON.stringify(changed));
      expect(() => verifyPreparationArtifact(root)).toThrow();
    }),
  );

  it("hashes actual provenance bytes on every observation and rechecks executable bytes", () =>
    withArtifact((root) => {
      const before = verifyPreparationArtifact(root);
      NodeFS.appendFileSync(NodePath.join(root, "provenance.json"), "\n");
      const after = verifyPreparationArtifact(root);
      expect(after.provenanceSha256).not.toBe(before.provenanceSha256);
      expect(after.provenanceBytes).toBe(before.provenanceBytes + 1);
      expect(after.source).toEqual(before.source);
      NodeFS.appendFileSync(NodePath.join(root, "jones-offline-preparation"), "changed");
      expect(() => verifyPreparationArtifact(root)).toThrow(/artifact changed/);
    }));

  it.each(["sqlite", "core-input", "plan-input", "worker-input", "held-classification"] as const)(
    "preserves old or unqualified output: %s",
    (fault) =>
      withArtifact((root, manifest) => {
        const changed = JSON.parse(JSON.stringify(manifest)) as ReturnType<typeof artifactManifest>;
        if (fault === "sqlite") delete (changed.upstream.runtime as Record<string, unknown>).sqlite;
        if (fault === "core-input")
          delete changed.source.hashes["apps/server/src/jones/updates/offlinePreparationCore.ts"];
        if (fault === "plan-input")
          delete changed.source.hashes["apps/server/src/jones/updates/offlinePreparationPlan.ts"];
        if (fault === "worker-input")
          delete changed.source.hashes["apps/server/src/jones/updates/offlinePreparationWorker.ts"];
        if (fault === "held-classification") changed.classification = "cold-preparation-worker";
        const raw = JSON.stringify(changed);
        NodeFS.writeFileSync(NodePath.join(root, "provenance.json"), raw);
        expect(() => preparePreparationOutput(root)).toThrow();
        expect(NodeFS.readFileSync(NodePath.join(root, "provenance.json"), "utf8")).toBe(raw);
        expect(NodeFS.existsSync(NodePath.join(root, "jones-offline-preparation"))).toBe(true);
      }),
  );

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
    '(() => {}).constructor("return globalThis")()["pro" + "cess"]["get" + "BuiltinModule"]("node:fs");',
    '(() => {})["con" + "structor"]("return this")();',
    'Object.getPrototypeOf(async function () {}).constructor("return 1")();',
    'Reflect.get(() => {}, "constructor")("return this")();',
    'const { constructor: compile } = (() => {}); compile("return this")();',
  ])("rejects loader/initializer escape %s", (source) => {
    expect(() => verifyPreparationClosure(source)).toThrow();
  });

  it.each([
    "success",
    "failure",
    "abort-command",
    "abort-download",
    "uncertain-close",
    "drain-unawaited-command",
    "signal-command",
  ] as const)("owns real producer resources through %s", async (scenario) => {
    const report = await runPreparationLifecycleFixture(scenario);
    expect(report.outcome).toBe(scenario === "success" ? "complete" : "rejected");
    expect(report.listenersInstalled).toBe(true);
    expect(report.listenersRestored).toBe(true);
    expect(report.rootRemoved).toBe(true);
    expect(report.serverClosed).toBe(true);
    expect(report.childCloseObserved).toBe(scenario !== "abort-download");
    expect(report.retainedBeforeFixtureCleanup).toBe(scenario === "uncertain-close");
    if (scenario === "uncertain-close")
      expect(report.custodyEvidence).toMatchObject({
        observation: "incomplete",
        classification: "unusable",
        signalAuthority: "captured-handle-only",
        resources: [{ kind: "command", closeObserved: false, exitObserved: true }],
      });
    if (scenario === "signal-command") {
      expect(report.failureStatus).toBe(143);
      expect(report.signalReport).toMatchObject({
        outcome: "rejected",
        listenersInstalled: true,
        listenersRestored: true,
        childCloseObserved: true,
        rootRemoved: true,
        failureMessage: "Preparation build interrupted by SIGTERM.",
      });
    }
    if (scenario === "failure") expect(report.failureStatus).toBe(7);
    if (scenario === "abort-command") expect(report.failureMessage).toBe("owned command abort");
    if (scenario === "abort-download") expect(report.failureMessage).toBe("owned download abort");
    if (scenario === "drain-unawaited-command")
      expect(report.failureMessage).toMatch(/admitted resources unfinished/);
  });

  it("keeps the command failure when actual nonempty-directory cleanup fails", async () => {
    const report = await runPreparationLifecycleFixture("cleanup-failure");
    expect(report.outcome).toBe("rejected");
    expect(report.childCloseObserved).toBe(true);
    expect(report.listenersRestored).toBe(true);
    expect(report.retentionKind).toBe("cleanup-retained");
    expect(report.custodyEvidence).toBeUndefined();
    expect(report.evidenceReadFailed).toBe(false);
    expect(report.failureCauses).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: 7 }),
        expect.objectContaining({ code: "ENOTEMPTY" }),
      ]),
    );
    expect(report.failureCauses.findIndex((cause) => cause.status === 7)).toBeLessThan(
      report.failureCauses.findIndex((cause) => cause.code === "ENOTEMPTY"),
    );
    expect(report.retainedBeforeRecovery).toBe(true);
    expect(report.fixtureRecoveryObserved).toBe(true);
    expect(report.rootRemoved).toBe(true);
  });

  it("aggregates receipt publication and read failures behind the original resource failure", async () => {
    const report = await runPreparationLifecycleFixture("receipt-failure");
    expect(report.outcome).toBe("rejected");
    expect(report.childCloseObserved).toBe(true);
    expect(report.listenersRestored).toBe(true);
    expect(report.retentionKind).toBe("custody-unresolved");
    expect(report.evidenceReadFailed).toBe(true);
    expect(report.failureCauses).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ message: "owned receipt primary failure" }),
        expect.objectContaining({ code: "EEXIST" }),
        expect.objectContaining({
          message: "Unbounded or non-regular build member: retained-custody.json",
        }),
      ]),
    );
    const primary = report.failureCauses.findIndex(
      (cause) => cause.message === "owned receipt primary failure",
    );
    const publication = report.failureCauses.findIndex((cause) => cause.code === "EEXIST");
    const inspection = report.failureCauses.findIndex(
      (cause) => cause.message === "Unbounded or non-regular build member: retained-custody.json",
    );
    expect(primary).toBeLessThan(publication);
    expect(publication).toBeLessThan(inspection);
    expect(report.retainedBeforeRecovery).toBe(true);
    expect(report.fixtureRecoveryObserved).toBe(true);
    expect(report.rootRemoved).toBe(true);
  });

  it("disables framework cleanup and retires only a verified completed output", () =>
    withArtifact((root) => {
      expect(offlinePreparationPack(root).clean).toBe(false);
      expect(NodeFS.existsSync(NodePath.join(root, "provenance.json"))).toBe(true);
      preparePreparationOutput(root);
      expect(NodeFS.readdirSync(root)).toEqual([]);
    }));

  it.each(["retained-root", "incomplete-receipt"] as const)(
    "preserves prior uncertainty: %s",
    (fault) =>
      withArtifact((root) => {
        const retained = NodePath.join(root, ".build-owned");
        if (fault === "retained-root") {
          NodeFS.mkdirSync(retained);
          NodeFS.writeFileSync(NodePath.join(retained, "retained-custody.json"), "owned evidence");
        } else
          NodeFS.writeFileSync(
            NodePath.join(root, "provenance.json"),
            '{"observation":"incomplete"}',
          );
        const before = NodeFS.readdirSync(root).sort();
        const receipt = NodeFS.readFileSync(NodePath.join(root, "provenance.json"));
        expect(() => preparePreparationOutput(root)).toThrow();
        expect(NodeFS.readdirSync(root).sort()).toEqual(before);
        expect(NodeFS.readFileSync(NodePath.join(root, "provenance.json"))).toEqual(receipt);
        if (fault === "retained-root")
          expect(
            NodeFS.readFileSync(NodePath.join(retained, "retained-custody.json"), "utf8"),
          ).toBe("owned evidence");
      }),
  );

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

async function withCandidateRoot(run: (root: string) => Promise<void>) {
  const root = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-preparation-candidate-test-")),
  );
  let failure: unknown;
  try {
    await run(root);
  } catch (cause) {
    failure = cause;
    throw cause;
  } finally {
    try {
      NodeFS.rmSync(root, { recursive: true, force: true });
    } catch (cleanup) {
      throw new AggregateError(
        failure === undefined ? [cleanup] : [failure, cleanup],
        `Candidate fixture cleanup failed: ${root}`,
      );
    }
  }
}

describe("unqualified preparation candidate graph", () => {
  it("records actual static, dynamic and require edges without executing their code", () => {
    const report = inspectPreparationCandidateJavaScript(
      'import "node:sqlite"; export * from "external"; import("./lazy.mjs"); import(variable); require("addon.node"); globalThis["danger"]();',
    );
    expect(
      report.imports.map(({ kind, specifier, computed, builtin }) => ({
        kind,
        specifier,
        computed,
        builtin,
      })),
    ).toEqual([
      { kind: "static", specifier: "node:sqlite", computed: false, builtin: true },
      { kind: "static", specifier: "external", computed: false, builtin: false },
      { kind: "dynamic", specifier: "./lazy.mjs", computed: false, builtin: false },
      { kind: "dynamic", specifier: null, computed: true, builtin: false },
      { kind: "require", specifier: "addon.node", computed: false, builtin: false },
    ]);
    expect(report.scannerQualification).toBe("syntax-observation-only");
    expect(report.loaderHazards.length).toBeGreaterThan(0);
  });

  it("distinguishes missing virtual/rendered content from observed empty code", () => {
    expect(inspectPreparationCandidateCode(null)).toEqual({
      availability: "unavailable",
      reason: "bundler-code-not-exposed",
    });
    expect(inspectPreparationCandidateCode(undefined)).not.toHaveProperty("sha256");
    expect(inspectPreparationCandidateCode("")).toEqual({
      availability: "observed",
      size: 0,
      sha256: sha(""),
    });
    expect(inspectPreparationCandidateCode("export const actual = 1;")).toEqual({
      availability: "observed",
      size: Buffer.byteLength("export const actual = 1;"),
      sha256: sha("export const actual = 1;"),
    });
  });

  it.each([
    {
      id: "node:sqlite",
      rendered: [],
      emitted: ["node:sqlite"],
      external: true,
      evidence: "emitted-import",
    },
    {
      id: "external-package",
      rendered: [],
      emitted: ["external-package"],
      external: true,
      evidence: "emitted-import",
    },
    {
      id: "/source/bundled.ts",
      rendered: ["/source/bundled.ts"],
      emitted: [],
      external: false,
      evidence: "rendered-module",
    },
    {
      id: "node:sqlite",
      rendered: [],
      emitted: [],
      external: null,
      evidence: "unavailable-module-info-api",
    },
    {
      id: "unresolved-or-tree-shaken",
      rendered: [],
      emitted: [],
      external: null,
      evidence: "unavailable-module-info-api",
    },
  ])("reports external status only from observed graph evidence: $id / $evidence", (input) => {
    expect(
      inspectPreparationCandidateExternal(
        input.id,
        new Set(input.rendered),
        new Set(input.emitted),
      ),
    ).toEqual({ external: input.external, externalEvidence: input.evidence });
  });

  it("refuses conflicting rendered and external observations instead of choosing a classification", () => {
    expect(() =>
      inspectPreparationCandidateExternal("conflict", new Set(["conflict"]), new Set(["conflict"])),
    ).toThrow(/observations conflict/);
  });

  it("does not confuse a candidate reflection observation with held-entry approval", () => {
    const code = '(() => {}).constructor("return this")();';
    expect(inspectPreparationCandidateJavaScript(code).loaderHazards.length).toBeGreaterThan(0);
    expect(() => verifyPreparationClosure(code)).toThrow();
  });

  it.each(["import {", 'import("' + "a".repeat(4097) + '");'])(
    "refuses invalid or unbounded candidate syntax",
    (code) => {
      expect(() => inspectPreparationCandidateJavaScript(code)).toThrow();
    },
  );

  it("candidate configuration is non-cleaning, separate and has no executable builder", () =>
    withArtifact((root) => {
      const candidate = offlinePreparationCandidatePack(root);
      const held = offlinePreparationPack(root);
      expect(candidate.outDir).toBe(
        NodePath.join(NodeFS.realpathSync(root), "apps/server/dist-exe/preparation-candidate"),
      );
      expect(candidate.outDir).not.toBe(held.outDir);
      expect(candidate.clean).toBe(false);
      expect("exe" in candidate).toBe(false);
      expect(candidate.plugins[0]!.name).toBe("jones:unqualified-preparation-candidate");
      expect(() => requireEmptyPreparationCandidateOutput(NodeFS.realpathSync(root))).toThrow(
        /retained/,
      );
      expect(() => verifyPreparationArtifact(root)).not.toThrow();
    }));

  it("preserves a frozen pre-cancellation cause before candidate source commands or output", () =>
    withCandidateRoot(async (root) => {
      const reason = Object.freeze(new Error("owned candidate cancellation"));
      const candidate = offlinePreparationCandidatePack(root, {
        signal: AbortSignal.abort(reason),
      });
      const start = candidate.plugins[0]!.buildStart;
      if (typeof start !== "function") throw new Error("Missing candidate build hook.");
      await expect(start.call({} as never, {} as never)).rejects.toBe(reason);
      expect(NodeFS.existsSync(candidate.outDir)).toBe(false);
    }));

  it("refuses report publication before observing a graph", () =>
    withCandidateRoot(async (root) => {
      await expect(offlinePreparationCandidatePack(root).onSuccess()).rejects.toThrow(/incomplete/);
    }));

  it("refuses prior candidate evidence and only admits a fresh empty child", () =>
    withArtifact((root) => {
      const child = NodePath.join(NodeFS.realpathSync(root), "candidate");
      requireEmptyPreparationCandidateOutput(child);
      NodeFS.mkdirSync(child);
      requireEmptyPreparationCandidateOutput(child);
      NodeFS.writeFileSync(
        NodePath.join(child, "candidate-graph.json"),
        '{"qualification":"complete"}',
      );
      expect(() => requireEmptyPreparationCandidateOutput(child)).toThrow(/retained/);
      expect(NodeFS.readFileSync(NodePath.join(child, "candidate-graph.json"), "utf8")).toBe(
        '{"qualification":"complete"}',
      );
    }));

  it("rejects symlink output without reading or deleting its target", () =>
    withArtifact((root) => {
      const alias = NodePath.join(NodeFS.realpathSync(root), "candidate-alias");
      NodeFS.symlinkSync(root, alias);
      expect(() => requireEmptyPreparationCandidateOutput(alias)).toThrow(/retained/);
      expect(NodeFS.existsSync(NodePath.join(root, "provenance.json"))).toBe(true);
    }));
  it("canonicalizes an explicit temporary-parent alias without changing the fixed output", () =>
    withCandidateRoot(async (root) => {
      const actual = NodePath.join(root, "actual"),
        alias = NodePath.join(root, "alias");
      NodeFS.mkdirSync(actual, { mode: 0o700 });
      NodeFS.symlinkSync(actual, alias);
      expect(offlinePreparationCandidatePack(alias).outDir).toBe(
        NodePath.join(NodeFS.realpathSync(actual), "apps/server/dist-exe/preparation-candidate"),
      );
      expect(NodeFS.readdirSync(actual)).toEqual([]);
    }));

  it("refuses a missing output beneath a foreign ancestor before source commands or child creation", () =>
    withCandidateRoot(async (root) => {
      const checkout = NodePath.join(root, "checkout"),
        foreign = NodePath.join(root, "foreign");
      NodeFS.mkdirSync(NodePath.join(checkout, "apps/server"), { recursive: true, mode: 0o700 });
      NodeFS.mkdirSync(foreign, { mode: 0o700 });
      NodeFS.writeFileSync(NodePath.join(foreign, "sentinel"), "preserved");
      NodeFS.symlinkSync(foreign, NodePath.join(checkout, "apps/server/dist-exe"));
      const candidate = offlinePreparationCandidatePack(checkout);
      const start = candidate.plugins[0]!.buildStart;
      if (typeof start !== "function") throw new Error("Missing candidate build hook.");
      await expect(start.call({} as never, {} as never)).rejects.toThrow(/ancestor/);
      expect(NodeFS.readdirSync(foreign)).toEqual(["sentinel"]);
      expect(NodeFS.readFileSync(NodePath.join(foreign, "sentinel"), "utf8")).toBe("preserved");
    }));

  it("retains replaced ancestors and refuses their captured identity even when the leaf is empty", () =>
    withCandidateRoot(async (root) => {
      const parent = NodePath.join(root, "parent"),
        output = NodePath.join(parent, "candidate");
      NodeFS.mkdirSync(output, { recursive: true, mode: 0o700 });
      const verify = requireEmptyPreparationCandidateOutput(output);
      NodeFS.renameSync(parent, NodePath.join(root, "retained-parent"));
      NodeFS.mkdirSync(output, { recursive: true, mode: 0o700 });
      expect(verify).toThrow(/identity changed/);
      expect(NodeFS.readdirSync(root).sort()).toEqual(["parent", "retained-parent"]);
      expect(NodeFS.readdirSync(output)).toEqual([]);
    }));

  it("permits owned emitted files while preserving the captured directory chain", () =>
    withCandidateRoot(async (root) => {
      const output = NodePath.join(root, "candidate");
      NodeFS.mkdirSync(output, { mode: 0o700 });
      const verify = requireEmptyPreparationCandidateOutput(output);
      NodeFS.writeFileSync(NodePath.join(output, "entry.mjs"), "export {};");
      expect(verify).not.toThrow();
      expect(() => requireEmptyPreparationCandidateOutput(output)).toThrow(/retained/);
    }));

  it("refuses writable output ancestors without creating or removing a child", () =>
    withCandidateRoot(async (root) => {
      const parent = NodePath.join(root, "parent");
      NodeFS.mkdirSync(parent, { mode: 0o700 });
      NodeFS.chmodSync(parent, 0o777);
      expect(() =>
        requireEmptyPreparationCandidateOutput(NodePath.join(parent, "candidate")),
      ).toThrow(/ancestor/);
      expect(NodeFS.readdirSync(parent)).toEqual([]);
    }));
});
