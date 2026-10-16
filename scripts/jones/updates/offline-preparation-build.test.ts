// @effect-diagnostics-next-line nodeBuiltinImport:off -- These tests allocate and remove only one captured synthetic artifact directory per case.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths identify only the test-owned directory.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture digests bind exact synthetic bytes.
import * as NodeCrypto from "node:crypto";
import * as NodeURL from "node:url";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Git commands initialize only the captured synthetic checkout and have a fixed deadline.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { capturePreparationProducer } from "./offline-preparation-producer-loader.mjs";
import {
  preparationSeaConfig,
  preparePreparationOutput,
  offlinePreparationPack,
  offlinePreparationCandidatePack,
  offlinePreparationProducerCandidatePack,
  inspectPreparationCandidateJavaScript,
  inspectPreparationCandidateCode,
  inspectPreparationCandidateExternal,
  inspectPreparationCandidateImports,
  inspectPreparationGeneratedRuntime,
  inspectPreparationCandidateTools,
  requireEmptyPreparationCandidateOutput,
  runPreparationLifecycleFixture,
  verifyNodeArchive,
  verifyPreparationArtifact,
  verifyPreparationClosure,
  verifyPreparationHeldOutput,
  verifyPreparationSeaConfig,
  verifyPreparationSourceOverlay,
  verifyPreparationSQLiteObservation,
  buildQualifiedPreparation,
  stageQualifiedPreparation,
  type QualifiedPreparationHandle,
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

describe("same-process preparation producer handles", () => {
  const identity = {
    repository: "Jones-Systems/Jones-Code" as const,
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    version: "1.0.0-preview.20261011.1",
  };
  it.each([
    ["caller boolean", true],
    ["frozen lookalike", Object.freeze({ qualified: true })],
    [
      "serialized completion",
      JSON.parse('{"observation":"complete","preparationCapable":true}') as unknown,
    ],
    ["verifier callback", () => true],
    ["empty clone", structuredClone(Object.freeze({}))],
  ] as const)("refuses %s without opening the supplied destination", async (_name, value) => {
    let destinationRead = false;
    await expect(
      stageQualifiedPreparation({
        handle: value as QualifiedPreparationHandle,
        expectedSource: identity,
        get stageResourcesDir(): string {
          destinationRead = true;
          throw new Error("Destination must remain unopened");
        },
      }),
    ).rejects.toThrow(/absent, expired, or consumed/);
    expect(destinationRead).toBe(false);
  });

  it("pre-aborted build refuses before resolving paths or allocating native resources", async () => {
    const abort = new AbortController(),
      reason = new Error("owned pre-build abort");
    abort.abort(reason);
    let inspected = false;
    await expect(
      buildQualifiedPreparation({
        signal: abort.signal,
        get repoRoot(): string {
          inspected = true;
          throw new Error("No source read after abort");
        },
        get stageRoot(): string {
          inspected = true;
          throw new Error("No stage read after abort");
        },
      }),
    ).rejects.toBe(reason);
    expect(inspected).toBe(false);
  });

  it("refuses missing source bindings without creating any producer child or output", () =>
    withCandidateRoot(async (root) => {
      await expect(buildQualifiedPreparation({ repoRoot: root, stageRoot: root })).rejects.toThrow(
        /ENOENT/,
      );
      expect(NodeFS.readdirSync(root)).toEqual([]);
    }));

  it("refuses unreviewed entry bytes after matching raw bytes in a distinct source root", () =>
    withBoundProducerSource(async ({ sourceRoot, stageRoot, candidate }) => {
      expect(sourceRoot).not.toBe(
        NodeFS.realpathSync(NodeURL.fileURLToPath(new URL("../../../", import.meta.url))),
      );
      await expect(buildQualifiedPreparation({ repoRoot: sourceRoot, stageRoot })).rejects.toThrow(
        "Functional preparation entry is outside the reviewed worker policy.",
      );
      expect(NodeFS.readdirSync(stageRoot)).toEqual([]);
      expect(NodeFS.readdirSync(candidate).sort()).toEqual([
        "candidate-graph.json",
        "offlinePreparationEntry.mjs",
      ]);
    }));

  it.each([
    "scripts/package.json",
    "scripts/jones/updates/offline-preparation-producer-entry.ts",
    "scripts/jones/updates/functional-policy.ts",
    "scripts/jones/updates/legacy-bootstrap-bundle.ts",
    "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs",
  ])("refuses source/code byte disagreement in %s before policy or native work", (relative) =>
    withBoundProducerSource(async ({ sourceRoot, stageRoot }) => {
      NodeFS.appendFileSync(NodePath.join(sourceRoot, relative), "\nchanged fixture bytes\n");
      await expect(buildQualifiedPreparation({ repoRoot: sourceRoot, stageRoot })).rejects.toThrow(
        "Preparation checkout raw source differs from the bound code source.",
      );
      expect(NodeFS.readdirSync(stageRoot)).toEqual([]);
    }),
  );

  it("rechecks source drift after a prior matching request without minting a handle", () =>
    withBoundProducerSource(async ({ sourceRoot, stageRoot }) => {
      const build = () => buildQualifiedPreparation({ repoRoot: sourceRoot, stageRoot });
      await expect(build()).rejects.toThrow(
        "Functional preparation entry is outside the reviewed worker policy.",
      );
      const path = NodePath.join(sourceRoot, "scripts/jones/updates/offline-preparation-build.ts");
      const original = NodeFS.readFileSync(path);
      const changed = Buffer.from(original);
      changed[0] = changed[0]! ^ 1;
      NodeFS.writeFileSync(path, changed);
      await expect(build()).rejects.toThrow(
        "Preparation checkout raw source differs from the bound code source.",
      );
      NodeFS.writeFileSync(path, original);
      await expect(build()).rejects.toThrow(
        "Functional preparation entry is outside the reviewed worker policy.",
      );
      expect(NodeFS.readdirSync(stageRoot)).toEqual([]);
    }));

  it("refuses a missing late qualifier even when all producer-only bindings match", () =>
    withBoundProducerSource(async ({ sourceRoot, stageRoot }) => {
      NodeFS.unlinkSync(
        NodePath.join(sourceRoot, "scripts/jones/updates/legacy-preparation-native-fixture.mjs"),
      );
      await expect(buildQualifiedPreparation({ repoRoot: sourceRoot, stageRoot })).rejects.toThrow(
        /ENOENT/,
      );
      expect(NodeFS.readdirSync(stageRoot)).toEqual([]);
    }));

  it("refuses an aliased stage before candidate reads or native work", () =>
    withCandidateRoot(async (root) => {
      const actual = NodePath.join(root, "actual"),
        alias = NodePath.join(root, "alias");
      NodeFS.mkdirSync(actual, { mode: 0o700 });
      NodeFS.symlinkSync(actual, alias);
      await expect(
        buildQualifiedPreparation({
          repoRoot: NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
          stageRoot: alias,
        }),
      ).rejects.toThrow(/aliased or unowned/);
      expect(NodeFS.readdirSync(actual)).toEqual([]);
    }));
});

async function withQualifiedProducerStage(
  body: (input: {
    repoRoot: string;
    stageRoot: string;
    resources: string;
    source: {
      repository: "Jones-Systems/Jones-Code";
      sourceSha: string;
      sourceTree: string;
      version: string;
    };
  }) => Promise<void>,
) {
  const outer = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-qualified-preparation-test-")),
  );
  process.stderr.write(
    `${JSON.stringify({ event: "qualified-preparation-test-root", root: outer })}\n`,
  );
  const captured = NodeFS.lstatSync(outer);
  let primary: unknown;
  try {
    const stageRoot = NodePath.join(outer, "stage"),
      resources = NodePath.join(stageRoot, "resources");
    NodeFS.mkdirSync(resources, { recursive: true, mode: 0o700 });
    const repoRoot = NodeFS.realpathSync(
      NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
    );
    const graph = JSON.parse(
      NodeFS.readFileSync(
        NodePath.join(repoRoot, "apps/server/dist-exe/preparation-candidate/candidate-graph.json"),
        "utf8",
      ),
    ) as { source: { sha: string; tree: string; version: string } };
    await body({
      repoRoot,
      stageRoot,
      resources,
      source: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: graph.source.sha,
        sourceTree: graph.source.tree,
        version: graph.source.version,
      },
    });
  } catch (cause) {
    primary = cause;
  }
  const stageMustRemain = (cause: unknown, depth = 0): boolean => {
    if (depth > 8 || typeof cause !== "object" || cause === null) return false;
    return (
      ("preserveStageRoot" in cause && cause.preserveStageRoot === true) ||
      (cause instanceof AggregateError &&
        cause.errors.slice(0, 32).some((inner: unknown) => stageMustRemain(inner, depth + 1))) ||
      ("cause" in cause && stageMustRemain(cause.cause, depth + 1))
    );
  };
  if (stageMustRemain(primary))
    throw Object.assign(
      new AggregateError([primary], "Qualified producer test retains its owning stage."),
      { retainedRoot: outer },
    );
  try {
    const current = NodeFS.lstatSync(outer);
    expect(
      current.isDirectory() && current.dev === captured.dev && current.ino === captured.ino,
    ).toBe(true);
    NodeFS.rmSync(outer, { recursive: true, force: false });
    expect(NodeFS.existsSync(outer)).toBe(false);
  } catch (cleanup) {
    throw new AggregateError(
      primary === undefined ? [cleanup] : [primary, cleanup],
      `Qualified producer test cleanup failed: ${outer}`,
    );
  }
  if (primary !== undefined) throw primary;
}

// These cases mint only through the actual native ten-case producer. A skipped case supplies no qualification evidence.
const nativeProducerTest =
  process.env.JONES_PREPARATION_PRODUCER_NATIVE_TESTS === "1" ? it : it.skip;
nativeProducerTest(
  "native producer rejects copied handles, stages exact bytes once, then refuses replay",
  () =>
    withQualifiedProducerStage(async ({ repoRoot, stageRoot, resources, source }) => {
      const handle = await buildQualifiedPreparation({ repoRoot, stageRoot });
      for (const copy of [
        Object.assign({}, handle),
        structuredClone(handle),
        JSON.parse(JSON.stringify(handle)) as unknown,
        Object.create(handle) as unknown,
      ])
        await expect(
          stageQualifiedPreparation({
            handle: copy as QualifiedPreparationHandle,
            expectedSource: source,
            stageResourcesDir: resources,
          }),
        ).rejects.toThrow(/absent, expired, or consumed/);
      expect(NodeFS.readdirSync(resources)).toEqual([]);
      const result = await stageQualifiedPreparation({
        handle,
        expectedSource: source,
        stageResourcesDir: resources,
      });
      const runtime = NodeFS.readFileSync(
        NodePath.join(resources, result.bootstrapRuntime.relativePath),
      );
      expect(NodeCrypto.createHash("sha256").update(runtime).digest("hex")).toBe(
        result.bootstrapRuntime.sha256,
      );
      expect(runtime.length).toBe(result.bootstrapRuntime.size);
      expect(
        NodeFS.statSync(NodePath.join(resources, result.bootstrapRuntime.relativePath)).mode &
          0o777,
      ).toBe(0o500);
      expect(result.diagnosticEvidence.nativeColdOwnerQualified).toBe(false);
      const before = NodeFS.readdirSync(NodePath.join(resources, "jones-bootstrap")).sort();
      await expect(
        stageQualifiedPreparation({ handle, expectedSource: source, stageResourcesDir: resources }),
      ).rejects.toThrow(/absent, expired, or consumed/);
      expect(NodeFS.readdirSync(NodePath.join(resources, "jones-bootstrap")).sort()).toEqual(
        before,
      );
    }),
  30 * 60_000,
);

nativeProducerTest(
  "native producer revokes an issued handle on cancellation before staging",
  () =>
    withQualifiedProducerStage(async ({ repoRoot, stageRoot, resources, source }) => {
      const abort = new AbortController();
      const handle = await buildQualifiedPreparation({ repoRoot, stageRoot, signal: abort.signal });
      abort.abort(new Error("owned cancellation after native qualification"));
      await expect(
        stageQualifiedPreparation({ handle, expectedSource: source, stageResourcesDir: resources }),
      ).rejects.toThrow(/absent, expired, or consumed/);
      expect(NodeFS.readdirSync(resources)).toEqual([]);
    }),
  30 * 60_000,
);

nativeProducerTest(
  "native producer refuses a replaced stage incarnation and consumes the stale handle",
  () =>
    withQualifiedProducerStage(async ({ repoRoot, stageRoot, resources, source }) => {
      const handle = await buildQualifiedPreparation({ repoRoot, stageRoot });
      NodeFS.renameSync(stageRoot, stageRoot + "-retained");
      NodeFS.mkdirSync(resources, { recursive: true, mode: 0o700 });
      await expect(
        stageQualifiedPreparation({ handle, expectedSource: source, stageResourcesDir: resources }),
      ).rejects.toThrow(/stage identity changed/);
      expect(NodeFS.readdirSync(resources)).toEqual([]);
      await expect(
        stageQualifiedPreparation({ handle, expectedSource: source, stageResourcesDir: resources }),
      ).rejects.toThrow(/absent, expired, or consumed/);
    }),
  30 * 60_000,
);

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
    const custodyUnknown = (cause: unknown, depth = 0): boolean => {
      if (depth > 8 || typeof cause !== "object" || cause === null) return false;
      return (
        ("preserveScratch" in cause && cause.preserveScratch === true) ||
        ("retainedRoot" in cause && typeof cause.retainedRoot === "string") ||
        (cause instanceof AggregateError &&
          cause.errors.slice(0, 32).some((inner: unknown) => custodyUnknown(inner, depth + 1))) ||
        ("cause" in cause && custodyUnknown(cause.cause, depth + 1))
      );
    };
    if (custodyUnknown(failure))
      throw Object.assign(
        new AggregateError(
          [failure],
          "Candidate fixture retains its checkout after uncertain custody.",
        ),
        { retainedRoot: root },
      );
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

function producerCandidateSourceFiles() {
  return new Set([
    ...Object.keys(artifactManifest("", "", "").source.hashes),
    "scripts/package.json",
    "scripts/jones/updates/offline-preparation-producer-entry.ts",
    "scripts/jones/updates/functional-policy.ts",
    "scripts/jones/updates/legacy-bootstrap-bundle.ts",
    "scripts/jones/updates/legacy-preparation-native-fixture.mjs",
    "scripts/jones/updates/legacy-preparation-native-fixture.d.mts",
    "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs",
    "scripts/jones/updates/legacy-identity-native-fixture.mjs",
    ...[
      "jonesLegacyBootstrapSource.ts",
      "jonesLegacyMaintenanceSource.ts",
      "jonesNativeHelperSource.ts",
      "jonesPairedStateSource.ts",
      "jonesColdPairSource.ts",
      "jonesLegacyIdentityNative.c",
      "jonesColdReceiverSource.ts",
      "jonesColdOwnerSource.ts",
      "jonesPreparationWorkerSource.ts",
    ].map((name) => `apps/desktop/src/jones/updates/${name}`),
    "apps/server/src/jones/updates/offlinePreparationWorker.test.ts",
  ]);
}

async function withBoundProducerSource(
  run: (input: { sourceRoot: string; stageRoot: string; candidate: string }) => Promise<void>,
) {
  await withCandidateRoot(async (root) => {
    const sourceRoot = NodePath.join(root, "source"),
      stageRoot = NodePath.join(root, "stage"),
      candidate = NodePath.join(sourceRoot, "apps/server/dist-exe/preparation-candidate");
    NodeFS.mkdirSync(candidate, { recursive: true, mode: 0o700 });
    NodeFS.mkdirSync(stageRoot, { mode: 0o700 });
    const codeRoot = NodeFS.realpathSync(
      NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
    );
    const files = producerCandidateSourceFiles();
    expect(files.size).toBe(33);
    for (const name of files) {
      const destination = NodePath.join(sourceRoot, name);
      NodeFS.mkdirSync(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(destination, NodeFS.readFileSync(NodePath.join(codeRoot, name)), {
        flag: "wx",
        mode: 0o600,
      });
    }
    NodeFS.writeFileSync(NodePath.join(candidate, "candidate-graph.json"), "{}\n", {
      flag: "wx",
      mode: 0o600,
    });
    NodeFS.writeFileSync(
      NodePath.join(candidate, "offlinePreparationEntry.mjs"),
      'throw new Error("raw binding fixture must never execute");\n',
      { flag: "wx", mode: 0o600 },
    );
    await run({ sourceRoot, stageRoot, candidate });
    expect(NodeFS.readdirSync(root).sort()).toEqual(["source", "stage"]);
  });
}

async function withProducerCandidateCheckout(
  run: (root: string) => Promise<void>,
  options: {
    readonly absent?: string;
    readonly oversized?: string;
    readonly contents?: Readonly<Record<string, string>>;
  } = {},
) {
  await withCandidateRoot(async (root) => {
    const files = new Set([
      ...producerCandidateSourceFiles(),
      ...Object.keys(options.contents ?? {}),
    ]);
    for (const path of files) {
      if (path === options.absent) continue;
      const destination = NodePath.join(root, path);
      NodeFS.mkdirSync(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(
        destination,
        path === options.oversized
          ? Buffer.alloc(8 * 1024 * 1024 + 1)
          : path.endsWith("package.json")
            ? JSON.stringify({ name: "synthetic-producer-package", version: "1.0.0" })
            : path === "pnpm-workspace.yaml"
              ? "patchedDependencies:\n"
              : (options.contents?.[path] ?? "export {};\n"),
        { flag: "wx", mode: 0o600 },
      );
    }
    NodeFS.writeFileSync(
      NodePath.join(root, ".gitignore"),
      "apps/server/dist-exe/\nnode_modules/\n",
    );
    const git = (...args: string[]) =>
      NodeChildProcess.execFileSync("/usr/bin/git", args, {
        cwd: root,
        timeout: 10_000,
        stdio: "pipe",
        env: {
          ...process.env,
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_TERMINAL_PROMPT: "0",
        },
      });
    git("init", "--quiet");
    git("add", "--all");
    git(
      "-c",
      "user.name=Candidate Fixture",
      "-c",
      "user.email=fixture@example.invalid",
      "commit",
      "--quiet",
      "-m",
      "synthetic candidate inputs",
    );
    await run(root);
  });
}

async function startProducerCandidate(root: string) {
  const candidate = offlinePreparationProducerCandidatePack(root);
  const start = candidate.plugins[0]!.buildStart;
  if (typeof start !== "function") throw new Error("Missing producer candidate build hook.");
  await start.call({} as never, {} as never);
  return candidate;
}

function emitProducerCandidate(
  candidate: ReturnType<typeof offlinePreparationProducerCandidatePack>,
  root: string,
  code: string,
  options: {
    readonly fileName?: string;
    readonly moduleCount?: number;
    readonly moduleId?: string;
  } = {},
) {
  const fileName = options.fileName ?? "offlinePreparationProducer.mjs";
  const id =
    options.moduleId ??
    NodePath.join(root, "scripts/jones/updates/offline-preparation-producer-entry.ts");
  const generate = candidate.plugins[0]!.generateBundle;
  if (typeof generate !== "function") throw new Error("Missing producer candidate graph hook.");
  generate.call(
    {
      getModuleIds: () =>
        options.moduleCount
          ? Array.from({ length: options.moduleCount }, (_, index) => `${id}?${index}`)
          : [id],
      getModuleInfo: () => ({
        isEntry: true,
        importedIds: ["node:fs"],
        dynamicallyImportedIds: [],
        code,
      }),
      meta: {},
    } as never,
    {} as never,
    {
      [fileName]: {
        type: "chunk",
        isEntry: true,
        fileName,
        code,
        modules: { [id]: { renderedLength: code.length, code } },
        imports: ["node:fs"],
        dynamicImports: [],
        map: null,
      },
    } as never,
    false,
  );
}

describe("unqualified preparation producer graph", () => {
  it("feeds the actual collector's fixed 14/29 bindings to a held capture without executing its output", () =>
    withProducerCandidateCheckout(async (root) => {
      const candidate = await startProducerCandidate(root);
      const code = 'throw new Error("collector output must remain unexecuted");\n';
      emitProducerCandidate(candidate, root, code);
      NodeFS.writeFileSync(NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"), code);
      await candidate.onSuccess();
      const stageRoot = NodePath.join(root, "capture-stage");
      NodeFS.mkdirSync(stageRoot, { mode: 0o700 });
      const captured = await capturePreparationProducer({ sourceRoot: root, stageRoot });
      expect(captured.status).toBe("held");
      expect(captured.imported).toBe(false);
      expect(captured.files).toHaveLength(35);
      expect(
        captured.files.find((file) => file.capturedPath.endsWith("offlinePreparationProducer.mjs"))
          ?.sha256,
      ).toBe(sha(code));
      expect(captured.cleanup).toEqual({ disposition: "removed", root: captured.codeRoot });
      expect(NodeFS.existsSync(captured.codeRoot)).toBe(false);
      expect(NodeFS.readdirSync(stageRoot)).toEqual([]);
      expect(Object.keys(artifactManifest("", "", "").source.hashes)).toHaveLength(15);
    }));

  it("keeps the producer, SEA candidate and held outputs separate without a runtime package whitelist", () =>
    withArtifact((root) => {
      const producer = offlinePreparationProducerCandidatePack(root);
      const sea = offlinePreparationCandidatePack(root);
      const held = offlinePreparationPack(root);
      expect(producer.entry).toEqual({
        offlinePreparationProducer: NodePath.join(
          NodeFS.realpathSync(root),
          "scripts/jones/updates/offline-preparation-producer-entry.ts",
        ),
      });
      expect(sea.entry).toEqual({
        offlinePreparationEntry: "src/jones/updates/offlinePreparationEntry.ts",
      });
      expect(new Set([producer.outDir, sea.outDir, held.outDir]).size).toBe(3);
      expect(producer.clean).toBe(false);
      expect("exe" in producer).toBe(false);
      expect(producer.deps.alwaysBundle("effect/Schema")).toBe(true);
      expect(producer.deps.alwaysBundle("typescript-legacy")).toBe(true);
      expect(producer.deps.neverBundle("typescript-legacy")).toBe(false);
      expect(producer.deps.neverBundle("node:fs")).toBe(true);
      expect(Object.keys(verifyPreparationArtifact(root).source.hashes)).toHaveLength(15);
    }));

  it("keeps missing and oversized fixed inputs as unavailable evidence", () =>
    withProducerCandidateCheckout(
      async (root) => {
        const candidate = await startProducerCandidate(root);
        emitProducerCandidate(candidate, root, "export {};\n");
        NodeFS.writeFileSync(
          NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"),
          "export {};\n",
        );
        await candidate.onSuccess();
        const report = JSON.parse(
          NodeFS.readFileSync(
            NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
            "utf8",
          ),
        ) as {
          source: { inputs: { path: string; content: { availability: string } }[] };
          lateQualifierSources: { files: { path: string; content: { availability: string } }[] };
        };
        expect(report.source.inputs).toHaveLength(14);
        expect(report.lateQualifierSources.files).toHaveLength(29);
        expect(
          report.source.inputs.find((item) => item.path.endsWith("functional-policy.ts"))!.content
            .availability,
        ).toBe("unavailable");
        expect(
          report.lateQualifierSources.files.find((item) => item.path.endsWith(".d.mts"))!.content
            .availability,
        ).toBe("unavailable");
      },
      {
        absent: "scripts/jones/updates/legacy-preparation-native-fixture.d.mts",
        oversized: "scripts/jones/updates/functional-policy.ts",
      },
    ));

  it.each([
    {
      name: "observed loader syntax",
      code: 'import "node:fs"; throw new Error("emitted producer must never execute");\n',
    },
    { name: "independent import and hazard counts", code: 'require("node:fs");\n'.repeat(2050) },
  ])("publishes exact unexecuted bytes with $name", ({ code, name }) =>
    withProducerCandidateCheckout(async (root) => {
      const candidate = await startProducerCandidate(root);
      emitProducerCandidate(candidate, root, code);
      NodeFS.writeFileSync(NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"), code);
      await candidate.onSuccess();
      const report = JSON.parse(
        NodeFS.readFileSync(
          NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
          "utf8",
        ),
      ) as {
        source: unknown;
        lateQualifierSources: { files: unknown[] };
        emitted: { syntax: unknown };
        modules: { staticImports: unknown[] }[];
      };
      expect(report).toMatchObject({
        classification: "preparation-producer-candidate-graph",
        qualification: "unqualified",
        observation: "partial",
        emittedProducerExecuted: false,
        completeDependencyClosure: false,
        loadedCollectorBytesAttested: false,
        emitted: { sha256: sha(code), size: Buffer.byteLength(code) },
      });
      expect(report.source).not.toHaveProperty("hashes");
      expect(report.lateQualifierSources.files).toHaveLength(29);
      expect(report.modules[0]!.staticImports[0]).toMatchObject({ id: "node:fs", builtin: true });
      if (name === "independent import and hazard counts")
        expect(report.emitted.syntax).toMatchObject({
          importsComplete: true,
          importCount: 2050,
          loaderHazardCount: 2050,
          loaderHazardsTruncated: false,
        });
      else
        expect(report.emitted.syntax).toMatchObject({
          imports: expect.arrayContaining([expect.objectContaining({ specifier: "node:fs" })]),
        });
      expect(
        NodeFS.existsSync(NodePath.join(root, "apps/server/dist-exe/preparation-candidate")),
      ).toBe(false);
      expect(NodeFS.existsSync(NodePath.join(candidate.outDir, "jones-offline-preparation"))).toBe(
        false,
      );
    }),
  );

  it("observes a later dynamic import after truncating producer reflection samples", () =>
    withProducerCandidateCheckout(async (root) => {
      const expression = 'import("./after-samples.mjs")';
      const code =
        'const label = "🙂";\n' + 'globalThis["sample"];\n'.repeat(4097) + expression + ";\n";
      expect(() => inspectPreparationCandidateJavaScript(code)).toThrow(
        /Candidate loader observations exceed bound/,
      );
      const candidate = await startProducerCandidate(root);
      emitProducerCandidate(candidate, root, code);
      NodeFS.writeFileSync(NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"), code);
      await candidate.onSuccess();
      const report = JSON.parse(
        NodeFS.readFileSync(
          NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
          "utf8",
        ),
      ) as {
        emitted: {
          syntax: {
            imports: { location: { start: number; end: number } }[];
            loaderHazards: unknown[];
          };
          importEvidence: unknown;
        };
      };
      expect(report.emitted.syntax).toMatchObject({
        importsComplete: true,
        importCount: 1,
        importLimit: 4096,
        loaderHazardCount: 4097,
        loaderHazardSampleLimit: 4096,
        loaderHazardsTruncated: true,
        imports: [
          {
            kind: "dynamic",
            specifier: "./after-samples.mjs",
            expressionSha256: sha(expression),
            location: {
              file: "offlinePreparationProducer.mjs",
              unit: "utf16-code-units",
              origin: 0,
              start: code.indexOf(expression),
              end: code.indexOf(expression) + expression.length,
              endExclusive: true,
            },
          },
        ],
      });
      expect(report.emitted.syntax.loaderHazards).toHaveLength(4096);
      const { start, end } = report.emitted.syntax.imports[0]!.location;
      expect(code.slice(start, end)).toBe(expression);
      expect(report.emitted.importEvidence).toMatchObject({
        dynamic: { emitted: ["./after-samples.mjs"], syntaxOnly: ["./after-samples.mjs"] },
        qualification: "unqualified",
      });
    }));

  it.each([
    {
      name: "import overflow",
      code: 'import("node:fs");\n'.repeat(4097),
      reason: "Producer candidate imports exceed bound.",
    },
    {
      name: "parser failure",
      code: "export const = ;",
      reason: "Candidate entry is not valid JavaScript.",
    },
  ])("reports unavailable syntax without completion after $name", ({ code, reason }) =>
    withProducerCandidateCheckout(async (root) => {
      const candidate = await startProducerCandidate(root);
      emitProducerCandidate(candidate, root, code);
      NodeFS.writeFileSync(NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"), code);
      await candidate.onSuccess();
      const report = JSON.parse(
        NodeFS.readFileSync(
          NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
          "utf8",
        ),
      ) as { emitted: { syntax: unknown; importEvidence: unknown } };
      expect(report.emitted.syntax).toEqual({ availability: "unavailable", reason });
      expect(report.emitted.importEvidence).toEqual({ availability: "unavailable", reason });
      expect(report.emitted.syntax).not.toHaveProperty("importsComplete");
      expect(report).toMatchObject({
        qualification: "unqualified",
        completeDependencyClosure: false,
      });
    }),
  );

  it("records the three fixed late modules and separates observed, computed and unresolved edges", () => {
    const preparation = "scripts/jones/updates/legacy-preparation-native-fixture.mjs";
    const cold = "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs";
    const identity = "scripts/jones/updates/legacy-identity-native-fixture.mjs";
    const extra = "scripts/jones/updates/additional-fixture.mjs";
    const coldSource = 'export { observation } from "./legacy-identity-native-fixture.mjs";\n';
    return withProducerCandidateCheckout(
      async (root) => {
        const candidate = await startProducerCandidate(root);
        emitProducerCandidate(candidate, root, "export {};\n");
        NodeFS.writeFileSync(
          NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"),
          "export {};\n",
        );
        await candidate.onSuccess();
        const report = JSON.parse(
          NodeFS.readFileSync(
            NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
            "utf8",
          ),
        ) as {
          lateQualifierSources: {
            modules: { path: string; syntax: unknown; importEdges: unknown }[];
          };
        };
        expect(report.lateQualifierSources.modules.map((item) => item.path)).toEqual([
          preparation,
          cold,
          identity,
        ]);
        const module = report.lateQualifierSources.modules[0]!;
        expect(module.syntax).toMatchObject({
          availability: "observed",
          importsComplete: true,
          imports: expect.arrayContaining([
            expect.objectContaining({
              specifier: "./legacy-cold-maintenance-fixture.mjs",
              location: expect.objectContaining({ file: preparation, unit: "utf16-code-units" }),
            }),
          ]),
        });
        expect(module.importEdges).toMatchObject({
          availability: "observed",
          literalSiblingFiles: [
            { target: cold, content: { availability: "observed", sha256: sha(coldSource) } },
          ],
          builtins: [{ specifier: "node:fs" }],
          computedImports: [{ kind: "dynamic", specifier: null, computed: true }],
          unresolvedEdges: [
            {
              target: extra,
              reason: "literal-sibling-outside-fixed-inventory",
              content: { availability: "observed" },
            },
            {
              reason: "literal-import-not-a-fixed-sibling-or-builtin",
              import: { specifier: "not-installed" },
            },
          ],
          builtinIdentityObserved: false,
          completeDependencyClosure: false,
        });
        expect(report.lateQualifierSources.modules[1]!.importEdges).toMatchObject({
          literalSiblingFiles: [{ target: identity, content: { availability: "observed" } }],
        });
        expect(report).toMatchObject({
          qualification: "unqualified",
          loadedCollectorBytesAttested: false,
        });
      },
      {
        contents: {
          [preparation]:
            'import "./legacy-cold-maintenance-fixture.mjs"; import "node:fs"; import(selected); import "./additional-fixture.mjs"; import "not-installed";\n',
          [cold]: coldSource,
          [identity]: "export const observation = 1;\n",
          [extra]: "export {};\n",
        },
      },
    );
  });

  it("keeps missing late modules and their literal edges unavailable", () => {
    const preparation = "scripts/jones/updates/legacy-preparation-native-fixture.mjs";
    const identity = "scripts/jones/updates/legacy-identity-native-fixture.mjs";
    return withProducerCandidateCheckout(
      async (root) => {
        const candidate = await startProducerCandidate(root);
        emitProducerCandidate(candidate, root, "export {};\n");
        NodeFS.writeFileSync(
          NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"),
          "export {};\n",
        );
        await candidate.onSuccess();
        const report = JSON.parse(
          NodeFS.readFileSync(
            NodePath.join(candidate.outDir, "producer-candidate-graph.json"),
            "utf8",
          ),
        ) as {
          lateQualifierSources: {
            modules: { path: string; syntax: unknown; importEdges: unknown }[];
          };
        };
        expect(report.lateQualifierSources.modules[0]!.importEdges).toMatchObject({
          literalSiblingFiles: [],
          unresolvedEdges: [
            {
              target: identity,
              reason: "literal-sibling-content-not-observed",
              content: { availability: "unavailable" },
            },
          ],
        });
        expect(
          report.lateQualifierSources.modules.find((item) => item.path === identity),
        ).toMatchObject({
          syntax: { availability: "unavailable" },
          importEdges: { availability: "unavailable" },
        });
      },
      {
        absent: identity,
        contents: { [preparation]: 'import "./legacy-identity-native-fixture.mjs";\n' },
      },
    );
  });

  it("rechecks observed additional late import bytes before reporting", () => {
    const preparation = "scripts/jones/updates/legacy-preparation-native-fixture.mjs";
    const extra = "node_modules/additional-late-fixture.mjs";
    return withProducerCandidateCheckout(
      async (root) => {
        const candidate = await startProducerCandidate(root);
        emitProducerCandidate(candidate, root, "export {};\n");
        NodeFS.writeFileSync(
          NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"),
          "export {};\n",
        );
        NodeFS.appendFileSync(NodePath.join(root, extra), "\n// changed after observation\n");
        await expect(candidate.onSuccess()).rejects.toThrow(/source inputs changed/);
        expect(
          NodeFS.existsSync(NodePath.join(candidate.outDir, "producer-candidate-graph.json")),
        ).toBe(false);
      },
      {
        contents: {
          [preparation]: 'import "../../../node_modules/additional-late-fixture.mjs";\n',
          [extra]: "export {};\n",
        },
      },
    );
  });

  it("refuses SEA entry names and excessive module graphs without publishing a report", () =>
    withProducerCandidateCheckout(async (root) => {
      const candidate = await startProducerCandidate(root);
      expect(() =>
        emitProducerCandidate(candidate, root, "export {};", {
          fileName: "offlinePreparationEntry.mjs",
        }),
      ).toThrow(/fixed ESM entry/);
      expect(() =>
        emitProducerCandidate(candidate, root, "export {};", { moduleCount: 4097 }),
      ).toThrow(/module bound/);
      expect(NodeFS.readdirSync(candidate.outDir)).toEqual([]);
    }));

  it.each(["functional-policy.ts", "legacy-preparation-native-fixture.mjs"])(
    "rejects a fixed %s source change before observing the graph",
    (name) =>
      withProducerCandidateCheckout(async (root) => {
        const candidate = await startProducerCandidate(root);
        NodeFS.appendFileSync(
          NodePath.join(root, "scripts/jones/updates", name),
          '\nimport("node:fs");\n',
        );
        expect(() => emitProducerCandidate(candidate, root, "export {};")).toThrow(
          /source inputs changed/,
        );
        expect(NodeFS.readdirSync(candidate.outDir)).toEqual([]);
      }),
  );

  it("rejects changed emitted bytes before reporting and retains those bytes", () =>
    withProducerCandidateCheckout(async (root) => {
      const candidate = await startProducerCandidate(root);
      emitProducerCandidate(candidate, root, "export {};\n");
      const output = NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs");
      NodeFS.writeFileSync(output, "changed emitted bytes");
      await expect(candidate.onSuccess()).rejects.toThrow(/emitted bytes changed/);
      expect(NodeFS.readFileSync(output, "utf8")).toBe("changed emitted bytes");
      expect(
        NodeFS.existsSync(NodePath.join(candidate.outDir, "producer-candidate-graph.json")),
      ).toBe(false);
    }));

  it.each(["index.js", "package.json"])(
    "rejects drift in an observed installed %s before reporting",
    (changed) =>
      withProducerCandidateCheckout(async (root) => {
        const directory = NodePath.join(root, "node_modules/synthetic-dependency");
        NodeFS.mkdirSync(directory, { recursive: true, mode: 0o700 });
        const moduleId = NodePath.join(directory, "index.js");
        NodeFS.writeFileSync(moduleId, "export {};\n");
        NodeFS.writeFileSync(
          NodePath.join(directory, "package.json"),
          '{"name":"synthetic-dependency","version":"1.0.0"}',
        );
        const candidate = await startProducerCandidate(root);
        emitProducerCandidate(candidate, root, "export {};\n", { moduleId });
        NodeFS.writeFileSync(
          NodePath.join(candidate.outDir, "offlinePreparationProducer.mjs"),
          "export {};\n",
        );
        NodeFS.appendFileSync(NodePath.join(directory, changed), "\n ");
        await expect(candidate.onSuccess()).rejects.toThrow(/graph inputs changed/);
        expect(
          NodeFS.existsSync(NodePath.join(candidate.outDir, "producer-candidate-graph.json")),
        ).toBe(false);
      }),
  );

  it("refuses publication without a graph and preserves pre-cancellation", () =>
    withCandidateRoot(async (root) => {
      await expect(offlinePreparationProducerCandidatePack(root).onSuccess()).rejects.toThrow(
        /incomplete/,
      );
      const reason = Object.freeze(new Error("producer collector cancelled"));
      const candidate = offlinePreparationProducerCandidatePack(root, {
        signal: AbortSignal.abort(reason),
      });
      const start = candidate.plugins[0]!.buildStart;
      if (typeof start !== "function") throw new Error("Missing producer candidate build hook.");
      await expect(start.call({} as never, {} as never)).rejects.toBe(reason);
      expect(NodeFS.existsSync(candidate.outDir)).toBe(false);
    }));
});

describe("unqualified preparation candidate graph", () => {
  it("keeps emitted imports and builtin calls separate from stale bundler metadata", () => {
    const report = inspectPreparationCandidateImports(
      'import "node:fs"; process.getBuiltinModule("module"); process["getBuiltinModule"]("node:sqlite"); process.getBuiltinModule(selected); import("./actual.mjs");',
      { imports: ["node:module", "node:fs"], dynamicImports: ["offlinePreparationEntry.mjs"] },
    );
    expect(report.static).toEqual({
      emitted: ["node:fs"],
      reported: ["node:fs", "node:module"],
      metadataOnly: ["node:module"],
      syntaxOnly: [],
    });
    expect(report.dynamic).toEqual({
      emitted: ["./actual.mjs"],
      reported: ["offlinePreparationEntry.mjs"],
      metadataOnly: ["offlinePreparationEntry.mjs"],
      syntaxOnly: ["./actual.mjs"],
    });
    expect(
      report.processBuiltinCalls.map(({ specifier, computed, builtin }) => ({
        specifier,
        computed,
        builtin,
      })),
    ).toEqual([
      { specifier: "module", computed: false, builtin: true },
      { specifier: "node:sqlite", computed: false, builtin: true },
      { specifier: null, computed: true, builtin: false },
    ]);
    expect(report.qualification).toBe("unqualified");
    expect(() => verifyPreparationClosure('process.getBuiltinModule("module");')).toThrow();
  });

  it("records the emitted globalThis process optional-chain builtin spelling", () => {
    const report = inspectPreparationCandidateImports(
      'function emitted() { return globalThis.process?.getBuiltinModule?.("node:async_hooks")?.AsyncResource; }',
      { imports: [], dynamicImports: [] },
    );
    expect(report.processBuiltinCalls).toEqual([
      {
        kind: "process-builtin",
        specifier: "node:async_hooks",
        computed: false,
        builtin: true,
        expressionSha256: sha('globalThis.process?.getBuiltinModule?.("node:async_hooks")'),
      },
    ]);
    expect(report.syntax.loaderHazards.length).toBeGreaterThan(0);
    expect(report.qualification).toBe("unqualified");
  });

  it("does not promote strings, unrelated receivers or aliased builtin names into observed calls", () => {
    const syntax = inspectPreparationCandidateJavaScript(
      'const example = "process.getBuiltinModule(\\\"fs\\\")"; const optional = "globalThis.process?.getBuiltinModule?.(\\\"fs\\\")"; other.getBuiltinModule("fs"); other.process?.getBuiltinModule?.("fs"); globalThis.other?.getBuiltinModule?.("fs"); const load = process.getBuiltinModule; load("fs");',
    );
    expect(syntax.imports).toEqual([]);
    expect(syntax.loaderHazards.length).toBeGreaterThan(0);
  });

  it("binds the generated runtime's exact bytes without approving its helpers", () => {
    const body = "\nvar helper = () => 1;\n";
    const prefix = 'import "node:fs";\n//#region \\0rolldown/runtime.js';
    const source = prefix + body + "//#endregion\nexport const value = helper();\n";
    expect(inspectPreparationGeneratedRuntime(source)).toEqual({
      availability: "observed",
      evidence: "emitted-top-level-region",
      qualification: "unqualified",
      sha256: sha(body),
      size: Buffer.byteLength(body),
      startByte: Buffer.byteLength(prefix),
      endByte: Buffer.byteLength(prefix + body),
    });
    expect(inspectPreparationGeneratedRuntime(source.replace("=> 1", "=> 2"))).not.toEqual(
      inspectPreparationGeneratedRuntime(source),
    );
  });

  it.each([
    'const marker = "//#region \\\\0rolldown/runtime.js";',
    "//#region \\0rolldown/runtime.js\nvar helper = 1;",
    "//#region \\0rolldown/runtime.js\nvar one = 1;\n//#endregion\n//#region \\0rolldown/runtime.js\nvar two = 2;\n//#endregion",
  ])("leaves missing or ambiguous generated runtime evidence unavailable: %s", (source) => {
    expect(inspectPreparationGeneratedRuntime(source).availability).toBe("unavailable");
    expect(inspectPreparationGeneratedRuntime(source)).not.toHaveProperty("sha256");
  });

  it("observes installed tool entries through their own resolver without executing package code", () =>
    withArtifact((allocated) => {
      const root = NodeFS.realpathSync(allocated);
      const tool = (directory: string, name: string, exports: Record<string, string>) => {
        NodeFS.mkdirSync(directory, { recursive: true });
        const manifest = JSON.stringify({ name, version: "1.2.3", exports });
        NodeFS.writeFileSync(NodePath.join(directory, "package.json"), manifest, { flag: "wx" });
        for (const file of Object.values(exports)) {
          NodeFS.mkdirSync(NodePath.dirname(NodePath.join(directory, file)), { recursive: true });
          NodeFS.writeFileSync(
            NodePath.join(directory, file),
            'throw new Error("must not execute");',
            { flag: "wx" },
          );
        }
        return manifest;
      };
      const vite = NodePath.join(root, "node_modules/vite-plus");
      tool(vite, "vite-plus", { ".": "./dist/index.cjs", "./pack": "./dist/pack.cjs" });
      const tsdown = NodePath.join(vite, "node_modules/tsdown");
      const manifest = tool(tsdown, "tsdown", { ".": "./index.cjs" });
      NodeFS.linkSync(
        NodePath.join(tsdown, "package.json"),
        NodePath.join(root, "manifest-hardlink"),
      );
      NodeFS.linkSync(NodePath.join(tsdown, "index.cjs"), NodePath.join(root, "entry-hardlink"));
      const effect = NodePath.join(root, "node_modules/effect");
      tool(effect, "effect", { ".": "./index.cjs" });
      NodeFS.writeFileSync(
        NodePath.join(effect, "package.json"),
        JSON.stringify({ name: "effect", exports: { ".": "./index.cjs" } }),
      );
      const report = inspectPreparationCandidateTools(root, NodePath.join(root, "helper.cjs"));
      const observed = report.attempts.find(
        (attempt) => attempt.name === "tsdown" && attempt.package?.availability === "observed",
      );
      expect(observed?.resolverBase).toBe("node_modules/vite-plus/dist/index.cjs");
      expect(observed?.entry).toMatchObject({
        availability: "observed",
        sha256: sha('throw new Error("must not execute");'),
      });
      expect(observed?.package).toMatchObject({
        availability: "observed",
        name: "tsdown",
        version: "1.2.3",
        manifest: { sha256: sha(manifest) },
      });
      expect(report.unresolved).toContain("rolldown");
      expect(report.unresolved).toContain("effect");
      expect(report.attempts.find((attempt) => attempt.name === "effect")?.package).toMatchObject({
        availability: "observed",
        version: null,
        versionObservation: "not-declared",
      });
      expect(report.qualification).toBe("unqualified");
    }));

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
