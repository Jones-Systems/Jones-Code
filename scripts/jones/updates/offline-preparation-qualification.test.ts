// @effect-diagnostics nodeBuiltinImport:off -- These synthetic lifecycle tests own their small roots and never execute emitted code.
import NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { runHostedNativePreparation } from "./offline-preparation-qualification.ts";
import { buildAndStagePreparationProducer } from "./offline-preparation-producer-loader.mjs";

vi.mock("./offline-preparation-producer-loader.mjs", () => ({
  buildAndStagePreparationProducer: vi.fn(),
}));
const build = vi.mocked(buildAndStagePreparationProducer);
const names = [
  "apply",
  "reconcile",
  "repeat-plan",
  "repeat-apply",
  "durable-plan-drift",
  "wrong-scope",
  "paused-cancel",
  "closed-query-failure",
  "close-uncertainty",
  "custody-uncertainty",
];
const sha = "a".repeat(40),
  tree = "b".repeat(40);

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  build.mockReset();
});

async function fixture(body: (source: string, root: string) => Promise<void>) {
  const outer = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-hosted-driver-test-")),
  );
  const source = NodePath.join(outer, "source"),
    root = NodePath.join(outer, "run");
  const saved = [
    Object.getOwnPropertyDescriptor(process, "platform")!,
    Object.getOwnPropertyDescriptor(process, "arch")!,
    Object.getOwnPropertyDescriptor(process.versions, "node")!,
    Object.getOwnPropertyDescriptor(process, "execArgv")!,
  ];
  try {
    Object.defineProperty(process, "platform", { configurable: true, value: "darwin" });
    Object.defineProperty(process, "arch", { configurable: true, value: "arm64" });
    Object.defineProperty(process.versions, "node", { configurable: true, value: "24.13.1" });
    Object.defineProperty(process, "execArgv", { configurable: true, value: [] });
    NodeFS.mkdirSync(root, { mode: 0o700 });
    for (const name of ["home", "tmp"])
      NodeFS.mkdirSync(NodePath.join(root, name), { mode: 0o700 });
    const candidate = NodePath.join(source, "apps/server/dist-exe/preparation-candidate");
    NodeFS.mkdirSync(candidate, { recursive: true });
    NodeFS.writeFileSync(
      NodePath.join(candidate, "candidate-graph.json"),
      JSON.stringify({ source: { sha, tree, version: "0.0.45-preview.20261013.1" } }),
    );
    vi.stubEnv("HOME", NodePath.join(root, "home"));
    vi.stubEnv("TMPDIR", NodePath.join(root, "tmp"));
    vi.stubEnv("NODE_OPTIONS", "");
    vi.stubEnv("NODE_PATH", "");
    vi.stubEnv("JONES_EXPECTED_SOURCE_SHA", sha);
    vi.stubEnv("JONES_EXPECTED_SOURCE_TREE", tree);
    vi.spyOn(NodeFS, "statfsSync").mockReturnValue({
      bavail: 8n * 1024n * 1024n,
      bsize: 4096n,
    } as NodeFS.BigIntStatsFs);
    await body(source, root);
  } finally {
    Object.defineProperty(process, "platform", saved[0]!);
    Object.defineProperty(process, "arch", saved[1]!);
    Object.defineProperty(process.versions, "node", saved[2]!);
    Object.defineProperty(process, "execArgv", saved[3]!);
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    // The mocked producer starts no child and holds no descriptor. After retention assertions,
    // this test is the cleanup owner of these synthetic files, including its modeled uncertainty.
    NodeFS.rmSync(outer, { recursive: true, force: true });
  }
}

function syntheticResult(): Awaited<ReturnType<typeof buildAndStagePreparationProducer>> {
  return {
    bootstrapBundle: [],
    bootstrapRuntime: {
      kind: "streamed-file",
      relativePath: "jones-bootstrap/jones-offline-preparation",
      sha256: "0".repeat(64),
      size: 0,
      maxBytes: 256 * 1024 * 1024,
      mode: 0o500,
    },
    diagnosticEvidence: {
      artifactManifest: "synthetic test only",
      nativeQualification: JSON.stringify({
        qualification: "synthetic-native-preparation",
        cases: names.map((name) => ({ cases: [{ case: name }] })),
      }),
      nativeColdOwnerQualified: false,
      stateAdmissionAllowed: false,
      activationAllowed: false,
    },
    handleConsumption: { recreatedRejected: 4, consumedRejected: true },
    cleanup: { disposition: "removed", root: "synthetic capture only" },
  };
}

describe.sequential("fixed hosted driver lifecycle (mocked producer; no native evidence)", () => {
  it("moves the checked stage into evidence and removes only its disposable roots", () =>
    fixture(async (source, root) => {
      build.mockImplementationOnce(async (input) => {
        NodeFS.writeFileSync(
          NodePath.join(input.stageResourcesDir, "synthetic.txt"),
          "test fixture",
        );
        return syntheticResult();
      });
      await runHostedNativePreparation(source, root);
      expect(NodeFS.existsSync(NodePath.join(root, "home"))).toBe(false);
      expect(NodeFS.existsSync(NodePath.join(root, "tmp"))).toBe(false);
      expect(NodeFS.existsSync(NodePath.join(root, "stage"))).toBe(false);
      expect(
        NodeFS.readFileSync(NodePath.join(root, "evidence/stage/resources/synthetic.txt"), "utf8"),
      ).toBe("test fixture");
      expect(NodeFS.existsSync(source)).toBe(true);
      expect(
        JSON.parse(NodeFS.readFileSync(NodePath.join(root, "evidence/result.json"), "utf8")),
      ).toMatchObject({
        status: "passed",
        nativeColdOwnerQualified: false,
        stateAdmissionAllowed: false,
        activationAllowed: false,
      });
    }));

  it("preserves ordinary failure while cleaning disposable roots", () =>
    fixture(async (source, root) => {
      const original = new Error("synthetic known-closed failure");
      build.mockRejectedValueOnce(original);
      await expect(runHostedNativePreparation(source, root)).rejects.toBe(original);
      expect(NodeFS.readdirSync(root)).toEqual(["evidence"]);
      expect(
        JSON.parse(NodeFS.readFileSync(NodePath.join(root, "evidence/result.json"), "utf8")),
      ).toMatchObject({ status: "failed", error: original.message });
    }));

  it("retains all affected roots for nested uncertain custody", () =>
    fixture(async (source, root) => {
      const original = new Error("wrapped uncertainty", {
        cause: Object.assign(new Error("synthetic unproved close"), { preserveScratch: true }),
      });
      build.mockRejectedValueOnce(original);
      await expect(runHostedNativePreparation(source, root)).rejects.toBe(original);
      expect(NodeFS.readdirSync(root).sort()).toEqual(["evidence", "home", "stage", "tmp"]);
      expect(
        JSON.parse(NodeFS.readFileSync(NodePath.join(root, "evidence/result.json"), "utf8")),
      ).toMatchObject({ status: "failed", retainedRoots: root });
    }));

  it("cleanup failure cannot hide the original producer failure", () =>
    fixture(async (source, root) => {
      const original = new Error("synthetic producer failure");
      build.mockRejectedValueOnce(original);
      const remove = NodeFS.rmSync;
      vi.spyOn(NodeFS, "rmSync").mockImplementation((path, options) => {
        if (path === NodePath.join(root, "stage")) throw new Error("synthetic cleanup failure");
        return remove(path, options);
      });
      await expect(runHostedNativePreparation(source, root)).rejects.toMatchObject({
        cause: original,
        preserveScratch: true,
      });
      expect(NodeFS.existsSync(NodePath.join(root, "tmp"))).toBe(true);
    }));

  it("normal cancellation reaches the fixed producer and leaves no disposable roots", () =>
    fixture(async (source, root) => {
      build.mockImplementationOnce(async (input) => {
        process.emit("SIGTERM");
        input.signal!.throwIfAborted();
        throw new Error("Unreachable synthetic producer");
      });
      await expect(runHostedNativePreparation(source, root)).rejects.toThrow("cancelled");
      expect(NodeFS.readdirSync(root)).toEqual(["evidence"]);
    }));

  it("refuses wrong architecture before calling the producer", () =>
    fixture(async (source, root) => {
      Object.defineProperty(process, "arch", { configurable: true, value: "x64" });
      await expect(runHostedNativePreparation(source, root)).rejects.toThrow("Darwin arm64");
      expect(build).not.toHaveBeenCalled();
    }));
});
