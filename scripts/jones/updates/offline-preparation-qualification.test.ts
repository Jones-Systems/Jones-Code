// @effect-diagnostics nodeBuiltinImport:off -- These synthetic lifecycle tests own their small roots and never execute emitted code.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  qualificationFailureDiagnostic,
  runHostedNativePreparation,
} from "./offline-preparation-qualification.ts";
import { buildAndStagePreparationProducer } from "./offline-preparation-producer-loader.mjs";

vi.mock("node:process", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:process")>()),
}));

vi.mock("node:fs", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs")>()),
}));

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
    Object.getOwnPropertyDescriptor(process.versions, "node")!,
    Object.getOwnPropertyDescriptor(process, "execArgv")!,
  ];
  try {
    vi.spyOn(NodeProcess, "platform", "get").mockReturnValue("darwin");
    vi.spyOn(NodeProcess, "arch", "get").mockReturnValue("arm64");
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
    Object.defineProperty(process.versions, "node", saved[0]!);
    Object.defineProperty(process, "execArgv", saved[1]!);
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

describe(
  "fixed hosted driver lifecycle (mocked producer; no native evidence)",
  { concurrent: false },
  () => {
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
          NodeFS.readFileSync(
            NodePath.join(root, "evidence/stage/resources/synthetic.txt"),
            "utf8",
          ),
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

    it("persists frozen nested native failure evidence after closed scratch cleanup", () =>
      fixture(async (source, root) => {
        const native = Object.freeze(
          Object.assign(new Error("Native helper exited."), {
            status: 7,
            signal: null,
            stdout: Buffer.from("native case apply failed"),
            stderr: "dlopen: missing native library\nAuthorization: Bearer synthetic-private-value",
            payload: "synthetic private payload",
          }),
        );
        const original = Object.assign(new Error("Producer capture failed.", { cause: native }), {
          cleanup: { disposition: "removed", root: NodePath.join(root, "tmp/capture") },
        });
        build.mockRejectedValueOnce(original);
        await expect(runHostedNativePreparation(source, root)).rejects.toBe(original);
        expect(NodeFS.readdirSync(root)).toEqual(["evidence"]);
        const bytes = NodeFS.readFileSync(NodePath.join(root, "evidence/result.json"), "utf8");
        expect(JSON.parse(bytes)).toMatchObject({
          status: "failed",
          nativeColdOwnerQualified: false,
          stateAdmissionAllowed: false,
          activationAllowed: false,
          failureDiagnostic: {
            cleanup: { disposition: "removed", root: NodePath.join(root, "tmp/capture") },
            cause: {
              status: 7,
              signal: null,
              stdout: "native case apply failed",
              stderr: "dlopen: missing native library\n[redacted diagnostic line]",
            },
          },
        });
        expect(bytes).not.toContain("synthetic-private-value");
        expect(bytes).not.toContain("synthetic private payload");
      }));

    it("retains aggregate cleanup children, custody and bounded cycle-safe diagnostics", () => {
      const native = Object.assign(new Error("Native exit"), { status: null, signal: "SIGTERM" });
      const cleanup = Object.assign(new Error("Unproved close"), {
        preserveScratch: true,
        preserveStageRoot: true,
        childPid: 123,
        retainedRoot: "/synthetic/capture",
        stageRoot: "/synthetic/stage",
      });
      const aggregate = new AggregateError([native, cleanup], "Capture and cleanup failed");
      Object.defineProperty(cleanup, "cause", { value: aggregate });
      const result = qualificationFailureDiagnostic(aggregate);
      expect(result).toMatchObject({
        errors: [
          { status: null, signal: "SIGTERM" },
          {
            preserveScratch: true,
            preserveStageRoot: true,
            childPid: 123,
            retainedRoot: "/synthetic/capture",
            stageRoot: "/synthetic/stage",
            cause: { omitted: "repeated error reference" },
          },
        ],
      });
      const wide = new AggregateError(
        Array.from({ length: 100 }, () =>
          Object.assign(new Error("synthetic failure"), {
            stdout: "line with diagnostic detail\n".repeat(2000),
          }),
        ),
        "bounded errors",
      );
      const bounded = qualificationFailureDiagnostic(wide);
      expect(bounded.omittedErrors).toBe(92);
      expect(JSON.stringify(bounded).length).toBeLessThan(20000);
      expect(JSON.stringify(bounded)).toContain("[truncated]");
      let deep: Error = new Error("deepest omitted");
      for (let index = 0; index < 30; index++) deep = new Error("wrapper", { cause: deep });
      expect(JSON.stringify(qualificationFailureDiagnostic(deep))).toContain("diagnostic limit");
      expect(JSON.stringify(qualificationFailureDiagnostic(deep))).not.toContain("deepest omitted");
    });

    it("redacts sensitive messages and output lines without evaluating arbitrary properties", () => {
      const failure = Object.assign(new Error("password=synthetic-password"), {
        stderr:
          'safe native error\ntoken=synthetic-token\n{"private":"synthetic-data"}\nhttps://user:pass@example.invalid/path\nghp_syntheticcredential',
        stdout: "private payload: synthetic-data",
      });
      Object.defineProperty(failure, "cleanup", {
        get: () => {
          throw new Error("must not execute");
        },
      });
      const serialized = JSON.stringify(qualificationFailureDiagnostic(failure));
      expect(serialized).toContain("safe native error");
      const multiline = qualificationFailureDiagnostic(
        Object.assign(new Error("native failure"), {
          stdout: '{\n  "data": "synthetic-multiline-private"\n}',
          stderr: "-----BEGIN PRIVATE KEY-----\nsynthetic-key-material\n-----END PRIVATE KEY-----",
        }),
      );
      expect(JSON.stringify(multiline)).not.toContain("synthetic-multiline-private");
      expect(JSON.stringify(multiline)).not.toContain("synthetic-key-material");
      expect(serialized).toContain("[redacted diagnostic line]");
      for (const sensitive of [
        "synthetic-password",
        "synthetic-token",
        "synthetic-data",
        "user:pass",
        "ghp_syntheticcredential",
      ])
        expect(serialized).not.toContain(sensitive);
    });

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
        vi.spyOn(NodeProcess, "arch", "get").mockReturnValue("x64");
        await expect(runHostedNativePreparation(source, root)).rejects.toThrow("Darwin arm64");
        expect(build).not.toHaveBeenCalled();
      }));
  },
);

it("copies strict native failure evidence into the receipt and rejects private extensions", async () => {
  await fixture(async (source, root) => {
    const snapshot = {
      stdoutBytes: 0,
      stdoutEof: true,
      stderrBytes: 0,
      stderrEof: true,
      returncode: 70,
      signal: null,
      stopRecords: 0,
      stderr: "",
    };
    const failureEvidence = {
      schema: 1,
      classification: "native-preparation-failure",
      controllerFailure: {
        records: [
          {
            parent: null,
            relation: "primary",
            type: "OperationalError",
            stage: "reader-query",
            frames: [{ source: "fixture.py", function: "readonly_rows", line: 321 }],
            duplicateOf: null,
            sqliteErrorCode: 1,
          },
        ],
        truncated: false,
      },
      case: "apply",
      completedCases: 0,
      expectedFrames: ["PLAN_READY", "CLOSED_RESULT"],
      lastPhase: "sent-CONTINUE",
      processes: [
        {
          beforeCleanup: { ...snapshot, returncode: null },
          beforeCleanupTiming: "receive-failure",
          afterCleanup: snapshot,
        },
      ],
      closure: {
        schema: 1,
        complete: true,
        capturedChildren: 1,
        constructorUnknown: false,
        pidSignalAuthority: false,
        processes: [
          {
            pid: 123,
            spawnAttempted: true,
            spawnUnknown: false,
            reaped: true,
            streamsClosed: true,
            signals: [],
            pidSignalAuthority: false,
          },
        ],
      },
    };
    const error = Object.assign(new Error("native failed"), {
      failureEvidencePath: "/synthetic/case-failure.json",
      failureEvidence,
    });
    build.mockRejectedValueOnce(error);
    await expect(runHostedNativePreparation(source, root)).rejects.toBe(error);
    const receipt = JSON.parse(
      NodeFS.readFileSync(NodePath.join(root, "evidence/result.json"), "utf8"),
    );
    expect(receipt.failureDiagnostic).toMatchObject({
      failureEvidencePath: error.failureEvidencePath,
      failureEvidence,
    });
    expect(
      qualificationFailureDiagnostic(
        Object.assign(new Error("invalid evidence"), {
          failureEvidence: { ...failureEvidence, settings: "private-settings" },
        }),
      ),
    ).toMatchObject({ failureEvidence: { omitted: "invalid native failure evidence" } });
  });
});
