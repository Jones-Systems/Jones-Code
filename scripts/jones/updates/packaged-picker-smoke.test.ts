// @effect-diagnostics nodeBuiltinImport:off - Fail-closed native consumer preflight and exact producer binding.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { afterEach, expect, it } from "vite-plus/test";
import {
  scanPickerBundle,
  readPickerFuses,
  requirePickerCopyPath,
  evaluatePickerReports,
  expectedPickerChecks,
  verifyPickerManifest,
  locatePickerProducer,
} from "./packaged-picker-smoke.ts";
import { withSmokeScratch } from "./desktop-worker-smoke.ts";

it("accepts only the entry's declared literal external modules", () => {
  expect(
    scanPickerBundle('const ipc=require("electron");require("node:path");', "main").dependencies,
  ).toEqual(["electron", "node:path"]);
  expect(scanPickerBundle('require("electron")', "preload").dependencies).toEqual(["electron"]);
  for (const source of [
    "require(id)",
    'const load=require;load("electron")',
    'require("node:fs")',
    'require("native.node")',
    "import(name)",
    'module.require("electron")',
    'require("./main.cjs")',
    'require("@clerk/electron")',
    'require("electron-updater")',
    'Module.createRequire(__filename)("original-fs")',
    "process.dlopen(x)",
  ])
    expect(() => scanPickerBundle(source, "preload")).toThrow();
  for (const source of [
    'const {createRequire: load} = require("node:module"); load(__filename)(name)',
    'import {createRequire as load} from "node:module"; load(import.meta.url)(name)',
    'const Module = require("module"); Module._load(name)',
    'process.getBuiltinModule("module").createRequire(__filename)(name)',
    "process.getBuiltinModule(name)",
    "const load = process.getBuiltinModule; load(name)",
  ])
    expect(() => scanPickerBundle(source, "main")).toThrow();
});
it("rejects unknown and enabled OnlyLoadAppFromAsar fuse layouts", () => {
  const binary = Buffer.concat([
    Buffer.from("dL7pKGdnNz796PbbjQWNKmHXBZaB9tsX"),
    Buffer.from([1, 8, 0x31, 0x30, 0x31, 0x31, 0x30, 0x30, 0x30, 0x31]),
  ]);
  expect(readPickerFuses(binary).onlyLoadAppFromAsar).toBe(0x30);
  binary[binary.length - 3] = 0x31;
  expect(() => readPickerFuses(binary)).toThrow("not off");
  expect(() => readPickerFuses(Buffer.alloc(100))).toThrow("Missing");
  expect(() => readPickerFuses(Buffer.concat([binary, binary]))).toThrow("ambiguous");
});
it("refuses unstable copied bundle paths without native execution", () => {
  requirePickerCopyPath("/private/var/folders/runner/picker/Applications/Jones.app");
  for (const value of [
    "/tmp/Jones.app",
    "/Volumes/Jones.app",
    "/x/AppTranslocation/Jones.app",
    "/x/jones-updates/Jones.app",
    "relative.app",
    "/x/../Jones.app",
  ])
    expect(() => requirePickerCopyPath(value)).toThrow();
});
it("never converts missing, synthetic-only or failed evidence into native success", () => {
  const reports = ["catalog", "persisted", "inflight", "held"].map((phase) => ({
    phase,
    status: "pass",
    nativeHop: true,
    checks: [
      "IPC-A1-capability-members",
      "IPC-A2-renderer-require",
      "IPC-A3-renderer-process",
      "IPC-A4-inert-list",
      "IPC-A5-inert-transport",
      "SAFE-A1-zero-native",
      "SAFE-A2-zero-install",
      "SAFE-A3-zero-transport-download",
      "SAFE-A4-no-install-operation",
      ...expectedPickerChecks[phase as keyof typeof expectedPickerChecks],
    ].map((id) => ({ id, status: "pass" })),
    preferences: { contextIsolation: true, nodeIntegration: false, sandbox: true },
    violations: [],
    nativeCalls: [],
    installCalls: 0,
  }));
  expect(evaluatePickerReports(reports)).toBe("PASS");
  expect(() => evaluatePickerReports([])).toThrow();
  expect(() =>
    evaluatePickerReports(reports.map((value) => ({ ...value, nativeHop: false }))),
  ).toThrow();
  expect(() =>
    evaluatePickerReports(reports.map((value) => ({ ...value, installCalls: 1 }))),
  ).toThrow();
});
it("binds the embedded synthetic merge source independently of the producer API head SHA", () => {
  const binding = {
    source: "a".repeat(40),
    tree: "b".repeat(40),
    runId: "123",
    attempt: "2",
    event: "pull_request",
    ref: "refs/pull/872/merge",
  };
  const value = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    ...binding,
    source: binding.source,
    tree: binding.tree,
    runId: binding.runId,
    runAttempt: binding.attempt,
    event: binding.event,
    ref: binding.ref,
    workflow: ".github/workflows/artifact-desktop-mac.yml",
    platform: "darwin",
    architecture: "arm64",
    signed: false,
    notarized: false,
    version: "0.0.44-preview.20261116.123.2",
    artifact: "T3-Code-0.0.44-preview.20261116.123.2-arm64.dmg",
    sha256: "c".repeat(64),
  };
  expect(verifyPickerManifest(value, binding).source).toBe(binding.source);
  expect(() => verifyPickerManifest({ ...value, source: "d".repeat(40) }, binding)).toThrow(
    "mismatch",
  );
  expect(() => verifyPickerManifest({ ...value, runAttempt: "1" }, binding)).toThrow("mismatch");
});
it("bounds producer discovery and rejects stale API sources", async () => {
  let calls = 0,
    waits = 0;
  await expect(
    locatePickerProducer(
      {
        source: "a".repeat(40),
        tree: "b".repeat(40),
        head: "c".repeat(40),
        event: "pull_request",
        pr: 872,
      },
      async () => {
        calls++;
        return {
          workflow_runs: [
            {
              id: 1,
              path: ".github/workflows/artifact-desktop-mac.yml",
              event: "pull_request",
              head_sha: "d".repeat(40),
              status: "completed",
              conclusion: "success",
              repository: { full_name: "Jones-Systems/Jones-Code" },
              head_repository: { full_name: "Jones-Systems/Jones-Code" },
              pull_requests: [{ number: 872 }],
            },
          ],
        };
      },
      async () => {
        waits++;
      },
      3,
    ),
  ).rejects.toThrow("UNAVAILABLE");
  expect(calls).toBe(3);
  expect(waits).toBe(2);
});
const parents: string[] = [];
afterEach(async () => {
  for (const parent of parents.splice(0)) await NodeFSP.rm(parent, { recursive: true });
});
it("cleans exact concurrent invocation roots on success and failure without losing the original result", async () => {
  const parent = await NodeFSP.mkdtemp(NodePath.resolve("node_modules/.cache/picker-smoke-"));
  parents.push(parent);
  const owned: string[] = [];
  const outcomes = await Promise.allSettled([
    withSmokeScratch(parent, async (root) => {
      owned.push(root);
      await NodeFSP.writeFile(NodePath.join(root, "evidence"), "fixture");
      return "ok";
    }),
    withSmokeScratch(parent, async (root) => {
      owned.push(root);
      throw new Error("original failure");
    }),
  ]);
  expect(outcomes[0]).toEqual({ status: "fulfilled", value: "ok" });
  expect(outcomes[1]).toMatchObject({
    status: "rejected",
    reason: expect.objectContaining({ message: "original failure" }),
  });
  expect(new Set(owned).size).toBe(2);
  expect(await NodeFSP.readdir(parent)).toEqual([]);
  await expect(
    withSmokeScratch(
      parent,
      async () => {
        throw new Error("primary");
      },
      async () => {
        throw new Error("cleanup");
      },
    ),
  ).rejects.toMatchObject({
    errors: [
      expect.objectContaining({ message: "primary" }),
      expect.objectContaining({ message: "cleanup" }),
    ],
  });
});
