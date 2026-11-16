// @effect-diagnostics nodeBuiltinImport:off - Writer fixtures use a per-test temporary directory and check repository files exist.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { afterEach, describe, expect, it } from "vite-plus/test";

import { parseCapture } from "./aggregate.ts";
import {
  CAPTURE_IDENTITY_FILES,
  CAPTURE_SCHEMA,
  CaptureWriter,
  type CaptureHeader,
} from "./capture.ts";

const repoRoot = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) NodeFS.rmSync(root, { recursive: true, force: true });
});

function scratchPath(): string {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-capture-"));
  roots.push(root);
  return NodePath.join(root, "server-timings.ndjson");
}

const header: CaptureHeader = {
  type: "header",
  schema: CAPTURE_SCHEMA,
  checkoutSha: "a".repeat(40),
  githubSha: "a".repeat(40),
  repository: "Jones-Systems/Jones-Code",
  workflowRef: "Jones-Systems/Jones-Code/.github/workflows/ci.yml@refs/pull/1/merge",
  event: "pull_request",
  ref: "refs/pull/1/merge",
  runId: "77",
  runAttempt: 1,
  shardIndex: 1,
  shardCount: 1,
  runner: { image: "ubuntu24", imageVersion: "20261012.1", release: "6.11" },
  tools: { node: "v24.13.1", vitest: "5.0.1", vitePlus: "1.0.0" },
  identities: Object.fromEntries(CAPTURE_IDENTITY_FILES.map((path) => [path, "b".repeat(64)])),
  fileParallelism: false,
  filters: {
    filenamePatterns: [],
    testNamePattern: null,
    projects: [],
    tagsFilter: [],
    changed: null,
    related: [],
    targetedFiles: 0,
  },
  weightsInput: { mode: "checked-in" },
  manifest: ["src/a.test.ts"],
};

describe("CaptureWriter", () => {
  it("writes the header, then appends each record as one line", () => {
    const path = scratchPath();
    const writer = new CaptureWriter(path, header);
    writer.append({
      type: "module",
      event: "queued",
      path: "src/a.test.ts",
      elapsedMs: 1,
      state: "queued",
      errorCount: 0,
      errors: [],
    });
    const partial = NodeFS.readFileSync(path, "utf8");
    writer.append({
      type: "terminal",
      reason: "interrupted",
      unhandledErrorCount: 0,
      completedFiles: [],
      elapsedMs: 2,
    });
    const complete = NodeFS.readFileSync(path, "utf8");

    expect(partial.split("\n")).toHaveLength(3);
    expect(complete.startsWith(partial)).toBe(true);
    expect(parseCapture(complete).map((record) => record.type)).toEqual([
      "header",
      "module",
      "terminal",
    ]);
  });

  it("never overwrites an earlier capture", () => {
    const path = scratchPath();
    NodeFS.writeFileSync(path, "earlier\n");

    expect(() => new CaptureWriter(path, header)).toThrow();
    expect(NodeFS.readFileSync(path, "utf8")).toBe("earlier\n");
  });
});

describe("CAPTURE_IDENTITY_FILES", () => {
  it.each(CAPTURE_IDENTITY_FILES)("names a file in the repository: %s", (path) => {
    expect(NodeFS.statSync(NodePath.join(repoRoot, path)).isFile()).toBe(true);
  });
});
