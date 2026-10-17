// @effect-diagnostics nodeBuiltinImport:off - Reporter fixtures own a per-test temporary directory and read the checkout's Git commit.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { TestModule, TestSpecification, Vitest } from "vite-plus/test/node";

import CiTestProgressReporter from "./ci-test-progress-reporter.ts";
import {
  TimingRejection,
  aggregateServerShardCaptures,
  parseCapture,
} from "./server-shard-weights/aggregate.ts";
import {
  TIMINGS_PATH_ENV,
  WEIGHTS_DIGEST_ENV,
  WEIGHTS_PATH_ENV,
  WEIGHTS_SOURCE_RUN_ENV,
  weightsDigest,
} from "./server-shard-weights/envelope.ts";

const REPO = "Jones-Systems/Jones-Code";
const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
const serverRoot = NodePath.join(repoRoot, "apps/server");
const head = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
  cwd: repoRoot,
  encoding: "utf8",
}).trim();
const roots: string[] = [];

afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) NodeFS.rmSync(root, { recursive: true, force: true });
});

function testModule(path: string, state = "passed", errors: { message: string }[] = []) {
  return {
    type: "module",
    moduleId: NodePath.join(serverRoot, path),
    relativeModuleId: path,
    state: () => state,
    errors: () => errors,
    diagnostic: () => ({
      duration: 5,
      setupDuration: 2,
      collectDuration: 3,
      prepareDuration: 4,
      environmentSetupDuration: 1,
    }),
  } as unknown as TestModule;
}

interface SetupOptions {
  capture?: boolean;
  config?: Record<string, unknown>;
  filenamePattern?: string[];
}

function setup({ capture = true, config = {}, filenamePattern }: SetupOptions = {}) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-timing-reporter-"));
  roots.push(root);
  const path = NodePath.join(root, "server-timings.ndjson");
  vi.stubEnv(TIMINGS_PATH_ENV, capture ? path : undefined);
  vi.stubEnv(WEIGHTS_PATH_ENV, undefined);
  vi.stubEnv(WEIGHTS_DIGEST_ENV, undefined);
  vi.stubEnv(WEIGHTS_SOURCE_RUN_ENV, undefined);
  vi.stubEnv("RUNNER_TEMP", root);
  vi.stubEnv("GITHUB_SHA", head);
  vi.stubEnv("GITHUB_REPOSITORY", REPO);
  vi.stubEnv("GITHUB_WORKFLOW_REF", `${REPO}/.github/workflows/ci.yml@refs/heads/main`);
  vi.stubEnv("GITHUB_EVENT_NAME", "push");
  vi.stubEnv("GITHUB_REF", "refs/heads/main");
  vi.stubEnv("GITHUB_RUN_ID", "1234");
  vi.stubEnv("GITHUB_RUN_ATTEMPT", "1");
  vi.stubEnv("ImageOS", "ubuntu24");
  vi.stubEnv("ImageVersion", "20261014.1");
  const log = vi.fn<(line: string) => void>();
  const reporter = new CiTestProgressReporter();
  reporter.onInit({
    logger: { log },
    version: "5.0.1",
    filenamePattern,
    config: { shard: { index: 1, count: 1 }, project: [], changed: false, ...config },
  } as unknown as Vitest);
  const specs = ["src/second.test.ts", "src/first.test.ts"].map((file) => ({
    moduleId: NodePath.join(serverRoot, file),
    project: { config: { maxWorkers: 1 } },
  })) as unknown as TestSpecification[];
  return { root, path, log, reporter, specs };
}

function runPassingShard(reporter: CiTestProgressReporter, specs: TestSpecification[]) {
  const first = testModule("src/first.test.ts");
  const second = testModule("src/second.test.ts");
  reporter.onTestRunStart(specs);
  for (const file of [first, second]) {
    reporter.onTestModuleQueued(file);
    reporter.onTestModuleCollected(file);
    reporter.onTestModuleStart(file);
    reporter.onTestModuleEnd(file);
  }
  reporter.onTestRunEnd([first, second], [], "passed");
}

const lines = (path: string) =>
  NodeFS.readFileSync(path, "utf8")
    .trimEnd()
    .split("\n")
    .map((line) => JSON.parse(line));

describe("CI progress reporter", () => {
  it("writes nothing and keeps its stdout progress when capture is off", () => {
    const { reporter, root, specs, log } = setup({ capture: false });
    runPassingShard(reporter, specs);

    expect(NodeFS.readdirSync(root)).toEqual([]);
    const events = log.mock.calls.map(([line]) => JSON.parse(line));
    expect(events.map((event) => event.event)).toEqual([
      "run-start",
      ...["first", "second"].flatMap(() => [
        "module-queued",
        "module-collected",
        "module-start",
        "module-end",
      ]),
      "run-end",
    ]);
    expect(Object.keys(events[0])).toEqual(["event", "elapsedMs"]);
    expect(events[1]).toMatchObject({
      event: "module-queued",
      relativeModuleId: "src/first.test.ts",
      entityName: "src/first.test.ts",
    });
    expect(events.at(-1)).toMatchObject({ event: "run-end", state: "passed", errorCount: 0 });
  });

  it("records the full discovered file list, filters and weights before sharding", () => {
    const { reporter, path, specs, log } = setup();
    runPassingShard(reporter, specs);
    const [header, ...records] = lines(path);

    expect(header).toMatchObject({
      type: "header",
      schema: "jones.server-shard-timings/v1",
      checkoutSha: head,
      githubSha: head,
      runId: "1234",
      runAttempt: 1,
      shardIndex: 1,
      shardCount: 1,
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
      manifest: ["src/first.test.ts", "src/second.test.ts"],
    });
    expect(records.at(-1)).toMatchObject({
      type: "terminal",
      reason: "passed",
      unhandledErrorCount: 0,
      completedFiles: ["src/first.test.ts", "src/second.test.ts"],
    });
    expect(log).toHaveBeenCalledTimes(10);
    const { envelope } = aggregateServerShardCaptures([NodeFS.readFileSync(path, "utf8")], {
      shardCount: 1,
    });
    expect(Object.keys(envelope.weights)).toEqual(["src/first.test.ts", "src/second.test.ts"]);
  });

  it("records the runtime weights the shard was given", () => {
    const { reporter, root, path, specs } = setup();
    const weightsPath = NodePath.join(root, "server-shard-weights.json");
    NodeFS.writeFileSync(weightsPath, "{}\n");
    vi.stubEnv(WEIGHTS_PATH_ENV, weightsPath);
    vi.stubEnv(WEIGHTS_DIGEST_ENV, weightsDigest("{}\n"));
    vi.stubEnv(WEIGHTS_SOURCE_RUN_ENV, "999");
    reporter.onTestRunStart(specs);

    expect(lines(path)[0].weightsInput).toEqual({
      mode: "runtime",
      sourceRunId: "999",
      digest: weightsDigest("{}\n"),
    });
  });

  it("refuses runtime weights that do not match their digest", () => {
    const { reporter, root, specs } = setup();
    const weightsPath = NodePath.join(root, "server-shard-weights.json");
    NodeFS.writeFileSync(weightsPath, "{}\n");
    vi.stubEnv(WEIGHTS_PATH_ENV, weightsPath);
    vi.stubEnv(WEIGHTS_DIGEST_ENV, "0".repeat(64));

    expect(() => reporter.onTestRunStart(specs)).toThrow(/does not match/);
  });

  it.each([
    ["a filename filter", { filenamePattern: ["src/first"] }],
    ["a test name filter", { config: { testNamePattern: /first/ } }],
    ["a changed-files run", { config: { changed: "HEAD~1", related: ["src/first.test.ts"] } }],
  ])("records %s so the measurement is refused", (_name, options) => {
    const { reporter, path, specs } = setup(options);
    runPassingShard(reporter, specs);

    expect(() => parseCapture(NodeFS.readFileSync(path, "utf8"))).toThrow(TimingRejection);
  });

  it("records a file that failed to load and the failed run", () => {
    const { reporter, path, specs } = setup();
    const broken = testModule("src/first.test.ts", "failed", [{ message: "Cannot find module" }]);
    reporter.onTestRunStart(specs);
    // A file that fails during import starts and ends without being collected.
    reporter.onTestModuleQueued(broken);
    reporter.onTestModuleStart(broken);
    reporter.onTestModuleEnd(broken);
    reporter.onTestRunEnd([broken], [], "failed");
    const records = parseCapture(NodeFS.readFileSync(path, "utf8"));

    expect(records.at(-2)).toMatchObject({
      event: "end",
      state: "failed",
      errorCount: 1,
      errors: ["Cannot find module"],
    });
    expect(records.at(-1)).toMatchObject({
      reason: "failed",
      completedFiles: ["src/first.test.ts"],
    });
  });

  it("keeps every completed record of an interrupted shard", () => {
    const { reporter, path, specs } = setup();
    reporter.onTestRunStart(specs);
    reporter.onTestModuleQueued(testModule("src/first.test.ts"));
    const partial = NodeFS.readFileSync(path, "utf8");
    expect(() => parseCapture(partial)).toThrow(/no final terminal record/);

    reporter.onTestRunEnd([], [], "interrupted");
    const retained = NodeFS.readFileSync(path, "utf8");
    expect(retained.startsWith(partial)).toBe(true);
    const terminal = parseCapture(retained).at(-1);
    expect(terminal).toMatchObject({ reason: "interrupted", completedFiles: [] });
  });

  it("refuses an output outside the runner temporary directory", () => {
    const { reporter, root, specs } = setup();
    vi.stubEnv(TIMINGS_PATH_ENV, NodePath.join(root, "nested", "outside.ndjson"));
    NodeFS.mkdirSync(NodePath.join(root, "nested"));

    expect(() => reporter.onTestRunStart(specs)).toThrow(/RUNNER_TEMP/);
    expect(NodeFS.readdirSync(NodePath.join(root, "nested"))).toEqual([]);
  });

  it("refuses to measure files that run in parallel", () => {
    const { reporter, root, specs } = setup();
    const parallel = specs.map((spec) => ({ ...spec, project: { config: { maxWorkers: 4 } } }));

    expect(() => reporter.onTestRunStart(parallel as unknown as TestSpecification[])).toThrow(
      /serially/,
    );
    expect(NodeFS.readdirSync(root)).toEqual([]);
  });
});
