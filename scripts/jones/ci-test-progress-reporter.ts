// @effect-diagnostics nodeBuiltinImport:off - The CI reporter writes runner-local timing captures synchronously, outside any Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import type {
  ReportedHookContext,
  Reporter,
  SerializedError,
  TestCase,
  TestModule,
  TestRunEndReason,
  TestSpecification,
  TestSuite,
  Vitest,
} from "vite-plus/test/node";

import {
  CAPTURE_IDENTITY_FILES,
  CAPTURE_SCHEMA,
  CaptureWriter,
  type CaptureFilters,
  type CaptureModule,
  type ModuleEvent,
} from "./server-shard-weights/capture.ts";
import {
  TIMINGS_PATH_ENV,
  WEIGHTS_DIGEST_ENV,
  WEIGHTS_PATH_ENV,
  WEIGHTS_SOURCE_RUN_ENV,
  compareStrings,
  safeRelativePath,
  sha256,
  weightsDigest,
  type WeightsInput,
} from "./server-shard-weights/envelope.ts";

type ProgressEntity = TestModule | TestSuite | TestCase;

const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
const serverRoot = NodePath.join(repoRoot, "apps/server");

export default class CiTestProgressReporter implements Reporter {
  private log: ((line: string) => void) | undefined;
  private runStartedAt = 0;
  private context: Vitest | undefined;
  private capture: CaptureWriter | undefined;
  private completedFiles: string[] = [];

  onInit(ctx: Vitest): void {
    this.context = ctx;
    this.log = (line) => ctx.logger.log(line);
  }

  onTestRunStart(specifications: ReadonlyArray<TestSpecification>): void {
    this.runStartedAt = performance.now();
    this.emit("run-start");
    const outputPath = process.env[TIMINGS_PATH_ENV];
    if (outputPath) this.startCapture(outputPath, specifications);
  }

  onTestRunEnd(
    _modules: ReadonlyArray<TestModule>,
    errors: ReadonlyArray<SerializedError>,
    reason: TestRunEndReason,
  ): void {
    this.emit("run-end", undefined, { state: reason, errors });
    this.capture?.append({
      type: "terminal",
      reason,
      unhandledErrorCount: errors.length,
      completedFiles: this.completedFiles,
      elapsedMs: this.elapsedMs(),
    });
  }

  onTestModuleQueued(module: TestModule): void {
    this.emit("module-queued", module);
    this.recordModule("queued", module);
  }

  onTestModuleCollected(module: TestModule): void {
    this.emit("module-collected", module);
    this.recordModule("collected", module);
  }

  onTestModuleStart(module: TestModule): void {
    this.emit("module-start", module);
    this.recordModule("start", module);
  }

  onTestModuleEnd(module: TestModule): void {
    this.emit("module-end", module, { state: module.state(), errors: module.errors() });
    this.recordModule("end", module);
  }

  onTestCaseReady(test: TestCase): void {
    this.emit("case-ready", test);
  }

  onTestCaseResult(test: TestCase): void {
    const result = test.result();
    this.emit("case-result", test, { state: result.state, errors: result.errors });
  }

  onHookStart(hook: ReportedHookContext): void {
    this.emit("hook-start", hook.entity, { hookName: hook.name });
  }

  onHookEnd(hook: ReportedHookContext): void {
    this.emit("hook-end", hook.entity, { hookName: hook.name });
  }

  /**
   * Starts this shard's timing capture. Vitest 5 passes the full discovered file
   * list here, before `--shard` splits it, so every shard records the same list
   * and the aggregate step can prove the shards covered it exactly once.
   */
  private startCapture(outputPath: string, specifications: ReadonlyArray<TestSpecification>) {
    const ctx = this.context;
    const runnerTemp = process.env.RUNNER_TEMP;
    if (!ctx || !runnerTemp || !ctx.config.shard) {
      throw new Error(`${TIMINGS_PATH_ENV} requires a --shard run with RUNNER_TEMP set`);
    }
    if (NodeFS.realpathSync(NodePath.dirname(outputPath)) !== NodeFS.realpathSync(runnerTemp)) {
      throw new Error(`${TIMINGS_PATH_ENV} must name a file directly inside RUNNER_TEMP`);
    }
    // Vitest 5 resolves `fileParallelism: false` to one worker.
    if (specifications.some((spec) => spec.project.config.maxWorkers !== 1)) {
      throw new Error("Server timing capture requires files to run serially");
    }
    const require = NodeModule.createRequire(import.meta.url);
    const vitePlus: { version: string } = JSON.parse(
      NodeFS.readFileSync(require.resolve("vite-plus/package.json"), "utf8"),
    );
    this.completedFiles = [];
    this.capture = new CaptureWriter(outputPath, {
      type: "header",
      schema: CAPTURE_SCHEMA,
      checkoutSha: NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
        cwd: repoRoot,
        encoding: "utf8",
      }).trim(),
      githubSha: process.env.GITHUB_SHA ?? "",
      repository: process.env.GITHUB_REPOSITORY ?? "",
      workflowRef: process.env.GITHUB_WORKFLOW_REF ?? "",
      event: process.env.GITHUB_EVENT_NAME ?? "",
      ref: process.env.GITHUB_REF ?? "",
      runId: process.env.GITHUB_RUN_ID ?? "",
      runAttempt: Number(process.env.GITHUB_RUN_ATTEMPT),
      shardIndex: ctx.config.shard.index,
      shardCount: ctx.config.shard.count,
      runner: {
        image: process.env.ImageOS ?? "",
        imageVersion: process.env.ImageVersion ?? "",
        release: NodeOS.release(),
      },
      tools: { node: process.version, vitest: ctx.version, vitePlus: vitePlus.version },
      identities: Object.fromEntries(
        CAPTURE_IDENTITY_FILES.map((path) => [
          path,
          sha256(NodeFS.readFileSync(NodePath.join(repoRoot, path))),
        ]),
      ),
      fileParallelism: false,
      filters: invocationFilters(ctx, specifications),
      weightsInput: weightsInput(),
      manifest: specifications.map((spec) => serverPath(spec.moduleId)).toSorted(compareStrings),
    });
  }

  private elapsedMs(): number {
    return Math.round((performance.now() - this.runStartedAt) * 1000) / 1000;
  }

  private recordModule(event: ModuleEvent, module: TestModule): void {
    if (!this.capture) return;
    const path = serverPath(module.moduleId);
    const errors = module.errors();
    const record: CaptureModule = {
      type: "module",
      event,
      path,
      elapsedMs: this.elapsedMs(),
      state: module.state(),
      errorCount: errors.length,
      errors: errors
        .slice(0, 3)
        .map((error) => (error.message ?? "Error without a message").slice(0, 1000)),
    };
    if (event === "end") {
      const diagnostic = module.diagnostic();
      record.diagnostic = {
        duration: diagnostic.duration,
        setupDuration: diagnostic.setupDuration,
        collectDuration: diagnostic.collectDuration,
        prepareDuration: diagnostic.prepareDuration,
        environmentSetupDuration: diagnostic.environmentSetupDuration,
      };
      this.completedFiles.push(path);
    }
    this.capture.append(record);
  }

  private emit(
    event: string,
    entity?: ProgressEntity,
    details: {
      state?: string;
      hookName?: string;
      errors?: ReadonlyArray<{ message: string }> | undefined;
    } = {},
  ): void {
    const module = entity?.type === "module" ? entity : entity?.module;
    // Elapsed time is observed by this reporter, not measured inside workers.
    // Queued-to-collected includes setup/import/collection; module-end does not prove worker teardown.
    this.log?.(
      JSON.stringify({
        event,
        elapsedMs: Math.round(performance.now() - this.runStartedAt),
        relativeModuleId: module?.relativeModuleId,
        entityId: entity?.id,
        entityName: entity?.type === "module" ? entity.relativeModuleId : entity?.fullName,
        hookName: details.hookName,
        state: details.state,
        errorCount: details.errors?.length,
        errors: details.errors?.slice(0, 3).map((error) => {
          const message = error.message ?? "Error without a message";
          return message.length > 1000 ? `${message.slice(0, 999)}…` : message;
        }),
      }),
    );
  }
}

/** The same server-relative key the shard sequencer weighs files by. */
function serverPath(moduleId: string): string {
  return safeRelativePath(NodePath.relative(serverRoot, moduleId).replaceAll("\\", "/"));
}

function invocationFilters(
  ctx: Vitest,
  specifications: ReadonlyArray<TestSpecification>,
): CaptureFilters {
  const { config } = ctx;
  // Positional filename filters live on an internal Vitest field in 5.0.1.
  const filenamePatterns = (ctx as unknown as { filenamePattern?: string[] }).filenamePattern;
  return {
    filenamePatterns: [...(filenamePatterns ?? [])],
    testNamePattern: config.testNamePattern?.source ?? null,
    projects: [...(config.project ?? [])],
    tagsFilter: [...(config.tagsFilter ?? [])],
    changed:
      config.changed === undefined || config.changed === false ? null : String(config.changed),
    related: [...(config.related ?? [])],
    targetedFiles: specifications.filter(
      (spec) =>
        (spec.testLines?.length ?? 0) > 0 ||
        (spec.testIds?.length ?? 0) > 0 ||
        spec.testNamePattern !== undefined ||
        (spec.testTagsFilter?.length ?? 0) > 0,
    ).length,
  };
}

/** The weights the shard sequencer is using, read from the same file it reads. */
function weightsInput(): WeightsInput {
  const path = process.env[WEIGHTS_PATH_ENV];
  if (!path) return { mode: "checked-in" };
  const digest = weightsDigest(NodeFS.readFileSync(path, "utf8"));
  if (digest !== process.env[WEIGHTS_DIGEST_ENV]) {
    throw new Error(`${WEIGHTS_PATH_ENV} does not match ${WEIGHTS_DIGEST_ENV}`);
  }
  return { mode: "runtime", sourceRunId: process.env[WEIGHTS_SOURCE_RUN_ENV] ?? "", digest };
}
