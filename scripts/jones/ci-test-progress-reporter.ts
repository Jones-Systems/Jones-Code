import type {
  ReportedHookContext,
  Reporter,
  SerializedError,
  TestCase,
  TestModule,
  TestRunEndReason,
  TestSuite,
  Vitest,
} from "vite-plus/test/node";

type ProgressEntity = TestModule | TestSuite | TestCase;

export default class CiTestProgressReporter implements Reporter {
  private log: ((line: string) => void) | undefined;
  private runStartedAt = 0;

  onInit(ctx: Vitest): void {
    this.log = (line) => ctx.logger.log(line);
  }

  onTestRunStart(): void {
    this.runStartedAt = performance.now();
    this.emit("run-start");
  }

  onTestRunEnd(
    _modules: ReadonlyArray<TestModule>,
    errors: ReadonlyArray<SerializedError>,
    reason: TestRunEndReason,
  ): void {
    this.emit("run-end", undefined, { state: reason, errors });
  }

  onTestModuleQueued(module: TestModule): void {
    this.emit("module-queued", module);
  }

  onTestModuleCollected(module: TestModule): void {
    this.emit("module-collected", module);
  }

  onTestModuleStart(module: TestModule): void {
    this.emit("module-start", module);
  }

  onTestModuleEnd(module: TestModule): void {
    this.emit("module-end", module, { state: module.state(), errors: module.errors() });
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
