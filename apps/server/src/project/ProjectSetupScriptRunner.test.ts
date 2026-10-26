import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import { ProjectId, type TerminalEvent, type TerminalSessionSnapshot } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import { LegacyOwnedTerminalControl } from "../orchestration-v2/RecordedTypes.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as NativeSetup from "../terminal/NativeSetupControl.ts";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { TestClock } from "effect/testing";

import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as ProjectService from "./ProjectService.ts";
import * as ProjectSetupScriptRunner from "./ProjectSetupScriptRunner.ts";

const threadId = "thread-1";
const terminalId = "setup-setup";
const event = { threadId, terminalId, sequence: 1, createdAt: "2026-01-01T00:00:00.000Z" };

class SyntheticSetupError extends Schema.TaggedError<SyntheticSetupError>()("SyntheticSetupError", {
  message: Schema.String,
}) {}

const openSnapshot = (history: string): TerminalSessionSnapshot => ({
  threadId,
  terminalId,
  cwd: "/repo/.t3/worktrees/feature",
  worktreePath: null,
  status: "running",
  pid: 123,
  history,
  exitCode: null,
  exitSignal: null,
  label: "zsh",
  updatedAt: "2026-01-01T00:00:00.000Z",
});

const makeHarness = Effect.fn(function* (options: {
  readonly history?: string;
  readonly opening?: Deferred.Deferred<void>;
}) {
  const listeners = new Set<(event: TerminalEvent) => Effect.Effect<void>>();
  const writes = yield* Queue.unbounded<string>();
  const outputLines = yield* Queue.unbounded<string>();
  const terminals = Layer.mock(TerminalManager.TerminalManager)({
    open: () =>
      (options.opening
        ? Deferred.succeed(options.opening, undefined).pipe(Effect.andThen(Effect.never))
        : Effect.void
      ).pipe(Effect.as(openSnapshot(options.history ?? ""))),
    subscribe: (listener) =>
      Effect.sync(() => {
        listeners.add(listener);
        return () => listeners.delete(listener);
      }),
    write: (input) => Queue.offer(writes, input.data).pipe(Effect.asVoid),
    closeIdle: () => Effect.void,
  });
  const runner = yield* ProjectSetupScriptRunner.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        terminals,
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: () => Effect.succeed(null),
        }),
        ServerSettings.layerTest(),
        NodeCrypto.layer,
      ),
    ),
    Effect.provideService(HostProcess.Platform, "darwin"),
    Effect.provideService(HostProcess.Environment, { SHELL: "/bin/zsh" }),
  );
  const start = runner.runForThread({
    threadId,
    worktreePath: "/repo/.t3/worktrees/feature",
    observeCompletion: { onOutputLine: (line) => Queue.offer(outputLines, line) },
    project: {
      id: ProjectId.make("project-1"),
      workspaceRoot: "/repo",
      scripts: [
        {
          id: "setup",
          name: "Setup",
          command: "vp i",
          icon: "configure",
          runOnWorktreeCreate: true,
        },
      ],
    },
  });
  const emit = (terminalEvent: TerminalEvent) =>
    Effect.suspend(() =>
      Effect.forEach([...listeners], (listener) => listener(terminalEvent), { discard: true }),
    );
  return { start, listeners, writes, outputLines, emit };
});

const setup = Effect.fn(function* (options: { readonly history?: string } = {}) {
  const harness = yield* makeHarness(options);
  const result = yield* harness.start;
  const completion = result.status === "started" ? result.completion : undefined;
  if (!completion) return yield* Effect.die("setup did not start an observed run");
  return { ...harness, result, completion };
});

describe("ProjectSetupScriptRunner", () => {
  it.effect("shows a startup prompt and types only once the line editor starts", () =>
    Effect.gen(function* () {
      const { result, writes, outputLines, emit } = yield* setup();
      expect(result).toMatchObject({ status: "started", terminalId });

      yield* emit({ ...event, type: "output", data: "Missing Node.\r\nInstall it? [y/N]: " });
      yield* emit({ ...event, type: "activity", hasRunningSubprocess: true, label: "fnm" });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(writes)).toBe(0);
      expect(yield* Queue.takeAll(outputLines)).toEqual(["Missing Node.", "Install it? [y/N]:"]);

      yield* emit({ ...event, type: "activity", hasRunningSubprocess: false, label: "zsh" });
      yield* emit({ ...event, type: "output", data: "\r\n% \x1b[?2004h" });
      expect(yield* Queue.take(writes)).toMatch(/^ \( vp i\r\);/);
    }),
  );

  it.effect("types into a quiet shell that has no line editor", () =>
    Effect.gen(function* () {
      const { writes } = yield* setup();
      yield* TestClock.adjust("3 seconds");
      expect(yield* Queue.take(writes)).toMatch(/^ \( vp i\r\);/);
    }),
  );

  it.effect("ignores line editor state restored from an earlier shell", () =>
    Effect.gen(function* () {
      const { writes, emit } = yield* setup({ history: "old run\r\n% \x1b[?2004h" });
      yield* emit({ ...event, type: "activity", hasRunningSubprocess: true, label: "fnm" });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(writes)).toBe(0);
    }),
  );

  it.effect("forgets a busy startup child when the shell restarts", () =>
    Effect.gen(function* () {
      const { writes, emit } = yield* setup();
      yield* emit({ ...event, type: "activity", hasRunningSubprocess: true, label: "fnm" });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(writes)).toBe(0);
      yield* emit({ ...event, type: "restarted", snapshot: openSnapshot("") });
      // The restart lands inside a quiet window, so the next full window decides.
      yield* TestClock.adjust("6 seconds");
      expect(yield* Queue.take(writes)).toMatch(/^ \( vp i\r\);/);
    }),
  );

  it.effect("drops its terminal listeners when cancelled while the terminal opens", () =>
    Effect.gen(function* () {
      const opening = yield* Deferred.make<void>();
      const { start, listeners } = yield* makeHarness({ opening });
      const fiber = yield* Effect.forkChild(start);
      yield* Deferred.await(opening);
      expect(listeners.size).toBe(2);
      yield* Fiber.interrupt(fiber);
      expect(listeners.size).toBe(0);
    }),
  );

  it.effect("does not type into a shell that exits during startup", () =>
    Effect.gen(function* () {
      const { completion, writes, emit } = yield* setup();
      yield* emit({ ...event, type: "exited", exitCode: 1, exitSignal: null });
      expect(yield* completion).toMatchObject({ exitCode: null });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(writes)).toBe(0);
    }),
  );
});

const makeOwnedHarness = Effect.fn(function* (
  mode: "legacy" | "native",
  options: {
    readonly async?: boolean;
    readonly earlySentinel?: boolean;
    readonly failAck?: boolean;
  } = {},
) {
  const lifetime = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(lifetime, Exit.void));
  const listeners = new Set<(event: TerminalEvent) => Effect.Effect<void>>();
  const writes = yield* Queue.unbounded<string>();
  const acknowledgement = yield* Deferred.make<void>();
  const milestones: string[] = [];
  let revoked = false;
  let commandLine = "";
  const emit = (terminalEvent: TerminalEvent) =>
    Effect.suspend(() =>
      Effect.forEach([...listeners], (listener) => listener(terminalEvent), { discard: true }),
    );
  const sentinel = (exitCode: number) => {
    const token = /__T3_SETUP_DONE___[a-f0-9]+:/.exec(commandLine)?.[0];
    if (!token) return Effect.die("owned setup did not capture a completion token");
    return emit({ ...event, type: "output", data: `\r\n${token}${exitCode}\r\n% ` });
  };
  const guard = Effect.suspend(() =>
    revoked
      ? Effect.fail(
          new SyntheticSetupError({ message: "synthetic authority or generation changed" }),
        )
      : Effect.sync(() => {
          milestones.push("write-intent");
        }),
  );
  const physicalWrite = (data: string) =>
    Effect.gen(function* () {
      milestones.push("write-entered");
      yield* Queue.offer(writes, data);
      if (options.earlySentinel) yield* sentinel(0);
    });
  const acknowledge = Deferred.await(acknowledgement).pipe(
    Effect.andThen(
      Effect.suspend(() =>
        options.failAck
          ? Effect.fail(
              new SyntheticSetupError({ message: "synthetic durable write readback failed" }),
            )
          : Effect.sync(() => {
              milestones.push("write-acknowledged");
            }),
      ),
    ),
  );
  type OpenInput = Parameters<TerminalManager.TerminalManager["Service"]["open"]>[0];
  type OpenHooks = TerminalManager.LegacyTerminalPreparationHooks;
  function open(input: OpenInput): Effect.Effect<never>;
  function open(
    input: OpenInput,
    hooks: OpenHooks,
  ): ReturnType<TerminalManager.TerminalManager["Service"]["open"]>;
  function open(
    _input: OpenInput,
    hooks?: OpenHooks,
  ): ReturnType<TerminalManager.TerminalManager["Service"]["open"]> {
    if (!hooks) return Effect.die("owned setup bypassed its spawn hooks");
    return Effect.gen(function* () {
      yield* hooks.beforeSpawn({
        binding: hooks.binding,
        shell: "/bin/zsh",
        shellArgs: [],
        cwd: "/repo/.t3/worktrees/feature",
      });
      yield* hooks.afterSpawn({ binding: hooks.binding, shell: "/bin/zsh", shellArgs: [] });
      return openSnapshot("");
    }).pipe(
      Effect.mapError(
        () =>
          new TerminalManager.LegacyTerminalControlError({
            operation: "open",
            detail: "synthetic open guard failed",
          }),
      ),
    );
  }
  type WriteInput = Parameters<TerminalManager.TerminalManager["Service"]["write"]>[0];
  type WriteHooks = Parameters<TerminalManager.TerminalManager["Service"]["write"]>[1];
  function write(input: WriteInput): Effect.Effect<never>;
  function write(
    input: WriteInput,
    hooks: WriteHooks,
  ): ReturnType<TerminalManager.TerminalManager["Service"]["write"]>;
  function write(
    input: WriteInput,
    hooks?: WriteHooks,
  ): ReturnType<TerminalManager.TerminalManager["Service"]["write"]> {
    if (!hooks) return Effect.die("owned setup bypassed its write hooks");
    return Effect.gen(function* () {
      yield* hooks.beforeWrite();
      yield* physicalWrite(input.data);
      yield* hooks.afterWrite("accepted", 1);
    }).pipe(
      Effect.mapError(
        () =>
          new TerminalManager.LegacyTerminalControlError({
            operation: "write",
            detail: "synthetic write guard or readback failed",
          }),
      ),
    );
  }
  const control: NativeSetup.NativeSetupControl = {
    claimId: "claim",
    effectId: "effect",
    bootId: "boot",
    producerId: "producer",
    threadId,
    terminalId,
    generation: "generation",
    projectCwd: "/repo",
    worktreePath: "/repo/.t3/worktrees/feature",
    definitionDigest: "definition",
  };
  const terminals = Layer.mock(TerminalManager.TerminalManager)({
    open,
    write,
    openNativeSetup: (input, hooks) =>
      Effect.gen(function* () {
        const plan = { control: hooks.control, shell: "/bin/zsh", shellArgs: [], cwd: input.cwd };
        yield* hooks.beforeSpawn(plan);
        yield* hooks.afterSpawn({ ...plan, pid: 123 });
        return openSnapshot("");
      }).pipe(
        Effect.mapError(
          () =>
            new NativeSetup.NativeSetupControlError({
              operation: "open",
              message: "synthetic native open guard failed",
            }),
        ),
      ),
    writeNativeSetup: (input, _control, beforeWrite) =>
      beforeWrite.pipe(
        Effect.andThen(physicalWrite(input.data)),
        Effect.andThen(acknowledge),
        Effect.mapError(
          () =>
            new NativeSetup.NativeSetupControlError({
              operation: "write",
              message: "synthetic native write failed",
            }),
        ),
      ),
    observeNativeSetup: () =>
      Effect.succeed({
        control,
        status: "running" as const,
        pid: 123,
        writeEntered: true,
        exitCode: null,
      }),
    subscribe: (listener) =>
      Effect.sync(() => {
        listeners.add(listener);
        return () => listeners.delete(listener);
      }),
    closeIdle: () => Effect.die("owned terminal was closed by ordinary cleanup"),
  });
  const runner = yield* ProjectSetupScriptRunner.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        terminals,
        NodeCrypto.layer,
        ServerSettings.layerTest(),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: () => Effect.succeed(null),
        }),
      ),
    ),
    Effect.provideService(HostProcess.Platform, "darwin"),
    Effect.provideService(HostProcess.Environment, { SHELL: "/bin/zsh" }),
  );
  const script = {
    id: "setup",
    name: "Setup",
    command: "vp i",
    icon: "configure" as const,
    runOnWorktreeCreate: true,
    async: options.async ?? false,
  };
  const afterCompletion = () =>
    Effect.sync(() => {
      milestones.push("completion");
    });
  const binding = yield* Schema.decodeEffect(LegacyOwnedTerminalControl)({
    version: 1,
    threadId,
    runId: "run",
    terminalId,
    generation: "generation",
    preparationGeneration: "preparation",
    claimEventId: "claim",
    claimSequence: 1,
    claimReceiptSequence: 1,
    birthEventId: "birth",
    birthSequence: 2,
    birthReceiptSequence: 2,
    policy: {
      version: 1,
      createCommandId: "create",
      birthCommandId: "birth-command",
      releaseCommandId: "release",
      projectId: "project-1",
      threadId,
      messageId: "message",
      runId: "run",
      ownsNewThread: true,
      payloadHash: "payload",
    },
  });
  const result = yield* runner.runForThread({
    threadId,
    worktreePath: control.worktreePath,
    project: { id: ProjectId.make("project-1"), workspaceRoot: "/repo", scripts: [script] },
    ...(mode === "native"
      ? {
          nativePreparation: {
            lifetime,
            control,
            script,
            beforeSpawn: (
              plan: Parameters<
                ProjectSetupScriptRunner.NativeSetupPreparationHooks["beforeSpawn"]
              >[0],
            ) =>
              Effect.sync(() => {
                commandLine = plan.commandLine;
              }),
            afterSpawn: () => Effect.void,
            beforeWrite: () => guard,
            afterCompletion,
          },
        }
      : {
          legacyPreparation: {
            lifetime,
            binding,
            noScript: () => Effect.die("script exists"),
            beforeSpawn: (
              plan: Parameters<
                ProjectSetupScriptRunner.LegacySetupPreparationHooks["beforeSpawn"]
              >[0],
            ) =>
              Effect.sync(() => {
                commandLine = plan.commandLine;
              }),
            afterSpawn: () => Effect.void,
            beforeWrite: () => guard,
            afterWrite: () => acknowledge,
            afterCompletion,
          },
        }),
  });
  if (result.status !== "started" || result.completion === undefined)
    return yield* Effect.die("owned setup was not observed");
  return {
    result,
    completion: result.completion,
    listeners,
    writes,
    milestones,
    emit,
    sentinel,
    ready: emit({ ...event, type: "output", data: "% \x1b[?2004h" }),
    acknowledge: Deferred.succeed(acknowledgement, undefined),
    revoke: Effect.sync(() => {
      revoked = true;
    }),
    cancelLifetime: Scope.close(lifetime, Exit.void),
  };
});

describe("owned setup write milestones", () => {
  it.effect.each(["legacy", "native"] as const)(
    "%s completion waits for durable write acknowledgement even after an early sentinel",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode, { earlySentinel: true });
        const completed = yield* h.completion.pipe(Effect.forkChild({ startImmediately: true }));
        expect(h.listeners.size).toBe(2);
        expect(yield* Queue.size(h.writes)).toBe(0);
        yield* h.ready;
        yield* Queue.take(h.writes);
        expect(h.milestones).toEqual(["write-intent", "write-entered"]);
        yield* h.acknowledge;
        yield* h.result.writeCompletion;
        yield* h.result.writeCompletion;
        expect((yield* Fiber.join(completed)).exitCode).toBe(0);
        expect((yield* h.completion).exitCode).toBe(0);
        expect(h.milestones).toEqual([
          "write-intent",
          "write-entered",
          "write-acknowledged",
          "completion",
        ]);
        expect(h.listeners.size).toBe(0);
        expect(yield* Queue.size(h.writes)).toBe(0);
      }),
  );

  it.effect("legacy async write acknowledgement does not wait for script exit", () =>
    Effect.gen(function* () {
      const h = yield* makeOwnedHarness("legacy", { async: true });
      yield* h.ready;
      yield* Queue.take(h.writes);
      yield* h.acknowledge;
      yield* h.result.writeCompletion;
      expect(h.result.async).toBe(true);
      expect(h.milestones).not.toContain("completion");
      yield* h.sentinel(0);
      expect((yield* h.completion).exitCode).toBe(0);
      expect(h.listeners.size).toBe(0);
    }),
  );

  it.effect("native async setup still exposes completion only after an attributed exit", () =>
    Effect.gen(function* () {
      const h = yield* makeOwnedHarness("native", { async: true });
      yield* h.ready;
      yield* Queue.take(h.writes);
      yield* h.acknowledge;
      yield* h.result.writeCompletion;
      expect(h.milestones).not.toContain("completion");
      const waiter = yield* h.completion.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect(h.milestones).not.toContain("completion");
      yield* h.sentinel(0);
      expect((yield* Fiber.join(waiter)).exitCode).toBe(0);
      expect(h.milestones.at(-1)).toBe("completion");
      expect(h.listeners.size).toBe(0);
    }),
  );

  it.effect("cancelling an owned write milestone prevents a later readiness write", () =>
    Effect.gen(function* () {
      const h = yield* makeOwnedHarness("legacy");
      const waiter = yield* h.result.writeCompletion.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Fiber.interrupt(waiter);
      yield* h.ready;
      yield* TestClock.adjust("1 minute");
      expect(h.listeners.size).toBe(0);
      expect(yield* Queue.size(h.writes)).toBe(0);
      expect(h.milestones).toEqual([]);
    }),
  );

  it.effect.each(["legacy", "native"] as const)(
    "cancelling a %s completion waiter prevents a later readiness write",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode);
        const waiter = yield* h.completion.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Fiber.interrupt(waiter);
        yield* h.ready;
        yield* TestClock.adjust("1 minute");
        expect(h.listeners.size).toBe(0);
        expect(yield* Queue.size(h.writes)).toBe(0);
        expect(h.milestones).toEqual([]);
      }),
  );

  it.effect.each(["legacy", "native"] as const)(
    "closing the %s preparation lifetime cancels a pending writer without any observer",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode);
        yield* h.cancelLifetime;
        yield* h.ready;
        yield* TestClock.adjust("1 minute");
        expect(h.listeners.size).toBe(0);
        expect(yield* Queue.size(h.writes)).toBe(0);
        expect(h.milestones).toEqual([]);
      }),
  );

  it.effect.each(["legacy", "native"] as const)(
    "%s revalidates current authority and generation after readiness",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode);
        yield* h.revoke;
        yield* h.ready;
        const failure = yield* h.result.writeCompletion.pipe(Effect.flip);
        expect(failure).toMatchObject({
          _tag: "ProjectSetupScriptOperationError",
          operation: "writeCommand",
        });
        expect(yield* Queue.size(h.writes)).toBe(0);
        expect(h.listeners.size).toBe(0);
        expect(h.milestones).toEqual([]);
      }),
  );

  it.effect.each(["legacy", "native"] as const)(
    "%s exposes a write acknowledgement failure before completion hooks",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode, { failAck: true, earlySentinel: true });
        yield* h.ready;
        yield* Queue.take(h.writes);
        yield* h.acknowledge;
        const failure = yield* h.completion.pipe(Effect.flip);
        expect(failure).toMatchObject({
          _tag: "ProjectSetupScriptOperationError",
          operation: "writeCommand",
        });
        expect(h.milestones).toEqual(["write-intent", "write-entered"]);
        expect(h.listeners.size).toBe(0);
      }),
  );

  it.effect.each(["legacy", "native"] as const)(
    "%s startup exit fails its write milestone without invoking completion hooks",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeOwnedHarness(mode);
        yield* h.emit({ ...event, type: "exited", exitCode: 1, exitSignal: null });
        const failure = yield* h.completion.pipe(Effect.flip);
        expect(failure).toMatchObject({
          _tag: "ProjectSetupScriptOperationError",
          operation: "writeCommand",
        });
        expect(yield* Queue.size(h.writes)).toBe(0);
        expect(h.listeners.size).toBe(0);
        expect(h.milestones).toEqual([]);
      }),
  );
});
