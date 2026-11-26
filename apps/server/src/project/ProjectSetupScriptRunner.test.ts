import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import {
  ProjectId,
  TerminalNotRunningError,
  type TerminalEvent,
  type TerminalOpenInput,
  type TerminalSessionSnapshot,
} from "@t3tools/contracts";
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
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
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
  readonly command?: string;
  readonly history?: string;
  readonly opening?: Deferred.Deferred<void>;
  readonly previousSession?: TerminalSessionSnapshot;
  readonly shell?: string | null;
}) {
  const listeners = new Set<(event: TerminalEvent) => Effect.Effect<void>>();
  const opens: TerminalOpenInput[] = [];
  const openedSessions: TerminalSessionSnapshot[] = [];
  let session = options.previousSession;
  const writes = yield* Queue.unbounded<string>();
  const outputLines = yield* Queue.unbounded<string>();
  const terminals = Layer.mock(TerminalManager.TerminalManager)({
    open: (input) =>
      Effect.sync(() => opens.push(input)).pipe(
        Effect.andThen(
          options.opening
            ? Deferred.succeed(options.opening, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
        ),
        Effect.map(() => {
          session ??= openSnapshot(options.history ?? "");
          openedSessions.push(session);
          return session;
        }),
      ),
    getShell: () =>
      options.shell === null
        ? Effect.fail(new TerminalNotRunningError({ threadId, terminalId }))
        : Effect.succeed(options.shell ?? "/bin/zsh"),
    subscribe: (listener) =>
      Effect.sync(() => {
        listeners.add(listener);
        return () => listeners.delete(listener);
      }),
    write: (input) => Queue.offer(writes, input.data).pipe(Effect.asVoid),
    close: () =>
      Effect.sync(() => {
        session = undefined;
      }).pipe(
        Effect.andThen(
          Effect.forEach([...listeners], (listener) => listener({ ...event, type: "closed" }), {
            discard: true,
          }),
        ),
      ),
    closeIdle: () => Effect.void,
    closeOnSettle: () => Effect.void,
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
          command: options.command ?? "vp i",
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
  return { start, listeners, opens, openedSessions, writes, outputLines, emit };
});

const setup = Effect.fn(function* (
  options: {
    readonly command?: string;
    readonly history?: string;
    readonly previousSession?: TerminalSessionSnapshot;
    readonly shell?: string | null;
  } = {},
) {
  const harness = yield* makeHarness(options);
  const result = yield* harness.start;
  const completion = result.status === "started" ? result.completion : undefined;
  if (!completion) return yield* Effect.die("setup did not start an observed run");
  return { ...harness, result, completion };
});

describe("ProjectSetupScriptRunner", () => {
  it.effect("keeps setup scripts independent of the interactive shell preference", () =>
    Effect.gen(function* () {
      const { opens } = yield* setup();
      expect(opens[0]?.env?.T3CODE_TERMINAL_SHELL).toBe("");
    }),
  );

  it.effect("writes immediately when the spawned fallback is PowerShell", () =>
    Effect.gen(function* () {
      const { writes } = yield* setup({
        shell: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
      });
      const submitted = yield* Queue.take(writes);
      expect(submitted).toContain("$global:LASTEXITCODE");
      expect(submitted).not.toContain("\x1b[200~");
    }),
  );

  it.effect("types an escaped Nushell wrapper once its line editor starts", () =>
    Effect.gen(function* () {
      const command =
        'print "quoted"; print \'C:\\tools\'; print "$env.PATH"\nexit 7 # comment\r\n\t';
      const escaped =
        'print \\"quoted\\"; print \'C:\\\\tools\'; print \\"$env.PATH\\"\\u{a}exit 7 # comment\\u{d}\\u{a}\\u{9}';
      const { writes, emit } = yield* setup({ shell: "/opt/homebrew/bin/nu", command });
      yield* TestClock.adjust("1 second");
      expect(yield* Queue.size(writes)).toBe(0);
      yield* emit({ ...event, type: "output", data: "> \x1b[?2004h" });
      const written = yield* Queue.take(writes);
      const sentinel = written.match(/__T3_SETUP_DONE___[0-9a-f]{32}:/)?.[0];
      expect(sentinel).toBeDefined();
      expect(written).toBe(
        ` ${[
          "$env.LAST_EXIT_CODE = 0",
          "let __t3f = try { mktemp --tmpdir-path $env.T3CODE_WORKTREE_PATH --suffix .nu .t3-setup-XXXXXXXX } catch {|e| print -e $e.msg; null }",
          `if $__t3f == null { $env.LAST_EXIT_CODE = 1 } else { try { [$"rm --permanent ($__t3f | to nuon)" "${escaped}"] | str join (char newline) | save --force --raw $__t3f; ^$nu.current-exe --login --commands $"cd $env.T3CODE_WORKTREE_PATH; source ($__t3f | to nuon)" } catch {|e| if $e != null and $e.exit_code? == null { print -e $e.msg }; if $env.LAST_EXIT_CODE == 0 { $env.LAST_EXIT_CODE = 1 } } }`,
          "let __t3c = $env.LAST_EXIT_CODE",
          "try { if $__t3f != null and ($__t3f | path exists) { rm --permanent $__t3f } } catch {|e| print -e $e.msg }",
          `print $"\\n${sentinel}($__t3c)"`,
        ].join("; ")}\r`,
      );
    }),
  );

  it.effect("replaces a surviving setup shell and types only once the line editor starts", () =>
    Effect.gen(function* () {
      const { result, completion, openedSessions, writes, outputLines, emit } = yield* setup({
        previousSession: openSnapshot("previous setup in the deleted checkout"),
      });
      expect(result).toMatchObject({ status: "started", terminalId });
      expect(openedSessions).toEqual([openSnapshot("")]);

      yield* emit({ ...event, type: "output", data: "Missing Node.\r\nInstall it? [y/N]: " });
      yield* emit({ ...event, type: "activity", hasRunningSubprocess: true, label: "fnm" });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(writes)).toBe(0);
      expect(yield* Queue.takeAll(outputLines)).toEqual(["Missing Node.", "Install it? [y/N]:"]);

      yield* emit({ ...event, type: "activity", hasRunningSubprocess: false, label: "zsh" });
      yield* emit({ ...event, type: "output", data: "\r\n% \x1b[?2004h" });
      const command = yield* Queue.take(writes);
      expect(command).toContain("vp i");
      const sentinel = command.match(/__T3_SETUP_DONE___[a-f0-9]+:/)?.[0];
      expect(sentinel).toBeDefined();
      yield* emit({ ...event, type: "output", data: `${sentinel}0\r\n% ` });
      expect(yield* completion).toMatchObject({ exitCode: 0 });
    }),
  );

  it.effect("drops whole escape sequences from output lines", () =>
    Effect.gen(function* () {
      const { outputLines, emit } = yield* setup();
      yield* emit({
        ...event,
        type: "output",
        data: "\x1b7\x1b[24;0fProgress: [ 40%]\x1b8\r\n\x1b%G\x1b#8\x1b[4:3mdone\x1bM\r\n",
      });
      expect(yield* Queue.takeAll(outputLines)).toEqual(["Progress: [ 40%]", "done"]);
    }),
  );

  it.effect("keeps emoji whole when capping a long output line or partial-line buffer", () =>
    Effect.gen(function* () {
      const { outputLines, emit } = yield* setup();
      yield* emit({ ...event, type: "output", data: `x${"🙂".repeat(250)}\r\n` });
      yield* emit({ ...event, type: "output", data: `${"🙂".repeat(2_100)}x` });
      yield* emit({ ...event, type: "output", data: "\r\n" });
      expect(yield* Queue.takeAll(outputLines)).toEqual([`x${"🙂".repeat(199)}`, "🙂".repeat(200)]);
    }),
  );

  it.effect("types into a quiet shell that has no line editor", () =>
    Effect.gen(function* () {
      const { writes } = yield* setup({ command: "printf '%s\\n' 'setup-ready'\n".repeat(120) });
      yield* TestClock.adjust("3 seconds");
      const submitted = yield* Queue.take(writes);
      expect(submitted).toContain("setup-ready");
      expect(submitted).not.toContain("\x1b[200~");
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
      expect(yield* Queue.take(writes)).toContain("vp i");
    }),
  );

  it.effect.each(["opening", "completion"] as const)(
    "stops pending setup when cancelled during %s",
    (phase) =>
      Effect.gen(function* () {
        const opening = yield* Deferred.make<void>();
        const { start, listeners, writes, emit } = yield* makeHarness(
          phase === "opening" ? { opening } : {},
        );
        const pending =
          phase === "opening"
            ? start.pipe(Effect.asVoid)
            : yield* start.pipe(
                Effect.map((result) =>
                  result.status === "started" && result.completion
                    ? result.completion.pipe(Effect.asVoid)
                    : Effect.die("setup did not start"),
                ),
              );
        const fiber = yield* pending.pipe(Effect.forkChild({ startImmediately: true }));
        if (phase === "opening") yield* Deferred.await(opening);
        expect(listeners.size).toBe(2);
        yield* Fiber.interrupt(fiber);
        expect(listeners.size).toBe(0);
        yield* emit({ ...event, type: "output", data: "\r\n% \x1b[?2004h" });
        expect(yield* Queue.size(writes)).toBe(0);
      }),
  );

  it.effect.each([false, true])(
    "does not type into a shell that exits during startup (before shell lookup: %s)",
    (beforeShellLookup) =>
      Effect.gen(function* () {
        const { completion, writes, emit, listeners } = yield* setup(
          beforeShellLookup ? { shell: null } : {},
        );
        if (!beforeShellLookup) {
          yield* emit({ ...event, type: "exited", exitCode: 1, exitSignal: null });
        }
        expect(yield* completion).toMatchObject({ exitCode: null });
        yield* TestClock.adjust("1 minute");
        expect(yield* Queue.size(writes)).toBe(0);
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

const decodeEffectLegacyOwnedTerminalControl = Schema.decodeEffect(LegacyOwnedTerminalControl);

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
  const binding = yield* decodeEffectLegacyOwnedTerminalControl({
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

// The terminal reports the shell that runs the submitted wrapper.
const observedShell = "/bin/bash";

const runObservedShell = Effect.fnUntraced(function* (args: ReadonlyArray<string>) {
  const cwd = yield* HostProcess.WorkingDirectory;
  const env = yield* HostProcess.Environment;
  return yield* Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner.spawn(
      ChildProcess.make(observedShell, ["--noprofile", "--norc", ...args], {
        cwd,
        env: { PATH: env.PATH },
      }),
    );
    return yield* Effect.all(
      {
        stdout: handle.stdout.pipe(Stream.decodeText(), Stream.mkString),
        stderr: handle.stderr.pipe(Stream.decodeText(), Stream.mkString),
        exitCode: handle.exitCode,
      },
      { concurrency: "unbounded" },
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
});

const runObservedSetup = Effect.fnUntraced(function* (command: string) {
  const { writes, outputLines, emit, completion } = yield* setup({ command, shell: observedShell });
  yield* emit({ ...event, type: "output", data: "\x1b[?2004h" });
  const submitted = yield* Queue.take(writes);
  const shellInput = submitted.replace("\x1b[200~", "").replace("\x1b[201~", "");

  // Enter becomes a newline at the terminal boundary; transport assertions below
  // cover line-editor submissions and canonical TTY limits that bash -c bypasses.
  const child = yield* runObservedShell(["-c", shellInput.replaceAll("\r", "\n")]);
  yield* emit({ ...event, type: "output", data: submitted + "\n" });
  yield* emit({ ...event, type: "output", data: child.stdout + child.stderr });
  yield* emit({ ...event, type: "closed" });
  return {
    submitted,
    shellInput,
    lines: yield* Queue.clear(outputLines),
    exitCode: (yield* completion).exitCode,
  };
});

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")("POSIX setup completion", () => {
  it.live("submits one line while preserving multiline shell syntax and literal text", () =>
    Effect.gen(function* () {
      const result = yield* runObservedSetup(
        [
          "printf '%s\\n' \"it's ready\" 'literal \\n $(printf wrong) `printf wrong` café'",
          "cat <<'END'",
          "$literal \\path 雪",
          "END",
          "printf '%s\\n' done # trailing comment",
        ].join("\r\n"),
      );
      assert.equal(result.submitted.at(-1), "\r");
      assert.notMatch(result.submitted.slice(0, -1), /[\r\n]/);
      assert.deepEqual(result.lines, [
        "it's ready",
        "literal \\n $(printf wrong) `printf wrong` café",
        "$literal \\path 雪",
        "done",
      ]);
      assert.equal(result.exitCode, 0);
    }),
  );

  it.live("completes long scripts without exceeding canonical terminal line limits", () =>
    Effect.gen(function* () {
      const expected = Array.from(
        { length: 120 },
        (_, index) => `line-${index}: café 雪 'quoted' \\path`,
      );
      const command = expected
        .map((line) => `printf '%s\\n' "${line.replaceAll("\\", "\\\\")}"`)
        .join("\n");
      const utf8 = new TextEncoder();
      assert.isAbove(utf8.encode(command).byteLength, 4_096);

      const result = yield* runObservedSetup(command);
      assert.deepEqual(result.lines, expected);
      assert.equal(result.exitCode, 0);
      assert.equal(result.submitted.at(-1), "\r");
      assert.isTrue(result.submitted.startsWith(" \x1b[200~"));
      assert.isTrue(result.submitted.endsWith("\x1b[201~\r"));
      assert.notInclude(result.submitted.slice(0, -1), "\r");
      for (const line of result.shellInput.slice(0, -1).split("\n")) {
        assert.isBelow(utf8.encode(line).byteLength, 1_024);
      }
    }),
  );

  it.live("preserves a trailing backslash-newline even when losing it would still exit zero", () =>
    Effect.gen(function* () {
      const result = yield* runObservedSetup("printf '%s\\n' direct\\\n");
      assert.deepEqual(result.lines, ["direct"]);
      assert.equal(result.exitCode, 0);
    }),
  );

  it.live.each([
    { name: "an explicit exit", command: "exit 7", exitCode: 7 },
    // The shell picks this status: bash 5 reports 2, while macOS's bash 3.2 reports 1.
    { name: "a syntax error", command: "if then", exitCode: null },
  ])("reports a nonzero completion for $name", ({ command, exitCode }) =>
    Effect.gen(function* () {
      // Completion must carry the status this shell itself gives when evaluating the script.
      const reference = yield* runObservedShell(["-c", 'eval "$1"', "t3-reference", command]);
      assert.notEqual(reference.exitCode, 0);
      if (exitCode !== null) assert.equal(reference.exitCode, exitCode);
      const result = yield* runObservedSetup(command);
      assert.equal(result.exitCode, reference.exitCode);
    }),
  );
});
