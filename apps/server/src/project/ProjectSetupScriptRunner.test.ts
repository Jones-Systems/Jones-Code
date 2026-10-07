import { assert, it, vi } from "@effect/vitest";
import { CommandId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { LegacyOwnedTerminalControl } from "../orchestration-v2/RecordedTypes.ts";
import { legacyBootstrapCreateCommandId } from "../orchestration-v2/LegacyBootstrap.ts";

import * as TerminalManager from "../terminal/Manager.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectService from "./ProjectService.ts";
import * as ProjectSetupScriptRunner from "./ProjectSetupScriptRunner.ts";

it.effect("resolves setup scripts through the standalone project service", () => {
  const open = vi.fn((input: Parameters<TerminalManager.TerminalManager["Service"]["open"]>[0]) =>
    Effect.succeed({
      threadId: input.threadId,
      terminalId: input.terminalId,
      cwd: input.cwd,
      worktreePath: input.worktreePath ?? null,
      status: "running" as const,
      pid: 123,
      history: "",
      exitCode: null,
      exitSignal: null,
      label: "Shell",
      updatedAt: "2026-06-20T00:00:00.000Z",
    }),
  );
  const write = vi.fn(
    (_input: Parameters<TerminalManager.TerminalManager["Service"]["write"]>[0]) => Effect.void,
  );
  const listeners: Array<Parameters<TerminalManager.TerminalManager["Service"]["subscribe"]>[0]> =
    [];
  const subscribe: TerminalManager.TerminalManager["Service"]["subscribe"] = (listener) =>
    Effect.sync(() => {
      listeners.push(listener);
      return () => undefined;
    });
  const projectId = ProjectId.make("project:setup-runner-v2");
  const project = {
    id: projectId,
    title: "Project",
    workspaceRoot: "/repo",
    repositoryIdentity: null,
    faviconPath: null,
    defaultModelSelection: null,
    scripts: [
      {
        id: "setup",
        name: "Setup",
        command: "vp install",
        icon: "configure" as const,
        runOnWorktreeCreate: true,
      },
    ],
    createdAt: "2026-06-20T00:00:00.000Z",
    updatedAt: "2026-06-20T00:00:00.000Z",
    deletedAt: null,
  };
  const layer = ProjectSetupScriptRunner.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectService.ProjectService)({
          getById: () => Effect.succeed(Option.some(project)),
        }),
        Layer.mock(TerminalManager.TerminalManager)({ open, write, subscribe }),
        ServerSettings.layerTest(),
      ),
    ),
  );

  return Effect.gen(function* () {
    const runner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
    const result = yield* runner.runForThread({
      threadId: "thread-1",
      projectId,
      worktreePath: "/repo-worktree",
    });
    assert.deepEqual(result, {
      status: "started",
      async: true,
      scriptId: "setup",
      scriptName: "Setup",
      scriptCommand: "vp install",
      terminalId: "setup-setup",
      cwd: "/repo-worktree",
    });
    assert.equal(open.mock.calls[0]?.[0].cwd, "/repo-worktree");
    assert.deepEqual(open.mock.calls[0]?.[0].env, {
      T3CODE_PROJECT_ROOT: "/repo",
      T3CODE_WORKTREE_PATH: "/repo-worktree",
      COLORTERM: "",
      NO_COLOR: "1",
      FORCE_COLOR: "0",
    });
    assert.equal(write.mock.calls[0]?.[0].data, "vp install\r");
    const lines: string[] = [];
    const observed = yield* runner.runForThread({
      threadId: "thread-1",
      projectId,
      worktreePath: "/repo-worktree",
      observeCompletion: {
        onOutputLine: (line) =>
          Effect.sync(() => {
            lines.push(line);
          }),
      },
    });
    assert.equal(observed.status, "started");
    const listener = listeners[0]!;
    yield* listener({
      type: "output",
      threadId: "thread-1",
      terminalId: "setup-setup",
      data: "Downloading 10%\rDownloading 20%\r\nDone\n",
    });
    assert.deepEqual(lines, ["Downloading 10%", "Downloading 20%", "Done"]);
    yield* listener({ type: "closed", threadId: "thread-1", terminalId: "setup-setup" });
  }).pipe(Effect.provide(layer));
});

it.effect(
  "legacy setup captures the actual shell and definition before spawn and never re-resolves a captured retry",
  () => {
    const threadId = ThreadId.make("legacy:setup-owner");
    const projectId = ProjectId.make("legacy:setup-owner:project");
    const releaseCommandId = CommandId.make("legacy:setup-owner:C");
    const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
    const binding = Schema.decodeUnknownSync(LegacyOwnedTerminalControl)({
      version: 1,
      threadId,
      runId: "legacy:setup-owner:run",
      terminalId: "legacy:setup-owner:terminal",
      generation: "owned-terminal-generation",
      preparationGeneration: "birth-generation",
      claimEventId: "claim",
      claimSequence: 1,
      claimReceiptSequence: 1,
      birthEventId: "birth",
      birthSequence: 2,
      birthReceiptSequence: 2,
      policy: {
        version: 1,
        createCommandId,
        birthCommandId: `${createCommandId}:initial-message`,
        releaseCommandId,
        projectId,
        threadId,
        messageId: "legacy:setup-owner:M",
        runId: "legacy:setup-owner:run",
        ownsNewThread: true,
        payloadHash: "legacy:setup-owner:payload",
      },
    });
    const script = {
      id: "setup",
      name: "Captured setup",
      command: "synthetic-command",
      icon: "configure" as const,
      runOnWorktreeCreate: true,
      async: false,
    };
    const project = {
      id: projectId,
      title: "Synthetic project",
      workspaceRoot: "/repo",
      repositoryIdentity: null,
      faviconPath: null,
      defaultModelSelection: null,
      scripts: [script],
      createdAt: "2026-10-01T00:00:00.000Z",
      updatedAt: "2026-10-01T00:00:00.000Z",
      deletedAt: null,
    };
    let definition:
      | NonNullable<ProjectSetupScriptRunner.LegacySetupPreparationHooks["capturedDefinition"]>
      | undefined;
    let spawns = 0;
    const writes: string[] = [];
    const milestones: string[] = [];
    type OpenInput = Parameters<TerminalManager.TerminalManager["Service"]["open"]>[0];
    type OpenHooks = Parameters<TerminalManager.TerminalManager["Service"]["open"]>[1];
    function open(input: OpenInput): Effect.Effect<never>;
    function open(
      input: OpenInput,
      hooks: OpenHooks,
    ): ReturnType<TerminalManager.TerminalManager["Service"]["open"]>;
    function open(
      input: OpenInput,
      hooks?: OpenHooks,
    ): ReturnType<TerminalManager.TerminalManager["Service"]["open"]> {
      return Effect.gen(function* () {
        if (hooks === undefined)
          return yield* Effect.die("Legacy fixture must use the executing owner hooks.");
        yield* hooks.beforeSpawn({
          binding: hooks.binding,
          shell: "/usr/bin/fish",
          shellArgs: ["-l"],
          cwd: input.cwd ?? "/repo",
        });
        milestones.push("spawn");
        spawns++;
        yield* hooks.afterSpawn({
          binding: hooks.binding,
          shell: "/usr/bin/fish",
          shellArgs: ["-l"],
        });
        return {
          threadId: input.threadId,
          terminalId: input.terminalId,
          cwd: input.cwd ?? "/repo",
          worktreePath: input.worktreePath ?? null,
          status: "running" as const,
          pid: 123,
          history: "",
          exitCode: null,
          exitSignal: null,
          label: "Shell",
          updatedAt: project.createdAt,
        };
      }).pipe(
        Effect.mapError(
          () =>
            new TerminalManager.LegacyTerminalControlError({
              operation: "open",
              detail: "Synthetic owner refused spawn.",
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
      return Effect.gen(function* () {
        if (hooks === undefined)
          return yield* Effect.die("Legacy fixture requires a write intent.");
        yield* hooks.beforeWrite();
        milestones.push("write");
        writes.push(input.data);
        yield* hooks.afterWrite("accepted", writes.length);
      }).pipe(
        Effect.mapError(
          () =>
            new TerminalManager.LegacyTerminalControlError({
              operation: "write",
              detail: "Synthetic owner refused write.",
            }),
        ),
      );
    }
    const terminal = Layer.mock(TerminalManager.TerminalManager)({
      open,
      write,
      subscribe: () => Effect.succeed(() => undefined),
    });
    const hooks: ProjectSetupScriptRunner.LegacySetupPreparationHooks = {
      binding,
      noScript: () => Effect.die("A setup definition exists."),
      beforeSpawn: (selected) =>
        Effect.sync(() => {
          definition = selected;
          milestones.push("open-intent");
        }),
      afterSpawn: () =>
        Effect.sync(() => {
          milestones.push("open-outcome");
        }),
      beforeWrite: () =>
        Effect.sync(() => {
          milestones.push("write-intent");
        }),
      afterWrite: () =>
        Effect.sync(() => {
          milestones.push("write-outcome");
        }),
      afterCompletion: () => Effect.void,
    };
    let settingsReads = 0;
    const settings = Layer.effect(
      ServerSettings.ServerSettingsService,
      Effect.gen(function* () {
        const original = yield* ServerSettings.ServerSettingsService;
        return {
          ...original,
          getSettings: Effect.suspend(() => {
            settingsReads++;
            return settingsReads === 1
              ? original.getSettings
              : Effect.die("Captured setup must not re-read settings.");
          }),
        };
      }),
    ).pipe(Layer.provide(ServerSettings.layerTest()));
    const layer = ProjectSetupScriptRunner.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          terminal,
          settings,
          Layer.mock(ProjectService.ProjectService)({
            getById: () => Effect.succeed(Option.some(project)),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const runner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
      const result = yield* runner.runForThread({
        threadId,
        projectId,
        worktreePath: "/owned",
        legacyPreparation: hooks,
      });
      assert.equal(result.status, "started");
      assert.deepEqual(milestones, [
        "open-intent",
        "spawn",
        "open-outcome",
        "write-intent",
        "write",
        "write-outcome",
      ]);
      if (definition === undefined) return yield* Effect.die("Missing captured definition.");
      assert.equal(definition.shell, "/usr/bin/fish");
      assert.deepEqual(definition.shellArgs, ["-l"]);
      assert.equal(definition.async, false);
      assert.equal(definition.command, script.command);
      assert.match(definition.commandLine, /^begin\rsynthetic-command\rend;/u);
      assert.equal(writes[0], `${definition.commandLine}\r`);
      const capturedDefinition = definition;
      const refused = yield* runner
        .runForThread({
          threadId,
          projectId,
          worktreePath: "/owned",
          legacyPreparation: {
            ...hooks,
            capturedDefinition,
            beforeSpawn: (same) =>
              Effect.gen(function* () {
                assert.deepEqual(same, capturedDefinition);
                return yield* new TerminalManager.LegacyTerminalControlError({
                  operation: "open",
                  detail: "Captured generation is reconciliation-only.",
                });
              }),
          },
        })
        .pipe(Effect.result);
      assert.equal(refused._tag, "Failure");
      assert.equal(spawns, 1);
      assert.lengthOf(writes, 1);
      const changed = yield* runner
        .runForThread({
          threadId,
          projectId,
          worktreePath: "/replacement",
          legacyPreparation: { ...hooks, capturedDefinition },
        })
        .pipe(Effect.result);
      assert.equal(changed._tag, "Failure");
      assert.equal(spawns, 1);
      assert.lengthOf(writes, 1);
      assert.equal(settingsReads, 1);
    }).pipe(Effect.provide(layer));
  },
);
