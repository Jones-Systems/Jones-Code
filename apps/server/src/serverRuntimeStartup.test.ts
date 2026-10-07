import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  DEFAULT_MODEL,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";

import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";

import * as ServerConfig from "./config.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";

it("uses the canonical Codex model for auto-bootstrap", () => {
  assert.deepEqual(ServerRuntimeStartup.getAutoBootstrapThreadModelSelection(), {
    instanceId: ProviderInstanceId.make("codex"),
    model: DEFAULT_MODEL,
  });
});

it.effect("starts without scanning or rebuilding projection history", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const record = (label: string) => Ref.update(calls, (current) => [...current, label]);

    const result = yield* ServerRuntimeStartup.runOrderedV2StartupPhases({
      importLegacyShells: record("import"),
      recover: record("recover").pipe(Effect.as({ closedRequests: 2 })),
      recoverDelegatedTasks: record("delegated"),
      startEffectWorker: record("worker"),
      autoBootstrap: record("bootstrap").pipe(Effect.as({ projectId: "project-1" })),
    });

    // Delegated recovery reads the runs recovery terminalizes, and settles them
    // before the worker runs restart continuations that would otherwise race it.
    assert.deepEqual(yield* Ref.get(calls), [
      "import",
      "recover",
      "delegated",
      "worker",
      "bootstrap",
    ]);
    assert.deepEqual(result, {
      recovery: { closedRequests: 2 },
      bootstrap: { projectId: "project-1" },
    });
  }),
);

it.effect("interrupts the effect worker when awareness relay startup fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const workerInterrupted = yield* Ref.make(false);
      const workerFiberRef = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);

      const exit = yield* ServerRuntimeStartup.startEffectWorkerWithRelay({
        runWorker: Effect.never.pipe(Effect.ensuring(Ref.set(workerInterrupted, true))),
        startRelay: Effect.yieldNow.pipe(
          Effect.andThen(Effect.die("awareness relay startup failed")),
        ),
        workerFiberRef,
      }).pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(exit));
      assert.isTrue(yield* Ref.get(workerInterrupted));
      assert.isNull(yield* Ref.get(workerFiberRef));
    }),
  ),
);

it.effect("queues commands until startup signals readiness", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const gate = yield* ServerRuntimeStartup.makeCommandGate;
      const count = yield* Ref.make(0);
      const queued = yield* gate
        .enqueueCommand(Ref.updateAndGet(count, (value) => value + 1))
        .pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(count), 0);
      yield* gate.signalCommandReady;
      assert.equal(yield* Fiber.join(queued), 1);
    }),
  ),
);

it.effect("enqueueCommand fails queued work when readiness fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
      const failure = yield* Deferred.make<void, never>();

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Deferred.await(failure).pipe(Effect.as("should-not-run")))
        .pipe(Effect.forkScoped);

      yield* commandGate.failCommandReady(
        new ServerRuntimeStartup.ServerRuntimeStartupError({
          mode: "web",
          host: "127.0.0.1",
          port: 3773,
          cause: new Error("test startup failure"),
        }),
      );

      const error = yield* Effect.flip(Fiber.join(queuedCommandFiber));
      assert.equal(error.message, "Server runtime startup failed before command readiness.");
    }),
  ),
);

it.effect("resolveWelcomeBase derives cwd and project name from server config", () =>
  Effect.gen(function* () {
    const welcome = yield* ServerRuntimeStartup.resolveWelcomeBase.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
      } as never),
    );

    assert.deepStrictEqual(welcome, {
      cwd: "/tmp/startup-project",
      projectName: "startup-project",
    });
  }),
);

it.effect("automatic pull only updates enabled, behind, clean default-branch checkouts", () =>
  Effect.gen(function* () {
    const pulled: string[] = [];
    const git = {
      statusDetails: (cwd: string) =>
        Effect.succeed({
          isRepo: true,
          isDefaultBranch: cwd !== "/feature",
          hasUpstream: true,
          hasWorkingTreeChanges: cwd === "/dirty",
          aheadCount: cwd === "/ahead" ? 1 : 0,
          behindCount: cwd === "/current" ? 0 : 1,
        } as never),
      pullCurrentBranch: (cwd: string) =>
        Effect.sync(() => {
          pulled.push(cwd);
          return {
            status: "pulled" as const,
            refName: "main",
            upstreamRef: "origin/main",
          };
        }),
    } as unknown as GitVcsDriver.GitVcsDriver["Service"];
    const project = (workspaceRoot: string) =>
      ({ id: ProjectId.make(workspaceRoot), workspaceRoot }) as never;
    const overrides = (entries: Record<string, boolean>) => ({
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: Object.fromEntries(
        Object.entries(entries).map(([root, defaultAutoPull]) => [
          ProjectId.make(root),
          { defaultAutoPull },
        ]),
      ),
    });

    yield* ServerRuntimeStartup.autoPullProjects(
      [
        project("/clean"),
        project("/current"),
        project("/dirty"),
        project("/ahead"),
        project("/feature"),
        project("/disabled"),
      ],
      overrides({
        "/clean": true,
        "/current": true,
        "/dirty": true,
        "/ahead": true,
        "/feature": true,
        "/disabled": false,
      }),
    ).pipe(Effect.provideService(GitVcsDriver.GitVcsDriver, git));

    assert.deepStrictEqual(pulled, ["/clean"]);

    pulled.length = 0;
    yield* ServerRuntimeStartup.autoPullProjects(
      [project("/inherited"), project("/opted-out"), project("/dirty")],
      { ...overrides({ "/opted-out": false }), defaultAutoPull: true },
    ).pipe(Effect.provideService(GitVcsDriver.GitVcsDriver, git));
    assert.deepStrictEqual(pulled, ["/inherited"]);
  }),
);

it.effect("holds recovery, activation and queued commands before the trial grant", () =>
  Effect.scoped(Effect.gen(function* () {
    const trialEntered = yield* Deferred.make<void>();
    const grant = yield* Deferred.make<void>();
    const gate = yield* ServerRuntimeStartup.makeCommandGate;
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const record = (label: string) => Ref.update(calls, (current) => [...current, label]);
    const queued = yield* gate.enqueueCommand(record("command")).pipe(Effect.forkScoped);
    const startup = yield* ServerRuntimeStartup.runOrderedV2StartupPhases({
      awaitTrialCommit: Deferred.succeed(trialEntered, undefined).pipe(
        Effect.andThen(Deferred.await(grant)), Effect.andThen(record("reservation")),
      ),
      importLegacyShells: record("legacy"), recover: record("provider"),
      recoverDelegatedTasks: record("delegated"), startEffectWorker: record("worker"),
      autoBootstrap: record("bootstrap"),
    }).pipe(
      Effect.andThen(record("activation")),
      Effect.andThen(gate.signalCommandReady),
      Effect.forkScoped,
    );
    yield* Deferred.await(trialEntered);
    assert.deepEqual(yield* Ref.get(calls), []);
    yield* Deferred.succeed(grant, undefined);
    yield* Fiber.join(startup);
    yield* Fiber.join(queued);
    assert.deepEqual(yield* Ref.get(calls), [
      "reservation", "legacy", "provider", "delegated", "worker", "bootstrap", "activation", "command",
    ]);
  })),
);

it.effect("a failed native trial fails readiness and leaves every recovery step held", () =>
  Effect.scoped(Effect.gen(function* () {
    const gate = yield* ServerRuntimeStartup.makeCommandGate;
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const record = Ref.update(calls, (current) => [...current, "must not dispatch"]);
    const failure = new ServerRuntimeStartup.ServerRuntimeStartupError({
      mode: "desktop", host: "127.0.0.1", port: 4888, cause: "Synthetic mismatched commit identity",
    });
    const queued = yield* gate.enqueueCommand(record).pipe(Effect.forkScoped);
    const exit = yield* ServerRuntimeStartup.runOrderedV2StartupPhases({
      awaitTrialCommit: Effect.fail(failure), importLegacyShells: record, recover: record,
      recoverDelegatedTasks: record, startEffectWorker: record, autoBootstrap: record,
    }).pipe(Effect.tapError((error) => gate.failCommandReady(error)), Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Effect.flip(Fiber.join(queued)), failure);
    assert.deepEqual(yield* Ref.get(calls), []);
  })),
);

it.effect("pre-activation native trial shutdown preserves continuation rows and cleans local sessions", () =>
  Effect.gen(function* () {
    const continuationRows = yield* Ref.make(["paired-continuation"]);
    const localCleanup = yield* Ref.make(false);
    yield* ServerRuntimeStartup.runRuntimeShutdown({
      continuationWritesAllowed: false,
      prepareForShutdown: Ref.set(continuationRows, ["rewritten"]),
      shutdownSessions: Ref.set(localCleanup, true),
      reconcile: Ref.set(continuationRows, []),
    });
    assert.deepEqual(yield* Ref.get(continuationRows), ["paired-continuation"]);
    assert.isTrue(yield* Ref.get(localCleanup));
  }),
);

it.effect("ordinary and activated runtime shutdown preserves preparation, cleanup and reconciliation order", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<string>>([]);
    const record = (label: string) => Ref.update(calls, (current) => [...current, label]);
    yield* ServerRuntimeStartup.runRuntimeShutdown({
      continuationWritesAllowed: true, prepareForShutdown: record("prepare"),
      shutdownSessions: record("cleanup"), reconcile: record("reconcile"),
    });
    assert.deepEqual(yield* Ref.get(calls), ["prepare", "cleanup", "reconcile"]);
  }),
);
