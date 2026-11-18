import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ProjectId, RunId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { makeLaunchFixture } from "./LaunchPreparation.testkit.ts";
import type { LaunchOperation } from "./LaunchPreparation.ts";

const seed = Effect.fn("LaunchPreparationTest.seed")(function* (
  g: Effect.Success<ReturnType<typeof makeLaunchFixture>>,
  strategy: "root" | "existing_worktree" | "worktree" = "worktree",
) {
  const now = DateTime.formatIso(yield* DateTime.now);
  yield* g.sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES('launch-project','Synthetic',${g.f.bPath},'[]',${now},${now})`;
  const command = {
    type: "thread.create" as const,
    commandId: CommandId.make("launch-original"),
    threadId: ThreadId.make("launch-thread"),
    projectId: ProjectId.make("launch-project"),
    title: "Preparing",
    modelSelection: { instanceId: g.instanceId, model: "synthetic" },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch: null,
    worktreePath: null,
    createdBy: "user" as const,
    creationSource: "web" as const,
    initialMessage: {
      messageId: MessageId.make("launch-message"),
      text: "Prepare target",
      attachments: [],
      workspaceStrategy:
        strategy === "root"
          ? { type: "root" as const }
          : strategy === "existing_worktree"
            ? { type: "existing_worktree" as const, worktreePath: g.f.bPath }
            : { type: "worktree" as const, baseRef: "main", branch: "work" },
    },
  };
  yield* g.launch.accept(command, [g.f.bPath], g.threads.dispatch);
  const origins = yield* g.checkout.readPreparationOrigins(command.commandId);
  assert.lengthOf(origins, 1);
  const origin = origins[0]!;
  const workspace = {
    type: "thread.metadata.update" as const,
    commandId: CommandId.make("launch-workspace"),
    threadId: command.threadId,
    branch: strategy === "worktree" ? "work" : null,
    worktreePath:
      strategy === "root" ? null : strategy === "existing_worktree" ? g.f.bPath : g.worktreePath,
  };
  const release = {
    type: "prepared-run.release" as const,
    commandId: CommandId.make("launch-release"),
    threadId: command.threadId,
    runId: RunId.make(origin.runId),
  };
  const materialize = (operation: LaunchOperation) =>
    strategy === "worktree"
      ? g.launch
          .createWorktree(operation, {
            cwd: g.f.bPath,
            refName: "main",
            newRefName: "work",
            path: null,
          })
          .pipe(Effect.asVoid)
      : Effect.void;
  const settleSetup = (operation: LaunchOperation) =>
    workspace.worktreePath === null
      ? Effect.void
      : g.launch
          .runSetup(operation, {
            threadId: command.threadId,
            projectId: command.projectId,
            projectCwd: g.f.bPath,
            worktreePath: workspace.worktreePath,
          })
          .pipe(Effect.asVoid);
  return { command, origin, workspace, release, materialize, settleSetup };
});

it.effect.each(["new", "reused"] as const)(
  "actual %s ThreadLaunch schedules materialization and forwards opaque context through ThreadManagement",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* g.sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
      VALUES('launch-project','Synthetic',${g.f.bPath},'[]',${now},${now})`;
        const threadId = ThreadId.make("actual-launch");
        if (mode === "reused")
          yield* g.launch.accept(
            {
              type: "thread.create",
              commandId: CommandId.make("empty-reused-thread"),
              threadId,
              projectId: ProjectId.make("launch-project"),
              title: "Empty",
              branch: null,
              worktreePath: null,
              modelSelection: { instanceId: g.instanceId, model: "synthetic" },
              runtimeMode: "full-access",
              interactionMode: "default",
              createdBy: "user",
              creationSource: "web",
            },
            [g.f.bPath],
            g.threads.dispatch,
          );
        const afterSequence = yield* g.sink.latestSequence();
        const request = {
          commandId: CommandId.make("actual-launch-original"),
          threadId,
          ...(mode === "reused" ? { reuseExistingThread: true } : {}),
          projectId: ProjectId.make("launch-project"),
          title: "Actual service",
          modelSelection: { instanceId: g.instanceId, model: "synthetic" },
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "work" },
          initialMessage: {
            messageId: MessageId.make("actual-launch-message"),
            text: "Prepare work",
            attachments: [],
          },
          createdBy: "user" as const,
          creationSource: "web" as const,
        };
        yield* g.threadLaunch.launch(request);
        const terminal = yield* g.threads
          .streamStoredEventsFrom({ threadId, afterSequence, eventType: "run.updated" })
          .pipe(
            Stream.filter(
              ({ event }) => event.type === "run.updated" && event.payload.status !== "preparing",
            ),
            Stream.runHead,
          );
        assert.isTrue(terminal._tag === "Some");
        const originalCommandId =
          mode === "new"
            ? request.commandId
            : CommandId.make(`${request.commandId}:initial-message`);
        const origin = (yield* g.checkout.readPreparationOrigins(originalCommandId))[0]!;
        const release = yield* g.checkout.readPreparationRelease(origin.originId);
        assert.isNotNull(release);
        assert.equal(release!.commandId, `${request.commandId}:release`);
        assert.equal(release!.target.worktreePath, g.worktreePath);
        assert.equal(g.observe().creates, 1);
        const before = g.observe();
        assert.isTrue((yield* g.threadLaunch.launch(request)).resumed);
        assert.deepEqual(g.observe(), before);
      }),
    ),
);

it.effect.each(["root", "existing_worktree", "worktree"] as const)(
  "actual origin → shared entry → %s result → metadata → release retains both command identities",
  (strategy) =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const s = yield* seed(g, strategy);
        assert.lengthOf(yield* g.sql`SELECT * FROM jones_ordinary_checkout_admissions`, 0);
        yield* g.launch.withRun(
          s.command.threadId,
          s.release.runId,
          g.threads.dispatch,
          (operation) =>
            Effect.gen(function* () {
              yield* s.materialize(operation);
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
              assert.isTrue(
                Exit.isFailure(
                  yield* g.launch
                    .release(operation, s.release, g.threads.dispatch)
                    .pipe(Effect.exit),
                ),
              );
              if (strategy !== "root")
                assert.isTrue(Exit.isFailure(yield* g.launch.ready(operation).pipe(Effect.exit)));
              yield* s.settleSetup(operation);
              yield* g.launch.ready(operation);
              yield* g.launch.release(operation, s.release, g.threads.dispatch);
              const before = g.observe();
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
              yield* g.launch.release(operation, s.release, g.threads.dispatch);
              assert.deepEqual(g.observe(), before);
            }),
        );
        const released = yield* g.checkout.readPreparationRelease(s.origin.originId);
        assert.isNotNull(released);
        assert.equal(released!.commandId, s.release.commandId);
        const admissions = yield* g.sql<{
          command_id: string;
        }>`SELECT command_id FROM jones_ordinary_checkout_admissions`;
        assert.deepEqual(
          admissions.map((row) => row.command_id),
          [s.command.commandId],
        );
        const links =
          yield* g.sql`SELECT * FROM jones_ordinary_checkout_effect_links WHERE effect_id=${released!.effectId}`;
        assert.lengthOf(links, 1);
        const before = g.observe();
        yield* g.launch.accept(s.command, [g.f.bPath], g.threads.dispatch);
        yield* g.launch.withRun(s.command.threadId, s.release.runId, g.threads.dispatch, () =>
          Effect.die("replayed preparation"),
        );
        assert.deepEqual(g.observe(), before);
      }),
    ),
);

it.effect(
  "rename obtains a separate fiber-bound preparation handle; release uses the final binding",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const s = yield* seed(g);
        yield* g.launch.withRun(
          s.command.threadId,
          s.release.runId,
          g.threads.dispatch,
          (operation) =>
            Effect.gen(function* () {
              yield* s.materialize(operation);
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
              const escaped = yield* g.launch.ready(operation).pipe(Effect.forkChild);
              assert.isTrue(Exit.isFailure(yield* Fiber.await(escaped)));
              const rename = yield* g.launch
                .rename(
                  operation,
                  { cwd: g.worktreePath, oldBranch: "work", newBranch: "final" },
                  (branch) => ({
                    ...s.workspace,
                    commandId: CommandId.make("launch-rename"),
                    branch,
                    expectedBranch: "work",
                    expectedWorktreePath: g.worktreePath,
                  }),
                  g.threads.dispatch,
                )
                .pipe(Effect.forkChild);
              yield* Fiber.join(rename);
              assert.equal(
                (yield* g.checkout.readPreparationBinding(s.origin.originId))?.revision,
                2,
              );
              yield* s.settleSetup(operation);
              yield* g.launch.ready(operation);
              yield* g.launch.release(operation, s.release, g.threads.dispatch);
            }),
        );
        assert.equal(
          (yield* g.checkout.readPreparationRelease(s.origin.originId))?.target.threadBranch,
          "final",
        );
        assert.equal(g.observe().renames, 1);
      }),
    ),
);

it.effect.each(["git-failure", "wrong-result", "metadata-rollback"] as const)(
  "%s keeps the entered fence and refuses blind recovery or a release",
  (failure) =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture({
          failCreate: failure === "git-failure",
          wrongPath: failure === "wrong-result",
        });
        const s = yield* seed(g);
        if (failure === "metadata-rollback")
          yield* g.sql`CREATE TRIGGER fail_launch_binding BEFORE INSERT ON jones_ordinary_checkout_preparing_bindings BEGIN SELECT RAISE(ABORT, 'synthetic binding failure'); END`;
        const attempted = yield* g.launch
          .withRun(s.command.threadId, s.release.runId, g.threads.dispatch, (operation) =>
            Effect.gen(function* () {
              yield* s.materialize(operation);
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
            }),
          )
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(attempted));
        assert.isNull(yield* g.checkout.readPreparationBinding(s.origin.originId));
        assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
        const before = g.observe();
        assert.isTrue(
          Exit.isFailure(
            yield* g.launch
              .withRun(s.command.threadId, s.release.runId, g.threads.dispatch, () =>
                Effect.die("must reconcile first"),
              )
              .pipe(Effect.exit),
          ),
        );
        assert.equal(g.observe().creates, before.creates);
        const rows = yield* g.f.b.common<{
          phase: string;
        }>`SELECT phase FROM jones_common_reservations WHERE phase='unknown'`;
        assert.lengthOf(rows, 1);
        assert.lengthOf(
          yield* g.sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${s.workspace.commandId}`,
          0,
        );
      }),
    ),
);

it.effect(
  "cancellation after materialization revokes the handle, retains material and never enters a second generation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const s = yield* seed(g);
        const entered = yield* Deferred.make<void>();
        let retained: LaunchOperation | undefined;
        const worker = yield* g.launch
          .withRun(s.command.threadId, s.release.runId, g.threads.dispatch, (operation) =>
            Effect.gen(function* () {
              retained = operation;
              yield* s.materialize(operation);
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
              yield* Deferred.succeed(entered, undefined);
              return yield* Effect.never;
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(worker);
        assert.isTrue(Exit.isFailure(yield* g.launch.ready(retained!).pipe(Effect.exit)));
        assert.isNotNull(yield* g.checkout.readPreparationBinding(s.origin.originId));
        assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
        assert.equal(g.observe().creates, 1);
        assert.isTrue(
          Exit.isFailure(
            yield* g.launch
              .withRun(s.command.threadId, s.release.runId, g.threads.dispatch, () =>
                Effect.die("must not repeat setup"),
              )
              .pipe(Effect.exit),
          ),
        );
      }),
    ),
);

it.effect(
  "structural handles and metadata without a completed materialization cannot enter Git or bind a target",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const s = yield* seed(g);
        assert.isTrue(
          Exit.isFailure(
            yield* g.launch
              .createWorktree({} as LaunchOperation, {
                cwd: g.f.bPath,
                refName: "main",
                newRefName: "work",
                path: null,
              })
              .pipe(Effect.exit),
          ),
        );
        assert.equal(g.observe().creates, 0);
        yield* g.launch.withRun(
          s.command.threadId,
          s.release.runId,
          g.threads.dispatch,
          (operation) =>
            Effect.gen(function* () {
              assert.isTrue(
                Exit.isFailure(
                  yield* g.launch
                    .workspace(operation, s.workspace, g.threads.dispatch)
                    .pipe(Effect.exit),
                ),
              );
            }),
        );
        assert.isNull(yield* g.checkout.readPreparationBinding(s.origin.originId));
        assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
      }),
    ),
);

it.effect("actual setup write acknowledgement and blocking completion both precede release", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const spawned = yield* Deferred.make<void>();
      const write = yield* Deferred.make<void>();
      const awaitingCompletion = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<{
        readonly exitCode: number;
        readonly durationMs: number;
      }>();
      const g = yield* makeLaunchFixture({
        setup: (input) =>
          Effect.succeed({
            status: "started",
            scriptId: "synthetic-setup",
            scriptName: "setup",
            scriptCommand: "synthetic",
            terminalId: "synthetic-terminal",
            cwd: input.worktreePath,
            async: false,
            writeCompletion: Deferred.succeed(spawned, undefined).pipe(
              Effect.andThen(Deferred.await(write)),
            ),
            completion: Deferred.succeed(awaitingCompletion, undefined).pipe(
              Effect.andThen(Deferred.await(completed)),
            ),
          }),
      });
      const s = yield* seed(g);
      const worker = yield* g.launch
        .withRun(s.command.threadId, s.release.runId, g.threads.dispatch, (operation) =>
          Effect.gen(function* () {
            yield* s.materialize(operation);
            yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
            const setup = yield* g.launch.runSetup(operation, {
              threadId: s.command.threadId,
              worktreePath: g.worktreePath,
            });
            assert.isTrue(Exit.isFailure(yield* g.launch.ready(operation).pipe(Effect.exit)));
            if (setup.status !== "started" || setup.completion === undefined)
              return yield* Effect.die("missing setup completion");
            yield* setup.completion;
            yield* g.launch.ready(operation);
            yield* g.launch.release(operation, s.release, g.threads.dispatch);
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(spawned);
      assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
      yield* Deferred.succeed(write, undefined);
      yield* Deferred.await(awaitingCompletion);
      assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
      yield* Deferred.succeed(completed, { exitCode: 0, durationMs: 1 });
      yield* Fiber.join(worker);
      assert.isNotNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
    }),
  ),
);

it.effect(
  "rename metadata race preserves the prior binding and blocks release after the physical branch changed",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* makeLaunchFixture();
        const s = yield* seed(g);
        yield* g.launch.withRun(
          s.command.threadId,
          s.release.runId,
          g.threads.dispatch,
          (operation) =>
            Effect.gen(function* () {
              yield* s.materialize(operation);
              yield* g.launch.workspace(operation, s.workspace, g.threads.dispatch);
              const before = yield* g.checkout.readPreparationBinding(s.origin.originId);
              assert.isTrue(
                Exit.isFailure(
                  yield* g.launch
                    .rename(
                      operation,
                      { cwd: g.worktreePath, oldBranch: "work", newBranch: "raced" },
                      (branch) => ({
                        ...s.workspace,
                        commandId: CommandId.make("raced-rename"),
                        branch,
                        expectedBranch: "different-predecessor",
                        expectedWorktreePath: g.worktreePath,
                      }),
                      g.threads.dispatch,
                    )
                    .pipe(Effect.exit),
                ),
              );
              assert.deepEqual(yield* g.checkout.readPreparationBinding(s.origin.originId), before);
              yield* s.settleSetup(operation);
              yield* g.launch.ready(operation);
              assert.isTrue(
                Exit.isFailure(
                  yield* g.launch
                    .release(operation, s.release, g.threads.dispatch)
                    .pipe(Effect.exit),
                ),
              );
              // The reverse ordering also refuses an unscoped target update after the
              // certified binding, rather than borrowing the parent's preparation.
              assert.isTrue(
                Exit.isFailure(
                  yield* g.threads
                    .dispatch({
                      ...s.workspace,
                      commandId: CommandId.make("unscoped-after-binding"),
                      branch: "raced",
                    })
                    .pipe(Effect.exit),
                ),
              );
            }),
        );
        assert.isNull(yield* g.checkout.readPreparationRelease(s.origin.originId));
        assert.equal(g.observe().renames, 1);
      }),
    ),
);
