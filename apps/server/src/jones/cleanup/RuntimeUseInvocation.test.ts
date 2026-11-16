// @effect-diagnostics nodeBuiltinImport:off -- Isolated native owner and SQLite fixtures, with no provider or installed state.
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Ownership from "../../serverOwnership.ts";
import * as ProcessRunner from "../../processRunner.ts";
import {
  bindCheckedDatabaseOwner,
  invalidateCheckedServerOwnership,
  type CheckedDatabaseOwnerToken,
} from "./CheckedRuntimeOwner.ts";
import { prepareRuntimeUseInvocation } from "./RuntimeUseInvocation.ts";
import { RuntimeUseError, type RuntimeUseBinding } from "./RuntimeUseProtocol.ts";
import { createRuntimeUseSchema } from "./RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import {
  captureRuntimeUseWorkspace,
  assertRuntimeUseDefaultWorkspace,
} from "./RuntimeUseWorkspace.ts";
import deletionMigration from "../persistence/Migrations/104_JonesDeletionAdmission.ts";

const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const fixture = <A, E>(
  use: (input: {
    readonly owner: CheckedDatabaseOwnerToken;
    readonly revalidateWorkspace: Effect.Effect<void, RuntimeUseError>;
    readonly binding: RuntimeUseBinding;
    readonly loseOwner: Effect.Effect<void>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "runtime-use-" }),
      );
      const stateDir = NodePath.join(root, "userdata");
      yield* fs.makeDirectory(stateDir);
      const workspacePath = NodePath.join(root, "worktree");
      yield* fs.makeDirectory(workspacePath);
      const dbPath = NodePath.join(stateDir, "statev2.sqlite");
      const serverRuntimeStatePath = NodePath.join(stateDir, "server-runtime.json");
      return yield* Effect.gen(function* () {
        const ownership = yield* Ownership.acquireServerOwnership(serverRuntimeStatePath);
        return yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* deletionMigration;
          yield* sql.withTransaction(createRuntimeUseSchema);
          const owner = yield* bindCheckedDatabaseOwner(ownership.heldRuntimeOwner, {
            stateDir,
            dbPath,
            serverRuntimeStatePath,
          });
          const workspace = yield* captureRuntimeUseWorkspace(workspacePath);
          return yield* use({
            owner,
            revalidateWorkspace: workspace.revalidate,
            loseOwner: Effect.sync(() =>
              invalidateCheckedServerOwnership(ownership.heldRuntimeOwner),
            ),
            binding: {
              schema: "jones.workspace-runtime-use/v1",
              useId: "original",
              canonicalPath: workspace.canonicalPath,
              directoryIdentity: workspace.directoryIdentity,
              threadId: "thread",
              actor: {
                kind: "provider_open",
                provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
              },
              original: {
                commandId: null,
                runId: null,
                attemptId: null,
                outboxReference: null,
                nativeReference: null,
                claim: null,
              },
            },
          });
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, root),
        Effect.provideService(HostProcess.Environment, {}),
      );
    }).pipe(Effect.provide(platform)),
  );
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

describe("live runtime use invocation owner", () => {
  it.effect(
    "native default cwd is accepted only when it names the intended physical server workspace",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const actualDefault = yield* captureRuntimeUseWorkspace(process.cwd());
          yield* assertRuntimeUseDefaultWorkspace(actualDefault);
          const intended = yield* captureRuntimeUseWorkspace(input.binding.canonicalPath);
          yield* refused(assertRuntimeUseDefaultWorkspace(intended));
        }),
      ),
  );
  it.effect(
    "canonical directory replacement before entry refuses the effect without rebinding its reservation",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate: Effect.void });
          yield* Effect.promise(() =>
            NodeFSP.rename(input.binding.canonicalPath, input.binding.canonicalPath + "-old"),
          );
          yield* Effect.promise(() => NodeFSP.mkdir(input.binding.canonicalPath));
          let calls = 0;
          yield* refused(
            use.invoke(
              Effect.sync(() => {
                calls++;
              }),
            ),
          );
          assert.strictEqual(calls, 0);
          const record = yield* (yield* makeRuntimeUseStore).read(input.binding.useId);
          assert.deepStrictEqual(record?.binding, input.binding);
          assert.strictEqual(record?.retirement?.outcome, "not_invoked");
        }),
      ),
  );
  it.effect("an alias or noncanonical spelling cannot supply a workspace observation", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const alias = input.binding.canonicalPath + "-alias";
        yield* Effect.promise(() => NodeFSP.symlink(input.binding.canonicalPath, alias));
        yield* refused(captureRuntimeUseWorkspace(alias));
        yield* refused(captureRuntimeUseWorkspace(input.binding.canonicalPath + "/../worktree"));
        yield* refused(captureRuntimeUseWorkspace("relative"));
        assert.deepStrictEqual(yield* (yield* makeRuntimeUseStore).pending(), []);
      }),
    ),
  );
  it.effect(
    "commits pending before entry, invokes once, and leaves successful open unresolved",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate: Effect.void });
          let calls = 0;
          yield* use.invoke(
            Effect.gen(function* () {
              assert.strictEqual((yield* store.read(input.binding.useId))?.state, "pending");
              calls++;
            }),
          );
          yield* refused(
            use.invoke(
              Effect.sync(() => {
                calls++;
              }),
            ),
          );
          assert.strictEqual(calls, 1);
          assert.isFalse(yield* use.revokeBeforeEntry);
          assert.strictEqual((yield* store.read(input.binding.useId))?.state, "pending");
          yield* refused(prepareRuntimeUseInvocation({ ...input, validate: Effect.void }));
        }),
      ),
  );
  it.effect("transaction validation failure admits neither reservation nor external call", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const store = yield* makeRuntimeUseStore;
        yield* refused(
          prepareRuntimeUseInvocation({
            ...input,
            validate: Effect.fail(new RuntimeUseError({ reason: "changed_binding" })),
          }),
        );
        assert.strictEqual(yield* store.read(input.binding.useId), null);
      }),
    ),
  );
  it.effect(
    "explicit pre-entry cancellation synchronizes with validation and cannot enter the effect",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const checking = yield* Deferred.make<void>();
          const continueCheck = yield* Deferred.make<void>();
          let validations = 0;
          const validate = Effect.suspend(() =>
            ++validations === 1
              ? Effect.void
              : Deferred.succeed(checking, undefined).pipe(
                  Effect.andThen(Deferred.await(continueCheck)),
                ),
          );
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate });
          let calls = 0;
          const fiber = yield* use
            .invoke(
              Effect.sync(() => {
                calls++;
              }),
            )
            .pipe(Effect.forkChild);
          yield* Deferred.await(checking);
          // Revocation synchronously fences entry, then persists on the same SQL
          // connection. Let the held validation transaction finish before joining.
          const cancellation = yield* use.revokeBeforeEntry.pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          yield* Deferred.succeed(continueCheck, undefined);
          assert.isTrue(yield* Fiber.join(cancellation));
          yield* refused(Fiber.join(fiber));
          assert.strictEqual(calls, 0);
          assert.strictEqual(
            (yield* (yield* makeRuntimeUseStore).read(input.binding.useId))?.retirement?.outcome,
            "not_invoked",
          );
        }),
      ),
  );
  it.effect(
    "interruption after entry preserves pending and cannot be converted into not-invoked",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate: Effect.void });
          const entered = yield* Deferred.make<void>();
          const fiber = yield* use
            .invoke(Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)))
            .pipe(Effect.forkChild);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(fiber);
          assert.isFalse(yield* use.revokeBeforeEntry);
          assert.strictEqual(
            (yield* (yield* makeRuntimeUseStore).read(input.binding.useId))?.state,
            "pending",
          );
        }),
      ),
  );
  it.effect(
    "a failed entered effect remains pending despite successful surrounding scope finalizers",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate: Effect.void });
          let finalized = false;
          yield* refused(
            use.invoke(
              Effect.scoped(
                Effect.gen(function* () {
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalized = true;
                    }),
                  );
                  return yield* Effect.fail("possibly spawned before failure");
                }),
              ),
            ),
          );
          assert.isTrue(finalized);
          assert.strictEqual(
            (yield* (yield* makeRuntimeUseStore).read(input.binding.useId))?.state,
            "pending",
          );
        }),
      ),
  );
  it.effect(
    "revoked actual server owner refuses entry and records only the original not-invoked outcome",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const use = yield* prepareRuntimeUseInvocation({ ...input, validate: Effect.void });
          yield* input.loseOwner;
          let calls = 0;
          yield* refused(
            use.invoke(
              Effect.sync(() => {
                calls++;
              }),
            ),
          );
          assert.strictEqual(calls, 0);
          assert.strictEqual(
            (yield* (yield* makeRuntimeUseStore).read(input.binding.useId))?.retirement?.outcome,
            "not_invoked",
          );
        }),
      ),
  );
});
