import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProviderRuntimeBinding } from "@t3tools/contracts";
import { AgentScope, AgentScopeStopError } from "@t3tools/shared/AgentScope";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { captureCodexProducerLifetime } from "../provider/codex/CodexCapturedLifetime.ts";
import { verifyCodexProcessExit } from "../orchestration/LateSteer.ts";
import {
  claimScopedCommand,
  observeScopedCommandRetirement,
  retainScopedCommand,
} from "../processPlacement/scopedCommand.ts";
import migration104 from "../persistence/Migrations/104_JonesDeletionAdmission.ts";
import { makeRuntimeUseLifetime } from "./RuntimeUseLifetime.ts";
import { digest, type RuntimeUseBinding } from "./RuntimeUseProtocol.ts";
import { createRuntimeUseSchema } from "./RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";

const binding: RuntimeUseBinding = {
  schema: "jones.workspace-runtime-use/v1",
  useId: "original-open",
  canonicalPath: "/synthetic/worktree",
  threadId: "thread",
  directoryIdentity: { version: 1, device: "device", inode: "inode", birth: "birth" },
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
};
const identity = (generation: string) => ({
  driver: "codex" as const,
  instanceId: "codex",
  providerSessionId: "session",
  threadId: "thread",
  generation,
});
const decodeBinding = Schema.decodeUnknownEffect(ProviderRuntimeBinding);
const fixture = <A, E, R>(effect: Effect.Effect<A, E, R | SqlClient.SqlClient | Scope.Scope>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration104;
      yield* sql.withTransaction(createRuntimeUseSchema);
      return yield* effect;
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" }), NodeServices.layer),
    ),
  );
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

// A real, single-process local fixture. It never starts a provider or creates
// descendants; its explicit writer-scope stop waits for the captured handle.
const worker = Effect.fn("RuntimeUseLifetime.testWorker")(function* (generation: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const owner = AgentScope.defaultValue();
  const command = ChildProcess.make(process.execPath, [
    "-e",
    "process.stdin.once('data',()=>process.exit(0));process.stdin.resume();",
  ]);
  const handle = yield* spawner.spawn(command);
  const stop = Effect.gen(function* () {
    if (yield* handle.isRunning)
      yield* Stream.run(Stream.encodeText(Stream.make("finish\n")), handle.stdin);
    yield* verifyCodexProcessExit(handle);
  }).pipe(
    Effect.mapError(
      (cause) =>
        new AgentScopeStopError({ unit: `synthetic-${generation}.scope`, detail: String(cause) }),
    ),
  );
  const retained = retainScopedCommand(command, owner, {
    command: process.execPath,
    args: [],
    unit: `synthetic-${generation}.scope`,
    stop,
  });
  assert.isDefined(claimScopedCommand(retained, owner));
  const receipt = yield* captureCodexProducerLifetime({
    identity: identity(generation),
    command: retained,
    owner,
    handle,
  });
  assert.isDefined(receipt);
  const retire = observeScopedCommandRetirement(retained, owner, stop);
  yield* Effect.addFinalizer(() => retire.pipe(Effect.ignoreCause));
  return { receipt, retire, handle, finishProcess: stop };
});
const lifetime = Effect.gen(function* () {
  const store = yield* makeRuntimeUseStore;
  yield* store.reserve(binding);
  const observer = yield* makeRuntimeUseLifetime({
    binding,
    observationScope: yield* Scope.Scope,
    prepareProducer: () => Effect.succeed({ validate: Effect.void, revalidate: Effect.void }),
  });
  return { store, observer };
});

describe("original runtime-use physical retirement", () => {
  it.effect(
    "a completed operation's inherited publication capability cannot be reused by its delayed child",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const ticket = yield* observer.beginProducer(identity("generation-1"));
          const original = yield* worker("generation-1");
          const observation = yield* ticket.capture(original.receipt);
          const release = yield* Deferred.make<void>();
          const native = yield* decodeBinding({
            threadId: "thread",
            providerThreadId: "revoked-publication",
            providerSessionId: "session",
            providerInstanceId: "codex",
            driver: "codex",
            runtimeGeneration: "generation-1",
            nativeThreadId: "revoked-native-thread",
          });
          const delayed = yield* observer.withOperation(
            Deferred.await(release).pipe(
              Effect.andThen(observer.observeBinding(native)),
              Effect.forkScoped,
            ),
          );
          yield* observer.seal;
          yield* original.retire;
          yield* Fiber.join(observation!);
          const completed = yield* store.read(binding.useId);
          assert.strictEqual(completed?.state, "retired");
          yield* Deferred.succeed(release, undefined);
          const outcome = yield* Effect.result(Fiber.join(delayed));
          if (Result.isSuccess(outcome))
            return yield* Effect.die("Revoked publication unexpectedly succeeded");
          assert.strictEqual(outcome.failure.reason, "runtime_use_uncovered_native_binding");
          assert.deepStrictEqual(yield* store.read(binding.useId), completed);
        }),
      ),
  );
  it.effect(
    "an unrelated active operation cannot borrow another lifetime's pending publication slot",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const ticket = yield* observer.beginProducer(identity("generation-1"));
          const original = yield* worker("generation-1");
          const observation = yield* ticket.capture(original.receipt);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const operation = yield* observer
            .withOperation(
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            )
            .pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* observer.seal;
          yield* original.retire;
          yield* Fiber.join(observation!);
          const otherBinding = { ...binding, useId: "unrelated-operation-use" };
          yield* store.reserve(otherBinding);
          const other = yield* makeRuntimeUseLifetime({
            binding: otherBinding,
            observationScope: yield* Scope.Scope,
            prepareProducer: () =>
              Effect.succeed({ validate: Effect.void, revalidate: Effect.void }),
          });
          const native = yield* decodeBinding({
            threadId: "thread",
            providerThreadId: "unrelated-publication",
            providerSessionId: "session",
            providerInstanceId: "codex",
            driver: "codex",
            runtimeGeneration: "generation-1",
            nativeThreadId: "unrelated-native-thread",
          });
          const outcome = yield* Effect.result(
            other.withOperation(observer.observeBinding(native)),
          );
          if (Result.isSuccess(outcome))
            return yield* Effect.die("Another lifetime authorized publication");
          assert.strictEqual(outcome.failure.reason, "runtime_use_uncovered_native_binding");
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(operation);
          const retained = yield* store.read(binding.useId);
          assert.strictEqual(retained?.state, "pending");
          assert.deepStrictEqual(retained?.nativeBindings, []);
        }),
      ),
  );
  it.effect(
    "an additional workspace owns independent observations and awaits every original and replacement producer",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const first = yield* observer.beginProducer(identity("generation-1"));
          const a = yield* worker("generation-1");
          const aObservation = yield* first.capture(a.receipt);
          const nativeA = yield* decodeBinding({
            threadId: "thread",
            providerThreadId: "provider-thread-1",
            providerSessionId: "session",
            providerInstanceId: "codex",
            driver: "codex",
            runtimeGeneration: "generation-1",
            nativeThreadId: "native-thread-1",
          });
          yield* observer.observeBinding(nativeA);
          const additional: RuntimeUseBinding = {
            ...binding,
            useId: "additional-workspace-use",
            canonicalPath: "/synthetic/other-worktree",
            threadId: "other-thread",
            directoryIdentity: {
              version: 1,
              device: "device",
              inode: "other-inode",
              birth: "other-birth",
            },
            actor: {
              kind: "provider_invoke",
              provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
            },
          };
          yield* store.reserve(additional);
          yield* observer.adoptUse(additional);
          const adopted = yield* store.read(additional.useId);
          assert.strictEqual(adopted?.producers.length, 1);
          assert.strictEqual(adopted?.nativeBindings.length, 1);
          yield* observer.adoptUse(additional);
          assert.deepStrictEqual(yield* store.read(additional.useId), adopted);
          const second = yield* observer.beginProducer(identity("generation-2"));
          const b = yield* worker("generation-2");
          const bObservation = yield* second.capture(b.receipt);
          const nativeB = yield* decodeBinding({
            threadId: "other-thread",
            providerThreadId: "provider-thread-2",
            providerSessionId: "session",
            providerInstanceId: "codex",
            driver: "codex",
            runtimeGeneration: "generation-2",
            nativeThreadId: "native-thread-2",
          });
          yield* observer.observeBinding(nativeB);
          yield* observer.seal;
          yield* a.retire;
          yield* Fiber.join(aObservation!);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          assert.strictEqual((yield* store.read(additional.useId))?.state, "pending");
          assert.isTrue(yield* b.handle.isRunning);
          yield* b.retire;
          yield* Fiber.join(bObservation!);
          for (const original of [binding, additional]) {
            const record = yield* store.read(original.useId);
            assert.strictEqual(record?.state, "retired");
            assert.deepStrictEqual(record?.binding, original);
            assert.strictEqual(record?.producers.length, 2);
            assert.deepStrictEqual(
              record?.nativeBindings.map((item) => item.binding),
              [nativeA, nativeB],
            );
            assert.isTrue(record!.producers.every((item) => item.useId === original.useId));
          }
          const completed = [yield* store.read(binding.useId), yield* store.read(additional.useId)];
          yield* observer.seal;
          assert.deepStrictEqual(
            [yield* store.read(binding.useId), yield* store.read(additional.useId)],
            completed,
          );
        }),
      ),
  );
  it.effect(
    "another session and an already retired additional record cannot join the captured writer authority",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const foreign: RuntimeUseBinding = {
            ...binding,
            useId: "foreign-workspace-use",
            canonicalPath: "/synthetic/foreign",
            actor: {
              kind: "provider_invoke",
              provider: { sessionId: "foreign-session", instanceId: "codex", driver: "codex" },
            },
          };
          yield* store.reserve(foreign);
          yield* refused(observer.adoptUse(foreign));
          const retired: RuntimeUseBinding = {
            ...binding,
            useId: "retired-workspace-use",
            canonicalPath: "/synthetic/retired",
            actor: {
              kind: "provider_invoke",
              provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
            },
          };
          yield* store.reserve(retired);
          yield* store.recordRetirement({
            schema: "jones.workspace-runtime-retirement/v1",
            useId: retired.useId,
            bindingSha256: digest(retired),
            outcome: "not_invoked",
            observationCount: 0,
          });
          yield* refused(observer.adoptUse(retired));
          assert.strictEqual((yield* store.read(foreign.useId))?.state, "pending");
          assert.strictEqual(
            (yield* store.read(retired.useId))?.retirement?.outcome,
            "not_invoked",
          );
        }),
      ),
  );
  it.effect(
    "captured exit cannot retire while an admitted operation still owns its binding publication",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const ticket = yield* observer.beginProducer(identity("generation-1"));
          const original = yield* worker("generation-1");
          const observation = yield* ticket.capture(original.receipt);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const native = yield* decodeBinding({
            threadId: "thread",
            providerThreadId: "late-published-thread",
            providerSessionId: "session",
            providerInstanceId: "codex",
            driver: "codex",
            runtimeGeneration: "generation-1",
            nativeThreadId: "late-native-thread",
          });
          const operation = yield* observer
            .withOperation(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(observer.observeBinding(native)),
              ),
            )
            .pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* observer.seal;
          yield* original.retire;
          yield* Fiber.join(observation!);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          let laterInvocations = 0;
          yield* refused(
            observer.withOperation(
              Effect.sync(() => {
                laterInvocations++;
              }),
            ),
          );
          assert.strictEqual(laterInvocations, 0);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(operation);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "retired");
          assert.deepStrictEqual(
            (yield* store.read(binding.useId))?.nativeBindings.map((item) => item.binding),
            [native],
          );
        }),
      ),
  );
  it.effect("generic close and sealing cannot retire without the captured producer proof", () =>
    fixture(
      Effect.gen(function* () {
        const { store, observer } = yield* lifetime;
        yield* observer.beginProducer(identity("generation-1"));
        const empty = yield* Scope.make();
        yield* Scope.close(empty, Exit.void);
        yield* observer.seal;
        assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
        yield* refused(observer.beginProducer(identity("generation-2")));
      }),
    ),
  );
  it.effect(
    "actual original process and complete writer-scope exit retire only after admission seals",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const ticket = yield* observer.beginProducer(identity("generation-1"));
          yield* ticket.beforeSpawn;
          const original = yield* worker("generation-1");
          const observation = yield* ticket.capture(original.receipt);
          assert.isDefined(observation);
          yield* original.retire;
          yield* Fiber.join(observation!);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          yield* observer.seal;
          const completed = yield* store.read(binding.useId);
          assert.strictEqual(completed?.state, "retired");
          assert.strictEqual(completed?.retirement?.outcome, "producers_exited");
          assert.deepStrictEqual(
            (yield* store.pending()).map((row) => row.useId),
            [],
          );
          yield* observer.seal;
          assert.deepStrictEqual(yield* store.read(binding.useId), completed);
        }),
      ),
  );
  it.effect("one exited producer cannot cover another captured generation", () =>
    fixture(
      Effect.gen(function* () {
        const { store, observer } = yield* lifetime;
        const first = yield* observer.beginProducer(identity("generation-1"));
        const second = yield* observer.beginProducer(identity("generation-2"));
        const a = yield* worker("generation-1");
        const b = yield* worker("generation-2");
        const aObservation = yield* first.capture(a.receipt);
        const bObservation = yield* second.capture(b.receipt);
        yield* observer.seal;
        yield* a.retire;
        yield* Fiber.join(aObservation!);
        assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
        assert.isTrue(yield* b.handle.isRunning);
        yield* b.retire;
        yield* Fiber.join(bObservation!);
        assert.strictEqual((yield* store.read(binding.useId))?.state, "retired");
        assert.strictEqual((yield* store.read(binding.useId))?.producers.length, 2);
      }),
    ),
  );
  it.effect(
    "captured native exit alone remains pending until the original writer-scope readback completes",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          const ticket = yield* observer.beginProducer(identity("generation-1"));
          const original = yield* worker("generation-1");
          const observation = yield* ticket.capture(original.receipt);
          yield* observer.seal;
          yield* original.finishProcess;
          assert.isFalse(yield* original.handle.isRunning);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          yield* original.retire;
          yield* Fiber.join(observation!);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "retired");
        }),
      ),
  );
  it.effect(
    "failed open without captured lifetime cannot disappear behind a later complete producer",
    () =>
      fixture(
        Effect.gen(function* () {
          const { store, observer } = yield* lifetime;
          yield* observer.beginProducer(identity("unknown-opening"));
          const complete = yield* observer.beginProducer(identity("generation-2"));
          const original = yield* worker("generation-2");
          const observation = yield* complete.capture(original.receipt);
          yield* observer.seal;
          yield* original.retire;
          yield* Fiber.join(observation!);
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          assert.strictEqual((yield* store.read(binding.useId))?.producers.length, 2);
        }),
      ),
  );
  it.effect("copied and wrong-generation lifetime receipts remain unknown after real exit", () =>
    fixture(
      Effect.gen(function* () {
        const { store, observer } = yield* lifetime;
        const first = yield* observer.beginProducer(identity("generation-1"));
        const second = yield* observer.beginProducer(identity("generation-2"));
        const a = yield* worker("generation-1");
        const wrong = yield* second.capture(a.receipt);
        const copied = yield* first.capture({ ...a.receipt });
        yield* observer.seal;
        yield* a.retire;
        yield* Fiber.join(wrong!);
        yield* Fiber.join(copied!);
        assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
      }),
    ),
  );
  it.effect("complete retirement binds the exact admitted native writer inventory", () =>
    fixture(
      Effect.gen(function* () {
        const { store, observer } = yield* lifetime;
        const ticket = yield* observer.beginProducer(identity("generation-1"));
        const original = yield* worker("generation-1");
        const observation = yield* ticket.capture(original.receipt);
        const native = yield* decodeBinding({
          threadId: "thread",
          providerThreadId: "provider-thread",
          providerSessionId: "session",
          providerInstanceId: "codex",
          driver: "codex",
          runtimeGeneration: "generation-1",
          nativeThreadId: "native-thread",
        });
        yield* observer.observeBinding(native);
        yield* observer.observeBinding(native);
        assert.strictEqual((yield* store.read(binding.useId))?.nativeBindings.length, 1);
        yield* observer.seal;
        yield* original.retire;
        yield* Fiber.join(observation!);
        const completed = yield* store.read(binding.useId);
        assert.strictEqual(completed?.state, "retired");
        assert.deepStrictEqual(
          completed?.nativeBindings.map((item) => item.binding),
          [native],
        );
      }),
    ),
  );
});
