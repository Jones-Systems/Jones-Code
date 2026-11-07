import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CodexSettings, ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import type * as CodexErrors from "effect-codex-app-server/errors";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope, AgentScopeStopError, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { verifyCodexProcessExit } from "../orchestration/LateSteer.ts";
import * as ProcessAttribution from "../resourceTelemetry/ProcessAttribution.ts";
import * as CodexAdapter from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CodexLifetime from "../provider/codex/CodexCapturedLifetime.ts";
import * as PlacementSpawner from "./processPlacementSpawner.ts";
import {
  awaitCapturedScopedRetirement,
  captureScopedCommandRetirement,
  claimScopedCommand,
  observeScopedCommandRetirement,
  retainScopedCommand,
} from "./scopedCommand.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeCodexSettings = Schema.decodeEffect(CodexSettings);
const retained = (
  owner: AgentScopeShape,
  stop: Effect.Effect<void, AgentScopeStopError>,
  unit = "synthetic-original.scope",
) => {
  const launch = { command: "/synthetic/original", args: [], unit, stop };
  const command = retainScopedCommand(ChildProcess.make(launch.command), owner, launch);
  const receipt = captureScopedCommandRetirement(command, owner);
  assert.isDefined(receipt);
  assert.strictEqual(claimScopedCommand(command, owner), launch);
  return { command, launch, receipt };
};

describe("original scoped launch retirement receipt", () => {
  it.effect(
    "the actual Codex factory captures both original native handle and writer-scope receipts",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const underlying = yield* ChildProcessSpawner.ChildProcessSpawner;
          let handle: ChildProcessSpawner.ChildProcessHandle | undefined;
          let scopeStops = 0;
          const owner: AgentScopeShape = {
            ...AgentScope.defaultValue(),
            wrap: () =>
              Effect.succeed({
                // Synthetic one-process writer program; this exercises real handle
                // capture and the real factory/spawner, not installed Codex or cgroups.
                command: process.execPath,
                args: [
                  "-e",
                  "process.stdin.once('data',()=>process.exit(0)); process.stdin.resume();",
                ],
                unit: "synthetic-codex-factory-writer.scope",
                stop: Effect.gen(function* () {
                  scopeStops++;
                  const captured = handle;
                  if (captured === undefined)
                    return yield* new AgentScopeStopError({
                      unit: "synthetic-codex-factory-writer.scope",
                      detail: "No captured fixture handle",
                    });
                  if (yield* captured.isRunning)
                    yield* Stream.run(Stream.encodeText(Stream.make("finish\n")), captured.stdin);
                  yield* verifyCodexProcessExit(captured);
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new AgentScopeStopError({
                        unit: "synthetic-codex-factory-writer.scope",
                        detail: String(cause),
                      }),
                  ),
                ),
              }),
          };
          const captureSpawner = ChildProcessSpawner.make((command) =>
            underlying.spawn(command).pipe(
              Effect.tap((captured) =>
                Effect.sync(() => {
                  handle = captured;
                }),
              ),
            ),
          );
          const placement = PlacementSpawner.layer({
            version: 2,
            workloadMode: "systemd-descendant",
            helperPath: "/synthetic/helper",
            helperSha256: "a".repeat(64),
            helperDevice: "1",
            helperInode: "2",
            control: { path: "/sys/fs/cgroup/synthetic/control", device: "1", inode: "3" },
            workload: { path: "/sys/fs/cgroup/synthetic/workload", device: "1", inode: "4" },
          }).pipe(
            Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, captureSpawner)),
          );
          const factory = yield* CodexAdapter.CodexAppServerClientFactory.pipe(
            Effect.provide(
              CodexAdapter.layerAppServerClientFactory.pipe(
                Layer.provide(placement),
                Layer.provide(ProcessAttribution.layer),
                Layer.provide(
                  Layer.succeed(
                    ProviderEventLoggers.ProviderEventLoggers,
                    ProviderEventLoggers.NoOpProviderEventLoggers,
                  ),
                ),
                Layer.provide(Layer.succeed(AgentScope, owner)),
              ),
            ),
          );
          let processExit: Effect.Effect<void, CodexErrors.CodexAppServerError> | undefined;
          let lifetimeReceipt: CodexLifetime.CapturedCodexProducerLifetime | undefined;
          const scope = yield* Effect.acquireRelease(Scope.make(), (owned) =>
            Scope.close(owned, Exit.void),
          );
          const settings = yield* decodeCodexSettings({ binaryPath: process.execPath });
          yield* factory
            .open({
              instanceId: ProviderInstanceId.make("codex"),
              threadId: ThreadId.make("synthetic-factory-thread"),
              providerSessionId: ProviderSessionId.make("synthetic-factory-session"),
              runtimePolicy: { cwd: null, runtimeMode: "full-access", interactionMode: "default" },
              settings,
              environment: {},
              captureProcessExit: (readback) => {
                processExit = readback;
              },
              runtimeGeneration: "synthetic-factory-generation",
              captureProducerLifetime: (receipt) => {
                lifetimeReceipt = receipt;
              },
            })
            .pipe(Effect.provideService(Scope.Scope, scope));
          const capturedHandle = handle;
          const capturedExit = processExit;
          const capturedLifetime = lifetimeReceipt;
          if (
            capturedHandle === undefined ||
            capturedExit === undefined ||
            capturedLifetime === undefined
          )
            return yield* Effect.die("Actual factory did not capture both required receipts");
          assert.isTrue(yield* capturedHandle.isRunning);
          const expected: CodexLifetime.CodexCapturedProducerIdentity = {
            driver: "codex",
            instanceId: "codex",
            providerSessionId: "synthetic-factory-session",
            threadId: "synthetic-factory-thread",
            generation: "synthetic-factory-generation",
          };
          for (const changed of [
            { ...expected, generation: "replacement-generation" },
            { ...expected, providerSessionId: "other-session" },
            { ...expected, instanceId: "other-instance" },
            { ...expected, threadId: "other-thread" },
          ])
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  CodexLifetime.awaitCapturedCodexProducerLifetime(capturedLifetime, changed),
                ),
              ),
            );
          const serialized = decodeJson(encodeJson(capturedLifetime));
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                CodexLifetime.awaitCapturedCodexProducerLifetime(serialized, expected),
              ),
            ),
          );
          const wait = yield* CodexLifetime.awaitCapturedCodexProducerLifetime(
            capturedLifetime,
            expected,
          ).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          assert.isUndefined(wait.pollUnsafe());
          yield* Scope.close(scope, Exit.void);
          yield* capturedExit;
          assert.deepStrictEqual(yield* Fiber.join(wait), {
            ...expected,
            pid: Number(capturedHandle.pid),
            scopeUnit: "synthetic-codex-factory-writer.scope",
          });
          assert.isFalse(yield* capturedHandle.isRunning);
          assert.strictEqual(scopeStops, 1);
        }).pipe(
          Effect.provide(NodeServices.layer),
          Effect.provideService(HostProcess.Platform, "linux"),
        ),
      ),
  );
  it.effect("observes one original stop without dispatching a second one", () =>
    Effect.gen(function* () {
      const owner = AgentScope.defaultValue();
      let stops = 0;
      const original = retained(
        owner,
        Effect.sync(() => {
          stops++;
        }),
      );
      const reader = yield* awaitCapturedScopedRetirement(original.receipt).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isUndefined(reader.pollUnsafe());
      assert.strictEqual(stops, 0);
      const stop = observeScopedCommandRetirement(original.command, owner, original.launch.stop);
      yield* stop;
      assert.deepStrictEqual(yield* Fiber.join(reader), { unit: "synthetic-original.scope" });
      yield* stop;
      yield* awaitCapturedScopedRetirement(original.receipt);
      assert.strictEqual(stops, 1);
    }),
  );
  it.effect(
    "successful generic scope closure does not erase a failed captured writer-stop result",
    () =>
      Effect.gen(function* () {
        const owner = AgentScope.defaultValue();
        const original = retained(
          owner,
          Effect.fail(
            new AgentScopeStopError({
              unit: "synthetic-original.scope",
              detail: "writer coverage unknown",
            }),
          ),
        );
        const closed = yield* Effect.scoped(
          Effect.addFinalizer(() =>
            observeScopedCommandRetirement(original.command, owner, original.launch.stop).pipe(
              // Match the existing spawner's explicit best-effort finalizer contract.
              Effect.ignoreCause({ log: true }),
            ),
          ),
        ).pipe(Effect.exit);
        assert.isTrue(Exit.isSuccess(closed));
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(awaitCapturedScopedRetirement(original.receipt))),
        );
      }),
  );
  it.effect("serialized receipts and copied commands cannot reconstruct captured authority", () =>
    Effect.gen(function* () {
      const owner = AgentScope.defaultValue();
      const original = retained(owner, Effect.void);
      const copied = decodeJson(encodeJson(original.receipt));
      assert.deepStrictEqual(copied, {});
      assert.isTrue(Exit.isFailure(yield* Effect.exit(awaitCapturedScopedRetirement(copied))));
      assert.isUndefined(
        captureScopedCommandRetirement(ChildProcess.make(original.launch.command), owner),
      );
      assert.isUndefined(
        captureScopedCommandRetirement(original.command, AgentScope.defaultValue()),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            observeScopedCommandRetirement(
              original.command,
              owner,
              Effect.suspend(() => Effect.void),
            ),
          ),
        ),
      );
      assert.throws(
        () => retainScopedCommand(original.command, owner, original.launch),
        /already retained/,
      );
    }),
  );
  it.effect(
    "late old completion cannot complete a replacement receipt even with the same displayed unit",
    () =>
      Effect.gen(function* () {
        const owner = AgentScope.defaultValue();
        const old = retained(owner, Effect.void);
        const replacement = retained(owner, Effect.void);
        yield* observeScopedCommandRetirement(old.command, owner, old.launch.stop);
        yield* awaitCapturedScopedRetirement(old.receipt);
        const waiting = yield* awaitCapturedScopedRetirement(replacement.receipt).pipe(
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        assert.isUndefined(waiting.pollUnsafe());
        yield* observeScopedCommandRetirement(replacement.command, owner, replacement.launch.stop);
        yield* Fiber.join(waiting);
      }),
  );
  it.effect("requires the actual captured worker exit and original complete-writer callback", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        // This deterministic local worker never spawns another writer. Its stdin
        // release is the complete fixture writer scope, not a kernel-cgroup claim.
        const handle = yield* spawner.spawn(
          ChildProcess.make(process.execPath, [
            "-e",
            "process.stdout.write('ready\\n'); process.stdin.once('data',()=>process.exit(0)); process.stdin.resume();",
          ]),
        );
        const ready = yield* handle.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.runHead,
        );
        assert.deepStrictEqual(Option.getOrNull(ready), "ready");
        assert.isTrue(yield* handle.isRunning);
        const finish = yield* Deferred.make<void>();
        const stopping = yield* Deferred.make<void>();
        let stops = 0;
        const owner = AgentScope.defaultValue();
        const stop = Effect.gen(function* () {
          stops++;
          yield* Deferred.succeed(stopping, undefined);
          yield* Deferred.await(finish);
          yield* Stream.run(Stream.encodeText(Stream.make("finish\n")), handle.stdin);
          yield* verifyCodexProcessExit(handle);
        }).pipe(
          Effect.mapError(
            (cause) =>
              new AgentScopeStopError({
                unit: "synthetic-captured-worker.scope",
                detail: String(cause),
              }),
          ),
        );
        const original = retained(owner, stop, "synthetic-captured-worker.scope");
        const worker = yield* observeScopedCommandRetirement(
          original.command,
          owner,
          original.launch.stop,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(stopping);
        const proof = yield* awaitCapturedScopedRetirement(original.receipt).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        assert.isUndefined(proof.pollUnsafe());
        assert.isTrue(yield* handle.isRunning);
        yield* Deferred.succeed(finish, undefined);
        yield* Fiber.join(worker);
        assert.deepStrictEqual(yield* Fiber.join(proof), {
          unit: "synthetic-captured-worker.scope",
        });
        yield* verifyCodexProcessExit(handle);
        assert.isFalse(yield* handle.isRunning);
        assert.strictEqual(stops, 1);
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
  );
});
