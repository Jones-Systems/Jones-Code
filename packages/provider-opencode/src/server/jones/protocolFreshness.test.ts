import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { CommandResolutionCache, SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import { makeOpenCodeRuntimeProbe, type ProbedOpenCode } from "../versionProbe.ts";
import { readOpenCodeProtocolSource } from "./protocolFreshness.ts";

const v1: ProbedOpenCode = { generation: "v1", version: "1.18.32" };
const v2: ProbedOpenCode = { generation: "v2", version: "2.0.18" };

describe("OpenCode protocol source identity", () => {
  it.effect("preserves real filesystem device and inode metadata without numeric coercion", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "opencode-source-" });
      const binaryPath = path.join(root, "opencode.exe");
      yield* fileSystem.writeFileString(binaryPath, "synthetic executable identity");
      yield* fileSystem.chmod(binaryPath, 0o755);
      const info = yield* fileSystem.stat(binaryPath);
      assert.isTrue(Option.isSome(info.ino));
      const read = readOpenCodeProtocolSource({ binaryPath, serverUrl: "" }, { PATH: root });
      const source = yield* read;
      const represented = yield* Schema.decodeEffect(
        Schema.fromJsonString(
          Schema.Struct({
            info: Schema.Struct({ dev: Schema.String, ino: Schema.String, size: Schema.String }),
          }),
        ),
      )(source);
      assert.deepStrictEqual(represented.info, {
        dev: info.dev.toString(10),
        ino: Option.getOrThrow(info.ino).toString(10),
        size: info.size.toString(),
      });
      assert.strictEqual(yield* read, source);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps escaped shell command identities stable and distinct without local stat", () =>
    Effect.gen(function* () {
      let shim = String.raw`C:\protocol-source\open code.cmd`;
      const read = readOpenCodeProtocolSource(
        { binaryPath: String.raw`C:\protocol-source\opencode.cmd`, serverUrl: "" },
        { PATH: "", PATHEXT: ".CMD" },
      ).pipe(
        Effect.provideService(HostProcess.Platform, "win32"),
        Effect.provideService(CommandResolutionCache, new Map()),
        Effect.provideService(SpawnExecutableResolution, () => shim),
        Effect.provide(Path.layer),
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.makeNoop({
            stat: () => Effect.die("A shell source must not stat a local executable"),
            realPath: () => Effect.die("A shell source must not resolve a local executable"),
          }),
        ),
      );
      const source = yield* read;
      const represented = yield* Schema.decodeEffect(
        Schema.fromJsonString(
          Schema.Struct({
            shell: Schema.Boolean,
            command: Schema.String,
            args: Schema.Array(Schema.String),
          }),
        ),
      )(source);
      assert.deepStrictEqual(represented, {
        shell: true,
        command: String.raw`^"C:\protocol-source\open^ code.cmd^" ^"--version^"`,
        args: [],
      });
      assert.strictEqual(yield* read, source);
      shim = String.raw`C:\protocol-source\open^ code.cmd`;
      assert.notStrictEqual(yield* read, source);
    }),
  );
});

describe("OpenCode admission freshness", () => {
  it.effect("keeps unchanged admissions cached until exactly the 60 second age bound", () =>
    Effect.gen(function* () {
      let calls = 0;
      let result = v1;
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.sync(() => {
          calls += 1;
          return result;
        }),
      );
      assert.deepStrictEqual(yield* probe.get, v1);
      result = v2;
      yield* TestClock.adjust("59 seconds");
      assert.deepStrictEqual(yield* probe.get, v1);
      assert.strictEqual(calls, 1);
      yield* TestClock.adjust("1 second");
      assert.deepStrictEqual(yield* probe.get, v2);
      assert.deepStrictEqual(yield* probe.get, v2);
      assert.strictEqual(calls, 2);
    }),
  );

  it.effect("coalesces concurrent cold, expired and manual refresh work", () =>
    Effect.gen(function* () {
      let calls = 0;
      let started = yield* Deferred.make<void>();
      let release = yield* Deferred.make<void>();
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.gen(function* () {
          calls += 1;
          const capturedRelease = release;
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(capturedRelease);
          return v2;
        }),
      );
      for (const [expectedCalls, refresh] of [
        [1, false],
        [2, false],
        [3, true],
      ] as const) {
        if (expectedCalls > 1) {
          started = yield* Deferred.make<void>();
          release = yield* Deferred.make<void>();
        }
        if (expectedCalls === 2) yield* TestClock.adjust("60 seconds");
        const admission = refresh ? probe.refresh : probe.get;
        const first = yield* admission.pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const second = yield* admission.pipe(Effect.forkChild);
        const third = yield* probe.get.pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        assert.strictEqual(calls, expectedCalls);
        yield* Deferred.succeed(release, undefined);
        assert.deepStrictEqual(yield* Fiber.join(first), v2);
        assert.deepStrictEqual(yield* Fiber.join(second), v2);
        assert.deepStrictEqual(yield* Fiber.join(third), v2);
        assert.strictEqual(calls, expectedCalls);
      }
    }).pipe(Effect.scoped),
  );

  it.effect(
    "rejects an old probe after a manual refresh and never overwrites its advisory result",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let calls = 0;
        const probe = yield* makeOpenCodeRuntimeProbe(
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              return v1;
            }
            return v2;
          }),
        );
        const old = yield* probe.get.pipe(Effect.exit, Effect.forkChild);
        yield* Deferred.await(started);
        assert.deepStrictEqual(yield* probe.refresh, v2);
        yield* Deferred.succeed(release, undefined);
        const oldExit = yield* Fiber.join(old);
        assert.isTrue(Exit.isFailure(oldExit));
        assert.deepStrictEqual(yield* probe.get, v2);
        assert.deepStrictEqual(yield* probe.lastSuccess, Option.some(v2));
        assert.strictEqual(calls, 2);
      }).pipe(Effect.scoped),
  );

  it.effect("fences a prior source epoch while the successor is admitted", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let source = "old-target";
      let calls = 0;
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.gen(function* () {
          calls += 1;
          if (calls === 1) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return v1;
          }
          return v2;
        }),
        Effect.sync(() => source),
      );
      const old = yield* probe.get.pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(started);
      source = "new-target";
      assert.deepStrictEqual(yield* probe.get, v2);
      yield* Deferred.succeed(release, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(old)));
      assert.deepStrictEqual(yield* probe.lastSuccess, Option.some(v2));
      assert.strictEqual(calls, 2);
    }).pipe(Effect.scoped),
  );

  it.effect("does not let a slow identity read reinstate the preceding source", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let source = "old-target";
      let holdRead = false;
      let calls = 0;
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.sync(() => {
          calls += 1;
          return source === "old-target" ? v1 : v2;
        }),
        Effect.gen(function* () {
          const captured = source;
          if (holdRead) {
            holdRead = false;
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          }
          return captured;
        }),
      );
      yield* probe.get;
      holdRead = true;
      const old = yield* probe.get.pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(started);
      source = "new-target";
      assert.deepStrictEqual(yield* probe.get, v2);
      yield* Deferred.succeed(release, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(old)));
      assert.deepStrictEqual(yield* probe.get, v2);
      assert.strictEqual(calls, 2);
    }).pipe(Effect.scoped),
  );

  it.effect("fails an identity change during the probe without an internal retry", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let source = "old-target";
      let calls = 0;
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.gen(function* () {
          calls += 1;
          if (calls === 1) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return v1;
          }
          return v2;
        }),
        Effect.sync(() => source),
      );
      const old = yield* probe.get.pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(started);
      source = "new-target";
      yield* Deferred.succeed(release, undefined);
      const error = yield* Fiber.join(old);
      assert.match(error.detail, /source changed/);
      assert.strictEqual(calls, 1);
      assert.deepStrictEqual(yield* probe.lastSuccess, Option.none());
      assert.deepStrictEqual(yield* probe.get, v2);
      assert.strictEqual(calls, 2);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps lastSuccess advisory after a failed refresh and retries only on a new admission",
    () =>
      Effect.gen(function* () {
        let failure = false;
        let calls = 0;
        const unavailable = new OpenCodeRuntime.OpenCodeRuntimeError({
          operation: "probeOpenCodeServer",
          detail: "401 Unauthorized: the OpenCode server rejected the password.",
        });
        const probe = yield* makeOpenCodeRuntimeProbe(
          Effect.suspend(() => {
            calls += 1;
            return failure ? Effect.fail(unavailable) : Effect.succeed(v2);
          }),
        );
        yield* probe.get;
        failure = true;
        assert.strictEqual(yield* Effect.flip(probe.refresh), unavailable);
        assert.deepStrictEqual(yield* probe.lastSuccess, Option.some(v2));
        assert.strictEqual(yield* Effect.flip(probe.get), unavailable);
        assert.strictEqual(calls, 3);
        failure = false;
        assert.deepStrictEqual(yield* probe.get, v2);
        assert.strictEqual(calls, 4);
      }),
  );

  it.effect("does not let an interrupted owner strand a joined admission", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let calls = 0;
      const probe = yield* makeOpenCodeRuntimeProbe(
        Effect.gen(function* () {
          calls += 1;
          if (calls === 1) {
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never;
          }
          return v2;
        }),
      );
      const first = yield* probe.get.pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const second = yield* probe.get.pipe(Effect.exit, Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(first);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(second)));
      assert.deepStrictEqual(yield* probe.get, v2);
      assert.strictEqual(calls, 2);
    }).pipe(Effect.scoped),
  );

  it.effect("keeps instance caches and invalidation separate for the same source", () =>
    Effect.gen(function* () {
      let calls = 0;
      let result = v1;
      const read = Effect.sync(() => {
        calls += 1;
        return result;
      });
      const first = yield* makeOpenCodeRuntimeProbe(read);
      const second = yield* makeOpenCodeRuntimeProbe(read);
      assert.deepStrictEqual(yield* first.get, v1);
      result = v2;
      assert.deepStrictEqual(yield* second.get, v2);
      assert.deepStrictEqual(yield* first.get, v1);
      assert.strictEqual(calls, 2);
      yield* first.refresh;
      assert.deepStrictEqual(yield* first.get, v2);
      assert.deepStrictEqual(yield* second.get, v2);
      assert.strictEqual(calls, 3);
    }),
  );
});
