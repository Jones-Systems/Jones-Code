// @effect-diagnostics nodeBuiltinImport:off -- Fixtures inspect only their cleanup-owned temporary directory.
import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import { makeNativeCredentialWriter } from "./credentialFile.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const baseDir = yield* fs.makeTempDirectoryScoped({
    prefix: "workstreams-credential-synthetic-",
  });
  const credentialPath = `${baseDir}/credential.json`;
  return {
    fs,
    baseDir,
    credentialPath,
    writer: makeNativeCredentialWriter({ baseDir, credentialPath }),
    children: () => Effect.tryPromise(() => NodeFSP.readdir(baseDir)).pipe(Effect.orDie),
  };
});

it.effect(
  "private credential publication is bounded, mode 0600, retryable and leaves no sibling stage",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      assert.deepEqual(yield* f.writer.observe(), { sha256: null });
      const first = yield* f.writer.publish("synthetic.token.signature", null);
      assert.strictEqual(first.state, "published");
      assert.match(first.afterSha256!, /^[a-f0-9]{64}$/);
      const stat = yield* Effect.tryPromise(() => NodeFSP.stat(f.credentialPath)).pipe(
        Effect.orDie,
      );
      assert.strictEqual(stat.mode & 0o777, 0o600);
      assert.deepEqual(yield* f.children(), ["credential.json"]);
      assert.strictEqual(
        (yield* f.writer.publish("synthetic.token.signature", null)).state,
        "unchanged",
      );
      const conflict = yield* f.writer.publish("other.token.signature", first.afterSha256);
      assert.strictEqual(conflict.state, "conflict");
      assert.deepEqual(yield* f.writer.observe(), { sha256: first.afterSha256 });
      assert.deepEqual(yield* f.children(), ["credential.json"]);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect(
  "credential collisions and cancellation preserve target bytes and clean owned stages",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const results = yield* Effect.all(
        [
          f.writer.publish("first.token.signature", null),
          f.writer.publish("second.token.signature", null),
        ],
        { concurrency: 2 },
      );
      assert.strictEqual(results.filter((result) => result.state === "published").length, 1);
      assert.strictEqual(results.filter((result) => result.state === "conflict").length, 1);
      assert.deepEqual(yield* f.children(), ["credential.json"]);
      const current = yield* f.writer.observe();
      const pending = yield* f.writer
        .publish("third.token.signature", current.sha256)
        .pipe(Effect.forkChild);
      yield* Fiber.interrupt(pending);
      assert.deepEqual(yield* f.writer.observe(), current);
      assert.deepEqual(yield* f.children(), ["credential.json"]);
      const cancelled = yield* fixture;
      const publishing = yield* cancelled.writer
        .publish("cancelled.token.signature", null)
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(publishing);
      const children = yield* cancelled.children();
      assert.strictEqual(
        children.length <= 1 && children.every((name) => name === "credential.json"),
        true,
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect(
  "plan-style file observation never initializes missing directories and rejects unsafe files",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const absent = makeNativeCredentialWriter({
        baseDir: `${f.baseDir}/missing`,
        credentialPath: `${f.baseDir}/missing/credential.json`,
      });
      assert.strictEqual((yield* absent.observe().pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(yield* f.fs.exists(`${f.baseDir}/missing`), false);
      yield* f.fs.writeFileString(
        f.credentialPath,
        encodeJson({ authorization_header: "Bearer synthetic.token" }),
      );
      yield* f.fs.chmod(f.credentialPath, 0o644);
      assert.strictEqual((yield* f.writer.observe().pipe(Effect.result))._tag, "Failure");
      yield* f.fs.chmod(f.credentialPath, 0o600);
      for (const content of [
        encodeJson({ authorization_header: "Bearer synthetic\r\nheader" }),
        encodeJson({ authorization_header: "Bearer synthetic.token", extra: true }),
        "x".repeat(4097),
      ]) {
        yield* f.fs.writeFileString(f.credentialPath, content);
        assert.strictEqual((yield* f.writer.observe().pipe(Effect.result))._tag, "Failure");
      }
      yield* f.fs.remove(f.credentialPath);
      yield* Effect.tryPromise(() => NodeFSP.symlink("other-file", f.credentialPath)).pipe(
        Effect.orDie,
      );
      assert.strictEqual(
        (yield* f.writer.publish("synthetic.token", null).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(yield* f.children(), ["credential.json"]);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);
