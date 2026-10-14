// @effect-diagnostics nodeBuiltinImport:off -- Key fixtures are fixed synthetic bytes inside a scoped temporary directory.
import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import { makeExistingEnrollmentSecretStore } from "./secretStore.ts";

it.effect(
  "existing signing key adapter never initializes directories or generates, writes or removes a key",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "enrollment-key-synthetic-" });
      const secretsDir = `${baseDir}/secrets`;
      const store = makeExistingEnrollmentSecretStore({ baseDir, secretsDir });
      assert.strictEqual(yield* fs.exists(secretsDir), false);
      assert.strictEqual(
        (yield* store.get("server-signing-key").pipe(Effect.result))._tag,
        "Failure",
      );
      for (const mutation of [
        store.getOrCreateRandom("server-signing-key", 32),
        store.set("server-signing-key", new Uint8Array(32)),
        store.create("server-signing-key", new Uint8Array(32)),
        store.remove("server-signing-key"),
      ])
        assert.strictEqual((yield* mutation.pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(yield* fs.exists(secretsDir), false);
      yield* fs.makeDirectory(secretsDir, { mode: 0o700 });
      const directory = yield* Effect.tryPromise(() => NodeFSP.stat(secretsDir)).pipe(Effect.orDie);
      assert.strictEqual(directory.mode & 0o777, 0o700);
      const synthetic = new Uint8Array(32).fill(7);
      yield* fs.writeFile(`${secretsDir}/server-signing-key.bin`, synthetic);
      yield* fs.chmod(`${secretsDir}/server-signing-key.bin`, 0o600);
      assert.deepEqual(Option.getOrNull(yield* store.get("server-signing-key")), synthetic);
      assert.strictEqual(
        (yield* store.get("unrelated-secret").pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(
        Array.from(yield* fs.readFile(`${secretsDir}/server-signing-key.bin`)),
        Array.from(synthetic),
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("existing signing key adapter rejects wrong mode, wrong length and symlinks", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({
      prefix: "enrollment-key-invalid-synthetic-",
    });
    const secretsDir = `${baseDir}/secrets`;
    yield* fs.makeDirectory(secretsDir, { mode: 0o700 });
    const keyPath = `${secretsDir}/server-signing-key.bin`;
    const store = makeExistingEnrollmentSecretStore({ baseDir, secretsDir });
    yield* fs.writeFile(keyPath, new Uint8Array(32));
    yield* fs.chmod(keyPath, 0o644);
    assert.strictEqual(
      (yield* store.get("server-signing-key").pipe(Effect.result))._tag,
      "Failure",
    );
    yield* fs.chmod(keyPath, 0o600);
    yield* fs.writeFile(keyPath, new Uint8Array(31));
    assert.strictEqual(
      (yield* store.get("server-signing-key").pipe(Effect.result))._tag,
      "Failure",
    );
    yield* fs.remove(keyPath);
    yield* Effect.tryPromise(() => NodeFSP.symlink("another-key", keyPath)).pipe(Effect.orDie);
    assert.strictEqual(
      (yield* store.get("server-signing-key").pipe(Effect.result))._tag,
      "Failure",
    );
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);
