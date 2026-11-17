import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  HOST_FILE_LIST_MAX_ENTRIES,
  HOST_FILE_READ_MAX_FILE_BYTES,
  HOST_FILE_READ_MAX_RESULT_BYTES,
  HOST_FILE_WRITE_MAX_BYTES,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as HostFiles from "./HostFiles.ts";

const layer = HostFiles.layer.pipe(Layer.provideMerge(NodeServices.layer));

const scratch = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "jones-host-files-" });
  return { fileSystem, root, at: (...parts: Array<string>) => path.join(root, ...parts) };
});

const reasonOf = <A>(effect: Effect.Effect<A, HostFiles.HostFileError>) =>
  effect.pipe(
    Effect.flip,
    Effect.map((error) => error.reason),
  );

it.layer(layer)("HostFiles", (it) => {
  it.effect("reads a text file in line windows that continue to its end", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.writeFileString(at("notes.txt"), "one\ntwo\nthree\nfour\n");

      const first = yield* files.read({ path: at("notes.txt"), maxLines: 3 });
      assert.deepStrictEqual(
        { ...first, modifiedAt: null },
        {
          kind: "file",
          path: at("notes.txt"),
          content: "one\ntwo\nthree",
          startLine: 1,
          totalLines: 4,
          nextStartLine: 4,
          sizeBytes: 19,
          modifiedAt: null,
        },
      );
      const rest = yield* files.read({ path: at("notes.txt"), startLine: 4 });
      assert.strictEqual(rest.kind, "file");
      if (rest.kind !== "file") return;
      assert.strictEqual(rest.content, "four");
      assert.strictEqual(rest.nextStartLine, null);

      assert.strictEqual(
        yield* reasonOf(files.read({ path: at("notes.txt"), startLine: 5 })),
        "start_past_end",
      );
    }),
  );

  it.effect("returns an empty file as zero lines", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.writeFileString(at("empty.txt"), "");
      const view = yield* files.read({ path: at("empty.txt") });
      assert.strictEqual(view.kind, "file");
      if (view.kind !== "file") return;
      assert.strictEqual(view.content, "");
      assert.strictEqual(view.totalLines, 0);
      assert.strictEqual(view.nextStartLine, null);
    }),
  );

  it.effect("stops a window at the result budget and continues from the next line", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      const line = "x".repeat(1023);
      const lineCount = HOST_FILE_READ_MAX_RESULT_BYTES / 1024 + 10;
      yield* fileSystem.writeFileString(
        at("wide.txt"),
        Array.from({ length: lineCount }, () => line).join("\n"),
      );
      const view = yield* files.read({ path: at("wide.txt") });
      assert.strictEqual(view.kind, "file");
      if (view.kind !== "file") return;
      assert.strictEqual(view.content.split("\n").length, HOST_FILE_READ_MAX_RESULT_BYTES / 1024);
      assert.strictEqual(view.nextStartLine, HOST_FILE_READ_MAX_RESULT_BYTES / 1024 + 1);
      assert.strictEqual(view.totalLines, lineCount);
    }),
  );

  it.effect("refuses what it cannot return as text lines", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.writeFile(at("image.bin"), new Uint8Array([1, 0, 2]));
      yield* fileSystem.writeFileString(
        at("minified.js"),
        "y".repeat(HOST_FILE_READ_MAX_RESULT_BYTES + 1),
      );
      yield* fileSystem.writeFile(at("huge.log"), new Uint8Array(HOST_FILE_READ_MAX_FILE_BYTES + 1));

      assert.strictEqual(yield* reasonOf(files.read({ path: at("image.bin") })), "binary_file");
      assert.strictEqual(yield* reasonOf(files.read({ path: at("minified.js") })), "line_too_long");
      assert.strictEqual(yield* reasonOf(files.read({ path: at("huge.log") })), "too_large");
      assert.strictEqual(yield* reasonOf(files.read({ path: at("absent.txt") })), "not_found");
      assert.strictEqual(
        yield* reasonOf(files.read({ path: "relative/notes.txt" })),
        "path_not_absolute",
      );
    }),
  );

  it.effect("lists a directory by name and marks a listing it cut short", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.makeDirectory(at("small", "nested"), { recursive: true });
      yield* fileSystem.writeFileString(at("small", "b.txt"), "b");
      yield* fileSystem.writeFileString(at("small", "a.txt"), "a");
      assert.deepStrictEqual(yield* files.read({ path: at("small") }), {
        kind: "directory",
        path: at("small"),
        entries: [
          { name: "a.txt", kind: "file" },
          { name: "b.txt", kind: "file" },
          { name: "nested", kind: "directory" },
        ],
        truncated: false,
      });

      yield* fileSystem.makeDirectory(at("big"));
      yield* Effect.forEach(
        Array.from({ length: HOST_FILE_LIST_MAX_ENTRIES + 1 }, (_, index) => index),
        (index) => fileSystem.writeFileString(at("big", `${index}.txt`), ""),
        { concurrency: 16, discard: true },
      );
      const big = yield* files.read({ path: at("big") });
      assert.strictEqual(big.kind, "directory");
      if (big.kind !== "directory") return;
      assert.strictEqual(big.entries.length, HOST_FILE_LIST_MAX_ENTRIES);
      assert.strictEqual(big.truncated, true);
    }),
  );

  it.effect("expands ~/ against this environment's home directory", () =>
    Effect.gen(function* () {
      const { fileSystem, root, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.writeFileString(at("home.txt"), "home");
      const view = yield* files
        .read({ path: "~/home.txt" })
        .pipe(Effect.provideService(HostProcess.HomeDirectory, root));
      assert.strictEqual(view.path, at("home.txt"));
    }),
  );

  it.effect("creates a file with its parents and replaces one only when asked", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      const target = at("deep", "er", "plan.md");

      assert.deepStrictEqual(yield* files.write({ path: target, content: "first" }), {
        path: target,
        sizeBytes: 5,
        created: true,
      });
      assert.strictEqual(
        yield* reasonOf(files.write({ path: target, content: "second" })),
        "already_exists",
      );
      assert.strictEqual(yield* fileSystem.readFileString(target), "first");

      assert.deepStrictEqual(
        yield* files.write({ path: target, content: "second", overwrite: true }),
        { path: target, sizeBytes: 6, created: false },
      );
      assert.strictEqual(yield* fileSystem.readFileString(target), "second");
    }),
  );

  it.effect("refuses to write over a directory or beyond the size limit", () =>
    Effect.gen(function* () {
      const { fileSystem, at } = yield* scratch;
      const files = yield* HostFiles.HostFiles;
      yield* fileSystem.makeDirectory(at("folder"));
      assert.strictEqual(
        yield* reasonOf(files.write({ path: at("folder"), content: "x", overwrite: true })),
        "is_directory",
      );
      assert.strictEqual(
        yield* reasonOf(
          files.write({ path: at("big.txt"), content: "z".repeat(HOST_FILE_WRITE_MAX_BYTES + 1) }),
        ),
        "content_too_large",
      );
      assert.strictEqual(yield* fileSystem.exists(at("big.txt")), false);
    }),
  );
});
