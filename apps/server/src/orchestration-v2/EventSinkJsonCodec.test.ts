import * as NodePathLayer from "@effect/platform-node/NodePath";
import * as JsonCodec from "./EventSinkJsonCodec.ts";
import { assert, describe, it } from "@effect/vitest";
import { EventId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Path from "effect/Path";
import { vi } from "vite-plus/test";

import { decodeJson, encodeBirthTupleJson, jsonCause } from "./EventSinkJsonCodec.ts";

const nativeMalformedJsonCause = (): unknown => {
  try {
    JSON.parse("{");
  } catch (cause) {
    return cause;
  }
};

describe("EventSink native JSON boundary", () => {
  it.effect("accepts JSON null, arrays and objects without projecting their shape", () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* decodeJson("null"), null);
      assert.deepEqual(yield* decodeJson('[null,1,"value"]'), [null, 1, "value"]);
      assert.deepEqual(yield* decodeJson('{"value":null}'), { value: null });
    }),
  );

  it.effect("returns the original native SyntaxError instance at the defect boundary", () =>
    Effect.gen(function* () {
      const nativeCause = nativeMalformedJsonCause();
      assert.instanceOf(nativeCause, SyntaxError);
      const parse = vi.spyOn(JSON, "parse").mockImplementationOnce(() => {
        throw nativeCause;
      });
      try {
        const exit = yield* Effect.exit(
          decodeJson("{").pipe(Effect.catch((error) => Effect.die(jsonCause(error)))),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), nativeCause);
      } finally {
        parse.mockRestore();
      }
    }),
  );

  const tuple: Parameters<typeof encodeBirthTupleJson>[0] = [
    "t3.orchestration-v2.thread-birth/v1",
    EventId.make('birth-"\\\n☃'),
    7,
  ];
  const nativeBirthTupleJson = JSON.stringify(tuple);

  it.effect("preserves the birth incarnation's native JSON bytes", () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* encodeBirthTupleJson(tuple), nativeBirthTupleJson);
    }),
  );
});

describe("EventSink raw JSON projections", () => {
  const expected = {
    threadId: "thread",
    projectId: "project",
    branch: "main",
    projectRoot: "/project",
    path: "/project/checkout",
  };

  it.effect("retains a malformed field projection's original SyntaxError instance", () =>
    Effect.gen(function* () {
      const nativeCause = nativeMalformedJsonCause();
      assert.instanceOf(nativeCause, SyntaxError);
      const parse = vi.spyOn(JSON, "parse").mockImplementationOnce(() => {
        throw nativeCause;
      });
      try {
        const exit = yield* Effect.exit(
          JsonCodec.decodeOwnerBirth("{").pipe(
            Effect.catch((error) => Effect.die(jsonCause(error))),
          ),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), nativeCause);
      } finally {
        parse.mockRestore();
      }
    }),
  );

  it.effect("preserves the actual null-field TypeError through its native defect boundary", () =>
    Effect.gen(function* () {
      const result = yield* Effect.result(JsonCodec.decodeOwnerBirth("null"));
      assert.strictEqual(result._tag, "Failure");
      if (result._tag !== "Failure") return;
      const original = jsonCause(result.failure);
      assert.instanceOf(original, TypeError);
      const exit = yield* Effect.exit(
        Effect.fail(result.failure).pipe(Effect.catch((error) => Effect.die(jsonCause(error)))),
      );
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), original);
    }),
  );

  it.effect("keeps primitive and missing raw properties undefined", () =>
    Effect.gen(function* () {
      for (const text of ["0", "false", '"text"', "{}", "[]"]) {
        assert.strictEqual(yield* JsonCodec.decodeOwnerBirth(text), undefined);
        assert.strictEqual(yield* JsonCodec.decodeBindingSha256(text), undefined);
        assert.strictEqual(yield* JsonCodec.decodeProviderThreadId(text), undefined);
      }
      assert.strictEqual(
        (yield* JsonCodec.decodeCleanupCorrelation('{"evidence":null}')).evidenceSchema,
        undefined,
      );
    }),
  );

  it.effect("does not resolve a malformed path after an earlier inventory mismatch", () =>
    Effect.gen(function* () {
      const NodePath = yield* Path.Path.pipe(Effect.provide(NodePathLayer.layer));
      const resolve = vi.fn((root: string, path: string) => NodePath.resolve(root, path));
      assert.isTrue(
        yield* JsonCodec.decodeDeletionInventoryMismatch(
          '{"id":"other","worktreePath":7}',
          expected,
          resolve,
        ),
      );
      assert.strictEqual(resolve.mock.calls.length, 0);
    }),
  );

  it.effect("returns the original native path TypeError instance for a reached path", () =>
    Effect.gen(function* () {
      let nativeCause: unknown;
      const NodePath = yield* Path.Path.pipe(Effect.provide(NodePathLayer.layer));
      const resolve = (root: string, path: string) => {
        try {
          return NodePath.resolve(root, path);
        } catch (cause) {
          nativeCause = cause;
          throw cause;
        }
      };
      const exit = yield* Effect.exit(
        JsonCodec.decodeDeletionInventoryMismatch(
          '{"id":"thread","projectId":"project","branch":"main","worktreePath":7}',
          expected,
          resolve,
        ).pipe(Effect.catch((error) => Effect.die(jsonCause(error)))),
      );
      assert.instanceOf(nativeCause, TypeError);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), nativeCause);
    }),
  );

  it.effect("keeps cleanup path evaluation before the later identity comparisons", () =>
    Effect.gen(function* () {
      const NodePath = yield* Path.Path.pipe(Effect.provide(NodePathLayer.layer));
      const resolve = vi.fn((root: string, path: string) => NodePath.resolve(root, path));
      const result = yield* Effect.result(
        JsonCodec.decodeCleanupDeletionMismatch(
          '{"id":"other","worktreePath":7}',
          expected,
          resolve,
        ),
      );
      assert.strictEqual(resolve.mock.calls.length, 1);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.instanceOf(jsonCause(result.failure), TypeError);
    }),
  );

  it.effect("does not resolve an inventory path with a null project root", () =>
    Effect.gen(function* () {
      const NodePath = yield* Path.Path.pipe(Effect.provide(NodePathLayer.layer));
      const resolve = vi.fn((root: string, path: string) => NodePath.resolve(root, path));
      assert.isFalse(
        yield* JsonCodec.decodeDeletionInventoryMismatch(
          '{"id":"thread","projectId":"project","branch":"main","worktreePath":7}',
          { ...expected, projectRoot: null, path: null },
          resolve,
        ),
      );
      assert.strictEqual(resolve.mock.calls.length, 0);
    }),
  );

  it.effect("preserves shared-worktree guards before project identity construction", () =>
    Effect.gen(function* () {
      assert.isNull(yield* JsonCodec.decodeSharedWorktreeCandidate('{"deletedAt":1}'));
      assert.isNull(
        yield* JsonCodec.decodeSharedWorktreeCandidate('{"deletedAt":null,"worktreePath":7}'),
      );
      assert.deepEqual(
        yield* JsonCodec.decodeSharedWorktreeCandidate(
          '{"deletedAt":null,"worktreePath":"checkout","projectId":"project"}',
        ),
        { projectId: "project", worktreePath: "checkout" },
      );
    }),
  );

  it.effect("retains parse-before-created ordering and short-circuits later thread fields", () =>
    Effect.gen(function* () {
      assert.isTrue(yield* JsonCodec.decodeThreadPathChanged("null", false, "project", null));
      assert.isTrue(yield* JsonCodec.decodeThreadPathChanged("0", true, "project", null));
      assert.isFalse(
        yield* JsonCodec.decodeThreadPathChanged(
          '{"projectId":"project","worktreePath":null}',
          false,
          "project",
          null,
        ),
      );
      const result = yield* Effect.result(
        JsonCodec.decodeThreadPathChanged("{", true, "project", null),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.instanceOf(jsonCause(result.failure), SyntaxError);
    }),
  );
});
