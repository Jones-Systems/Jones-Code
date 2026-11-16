import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";
import { hasWorkspaceLeaseProof } from "./WorkspaceLeaseProof.ts";

describe("workspace lease exact scoped reentrancy", () => {
  it.effect("the read-only proof requires the live exact key and original fiber", () =>
    Effect.gen(function* () {
      const key = "/synthetic/proof";
      assert.isFalse(yield* hasWorkspaceLeaseProof(key));
      yield* withWorkspaceLease(
        key,
        Effect.gen(function* () {
          assert.isTrue(yield* hasWorkspaceLeaseProof(key));
          assert.isFalse(yield* hasWorkspaceLeaseProof(key + "/other"));
          const inherited = yield* hasWorkspaceLeaseProof(key).pipe(Effect.forkChild);
          assert.isFalse(yield* Fiber.join(inherited));
          assert.isTrue(yield* hasWorkspaceLeaseProof(key));
        }),
      );
      assert.isFalse(yield* hasWorkspaceLeaseProof(key));
    }),
  );
  it.effect("reuses the same live key on the same fiber without taking a second permit", () =>
    Effect.gen(function* () {
      const order: string[] = [];
      yield* withWorkspaceLease(
        "/synthetic/worktree",
        Effect.gen(function* () {
          order.push("outer");
          yield* withWorkspaceLease(
            "/synthetic/worktree",
            Effect.sync(() => order.push("inner")),
          );
          order.push("after");
        }),
      );
      assert.deepStrictEqual(order, ["outer", "inner", "after"]);
    }),
  );
  it.effect("an inherited child fiber must wait for the actual outer acquisition to end", () =>
    Effect.gen(function* () {
      const attempting = yield* Deferred.make<void>();
      const entered = yield* Deferred.make<void>();
      const child = yield* withWorkspaceLease(
        "/synthetic/worktree",
        Effect.gen(function* () {
          const fiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(attempting, undefined);
            yield* withWorkspaceLease("/synthetic/worktree", Deferred.succeed(entered, undefined));
          }).pipe(Effect.forkChild);
          yield* Deferred.await(attempting);
          yield* Effect.yieldNow;
          assert.isTrue(Option.isNone(yield* Deferred.poll(entered)));
          return fiber;
        }),
      );
      yield* Fiber.join(child);
      assert.isTrue(Option.isSome(yield* Deferred.poll(entered)));
    }),
  );
  it.effect("a different exact key acquires independently while retaining the outer key", () =>
    Effect.gen(function* () {
      const order: string[] = [];
      yield* withWorkspaceLease(
        "/synthetic/one",
        withWorkspaceLease(
          "/synthetic/two",
          withWorkspaceLease(
            "/synthetic/one",
            Effect.sync(() => order.push("inside")),
          ),
        ),
      );
      assert.deepStrictEqual(order, ["inside"]);
    }),
  );
  it.effect("interrupted waiters cannot consume or retain a permit", () =>
    Effect.gen(function* () {
      const attempting = yield* Deferred.make<void>();
      let entered = 0;
      yield* withWorkspaceLease(
        "/synthetic/worktree",
        Effect.gen(function* () {
          const waiter = yield* Effect.gen(function* () {
            yield* Deferred.succeed(attempting, undefined);
            yield* withWorkspaceLease(
              "/synthetic/worktree",
              Effect.sync(() => {
                entered++;
              }),
            );
          }).pipe(Effect.forkChild);
          yield* Deferred.await(attempting);
          yield* Fiber.interrupt(waiter);
          assert.strictEqual(entered, 0);
        }),
      );
      yield* withWorkspaceLease(
        "/synthetic/worktree",
        Effect.sync(() => {
          entered++;
        }),
      );
      assert.strictEqual(entered, 1);
    }),
  );
  it.effect("failure revokes the original scope before the next acquisition", () =>
    Effect.gen(function* () {
      yield* Effect.exit(withWorkspaceLease("/synthetic/worktree", Effect.fail("fixture")));
      assert.strictEqual(
        yield* withWorkspaceLease("/synthetic/worktree", Effect.succeed("next")),
        "next",
      );
    }),
  );
});
