import { assert, it } from "@effect/vitest";
import { CommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { IncomingLinkedMessage } from "../peerTransport/LinkedMessageOrigin.ts";
import * as Preparation from "./SendPreparation.ts";

const commandId = CommandId.make("command:mcp:authenticated-namespace:thread-send:original-key");

it.effect("an authenticated remote send without the actual core port never restores", () =>
  Effect.gen(function* () {
    let restored = 0;
    const result = yield* Preparation.prepare(
      {},
      commandId,
      Effect.sync(() => {
        restored++;
      }),
    ).pipe(Effect.provideService(IncomingLinkedMessage, true), Effect.result);
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure.reason, "preparation_port_missing");
    assert.equal(restored, 0);
  }),
);

it.effect(
  "ordinary local preparation preserves its behavior and does not require a remote capture",
  () =>
    Effect.gen(function* () {
      let remoteCalls = 0;
      const owner: Preparation.SendPreparationPorts = {
        remoteSendReceipts: {
          guardPreparation: () => {
            remoteCalls++;
            return Effect.fail({
              _tag: "RemoteSendReceiptError",
              reason: "preparation_capture_missing",
            });
          },
        },
      };
      assert.equal(
        yield* Preparation.prepare(owner, commandId, Effect.succeed("local restored")),
        "local restored",
      );
      assert.equal(remoteCalls, 0);
    }),
);

it.effect("missing immutable capture or changed birth refuses before preparation", () =>
  Effect.gen(function* () {
    for (const reason of ["preparation_capture_missing", "target_generation_changed"]) {
      let restored = 0;
      const owner: Preparation.SendPreparationPorts = {
        remoteSendReceipts: {
          guardPreparation: (capturedCommandId) => {
            assert.equal(capturedCommandId, commandId);
            return Effect.fail({ _tag: "RemoteSendReceiptError", reason });
          },
        },
      };
      const result = yield* Preparation.prepare(
        owner,
        commandId,
        Effect.sync(() => {
          restored++;
        }),
      ).pipe(Effect.provideService(IncomingLinkedMessage, true), Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, reason);
      assert.equal(restored, 0);
    }
  }),
);

// The core owner must separately prove real ThreadCommandExecutor identity,
// birth validation, recursive-dispatch refusal and EventSink recheck. This test
// proves the production S3 helper delegates only preparation, then returns.
it.effect("the actual nested port owns preparation and releases before normal dispatch", () =>
  Effect.gen(function* () {
    let locked = false;
    const seen: string[] = [];
    const owner: Preparation.SendPreparationPorts = {
      remoteSendReceipts: {
        guardPreparation: (capturedCommandId, preparation) =>
          Effect.acquireUseRelease(
            Effect.sync(() => {
              assert.equal(capturedCommandId, commandId);
              assert.isFalse(locked);
              locked = true;
              seen.push("guard");
            }),
            () => preparation,
            () =>
              Effect.sync(() => {
                locked = false;
                seen.push("release");
              }),
          ),
      },
    };
    yield* Preparation.prepare(
      owner,
      commandId,
      Effect.sync(() => {
        assert.isTrue(locked);
        seen.push("restore");
      }),
    ).pipe(Effect.provideService(IncomingLinkedMessage, true));
    assert.isFalse(locked);
    seen.push("normal-dispatch-reacquires-and-rechecks");
    assert.deepEqual(seen, [
      "guard",
      "restore",
      "release",
      "normal-dispatch-reacquires-and-rechecks",
    ]);
  }),
);

it.effect("preparation errors retain their existing owner and still release the guard", () =>
  Effect.gen(function* () {
    const original = new Error("workspace restore refused");
    let released = false;
    const owner: Preparation.SendPreparationPorts = {
      remoteSendReceipts: {
        guardPreparation: (_id, preparation) =>
          preparation.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                released = true;
              }),
            ),
          ),
      },
    };
    const result = yield* Preparation.prepare(owner, commandId, Effect.fail(original)).pipe(
      Effect.provideService(IncomingLinkedMessage, true),
      Effect.result,
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure, original);
    assert.isTrue(released);
  }),
);
