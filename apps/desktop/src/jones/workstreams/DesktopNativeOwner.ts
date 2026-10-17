// @effect-diagnostics nodeBuiltinImport:off -- The private channel belongs to the captured desktop child.
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import type * as Cause from "effect/Cause";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type * as PlatformError from "effect/PlatformError";
import type { JonesNativeWriterAdmission } from "@t3tools/shared/jones/nativeWriterFence";
import {
  decodeDesktopNativeOwnerMessage,
  encodeDesktopNativeOwnerMessage,
  sameDesktopNativeOwnerBinding,
  DESKTOP_NATIVE_OWNER_MAX_BYTES,
} from "@t3tools/shared/jones/desktopNativeOwner";

/** One channel is created for one Manager-owned spawn; it cannot be rebound after exit. */
export const makeDesktopNativeOwnerChannel = Effect.fn("makeDesktopNativeOwnerChannel")(function* (
  resolveOwner: () => JonesNativeWriterAdmission,
) {
  const replies = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const runId = NodeCrypto.randomBytes(32).toString("hex");
  let active = true;
  let childPid: number | undefined;
  let sequence = 0;
  let buffer = "";
  const close = Effect.gen(function* () {
    active = false;
    yield* Queue.end(replies);
  });
  yield* Effect.addFinalizer(() => close);
  return {
    input: Stream.fromQueue(replies),
    close,
    bind: (pid: number, output: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
      Effect.gen(function* () {
        if (!active || childPid !== undefined || !Number.isSafeInteger(pid) || pid < 1)
          return yield* Effect.die(new Error("Desktop owner channel cannot adopt another child."));
        childPid = pid;
        yield* output.pipe(
          Stream.decodeText(),
          Stream.runForEach((chunk) =>
            Effect.gen(function* () {
              buffer += chunk;
              yield* Effect.try(() => {
                if (new TextEncoder().encode(buffer).byteLength > DESKTOP_NATIVE_OWNER_MAX_BYTES)
                  throw new Error("Desktop owner challenge exceeds the channel bound.");
              });
              let newline: number;
              while ((newline = buffer.indexOf("\n")) >= 0) {
                const line = buffer.slice(0, newline);
                buffer = buffer.slice(newline + 1);
                const response = yield* Effect.try(() => {
                  if (!active) throw new Error("Desktop owner channel is closed.");
                  const owner = resolveOwner();
                  owner.assertCurrent();
                  const request = decodeDesktopNativeOwnerMessage(line, false);
                  if (
                    request.childPid !== childPid ||
                    request.sequence !== sequence + 1 ||
                    !sameDesktopNativeOwnerBinding(request.binding, owner.binding)
                  )
                    throw new Error("Desktop owner challenge differs from the captured child.");
                  sequence = request.sequence;
                  return { ...request, runId, parentPid: process.pid };
                });
                yield* Queue.offer(replies, encodeDesktopNativeOwnerMessage(response));
              }
            }),
          ),
          Effect.ensuring(close),
          Effect.ignore,
          Effect.forkScoped,
        );
      }),
  };
});
