import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type * as Cause from "effect/Cause";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { makeDesktopNativeOwnerChannel } from "./DesktopNativeOwner.ts";

const binding = {
  home: "/fixture",
  databasePath: "/fixture/statev2.sqlite",
  profile: "/fixture/profile",
  environmentId: "fixture",
  version: "fixture",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  manifestSha256: "c".repeat(64),
};
const request = { protocol: 1, sequence: 1, nonce: "d".repeat(64), childPid: 123, binding };
const bytes = (value: unknown) => new TextEncoder().encode(`${JSON.stringify(value)}\n`);

it.effect("answers only for the captured child and closes when its source ends", () =>
  Effect.gen(function* () {
    const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    const channel = yield* makeDesktopNativeOwnerChannel({ binding, assertCurrent() {} });
    yield* channel.bind(123, Stream.fromQueue(output));
    const response = yield* channel.input.pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkScoped,
    );
    yield* Queue.offer(output, bytes(request));
    const chunks = yield* Fiber.join(response);
    const decoded = JSON.parse(new TextDecoder().decode(chunks[0]));
    assert.strictEqual(decoded.nonce, request.nonce);
    assert.strictEqual(decoded.parentPid, process.pid);
    assert.match(decoded.runId, /^[a-f0-9]{64}$/);
    yield* Queue.end(output);
    const rest = yield* Stream.runCollect(channel.input);
    assert.deepStrictEqual(rest, []);
  }).pipe(Effect.scoped),
);

it.effect("refuses stale admission and another child before producing proof", () =>
  Effect.gen(function* () {
    for (const fault of ["stale", "child", "oversized"]) {
      const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      let current = true;
      const channel = yield* makeDesktopNativeOwnerChannel({
        binding,
        assertCurrent() {
          if (!current) throw new Error("stale");
        },
      });
      yield* channel.bind(123, Stream.fromQueue(output));
      if (fault === "stale") current = false;
      const response = yield* Stream.runCollect(channel.input).pipe(Effect.forkScoped);
      yield* Queue.offer(
        output,
        fault === "oversized"
          ? new TextEncoder().encode("x".repeat(8193))
          : bytes({ ...request, childPid: fault === "child" ? 456 : 123 }),
      );
      assert.deepStrictEqual(yield* Fiber.join(response), []);
      yield* Queue.end(output);
    }
  }).pipe(Effect.scoped),
);
