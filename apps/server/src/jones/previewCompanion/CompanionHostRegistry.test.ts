import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, type CompanionDown, type CompanionUp } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as Registry from "./CompanionHostRegistry.ts";
import * as Channel from "./CompanionBrowserChannel.ts";

const identity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "synthetic-companion",
  appVersion: "test",
  buildCommit: null,
} as const;
const hello: CompanionUp = {
  type: "hello",
  protocol: 1,
  hostId: "mini",
  label: "Mini",
  platform: "test",
  runtimeIdentity: identity,
  capabilities: {
    cdp: true,
    clipboardText: true,
    uploads: false,
    downloads: false,
    recording: false,
  },
};
const dependencies = Channel.layer.pipe(
  Layer.provideMerge(Registry.layer),
  Layer.provide(
    Layer.succeed(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("test")),
      getDescriptor: Effect.die("unused"),
    }),
  ),
);
const client = Effect.gen(function* () {
  const registry = yield* Registry.CompanionHostRegistry;
  const input = yield* Queue.unbounded<ReadonlyArray<string>>();
  const output = yield* Queue.unbounded<CompanionDown>();
  const closes = yield* Queue.unbounded<number>();
  yield* registry
    .connect({
      read: Queue.take(input).pipe(
        Effect.mapError((cause) => new Registry.CompanionTransportError({ cause })),
      ),
      write: (text) => Queue.offer(output, JSON.parse(text) as CompanionDown).pipe(Effect.asVoid),
      close: (code) =>
        Queue.offer(closes, code).pipe(Effect.andThen(Queue.shutdown(input)), Effect.asVoid),
      writableLength: () => 0,
    })
    .pipe(Effect.scoped, Effect.forkScoped);
  const send = (message: CompanionUp) => Queue.offer(input, [JSON.stringify(message)]);
  yield* send(hello);
  const welcome = yield* Queue.take(output);
  expect(welcome.type).toBe("welcome");
  expect((yield* Queue.take(output)).type).toBe("assignments");
  return { send, output, closes, welcome };
});
it.effect("supersedes an uplink without accepting its stale generation or replaying commands", () =>
  Effect.gen(function* () {
    const registry = yield* Registry.CompanionHostRegistry;
    const first = yield* client;
    const second = yield* client;
    expect(yield* Queue.take(first.closes)).toBe(4409);
    expect(
      second.welcome.type === "welcome" &&
        first.welcome.type === "welcome" &&
        second.welcome.connectionGeneration > first.welcome.connectionGeneration,
    ).toBe(true);
    yield* registry.assign("mini", { threadId: ThreadId.make("thread"), tabId: "tab" });
    expect(yield* Queue.take(second.output)).toEqual({
      type: "mount",
      threadId: "thread",
      tabId: "tab",
    });
    expect((yield* registry.status("mini"))?.runtimeIdentity).toEqual(identity);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("closes after 45 seconds of silence and clears runtime evidence", () =>
  Effect.gen(function* () {
    const registry = yield* Registry.CompanionHostRegistry;
    const connection = yield* client;
    yield* TestClock.adjust("45 seconds");
    expect(yield* Queue.take(connection.closes)).toBe(4408);
    expect(yield* registry.status("mini")).toMatchObject({
      online: false,
      runtimeIdentity: null,
      connectionGeneration: null,
    });
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("retains assignments through reconnection without replaying CDP actions", () =>
  Effect.gen(function* () {
    const registry = yield* Registry.CompanionHostRegistry;
    const first = yield* client;
    yield* registry.assign("mini", { threadId: "thread", tabId: "tab" });
    yield* Queue.take(first.output);
    yield* registry.send("mini", {
      type: "browser",
      command: {
        type: "cdp",
        threadId: "thread",
        tabId: "tab",
        message: '{"id":1,"method":"Input.insertText"}',
      },
    });
    yield* Queue.take(first.output);
    const second = yield* client;
    expect(yield* Queue.take(first.closes)).toBe(4409);
    yield* registry.send("mini", { type: "heartbeat", sentAt: 5 });
    expect(yield* Queue.take(second.output)).toEqual({ type: "heartbeat", sentAt: 5 });
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("detaches a superseded channel and gives the replacement fresh attachment evidence", () =>
  Effect.gen(function* () {
    const registry = yield* Registry.CompanionHostRegistry;
    const channel = yield* Channel.CompanionBrowserChannel;
    const first = yield* client;
    const key = { threadId: "thread", tabId: "tab" };
    yield* registry.assign("mini", key);
    yield* Queue.take(first.output);
    yield* first.send({
      type: "browser",
      event: { type: "attached", ...key, runtimeIdentity: identity },
    });
    expect(yield* channel.awaitAttached(key, "1 second")).toBe(true);
    const before = yield* channel.runtimeEvidence(key);
    const detached = yield* channel.detached.pipe(Stream.runHead, Effect.forkScoped);
    yield* Effect.yieldNow;
    const second = yield* client;
    yield* Fiber.join(detached);
    expect(yield* channel.runtimeEvidence(key)).toBeNull();
    yield* second.send({
      type: "browser",
      event: { type: "attached", ...key, runtimeIdentity: identity },
    });
    expect(yield* channel.awaitAttached(key, "1 second")).toBe(true);
    const after = yield* channel.runtimeEvidence(key);
    expect(after?.runtimeIdentity).toEqual(identity);
    expect(after!.attachmentGeneration).toBeGreaterThan(before!.attachmentGeneration);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);
