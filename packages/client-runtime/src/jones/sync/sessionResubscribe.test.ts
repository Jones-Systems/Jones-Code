import { EnvironmentId, WS_METHODS } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import { RpcClientError } from "effect/rpc";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import { subscribe } from "../../rpc/client.ts";
import type { WsRpcProtocolClient } from "../../rpc/protocol.ts";
import type * as RpcSession from "../../rpc/session.ts";
import { failedSessionStreamEnd } from "./sessionResubscribe.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const transportLost = () =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({
      message: "stream reset",
      cause: new Error("stream reset"),
    }),
  });

function session(client: WsRpcProtocolClient): RpcSession.RpcSession {
  return {
    client,
    initialConfig: Effect.never,
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

const makeHarness = Effect.fn("TestSessionResubscribe.makeHarness")(function* () {
  const state = yield* SubscriptionRef.make<SupervisorConnectionState>(AVAILABLE_CONNECTION_STATE);
  const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
    Option.none(),
  );
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(Option.none());
  const retryCount = yield* Ref.make(0);
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: TARGET,
    state,
    session: activeSession,
    prepared,
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Ref.update(retryCount, (count) => count + 1),
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
  return { activeSession, retryCount, supervisor };
});

const settle = Effect.fn("TestSessionResubscribe.settle")(function* () {
  for (let attempt = 0; attempt < 100; attempt += 1) yield* Effect.yieldNow;
});

/** Serves terminal events from `serve`, numbering each subscription from 1. */
const terminalClient = (serve: (subscription: number) => Stream.Stream<unknown, unknown>) => {
  const counter = { subscriptions: 0 };
  const client = {
    [WS_METHODS.subscribeTerminalEvents]: () => {
      counter.subscriptions += 1;
      return serve(counter.subscriptions);
    },
  } as unknown as WsRpcProtocolClient;
  return { client, counter };
};

describe("failedSessionStreamEnd", () => {
  it("classifies the endings that may resubscribe and leaves domain failures alone", () => {
    expect(failedSessionStreamEnd(Cause.fail(transportLost()), true)).toBe("transport-lost");
    expect(failedSessionStreamEnd(Cause.interrupt(), false)).toBe("interrupted");
    expect(failedSessionStreamEnd(Cause.die(new Error("boom")), false)).toBe("defect");
    expect(failedSessionStreamEnd(Cause.fail("domain failure"), false)).toBeUndefined();
  });
});

describe("same-session resubscribe", () => {
  it.effect("keeps a completed subscription dormant without the option", () =>
    Effect.gen(function* () {
      const { client, counter } = terminalClient(() => Stream.empty);
      const { activeSession, retryCount, supervisor } = yield* makeHarness();

      yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
      const subscriptionFiber = yield* subscribe(WS_METHODS.subscribeTerminalEvents, {}).pipe(
        Stream.runDrain,
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.forkChild,
      );
      yield* settle();
      yield* TestClock.adjust("1 minute");
      yield* settle();
      yield* Fiber.interrupt(subscriptionFiber);

      expect(counter.subscriptions).toBe(1);
      expect(yield* Ref.get(retryCount)).toBe(0);
    }),
  );

  it.effect.each(["completed", "transport-lost", "interrupted", "defect"] as const)(
    "resubscribes on the same live session after the stream is %s, with backoff",
    (ending) =>
      Effect.gen(function* () {
        // Regression: the sidebar stream ended while its connection stayed up and
        // never resumed, so new threads stayed blank until the window was reloaded.
        const { client, counter } = terminalClient((subscription) => {
          if (subscription > 2) return Stream.never;
          switch (ending) {
            case "completed":
              return Stream.empty;
            case "transport-lost":
              return Stream.fail(transportLost());
            case "interrupted":
              return Stream.failCause(Cause.interrupt());
            case "defect":
              return Stream.die(new Error("boom"));
          }
        });
        const { activeSession, retryCount, supervisor } = yield* makeHarness();
        let defects = 0;

        yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
        const subscriptionFiber = yield* subscribe(
          WS_METHODS.subscribeTerminalEvents,
          {},
          {
            onDefect: () =>
              Effect.sync(() => {
                defects += 1;
              }),
            resubscribeWhileSessionCurrent: "1 second",
          },
        ).pipe(
          Stream.runDrain,
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.forkChild,
        );
        yield* settle();
        expect(counter.subscriptions).toBe(1);
        yield* TestClock.adjust("999 millis");
        yield* settle();
        expect(counter.subscriptions).toBe(1);
        yield* TestClock.adjust("1 millis");
        yield* settle();
        expect(counter.subscriptions).toBe(2);
        // The second consecutive ending doubles the delay.
        yield* TestClock.adjust("1 second");
        yield* settle();
        expect(counter.subscriptions).toBe(2);
        yield* TestClock.adjust("1 second");
        yield* settle();
        expect(counter.subscriptions).toBe(3);
        yield* TestClock.adjust("1 minute");
        yield* settle();
        yield* Fiber.interrupt(subscriptionFiber);

        expect(counter.subscriptions).toBe(3);
        expect(yield* Ref.get(retryCount)).toBe(0);
        // Both RpcClientDefect transport failures and dies report through onDefect.
        expect(defects).toBe(ending === "defect" || ending === "transport-lost" ? 2 : 0);
      }),
  );

  it.effect("keeps backing off when each stream emits a value and then ends", () =>
    Effect.gen(function* () {
      const { client, counter } = terminalClient((subscription) =>
        subscription > 2 ? Stream.never : Stream.make({ type: "output" }),
      );
      const { activeSession, supervisor } = yield* makeHarness();

      yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
      const subscriptionFiber = yield* subscribe(
        WS_METHODS.subscribeTerminalEvents,
        {},
        { resubscribeWhileSessionCurrent: "1 second" },
      ).pipe(
        Stream.runDrain,
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.forkChild,
      );
      yield* settle();
      yield* TestClock.adjust("1 second");
      yield* settle();
      expect(counter.subscriptions).toBe(2);
      // A first value alone does not prove the stream healthy.
      yield* TestClock.adjust("1 second");
      yield* settle();
      expect(counter.subscriptions).toBe(2);
      yield* TestClock.adjust("1 second");
      yield* settle();
      expect(counter.subscriptions).toBe(3);
      yield* Fiber.interrupt(subscriptionFiber);
    }),
  );

  it.effect("restarts the backoff after a stream stays open for 30 seconds", () =>
    Effect.gen(function* () {
      const { client, counter } = terminalClient((subscription) => {
        if (subscription === 1) return Stream.empty;
        if (subscription === 2) return Stream.fromEffect(Effect.sleep("40 seconds"));
        return Stream.never;
      });
      const { activeSession, supervisor } = yield* makeHarness();

      yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
      const subscriptionFiber = yield* subscribe(
        WS_METHODS.subscribeTerminalEvents,
        {},
        { resubscribeWhileSessionCurrent: "1 second" },
      ).pipe(
        Stream.runDrain,
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.forkChild,
      );
      yield* settle();
      yield* TestClock.adjust("1 second");
      yield* settle();
      expect(counter.subscriptions).toBe(2);
      yield* TestClock.adjust("40 seconds");
      yield* settle();
      expect(counter.subscriptions).toBe(2);
      // Without the reset this would wait 2 seconds.
      yield* TestClock.adjust("1 second");
      yield* settle();
      expect(counter.subscriptions).toBe(3);
      yield* Fiber.interrupt(subscriptionFiber);
    }),
  );

  it.effect("leaves a replaced session to the new subscription instead of resubscribing", () =>
    Effect.gen(function* () {
      const subscriptions: string[] = [];
      const firstClient = {
        [WS_METHODS.subscribeTerminalEvents]: () => {
          subscriptions.push("first");
          return Stream.empty;
        },
      } as unknown as WsRpcProtocolClient;
      const secondClient = {
        [WS_METHODS.subscribeTerminalEvents]: () => {
          subscriptions.push("second");
          return Stream.never;
        },
      } as unknown as WsRpcProtocolClient;
      const { activeSession, supervisor } = yield* makeHarness();

      yield* SubscriptionRef.set(activeSession, Option.some(session(firstClient)));
      const subscriptionFiber = yield* subscribe(
        WS_METHODS.subscribeTerminalEvents,
        {},
        { resubscribeWhileSessionCurrent: "1 second" },
      ).pipe(
        Stream.runDrain,
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.forkChild,
      );
      yield* settle();
      yield* SubscriptionRef.set(activeSession, Option.some(session(secondClient)));
      yield* settle();
      yield* TestClock.adjust("1 minute");
      yield* settle();
      yield* Fiber.interrupt(subscriptionFiber);

      expect(subscriptions).toEqual(["first", "second"]);
    }),
  );
});
