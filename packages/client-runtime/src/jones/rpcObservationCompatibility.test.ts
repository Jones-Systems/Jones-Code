import { EnvironmentId, WS_METHODS } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { RpcClientError } from "effect/rpc";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as RpcSession from "../rpc/session.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import {
  EnvironmentRpcUnavailableError,
  EnvironmentRpcRequestObserver,
  request,
} from "../rpc/client.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
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

const makeHarness = Effect.fn("TestEnvironmentRpc.makeHarness")(function* () {
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
  return {
    activeSession,
    retryCount,
    supervisor,
  };
});

describe("environment RPC", () => {
  it.effect.each(["validation failure", "disconnected"] as const)(
    "keeps %s local without observing, dispatching, or retrying a request",
    (reason) =>
      Effect.gen(function* () {
        let validations = 0;
        let requests = 0;
        let observations = 0;
        const client = {
          [WS_METHODS.cloudGetRelayClientStatus]: () =>
            Effect.sync(() => {
              requests += 1;
              return { status: "available", version: "2026.6.0" };
            }),
        } as unknown as WsRpcProtocolClient;
        const { activeSession, retryCount, supervisor } = yield* makeHarness();
        if (reason === "validation failure") {
          yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
        }

        const failure = yield* request(
          WS_METHODS.cloudGetRelayClientStatus,
          {},
          {
            validateSession: () =>
              Effect.sync(() => {
                validations += 1;
              }).pipe(Effect.andThen(Effect.fail("validation failed"))),
          },
        ).pipe(
          Effect.flip,
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.provideService(
            EnvironmentRpcRequestObserver,
            EnvironmentRpcRequestObserver.of({
              observe: () =>
                Effect.sync(() => {
                  observations += 1;
                  return Effect.void;
                }),
            }),
          ),
        );

        if (reason === "validation failure") {
          expect(failure).toBe("validation failed");
          expect(validations).toBe(1);
        } else {
          expect(failure).toBeInstanceOf(EnvironmentRpcUnavailableError);
          expect(validations).toBe(0);
        }
        expect(requests).toBe(0);
        expect(observations).toBe(0);
        expect(yield* Ref.get(retryCount)).toBe(0);
      }),
  );

  it.effect.each(["failure", "interruption"] as const)(
    "finalizes a validated unary request observation after %s without retrying",
    (reason) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const finish = yield* Deferred.make<void>();
        const observations: string[] = [];
        const client = {
          [WS_METHODS.cloudGetRelayClientStatus]: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(finish)),
              Effect.andThen(
                Effect.fail(
                  new RpcClientError.RpcClientError({
                    reason: new RpcClientError.RpcClientDefect({
                      message: "socket closed",
                      cause: new Error("socket closed"),
                    }),
                  }),
                ),
              ),
            ),
        } as unknown as WsRpcProtocolClient;
        const { activeSession, retryCount, supervisor } = yield* makeHarness();
        yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
        const requestFiber = yield* request(
          WS_METHODS.cloudGetRelayClientStatus,
          {},
          {
            validateSession: () => Effect.void,
          },
        ).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.provideService(
            EnvironmentRpcRequestObserver,
            EnvironmentRpcRequestObserver.of({
              observe: () =>
                Effect.sync(() => {
                  observations.push("start");
                  return Effect.sync(() => {
                    observations.push("finish");
                  });
                }),
            }),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(started);
        expect(observations).toEqual(["start"]);
        if (reason === "failure") {
          yield* Deferred.succeed(finish, undefined);
          expect(Exit.isFailure(yield* Fiber.await(requestFiber))).toBe(true);
        } else {
          yield* Fiber.interrupt(requestFiber);
        }
        expect(observations).toEqual(["start", "finish"]);
        expect(yield* Ref.get(retryCount)).toBe(0);
      }),
  );
});
