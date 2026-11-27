import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as RpcSession from "../rpc/session.ts";
import type { ConnectionCatalogEntry } from "./catalog.ts";
import * as ConnectionDriver from "./driver.ts";
import {
  BearerConnectionTarget,
  ConnectionTransientError,
  RelayConnectionTarget,
  SshConnectionTarget,
  type PreparedConnection,
} from "./model.ts";
import * as ConnectionResolver from "./resolver.ts";

const sshTarget = (connectionId: string) =>
  new SshConnectionTarget({
    environmentId: EnvironmentId.make("environment-1"),
    label: "Test environment",
    connectionId,
  });

const ENTRY: ConnectionCatalogEntry = {
  target: sshTarget("ssh-first"),
  profile: Option.none(),
  enabled: true,
  alternateRoutes: [{ target: sshTarget("ssh-second"), profile: Option.none() }],
};

const directTarget = new BearerConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Direct environment",
  connectionId: "paired-address",
});
const relayTarget = new RelayConnectionTarget({
  environmentId: directTarget.environmentId,
  label: "T3 Connect environment",
});
const noHttp = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make(() => Effect.die("Unexpected HTTP request.")),
);

describe("ConnectionDriver", () => {
  it.effect("reports preparing before each route it prepares", () =>
    Effect.gen(function* () {
      const stages = yield* Ref.make<ReadonlyArray<string>>([]);
      const driver = yield* ConnectionDriver.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(ConnectionResolver.ConnectionResolver)({
              prepare: (entry) =>
                Effect.succeed({
                  environmentId: entry.target.environmentId,
                  label: entry.target.label,
                  httpBaseUrl: "http://127.0.0.1:1",
                  socketUrl: "ws://127.0.0.1:1/ws",
                  httpAuthorization: null,
                  target: entry.target,
                } satisfies PreparedConnection),
            }),
            Layer.mock(RpcSession.RpcSessionFactory)({
              connect: () =>
                Effect.fail(
                  new ConnectionTransientError({ reason: "transport", detail: "Socket closed." }),
                ),
            }),
            Layer.mock(RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization)({}),
            noHttp,
          ),
        ),
      );

      yield* driver
        .connect(ENTRY, (progress) => Ref.update(stages, (all) => [...all, progress.stage]))
        .pipe(Effect.scoped, Effect.flip);

      expect(yield* Ref.get(stages)).toEqual([
        "preparing",
        "preparing",
        "opening",
        "preparing",
        "opening",
      ]);
    }),
  );

  it.effect.each([
    {
      kind: "direct",
      target: directTarget,
      directConnection: {
        connectionId: directTarget.connectionId,
        httpBaseUrl: "https://saved.example.test",
      },
    },
    { kind: "relay", target: relayTarget, directConnection: undefined },
  ])(
    "renews the $kind credential only for the synchronized lease",
    ({ target, directConnection }) =>
      Effect.gen(function* () {
        const ready = yield* Deferred.make<void>();
        const synchronizing = yield* Deferred.make<void>();
        const expectedRenewal = {
          expectedEnvironmentId: target.environmentId,
          expiresAtEpochMs: 20_000,
          ...(directConnection === undefined ? {} : { directConnection }),
        };
        const renewal = yield* Deferred.make<typeof expectedRenewal>();
        const renewalStopped = yield* Deferred.make<void>();
        const prepared: PreparedConnection = {
          environmentId: target.environmentId,
          label: target.label,
          httpBaseUrl: "https://saved.example.test",
          socketUrl: "wss://saved.example.test/ws",
          httpAuthorization: {
            _tag: "Dpop",
            accessToken: "test-token",
            expiresAtEpochMs: 20_000,
            ...(directConnection === undefined ? {} : { directConnection }),
          },
          target,
        };
        const driver = yield* ConnectionDriver.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(ConnectionResolver.ConnectionResolver)({
                prepare: () => Effect.succeed(prepared),
              }),
              Layer.mock(RpcSession.RpcSessionFactory)({
                connect: () =>
                  Effect.succeed({
                    client: {} as RpcSession.RpcSession["client"],
                    initialConfig: Effect.never,
                    subscribeServerConfig: () => Stream.never,
                    ready: Deferred.await(ready),
                    probe: Effect.void,
                    inboundFrames: Effect.succeed(0),
                    closed: Effect.never,
                  }),
              }),
              Layer.mock(RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization)({
                renewDpopBeforeExpiry: (input) =>
                  Deferred.succeed(renewal, input).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(Deferred.succeed(renewalStopped, undefined)),
                  ),
              }),
              noHttp,
            ),
          ),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const connecting = yield* driver
              .connect({ target, profile: Option.none(), enabled: true }, (progress) =>
                progress.stage === "synchronizing"
                  ? Deferred.succeed(synchronizing, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              )
              .pipe(Effect.forkScoped);
            yield* Deferred.await(synchronizing);
            expect(yield* Deferred.isDone(renewal)).toBe(false);

            yield* Deferred.succeed(ready, undefined);
            const lease = yield* Fiber.join(connecting);
            expect(lease.prepared).toBe(prepared);
            expect(yield* Deferred.await(renewal)).toEqual(expectedRenewal);
          }),
        );
        expect(yield* Deferred.isDone(renewalStopped)).toBe(true);
      }),
  );
});
