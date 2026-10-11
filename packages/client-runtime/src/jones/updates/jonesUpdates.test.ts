import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import { normalizeDpopHtu } from "@t3tools/shared/dpopCommon";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import { Atom, AtomRegistry } from "effect/reactivity";
import { JonesUpdateState as JonesUpdateStateSchema } from "@t3tools/contracts/jones/jonesUpdates";
import {
  createJonesUpdateAtoms,
  jonesUpdatePresentation,
  observeJonesUpdateState,
} from "./jonesUpdates.ts";
import { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  RelayConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import {
  ManagedRelayDpopSigner,
  type ManagedRelayDpopProofInput,
} from "../../relay/managedRelay.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import type * as RpcSession from "../../rpc/session.ts";

const decodeJonesUpdateState = Schema.decodeUnknownSync(JonesUpdateStateSchema);

const staged: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "staged",
  revision: 2,
  capability: { check: true, download: true, install: true },
  stagedHandle: "fixed-stage",
};
it.effect(
  "keeps relay update polling authenticated while transmitting zero and later revision cursors",
  () =>
    Effect.gen(function* () {
      const target = new RelayConnectionTarget({
        environmentId: EnvironmentId.make("update-relay-auth-test"),
        label: "Synthetic relay",
      });
      const origin = "https://updates.example.test";
      const prepared: PreparedConnection = {
        environmentId: target.environmentId,
        label: target.label,
        httpBaseUrl: origin,
        socketUrl: "wss://updates.example.test/ws",
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "synthetic-token",
          expiresAtEpochMs: 3_600_000,
        },
        target,
      };
      const supervisor = EnvironmentSupervisor.of({
        target,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(Option.none()),
        prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
          Option.some(prepared),
        ),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const followStream: EnvironmentRegistry["Service"]["followStream"] = (_id, stream) =>
        Stream.provideService(stream, EnvironmentSupervisor, supervisor);
      const run: EnvironmentRegistry["Service"]["run"] = (_id, effect) =>
        Effect.provideService(effect, EnvironmentSupervisor, supervisor);
      const environmentRegistry = EnvironmentRegistry.of({
        run,
        followStream,
      } as unknown as EnvironmentRegistry["Service"]);
      const authorizations: Array<string | undefined> = [];
      const authorization = RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("Unexpected bearer authorization"),
        authorizeDpop: () => Effect.die("Polling must not replace the socket"),
        authorizeDpopHttp: (input) =>
          Effect.sync(() => {
            authorizations.push(input.rejectedAccessToken);
            return {
              environmentId: target.environmentId,
              label: target.label,
              httpBaseUrl: origin,
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "synthetic-token",
                expiresAtEpochMs: 3_600_000,
              },
            };
          }),
      });
      const proofs: Array<ManagedRelayDpopProofInput> = [];
      const signer = ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("synthetic-thumbprint"),
        createProof: (input) =>
          Effect.sync(() => {
            proofs.push(input);
            return JSON.stringify({ method: input.method, htu: normalizeDpopHtu(input.url) });
          }),
      });
      const calls: Array<{ url: string; proofHtu: string; method: string }> = [];
      const polled = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const fetchFn: typeof fetch = async (request, init) => {
        const url = String(request);
        const proof = JSON.parse(new Headers(init?.headers).get("dpop")!);
        calls.push({ url, proofHtu: proof.htu, method: init?.method ?? "GET" });
        if (calls.length === 3) Effect.runSync(Deferred.succeed(polled, undefined));
        if (proof.htu !== normalizeDpopHtu(url) || proof.method !== (init?.method ?? "GET"))
          return Response.json(
            {
              _tag: "EnvironmentAuthInvalidError",
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "synthetic-rejection",
            },
            { status: 401 },
          );
        if (calls.length === 3) {
          await Effect.runPromise(Deferred.await(release));
          return Response.json(null);
        }
        return Response.json({ ...staged, revision: calls.length === 1 ? 0 : 7 });
      };
      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry, environmentRegistry),
          Layer.succeed(RemoteEnvironmentAuthorization, authorization),
          Layer.succeed(ManagedRelayDpopSigner, signer),
          layerRemoteHttpClient(fetchFn),
        ),
      );
      const registry = AtomRegistry.make();
      const value = createJonesUpdateAtoms(runtime).value(target.environmentId);
      const observed: Array<number> = [];
      let unsubscribe = () => {};
      let unmount = () => {};
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          Effect.runSync(Deferred.succeed(release, undefined));
          unsubscribe();
          unmount();
          registry.dispose();
        }),
      );
      unsubscribe = registry.subscribe(value, (state) => {
        if (state !== null) observed.push(state.revision);
      });
      unmount = registry.mount(value);
      yield* Deferred.await(polled);
      yield* Effect.yieldNow;
      expect(calls.map((call) => new URL(call.url).searchParams.get("after"))).toEqual([
        null,
        "0",
        "7",
      ]);
      expect(
        calls.every(
          (call) =>
            !call.url.includes("%3F") &&
            call.proofHtu === normalizeDpopHtu(call.url) &&
            call.method === "GET",
        ),
      ).toBe(true);
      expect(proofs.map((proof) => proof.url)).toEqual(
        Array(3).fill(`${origin}/api/jones-updates`),
      );
      expect(authorizations).toEqual([undefined, undefined, undefined]);
      expect(observed).toEqual([0, 7]);
      expect(registry.get(value)?.revision).toBe(7);
    }).pipe(Effect.scoped),
);
it.effect("starts observation after an initially disconnected mount prepares its connection", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.none<string>());
    const disconnected = yield* Deferred.make<void>();
    let reads = 0;
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) =>
        after === undefined
          ? Effect.sync(() => {
              reads++;
              return staged;
            })
          : Effect.never,
    ).pipe(
      Stream.tap((state) =>
        state === null ? Deferred.succeed(disconnected, undefined) : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(disconnected);
    expect(reads).toBe(0);
    yield* SubscriptionRef.set(connections, Option.some("prepared"));
    expect(yield* Fiber.join(observed)).toEqual([null, staged]);
    expect(reads).toBe(1);
  }),
);
it.effect("hides Jones controls when an older host has no updater endpoint", () =>
  Effect.gen(function* () {
    const state = yield* observeJonesUpdateState(Stream.succeed(Option.some("older-host")), () =>
      Effect.fail("missing-endpoint"),
    ).pipe(Stream.runHead);
    expect(state).toEqual(Option.some(null));
  }),
);

it.effect("an ordinary Release host leaves Jones absent and ends HTTP observation", () =>
  Effect.gen(function* () {
    let reads = 0;
    const states = yield* observeJonesUpdateState(Stream.succeed(Option.some("release-host")), () =>
      Effect.sync(() => {
        reads++;
        return null;
      }),
    ).pipe(Stream.runCollect);
    expect(states).toEqual([null]);
    expect(reads).toBe(1);
  }),
);

it.effect("resumes mounted observation with a fresh cursor after connection replacement", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const firstState = yield* Deferred.make<void>();
    const disconnected = yield* Deferred.make<void>();
    const cursors: Array<number | undefined> = [];
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) => {
        cursors.push(after);
        return after === undefined ? Effect.succeed(staged) : Effect.never;
      },
    ).pipe(
      Stream.tap((state) =>
        state === null
          ? Deferred.succeed(disconnected, undefined)
          : Deferred.succeed(firstState, undefined),
      ),
      Stream.take(3),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(firstState);
    yield* SubscriptionRef.set(connections, Option.none());
    yield* Deferred.await(disconnected);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    expect(yield* Fiber.join(observed)).toEqual([staged, null, staged]);
    expect(cursors.filter((cursor) => cursor === undefined)).toHaveLength(2);
  }),
);

it.effect("long-polls by the returned revision until the host reports no Jones state", () =>
  Effect.gen(function* () {
    const cursors: Array<number | undefined> = [];
    const newer: JonesUpdateState = {
      ...staged,
      revision: 3,
      phase: "available",
      capability: { check: true, download: false, install: true },
    };
    const states = yield* observeJonesUpdateState(
      Stream.succeed(Option.some("prepared")),
      (after) => {
        cursors.push(after);
        return Effect.succeed(after === undefined ? staged : after === 2 ? newer : null);
      },
    ).pipe(Stream.runCollect);
    expect(cursors).toEqual([undefined, 2, 3]);
    expect(states).toEqual([staged, newer, null]);
  }),
);

it("keeps restart pending until a host outcome arrives and presents rollback reasons", () => {
  expect(
    jonesUpdatePresentation({ ...staged, phase: "installing", updateId: "native-id" }),
  ).toMatchObject({
    busy: true,
    message: "Installing — server restarting. Waiting for the launcher outcome.",
    outcomeMessage: undefined,
  });
  expect(
    jonesUpdatePresentation({
      ...staged,
      phase: "rolled-back",
      outcome: {
        status: "rolled-back",
        fromVersion: "1.0.0",
        targetVersion: "2.0.0",
        reason: "candidate older than database",
      },
    }),
  ).toMatchObject({
    busy: false,
    outcomeMessage: "Rolled back: 1.0.0 → 2.0.0 · candidate older than database",
  });
});

it("decodes older Jones update payloads without outcome metadata", () => {
  expect(decodeJonesUpdateState(staged)).toEqual(staged);
});
