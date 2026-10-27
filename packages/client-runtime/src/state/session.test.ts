import { EnvironmentId, sessionGrantsScope, type AuthSessionState } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { HttpClient, HttpClientResponse } from "effect/http";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";

import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import { PrimaryConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import { ConnectionWakeups, type ConnectionWakeup } from "../connection/wakeups.ts";
import { createEnvironmentSessionAtoms } from "./session.ts";

const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("session-test"),
  label: "Synthetic environment",
  httpBaseUrl: "https://session.example.test",
  wsBaseUrl: "wss://session.example.test",
});
const session: AuthSessionState = {
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test",
  },
  scopes: ["orchestration:operate"],
};
const disposers: Array<() => void> = [];
afterEach(() => {
  for (const dispose of disposers.splice(0)) dispose();
  vi.useRealTimers();
});

const harness = Effect.fn("SessionTest.harness")(function* (
  responses: ReadonlyArray<{ status: number; body: unknown }>,
) {
  vi.useFakeTimers();
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
    Option.some({
      environmentId: target.environmentId,
      label: target.label,
      httpBaseUrl: target.httpBaseUrl,
      socketUrl: target.wsBaseUrl,
      httpAuthorization: null,
      target,
    }),
  );
  const supervisor = { prepared } as EnvironmentSupervisor.EnvironmentSupervisor["Service"];
  const wakeups = yield* Queue.unbounded<ConnectionWakeup>();
  const networkStatus = yield* SubscriptionRef.make<"online" | "offline">("online");
  const environmentRegistry = {
    networkStatus,
    followStream: (_environmentId, stream) =>
      Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
  } as EnvironmentRegistry.EnvironmentRegistry["Service"];
  let requests = 0;
  const client = HttpClient.make((request) =>
    Effect.promise(async () => {
      const response = responses[Math.min(requests++, responses.length - 1)]!;
      return HttpClientResponse.fromWeb(
        request,
        new Response(JSON.stringify(response.body), {
          status: response.status,
          headers: { "content-type": "application/json" },
        }),
      );
    }),
  );
  const runtime = Atom.runtime(
    Layer.mergeAll(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
      Layer.succeed(HttpClient.HttpClient, client),
      Layer.succeed(ConnectionWakeups, { changes: Stream.fromQueue(wakeups) }),
    ),
  );
  const atoms = createEnvironmentSessionAtoms(runtime);
  const atom = atoms.sessionStateAtom(target.environmentId);
  const registry = AtomRegistry.make();
  disposers.push(() => registry.dispose());
  disposers.push(registry.mount(atom));
  yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
  return {
    registry,
    atom,
    requests: () => requests,
    foreground: () => Queue.offer(wakeups, "application-active"),
    network: (status: "online" | "offline") => SubscriptionRef.set(networkStatus, status),
  };
});

function granted(result: AsyncResult.AsyncResult<AuthSessionState, unknown>) {
  return result._tag === "Success" && sessionGrantsScope(result.value, "orchestration:operate");
}

describe("mounted environment session recovery", () => {
  it.effect("recovers a failed HTTP read while the prepared connection stays unchanged", () =>
    Effect.gen(function* () {
      const h = yield* harness([
        { status: 503, body: {} },
        { status: 200, body: session },
      ]);
      expect(h.registry.get(h.atom)._tag).toBe("Failure");
      expect(granted(h.registry.get(h.atom))).toBe(false);
      expect(h.requests()).toBe(1);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(2_000));
      expect(h.requests()).toBe(2);
      expect(granted(h.registry.get(h.atom))).toBe(true);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5 * 60_000));
      expect(h.requests()).toBe(2);
    }),
  );

  it.effect("stops after three transient retries with no mounted polling", () =>
    Effect.gen(function* () {
      const h = yield* harness([{ status: 503, body: {} }]);
      for (const [delay, count] of [
        [2_000, 2],
        [5_000, 3],
        [15_000, 4],
      ]) {
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(delay!));
        expect(h.requests()).toBe(count);
        expect(granted(h.registry.get(h.atom))).toBe(false);
      }
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5 * 60_000));
      expect(h.requests()).toBe(4);
      expect(h.registry.get(h.atom)._tag).toBe("Failure");
    }),
  );

  it.effect.each([
    { ...session, authenticated: false },
    { ...session, scopes: ["orchestration:read"] },
  ])("does not retry a resolved permission denial", (body) =>
    Effect.gen(function* () {
      const h = yield* harness([{ status: 200, body }]);
      expect(granted(h.registry.get(h.atom))).toBe(false);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5 * 60_000));
      yield* h.foreground();
      yield* h.network("offline");
      yield* h.network("online");
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
      expect(h.requests()).toBe(1);
      expect(granted(h.registry.get(h.atom))).toBe(false);
    }),
  );

  it.effect(
    "recovers transient failure on foreground or online without polling or a wakeup storm",
    () =>
      Effect.gen(function* () {
        const h = yield* harness([{ status: 503, body: {} }]);
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(22_000));
        expect(h.requests()).toBe(4);
        yield* h.foreground();
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.requests()).toBe(5);
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(22_000));
        expect(h.requests()).toBe(8);
        yield* h.foreground();
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.requests()).toBe(8);
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(8_000));
        yield* h.network("offline");
        yield* h.network("online");
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.requests()).toBe(9);
      }),
  );

  it.effect("does not retry a malformed session response", () =>
    Effect.gen(function* () {
      const h = yield* harness([{ status: 200, body: {} }]);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5 * 60_000));
      expect(h.requests()).toBe(1);
      expect(h.registry.get(h.atom)._tag).toBe("Failure");
    }),
  );
});
