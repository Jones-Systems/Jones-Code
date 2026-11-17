import { expect, it } from "@effect/vitest";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import * as Bindings from "./OriginPeerBindings.ts";

const environmentId = EnvironmentId.make("receiver");
const status = (
  sessionId = "receiver-auth-session",
  receiverEnvironmentId: string = environmentId,
) => ({
  after: "session-cursor",
  receiverEnvironmentId,
  sessionId,
  peerLongPoll: false,
  serverNow: "2026-11-16T00:00:00.000Z",
  grants: [],
  attempts: [],
  notices: [],
});
const unused = () => Effect.die("Binding must not resolve tokens or administer links");
function fixture() {
  let generation: string | null = "generation-1";
  let reported = status();
  let fail = false;
  let refusal = false;
  let replyHook = () => {};
  const calls: Array<{
    tool: string;
    arguments: Record<string, unknown>;
    expectedGeneration?: string;
    limits: PeerMcpClient.PeerCallLimits;
  }> = [];
  const links = Layer.succeed(PeerLinks.PeerLinks, {
    enabled: Effect.succeed(true),
    list: Effect.succeed([]),
    currentGeneration: () => Effect.sync(() => Option.fromNullishOr(generation)),
    link: unused,
    describe: unused,
    get: unused,
    unlink: unused,
    resolve: unused,
    recordOutcome: unused,
  });
  const peers = Layer.succeed(PeerMcpClient.PeerMcpClient, {
    call: (input) =>
      Effect.suspend(() => {
        calls.push(input);
        if (refusal)
          return Effect.fail(
            new OrchestratorMcpFailure({ code: "capability_denied", message: "Denied" }),
          );
        if (fail)
          return Effect.fail(
            new OrchestratorMcpFailure({ code: "orchestration_error", message: "Offline" }),
          );
        replyHook();
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- The generic caller supplies input.success per invocation; retain that exact response decoder.
        return Schema.decodeEffect(input.success)(
          input.tool === "t3_remote_admission_peer_status"
            ? reported
            : input.tool === "t3_remote_admission_ack"
              ? { acknowledged: true }
              : { admission: null, result: null },
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Invalid response",
              }),
          ),
        );
      }),
  });
  return {
    calls,
    layer: Bindings.layer.pipe(Layer.provide(links), Layer.provide(peers)),
    replace: (value: string | null) => {
      generation = value;
    },
    report: (value: ReturnType<typeof status>) => {
      reported = value;
    },
    offline: () => {
      fail = true;
    },
    deny: () => {
      refusal = true;
    },
    allow: () => {
      refusal = false;
    },
    onReply: (hook: () => void) => {
      replyHook = hook;
    },
  };
}

it.effect(
  "uses only authenticated status sessionId, single-flights and caches the exact generation",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const bindings = yield* Bindings.OriginPeerBindings;
      const [first, second] = yield* Effect.all(
        [bindings.current(environmentId), bindings.current(environmentId)],
        { concurrency: "unbounded" },
      );
      expect(first).toEqual(second);
      expect(first?.receiverSessionId).toBe("receiver-auth-session");
      expect(first?.peerLongPoll).toBe(false);
      expect(f.calls.length).toBe(1);
      expect(f.calls[0]).toMatchObject({
        tool: "t3_remote_admission_peer_status",
        arguments: { timeoutMs: 0 },
        expectedGeneration: "generation-1",
        limits: Bindings.protocolLimits,
      });
      f.replace("generation-2");
      f.report(status("replacement-session"));
      expect((yield* bindings.current(environmentId))?.receiverSessionId).toBe(
        "replacement-session",
      );
      expect(f.calls[1]?.expectedGeneration).toBe("generation-2");
      f.replace(null);
      expect(yield* bindings.current(environmentId)).toBeNull();
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "retry observe and ack use registered identity union, exact generation and narrow protocol limits",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const bindings = yield* Bindings.OriginPeerBindings;
      const binding = (yield* bindings.current(environmentId))!;
      yield* bindings.observeRetry(binding, "attempt", "observation");
      yield* bindings.ackRetry(binding, "attempt", "observation", "suppressed");
      expect(f.calls[1]).toMatchObject({
        tool: "t3_remote_admission_observe",
        arguments: { attemptId: "attempt", observationKey: "observation" },
        expectedGeneration: "generation-1",
        limits: Bindings.protocolLimits,
      });
      expect(f.calls[2]?.arguments).toEqual({
        attemptId: "attempt",
        observationKey: "observation",
        disposition: "suppressed",
      });
      expect(f.calls[1]?.arguments).not.toHaveProperty("clientRequestId");
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "offline identity discovery returns a typed failure, never an invented local namespace",
  () => {
    const f = fixture();
    f.offline();
    return Effect.gen(function* () {
      const bindings = yield* Bindings.OriginPeerBindings;
      expect((yield* bindings.current(environmentId).pipe(Effect.flip)).code).toBe(
        "orchestration_error",
      );
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("wrong receiver cannot populate the session cache", () => {
  const f = fixture();
  f.report(status("session", "wrong-receiver"));
  return Effect.gen(function* () {
    const bindings = yield* Bindings.OriginPeerBindings;
    expect((yield* bindings.current(environmentId).pipe(Effect.flip)).code).toBe(
      "capability_denied",
    );
    f.report(status());
    expect((yield* bindings.current(environmentId))?.receiverSessionId).toBe(
      "receiver-auth-session",
    );
  }).pipe(Effect.provide(f.layer));
});

it.effect("late authenticated status from a replaced generation cannot bind a new launch", () => {
  const f = fixture();
  f.onReply(() => f.replace("replacement"));
  return Effect.gen(function* () {
    const bindings = yield* Bindings.OriginPeerBindings;
    expect(yield* bindings.current(environmentId)).toBeNull();
    f.onReply(() => {});
    f.report(status("new-session"));
    expect((yield* bindings.current(environmentId))?.receiverSessionId).toBe("new-session");
    expect(f.calls[1]?.expectedGeneration).toBe("replacement");
  }).pipe(Effect.provide(f.layer));
});

it.effect("authentication denial pauses handshake until link generation changes", () => {
  const f = fixture();
  f.deny();
  return Effect.gen(function* () {
    const bindings = yield* Bindings.OriginPeerBindings;
    yield* bindings.current(environmentId).pipe(Effect.flip);
    f.allow();
    expect((yield* bindings.current(environmentId).pipe(Effect.flip)).code).toBe(
      "capability_denied",
    );
    expect(f.calls.length).toBe(1);
    f.replace("replacement");
    expect((yield* bindings.current(environmentId))?.generation).toBe("replacement");
    expect(f.calls.length).toBe(2);
  }).pipe(Effect.provide(f.layer));
});
