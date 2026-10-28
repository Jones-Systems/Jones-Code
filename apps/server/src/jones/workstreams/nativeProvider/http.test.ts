import { assert, it } from "@effect/vitest";
import {
  WorkstreamsNativeSettlementResponse,
  WorkstreamsNativeContextResponse,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { TestClock } from "effect/testing";
import { createNativeProviderHandlers } from "./http.ts";
import { makeWorkstreamsNativeProvider } from "./service.ts";
import { makeProviderFixture, binding, requestText, attestationRequest } from "./testFixtures.ts";

const body = (text: string) => Stream.succeed(new TextEncoder().encode(text));
const responseText = (response: HttpServerResponse.HttpServerResponse): string => {
  assert.strictEqual(response.body._tag, "Uint8Array");
  if (response.body._tag !== "Uint8Array") throw new Error("Expected bounded JSON response.");
  return new TextDecoder().decode(response.body.body);
};
const handlers = () => {
  const fixture = makeProviderFixture();
  return {
    fixture,
    http: createNativeProviderHandlers(fixture.provider(), {
      getBySessionId: (id) =>
        Effect.succeed(id === binding.session_id ? Option.some(binding) : Option.none()),
    }),
  };
};

it.effect(
  "four HTTP handlers return only closed provider metadata and exact terminal evidence",
  () =>
    Effect.gen(function* () {
      const { fixture, http } = handlers();
      const context = yield* http.handle("context", binding.session_id, Stream.empty);
      assert.strictEqual(context.status, 200);
      const parsedContext = yield* Schema.decodeEffect(
        Schema.fromJsonString(WorkstreamsNativeContextResponse),
      )(responseText(context));
      assert.strictEqual(parsedContext.state, "ready");
      const attestationText = yield* Schema.encodeEffect(
        Schema.fromJsonString(WorkstreamsNativeAttestationRequest),
      )(attestationRequest);
      const attested = yield* http.handle(
        "attestations",
        binding.session_id,
        body(attestationText),
      );
      const attestation = yield* Schema.decodeEffect(
        Schema.fromJsonString(WorkstreamsNativeAttestationResponse),
      )(responseText(attested));
      assert.strictEqual(attestation.state, "attested");
      const settled = yield* http.handle("settlements", binding.session_id, body(requestText));
      const result = yield* Schema.decodeEffect(
        Schema.fromJsonString(WorkstreamsNativeSettlementResponse),
      )(responseText(settled));
      assert.strictEqual(result.state, "terminal");
      const lookup = yield* http.handle(
        "settlements/lookup",
        binding.session_id,
        body(requestText),
      );
      assert.strictEqual(responseText(lookup), responseText(settled));
      assert.strictEqual(fixture.calls.length, 1);
      assert.strictEqual(settled.headers["cache-control"], "no-store");
      assert.strictEqual(responseText(context).includes("registry_origin"), false);
      assert.strictEqual(responseText(context).includes("session_id"), false);
    }),
);

it.effect("settlement timeout retains dispatch-start and lookup never resubmits", () =>
  Effect.gen(function* () {
    const fixture = makeProviderFixture();
    const started = yield* Deferred.make<void>();
    let calls = 0;
    const provider = makeWorkstreamsNativeProvider({
      ...fixture.ports,
      engine: {
        dispatch: () =>
          Effect.gen(function* () {
            calls += 1;
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never;
          }),
      },
    });
    const http = createNativeProviderHandlers(provider, {
      getBySessionId: () => Effect.succeedSome(binding),
    });
    const pending = yield* Effect.forkChild(
      http.handle("settlements", binding.session_id, body(requestText)),
    );
    yield* Deferred.await(started);
    yield* TestClock.adjust("15 seconds");
    const response = yield* Fiber.join(pending);
    const result = yield* Schema.decodeEffect(
      Schema.fromJsonString(WorkstreamsNativeSettlementResponse),
    )(responseText(response));
    assert.strictEqual(result.state, "unknown");
    if (result.state === "unknown") assert.strictEqual(result.reason, "provider_unavailable");
    const lookedUp = yield* http.handle(
      "settlements/lookup",
      binding.session_id,
      body(requestText),
    );
    const observation = yield* Schema.decodeEffect(
      Schema.fromJsonString(WorkstreamsNativeSettlementResponse),
    )(responseText(lookedUp));
    assert.strictEqual(observation.state, "unknown");
    if (observation.state === "unknown") assert.strictEqual(observation.reason, "receipt_missing");
    assert.strictEqual(calls, 1);
    assert.strictEqual(fixture.attempts.size, 1);
  }),
);

it.effect("ordinary or unauthenticated browser credentials do not become provider enrollment", () =>
  Effect.gen(function* () {
    const { fixture, http } = handlers();
    assert.strictEqual((yield* http.handle("settlements", null, body(requestText))).status, 401);
    assert.strictEqual(
      (yield* http.handle("settlements", "ordinary-browser-session", body(requestText))).status,
      403,
    );
    const wrongSession = createNativeProviderHandlers(fixture.provider(), {
      getBySessionId: () => Effect.succeedSome(binding),
    });
    assert.strictEqual(
      (yield* wrongSession.handle("settlements", "other-session", body(requestText))).status,
      400,
    );
    assert.strictEqual(fixture.attempts.size, 0);
  }),
);

it.effect(
  "oversized, deep, invalid UTF-8 and additional command keys fail before reservation",
  () =>
    Effect.gen(function* () {
      const { fixture, http } = handlers();
      const variants = [
        body("x".repeat(32_769)),
        Stream.fromIterable([new Uint8Array(32_768), new Uint8Array(1)]),
        body('{"unknown":' + "[".repeat(11) + "null" + "]".repeat(11) + "}"),
        Stream.succeed(new Uint8Array([0xff])),
        body(requestText.slice(0, -1) + ',"command":{"type":"thread.delete"}}'),
        body("not-json"),
      ];
      for (const input of variants) {
        const response = yield* http.handle("settlements", binding.session_id, input);
        assert.strictEqual(response.status, 400);
      }
      assert.strictEqual(fixture.calls.length, 0);
      assert.strictEqual(fixture.attempts.size, 0);
    }),
);

it.effect("context rejects a body and settlement duplicate changed wire bytes conflicts", () =>
  Effect.gen(function* () {
    const { fixture, http } = handlers();
    assert.strictEqual((yield* http.handle("context", binding.session_id, body("{}"))).status, 400);
    assert.strictEqual(
      (yield* http.handle("settlements", binding.session_id, body(requestText))).status,
      200,
    );
    const duplicate = yield* http.handle(
      "settlements",
      binding.session_id,
      body(` ${requestText}`),
    );
    assert.strictEqual(duplicate.status, 409);
    assert.strictEqual(fixture.calls.length, 1);
  }),
);

it.effect(
  "historic unrepresentable event IDs return no-store invalid_request without redispatch",
  () =>
    Effect.gen(function* () {
      const { fixture, http } = handlers();
      assert.strictEqual(
        (yield* http.handle("settlements", binding.session_id, body(requestText))).status,
        200,
      );
      const commandId = fixture.calls[0]!.commandId;
      const event = fixture.events.get(commandId)![0]!;
      const storedId = `event:thread:thread-synthetic:command:workstreams%3A${"a".repeat(64)}:00000000-0000-4000-8000-000000000001`;
      assert.strictEqual(storedId.length, 153);
      fixture.events.set(commandId, [{ ...event, eventId: storedId }]);
      const before = {
        attempts: [...fixture.attempts],
        receipts: [...fixture.receipts],
        events: [...fixture.events],
      };
      for (const operation of ["settlements/lookup", "settlements"] as const) {
        const response = yield* http.handle(operation, binding.session_id, body(requestText));
        assert.strictEqual(response.status, 500);
        assert.deepEqual(
          yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(responseText(response)),
          {
            protocol: binding.protocol,
            state: "rejected",
            reason: "invalid_request",
          },
        );
        assert.strictEqual(response.headers["cache-control"], "no-store");
      }
      assert.strictEqual(fixture.calls.length, 1);
      assert.deepEqual(
        {
          attempts: [...fixture.attempts],
          receipts: [...fixture.receipts],
          events: [...fixture.events],
        },
        before,
      );
    }),
);

it.effect(
  "v2 HTTP keeps its protocol on auth, invalid body and unrepresentable evidence errors",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider(
        {
          ...fixture.ports,
          build: Effect.succeedSome({ ...binding.build, sha: "c".repeat(40) }),
        },
        "v2",
      );
      const http = createNativeProviderHandlers(provider, {
        getBySessionId: (id) =>
          Effect.succeed(id === binding.session_id ? Option.some(binding) : Option.none()),
      });
      for (const [id, text, status, reason] of [
        [null, requestText, 401, "unauthorized"],
        ["other-session", requestText, 403, "forbidden"],
        [binding.session_id, "not-json", 400, "invalid_request"],
      ] as const) {
        const response = yield* http.handle("settlements", id, body(text));
        assert.strictEqual(response.status, status);
        assert.deepEqual(JSON.parse(responseText(response)), {
          protocol: "workstreams-t3-provider/2.0.0",
          state: "rejected",
          reason,
        });
      }
      assert.strictEqual(
        (yield* http.handle("settlements", binding.session_id, body(requestText))).status,
        200,
      );
      const commandId = fixture.calls[0]!.commandId;
      fixture.events.set(commandId, [
        { ...fixture.events.get(commandId)![0]!, eventId: "x".repeat(129) },
      ]);
      const response = yield* http.handle(
        "settlements/lookup",
        binding.session_id,
        body(requestText),
      );
      assert.strictEqual(response.status, 500);
      assert.strictEqual(response.headers["cache-control"], "no-store");
      assert.deepEqual(JSON.parse(responseText(response)), {
        protocol: "workstreams-t3-provider/2.0.0",
        state: "rejected",
        reason: "invalid_request",
      });
      assert.strictEqual(fixture.calls.length, 1);
    }),
);
