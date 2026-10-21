import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Tracer from "effect/Tracer";
import * as HttpMiddleware from "effect/http/HttpMiddleware";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import * as HttpObservability from "./httpObservability.ts";

describe("HTTP observability redaction", () => {
  it.effect("keeps WebSocket tickets and DPoP proofs out of server spans", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const request = HttpServerRequest.fromWeb(
        new Request("http://localhost/ws?wsTicket=ticket-value&Signature=signed-value&x=1", {
          headers: {
            authorization: "Bearer bearer-value",
            dpop: "proof-value",
            "expo-session": "expo-session-value",
            accept: "application/json",
          },
        }),
      );

      yield* HttpMiddleware.tracer(Effect.succeed(HttpServerResponse.empty({ status: 204 }))).pipe(
        Effect.provideService(HttpServerRequest.HttpServerRequest, request),
        Effect.provideService(HttpMiddleware.TracerHeaderFilter, () => true),
        Effect.withTracer(tracer),
        Effect.provide(HttpObservability.layer),
      );

      assert.deepStrictEqual(
        spans.map((span) => span.kind),
        ["server"],
      );
      const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
      assert.strictEqual(
        attributes["url.full"],
        "http://localhost/ws?wsTicket=REDACTED&Signature=REDACTED&x=1",
      );
      assert.strictEqual(attributes["url.query"], "wsTicket=REDACTED&Signature=REDACTED&x=1");
      assert.strictEqual(attributes["http.request.header.dpop"], "<redacted>");
      assert.strictEqual(attributes["http.request.header.expo-session"], "<redacted>");
      assert.strictEqual(attributes["http.request.header.authorization"], "<redacted>");
      assert.strictEqual(attributes["http.request.header.accept"], "application/json");

      const recorded = JSON.stringify(attributes);
      for (const secret of ["ticket-value", "signed-value", "bearer-value", "proof-value", "expo-session-value"]) {
        assert.notInclude(recorded, secret);
      }
    }),
  );
});
