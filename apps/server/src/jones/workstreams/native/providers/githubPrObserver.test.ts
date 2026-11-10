import { assert, it } from "@effect/vitest";
import { WorkstreamPrObservation } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as GitHub from "./githubPrObserver.ts";
import { ProviderError } from "./localAttestation.ts";

const decodeWorkstreamPrObservation = Schema.decodeEffect(WorkstreamPrObservation);

const locator = {
  host: "github.com" as const,
  repository_owner: "jones-systems",
  repository_name: "jones-code",
  number: 660,
};
const input: GitHub.LinkedPrObservationRequest = {
  source_instance_id: "github",
  locator,
  previous_success: null,
  etag: null,
};
const pull = {
  number: 660,
  state: "open",
  draft: false,
  merged: false,
  merged_at: null,
  updated_at: "2026-01-01T00:00:00Z",
  base: { repo: { full_name: "Jones-Systems/Jones-Code" } },
};
const body = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const layer = (get: GitHub.GitHubPrTransport["Service"]["get"]) =>
  GitHub.layer.pipe(
    Layer.provide(
      Layer.merge(
        Layer.succeed(GitHub.GitHubPrAccess, {
          source_instance_id: "github",
          authorization_header: "Bearer synthetic",
          user_agent: "native-workstreams-test",
        }),
        Layer.succeed(GitHub.GitHubPrTransport, { get }),
      ),
    ),
  );

it.effect(
  "GitHub 200 and 304 keep the existing observation schema and caller-owned ETag representation",
  () =>
    Effect.gen(function* () {
      const calls: GitHub.GitHubPrHttpRequest[] = [];
      const configured = layer((request) =>
        Effect.sync(() => {
          calls.push(request);
          return calls.length === 1
            ? { status: 200, headers: { ETag: '"first"' }, body: body(pull) }
            : { status: 304, headers: {}, body: new Uint8Array() };
        }),
      );
      yield* Effect.gen(function* () {
        const observer = yield* GitHub.LinkedPrObserver;
        const first = yield* observer.observe(input);
        assert.strictEqual(first.outcome, "observed");
        assert.strictEqual(first.last_success!.state, "open");
        const second = yield* observer.observe({
          ...input,
          etag: first.etag,
          previous_success: first.last_success,
        });
        assert.strictEqual(second.outcome, "not_modified");
        assert.strictEqual(second.etag, '"first"');
        assert.strictEqual(second.last_success!.provider_updated_at, pull.updated_at);
        yield* decodeWorkstreamPrObservation({
          native_reference_id: "reference:synthetic",
          observation_version: 2,
          attempted_at: second.observed_at,
          outcome: second.outcome,
          retry_after_seconds: second.retry_after_seconds,
          last_success: second.last_success,
          command_id: "command-synthetic-long",
        });
        assert.strictEqual(calls[1]!.headers["if-none-match"], '"first"');
        assert.strictEqual(
          calls[0]!.url,
          "https://api.github.com/repos/jones-systems/jones-code/pulls/660",
        );
        assert.strictEqual(calls[0]!.redirect, "error");
        assert.strictEqual(calls[0]!.signal.aborted, true);
      }).pipe(Effect.provide(configured));
    }),
);

it.effect(
  "rate limits, inaccessible and unavailable observations retain cache without claiming fresh success",
  () =>
    Effect.gen(function* () {
      const cached = {
        state: "merged" as const,
        draft: false,
        observed_at: "2026-01-01T00:00:00Z",
        provider_updated_at: "2026-01-01T00:00:00Z",
      };
      for (const [status, headers, outcome, delay] of [
        [429, { "Retry-After": "120" }, "rate_limited", 120],
        [403, { "x-ratelimit-remaining": "0" }, "rate_limited", 60],
        [429, { "retry-after": "999999" }, "rate_limited", 3600],
        [401, {}, "inaccessible", null],
        [403, {}, "inaccessible", null],
        [404, {}, "inaccessible", null],
        [500, {}, "unavailable", null],
        [302, {}, "invalid_response", null],
      ] as const) {
        const result = yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
          observer.observe({ ...input, previous_success: cached, etag: '"cache"' }),
        ).pipe(
          Effect.provide(layer(() => Effect.succeed({ status, headers, body: new Uint8Array() }))),
        );
        assert.strictEqual(result.outcome, outcome);
        assert.strictEqual(result.retry_after_seconds, delay);
        assert.deepEqual(result.last_success, cached);
        assert.strictEqual(result.etag, '"cache"');
      }
    }),
);

it.effect(
  "uncached 304, invalid identity/body/ETag, oversized and interrupted reads fail honestly",
  () =>
    Effect.gen(function* () {
      for (const response of [
        { status: 304, headers: {}, body: new Uint8Array() },
        { status: 200, headers: {}, body: body({ ...pull, number: 661 }) },
        { status: 200, headers: {}, body: body({ ...pull, merged: true }) },
        { status: 200, headers: { etag: "bad\r\nheader" }, body: body(pull) },
        {
          status: 200,
          headers: {},
          body: new Uint8Array(GitHub.GITHUB_PR_OBSERVATION_BODY_LIMIT_BYTES + 1),
        },
        { status: 200, headers: {}, body: new Uint8Array([255]) },
      ]) {
        const result = yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
          observer.observe(input),
        ).pipe(Effect.provide(layer(() => Effect.succeed(response))));
        assert.strictEqual(result.outcome, "invalid_response");
        assert.strictEqual(result.last_success, null);
      }
      let calls = 0;
      const configured = layer(() =>
        Effect.sync(() => {
          calls++;
          return { status: 200, headers: {}, body: body(pull) };
        }),
      );
      for (const request of [
        { ...input, source_instance_id: "other" },
        { ...input, etag: "bad\nheader" },
        { ...input, locator: { ...locator, repository_name: ".." } },
      ])
        yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
          observer.observe(request),
        ).pipe(Effect.provide(configured));
      assert.strictEqual(calls, 0);
      const unavailable = yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
        observer.observe(input),
      ).pipe(
        Effect.provide(
          layer(() => Effect.fail(new ProviderError({ code: "provider_unavailable" }))),
        ),
      );
      assert.strictEqual(unavailable.outcome, "unavailable");
    }),
);

it.effect("timeout aborts the single request and preserves unavailable semantics", () =>
  Effect.gen(function* () {
    let signal: AbortSignal | undefined;
    const configured = layer((request) => {
      signal = request.signal;
      return Effect.never;
    });
    const fiber = yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
      observer.observe(input),
    ).pipe(Effect.provide(configured), Effect.forkChild);
    yield* TestClock.adjust(GitHub.GITHUB_PR_OBSERVATION_TIMEOUT_MS + 1);
    const result = yield* Fiber.join(fiber);
    assert.strictEqual(result.outcome, "unavailable");
    assert.strictEqual(signal!.aborted, true);
  }),
);

it.effect("unconfigured GitHub remains an explicit provider_unconfigured error", () =>
  Effect.gen(function* () {
    const missing = yield* Effect.flatMap(GitHub.LinkedPrObserver, (observer) =>
      observer.observe(input),
    ).pipe(Effect.provide(GitHub.unconfiguredLayer), Effect.flip);
    assert.strictEqual(missing.code, "provider_unconfigured");
  }),
);
