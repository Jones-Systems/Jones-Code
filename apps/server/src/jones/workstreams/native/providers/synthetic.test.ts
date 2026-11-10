import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { LinkedPrObserver } from "./githubPrObserver.ts";
import { syntheticGitHubLayer } from "./synthetic.ts";

it.effect(
  "synthetic GitHub fixtures match exact source/locator and fail explicitly outside their fixture",
  () =>
    Effect.gen(function* () {
      const locator = {
        host: "github.com" as const,
        repository_owner: "owner",
        repository_name: "repo",
        number: 1,
      };
      const result = {
        outcome: "unavailable" as const,
        observed_at: "2026-01-01T00:00:00Z",
        retry_after_seconds: null,
        last_success: null,
        etag: null,
      };
      const input = {
        source_instance_id: "synthetic",
        locator,
        previous_success: null,
        etag: null,
      };
      yield* Effect.gen(function* () {
        const observer = yield* LinkedPrObserver;
        assert.deepEqual(yield* observer.observe(input), result);
        const missing = yield* observer
          .observe({ ...input, locator: { ...locator, number: 2 } })
          .pipe(Effect.flip);
        assert.strictEqual(missing.code, "provider_unconfigured");
      }).pipe(
        Effect.provide(
          syntheticGitHubLayer([{ source_instance_id: "synthetic", locator, result }]),
        ),
      );
    }),
);
