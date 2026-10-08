import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { JonesRuntimePolicyError, requireExplicitJonesReleaseBaseUrl } from "./releasePolicy.ts";

it.effect.each([undefined, "", "   "])("refuses an implicit download origin (%s)", (baseUrl) =>
  Effect.gen(function* () {
    const error = yield* requireExplicitJonesReleaseBaseUrl(baseUrl).pipe(Effect.flip);
    assert.instanceOf(error, JonesRuntimePolicyError);
    assert.include(error.message, "t3 jones host stage-runtime");
  }),
);

it.effect("accepts an explicitly supplied release origin", () =>
  Effect.gen(function* () {
    assert.equal(
      yield* requireExplicitJonesReleaseBaseUrl(" https://jones.example/releases/download "),
      "https://jones.example/releases/download",
    );
  }),
);
