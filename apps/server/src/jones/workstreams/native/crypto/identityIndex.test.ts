import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as IdentityIndex from "./identityIndex.ts";
import { scope, seed, testLayer, secrets } from "../store/sql.testkit.ts";

it.effect(
  "identity indexes are stable under content-key rotation and partitioned by owner and source",
  () => {
    const keys = secrets();
    return Effect.gen(function* () {
      yield* seed;
      const index = yield* IdentityIndex.WorkstreamsIdentityIndex;
      const identity = {
        provider: "t3",
        source_instance_id: "source:test",
        resource_kind: "thread",
        id_kind: "internal" as const,
        native_id: "thread:test",
        account_provenance: { kind: "not_account_scoped" as const },
      };
      const digest = yield* index.digest(scope, identity);
      keys.delete("workstreams-content-key-test-home");
      expect(yield* index.digest(scope, identity)).toBe(digest);
      expect(yield* index.digest({ ...scope, owner_id: "other" }, identity)).not.toBe(digest);
      expect(
        yield* index.digest(scope, { ...identity, source_instance_id: "source:other" }),
      ).not.toBe(digest);
    }).pipe(Effect.provide(testLayer({ keys })));
  },
);
