import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as KeyProvider from "./keyProvider.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as RegistryStore from "../store/registryStore.ts";
import * as Envelope from "./envelope.ts";
import { scope, seed, testLayer } from "../store/sql.testkit.ts";

it.effect("missing and malformed keys fail typed without calling any secret creation path", () =>
  Effect.gen(function* () {
    yield* seed;
    const keys = yield* KeyProvider.WorkstreamsKeyProvider;
    const crypto = yield* Envelope.WorkstreamsEnvelope;
    const registry = yield* RegistryStore.RegistryStore;
    const sql = yield* SqlClient.SqlClient;
    expect((yield* Effect.flip(keys.get("missing")))._tag).toBe("KeyUnavailable");
    expect(yield* Effect.flip(keys.get("malformed"))).toMatchObject({
      _tag: "KeyUnavailable",
      reason: "invalid_key",
    });
    expect((yield* Effect.flip(keys.indexKey(scope.registry_instance_id)))._tag).toBe(
      "KeyUnavailable",
    );
    expect(
      (yield* Effect.flip(
        crypto.seal(
          { ...scope, purpose: "receipt", record_id: "command", revision: 0 },
          "protected",
        ),
      ))._tag,
    ).toBe("KeyUnavailable");
    expect(
      (yield* Effect.flip(
        registry.put(
          scope,
          "records",
          {
            metadata: { workstream_id: "ws:missing-key", version: 1, sort_order: 0 },
            value: { name: "protected" },
          },
          null,
        ),
      ))._tag,
    ).toBe("KeyUnavailable");
    expect(yield* sql`SELECT workstream_id FROM jones_workstreams_records`).toEqual([]);
  }).pipe(
    Effect.provide(
      testLayer({ keys: new Map([["workstreams-content-key-malformed", new Uint8Array(2)]]) }),
    ),
  ),
);
