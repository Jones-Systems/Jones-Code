import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as RegistryStore from "./registryStore.ts";
import { scope, seed, testLayer } from "./sql.testkit.ts";

it.effect("encrypted aggregates preserve exact revisions and stale registry CAS never writes", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* RegistryStore.RegistryStore;
    const value = {
      name: "protected-name",
      progress: { state: "blocked", impediment: "protected-reason" },
    };
    yield* store.put(
      scope,
      "records",
      { metadata: { workstream_id: "ws", version: 1, sort_order: 7 }, value },
      null,
    );
    yield* store.put(
      scope,
      "records",
      { metadata: { workstream_id: "ws", version: 2, sort_order: 8 }, value },
      1,
    );
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          store.put(
            scope,
            "records",
            { metadata: { workstream_id: "ws", version: 3, sort_order: 8 }, value },
            1,
          ),
        ),
      ),
    ).toBe(true);
    expect((yield* store.list(scope, "records"))[0]).toEqual({
      metadata: { workstream_id: "ws", version: 2, sort_order: 8 },
      value,
    });
    yield* store.advance(scope, { server_generation: 1, registry_version: 0 });
    expect(
      Exit.isFailure(
        yield* Effect.exit(store.advance(scope, { server_generation: 1, registry_version: 0 })),
      ),
    ).toBe(true);
    yield* store.advance(
      scope,
      { server_generation: 1, registry_version: 1 },
      { generation: true },
    );
    expect(yield* store.context(scope)).toMatchObject({
      server_generation: 2,
      registry_version: 1,
    });
  }).pipe(Effect.provide(testLayer())),
);

it.effect("competing owner transactions admit one registry CAS winner", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* RegistryStore.RegistryStore;
    const results = yield* Effect.all(
      [1, 2, 3].map(() =>
        Effect.exit(store.advance(scope, { server_generation: 1, registry_version: 0 })),
      ),
      { concurrency: 3 },
    );
    expect(results.filter(Exit.isSuccess)).toHaveLength(1);
    expect(results.filter(Exit.isFailure)).toHaveLength(2);
    expect(yield* store.context(scope)).toMatchObject({
      registry_version: 1,
      server_generation: 1,
    });
  }).pipe(Effect.provide(testLayer())),
);
