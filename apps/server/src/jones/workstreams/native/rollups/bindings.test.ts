import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Fixture from "../store/sql.testkit.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Bindings from "./bindings.ts";
import * as Registry from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import { syntheticFixture } from "./synthetic.ts";
const home = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const layer = Bindings.layer.pipe(
  Layer.provideMerge(Fixture.testLayer()),
  Layer.provideMerge(Layer.succeed(NativeHome, home)),
);
it.effect("stores binding selections encrypted, advances exact CAS and preserves revocation", () =>
  Effect.gen(function* () {
    yield* Fixture.seed;
    const b = yield* Bindings.Bindings,
      f = syntheticFixture();
    yield* b.put(f.binding, null);
    expect(yield* b.list()).toEqual([f.binding]);
    expect(
      Exit.isFailure(yield* Effect.exit(b.put({ ...f.binding, binding_version: 2 }, null))),
    ).toBe(true);
    yield* b.put({ ...f.binding, binding_version: 2, state: "revoked" }, 1);
    expect((yield* b.list())[0]?.state).toBe("revoked");
    expect(
      Exit.isFailure(
        yield* Effect.exit(b.put({ ...f.binding, owner_id: "other:owner", binding_version: 3 }, 2)),
      ),
    ).toBe(true);
  }).pipe(Effect.provide(layer)),
);
it.effect("rejects an overlapping active source mapping for another Workstream", () =>
  Effect.gen(function* () {
    yield* Fixture.seed;
    const b = yield* Bindings.Bindings,
      f = syntheticFixture();
    yield* b.put(f.binding, null);
    expect(
      Exit.isFailure(yield* Effect.exit(b.put({ ...f.binding, workstream_id: "ws:other" }, null))),
    ).toBe(true);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "advances native context atomically for revisions and rolls back failed binding changes",
  () =>
    Effect.gen(function* () {
      yield* Fixture.seed;
      const bindings = yield* Bindings.Bindings,
        registry = yield* Registry.RegistryStore,
        db = yield* NativeSql.NativeSql,
        f = syntheticFixture();
      expect((yield* registry.context(Fixture.scope))?.registry_version).toBe(0);
      yield* bindings.put(f.binding, null);
      expect((yield* registry.context(Fixture.scope))?.registry_version).toBe(1);
      const changed = {
        ...f.binding,
        binding_version: 2,
        selection: { kind: "project" as const, project_id: "project:other", project_version: 2 },
      };
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            db.transaction(
              Fixture.scope,
              bindings.put(changed, 1).pipe(Effect.andThen(Effect.fail("synthetic rollback"))),
            ),
          ),
        ),
      ).toBe(true);
      expect(yield* bindings.list()).toEqual([f.binding]);
      expect((yield* registry.context(Fixture.scope))?.registry_version).toBe(1);
      yield* bindings.put(changed, 1);
      expect((yield* registry.context(Fixture.scope))?.registry_version).toBe(2);
      expect(
        Exit.isFailure(yield* Effect.exit(bindings.put({ ...changed, binding_version: 3 }, 1))),
      ).toBe(true);
      expect((yield* registry.context(Fixture.scope))?.registry_version).toBe(2);
    }).pipe(Effect.provide(layer)),
);
