import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as BindingStore from "./bindingStore.ts";
import { binding, seed, testLayer } from "./sql.testkit.ts";

it.effect("source binding CAS rejects other registries and unknown home generations", () =>
  Effect.gen(function* () {
    yield* seed;
    const bindings = yield* BindingStore.BindingStore;
    yield* bindings.bind(binding, null);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          bindings.bind({ ...binding, registry_instance_id: "different", binding_version: 2 }, 1),
        ),
      ),
    ).toBe(true);
    expect(
      Exit.isFailure(
        yield* Effect.exit(bindings.assertDestination({ ...binding, home_generation: 2 })),
      ),
    ).toBe(true);
    yield* bindings.bind(
      { ...binding, binding_version: 2, home_generation: 2, home_environment_id: "new-home" },
      1,
    );
    expect(yield* bindings.read()).toMatchObject({
      binding_version: 2,
      home_generation: 2,
      home_environment_id: "new-home",
    });
    expect(Exit.isFailure(yield* Effect.exit(bindings.assertDestination(binding)))).toBe(true);
  }).pipe(Effect.provide(testLayer())),
);
