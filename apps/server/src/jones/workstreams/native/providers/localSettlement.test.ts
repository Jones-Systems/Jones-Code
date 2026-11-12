import { assert, it } from "@effect/vitest";
import { WorkstreamsNativeSettlementResponseV2 } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { NativeProviderAttempts } from "../../nativeProvider/attemptRepository.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { makeWorkstreamsNativeProvider, sha256Bytes } from "../../nativeProvider/service.ts";
import { binding, makeProviderFixture, request } from "../../nativeProvider/testFixtures.ts";
import * as Attestation from "./localAttestation.ts";
import * as Settlement from "./localSettlement.ts";

const decodeWorkstreamsNativeSettlementResponseV2 = Schema.decodeEffect(
  WorkstreamsNativeSettlementResponseV2,
);

const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: binding.owner_id,
  home_environment_id: "home-synthetic",
  server_generation: 1,
};
const input = { expected_home: home, request };
const services = (
  fixture: ReturnType<typeof makeProviderFixture>,
  authorize: Attestation.SourceAdmission["Service"]["authorize"] = () => Effect.succeed(binding),
) =>
  Settlement.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(
          Attestation.NativeProvider,
          makeWorkstreamsNativeProvider(fixture.ports, "v2"),
        ),
        Layer.succeed(NativeProviderAttempts, fixture.ports.attempts),
        Layer.succeed(Attestation.SourceAdmission, { authorize }),
      ),
    ),
  );

it.effect(
  "fresh settlement looks up first, dispatches through stored attempts once, then becomes lookup-only",
  () =>
    Effect.gen(function* () {
      for (const action of ["settle", "unsettle"] as const) {
        const fixture = makeProviderFixture();
        const admissions: string[] = [];
        const layer = services(fixture, (call) =>
          Effect.sync(() => {
            admissions.push(call.operation);
            return binding;
          }),
        );
        yield* Effect.gen(function* () {
          const source = yield* Settlement.SourceSettlement;
          const call = { ...input, request: { ...request, native_action: action } };
          const terminal = yield* source.settle(call);
          yield* decodeWorkstreamsNativeSettlementResponseV2(terminal);
          assert.strictEqual(terminal.state, "terminal");
          assert.deepEqual(yield* source.settle(call), terminal);
          assert.deepEqual(yield* source.lookup(call), terminal);
          assert.strictEqual(fixture.calls.length, 1);
          assert.strictEqual(
            fixture.calls[0]!.action,
            action === "settle" ? "thread.settle" : "thread.unsettle",
          );
          assert.deepEqual(admissions, [
            "settle_lookup",
            "settle",
            "settle_lookup",
            "settle_lookup",
          ]);
        }).pipe(Effect.provide(layer));
      }
    }),
);

it.effect(
  "ambiguous dispatch and reconstructed adapters recover by exact lookup without another dispatch",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      let dispatches = 0;
      const ports = {
        ...fixture.ports,
        engine: {
          dispatch: () =>
            Effect.sync(() => {
              dispatches++;
              return { sequence: 0, storedEvents: [] };
            }),
        },
      };
      const layer = Settlement.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(Attestation.NativeProvider, makeWorkstreamsNativeProvider(ports, "v2")),
            Layer.succeed(NativeProviderAttempts, fixture.ports.attempts),
            Layer.succeed(Attestation.SourceAdmission, {
              authorize: () => Effect.succeed(binding),
            }),
          ),
        ),
      );
      const first = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.settle(input),
      ).pipe(Effect.provide(layer));
      assert.strictEqual(first.state, "unknown");
      const attempt = fixture.attempts.get(fixture.key(request))!;
      assert.notStrictEqual(attempt.dispatchStartedAt, null);
      const stillUnknown = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.settle(input),
      ).pipe(Effect.provide(layer));
      assert.strictEqual(stillUnknown.state, "unknown");
      assert.strictEqual(dispatches, 1);
      fixture.commit(attempt.nativeCommandId);
      const recovered = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.lookup(input),
      ).pipe(Effect.provide(services(fixture)));
      assert.strictEqual(recovered.state, "terminal");
      assert.strictEqual(dispatches, 1);
      assert.strictEqual(fixture.calls.length, 0);
    }),
);

it.effect("recovery of an absent or pre-dispatch crashed attempt never executes it", () =>
  Effect.gen(function* () {
    const fixture = makeProviderFixture();
    const layer = services(fixture);
    const absent = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
      source.lookup(input),
    ).pipe(Effect.provide(layer));
    assert.strictEqual(absent.state, "unknown");
    assert.strictEqual(fixture.attempts.size, 0);
    const interrupted = {
      ...fixture,
      ports: {
        ...fixture.ports,
        attempts: { ...fixture.ports.attempts, startDispatch: () => Effect.succeed(false) },
      },
    };
    yield* Effect.flatMap(Settlement.SourceSettlement, (source) => source.settle(input)).pipe(
      Effect.provide(services(interrupted)),
    );
    assert.strictEqual(fixture.attempts.get(fixture.key(request))!.dispatchStartedAt, null);
    const crashed = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
      source.lookup(input),
    ).pipe(Effect.provide(services(fixture)));
    assert.strictEqual(crashed.state, "unknown");
    assert.strictEqual(fixture.calls.length, 0);
  }),
);

it.effect("home/admission fencing and unconfigured settlement prevent attempt creation", () =>
  Effect.gen(function* () {
    const fixture = makeProviderFixture();
    const denied = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
      source.settle(input),
    ).pipe(
      Effect.provide(
        services(fixture, () =>
          Effect.fail(new Attestation.ProviderError({ code: "provider_mismatch" })),
        ),
      ),
      Effect.flip,
    );
    assert.strictEqual(denied.code, "provider_mismatch");
    assert.strictEqual(fixture.attempts.size, 0);
    for (const operation of ["settle", "lookup"] as const) {
      const missing = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source[operation](input),
      ).pipe(Effect.provide(Settlement.unconfiguredLayer), Effect.flip);
      assert.strictEqual(missing.code, "provider_unconfigured");
    }
  }),
);

it.effect(
  "changed home admission between lookup and dispatch refuses without creating a source attempt",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const layer = services(fixture, (call) =>
        call.operation === "settle"
          ? Effect.fail(new Attestation.ProviderError({ code: "generation_mismatch" }))
          : Effect.succeed(binding),
      );
      const error = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.settle(input),
      ).pipe(Effect.provide(layer), Effect.flip);
      assert.strictEqual(error.code, "generation_mismatch");
      assert.strictEqual(fixture.attempts.size, 0);
      assert.strictEqual(fixture.calls.length, 0);
    }),
);

it.effect(
  "recovery reads a pre-existing wire-provider attempt using the same canonical request digest",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider(fixture.ports, "v2");
      const before = yield* provider.settle(binding, request, sha256Bytes(canonicalJson(request)));
      assert.strictEqual(before.state, "terminal");
      const recovered = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.lookup(input),
      ).pipe(Effect.provide(services(fixture)));
      assert.deepEqual(recovered, before);
      assert.strictEqual(fixture.calls.length, 1);
    }),
);

it.effect(
  "current admission can recover retained exact bytes after session renewal, but cannot cross registry or source fences",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider(fixture.ports, "v2");
      const before = yield* provider.settle(
        binding,
        request,
        sha256Bytes(`\n${canonicalJson(request)}\n`),
      );
      assert.strictEqual(before.state, "terminal");
      const renewed = {
        ...binding,
        session_id: "session-renewed",
        enrollment_id: "enrollment-renewed",
      };
      const recovered = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
        source.lookup(input),
      ).pipe(Effect.provide(services(fixture, () => Effect.succeed(renewed))));
      assert.deepEqual(recovered, before);
      for (const [changed, code] of [
        [{ ...renewed, registry_origin: "https://foreign.invalid" }, "registry_mismatch"],
        [{ ...renewed, store_generation: renewed.store_generation + 1 }, "generation_mismatch"],
        [{ ...renewed, principal_id: "foreign-principal" }, "not_granted"],
        [{ ...renewed, scopes: [] }, "not_granted"],
      ] as const) {
        const error = yield* Effect.flatMap(Settlement.SourceSettlement, (source) =>
          source.lookup(input),
        ).pipe(Effect.provide(services(fixture, () => Effect.succeed(changed))), Effect.flip);
        assert.strictEqual(error.code, code);
      }
      assert.strictEqual(fixture.calls.length, 1);
    }),
);
