import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Member from "./memberRead.ts";
import { RegistryError } from "../registry/session.ts";
const target = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const envelope: WorkstreamsNativeReadEnvelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: target.registry_instance_id,
    owner_id: target.owner_id,
    canonical_origin: "https://native.workstreams.invalid",
    server_generation: 1,
    registry_version: 3,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: target.home_environment_id,
    served_by_environment_id: target.home_environment_id,
    source: "live",
    observed_at: "2026-01-01T00:00:00Z",
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const countValue = {
  context: { owner_id: target.owner_id, server_generation: 1, registry_version: 3 },
  principal_id: "principal:test",
  observed_at: envelope.provenance.observed_at,
  authority_effect: "none",
  active: 1,
  total: 1,
  unknown_lifecycle: 0,
};
it.effect("refuses an absent J3 adapter without any successful local or CP read", () =>
  Effect.gen(function* () {
    const member = yield* Member.MemberReads;
    expect(yield* member.read({ route: "counts", params: {} }).pipe(Effect.flip)).toMatchObject({
      code: "unavailable",
    });
  }).pipe(Effect.provide(Member.layer)),
);

it.effect(
  "uses exactly the fixed home port, rechecks admission, and preserves complete v1 data",
  () => {
    let calls = 0,
      checks = 0;
    const ports = Layer.mergeAll(
      Layer.succeed(Member.MemberReadAdmission, {
        current: () =>
          Effect.succeed({
            expected_home: target,
            principal_id: "principal:test",
            authorization_revision: 1,
            check: () =>
              Effect.sync(() => {
                checks++;
              }),
          }),
      }),
      Layer.succeed(Member.HomeReadPort, {
        read: (home, request) => {
          calls++;
          expect(home).toEqual(target);
          expect(request).toEqual({ route: "counts", params: {} });
          return Effect.succeed({
            route: "counts",
            envelope,
            authorization_revision: 1,
            value: countValue,
          });
        },
      }),
    );
    return Effect.gen(function* () {
      const result = yield* (yield* Member.MemberReads).read({ route: "counts", params: {} });
      expect(result.value).toEqual(countValue);
      expect(result.envelope).toEqual(envelope);
      expect(calls).toBe(1);
      expect(checks).toBe(3);
    }).pipe(Effect.provide(Member.layer.pipe(Layer.provide(ports))));
  },
);

it.effect.each([
  "generation",
  "registry",
  "route",
  "revoked",
  "retarget",
  "grant",
  "principal",
  "served-by",
] as const)("refuses %s drift after a forwarded read without retry or fallback", (drift) => {
  let calls = 0,
    admissionReads = 0;
  const ports = Layer.mergeAll(
    Layer.succeed(Member.MemberReadAdmission, {
      current: () =>
        Effect.sync(() => {
          admissionReads++;
          return {
            expected_home:
              drift === "retarget" && admissionReads > 1
                ? { ...target, home_environment_id: "other:home" }
                : target,
            principal_id: "principal:test",
            authorization_revision: 1,
            check: () =>
              drift === "revoked" && calls > 0
                ? Effect.fail(new RegistryError({ code: "forbidden" }))
                : Effect.void,
          };
        }),
    }),
    Layer.succeed(Member.HomeReadPort, {
      read: () => {
        calls++;
        return Effect.succeed({
          route: drift === "route" ? "workstreams" : "counts",
          envelope: {
            ...envelope,
            provenance: {
              ...envelope.provenance,
              ...(drift === "served-by" ? { served_by_environment_id: "other:server" } : {}),
            },
            registry: {
              ...envelope.registry,
              ...(drift === "generation" ? { server_generation: 2 } : {}),
              ...(drift === "registry" ? { owner_id: "other:owner" } : {}),
            },
          },
          value:
            drift === "principal" ? { ...countValue, principal_id: "other:principal" } : countValue,
          authorization_revision: drift === "grant" ? 2 : 1,
        });
      },
    }),
  );
  return Effect.gen(function* () {
    const result = yield* (yield* Member.MemberReads)
      .read({ route: "counts", params: {} })
      .pipe(Effect.flip);
    expect(result.code).toBe(
      drift === "generation"
        ? "generation_conflict"
        : drift === "revoked" || drift === "grant" || drift === "principal"
          ? "forbidden"
          : drift === "route"
            ? "invalid_response"
            : "registry_mismatch",
    );
    expect(calls).toBe(1);
  }).pipe(Effect.provide(Member.layer.pipe(Layer.provide(ports))));
});

it.effect("labels cached projections stale and never serves them as a complete v1 page", () => {
  const cache: WorkstreamsNativeReadEnvelope = {
    ...envelope,
    provenance: { ...envelope.provenance, source: "cache", stale: true },
    coverage: { registry: "stale", sources: [] },
  };
  const ports = Layer.mergeAll(
    Layer.succeed(Member.MemberReadAdmission, {
      current: () =>
        Effect.succeed({
          expected_home: target,
          principal_id: "principal:test",
          authorization_revision: 1,
          check: () => Effect.void,
        }),
    }),
    Layer.succeed(Member.HomeReadPort, {
      read: (_home, request) =>
        Effect.succeed({
          route: request.route,
          envelope: cache,
          value: request.route === "counts" ? countValue : { items: [], next_cursor: null },
          authorization_revision: 1,
        }),
    }),
  );
  return Effect.gen(function* () {
    const member = yield* Member.MemberReads;
    const projection = yield* member.read({ route: "read-projection", params: {} });
    expect(projection.envelope.coverage.registry).toBe("stale");
    expect(projection.envelope.provenance).toMatchObject({ source: "cache", stale: true });
    expect(yield* member.read({ route: "counts", params: {} }).pipe(Effect.flip)).toMatchObject({
      code: "unavailable",
    });
  }).pipe(Effect.provide(Member.layer.pipe(Layer.provide(ports))));
});

it.effect("rejects payload context drift even when the forwarded envelope itself is valid", () => {
  const ports = Layer.mergeAll(
    Layer.succeed(Member.MemberReadAdmission, {
      current: () =>
        Effect.succeed({
          expected_home: target,
          principal_id: "principal:test",
          authorization_revision: 1,
          check: () => Effect.void,
        }),
    }),
    Layer.succeed(Member.HomeReadPort, {
      read: () =>
        Effect.succeed({
          route: "counts",
          envelope,
          value: { ...countValue, context: { ...countValue.context, registry_version: 4 } },
          authorization_revision: 1,
        }),
    }),
  );
  return Effect.gen(function* () {
    expect(
      yield* (yield* Member.MemberReads).read({ route: "counts", params: {} }).pipe(Effect.flip),
    ).toMatchObject({ code: "invalid_response" });
  }).pipe(Effect.provide(Member.layer.pipe(Layer.provide(ports))));
});

it.effect("rejects a placement selector on another route before contacting a peer", () =>
  Effect.gen(function* () {
    expect(
      yield* (yield* Member.MemberReads)
        .read({ route: "counts", params: {} }, "workstreams-t3-placement/2.0.0")
        .pipe(Effect.flip),
    ).toMatchObject({ code: "invalid_request" });
  }).pipe(Effect.provide(Member.layer)),
);

it.effect.each(["revision", "principal"] as const)(
  "fences changed current member %s after one synthetic peer call",
  (drift) => {
    let calls = 0;
    const ports = Layer.mergeAll(
      Layer.succeed(Member.MemberReadAdmission, {
        current: () =>
          Effect.succeed({
            expected_home: target,
            principal_id: calls && drift === "principal" ? "other:principal" : "principal:test",
            authorization_revision: calls && drift === "revision" ? 2 : 1,
            check: () => Effect.void,
          }),
      }),
      Layer.succeed(Member.HomeReadPort, {
        read: () =>
          Effect.sync(() => {
            calls++;
            return {
              route: "counts" as const,
              envelope,
              authorization_revision: 1,
              value: countValue,
            };
          }),
      }),
    );
    return Effect.gen(function* () {
      expect(
        yield* (yield* Member.MemberReads).read({ route: "counts", params: {} }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect(calls).toBe(1);
    }).pipe(Effect.provide(Member.layer.pipe(Layer.provide(ports))));
  },
);
