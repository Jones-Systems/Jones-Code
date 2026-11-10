// @effect-diagnostics nodeBuiltinImport:off -- Synthetic request inventory digests; no live peers.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT as contract,
  WorkstreamsHomeReadResult,
  WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import type {
  WorkstreamsNativeHomeRead,
  WorkstreamsNativeReadEnvelope,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Protocol from "./homeReadProtocol.ts";
import * as Routes from "../read/routes.ts";
import * as Member from "../read/memberRead.ts";
import { RegistryError } from "../registry/session.ts";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const caller = {
  principal_id: "principal:test",
  source_environment_id: "member:test",
  binding_version: 1,
  origin: { kind: "app" as const },
};
// Admission, peer client and HomeReads below are synthetic boundary fixtures.
// Real encrypted HomeReads composition is exercised separately in routes.test.ts.
const envelope: WorkstreamsNativeReadEnvelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: home.registry_instance_id,
    owner_id: home.owner_id,
    server_generation: home.server_generation,
    canonical_origin: "https://native.workstreams.invalid",
    registry_version: 3,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: home.home_environment_id,
    served_by_environment_id: home.home_environment_id,
    source: "live",
    observed_at: "2026-01-01T00:00:00Z",
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const context = { owner_id: home.owner_id, server_generation: 1, registry_version: 3 };
const count = {
  context,
  principal_id: caller.principal_id,
  observed_at: envelope.provenance.observed_at,
  authority_effect: "none",
  active: 1,
  total: 1,
  unknown_lifecycle: 0,
};
const counts: WorkstreamsNativeHomeRead = { route: "counts", params: {} };
const identities = [{ source_instance_id: "member:test", native_thread_id: "thread:test" }];
const placementRequest: WorkstreamsNativeHomeRead = { route: "placements", params: { identities } };
const placement = {
  inventory_sha256: NodeCrypto.createHash("sha256")
    .update(Wire.t3PlacementInventoryJson(identities))
    .digest("hex"),
  context: { ...context, principal_id: caller.principal_id, authorization_revision: 1 },
  items: [
    {
      membership_id: "membership:test",
      workstream_id: "workstream:test",
      native_reference_id: "reference:test",
      kind: "primary" as const,
      source_instance_id: "member:test",
      native_thread_id: "thread:test",
      attestation_version: 1,
      attested_at: "2026-01-01T00:00:00Z",
      expires_at: "2026-01-01T00:05:00Z",
      evidence_sha256: "a".repeat(64),
      source_binding_version: 1,
      authority_namespace: "authority:test",
      store_generation: 1,
    },
  ],
  next_cursor: null,
};
const provenance = {
  authority: "native-home" as const,
  registry_instance_id: home.registry_instance_id,
  home_environment_id: home.home_environment_id,
  home_generation: 1,
  served_by_environment_id: home.home_environment_id,
  observed_at: envelope.provenance.observed_at,
};
const result = (
  request: WorkstreamsNativeHomeRead = counts,
  value: unknown = count,
): Routes.ReadResult => ({ route: request.route, envelope, value, authorization_revision: 1 });

it.effect(
  "produces only the negotiated result from supplied genuine-port admission and calls no other route",
  () => {
    let checks = 0,
      reads = 0;
    const ports = Layer.mergeAll(
      Layer.succeed(Protocol.HomePeerReadAdmission, {
        current: () =>
          Effect.succeed({
            caller,
            authorization_revision: 1,
            checkCurrent: () =>
              Effect.sync(() => {
                checks++;
              }),
          }),
      }),
      Layer.succeed(Routes.HomeReads, {
        read: (admitted, request, selected) => {
          expect(admitted).toEqual(caller);
          expect(request).toEqual(counts);
          expect(selected).toBeUndefined();
          reads++;
          return Effect.succeed(result());
        },
      }),
    );
    return Effect.gen(function* () {
      const output = yield* (yield* Protocol.HomeReadProtocol).read({ contract, request: counts });
      expect(output).toEqual({ contract, ...result() });
      expect(reads).toBe(1);
      expect(checks).toBe(3);
    }).pipe(Effect.provide(Protocol.layer.pipe(Layer.provide(ports))));
  },
);

it.effect.each(["revision", "caller", "revoked"] as const)(
  "fences home %s drift during a read",
  (drift) => {
    let done = false;
    const ports = Layer.mergeAll(
      Layer.succeed(Protocol.HomePeerReadAdmission, {
        current: () =>
          Effect.succeed({
            caller: done && drift === "caller" ? { ...caller, binding_version: 2 } : caller,
            authorization_revision: done && drift === "revision" ? 2 : 1,
            checkCurrent: () =>
              done && drift === "revoked"
                ? Effect.fail(new RegistryError({ code: "forbidden" }))
                : Effect.void,
          }),
      }),
      Layer.succeed(Routes.HomeReads, {
        read: () =>
          Effect.sync(() => {
            done = true;
            return result();
          }),
      }),
    );
    return Effect.gen(function* () {
      expect(
        yield* (yield* Protocol.HomeReadProtocol)
          .read({ contract, request: counts })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }).pipe(Effect.provide(Protocol.layer.pipe(Layer.provide(ports))));
  },
);

it.effect.each(["bare", "unknown-field", "wrong-route", "bad-context", "bad-coverage"] as const)(
  "refuses %s peer data without retries or fallback",
  (fault) => {
    let calls = 0;
    const raw =
      fault === "bare"
        ? count
        : {
            contract,
            ...result(),
            ...(fault === "unknown-field" ? { unexpected: true } : {}),
            ...(fault === "wrong-route"
              ? { route: "read-projection", value: { items: [], next_cursor: null } }
              : {}),
            ...(fault === "bad-context"
              ? { value: { ...count, context: { ...context, registry_version: 4 } } }
              : {}),
            ...(fault === "bad-coverage"
              ? { envelope: { ...envelope, coverage: { registry: "unavailable", sources: [] } } }
              : {}),
          };
    const ports = Protocol.homeReadPortLayer.pipe(
      Layer.provide(
        Layer.succeed(Protocol.HomeReadResultClient, {
          read: (target, wrapped) => {
            calls++;
            expect(target).toEqual(home);
            expect(wrapped).toEqual({ contract, request: counts });
            return Effect.succeed(raw);
          },
        }),
      ),
    );
    return Effect.gen(function* () {
      expect(yield* (yield* Member.MemberReads).read(counts).pipe(Effect.flip)).toMatchObject({
        code: fault === "bad-coverage" ? "unavailable" : "invalid_response",
      });
      expect(calls).toBe(1);
    }).pipe(
      Effect.provide(
        Member.layer.pipe(
          Layer.provide(ports),
          Layer.provide(
            Layer.succeed(Member.MemberReadAdmission, {
              current: () =>
                Effect.succeed({
                  expected_home: home,
                  principal_id: caller.principal_id,
                  authorization_revision: 1,
                  check: () => Effect.void,
                }),
            }),
          ),
        ),
      ),
    );
  },
);

it.effect.each(["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"] as const)(
  "roundtrips explicit %s and preserves the selected evidence",
  (selected) => {
    let calls = 0;
    const value = selected.endsWith("/2.0.0")
      ? {
          ...placement,
          provenance,
          items: placement.items.map((item) => ({
            ...item,
            attestation_source: "native",
            evidence_contract: "workstreams-t3-provider/2.0.0",
          })),
        }
      : placement;
    const port = Protocol.homeReadPortLayer.pipe(
      Layer.provide(
        Layer.succeed(Protocol.HomeReadResultClient, {
          read: (_target, request) => {
            calls++;
            expect(request.placement_contract).toBe(selected);
            return Effect.succeed({
              contract,
              ...result(placementRequest, value),
              placement_contract: selected,
            });
          },
        }),
      ),
    );
    return Effect.gen(function* () {
      const output = yield* (yield* Member.MemberReads).read(placementRequest, selected);
      expect(output.value).toEqual(value);
      expect(output.placement_contract).toBe(selected);
      expect(calls).toBe(1);
    }).pipe(
      Effect.provide(
        Member.layer.pipe(
          Layer.provide(port),
          Layer.provide(
            Layer.succeed(Member.MemberReadAdmission, {
              current: () =>
                Effect.succeed({
                  expected_home: home,
                  principal_id: caller.principal_id,
                  authorization_revision: 1,
                  check: () => Effect.void,
                }),
            }),
          ),
        ),
      ),
    );
  },
);

it.effect.each([
  "mixed-v1",
  "mixed-v2",
  "selection",
  "inventory",
  "generation",
  "grant",
  "native-home",
  "foreign-item",
] as const)("rejects placement %s before exposing a page", (fault) => {
  const v2 = {
    ...placement,
    provenance: {
      ...provenance,
      ...(fault === "generation" ? { home_generation: 2 } : {}),
      ...(fault === "native-home" ? { home_environment_id: "other:home" } : {}),
    },
    ...(fault === "inventory" ? { inventory_sha256: "b".repeat(64) } : {}),
    context: { ...placement.context, ...(fault === "grant" ? { authorization_revision: 2 } : {}) },
    items: placement.items.map((item) => ({
      ...item,
      ...(fault === "foreign-item" ? { native_thread_id: "other:thread" } : {}),
      attestation_source: "native",
      evidence_contract: "workstreams-t3-provider/2.0.0",
    })),
  };
  const selected =
    fault === "mixed-v1" ? "workstreams-t3-placement/1.0.0" : "workstreams-t3-placement/2.0.0";
  const port = Protocol.homeReadPortLayer.pipe(
    Layer.provide(
      Layer.succeed(Protocol.HomeReadResultClient, {
        read: () =>
          Effect.succeed({
            contract,
            ...result(
              placementRequest,
              fault === "mixed-v2" || fault === "selection" ? placement : v2,
            ),
            placement_contract: fault === "selection" ? "workstreams-t3-placement/1.0.0" : selected,
          }),
      }),
    ),
  );
  return Effect.gen(function* () {
    expect(
      yield* (yield* Member.HomeReadPort).read(home, placementRequest, selected).pipe(Effect.flip),
    ).toMatchObject({ code: "invalid_response" });
  }).pipe(Effect.provide(port));
});

it("closes wrapper requests and preserves exact lifecycle/projection/summary codecs", () => {
  const decode = Schema.decodeUnknownSync(WorkstreamsHomeReadResultRequest, {
    onExcessProperty: "error",
  });
  expect(() =>
    decode({ contract, request: counts, placement_contract: "workstreams-t3-placement/2.0.0" }),
  ).toThrow();
  expect(() => decode({ contract, request: counts, unexpected: true })).toThrow();
  const response = Schema.decodeUnknownSync(WorkstreamsHomeReadResult, {
    onExcessProperty: "error",
  });
  expect(() =>
    response({
      contract,
      ...result(
        { route: "lifecycle-declarations", params: { workstream_id: "workstream:test" } },
        { items: [], next_cursor: null },
      ),
    }),
  ).toThrow();
  expect(
    response({
      contract,
      ...result(
        { route: "lifecycle-declarations", params: { workstream_id: "workstream:test" } },
        { context, items: [], next_cursor: null },
      ),
    }).route,
  ).toBe("lifecycle-declarations");
  expect(() =>
    response({
      contract,
      ...result(
        { route: "read-projection", params: {} },
        { context, items: [], next_cursor: null },
      ),
    }),
  ).toThrow();
  expect(() =>
    response({
      contract,
      ...result(
        { route: "rollup-summary", params: { workstream_id: "workstream:test" } },
        { state: "ready", summary: {} },
      ),
    }),
  ).toThrow();
});

it.effect("checks the placement principal inside the page against current member admission", () => {
  const port = Protocol.homeReadPortLayer.pipe(
    Layer.provide(
      Layer.succeed(Protocol.HomeReadResultClient, {
        read: () =>
          Effect.succeed({
            contract,
            ...result(placementRequest, {
              ...placement,
              context: { ...placement.context, principal_id: "other:principal" },
            }),
            placement_contract: "workstreams-t3-placement/1.0.0",
          }),
      }),
    ),
  );
  return Effect.gen(function* () {
    expect(
      yield* (yield* Member.MemberReads).read(placementRequest).pipe(Effect.flip),
    ).toMatchObject({ code: "forbidden" });
  }).pipe(
    Effect.provide(
      Member.layer.pipe(
        Layer.provide(port),
        Layer.provide(
          Layer.succeed(Member.MemberReadAdmission, {
            current: () =>
              Effect.succeed({
                expected_home: home,
                principal_id: caller.principal_id,
                authorization_revision: 1,
                check: () => Effect.void,
              }),
          }),
        ),
      ),
    ),
  );
});
