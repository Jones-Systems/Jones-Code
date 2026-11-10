// @effect-diagnostics nodeBuiltinImport:off -- Synthetic exact inventory hashes match the frozen placement protocol.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Wire from "@t3tools/contracts";
import * as Native from "@t3tools/contracts/jones/workstreamsNativePlacement";
import type { WorkstreamsHomeAccessResult } from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Application from "../runtime/memberApplication.ts";
import { workstreamCapabilities } from "../registry/capabilities.ts";
import type { ReadResult } from "./routes.ts";
import * as Placements from "./memberPlacements.ts";

const now = "2026-01-01T00:00:00.000Z";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  canonical_origin: "https://home.invalid",
  home_environment_id: "home:test",
  server_generation: 1,
  registry_version: 9,
};
const context = {
  owner_id: home.owner_id,
  principal_id: "principal:test",
  authorization_revision: 4,
  server_generation: 1,
  registry_version: 9,
};
const identities = [{ source_instance_id: "source:test", native_thread_id: "thread:one" }];
const digest = NodeCrypto.createHash("sha256")
  .update(Wire.t3PlacementInventoryJson(identities))
  .digest("hex");
const trust = {
  readiness: "ready" as const,
  trustedEnvironments: [
    { environmentId: "source:test", authorityNamespace: "authority:local", storeGeneration: 1 },
  ],
};
const observation = (): WorkstreamsHomeAccessResult => ({
  contract: "workstreams-native-home-access/1.0.0",
  home,
  principal_id: context.principal_id,
  authorization_revision: 4,
  observed_at: now,
  capabilities: workstreamCapabilities(context, ["workstreams:read"]),
});
const item = (): Native.WorkstreamsNativeThreadPlacement => ({
  membership_id: "membership:one",
  workstream_id: "workstream:one",
  native_reference_id: "reference:one",
  kind: "primary",
  ...identities[0]!,
  attestation_version: 1,
  attested_at: now,
  expires_at: "2026-01-01T00:05:00.000Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
  authority_namespace: "authority:remote-cannot-choose-trust",
  store_generation: 9,
  attestation_source: "native",
  evidence_contract: "workstreams-t3-provider/2.0.0",
});
const page = (): Native.WorkstreamsNativePlacementPage => ({
  inventory_sha256: digest,
  context,
  provenance: {
    authority: "native-home",
    registry_instance_id: home.registry_instance_id,
    home_environment_id: home.home_environment_id,
    home_generation: 1,
    served_by_environment_id: home.home_environment_id,
    observed_at: now,
  },
  items: [item()],
  next_cursor: null,
});
const result = (value: Native.WorkstreamsNativePlacementPage): ReadResult => ({
  route: "placements",
  placement_contract: "workstreams-t3-placement/2.0.0",
  authorization_revision: 4,
  envelope: {
    contract: "workstreams-read/1.0.0",
    registry: {
      registry_instance_id: home.registry_instance_id,
      owner_id: home.owner_id,
      canonical_origin: home.canonical_origin,
      server_generation: 1,
      registry_version: 9,
    },
    provenance: {
      authority: "native-home",
      home_environment_id: home.home_environment_id,
      served_by_environment_id: home.home_environment_id,
      source: "live",
      observed_at: now,
      stale: false,
    },
    coverage: { registry: "complete", sources: [] },
  },
  value,
});
const fixture = (
  options: {
    reply?: (call: number) => ReadResult;
    access?: (call: number) => WorkstreamsHomeAccessResult;
    trust?: (call: number) => typeof trust;
    wait?: Effect.Effect<void>;
  } = {},
) => {
  const calls: Array<Parameters<Application.MemberApplication["Service"]["read"]>> = [];
  let accesses = 0,
    trusts = 0;
  const ports = Layer.merge(
    Layer.mock(Application.MemberApplication)({
      access: () => Effect.sync(() => options.access?.(++accesses) ?? observation()),
      read: (request, contract) =>
        Effect.gen(function* () {
          calls.push([request, contract]);
          if (options.wait) yield* options.wait;
          return options.reply?.(calls.length) ?? result(page());
        }),
    }),
    Layer.mock(Authority.NativeStoreAuthority)({
      trustProvider: { readTrustSnapshot: () => options.trust?.(++trusts) ?? trust },
    }),
  );
  return { calls, layer: Placements.layer.pipe(Layer.provide(ports)) };
};
const load = (input: Wire.T3PlacementLoadRequest = { identities }) =>
  Effect.flatMap(Placements.MemberPlacements, (service) =>
    service.load(input, "workstreams-t3-placement/2.0.0"),
  );
const atNow = TestClock.setTime(Date.parse(now));

it.effect("completes coherent native pages and retains independently provided local trust", () => {
  const f = fixture({
    reply: (n) =>
      result({
        ...page(),
        items:
          n === 1 ? [item()] : [{ ...item(), membership_id: "membership:two", kind: "secondary" }],
        next_cursor: n === 1 ? "next_cursor" : null,
      }),
  });
  return Effect.gen(function* () {
    yield* atNow;
    const value = yield* load();
    expect(value.page.items).toHaveLength(2);
    expect(value.page.next_cursor).toBeNull();
    expect(value.page.provenance).toEqual(page().provenance);
    expect(value.trustedEnvironments).toEqual(trust.trustedEnvironments);
    expect(value.trustedEnvironments[0]?.authorityNamespace).not.toBe(
      value.page.items[0]?.authority_namespace,
    );
    expect(f.calls).toEqual([
      [
        { route: "placements", params: { identities, limit: 100 } },
        "workstreams-t3-placement/2.0.0",
      ],
      [
        { route: "placements", params: { identities, limit: 100, cursor: "next_cursor" } },
        "workstreams-t3-placement/2.0.0",
      ],
    ]);
  }).pipe(Effect.provide(f.layer));
});
it.effect.each(["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/3.0.0"])(
  "refuses %s before observing the home",
  (contract) => {
    const f = fixture();
    return Effect.gen(function* () {
      expect(
        yield* (yield* Placements.MemberPlacements)
          .load({ identities }, contract)
          .pipe(Effect.flip),
      ).toMatchObject({ code: "contract_mismatch" });
      expect(f.calls).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("rejects excess request fields without allowing a client-selected route", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const input = { identities, route: "metadata" };
    expect(yield* load(input).pipe(Effect.flip)).toMatchObject({ code: "invalid_request" });
    expect(f.calls).toHaveLength(0);
  }).pipe(Effect.provide(f.layer));
});
it.effect.each([
  "inventory",
  "principal",
  "home",
  "served",
  "proof",
  "foreign",
  "future",
  "expired",
  "duplicate",
] as const)("rejects %s substitution without releasing a partial result", (fault) => {
  const f = fixture({
    reply: () => {
      const p = page();
      if (fault === "inventory") return result({ ...p, inventory_sha256: "b".repeat(64) });
      if (fault === "principal")
        return result({ ...p, context: { ...context, principal_id: "another" } });
      if (fault === "home")
        return result({
          ...p,
          provenance: {
            ...p.provenance,
            registry_instance_id: "22222222-2222-4222-8222-222222222222",
          },
        });
      if (fault === "served")
        return result({
          ...p,
          provenance: { ...p.provenance, served_by_environment_id: "member:test" },
        });
      if (fault === "proof")
        return {
          ...result(p),
          value: {
            ...p,
            items: [{ ...item(), evidence_contract: "workstreams-t3-provider/1.0.0" }],
          },
        };
      if (fault === "foreign")
        return result({ ...p, items: [{ ...item(), native_thread_id: "thread:another" }] });
      if (fault === "future")
        return result({ ...p, items: [{ ...item(), attested_at: "2026-01-01T00:00:00.000001Z" }] });
      if (fault === "expired")
        return result({
          ...p,
          items: [{ ...item(), attested_at: "2025-12-31T23:59:00.000Z", expires_at: now }],
        });
      return result({ ...p, items: [item(), item()] });
    },
  });
  return Effect.gen(function* () {
    yield* atNow;
    expect((yield* load().pipe(Effect.result))._tag).toBe("Failure");
  }).pipe(Effect.provide(f.layer));
});
it.effect.each(["observation", "repeated_cursor", "bounded", "primary"] as const)(
  "refuses %s continuation without partial completion",
  (fault) => {
    const f = fixture({
      reply: (n) => {
        const p = page();
        if (fault === "bounded") return result({ ...p, items: [], next_cursor: "cursor_" + n });
        if (fault === "primary")
          return result({
            ...p,
            items: [{ ...item(), membership_id: "membership:" + n }],
            next_cursor: n === 1 ? "next" : null,
          });
        if (fault === "repeated_cursor") return result({ ...p, items: [], next_cursor: "repeat" });
        const observed = n === 1 ? now : "2026-01-01T00:00:01.000Z";
        const r = result({
          ...p,
          items: [],
          provenance: { ...p.provenance, observed_at: observed },
          next_cursor: n === 1 ? "next" : null,
        });
        return {
          ...r,
          envelope: {
            ...r.envelope,
            provenance: { ...r.envelope.provenance, observed_at: observed },
          },
        };
      },
    });
    return Effect.gen(function* () {
      yield* atNow;
      expect(yield* load().pipe(Effect.flip)).toMatchObject({
        code:
          fault === "observation"
            ? "cursor_stale"
            : fault === "repeated_cursor"
              ? "cursor_invalid"
              : fault === "bounded"
                ? "backpressure"
                : "invalid_response",
      });
      expect(f.calls.length).toBeLessThanOrEqual(10);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["grant", "trust", "expiry"] as const)(
  "refuses %s drift before release",
  (fault) => {
    const f = fixture({
      access: (n) =>
        n === 2 && fault === "grant"
          ? { ...observation(), authorization_revision: 5 }
          : observation(),
      trust: (n) => (n === 2 && fault === "trust" ? { ...trust, trustedEnvironments: [] } : trust),
      wait: fault === "expiry" ? TestClock.adjust(300_000) : Effect.void,
    });
    return Effect.gen(function* () {
      yield* atNow;
      expect((yield* load().pipe(Effect.result))._tag).toBe("Failure");
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("preserves supported microsecond placement precision", () => {
  const f = fixture({
    reply: () => {
      const p = page(),
        observed = "2026-01-01T00:00:00.000001Z";
      const r = result({
        ...p,
        provenance: { ...p.provenance, observed_at: observed },
        items: [{ ...item(), attested_at: observed, expires_at: "2026-01-01T00:00:00.000002Z" }],
      });
      return {
        ...r,
        envelope: {
          ...r.envelope,
          provenance: { ...r.envelope.provenance, observed_at: observed },
        },
      };
    },
  });
  return Effect.gen(function* () {
    yield* atNow;
    expect((yield* load()).page.items).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["grant", "trust"] as const)(
  "rejects in-place %s snapshot drift before release",
  (fault) => {
    const sharedAccess = { ...observation() };
    const sharedTrust = { ...trust, trustedEnvironments: [...trust.trustedEnvironments] };
    const f = fixture({
      access: (n) => {
        if (n === 2 && fault === "grant") sharedAccess.authorization_revision = 5;
        return sharedAccess;
      },
      trust: (n) => {
        if (n === 2 && fault === "trust") sharedTrust.trustedEnvironments = [];
        return sharedTrust;
      },
    });
    return Effect.gen(function* () {
      yield* atNow;
      expect(yield* load().pipe(Effect.flip)).toMatchObject({ code: "forbidden" });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("completes an empty requested inventory with the same current authority checks", () => {
  const f = fixture({
    reply: () =>
      result({
        ...page(),
        items: [],
        inventory_sha256: NodeCrypto.createHash("sha256")
          .update(Wire.t3PlacementInventoryJson([]))
          .digest("hex"),
      }),
  });
  return Effect.gen(function* () {
    yield* atNow;
    expect((yield* load({ identities: [] })).page.items).toEqual([]);
    expect(f.calls).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "internal observation retains validated source coverage without extending the public placement codec",
  () => {
    const f = fixture({
      reply: () => {
        const r = result(page());
        return {
          ...r,
          envelope: {
            ...r.envelope,
            coverage: {
              registry: "complete",
              sources: [
                {
                  source_instance_id: "source:test",
                  mapping: "mapped",
                  attestation: "current",
                  observed_at: now,
                },
              ],
            },
          },
        };
      },
    });
    return Effect.gen(function* () {
      yield* atNow;
      const service = yield* Placements.MemberPlacements;
      const observed = yield* service.observe({ identities }, "workstreams-t3-placement/2.0.0");
      expect(observed.envelope.coverage.sources).toEqual([
        {
          source_instance_id: "source:test",
          mapping: "mapped",
          attestation: "current",
          observed_at: now,
        },
      ]);
      expect(yield* service.load({ identities }, "workstreams-t3-placement/2.0.0")).toEqual(
        observed.result,
      );
      expect(Object.keys(observed.result).toSorted()).toEqual([
        "page",
        "readiness",
        "trustedEnvironments",
      ]);
    }).pipe(Effect.provide(f.layer));
  },
);
