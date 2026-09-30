import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  T3_PLACEMENT_CONTRACT,
  T3_PLACEMENT_MANIFEST_SHA256,
  T3_PLACEMENT_ROUTE,
  t3PlacementInventoryJson,
  type T3PlacementIdentity,
  type T3ThreadPlacement,
  type T3PlacementPage,
} from "@t3tools/contracts";
import {
  makeControlPlaneWorkstreamTransport,
  signWorkstreamRequest,
} from "./ControlPlaneWorkstreamTransport.ts";
import { make } from "./WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "./SyntheticWorkstreamTransport.ts";

const now = Date.parse("2026-09-12T12:00:00Z");
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const identities = [{ source_instance_id: "environment:1", native_thread_id: "thread:1" }];
const digest = (values: readonly T3PlacementIdentity[]) =>
  NodeCrypto.createHash("sha256").update(t3PlacementInventoryJson(values)).digest("hex");
const request = { identities, limit: 100 };
const page: T3PlacementPage = {
  inventory_sha256: digest(identities),
  context: {
    owner_id: "owner-fixture",
    principal_id: "principal-fixture",
    authorization_revision: 3,
    server_generation: 7,
    registry_version: 11,
  },
  items: [],
  next_cursor: null,
};
const binding = {
  registryId: "registry",
  ownerId: "owner-fixture",
  principalId: "principal-fixture",
  authorizationRevision: 3,
};
const activation = {
  state: "enabled",
  value: {
    baseUrl: new URL("https://control.example.test"),
    ...binding,
    keyId: "key-fixture",
    signingSecret: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
  },
} as const;
const headers = {
  "x-control-contract-version": T3_PLACEMENT_CONTRACT,
  "x-control-contract-manifest": T3_PLACEMENT_MANIFEST_SHA256,
};

it.effect("signs the distinct placement contract and validates the response identity", () =>
  Effect.gen(function* () {
    const transport = makeControlPlaneWorkstreamTransport(activation, async (url, init) => {
      expect(String(url)).toBe(`https://control.example.test${T3_PLACEMENT_ROUTE}`);
      expect(init?.method).toBe("POST");
      expect(init?.body).toBe(json(request));
      const sent = new Headers(init?.headers);
      expect(sent.get("x-control-contract-version")).toBe(T3_PLACEMENT_CONTRACT);
      expect(sent.get("x-control-contract-manifest")).toBe(T3_PLACEMENT_MANIFEST_SHA256);
      expect(sent.get("x-control-signature")).toBe(
        signWorkstreamRequest({
          contractVersion: T3_PLACEMENT_CONTRACT,
          requestId: sent.get("x-control-request-id")!,
          idempotencyKey: sent.get("idempotency-key")!,
          sentAt: sent.get("x-control-timestamp")!,
          nonce: sent.get("x-control-nonce")!,
          principalId: binding.principalId,
          keyId: "key-fixture",
          method: "POST",
          target: T3_PLACEMENT_ROUTE,
          contentSha256: sent.get("x-control-content-sha256")!,
          signingSecret: activation.value.signingSecret,
        }),
      );
      return new Response(json(page), { headers });
    }).transport;
    expect(yield* transport.listThreadPlacements!(request)).toEqual(page);
  }),
);

it.effect(
  "rejects wrong contract, overbound streamed bodies and unrecognized placement fields",
  () =>
    Effect.gen(function* () {
      const responses = [
        new Response(json(page)),
        new Response(json({ ...page, content: "private" }), { headers }),
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new Uint8Array(600_000));
              controller.enqueue(new Uint8Array(600_000));
              controller.close();
            },
          }),
          { headers },
        ),
      ];
      for (const response of responses) {
        const transport = makeControlPlaneWorkstreamTransport(
          activation,
          async () => response,
        ).transport;
        const result = yield* Effect.result(transport.listThreadPlacements!(request));
        expect(result._tag).toBe("Failure");
      }
    }),
);

it.effect("defaults to empty native trust and permits independent server injection", () =>
  Effect.gen(function* () {
    const transport = {
      ...makeSyntheticWorkstreamTransport(),
      listThreadPlacements: () => Effect.succeed(page),
    };
    const gateway = yield* make(transport, { binding, now: () => now });
    expect(yield* gateway.readThreadPlacements({ identities })).toEqual({
      page,
      trustedEnvironments: [],
      readiness: "trust-provider-required",
    });
    const trustedEnvironments = [
      { environmentId: "environment:1", authorityNamespace: "t3:store", storeGeneration: 9 },
    ];
    const injected = yield* make(transport, {
      binding,
      now: () => now,
      placementTrustProvider: {
        readTrustSnapshot: () => ({ trustedEnvironments, readiness: "ready" }),
      },
    });
    expect((yield* injected.readThreadPlacements({ identities })).trustedEnvironments).toEqual(
      trustedEnvironments,
    );
  }),
);

it.effect("keeps placement fail-closed when the independent native authority is fenced", () =>
  Effect.gen(function* () {
    const transport = {
      ...makeSyntheticWorkstreamTransport(),
      listThreadPlacements: () => Effect.succeed(page),
    };
    const gateway = yield* make(transport, {
      binding,
      now: () => now,
      placementTrustProvider: {
        readTrustSnapshot: () => ({
          trustedEnvironments: [],
          readiness: "trust-provider-required",
        }),
      },
    });
    const result = yield* gateway.readThreadPlacements({ identities });
    expect(result.trustedEnvironments).toEqual([]);
    expect(result.readiness).toBe("trust-provider-required");
  }),
);

it.effect("fences actual principal/grant and generation/revision values", () =>
  Effect.gen(function* () {
    for (const context of [
      { ...page.context, authorization_revision: 4 },
      { ...page.context, principal_id: "other" },
      { ...page.context, owner_id: "other" },
      { ...page.context, server_generation: 8 },
      { ...page.context, registry_version: 12 },
    ]) {
      const transport = {
        ...makeSyntheticWorkstreamTransport(),
        listThreadPlacements: () => Effect.succeed({ ...page, context }),
      };
      const gateway = yield* make(transport, { binding, now: () => now });
      expect((yield* Effect.result(gateway.readThreadPlacements({ identities })))._tag).toBe(
        "Failure",
      );
    }
  }),
);

const placement = (index: number): T3ThreadPlacement => ({
  membership_id: `membership:${String(index).padStart(5, "0")}`,
  native_reference_id: `reference:${String(index).padStart(5, "0")}`,
  workstream_id: "workstream:1",
  kind: "primary",
  source_instance_id: "environment:1",
  native_thread_id: `thread:${index}`,
  attestation_version: 1,
  attested_at: "2026-09-12T11:00:00Z",
  expires_at: "2026-09-12T13:00:00Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
  authority_namespace: "store",
  store_generation: 1,
});

it.effect(
  "loads a two-thread inventory from 20,000 owner memberships with one capability decision",
  () =>
    Effect.gen(function* () {
      const ownerRows = Array.from({ length: 20_000 }, (_, index) => placement(index));
      const current = [
        identities[0]!,
        { source_instance_id: "environment:1", native_thread_id: "thread:19999" },
      ];
      let capabilityCalls = 0;
      let pageCalls = 0;
      const selected: string[] = [];
      const base = makeSyntheticWorkstreamTransport();
      const gateway = yield* make(
        {
          ...base,
          getCapabilities: (input) => {
            capabilityCalls++;
            return base.getCapabilities(input);
          },
          listThreadPlacements: (input) => {
            pageCalls++;
            const keys = new Set(input.identities.map((value) => value.native_thread_id));
            const items = ownerRows.filter((value) => keys.has(value.native_thread_id));
            selected.push(...items.map((value) => value.native_thread_id));
            return Effect.succeed({ ...page, inventory_sha256: digest(input.identities), items });
          },
        },
        { binding, now: () => now },
      );
      const result = yield* gateway.readThreadPlacements({ identities: current });
      expect(result.page.items.map((value) => value.native_thread_id)).toEqual([
        "thread:1",
        "thread:19999",
      ]);
      expect(selected).toEqual(["thread:1", "thread:19999"]);
      expect(capabilityCalls).toBe(1);
      expect(pageCalls).toBe(1);
      expect(result.trustedEnvironments).toEqual([]);
    }),
);

it.effect(
  "bounds the entire load at ten CP page calls and never returns a partial projection",
  () =>
    Effect.gen(function* () {
      for (const complete of [true, false]) {
        let capabilityCalls = 0;
        let pageCalls = 0;
        const base = makeSyntheticWorkstreamTransport();
        const gateway = yield* make(
          {
            ...base,
            getCapabilities: (input) => {
              capabilityCalls++;
              return base.getCapabilities(input);
            },
            listThreadPlacements: () => {
              pageCalls++;
              const items = Array.from({ length: 100 }, (_, i) => ({
                ...placement((pageCalls - 1) * 100 + i),
                native_reference_id: "reference:one",
                native_thread_id: "thread:1",
                kind: "secondary" as const,
              }));
              return Effect.succeed({
                ...page,
                items,
                next_cursor: complete && pageCalls === 10 ? null : `cursor-${pageCalls}`,
              });
            },
          },
          { binding, now: () => now },
        );
        const result = yield* Effect.result(gateway.readThreadPlacements({ identities }));
        expect(result._tag).toBe(complete ? "Success" : "Failure");
        if (result._tag === "Success") expect(result.success.page.items).toHaveLength(1000);
        expect(capabilityCalls).toBe(1);
        expect(pageCalls).toBe(10);
      }
    }),
);

it.effect("rejects changed, repeated, or mixed pages after a single capability check", () =>
  Effect.gen(function* () {
    const first = { ...page, items: [placement(1)], next_cursor: "next" };
    for (const second of [
      { ...page, inventory_sha256: "b".repeat(64) },
      { ...page, context: { ...page.context, registry_version: 12 } },
      { ...page, context: { ...page.context, server_generation: 8 } },
      { ...page, context: { ...page.context, authorization_revision: 4 } },
      { ...page, context: { ...page.context, principal_id: "other" } },
      { ...page, context: { ...page.context, owner_id: "other" } },
      { ...page, next_cursor: "next" },
      { ...page, items: [placement(1)] },
      { ...page, items: [placement(2)] },
      {
        ...page,
        items: [
          { ...placement(1), membership_id: "membership:00002", authority_namespace: "different" },
        ],
      },
    ]) {
      let pageCalls = 0;
      let capabilityCalls = 0;
      const base = makeSyntheticWorkstreamTransport();
      const gateway = yield* make(
        {
          ...base,
          getCapabilities: (input) => {
            capabilityCalls++;
            return base.getCapabilities(input);
          },
          listThreadPlacements: () => Effect.succeed(pageCalls++ === 0 ? first : second),
        },
        { binding, now: () => now },
      );
      expect((yield* Effect.result(gateway.readThreadPlacements({ identities })))._tag).toBe(
        "Failure",
      );
      expect(pageCalls).toBe(2);
      expect(capabilityCalls).toBe(1);
    }
  }),
);

it.effect("rejects oversized inventories before capability or transport calls", () =>
  Effect.gen(function* () {
    for (const values of [
      Array.from({ length: 1001 }, (_, index) => ({
        source_instance_id: "env",
        native_thread_id: String(index),
      })),
      Array.from({ length: 1000 }, (_, index) => ({
        source_instance_id: "env",
        native_thread_id: `${index}:` + "界".repeat(500),
      })),
      [identities[0]!, identities[0]!],
    ]) {
      let calls = 0;
      const base = makeSyntheticWorkstreamTransport();
      const gateway = yield* make(
        {
          ...base,
          getCapabilities: (input) => {
            calls++;
            return base.getCapabilities(input);
          },
          listThreadPlacements: () => {
            calls++;
            return Effect.succeed(page);
          },
        },
        { binding, now: () => now },
      );
      expect(
        (yield* Effect.result(gateway.readThreadPlacements({ identities: values })))._tag,
      ).toBe("Failure");
      const configured = makeControlPlaneWorkstreamTransport(activation, async () => {
        calls++;
        return new Response(json(page), { headers });
      });
      expect(
        (yield* Effect.result(
          configured.transport.listThreadPlacements!({ identities: values, limit: 100 }),
        ))._tag,
      ).toBe("Failure");
      expect(calls).toBe(0);
    }
  }),
);

it.effect("fails closed before aggregate output exceeds one MiB", () =>
  Effect.gen(function* () {
    let calls = 0;
    const longId = "界".repeat(500);
    const current = [{ source_instance_id: "environment:1", native_thread_id: longId }];
    const gateway = yield* make(
      {
        ...makeSyntheticWorkstreamTransport(),
        listThreadPlacements: () => {
          calls++;
          const items = Array.from({ length: 100 }, (_, i) => ({
            ...placement((calls - 1) * 100 + i),
            native_reference_id: "reference:one",
            native_thread_id: longId,
            authority_namespace: longId,
            kind: "secondary" as const,
          }));
          return Effect.succeed({
            ...page,
            inventory_sha256: digest(current),
            items,
            next_cursor: `cursor-${calls}`,
          });
        },
      },
      { binding, now: () => now },
    );
    expect((yield* Effect.result(gateway.readThreadPlacements({ identities: current })))._tag).toBe(
      "Failure",
    );
    expect(calls).toBeLessThanOrEqual(4);
  }),
);
