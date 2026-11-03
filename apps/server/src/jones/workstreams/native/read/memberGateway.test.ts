import { expect, it } from "@effect/vitest";
import * as Wire from "@t3tools/contracts";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import type { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as App from "../runtime/memberApplication.ts";
import { RegistryError } from "../registry/session.ts";
import { workstreamCapabilities } from "../registry/capabilities.ts";
import type { ReadResult } from "./routes.ts";
import * as MemberGateway from "./memberGateway.ts";

// Application ports are synthetic here; the combined H/M suite exercises original auth and the real peer.
const isBinding = Schema.is(Wire.T3WorkstreamBinding);
const isListResult = Schema.is(Wire.T3WorkstreamListResult);
const now = "2026-01-01T00:00:00.000Z";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  canonical_origin: "https://home.invalid",
  home_environment_id: "home:test",
  server_generation: 1,
  registry_version: 9,
};
const context = { owner_id: home.owner_id, server_generation: 1, registry_version: 9 };
const observation = (): Access.WorkstreamsHomeAccessResult => ({
  contract: Access.WORKSTREAMS_HOME_ACCESS_CONTRACT,
  home,
  principal_id: "principal:test",
  authorization_revision: 4,
  observed_at: now,
  capabilities: workstreamCapabilities(context, ["workstreams:read", "workstreams:write"]),
});
const workstream: Wire.Workstream = {
  workstream_id: "workstream:test",
  owner_id: home.owner_id,
  name: "Synthetic member workstream",
  lifecycle: "active",
  progress: { state: "unknown" },
  delivery: "unknown",
  freshness: "unknown",
  sort_order: 0,
  version: 1,
  created_at: now,
  updated_at: now,
  created_by: { principal_id: "principal:test" },
  updated_by: { principal_id: "principal:test" },
};
const envelope = (): WorkstreamsNativeReadEnvelope => {
  const { home_environment_id: _home, ...registry } = home;
  return {
    contract: "workstreams-read/1.0.0",
    registry,
    provenance: {
      authority: "native-home",
      home_environment_id: home.home_environment_id,
      served_by_environment_id: home.home_environment_id,
      source: "live",
      observed_at: now,
      stale: false,
    },
    coverage: { registry: "complete", sources: [] },
  };
};
const fixture = (
  options: {
    alterAccess?: (
      value: Access.WorkstreamsHomeAccessResult,
      call: number,
    ) => Access.WorkstreamsHomeAccessResult;
    alterRead?: (value: ReadResult) => ReadResult;
    fail?: string;
    beforeRead?: Effect.Effect<void>;
  } = {},
) => {
  const reads: Array<Parameters<App.MemberApplication["Service"]["read"]>> = [];
  let accesses = 0;
  const ports = Layer.mock(App.MemberApplication)({
    access: () =>
      Effect.sync(() => {
        accesses++;
        return options.alterAccess?.(observation(), accesses) ?? observation();
      }),
    read: (request, placement) =>
      Effect.gen(function* () {
        reads.push([request, placement]);
        if (options.beforeRead) yield* options.beforeRead;
        if (options.fail)
          return yield* new RegistryError({ code: options.fail, cause: "PRIVATE_SENTINEL" });
        let value: unknown = { context, items: [], next_cursor: null };
        if (request.route === "workstreams")
          value = {
            context,
            items: [workstream],
            next_cursor: request.params.cursor ? null : "next_cursor",
          };
        if (request.route === "workstream") value = { context, workstream };
        if (request.route === "counts")
          value = {
            context,
            principal_id: "principal:test",
            observed_at: now,
            authority_effect: "none",
            total: 1,
            active: 1,
            unknown_lifecycle: 0,
          };
        if (request.route === "appearance")
          value = {
            supported: true,
            page: {
              owner_id: home.owner_id,
              server_generation: 1,
              permissions: ["workstreams:read", "workstreams:write"],
              items: request.params.workstream_ids.map((workstream_id) => ({
                workstream_id,
                border_color: null,
                version: 0,
              })),
            },
          };
        if (request.route === "placements")
          value = {
            context: { ...context, principal_id: "principal:test", authorization_revision: 4 },
            inventory_sha256: "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
            items: [],
            next_cursor: null,
          };
        const result: ReadResult = {
          route: request.route,
          value,
          envelope: envelope(),
          authorization_revision: 4,
          ...(request.route === "placements" && placement !== undefined
            ? { placement_contract: placement }
            : {}),
        };
        return options.alterRead?.(result) ?? result;
      }),
  });
  return { layer: MemberGateway.layer.pipe(Layer.provide(ports)), reads, accesses: () => accesses };
};

it.effect(
  "serves fresh read-only sessions and schema-valid metadata without retaining pages",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const gateway = yield* Gateway.WorkstreamGateway;
      const session = yield* gateway.readSession();
      expect(session).toMatchObject({
        registryId: home.canonical_origin,
        ownerId: home.owner_id,
        principalId: "principal:test",
        permissions: ["workstreams:read"],
        authorizationRevision: 4,
      });
      expect(isBinding(session)).toBe(true);
      const first = yield* gateway.readMetadata({ limit: 1 });
      expect(first).toMatchObject({
        source: "live",
        stale: false,
        items: [{ workstreamId: workstream.workstream_id }],
        nextCursor: "next_cursor",
      });
      expect(isListResult(first)).toBe(true);
      const next = yield* gateway.readMetadata({ limit: 1, cursor: first.nextCursor! });
      expect(next.nextCursor).toBeNull();
      expect(f.reads.map(([request]) => request)).toEqual([
        { route: "workstreams", params: { limit: 1 } },
        { route: "workstreams", params: { limit: 1, cursor: "next_cursor" } },
      ]);
      expect(f.accesses()).toBe(6);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "maps detail, counts, appearance and collection reads through existing closed validators",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const gateway = yield* Gateway.WorkstreamGateway;
      expect((yield* gateway.readDetail(workstream.workstream_id)).workstream).toEqual(workstream);
      expect(yield* gateway.readRegistryCounts()).toMatchObject({
        counts: { total: 1 },
        binding: { authorizationRevision: 4 },
      });
      expect(yield* gateway.readAppearance([workstream.workstream_id])).toMatchObject({
        supported: true,
        page: {
          permissions: ["workstreams:read"],
          items: [{ workstream_id: workstream.workstream_id }],
        },
      });
      for (const effect of [
        gateway.readReferences({ limit: 1, cursor: "page_two" }),
        gateway.readMemberships(workstream.workstream_id),
        gateway.readDeclarations(workstream.workstream_id),
        gateway.readEdges(workstream.workstream_id),
        gateway.readHistory(workstream.workstream_id),
        gateway.readHistory(undefined),
      ])
        expect((yield* effect).items).toEqual([]);
      expect(f.reads.at(-1)?.[0]).toEqual({ route: "history", params: { limit: 50 } });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("negotiates frozen placement v1 without manufacturing trust or absence authority", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    expect(yield* gateway.readThreadPlacements({ identities: [] })).toMatchObject({
      page: { items: [], next_cursor: null },
      trustedEnvironments: [],
      readiness: "trust-provider-required",
    });
    expect(f.reads[0]?.[1]).toBe("workstreams-t3-placement/1.0.0");
  }).pipe(Effect.provide(f.layer));
});
it.effect.each([
  [
    "another home",
    (r: ReadResult) => ({
      ...r,
      envelope: {
        ...r.envelope,
        registry: {
          ...r.envelope.registry,
          registry_instance_id: "22222222-2222-4222-8222-222222222222",
        },
      },
    }),
  ],
  [
    "another origin",
    (r: ReadResult) => ({
      ...r,
      envelope: {
        ...r.envelope,
        registry: { ...r.envelope.registry, canonical_origin: "https://other.invalid" },
      },
    }),
  ],
  ["another grant", (r: ReadResult) => ({ ...r, authorization_revision: 5 })],
  [
    "another serving environment",
    (r: ReadResult) => ({
      ...r,
      envelope: {
        ...r.envelope,
        provenance: { ...r.envelope.provenance, served_by_environment_id: "relay:test" },
      },
    }),
  ],
  ["another result route", (r: ReadResult) => ({ ...r, route: "references" as const })],
  [
    "extra result fields",
    (r: ReadResult) => ({
      ...r,
      value: { context, items: [], next_cursor: null, private_extra: true },
    }),
  ],
  [
    "another owner item",
    (r: ReadResult) => ({
      ...r,
      value: { context, items: [{ ...workstream, owner_id: "other" }], next_cursor: null },
    }),
  ],
] as const)("refuses %s without exposing an observation", ([_name, alterRead]) => {
  const f = fixture({ alterRead });
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    expect((yield* gateway.readMetadata().pipe(Effect.result))._tag).toBe("Failure");
  }).pipe(Effect.provide(f.layer));
});
it.effect.each([
  ["principal", (v: Access.WorkstreamsHomeAccessResult) => ({ ...v, principal_id: "replacement" })],
  ["grant", (v: Access.WorkstreamsHomeAccessResult) => ({ ...v, authorization_revision: 5 })],
  [
    "permissions",
    (v: Access.WorkstreamsHomeAccessResult) => ({
      ...v,
      capabilities: { ...v.capabilities, permissions: ["workstreams:read" as const] },
    }),
  ],
  [
    "generation",
    (v: Access.WorkstreamsHomeAccessResult) => ({
      ...v,
      home: { ...v.home, server_generation: 2 },
      capabilities: {
        ...v.capabilities,
        context: { ...v.capabilities.context, server_generation: 2 },
      },
    }),
  ],
  [
    "registry version",
    (v: Access.WorkstreamsHomeAccessResult) => ({
      ...v,
      home: { ...v.home, registry_version: 10 },
      capabilities: {
        ...v.capabilities,
        context: { ...v.capabilities.context, registry_version: 10 },
      },
    }),
  ],
] as const)("fences %s changes after the home reply", ([_name, change]) => {
  const f = fixture({ alterAccess: (v, call) => (call === 1 ? v : change(v)) });
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    expect(yield* gateway.readMetadata().pipe(Effect.flip)).toMatchObject({
      reason: "authorization-not-current",
    });
    expect(f.accesses()).toBe(2);
  }).pipe(Effect.provide(f.layer));
});
it.effect("purge fences an in-flight read and leaves a successor request usable", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>(),
      release = yield* Deferred.make<void>();
    const f = fixture({
      beforeRead: Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
      ),
    });
    yield* Effect.gen(function* () {
      const gateway = yield* Gateway.WorkstreamGateway;
      const pending = yield* gateway.readMetadata().pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      gateway.purgeAuthorization();
      yield* Deferred.succeed(release, undefined);
      const failed = yield* Fiber.join(pending);
      expect(failed._tag).toBe("Failure");
      if (failed._tag === "Failure")
        expect(failed.failure.reason).toBe("authorization-not-current");
      expect((yield* gateway.readMetadata()).source).toBe("live");
    }).pipe(Effect.provide(f.layer));
  }),
);
it.effect.each([
  ["forbidden", "authorization-not-current"],
  ["contract_mismatch", "contract-mismatch"],
  ["cursor_stale", "cursor-stale"],
  ["invalid_response", "invalid-response"],
  ["unavailable", "offline"],
] as const)("keeps %s refusals bounded and does not retry", ([code, reason]) => {
  const f = fixture({ fail: code });
  return Effect.gen(function* () {
    const error = yield* (yield* Gateway.WorkstreamGateway).readMetadata().pipe(Effect.flip);
    expect(error.reason).toBe(reason);
    expect(JSON.stringify(error)).not.toContain("PRIVATE_SENTINEL");
    expect(f.reads).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});
it.effect("leaves command recovery and mutation unavailable before any peer observation", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    const command: Wire.WorkstreamCommand = {
      command_id: "gateway-unsupported-command",
      expected_server_generation: 1,
      expected_registry_version: 9,
      action: {
        operation: "create_workstream",
        name: "Synthetic",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    };
    const attempts: readonly Effect.Effect<unknown, Gateway.WorkstreamGatewayError>[] = [
      gateway.submit(command),
      gateway.pollCommand(command.command_id),
      gateway.readReceipt(command.command_id),
      gateway.resubmit(command.command_id, "{}"),
      gateway.saveAppearance({
        command_id: command.command_id,
        workstream_id: workstream.workstream_id,
        expected_server_generation: 1,
        expected_version: 0,
        border_color: null,
      }),
    ];
    for (const effect of attempts) {
      const outcome = yield* Effect.result(effect);
      expect(outcome._tag).toBe("Failure");
      if (outcome._tag === "Failure") expect(outcome.failure.reason).toBe("not-configured");
    }
    expect(f.accesses()).toBe(0);
    expect(f.reads).toEqual([]);
  }).pipe(Effect.provide(f.layer));
});

it.effect("does not use an earlier live page when the next read fails", () => {
  const state: { fail?: string } = {};
  const f = fixture(state);
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    expect((yield* gateway.readMetadata()).source).toBe("live");
    state.fail = "unavailable";
    expect(yield* gateway.readMetadata().pipe(Effect.flip)).toMatchObject({ reason: "offline" });
    expect(f.reads).toHaveLength(2);
  }).pipe(Effect.provide(f.layer));
});
it.effect("refuses a native placement response at the frozen-v1 Gateway boundary", () => {
  const f = fixture({
    alterRead: (r) => ({ ...r, placement_contract: "workstreams-t3-placement/2.0.0" }),
  });
  return Effect.gen(function* () {
    const gateway = yield* Gateway.WorkstreamGateway;
    expect(yield* gateway.readThreadPlacements({ identities: [] }).pipe(Effect.flip)).toMatchObject(
      { reason: "invalid-response" },
    );
    expect(f.reads).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});
it.effect("refuses counts from another principal despite matching owner and grant revision", () => {
  const f = fixture({
    alterRead: (r) => ({
      ...r,
      value: {
        context,
        principal_id: "another",
        observed_at: now,
        authority_effect: "none",
        total: 1,
        active: 1,
        unknown_lifecycle: 0,
      },
    }),
  });
  return Effect.gen(function* () {
    expect(
      yield* (yield* Gateway.WorkstreamGateway).readRegistryCounts().pipe(Effect.flip),
    ).toMatchObject({ reason: "authorization-not-current" });
  }).pipe(Effect.provide(f.layer));
});
