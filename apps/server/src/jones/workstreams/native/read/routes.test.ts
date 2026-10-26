// @effect-diagnostics nodeBuiltinImport:off -- Synthetic in-memory encrypted stores and exact emitted byte assertions.
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../../jones/persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../cli.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Projection from "./projection.ts";
import * as Appearance from "../registry/appearance.ts";
import * as Admission from "../runtime/admission.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
const home = {
  ...Fixture.scope,
  canonical_origin: "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const base = (keys = Fixture.secrets()) => {
  const registry = Registry.layer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(Fixture.testLayer({ keys })),
  );
  return Layer.mergeAll(Projection.layer, Appearance.layer, Admission.layer).pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
};
const initialize = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Fixture.now));
  yield* migration;
  const admin = yield* Admin.NativeAdmin.pipe(Effect.provide(Admin.layer));
  const { served_by_environment_id: _served, ...identity } = home;
  yield* admin.initialize({
    registry: identity,
    principal_id: caller.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
});
const commands = Effect.gen(function* () {
  const registry = yield* Registry.Registry;
  let sequence = 0;
  const submit = Effect.fn("ReadTest.submit")(function* (action: Wire.WorkstreamCommand["action"]) {
    const s = yield* registry.snapshot(caller);
    return yield* registry.submit(
      caller,
      JSON.stringify({
        command_id: "read-command-" + String(++sequence).padStart(20, "0"),
        expected_server_generation: s.context.server_generation,
        expected_registry_version: s.context.registry_version,
        action,
      }),
    );
  });
  const create = (name = "Synthetic private name") =>
    submit({
      operation: "create_workstream",
      name,
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    }).pipe(
      Effect.map((accepted) => {
        if (accepted.receipt.state !== "committed") throw new Error("create rejected");
        return accepted.receipt.effects.workstream_versions[0]!.workstream_id;
      }),
    );
  return { registry, submit, create };
});
const nativeReference = Effect.fn("ReadTest.nativeReference")(function* (
  h: Effect.Success<typeof commands>,
  workstream: string,
  nativeId = "thread:test",
  source = "home:test",
) {
  const store = yield* Store.RegistryStore;
  if (
    !(yield* store.listPlain(Fixture.scope, "source_bindings")).some(
      (b) => b.source_instance_id === source,
    )
  )
    yield* store.appendPlain(Fixture.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: source,
      binding_version: 1,
      authority_namespace: "authority:test",
      store_generation: 1,
      state: "active",
    });
  const registered = yield* h.submit({
    operation: "register_reference",
    identity: {
      provider: "t3",
      source_instance_id: source,
      resource_kind: "thread",
      id_kind: "internal",
      native_id: nativeId,
      account_provenance: { kind: "not_account_scoped" },
    },
    pr_locator: null,
  });
  if (registered.receipt.state !== "committed") throw new Error("register rejected");
  const id = registered.receipt.effects.native_reference_id!;
  const verification = yield* h.submit({
    operation: "verify_reference",
    native_reference_id: id,
    expected_attestation_version: 0,
  });
  if (!verification.intent) throw new Error("missing external verification intent");
  const receipt = yield* h.registry.complete(verification.intent, {
    registration: {
      state: "attested",
      attestation_version: 1,
      attested_at: Fixture.now,
      expires_at: "2026-01-01T00:05:00.000Z",
      evidence: {
        provider: "t3",
        source_instance_id: source,
        authority_namespace: "authority:test",
        native_id: nativeId,
        store_generation: 1,
        evidence_sha256: Fixture.hash,
      },
    },
  });
  expect(receipt.state).toBe("committed");
  const snapshot = yield* h.registry.snapshot(caller);
  const attached = yield* h.submit({
    operation: "attach_primary",
    workstream_id: workstream,
    expected_version: snapshot.state.workstreams.get(workstream)!.version,
    native_reference_id: id,
  });
  expect(attached.receipt.state).toBe("committed");
  return id;
});

import * as Routes from "./routes.ts";
import { ProviderError } from "../providers/localAttestation.ts";
import * as Local from "../registry/localTransport.ts";
import * as Counts from "../registry/counts.ts";
import * as Placements from "../registry/placements.ts";
import * as Runner from "../registry/effectRunner.ts";
import * as Schema from "effect/Schema";
const decodeWorkstreamPage = Schema.decodeUnknownEffect(Wire.WorkstreamPage);
const decodeReferencePage = Schema.decodeUnknownEffect(Wire.WorkstreamReferencePage);
const decodeHistoryPage = Schema.decodeUnknownEffect(Wire.WorkstreamHistoryPage);
const localLayer = Local.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Counts.layer, Placements.layer)),
  Layer.provideMerge(base()),
  Layer.provide(
    Layer.succeed(Runner.EffectRunner, {
      run: () => Effect.die("Unexpected provider call"),
      recover: () => Effect.die("Unexpected provider recovery"),
      recoverOne: () => Effect.die("Unexpected provider lookup"),
    }),
  ),
  Layer.provide(
    Layer.succeed(Placements.PlacementTrust, {
      readTrustSnapshot: () => ({
        readiness: "ready",
        trustedEnvironments: [
          {
            environmentId: home.home_environment_id,
            authorityNamespace: "authority:test",
            storeGeneration: 1,
          },
        ],
      }),
    }),
  ),
);
const services = Routes.authorizedLayer.pipe(
  Layer.provideMerge(Routes.layer),
  Layer.provideMerge(localLayer),
  Layer.provide(
    Layer.succeed(Admission.CallerBoundary, {
      current: () =>
        Effect.succeed({
          origin: { kind: "app" },
          check: () => Effect.void,
        }),
    }),
  ),
);
const read = (request: Parameters<Routes.AuthorizedReads["Service"]["read"]>[0]) =>
  Effect.flatMap(Routes.AuthorizedReads, (port) => port.read(request));
it.effect(
  "serves v1 complete pages, native counts, appearance and lifecycle from the home without CP calls",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const cp = vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport");
      try {
        const h = yield* commands,
          id = yield* h.create();
        yield* nativeReference(h, id);
        const workstreams = yield* read({ route: "workstreams", params: {} });
        expect(yield* decodeWorkstreamPage(workstreams.value)).toMatchObject({
          items: [{ workstream_id: id }],
        });
        expect(
          (yield* read({ route: "workstream", params: { workstream_id: id } })).value,
        ).toMatchObject({ workstream: { workstream_id: id } });
        const references = yield* read({ route: "references", params: {} });
        const referencePage = yield* decodeReferencePage(references.value);
        expect(referencePage.items).toHaveLength(1);
        expect(
          (yield* read({
            route: "reference",
            params: { native_reference_id: referencePage.items[0]!.native_reference_id },
          })).value,
        ).toMatchObject({
          reference: { identity: { native_id: "thread:test" } },
        });
        expect(
          (yield* read({ route: "memberships", params: { workstream_id: id } })).value,
        ).toMatchObject({ items: [{ kind: "primary", closed: null }] });
        expect(
          (yield* read({ route: "declarations", params: { workstream_id: id } })).value,
        ).toMatchObject({ items: [], next_cursor: null });
        expect(
          (yield* read({ route: "edges", params: { workstream_id: id } })).value,
        ).toMatchObject({ items: [], next_cursor: null });
        expect(
          (yield* read({
            route: "placements",
            params: {
              identities: [
                { source_instance_id: home.home_environment_id, native_thread_id: "thread:test" },
              ],
            },
          })).value,
        ).toMatchObject({
          items: [{ workstream_id: id, native_thread_id: "thread:test" }],
          next_cursor: null,
        });
        const counts = yield* read({ route: "counts", params: {} });
        expect(counts.value).toMatchObject({
          active: 1,
          total: 1,
          observed_at: counts.envelope.provenance.observed_at,
          context: { registry_version: counts.envelope.registry.registry_version },
        });
        expect(counts.authorization_revision).toBe(1);
        expect(
          (yield* read({ route: "appearance", params: { workstream_ids: [id] } })).value,
        ).toMatchObject({
          supported: true,
          page: { items: [{ workstream_id: id, border_color: null }] },
        });
        expect(
          (yield* read({ route: "lifecycle-declarations", params: { workstream_id: id } })).value,
        ).toMatchObject({
          items: [{ new_lifecycle: "active" }],
          next_cursor: null,
        });
        expect((yield* read({ route: "read-projection", params: {} })).value).toMatchObject({
          items: [{ active_memberships: { primary: 1, secondary: 0 } }],
          next_cursor: null,
        });
        expect(
          yield* read({ route: "rollup-summary", params: { workstream_id: id } }).pipe(Effect.flip),
        ).toMatchObject({ code: "provider_unconfigured" });
        expect(Routes.responseHeaders).toEqual({
          "cache-control": "private, no-store",
          "x-content-type-options": "nosniff",
        });
        expect(cp).not.toHaveBeenCalled();
      } finally {
        cp.mockRestore();
      }
    }).pipe(Effect.provide(services)),
);

it.effect(
  "retains bytewise history order and the high-water fence for later rejected events at the same registry version",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands,
        id = yield* h.create("first");
      yield* h.create("second");
      const workstreamPage = yield* decodeWorkstreamPage(
        (yield* read({ route: "workstreams", params: { limit: 1 } })).value,
      );
      expect(workstreamPage.next_cursor).not.toBeNull();
      expect(
        yield* read({
          route: "references",
          params: { limit: 1, cursor: workstreamPage.next_cursor! },
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "cursor_invalid" });
      const before = yield* read({ route: "history", params: { limit: 100 } });
      const expected = yield* decodeHistoryPage(before.value);
      const first = yield* read({ route: "history", params: { limit: 1 } });
      const page = yield* decodeHistoryPage(first.value);
      expect(page.next_cursor).not.toBeNull();
      const prior = yield* h.registry.snapshot(caller);
      const rejected = yield* h.submit({
        operation: "update_workstream",
        workstream_id: id,
        expected_version: 99,
        name: "rejected",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      expect(rejected.receipt.state).toBe("rejected");
      expect((yield* h.registry.snapshot(caller)).context.registry_version).toBe(
        prior.context.registry_version,
      );
      const seen = [...page.items];
      let cursor = page.next_cursor;
      while (cursor) {
        const next = yield* read({ route: "history", params: { limit: 1, cursor } });
        const decoded = yield* decodeHistoryPage(next.value);
        seen.push(...decoded.items);
        cursor = decoded.next_cursor;
      }
      expect(seen.map((e) => e.event_id)).toEqual(expected.items.map((e) => e.event_id));
      expect(
        yield* read({
          route: "read-projection",
          params: { limit: 1, cursor: page.next_cursor! },
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "cursor_invalid" });
    }).pipe(Effect.provide(services)),
);

it.effect(
  "rechecks authentic entry evidence and refuses revoked grants instead of returning observations",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const port = yield* Routes.AuthorizedReads;
      let allowed = true;
      const check = () =>
        allowed ? Effect.void : Effect.fail(new ProviderError({ code: "not_granted" }));
      const call = port
        .read({ route: "counts", params: {} })
        .pipe(Effect.provideService(Admission.RequestCaller, { origin: { kind: "app" }, check }));
      // The production boundary supplies captured evidence, with limits checked on admission and again before return.
      const boundary = Layer.fresh(Routes.authorizedLayer).pipe(
        Layer.provide(
          Layer.succeed(Admission.CallerBoundary, {
            current: () => Effect.succeed({ origin: { kind: "app" }, check }),
          }),
        ),
      );
      const guarded = yield* Routes.AuthorizedReads.pipe(Effect.provide(boundary));
      expect(
        (yield* guarded.read({ route: "counts", params: {} })).envelope.coverage.registry,
      ).toBe("complete");
      allowed = false;
      expect(yield* guarded.read({ route: "counts", params: {} }).pipe(Effect.flip)).toMatchObject({
        code: "forbidden",
      });
      const store = yield* Store.RegistryStore;
      yield* store.appendPlain(Fixture.scope, "grants", {
        principal_id: caller.principal_id,
        grant_id: "grant:principal:test",
        grant_version: 2,
        permissions: '["workstreams:read"]',
        state: "revoked",
      });
      expect(yield* call.pipe(Effect.flip)).toMatchObject({ code: "forbidden" });
    }).pipe(Effect.provide(services)),
);

it.effect(
  "delegates rollup reads to the typed port and fences a changed registry before return",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands;
      const id = yield* h.create();
      let change = false;
      let calls = 0;
      const summary = {
        workstream_id: id,
        rollup_version: 1,
        manifest_sha256: Fixture.hash,
        generated_at: Fixture.now,
        output_sha256: Fixture.hash,
        result: {
          outcome: {
            text: "Synthetic outcome",
            sources: [{ source_set_id: "set:test", member_ordinal: 0 }],
          },
          semantic_status: "active" as const,
          blockers: [],
          decisions: [],
          next_steps: [],
          follow_up_candidates: [],
          confidence: 1,
        },
      };
      const port = yield* Routes.HomeReads.pipe(
        Effect.provide(
          Layer.fresh(Routes.layer).pipe(
            Layer.provide(
              Layer.succeed(Routes.RollupSummaryRead, {
                read: Effect.fn("ReadTest.rollup")(function* (admitted, workstreamId) {
                  expect(admitted).toEqual(caller);
                  expect(workstreamId).toBe(id);
                  calls++;
                  if (change) yield* h.create("Changed during port call");
                  return summary;
                }),
              }),
            ),
          ),
        ),
      );
      expect(
        (yield* port.read(caller, { route: "rollup-summary", params: { workstream_id: id } }))
          .value,
      ).toEqual(summary);
      change = true;
      expect(
        yield* port
          .read(caller, { route: "rollup-summary", params: { workstream_id: id } })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "cursor_stale" });
      expect(calls).toBe(2);
    }).pipe(Effect.provide(services)),
);
