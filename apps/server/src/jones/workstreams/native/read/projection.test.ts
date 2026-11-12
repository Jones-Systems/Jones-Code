// @effect-diagnostics nodeBuiltinImport:off -- Synthetic in-memory encrypted stores and exact emitted byte assertions.
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../../jones/persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../cli.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Projection from "./projection.ts";
import * as IdentityIndex from "../crypto/identityIndex.ts";
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

it.effect(
  "reads one encrypted home snapshot with native evidence, membership, appearance, lifecycle and coherent counts",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const cp = vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport");
      try {
        const h = yield* commands,
          id = yield* h.create();
        yield* nativeReference(h, id);
        yield* (yield* Appearance.Appearance).save(caller, {
          command_id: "read-appearance-command-00000001",
          expected_server_generation: 1,
          workstream_id: id,
          expected_version: 0,
          border_color: "#123456",
        });
        const snapshot = yield* (yield* Projection.Projection).capture(caller);
        expect(snapshot.items).toMatchObject([
          {
            workstream: { workstream_id: id, name: "Synthetic private name" },
            active_memberships: { primary: 1, secondary: 0 },
            appearance: { border_color: "#123456", version: 1 },
            lifecycle: { new_lifecycle: "active" },
          },
        ]);
        expect(snapshot.envelope).toMatchObject({
          registry: {
            ...Fixture.scope,
            server_generation: 1,
            registry_version: snapshot.counts.context.registry_version,
          },
          provenance: {
            authority: "native-home",
            source: "live",
            stale: false,
            observed_at: snapshot.counts.observed_at,
          },
          coverage: {
            registry: "complete",
            sources: [
              {
                source_instance_id: "home:test",
                mapping: "mapped",
                attestation: "current",
                observed_at: Fixture.now,
              },
            ],
          },
        });
        expect(snapshot.counts).toMatchObject({ active: 1, total: 1, unknown_lifecycle: 0 });
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          envelope: string;
        }>`SELECT envelope FROM jones_workstreams_records`;
        expect(rows[0]!.envelope).not.toContain("Synthetic private name");
        expect(cp).not.toHaveBeenCalled();
      } finally {
        cp.mockRestore();
      }
    }).pipe(Effect.provide(base())),
);

it.effect(
  "expires evidence honestly and does not manufacture native evidence for a legacy-shaped registration",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands,
        id = yield* h.create();
      const ref = yield* nativeReference(h, id);
      const projection = yield* Projection.Projection,
        store = yield* Store.RegistryStore;
      const rows = yield* store.list(Fixture.scope, "references"),
        row = rows.find((r) => r.metadata.native_reference_id === ref)!;
      const { native_attestation: _marker, ...legacy } = row.value as Record<string, unknown>;
      yield* store.put(
        Fixture.scope,
        "references",
        {
          metadata: { ...row.metadata, attestation_version: 2 },
          value: {
            ...legacy,
            registration: {
              ...(legacy.registration as Wire.NativeReference["registration"]),
              attestation_version: 2,
            },
          },
        },
        1,
      );
      expect((yield* projection.capture(caller)).envelope.coverage.sources[0]).toMatchObject({
        mapping: "mapped",
        attestation: "unknown",
        observed_at: null,
      });
      yield* store.put(
        Fixture.scope,
        "references",
        {
          metadata: { ...row.metadata, attestation_version: 3 },
          value: {
            ...(row.value as Wire.NativeReference),
            registration: {
              ...(row.value as Wire.NativeReference).registration,
              attestation_version: 3,
            },
          },
        },
        2,
      );
      yield* TestClock.setTime(Date.parse("2026-01-01T00:06:00Z"));
      expect((yield* projection.capture(caller)).envelope.coverage.sources[0]).toMatchObject({
        attestation: "expired",
        observed_at: null,
      });
    }).pipe(Effect.provide(base())),
);

it.effect(
  "keeps orphan sources unmapped and partitions unverified references by their creator",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands;
      yield* h.create();
      const register = yield* h.submit({
        operation: "register_reference",
        identity: {
          provider: "t3",
          source_instance_id: "orphan:source",
          resource_kind: "thread",
          id_kind: "internal",
          native_id: "orphan:thread",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: null,
      });
      expect(register.receipt.state).toBe("committed");
      const projection = yield* Projection.Projection;
      expect((yield* projection.capture(caller)).envelope.coverage.sources).toEqual([
        {
          source_instance_id: "orphan:source",
          mapping: "unmapped",
          attestation: "unknown",
          observed_at: null,
        },
      ]);
      const store = yield* Store.RegistryStore,
        row = (yield* store.list(Fixture.scope, "references"))[0]!;
      const other = {
        ...(row.value as Wire.NativeReference),
        native_reference_id: "reference:other",
        identity: {
          ...(row.value as Wire.NativeReference).identity,
          source_instance_id: "source:private",
          native_id: "private:thread",
        },
        created_by: { principal_id: "principal:other" },
      };
      const index = yield* IdentityIndex.WorkstreamsIdentityIndex;
      yield* store.put(
        Fixture.scope,
        "references",
        {
          metadata: {
            ...row.metadata,
            native_reference_id: other.native_reference_id,
            created_by_principal: "principal:other",
            identity_index: yield* index.digest(Fixture.scope, other.identity),
          },
          value: other,
        },
        null,
      );
      expect((yield* projection.capture(caller)).envelope.coverage.sources).toEqual([
        {
          source_instance_id: "orphan:source",
          mapping: "unmapped",
          attestation: "unknown",
          observed_at: null,
        },
      ]);
      expect(
        yield* projection.capture({ ...caller, principal_id: "principal:other" }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }).pipe(Effect.provide(base())),
);

it.effect("fences projection cursors by route, owner, generation, version and page size", () =>
  Effect.gen(function* () {
    yield* initialize;
    const h = yield* commands;
    yield* h.create("one");
    yield* h.create("two");
    const projection = yield* Projection.Projection,
      first = yield* projection.page(caller, { limit: 1 });
    expect(first.items).toHaveLength(1);
    expect(first.next_cursor).not.toBeNull();
    const query = { limit: 1, cursor: first.next_cursor! };
    const second = yield* projection.page(caller, query);
    expect(second.items).toHaveLength(1);
    expect(second.items[0]!.workstream.workstream_id).not.toBe(
      first.items[0]!.workstream.workstream_id,
    );
    expect(yield* projection.page(caller, { ...query, limit: 2 }).pipe(Effect.flip)).toMatchObject({
      code: "cursor_invalid",
    });
    yield* h.create("three");
    expect(yield* projection.page(caller, query).pipe(Effect.flip)).toMatchObject({
      code: "cursor_stale",
    });
    const s = yield* projection.capture(caller),
      store = yield* Store.RegistryStore;
    yield* store.advance(Fixture.scope, s.counts.context, { generation: true });
    expect(yield* projection.page(caller, query).pipe(Effect.flip)).toMatchObject({
      code: "generation_conflict",
    });
  }).pipe(Effect.provide(base())),
);

it.effect(
  "fails closed on missing keys and immediately revoked grants, including an empty registry",
  () => {
    const keys = Fixture.secrets();
    return Effect.gen(function* () {
      yield* initialize;
      const projection = yield* Projection.Projection;
      const s = yield* projection.capture(caller);
      keys.delete("workstreams-content-key-test-home");
      expect(yield* projection.capture(caller).pipe(Effect.flip)).toMatchObject({
        code: "key_unavailable",
      });
      keys.set("workstreams-content-key-test-home", Fixture.keyBytes);
      const store = yield* Store.RegistryStore;
      yield* store.appendPlain(Fixture.scope, "grants", {
        principal_id: caller.principal_id,
        grant_id: "grant:principal:test",
        grant_version: 2,
        permissions: '["workstreams:read"]',
        state: "revoked",
      });
      expect(yield* projection.capture(caller).pipe(Effect.flip)).toMatchObject({
        code: "forbidden",
      });
      expect(yield* projection.assertCurrent(caller, s).pipe(Effect.flip)).toMatchObject({
        code: "forbidden",
      });
    }).pipe(Effect.provide(base(keys)));
  },
);
