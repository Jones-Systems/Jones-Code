// @effect-diagnostics nodeBuiltinImport:off -- Synthetic in-memory encrypted stores and exact emitted byte assertions.
import { expect, it, vi } from "@effect/vitest";
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
import * as Projection from "../read/projection.ts";
import * as Appearance from "../registry/appearance.ts";
import * as Admission from "../runtime/admission.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import * as Clock from "effect/Clock";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import auth20 from "../../../../persistence/Migrations/020_AuthAccessManagement.ts";
import auth21 from "../../../../persistence/Migrations/021_AuthSessionClientMetadata.ts";
import auth22 from "../../../../persistence/Migrations/022_AuthSessionLastConnectedAt.ts";
import auth31 from "../../../../persistence/Migrations/031_AuthAuthorizationScopes.ts";
import auth41 from "../../../../persistence/Migrations/041_AuthSessionClientConnection.ts";
import * as Routes from "../read/routes.ts";
import * as Local from "../registry/localTransport.ts";
import * as Counts from "../registry/counts.ts";
import * as Placements from "../registry/placements.ts";
import * as Runner from "../registry/effectRunner.ts";
import * as Protocol from "../forwarding/homeReadProtocol.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as SqlClient from "effect/sql/SqlClient";
import {
  NativeWorkstreamReader,
  nativeReadRequest,
  validateNativeReadResult,
  type NativeReadScope,
  type NativeReadTransport,
} from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeRead.ts";
import { NativeWorkstreamReadCache } from "../../../../../../../packages/client-runtime/src/state/workstreams/native.ts";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT as resultContract,
  type WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import type { WorkstreamsNativeHomeRead } from "@t3tools/contracts/jones/workstreamsNativeRead";

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
    Layer.provideMerge(AuthSessions.layer.pipe(Layer.provideMerge(Fixture.testLayer({ keys })))),
  );
  return Layer.mergeAll(Projection.layer, Appearance.layer, Admission.layer).pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
};
const initialize = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Fixture.now));
  yield* migration;
  yield* auth20;
  yield* auth21;
  yield* auth22;
  yield* auth31;
  yield* auth41;
  yield* (yield* AuthSessions.AuthSessionRepository).create({
    sessionId: principal.sessionId,
    subject: principal.subject,
    method: principal.method,
    scopes: [Wire.AuthOrchestrationReadScope],
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "desktop",
      os: null,
      browser: null,
    },
    issuedAt: DateTime.makeUnsafe(Fixture.now),
    expiresAt: DateTime.makeUnsafe("2026-01-02T00:00:00Z"),
  });
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

const principal = {
  sessionId: Wire.AuthSessionId.make("coherent-browser"),
  subject: "synthetic-browser",
  method: "browser-session-cookie" as const,
  scopes: new Set([Wire.AuthOrchestrationReadScope]),
};
// Only keys and native provider completion evidence are synthetic. Current HTTP session,
// owner grant, caller boundary, registry, encrypted SQL, page producers and both validators are real.
const localLayer = Local.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Counts.layer, Placements.layer)),
  Layer.provideMerge(base()),
  Layer.provide(
    Layer.succeed(Runner.EffectRunner, {
      run: () => Effect.die("Unexpected provider dispatch"),
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
const services = Layer.mergeAll(Routes.layer, Admission.callerBoundaryLayer).pipe(
  Layer.provideMerge(localLayer),
);
const fixture = Effect.gen(function* () {
  yield* initialize;
  const h = yield* commands;
  const reads = yield* Routes.HomeReads;
  const boundary = yield* Admission.CallerBoundary;
  const admission = yield* Admission.Admission;
  const session = yield* Session.Session;
  const incoming = Protocol.HomePeerReadAdmission.of({
    current: Effect.fn("CoherentFixture.current")(
      function* () {
        const evidence = yield* boundary.current();
        const admitted = yield* admission
          .admit("workstreams:read")
          .pipe(Effect.provideService(Admission.RequestCaller, evidence));
        const authorized = yield* session.authorize(admitted, "workstreams:read");
        return {
          caller: admitted,
          authorization_revision: authorized.authorization.grant.grant_version,
          checkCurrent: Effect.fn("CoherentFixture.checkCurrent")(
            function* () {
              yield* evidence.check("workstreams:read", null);
              const current = yield* session.authorize(admitted, "workstreams:read");
              if (
                current.authorization.grant.grant_version !==
                authorized.authorization.grant.grant_version
              )
                return yield* new Session.RegistryError({ code: "forbidden" });
            },
            (effect) => effect.pipe(Effect.mapError(Projection.readFailure)),
          ),
        };
      },
      (effect) => effect.pipe(Effect.mapError(Projection.readFailure)),
    ),
  });
  const protocol = yield* Protocol.HomeReadProtocol.pipe(
    Effect.provide(
      Protocol.layer.pipe(Layer.provide(Layer.succeed(Protocol.HomePeerReadAdmission, incoming))),
    ),
  );
  const context = yield* Effect.context<never>();
  // This is the Promise transport boundary required by the production reader;
  // keep the actual it.effect service context/clock and forward cancellation.
  const run = <A, E>(effect: Effect.Effect<A, E>, signal?: AbortSignal) => {
    // oxlint-disable-next-line t3code/no-manual-effect-runtime-in-tests -- Required Promise adapter for the production consumer; retain the actual it.effect services and clock.
    return Effect.runPromiseWith(context)(effect, signal ? { signal } : undefined);
  };
  const scope = (): Effect.Effect<NativeReadScope, Session.RegistryError> =>
    h.registry.snapshot(caller).pipe(
      Effect.map((s) => ({
        capability: {
          contract: "workstreams-native/1.0.0",
          mode: "native-home",
          reads: ["workstreams-read/1.0.0"],
          placement_proofs: ["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"],
          registry: {
            registry_instance_id: home.registry_instance_id,
            owner_id: home.owner_id,
            canonical_origin: home.canonical_origin,
            home_environment_id: home.home_environment_id,
            server_generation: home.server_generation,
          },
        },
        binding: {
          registryId: home.canonical_origin,
          ownerId: home.owner_id,
          principalId: caller.principal_id,
          authorizationRevision: s.authorization.grant.grant_version,
          serverGeneration: 1,
          registryVersion: s.context.registry_version,
          permissions: ["workstreams:read"],
          contractVersion: Wire.WORKSTREAM_CONTRACT_HEADER_VERSION,
          contractManifest: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        },
      })),
    );
  const digest = async (bytes: Uint8Array) =>
    NodeCrypto.createHash("sha256").update(bytes).digest("hex");
  const reader = Effect.fn("CoherentFixture.reader")(function* (
    between?: (index: number) => Effect.Effect<void, { readonly _tag: string }>,
    cache?: NativeWorkstreamReadCache,
  ) {
    const bound = yield* scope();
    const seen: unknown[] = [];
    let calls = 0;
    const transport: NativeReadTransport = (input: WorkstreamsHomeReadResultRequest, signal) =>
      run(
        Effect.gen(function* () {
          if (calls && between) yield* between(calls);
          yield* TestClock.adjust(1);
          const result = yield* protocol.read(input);
          seen.push(result);
          calls++;
          return result;
        }).pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal)),
        signal,
      );
    return {
      client: new NativeWorkstreamReader({
        scope: bound,
        read: transport,
        sha256: digest,
        ...(cache ? { cache } : {}),
      }),
      seen,
      bound,
      transport,
    };
  });
  return { ...h, reads, protocol, run, reader, scope, digest };
});
const nextCursor = (result: Routes.ReadResult) =>
  (result.value as { next_cursor: string | null }).next_cursor!;
const pageRequest = (
  request: WorkstreamsNativeHomeRead,
  cursor: string,
): WorkstreamsNativeHomeRead =>
  ({ ...request, params: { ...request.params, cursor } }) as WorkstreamsNativeHomeRead;

it.effect(
  "composes real encrypted home pages with the frozen complete consumer under an advancing clock",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const a = yield* f.create("first"),
        b = yield* f.create("second"),
        c = yield* f.create("third");
      yield* nativeReference(f, a, "thread:a");
      yield* nativeReference(f, a, "thread:b");
      for (let n = 0; n < 2; n++) {
        let s = yield* f.registry.snapshot(caller);
        yield* f.submit({
          operation: "set_declaration",
          workstream_id: a,
          expected_version: s.state.workstreams.get(a)!.version,
          declaration_id: null,
          expected_revision: 0,
          text: "synthetic declaration " + n,
        });
        s = yield* f.registry.snapshot(caller);
        yield* f.submit({
          operation: "add_edge",
          from_workstream_id: a,
          expected_from_version: s.state.workstreams.get(a)!.version,
          to_workstream_id: [b, c][n]!,
          expected_to_version: s.state.workstreams.get([b, c][n]!)!.version,
          relation: "continues_as",
        });
      }
      const s = yield* f.registry.snapshot(caller),
        w = s.state.workstreams.get(a)!;
      yield* f.submit({
        operation: "update_workstream",
        workstream_id: a,
        expected_version: w.version,
        name: w.name,
        lifecycle: "completed",
        progress: w.progress,
        sort_order: w.sort_order,
      });
      const requests: WorkstreamsNativeHomeRead[] = [
        { route: "read-projection", params: { limit: 1 } },
        { route: "workstreams", params: { limit: 1 } },
        { route: "history", params: { limit: 1 } },
        { route: "lifecycle-declarations", params: { limit: 1, workstream_id: a } },
        { route: "memberships", params: { limit: 1, workstream_id: a } },
        { route: "references", params: { limit: 1 } },
        { route: "edges", params: { limit: 1, workstream_id: a } },
        { route: "declarations", params: { limit: 1, workstream_id: a } },
        {
          route: "placements",
          params: {
            limit: 1,
            identities: [
              { source_instance_id: home.home_environment_id, native_thread_id: "thread:a" },
              { source_instance_id: home.home_environment_id, native_thread_id: "thread:b" },
            ],
          },
        },
      ];
      for (const request of requests) {
        const { client, seen } = yield* f.reader();
        const completed = yield* Effect.promise(() =>
          client.complete(
            nativeReadRequest(
              request,
              request.route === "placements" ? "workstreams-t3-placement/2.0.0" : undefined,
            ),
          ),
        );
        expect(completed.pages.length, request.route).toBeGreaterThanOrEqual(2);
        expect(completed.items.length, request.route).toBeGreaterThanOrEqual(2);
        expect(completed.stale).toBe(false);
        expect(seen).toHaveLength(completed.pages.length);
        for (const page of completed.pages) expect(page.envelope).toEqual(completed.envelope);
        if (request.route === "placements")
          for (const page of completed.pages)
            expect(page.value).toMatchObject({
              provenance: { observed_at: completed.envelope.provenance.observed_at },
              items: [
                expect.objectContaining({
                  attestation_source: "native",
                  evidence_contract: "workstreams-t3-provider/2.0.0",
                }),
              ],
            });
      }
      const counts = yield* f.reads.read(caller, { route: "counts", params: {} });
      expect(counts.value).toMatchObject({ observed_at: counts.envelope.provenance.observed_at });
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql.unsafe("SELECT * FROM jones_workstreams_records");
      expect(JSON.stringify(rows)).not.toContain("first");
      expect(JSON.stringify(rows)).toContain("A256GCM");
    }).pipe(Effect.provide(services)),
);

it.effect.each([
  "appearance",
  "expiry",
  "source-binding",
  "grant-renewal",
  "grant-revoke",
  "key",
  "binding",
  "generation",
  "context",
  "session",
] as const)("fences a real complete read after between-page %s drift", (drift) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const id = yield* f.create("first");
    yield* f.create("second");
    yield* nativeReference(f, id);
    const store = yield* Store.RegistryStore;
    const sql = yield* SqlClient.SqlClient;
    const bindings = yield* BindingStore.BindingStore;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const before = yield* f.registry.snapshot(caller);
    const appearanceId = (yield* (yield* Projection.Projection).capture(caller)).items.at(-1)!
      .workstream.workstream_id;
    const appearance = yield* Appearance.Appearance;
    const { client, seen } = yield* f.reader(() =>
      Effect.gen(function* () {
        if (drift === "appearance") {
          yield* appearance.save(caller, {
            command_id: "coherent-appearance-00000000000001",
            expected_server_generation: 1,
            workstream_id: appearanceId,
            expected_version: 0,
            border_color: "#123456",
          });
          expect((yield* f.registry.snapshot(caller)).context.registry_version).toBe(
            before.context.registry_version,
          );
        } else if (drift === "expiry") yield* TestClock.adjust(300_001);
        else if (drift === "source-binding")
          yield* store.appendPlain(home, "source_bindings", {
            provider: "t3",
            source_instance_id: home.home_environment_id,
            binding_version: 2,
            authority_namespace: "authority:test",
            store_generation: 1,
            state: "active",
          });
        else if (drift === "grant-renewal" || drift === "grant-revoke")
          yield* store.appendPlain(home, "grants", {
            principal_id: caller.principal_id,
            grant_id: "grant:principal:test",
            grant_version: 2,
            permissions: '["workstreams:read"]',
            state: drift === "grant-renewal" ? "active" : "revoked",
          });
        else if (drift === "key")
          yield* sql`UPDATE jones_workstreams_keys SET state = 'retired' WHERE key_id = 'test-home'`;
        else if (drift === "binding")
          yield* bindings.bind({ ...Fixture.binding, binding_version: 2 }, 1);
        else if (drift === "generation")
          yield* sql`UPDATE jones_workstreams_registries SET server_generation = 2`;
        else if (drift === "context") yield* f.create("new-version");
        else
          yield* sessions.revoke({
            sessionId: principal.sessionId,
            revokedAt: DateTime.makeUnsafe(Fixture.now),
          });
      }),
    );
    yield* Effect.promise(() =>
      expect(
        client.complete(nativeReadRequest({ route: "read-projection", params: { limit: 1 } })),
      ).rejects.toBeDefined(),
    );
    expect(seen).toHaveLength(1);
    yield* Effect.promise(() =>
      expect(client.list()).rejects.toMatchObject({ code: "scope_changed" }),
    );
  }).pipe(Effect.provide(services)),
);

it.effect(
  "retains original history high-water across rejected and unchanged events at the same registry version",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const id = yield* f.create("first");
      yield* f.create("second");
      const request = { route: "history" as const, params: { limit: 1 } };
      const initial = yield* f.reads.read(caller, { route: "history", params: { limit: 100 } });
      const expected = (initial.value as Wire.WorkstreamHistoryPage).items;
      const before = yield* f.registry.snapshot(caller);
      const { client } = yield* f.reader((index) =>
        Effect.gen(function* () {
          if (index !== 1) return;
          const w = before.state.workstreams.get(id)!;
          const rejected = yield* f.submit({
            operation: "update_workstream",
            workstream_id: id,
            expected_version: 999,
            name: w.name,
            lifecycle: w.lifecycle,
            progress: w.progress,
            sort_order: w.sort_order,
          });
          expect(rejected.receipt.state).toBe("rejected");
          const unchanged = yield* f.submit({
            operation: "update_workstream",
            workstream_id: id,
            expected_version: w.version,
            name: w.name,
            lifecycle: w.lifecycle,
            progress: w.progress,
            sort_order: w.sort_order,
          });
          expect(unchanged.receipt.state).toBe("committed");
          expect((yield* f.registry.snapshot(caller)).context.registry_version).toBe(
            before.context.registry_version,
          );
        }),
      );
      const completed = yield* Effect.promise(() => client.complete(nativeReadRequest(request)));
      expect(completed.items.map((x) => x.event_id)).toEqual(expected.map((x) => x.event_id));
      expect(completed.pages).toHaveLength(expected.length);
      const later = yield* f.reads.read(caller, { route: "history", params: { limit: 100 } });
      expect((later.value as Wire.WorkstreamHistoryPage).items.length).toBe(expected.length + 2);
    }).pipe(Effect.provide(services)),
);

it.effect.each(["route", "limit", "filter", "principal", "source", "binding", "unknown"] as const)(
  "rejects native continuation with wrong %s without exposing a page",
  (mismatch) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const id = yield* f.create();
      yield* f.create();
      const request = { route: "history" as const, params: { limit: 1 } };
      const first = yield* f.reads.read(caller, request);
      const cursor = nextCursor(first);
      expect(cursor).toMatch(/^[A-Za-z0-9_-]{43}$/);
      let next: WorkstreamsNativeHomeRead = pageRequest(request, cursor);
      let actor = caller;
      if (mismatch === "route") next = { route: "workstreams", params: { limit: 1, cursor } };
      else if (mismatch === "limit") next = { route: "history", params: { limit: 2, cursor } };
      else if (mismatch === "filter")
        next = { route: "history", params: { limit: 1, cursor, workstream_id: id } };
      else if (mismatch === "principal") actor = { ...caller, principal_id: "other" };
      else if (mismatch === "source") actor = { ...caller, source_environment_id: "other" };
      else if (mismatch === "binding") actor = { ...caller, binding_version: 2 };
      else next = pageRequest(request, "a".repeat(43));
      expect(yield* f.reads.read(actor, next).pipe(Effect.flip)).toMatchObject({
        code: expect.any(String),
      });
    }).pipe(Effect.provide(services)),
);

it.effect.each(["inventory", "contract"] as const)(
  "binds native placement cursors to exact %s",
  (mismatch) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const id = yield* f.create();
      yield* nativeReference(f, id, "thread:a");
      yield* nativeReference(f, id, "thread:b");
      const request = {
        route: "placements" as const,
        params: {
          limit: 1,
          identities: [
            { source_instance_id: home.home_environment_id, native_thread_id: "thread:a" },
            { source_instance_id: home.home_environment_id, native_thread_id: "thread:b" },
          ],
        },
      };
      const first = yield* f.reads.read(caller, request, "workstreams-t3-placement/2.0.0");
      let next = pageRequest(request, nextCursor(first));
      if (mismatch === "inventory" && next.route === "placements")
        next = {
          ...next,
          params: { ...next.params, identities: next.params.identities.toReversed() },
        };
      expect(
        yield* f.reads
          .read(
            caller,
            next,
            mismatch === "contract"
              ? "workstreams-t3-placement/1.0.0"
              : "workstreams-t3-placement/2.0.0",
          )
          .pipe(Effect.flip),
      ).toMatchObject({ code: "cursor_invalid" });
    }).pipe(Effect.provide(services)),
);

it.effect.each(["reconstruction", "expiry", "eviction"] as const)(
  "fails honestly on snapshot %s",
  (reason) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.create();
      yield* f.create();
      const request = { route: "read-projection" as const, params: { limit: 1 } };
      const first = yield* f.reads.read(caller, request);
      const next = pageRequest(request, nextCursor(first));
      let port = f.reads;
      if (reason === "reconstruction")
        port = yield* Routes.HomeReads.pipe(Effect.provide(Layer.fresh(Routes.layer)));
      else if (reason === "expiry") yield* TestClock.adjust(900_001);
      else for (let n = 0; n < 16; n++) yield* f.reads.read(caller, request);
      expect(yield* port.read(caller, next).pipe(Effect.flip)).toMatchObject({
        code: "cursor_invalid",
      });
      expect((yield* port.read(caller, request)).envelope.coverage.registry).toBe("complete");
    }).pipe(Effect.provide(services)),
);

it.effect(
  "retries duplicate positions deterministically and purges the caller partition on failed currentness",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const id = yield* f.create();
      yield* f.create();
      yield* f.create();
      const request = { route: "read-projection" as const, params: { limit: 1 } };
      const first = yield* f.reads.read(caller, request);
      const next = pageRequest(request, nextCursor(first));
      const second = yield* f.reads.read(caller, next);
      yield* TestClock.adjust(10);
      expect(yield* f.reads.read(caller, next)).toEqual(second);
      const appearance = yield* Appearance.Appearance;
      yield* appearance.save(caller, {
        command_id: "coherent-update-0000000000000001",
        expected_server_generation: 1,
        workstream_id: id,
        expected_version: 0,
        border_color: "#123456",
      });
      expect(yield* f.reads.read(caller, next).pipe(Effect.flip)).toMatchObject({
        code: "cursor_stale",
      });
      yield* appearance.save(caller, {
        command_id: "coherent-update-0000000000000002",
        expected_server_generation: 1,
        workstream_id: id,
        expected_version: 1,
        border_color: null,
      });
      expect(yield* f.reads.read(caller, next).pipe(Effect.flip)).toMatchObject({
        code: "cursor_invalid",
      });
    }).pipe(Effect.provide(services)),
);

it.effect(
  "never publishes partial consumer cache after abort, scope change or bounds failure; refreshed scopes retry cleanly",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.create();
      yield* f.create();
      const abort = new AbortController();
      const { client, bound, seen } = yield* f.reader(() => Effect.sync(() => abort.abort()));
      yield* Effect.promise(() =>
        expect(
          client.complete(
            nativeReadRequest({ route: "workstreams", params: { limit: 1 } }),
            abort.signal,
          ),
        ).rejects.toMatchObject({ code: "cancelled" }),
      );
      expect(seen.length).toBeLessThanOrEqual(2);
      client.setScope(bound);
      const retried = yield* Effect.promise(() =>
        client.complete(nativeReadRequest({ route: "workstreams", params: { limit: 1 } })),
      );
      expect(retried.items).toHaveLength(2);
      const limited = yield* f.reader();
      const small = new NativeWorkstreamReader({
        scope: limited.bound,
        read: limited.transport,
        maxPages: 1,
      });
      yield* Effect.promise(() =>
        expect(
          small.complete(nativeReadRequest({ route: "workstreams", params: { limit: 1 } })),
        ).rejects.toMatchObject({ code: "bounds" }),
      );
      yield* Effect.promise(() =>
        expect(small.list()).rejects.toMatchObject({ code: "scope_changed" }),
      );
      let active!: NativeWorkstreamReader;
      const changed = yield* f.reader(() => Effect.sync(() => active.invalidate()));
      active = changed.client;
      yield* Effect.promise(() =>
        expect(
          active.complete(nativeReadRequest({ route: "workstreams", params: { limit: 1 } })),
        ).rejects.toMatchObject({ code: "cancelled" }),
      );
    }).pipe(Effect.provide(services)),
);

it.effect(
  "validates genuine v2 observation coherence and rejects relabelled placement evidence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const id = yield* f.create();
      yield* nativeReference(f, id);
      const request = {
        route: "placements" as const,
        params: {
          identities: [
            { source_instance_id: home.home_environment_id, native_thread_id: "thread:test" },
          ],
        },
      };
      const result = yield* f.reads.read(caller, request, "workstreams-t3-placement/2.0.0");
      const validated = yield* Routes.validateReadResult(
        request,
        result,
        "workstreams-t3-placement/2.0.0",
      );
      expect(validated.value).toMatchObject({
        provenance: { observed_at: result.envelope.provenance.observed_at },
      });
      const scope = yield* f.scope();
      yield* Effect.promise(() =>
        validateNativeReadResult(
          { contract: resultContract, ...result },
          nativeReadRequest(request, "workstreams-t3-placement/2.0.0"),
          scope,
          f.digest,
        ),
      );
      const changed = structuredClone(result) as Routes.ReadResult & {
        value: { provenance: { observed_at: string } };
      };
      changed.value.provenance.observed_at = "2026-01-01T00:00:01.000Z";
      expect(
        yield* Routes.validateReadResult(request, changed, "workstreams-t3-placement/2.0.0").pipe(
          Effect.flip,
        ),
      ).toMatchObject({ code: "invalid_response" });
      yield* Effect.promise(() =>
        expect(
          validateNativeReadResult(
            { contract: resultContract, ...changed },
            nativeReadRequest(request, "workstreams-t3-placement/2.0.0"),
            scope,
            f.digest,
          ),
        ).rejects.toBeDefined(),
      );
    }).pipe(Effect.provide(services)),
);

it.effect.each(["owner", "home"] as const)(
  "rejects continuation in another receiving %s binding",
  (field) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.create();
      yield* f.create();
      const request = { route: "workstreams" as const, params: { limit: 1 } };
      const first = yield* f.reads.read(caller, request);
      const port = yield* Routes.HomeReads.pipe(
        Effect.provide(
          Layer.fresh(Routes.layer).pipe(
            Layer.provide(
              Layer.succeed(
                NativeHome,
                field === "owner"
                  ? { ...home, owner_id: "other-owner" }
                  : { ...home, home_environment_id: "other-home" },
              ),
            ),
          ),
        ),
      );
      expect(
        yield* port.read(caller, pageRequest(request, nextCursor(first))).pipe(Effect.flip),
      ).toMatchObject({ code: expect.any(String) });
    }).pipe(Effect.provide(services)),
);

it.effect(
  "purges completed protected consumer cache on revocation without leaking it to a replacement scope",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.create();
      yield* f.create();
      const cache = new NativeWorkstreamReadCache();
      const writes = vi.spyOn(cache, "writeProtected");
      const { client, bound } = yield* f.reader(undefined, cache);
      const request = nativeReadRequest({ route: "workstreams", params: { limit: 1 } });
      yield* Effect.promise(() => client.complete(request));
      expect(writes).toHaveBeenCalledTimes(1);
      const [scopeKey, key] = writes.mock.calls[0]!;
      expect(cache.readProtected(scopeKey, key)).not.toBeNull();
      client.invalidate();
      expect(cache.readProtected(scopeKey, key)).toBeNull();
      const changed = { ...bound, binding: { ...bound.binding, principalId: "other-principal" } };
      client.setScope(changed);
      expect(cache.readProtected(scopeKey, key)).toBeNull();
      // Workstream pages have no principal field: the authenticated transport must
      // bind this scope. A changed scope must never recover the old cached bytes.
      client.invalidate();
      yield* Effect.promise(() =>
        expect(client.complete(request)).rejects.toMatchObject({ code: "scope_changed" }),
      );
    }).pipe(Effect.provide(services)),
);

it.effect("fences source-evidence expiry during actual production even without a store write", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const id = yield* f.create();
    yield* nativeReference(f, id);
    const clock = yield* Clock.Clock;
    let captured = false;
    const tick = () =>
      Date.parse(captured ? "2026-01-01T00:05:00.001Z" : "2026-01-01T00:04:59.999Z");
    const advancing: Clock.Clock = {
      ...clock,
      currentTimeMillisUnsafe: tick,
      currentTimeMillis: Effect.sync(tick),
      currentTimeNanosUnsafe: () => BigInt(tick()) * 1_000_000n,
      currentTimeNanos: Effect.sync(() => BigInt(tick()) * 1_000_000n),
    };
    const production = yield* Projection.Projection;
    // Observe actual capture completion to advance the synthetic clock exactly once;
    // tracing also reads Clock, so counting clock calls would expire before capture.
    const observed = Projection.Projection.of({
      ...production,
      capture: (actor) =>
        production.capture(actor).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              captured = true;
            }),
          ),
        ),
    });
    const port = yield* Routes.HomeReads.pipe(
      Effect.provide(
        Layer.fresh(Routes.layer).pipe(
          Layer.provide(Layer.succeed(Projection.Projection, observed)),
        ),
      ),
    );
    expect(
      yield* port
        .read(caller, { route: "read-projection", params: { limit: 1 } })
        .pipe(Effect.provideService(Clock.Clock, advancing), Effect.flip),
    ).toMatchObject({ code: "cursor_invalid" });
  }).pipe(Effect.provide(services)),
);
