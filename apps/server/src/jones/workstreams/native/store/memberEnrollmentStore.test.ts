// @effect-diagnostics nodeBuiltinImport:off -- Isolated SQLite/WAL scratch is scoped; explicitly requested evidence is retained.
// @effect-diagnostics preferSchemaOverJson:off -- Exact original fixture bytes and deliberate corruptions are test inputs.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as MemberWire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import migration126 from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import migration128 from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import migration129 from "../../../persistence/Migrations/129_JonesWorkstreamsMemberBindings.ts";
import * as Test from "./sql.testkit.ts";
import * as Records from "./memberEnrollmentStore.ts";
import * as Database from "./sql.ts";

const decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindBodyV2,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentIntentV1,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentPreparedV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentPreparedV1,
);
const decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindCommittedV2,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentBoundV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentBoundV1,
);
const scratch = (prefix: string) => {
  const parent = process.env.NW_MEMBER_ENROLLMENT_EVIDENCE_DIR;
  return Effect.acquireRelease(
    Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(parent ?? NodeOS.tmpdir(), prefix))),
    (directory) =>
      Effect.sync(() => {
        if (parent === undefined) NodeFS.rmSync(directory, { recursive: true, force: true });
      }),
  );
};
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const sourceAuthority = {
  environment_id: "member:test",
  authority_namespace: "member-store",
  store_generation: 1,
};
const homeAuthority = {
  environment_id: "home:test",
  authority_namespace: "home-store",
  store_generation: 1,
};
const build = {
  repository: "Jones-Systems/Jones-Code" as const,
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "member-enrollment:test",
  registry_origin: "https://native.workstreams.invalid",
  context: {
    owner_id: Test.scope.owner_id,
    principal_id: "principal:test",
    source_instance_id: "member:test",
    authority_namespace: "member-store",
    store_generation: 1,
    enrollment_id: "member-enrollment:test",
    protocol: "workstreams-t3-provider/1.0.0",
    build,
  },
  session: {
    session_id: "provider-session:test",
    issued_at: Test.now,
    expires_at: "2026-01-02T00:00:00.000Z",
  },
};
const syntheticSession = {
  session_id: "member-link:test",
  subject: "mcp-client",
  method: "bearer-access-token",
  issued_at: Test.now,
  expires_at: "2026-01-31T00:00:00.000Z",
  peer_link: true,
  ceiling: {
    access: "full-access",
    runtime_mode: "approval-required",
    interaction_mode: "default",
    scopes: [
      "orchestration:read",
      "orchestration:operate",
      "settings:write",
      "source-control:write",
    ],
    permissions: ["workstreams:read", "workstreams:write"],
  },
};
const makeIntent = (session: unknown = syntheticSession) => {
  const assertion = (atHome: boolean) => ({
    schema: "jones-workstreams-peer-session-pin/1",
    pin_id: atHome ? "home-pin:test" : "member-pin:test",
    revision: 1,
    receiver_authority: atHome ? homeAuthority : sourceAuthority,
    session: atHome ? { ...syntheticSession, session_id: "home-link:test" } : session,
    peer_environment_id: atHome ? "member:test" : "home:test",
    roles: [atHome ? "member_home" : "home_source"],
    expected_home: home,
    source_authority: sourceAuthority,
    source_enrollment_id: issuance.enrollment_id,
    source_binding_version: null,
    singleton_binding_version: null,
    state: "prepared",
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: atHome ? "home-setup:test" : "member-setup:test",
    owner_qualification_digest: "c".repeat(64),
    bind_receipt: null,
  });
  const preconditions = { registry_version: 9, grant_id: "grant:test", grant_version: 1 };
  const setup = (atHome: boolean) =>
    JSON.stringify(
      {
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion: assertion(atHome),
        expected: preconditions,
      },
      null,
      2,
    ) + "\n";
  const homeBytes = setup(true),
    memberBytes = setup(false);
  const body = HomeWire.canonicalWorkstreamsHomeBindBody(
    decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2({
      bind_id: "bind:test",
      expected_home: home,
      expected_canonical_origin: issuance.registry_origin,
      expected_home_authority: homeAuthority,
      source: { authority: sourceAuthority, enrollment: issuance },
      home_setup: {
        pin_id: "home-pin:test",
        prepared_revision: 1,
        setup_request_id: "home-setup:test",
        setup_request_sha256: sha(homeBytes),
      },
      member_setup: {
        pin_id: "member-pin:test",
        prepared_revision: 1,
        setup_request_id: "member-setup:test",
        outbound_link_generation: "opaque-link-generation:1",
        expected_singleton_version: null,
        singleton_binding_version: 1,
      },
      expected: { principal_id: "principal:test", ...preconditions, source_binding: null },
      rebind: false,
    }),
  );
  return decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1({
    issuance_body: JSON.stringify(issuance, null, 2) + "\n",
    bind_request_body: body,
    bind_request_sha256: sha(body),
    home_setup: {
      contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
      kind: "prepared",
      request: {
        contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
        expected_home: home,
        home_setup_request_id: "home-setup:test",
      },
      canonical_origin: issuance.registry_origin,
      home_authority: homeAuthority,
      setup_request_body: homeBytes,
      pin: { ...assertion(true), setup_request_sha256: sha(homeBytes) },
    },
    member_setup_request_body: memberBytes,
    member_setup_request_sha256: sha(memberBytes),
  });
};
const preparedRecord = (intent = makeIntent()) =>
  decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentPreparedV1({
    schema: MemberWire.WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA,
    intent,
    intent_sha256: sha(MemberWire.canonicalWorkstreamsMemberEnrollmentIntent(intent)),
    state: "prepared",
    revision: 1,
    home_result: null,
    local_binding: null,
  });
const homeResult = (intent = makeIntent()) =>
  decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2({
    contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
    kind: "committed",
    request_body: intent.bind_request_body,
    receipt: {
      principal_id: "principal:test",
      binding_version: 1,
      registry_version: 10,
      request_sha256: intent.bind_request_sha256,
    },
    home_pin_bound_revision: 2,
  });
const boundRecord = (intent = makeIntent()) =>
  decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentBoundV1({
    ...preparedRecord(intent),
    state: "bound",
    revision: 2,
    home_result: homeResult(intent),
    local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
  });

const services = (filename?: string) =>
  Records.layer.pipe(Layer.provideMerge(Test.testLayer(filename ? { filename } : {})));
const seed = Effect.gen(function* () {
  yield* Test.seed;
  yield* migration126;
  yield* migration128;
  yield* migration129;
});
it.effect(
  "local encrypted prepared/bound history checks all selectors and retains exact original bytes",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const store = yield* Records.MemberEnrollmentStore,
        sql = yield* SqlClient.SqlClient;
      const p = preparedRecord(),
        b = boundRecord();
      yield* store.insert(p);
      const raw = (yield* sql<{
        envelope: string;
      }>`SELECT envelope FROM jones_workstreams_member_enrollments`)[0]!.envelope;
      expect(raw).not.toContain("principal:test");
      expect(raw).not.toContain("outbound_link_generation");
      expect(raw).not.toContain(p.intent.issuance_body);
      for (const selector of [
        { enrollment_id: issuance.enrollment_id },
        { session_id: issuance.session.session_id },
        { setup_request_id: "member-setup:test" },
        { ...Test.scope, bind_id: "bind:test" },
      ])
        expect(yield* store.read(selector)).toEqual(p);
      yield* store.accept(b, 1);
      expect(yield* store.read({ enrollment_id: issuance.enrollment_id })).toEqual(b);
      expect(yield* store.accept(b, 1).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      expect(yield* store.insert(p).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      expect(
        (yield* store.read({ session_id: issuance.session.session_id }))?.intent.issuance_body,
      ).toBe(p.intent.issuance_body);
    }).pipe(Effect.provide(services())),
);

it.effect(
  "every retained digest and original issuance/setup/source/home selector is verified",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const store = yield* Records.MemberEnrollmentStore,
        p = preparedRecord();
      const variants = [
        { ...p, intent_sha256: "e".repeat(64) },
        { ...p, intent: { ...p.intent, bind_request_sha256: "e".repeat(64) } },
        { ...p, intent: { ...p.intent, member_setup_request_sha256: "e".repeat(64) } },
        {
          ...p,
          intent: {
            ...p.intent,
            issuance_body: p.intent.issuance_body.replace("member-store", "other-store"),
          },
        },
        {
          ...p,
          intent: {
            ...p.intent,
            member_setup_request_body: p.intent.member_setup_request_body + " ",
          },
        },
        {
          ...p,
          intent: {
            ...p.intent,
            home_setup: {
              ...p.intent.home_setup,
              pin: { ...p.intent.home_setup.pin, setup_request_sha256: "e".repeat(64) },
            },
          },
        },
      ];
      for (const v of variants)
        expect(yield* store.insert(v).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      expect(yield* store.read({ enrollment_id: issuance.enrollment_id })).toBeNull();
    }).pipe(Effect.provide(services())),
);

it.effect("AAD binds owner, registry, purpose, enrollment and revision", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* Records.MemberEnrollmentStore,
      db = yield* Database.NativeSql,
      sql = yield* SqlClient.SqlClient;
    yield* store.insert(preparedRecord());
    const raw = (yield* sql<{
      envelope: string;
    }>`SELECT envelope FROM jones_workstreams_member_enrollments`)[0]!.envelope;
    const context = {
      ...Test.scope,
      key_scope: "local" as const,
      purpose: "native_member_enrollment",
      record_id: issuance.enrollment_id,
      revision: 1,
    };
    for (const changed of [
      { owner_id: "wrong" },
      { registry_instance_id: "wrong" },
      { purpose: "native_source_enrollment" },
      { record_id: "wrong" },
      { revision: 2 },
    ])
      expect(yield* db.openJson({ ...context, ...changed }, raw).pipe(Effect.result)).toMatchObject(
        { _tag: "Failure" },
      );
  }).pipe(Effect.provide(services())),
);

it.effect("authenticated payload still cannot disagree with indexed bind selector", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* Records.MemberEnrollmentStore,
      db = yield* Database.NativeSql,
      sql = yield* SqlClient.SqlClient;
    yield* store.insert(preparedRecord());
    const original = boundRecord(),
      body = HomeWire.decodeWorkstreamsHomeBindBody(original.intent.bind_request_body);
    const alteredBytes = HomeWire.canonicalWorkstreamsHomeBindBody({
      ...body,
      bind_id: "other-bind",
    });
    const intent = {
      ...original.intent,
      bind_request_body: alteredBytes,
      bind_request_sha256: sha(alteredBytes),
    };
    const altered = boundRecord(intent);
    const envelope = yield* db.sealJson(
      {
        ...Test.scope,
        key_scope: "local",
        purpose: "native_member_enrollment",
        record_id: issuance.enrollment_id,
        revision: 2,
      },
      altered,
    );
    yield* sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=2,envelope=${envelope}`;
    expect(
      yield* store.read({ session_id: issuance.session.session_id }).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure" });
  }).pipe(Effect.provide(services())),
);

it.effect("an outer failed writer rolls back both preparation and acceptance", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* Records.MemberEnrollmentStore,
      db = yield* Database.NativeSql;
    yield* db
      .transaction(
        Test.scope,
        Effect.gen(function* () {
          yield* store.insert(preparedRecord());
          yield* store.accept(boundRecord(), 1);
          return yield* Effect.fail("deliberate rollback");
        }),
      )
      .pipe(Effect.result);
    expect(yield* store.read({ enrollment_id: issuance.enrollment_id })).toBeNull();
  }).pipe(Effect.provide(services())),
);

it.effect(
  "file-backed WAL contains encrypted bytes and restart recovers immutable bound history",
  () =>
    Effect.gen(function* () {
      const directory = yield* scratch("nw-member-store-"),
        filename = NodePath.join(directory, "state.sqlite");
      const expected = boundRecord();
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* seed;
          const sql = yield* SqlClient.SqlClient,
            store = yield* Records.MemberEnrollmentStore;
          yield* sql`PRAGMA journal_mode=WAL`;
          yield* store.insert(preparedRecord());
          yield* store.accept(expected, 1);
          for (const path of [filename, filename + "-wal"])
            if (NodeFS.existsSync(path)) {
              const bytes = NodeFS.readFileSync(path).toString("utf8");
              expect(bytes).not.toContain(expected.intent.issuance_body);
              expect(bytes).not.toContain("member-store");
              expect(bytes).not.toContain("opaque-link-generation:1");
            }
        }).pipe(Effect.provide(services(filename))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Records.MemberEnrollmentStore;
          expect(yield* store.read({ session_id: issuance.session.session_id })).toEqual(expected);
          expect(yield* store.accept(expected, 1).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
          });
        }).pipe(Effect.provide(services(filename))),
      );
    }).pipe(Effect.scoped),
);

it.effect("home-key substitution and a zero-row CAS are refused", () =>
  Effect.gen(function* () {
    yield* seed;
    const store = yield* Records.MemberEnrollmentStore,
      db = yield* Database.NativeSql,
      sql = yield* SqlClient.SqlClient;
    yield* store.insert(preparedRecord());
    yield* sql`CREATE TRIGGER synthetic_zero_row BEFORE UPDATE ON jones_workstreams_member_enrollments BEGIN SELECT RAISE(IGNORE); END`;
    expect(yield* store.accept(boundRecord(), 1).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { reason: "conflict" },
    });
    expect((yield* store.read({ enrollment_id: issuance.enrollment_id }))?.state).toBe("prepared");
    yield* sql`DROP TRIGGER synthetic_zero_row`;
    const substituted = yield* db.sealJson(
      {
        ...Test.scope,
        key_scope: "home",
        purpose: "native_member_enrollment",
        record_id: issuance.enrollment_id,
        revision: 2,
      },
      boundRecord(),
    );
    yield* sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=2,envelope=${substituted}`;
    expect(
      yield* store.read({ enrollment_id: issuance.enrollment_id }).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure" });
  }).pipe(Effect.provide(services())),
);
