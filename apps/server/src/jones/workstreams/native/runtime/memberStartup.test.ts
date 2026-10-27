// @effect-diagnostics nodeBuiltinImport:off -- File-backed synthetic SQLite scratch is allocated and disposed in its owning Effect scope.
// @effect-diagnostics preferSchemaOverJson:off -- Synthetic retained evidence exercises exact original JSON bytes.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WORKSTREAMS_HOME_BIND_CONTRACT as contract,
  WorkstreamsHomeBindBodyV2,
  canonicalWorkstreamsHomeBindBody,
} from "@t3tools/contracts/jones/workstreamsHomeBind";
import {
  WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA as schema,
  WorkstreamsMemberEnrollmentIntentV1,
  WorkstreamsMemberEnrollmentBoundV1,
  canonicalWorkstreamsMemberEnrollmentIntent,
} from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Members from "./memberEnrollment.ts";
import * as Startup from "./startup.ts";
import * as SqlFixture from "../store/sql.testkit.ts";
import { selectMember } from "./memberStartup.ts";
const decodeUnknownSyncWorkstreamsHomeBindBodyV2 =
  Schema.decodeUnknownSync(WorkstreamsHomeBindBodyV2);
const decodeUnknownSyncWorkstreamsMemberEnrollmentIntentV1 = Schema.decodeUnknownSync(
  WorkstreamsMemberEnrollmentIntentV1,
);
const decodeBound = Schema.decodeUnknownSync(WorkstreamsMemberEnrollmentBoundV1);
const home = {
  registry_instance_id: "12345678-1234-1234-1234-123456789abc",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 7,
};
const homeAuthority = {
  environment_id: "home",
  authority_namespace: "home-store",
  store_generation: 3,
};
const source = {
  environment_id: "member",
  authority_namespace: "source-store",
  store_generation: 4,
};
const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "enrollment-1",
  registry_origin: "https://home.invalid",
  context: {
    owner_id: "owner",
    principal_id: "principal",
    source_instance_id: source.environment_id,
    authority_namespace: source.authority_namespace,
    store_generation: source.store_generation,
    enrollment_id: "enrollment-1",
    protocol: "workstreams-t3-provider/1.0.0",
    build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
  },
  session: {
    session_id: "provider-session",
    issued_at: "2026-10-27T00:00:00.000Z",
    expires_at: "2026-10-28T00:00:00.000Z",
  },
};
const assertion = (atHome: boolean) => ({
  schema: "jones-workstreams-peer-session-pin/1",
  pin_id: atHome ? "home-pin" : "member-pin",
  revision: 1,
  receiver_authority: atHome ? homeAuthority : source,
  session: {
    session_id: atHome ? "home-link-session" : "source-link-session",
    subject: "native-peer",
    method: "bearer-access-token",
    issued_at: "2026-10-27T00:00:00Z",
    expires_at: "2026-10-28T00:00:00Z",
    peer_link: true,
    ceiling: {
      access: "read-only",
      runtime_mode: "approval-required",
      interaction_mode: "default",
      scopes: ["orchestration:read"],
      permissions: ["workstreams:read"],
    },
  },
  peer_environment_id: atHome ? "member" : "home",
  roles: [atHome ? "member_home" : "home_source"],
  expected_home: home,
  source_authority: source,
  source_enrollment_id: issuance.enrollment_id,
  source_binding_version: null,
  singleton_binding_version: null,
  state: "prepared",
  expires_at: "2026-10-27T12:00:00Z",
  setup_request_id: atHome ? "home-setup" : "member-setup",
  owner_qualification_digest: "c".repeat(64),
  bind_receipt: null,
});
const expected = { registry_version: 9, grant_id: "grant", grant_version: 2 };
const setup = (atHome: boolean) => ({
  schema: "jones-workstreams-peer-pin-setup/1",
  assertion: assertion(atHome),
  expected,
});
// Deliberately noncanonical original bytes must survive encrypted-record round trips.
const homeSetupBody = JSON.stringify(setup(true), null, 2) + "\n";
const memberSetupBody = JSON.stringify(setup(false), null, 2) + "\n";
const bodyValue = {
  bind_id: "bind-1",
  expected_home: home,
  expected_canonical_origin: issuance.registry_origin,
  expected_home_authority: homeAuthority,
  source: { authority: source, enrollment: issuance },
  home_setup: {
    pin_id: "home-pin",
    prepared_revision: 1,
    setup_request_id: "home-setup",
    setup_request_sha256: sha(homeSetupBody),
  },
  member_setup: {
    pin_id: "member-pin",
    prepared_revision: 1,
    setup_request_id: "member-setup",
    outbound_link_generation: "peer-link-secret:opaque-generation-v1",
    expected_singleton_version: null,
    singleton_binding_version: 1,
  },
  expected: { principal_id: "principal", ...expected, source_binding: null },
  rebind: false,
};
const decodeBody = decodeUnknownSyncWorkstreamsHomeBindBodyV2;
const body = () => canonicalWorkstreamsHomeBindBody(decodeBody(bodyValue));
const committed = () => ({
  contract,
  kind: "committed",
  request_body: body(),
  receipt: {
    principal_id: "principal",
    binding_version: 1,
    registry_version: 10,
    request_sha256: sha(body()),
  },
  home_pin_bound_revision: 2,
});
const homeSetup = () => ({
  contract,
  kind: "prepared",
  request: { contract, expected_home: home, home_setup_request_id: "home-setup" },
  canonical_origin: issuance.registry_origin,
  home_authority: homeAuthority,
  setup_request_body: homeSetupBody,
  pin: { ...assertion(true), setup_request_sha256: sha(homeSetupBody) },
});

const intentValue = () => ({
  issuance_body: JSON.stringify(issuance, null, 2) + "\n",
  bind_request_body: body(),
  bind_request_sha256: sha(body()),
  home_setup: homeSetup(),
  member_setup_request_body: memberSetupBody,
  member_setup_request_sha256: sha(memberSetupBody),
});
const prepared = () => {
  const intent = intentValue();
  return {
    schema,
    state: "prepared",
    revision: 1,
    intent,
    intent_sha256: sha(
      canonicalWorkstreamsMemberEnrollmentIntent(
        decodeUnknownSyncWorkstreamsMemberEnrollmentIntentV1(intent),
      ),
    ),
    home_result: null,
    local_binding: null,
  };
};
const bound = () => ({
  ...prepared(),
  state: "bound",
  revision: 2,
  home_result: committed(),
  local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
});

// M's producer and its source/provider/session/link currentness are synthetic here.
// SQL, selected paths, local-key metadata and WorkstreamsKeyProvider are real.
const fixture = Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-member-startup-"))),
    (allocated) => Effect.sync(() => NodeFS.rmSync(allocated, { recursive: true, force: true })),
  );
  const database = NodePath.join(directory, "selected.sqlite");
  const config = {
    mode: "native-member",
    registry: { ...home, canonical_origin: issuance.registry_origin },
    bootstrap: {
      home: directory,
      database,
      key_provider: "server-secret-store",
      source_session_id: issuance.session.session_id,
    },
  };
  const input = {
    text: JSON.stringify(config),
    legacyActivationPresent: false,
    home: directory,
    database,
    environmentId: source.environment_id,
  };
  const record = decodeBound(bound());
  const selectedSessions: string[] = [];
  let read = (
    _call: number,
  ): Effect.Effect<Option.Option<typeof record>, Members.MemberEnrollmentError> =>
    Effect.succeed(Option.some(record));
  const forbidden = () =>
    Effect.die("Startup must not issue, bind, materialize or use home authority");
  const members = Members.MemberEnrollments.of({
    getValidatedBySessionId: (id) =>
      Effect.suspend(() => {
        selectedSessions.push(id);
        return read(selectedSessions.length);
      }),
    ownerCurrent: forbidden(),
    validateSource: forbidden,
    inspectSession: forbidden,
    reserveSession: forbidden,
    validateLocal: forbidden,
    readProof: forbidden,
    getValidatedByEnrollmentId: forbidden,
    getBySessionId: forbidden,
    materialize: forbidden,
  });
  const keyReads: string[] = [];
  let acquireKey = (): Effect.Effect<Option.Option<Uint8Array>, Secrets.SecretStoreReadError> =>
    Effect.succeed(Option.some(new Uint8Array(32).fill(7)));
  const secrets = Secrets.ServerSecretStore.of({
    get: (name) => {
      keyReads.push(name);
      if (name !== "workstreams-content-key-local-key") return forbidden();
      return acquireKey();
    },
    set: forbidden,
    create: forbidden,
    getOrCreateRandom: forbidden,
    remove: forbidden,
  });
  const sqlLayer = NodeSqliteClient.layer({ filename: database });
  const keyLayer = Keys.layer.pipe(
    Layer.provide(Layer.succeed(Secrets.ServerSecretStore, secrets)),
    Layer.provide(sqlLayer),
  );
  const layer = Layer.mergeAll(
    sqlLayer,
    keyLayer,
    NodeServices.layer,
    Layer.succeed(Members.MemberEnrollments, members),
  );
  return {
    directory,
    database,
    config,
    input,
    record,
    selectedSessions,
    keyReads,
    layer,
    sqlLayer,
    keyLayer,
    setRead: (next: typeof read) => {
      read = next;
    },
    setAcquireKey: (next: typeof acquireKey) => {
      acquireKey = next;
    },
  };
});
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // No home registry/principal/grant table exists. Any accidental access fails.
  yield* sql`CREATE TABLE jones_workstreams_keys (registry_instance_id TEXT, key_id TEXT, scope TEXT, state TEXT)`;
  yield* sql`INSERT INTO jones_workstreams_keys VALUES (${home.registry_instance_id}, 'local-key', 'local', 'active')`;
  yield* sql`PRAGMA query_only=ON`;
});
const fenced = (selection: Startup.Selection, code?: Startup.StartupError["code"]) => {
  expect(selection).toMatchObject({ mode: "transition-fenced", home: null, sourceSessionId: null });
  expect(selection.problem).not.toBeNull();
  if (code) expect(selection.problem?.code).toBe(code);
};

it.effect(
  "qualifies only the original member, reads local keys, and performs no writes or CP construction",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        const sql = yield* SqlClient.SqlClient;
        const before = yield* sql`SELECT total_changes() AS changes`;
        const cp = vi
          .spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport")
          .mockImplementation(() => {
            throw new Error("CP construction forbidden");
          });
        const writer = vi
          .spyOn(sql, "withTransaction")
          .mockImplementation(() => Effect.die("No startup writer"));
        try {
          const selection = yield* selectMember(f.input);
          expect(selection).toEqual({
            mode: "native-member",
            config: f.config,
            home: null,
            sourceSessionId: issuance.session.session_id,
            problem: null,
          });
          expect(f.selectedSessions).toEqual([
            issuance.session.session_id,
            issuance.session.session_id,
          ]);
          expect(f.keyReads).toEqual(["workstreams-content-key-local-key"]);
          expect(yield* sql`SELECT total_changes() AS changes`).toEqual(before);
          expect(yield* sql`SELECT name FROM sqlite_schema WHERE type='table'`).toEqual([
            { name: "jones_workstreams_keys" },
          ]);
          expect(writer).not.toHaveBeenCalled();
          expect(cp).not.toHaveBeenCalled();
          // Qualification cannot advertise or mount member runtime through the held startup path.
          expect(
            Startup.capability(selection, { readProjection: true, placementsV2: true }),
          ).toBeUndefined();
        } finally {
          writer.mockRestore();
          cp.mockRestore();
        }
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "rejects absent, non-member, malformed or incomplete explicit selections before M or keys",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        for (const text of [
          null,
          "",
          "{",
          JSON.stringify({}),
          ...["legacy", "disabled", "native-home", "transition-fenced"].map((mode) =>
            JSON.stringify({ ...f.config, mode }),
          ),
          JSON.stringify({ ...f.config, registry: null }),
          JSON.stringify({ mode: "native-member", registry: f.config.registry }),
          JSON.stringify({
            ...f.config,
            bootstrap: { ...f.config.bootstrap, source_session_id: undefined },
          }),
          JSON.stringify({
            ...f.config,
            bootstrap: { ...f.config.bootstrap, source_session_id: "" },
          }),
          JSON.stringify({
            ...f.config,
            bootstrap: { ...f.config.bootstrap, key_provider: "other" },
          }),
          JSON.stringify({
            ...f.config,
            registry: { ...f.config.registry, canonical_origin: "invalid" },
          }),
        ])
          fenced(yield* selectMember({ ...f.input, text }));
        fenced(
          yield* selectMember({ ...f.input, legacyActivationPresent: true }),
          "authority_fenced",
        );
        expect(f.selectedSessions).toEqual([]);
        expect(f.keyReads).toEqual([]);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "checks configured and actual selected paths including PRAGMA main rather than a fabricated filename",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        const text = (bootstrap: typeof f.config.bootstrap) =>
          JSON.stringify({ ...f.config, bootstrap });
        for (const input of [
          { ...f.input, home: f.directory + "-other" },
          { ...f.input, database: f.database + "-other" },
          { ...f.input, home: "relative" },
          { ...f.input, database: "relative.sqlite" },
          { ...f.input, text: text({ ...f.config.bootstrap, home: "relative" }) },
          { ...f.input, text: text({ ...f.config.bootstrap, database: "relative.sqlite" }) },
          // Config and supplied input agree with one another, but not the selected SQL resource.
          {
            ...f.input,
            database: f.database + "-invented",
            text: text({ ...f.config.bootstrap, database: f.database + "-invented" }),
          },
        ])
          fenced(yield* selectMember(input), "database_mismatch");
        expect(f.selectedSessions).toEqual([]);
        expect(
          (yield* selectMember({ ...f.input, home: NodePath.join(f.directory, ".") })).mode,
        ).toBe("native-member");
      }).pipe(Effect.provide(f.layer));
      // An in-memory main database has no pathname; matching configured strings cannot qualify it.
      fenced(
        yield* selectMember(f.input).pipe(
          Effect.provide(
            Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" }), NodeServices.layer),
          ),
        ),
        "database_mismatch",
      );
    }),
);

it.effect(
  "matches the complete configured destination and original source environment/session",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        for (const registry of [
          { ...f.config.registry, registry_instance_id: "22222222-2222-4222-8222-222222222222" },
          { ...f.config.registry, owner_id: "other-owner" },
          { ...f.config.registry, home_environment_id: "other-home" },
          { ...f.config.registry, server_generation: 8 },
          { ...f.config.registry, canonical_origin: "https://other.invalid" },
        ])
          fenced(
            yield* selectMember({ ...f.input, text: JSON.stringify({ ...f.config, registry }) }),
            "binding_mismatch",
          );
        fenced(
          yield* selectMember({ ...f.input, environmentId: "other-source" }),
          "binding_mismatch",
        );
        fenced(
          yield* selectMember({
            ...f.input,
            text: JSON.stringify({
              ...f.config,
              bootstrap: { ...f.config.bootstrap, source_session_id: "replacement-session" },
            }),
          }),
          "binding_mismatch",
        );
        expect(f.selectedSessions.at(-1)).toBe("replacement-session");
        expect(f.keyReads).toEqual([]);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "fences unavailable M, prepared/missing evidence and currentness refusals before or after keys",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        f.setRead(() => Effect.succeed(Option.none()));
        fenced(yield* selectMember(f.input), "authority_fenced");
        for (const code of [
          "authority_unavailable",
          "authority_mismatch",
          "build_unavailable",
          "build_mismatch",
          "session_conflict",
          "pin_mismatch",
          "link_changed",
          "unavailable",
          "key_unavailable",
        ] as const) {
          f.setRead(() => Effect.fail(new Members.MemberEnrollmentError({ code })));
          fenced(
            yield* selectMember(f.input),
            code === "key_unavailable" ? code : "authority_fenced",
          );
          let calls = 0;
          f.setRead(() =>
            ++calls === 1
              ? Effect.succeed(Option.some(f.record))
              : Effect.fail(new Members.MemberEnrollmentError({ code })),
          );
          fenced(
            yield* selectMember(f.input),
            code === "key_unavailable" ? code : "authority_fenced",
          );
        }
      }).pipe(Effect.provide(f.layer));
      fenced(
        yield* selectMember(f.input).pipe(
          Effect.provide(Layer.mergeAll(f.sqlLayer, f.keyLayer, NodeServices.layer)),
        ),
        "authority_fenced",
      );
    }),
);

it.effect(
  "refuses missing local keys/metadata and absent key service without reading home/index keys",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        f.setAcquireKey(() => Effect.succeed(Option.none()));
        fenced(yield* selectMember(f.input), "key_unavailable");
        f.setAcquireKey(() =>
          Effect.fail(
            new Secrets.SecretStoreReadError({
              resource: "synthetic-key",
              cause: new Error("synthetic"),
            }),
          ),
        );
        fenced(yield* selectMember(f.input), "key_unavailable");
        f.setAcquireKey(() => Effect.succeed(Option.some(new Uint8Array(2))));
        fenced(yield* selectMember(f.input), "key_unavailable");
        expect(f.keyReads.every((key) => key === "workstreams-content-key-local-key")).toBe(true);
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA query_only=OFF`;
        yield* sql`DELETE FROM jones_workstreams_keys`;
        yield* sql`PRAGMA query_only=ON`;
        fenced(yield* selectMember(f.input), "key_unavailable");
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "rechecks immutable bound bytes and source/link currentness after external key acquisition",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* seed;
        let changed = false;
        f.setAcquireKey(() =>
          Effect.sync(() => {
            changed = true;
            return Option.some(new Uint8Array(32));
          }),
        );
        f.setRead(() =>
          changed
            ? Effect.fail(new Members.MemberEnrollmentError({ code: "link_changed" }))
            : Effect.succeed(Option.some(f.record)),
        );
        fenced(yield* selectMember(f.input), "authority_fenced");
        changed = false;
        // Even a new apparently valid association may not replace the captured original bytes.
        f.setRead(() =>
          Effect.succeed(
            Option.some(
              changed
                ? {
                    ...f.record,
                    local_binding: { ...f.record.local_binding, pin_bound_revision: 3 },
                  }
                : f.record,
            ),
          ),
        );
        fenced(yield* selectMember(f.input), "binding_mismatch");
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "leaves existing startup member fencing and legacy/disabled/home selection unchanged",
  () =>
    Effect.gen(function* () {
      const input = {
        text: null,
        legacyActivationPresent: false,
        home: "/synthetic",
        database: "/synthetic/selected.sqlite",
        environmentId: "member",
      };
      expect((yield* Startup.select(input)).mode).toBe("disabled");
      expect((yield* Startup.select({ ...input, legacyActivationPresent: true })).mode).toBe(
        "legacy",
      );
      fenced(
        yield* Startup.select({
          ...input,
          text: JSON.stringify({
            mode: "native-member",
            registry: { ...home, canonical_origin: issuance.registry_origin },
          }),
        }),
        "member_not_supported",
      );
      fenced(
        yield* Startup.select({
          ...input,
          text: JSON.stringify({
            mode: "native-home",
            registry: { ...home, canonical_origin: issuance.registry_origin },
          }),
        }),
        "explicit_selection_required",
      );
    }).pipe(Effect.provide(Layer.mergeAll(SqlFixture.testLayer(), NodeServices.layer))),
);

it.effect("fences when the key service is missing even if M validates the association", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const members = Members.MemberEnrollments.of({
      ...(yield* Members.MemberEnrollments.pipe(Effect.provide(f.layer))),
    });
    fenced(
      yield* selectMember(f.input).pipe(
        Effect.provide(
          Layer.mergeAll(
            f.sqlLayer,
            NodeServices.layer,
            Layer.succeed(Members.MemberEnrollments, members),
          ),
        ),
      ),
      "key_unavailable",
    );
    expect(f.keyReads).toEqual([]);
  }),
);

it.effect("rechecks local active-key metadata and captures bytes before in-place drift", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      f.setAcquireKey(() =>
        Effect.gen(function* () {
          // The synthetic external key acquisition changes local metadata before returning.
          yield* sql`PRAGMA query_only=OFF`;
          yield* sql`UPDATE jones_workstreams_keys SET state='retired'`;
          yield* sql`PRAGMA query_only=ON`;
          return Option.some(new Uint8Array(32));
        }).pipe(
          Effect.mapError(
            (cause) => new Secrets.SecretStoreReadError({ resource: "fixture-key", cause }),
          ),
        ),
      );
      fenced(yield* selectMember(f.input), "key_unavailable");
      yield* sql`PRAGMA query_only=OFF`;
      yield* sql`UPDATE jones_workstreams_keys SET state='active'`;
      yield* sql`PRAGMA query_only=ON`;
      f.setAcquireKey(() =>
        Effect.sync(() => {
          // The service may reuse an object; the first snapshot must already be immutable.
          Object.assign(f.record.local_binding, { pin_bound_revision: 3 });
          return Option.some(new Uint8Array(32));
        }),
      );
      fenced(yield* selectMember(f.input), "binding_mismatch");
    }).pipe(Effect.provide(f.layer));
  }),
);
