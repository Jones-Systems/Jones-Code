import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { canonical, digest, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";
import { runtimeUseCreationObjectNames } from "./RuntimeUseCreationSchema.ts";

const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const sequence = Schema.Int.check(Schema.isGreaterThan(0));

// These identify the supported source contracts, not a claim that a source
// checkout or an installed binary was independently qualified. Their actual
// implementation/composition is bound separately by the build/source receipt.
export const runtimeUseCoverageSource = {
  schema: "jones.workspace-runtime-coverage-source/v1",
  journal: "jones.workspace-runtime-use/v1",
  invocation: "original-claim-and-before-effect/v1",
  attachment: "original-claim-and-publication-fence/v1",
  terminal: "original-logical-start-and-before-spawn/v1",
  preparedTerminal: "original-consumed-native-setup-and-before-spawn/v1",
  retirement: "captured-original-producer-and-writer-set/v1",
  creation: "original-native-admission-before-first-material-effect/v1",
} as const;

export const RuntimeUseInstallation = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-installation/v1"),
  migration: Schema.Struct({
    // Ledger id of 150_JonesWorkspaceRuntimeUses.ts.
    id: Schema.Literal(150),
    name: Schema.Literal("WorkspaceRuntimeUses"),
  }),
  coverageSourceSha256: hash,
  journalSchemaSha256: hash,
  globalEventWatermark: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("empty") }),
    Schema.Struct({ kind: Schema.Literal("sequence"), sequence }),
  ]),
});
export type RuntimeUseInstallation = typeof RuntimeUseInstallation.Type;
const decodeInstallation = Schema.decodeUnknownEffect(RuntimeUseInstallation, {
  onExcessProperty: "error",
});
const parseJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeRow = Schema.decodeUnknownEffect(
  Schema.Struct({ id: Schema.Literal(1), basis_json: Schema.String, basis_sha256: hash }),
  { onExcessProperty: "error" },
);

const journalNames = [
  "jones_workspace_runtime_uses",
  "jones_workspace_runtime_use_events",
  "jones_workspace_runtime_use_pending",
  "jones_workspace_runtime_use_deletion_conflict",
  "jones_workspace_runtime_deletion_use_conflict",
  "jones_workspace_runtime_uses_no_delete",
  "jones_workspace_runtime_uses_no_replace",
  "jones_workspace_runtime_use_events_no_delete",
  "jones_workspace_runtime_use_events_no_replace",
  "jones_workspace_runtime_uses_initial",
  "jones_workspace_runtime_uses_reserved",
  "jones_workspace_runtime_events_immutable",
  "jones_workspace_runtime_event_guard",
  "jones_workspace_runtime_event_transition",
  "jones_workspace_runtime_producer_identity",
  "jones_workspace_runtime_native_binding_identity",
  "jones_workspace_runtime_retirement_coverage",
  "jones_workspace_runtime_use_transition",
].sort();
const installationNames = [
  "jones_workspace_runtime_installation",
  "jones_workspace_runtime_installation_no_update",
  "jones_workspace_runtime_installation_no_delete",
  "jones_workspace_runtime_installation_no_replace",
];
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));
class InstallationAdmission extends Context.Service<
  InstallationAdmission,
  {
    readonly sql: SqlClient.SqlClient;
    readonly fiberId: number;
    readonly active: () => boolean;
  }
>()("t3/jones/cleanup/RuntimeUseInstallation/InstallationAdmission") {}

// The actual migrator inserts its tracking row before executing its migration.
// Capture eligibility in the owning guard transaction BEFORE that insertion,
// rather than treating the newly inserted row as a historical installation.
export const withRuntimeUseInstallationAdmission = Effect.fn(
  "RuntimeUseInstallation.withAdmission",
)(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | SqlError, R | SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new SqlError({
      reason: new UnknownError({
        cause: "Installation admission requires the original migration transaction",
      }),
    });
  const ledger =
    yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='jones_sql_migrations'`;
  const prior =
    ledger.length === 0
      ? []
      : yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id>=150`;
  if (prior.length !== 0) return yield* effect;
  const fiberId = yield* Effect.fiberId;
  let active = true;
  return yield* effect.pipe(
    Effect.provideService(InstallationAdmission, { sql, fiberId, active: () => active }),
    Effect.ensuring(
      Effect.sync(() => {
        active = false;
      }),
    ),
  );
});
const schemaIdentity = Effect.fn("RuntimeUseInstallation.schemaIdentity")(function* (
  installed: boolean,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly type: string; readonly name: string; readonly sql: string }>`
    SELECT type,name,sql FROM sqlite_master
    WHERE name GLOB 'jones_workspace_runtime_*' ORDER BY name`;
  const expected = [
    ...journalNames,
    ...runtimeUseCreationObjectNames,
    ...(installed ? installationNames : []),
  ].sort();
  if (canonical(rows.map((row) => row.name)) !== canonical(expected))
    return yield* fail("runtime_use_installation_schema_membership");
  if (rows.some((row) => typeof row.sql !== "string" || row.sql.length === 0))
    return yield* fail("runtime_use_installation_schema_definition");
  return digest(rows);
});

/** Migration-only. The original migration transaction captures the global
 * committed event boundary once; it cannot retrofit an already-applied runtime-use migration. */
export const createRuntimeUseInstallation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
    return yield* fail("runtime_use_installation_requires_migration_transaction");
  const admission = yield* Effect.serviceOption(InstallationAdmission);
  if (
    Option.isNone(admission) ||
    !admission.value.active() ||
    admission.value.sql !== sql ||
    admission.value.fiberId !== (yield* Effect.fiberId)
  )
    return yield* fail("runtime_use_installation_cannot_backfill");
  yield* schemaIdentity(false);
  const rows = yield* sql<{ readonly maximum: number | null }>`
    SELECT max(sequence) AS maximum FROM orchestration_events`;
  const maximum = rows[0]?.maximum;
  if (
    rows.length !== 1 ||
    maximum === undefined ||
    (maximum !== null && (!Number.isSafeInteger(maximum) || maximum <= 0))
  )
    return yield* fail("runtime_use_installation_global_sequence");
  yield* sql`CREATE TABLE jones_workspace_runtime_installation (
    id INTEGER PRIMARY KEY NOT NULL CHECK(id=1),
    basis_json TEXT NOT NULL CHECK(json_valid(basis_json)),
    basis_sha256 TEXT NOT NULL CHECK(length(basis_sha256)=64 AND basis_sha256 NOT GLOB '*[^a-f0-9]*'),
    CHECK(json_extract(basis_json,'$.schema') IS 'jones.workspace-runtime-installation/v1'
      AND json_extract(basis_json,'$.migration.id') IS 150
      AND json_extract(basis_json,'$.migration.name') IS 'WorkspaceRuntimeUses'
      AND ((json_extract(basis_json,'$.globalEventWatermark.kind') IS 'empty'
        AND json_type(basis_json,'$.globalEventWatermark.sequence') IS NULL)
        OR (json_extract(basis_json,'$.globalEventWatermark.kind') IS 'sequence'
          AND json_type(basis_json,'$.globalEventWatermark.sequence') IS 'integer'
          AND json_extract(basis_json,'$.globalEventWatermark.sequence')>0)))
  )`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_installation_no_update
    BEFORE UPDATE ON jones_workspace_runtime_installation
    BEGIN SELECT RAISE(ABORT,'runtime coverage installation is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_installation_no_delete
    BEFORE DELETE ON jones_workspace_runtime_installation
    BEGIN SELECT RAISE(ABORT,'runtime coverage installation is permanent'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_installation_no_replace
    BEFORE INSERT ON jones_workspace_runtime_installation
    WHEN EXISTS(SELECT 1 FROM jones_workspace_runtime_installation)
    BEGIN SELECT RAISE(ABORT,'runtime coverage installation cannot be replaced'); END`;
  const basis: RuntimeUseInstallation = {
    schema: "jones.workspace-runtime-installation/v1",
    migration: { id: 150, name: "WorkspaceRuntimeUses" },
    coverageSourceSha256: digest(runtimeUseCoverageSource),
    journalSchemaSha256: yield* schemaIdentity(true),
    globalEventWatermark:
      maximum === null ? { kind: "empty" } : { kind: "sequence", sequence: maximum },
  };
  yield* decodeInstallation(basis);
  yield* sql`INSERT INTO jones_workspace_runtime_installation (id,basis_json,basis_sha256)
    VALUES (1,${canonical(basis)},${digest(basis)})`;
}).pipe(Effect.mapError((cause) => new SqlError({ reason: new UnknownError({ cause }) })));

/** Missing installation evidence stays missing, including a database whose old
 * candidate139 ran without it. Never read today's MAX to repair that absence. */
export const readRuntimeUseInstallation = Effect.fn("RuntimeUseInstallation.read")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const table = yield* sql`SELECT name FROM sqlite_master WHERE type='table'
    AND name='jones_workspace_runtime_installation'`;
  if (table.length === 0) return null;
  const rows =
    yield* sql`SELECT id,basis_json,basis_sha256 FROM jones_workspace_runtime_installation`;
  if (rows.length !== 1) return yield* fail("runtime_use_installation_missing_or_ambiguous");
  const row = yield* decodeRow(rows[0]);
  const basis = yield* decodeInstallation(yield* parseJson(row.basis_json));
  if (
    canonical(basis) !== row.basis_json ||
    digest(basis) !== row.basis_sha256 ||
    basis.coverageSourceSha256 !== digest(runtimeUseCoverageSource) ||
    basis.journalSchemaSha256 !== (yield* schemaIdentity(true))
  )
    return yield* fail("runtime_use_installation_identity_changed");
  return basis;
}, Effect.mapError(runtimeUseError));
