// @effect-diagnostics nodeBuiltinImport:off -- Allocate durable claim identities within the writer transaction.
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import { WorkstreamOwnerGrant } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import { canonicalWorkstreamsSnapshotJson as recordKey } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Wire from "@t3tools/contracts/jones/workstreamsRollups";
import * as RegistryStore from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Evidence from "./evidence.ts";
import { validateManifest, validateSummary, summaryState } from "./generation.ts";
import * as Bindings from "./bindings.ts";
const decodeWorkstreamsRollupManifest = Schema.decodeUnknownSync(Wire.WorkstreamsRollupManifest);
const decodeWorkstreamsRollupClaim = Schema.decodeUnknownSync(Wire.WorkstreamsRollupClaim);
const decodeWorkstreamsRollupSummary = Schema.decodeUnknownSync(Wire.WorkstreamsRollupSummary);

const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOwnerGrant.fields.permissions),
);
const decodeMetadata = Schema.decodeUnknownEffect(
  Schema.Struct({
    coverage: Schema.Literals(["complete", "partial"]),
    freshness: Wire.WorkstreamsRollupFreshness,
  }),
);

export interface SummaryMetadata {
  readonly owner_id: string;
  readonly workstream_id: string;
  readonly rollup_version: number;
  readonly manifest_sha256: string;
  readonly output_sha256: string;
  readonly account_ids: readonly string[];
  readonly coverage: "complete" | "partial";
  readonly freshness: Evidence.Freshness;
}
export const StoreOptions = Context.Reference<{
  readonly max_dirty_subjects: number;
  readonly max_concurrent: number;
}>("t3/jones/workstreams/native/rollups/StoreOptions", {
  defaultValue: () => ({ max_dirty_subjects: 1000, max_concurrent: 1 }),
});
export class RollupStore extends Context.Service<
  RollupStore,
  {
    readonly verifyAdmission: (
      m: Evidence.Manifest,
    ) => Effect.Effect<boolean, Evidence.RollupError>;
    readonly persistManifest: (m: Evidence.Manifest) => Effect.Effect<void, Evidence.RollupError>;
    readonly manifest: (
      id: string,
      hash: string,
    ) => Effect.Effect<Evidence.Manifest, Evidence.RollupError>;
    readonly enqueue: (
      m: Evidence.Manifest,
      now: string,
    ) => Effect.Effect<"enqueued" | "coalesced" | "unchanged" | "overloaded", Evidence.RollupError>;
    readonly claim: (
      now: string,
      leaseMs: number,
    ) => Effect.Effect<Evidence.Claim | null, Evidence.RollupError>;
    readonly expireClaims: (now: string) => Effect.Effect<number, Evidence.RollupError>;
    readonly nextRollupVersion: (c: Evidence.Claim) => Effect.Effect<number, Evidence.RollupError>;
    readonly failClaim: (
      c: Evidence.Claim,
      code: "input_unavailable" | "provider_failure" | "provider_timeout",
      now: string,
    ) => Effect.Effect<void, Evidence.RollupError>;
    readonly completeClaim: (
      c: Evidence.Claim,
      s: Evidence.Summary,
      now: string,
    ) => Effect.Effect<void, Evidence.RollupError>;
    readonly latestMetadata: (
      id: string,
    ) => Effect.Effect<SummaryMetadata | null, Evidence.RollupError>;
    readonly readSummary: (
      m: SummaryMetadata,
    ) => Effect.Effect<Evidence.Summary, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/store/RollupStore") {}
interface QueueRow {
  readonly workstream_id: string;
  readonly manifest_sha256: string;
  readonly state: string;
  readonly attempt_id: string | null;
  readonly fence: number;
  readonly lease_expires_at: string | null;
  readonly failure_code: string | null;
}
const isRollupError = Schema.is(Evidence.RollupError);
const failure = (e: unknown) =>
  isRollupError(e) ? e : new Evidence.RollupError({ code: "content_unavailable" });
const timestamp = Schema.decodeUnknownSync(Wire.WorkstreamsRollupSummary.fields.generated_at);
const make = Effect.gen(function* () {
  const home = yield* NativeHome,
    sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql,
    registry = yield* RegistryStore.RegistryStore,
    provider = yield* Evidence.RollupEvidenceProvider,
    bindings = yield* Bindings.Bindings,
    options = yield* StoreOptions;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  yield* Evidence.attempt(() =>
    Evidence.requireRollup(
      Number.isSafeInteger(options.max_dirty_subjects) &&
        options.max_dirty_subjects >= 1 &&
        options.max_dirty_subjects <= 10000 &&
        Number.isSafeInteger(options.max_concurrent) &&
        options.max_concurrent >= 1 &&
        options.max_concurrent <= 32,
      "invalid_request",
    ),
  );
  const atomic = <A, E, R>(e: Effect.Effect<A, E, R>) =>
    db.transaction(scope, e).pipe(Effect.mapError(failure));
  const active = Effect.fn("RollupStore.active")(function* () {
    const c = yield* registry.context(scope);
    if (
      !c ||
      c.state !== "active" ||
      c.home_environment_id !== home.home_environment_id ||
      home.served_by_environment_id !== home.home_environment_id ||
      c.server_generation !== home.server_generation
    )
      return yield* new Evidence.RollupError({ code: "content_unavailable" });
    return c;
  });
  const manifest = Effect.fn("RollupStore.manifest")((id: string, hash: string) =>
    atomic(
      Effect.gen(function* () {
        const rows = yield* sql<{
          workstream_version: number;
          registry_version: number;
          envelope: string;
        }>`SELECT * FROM jones_workstreams_rollup_manifests WHERE
      registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${id} AND manifest_sha256=${hash}`;
        const row = rows[0];
        if (!row) return yield* new Evidence.RollupError({ code: "content_unavailable" });
        const value = yield* db.openJson(
          {
            ...scope,
            purpose: "rollup_manifest",
            record_id: recordKey([id, hash]),
            revision: row.registry_version,
          },
          row.envelope,
        );
        return yield* Evidence.attempt(() => {
          const m = decodeWorkstreamsRollupManifest(value, {
            onExcessProperty: "error",
          });
          validateManifest(m);
          Evidence.requireRollup(
            m.owner_id === scope.owner_id &&
              m.workstream_id === id &&
              m.manifest_sha256 === hash &&
              m.registry_version === row.registry_version &&
              m.workstream_version === row.workstream_version,
          );
          return m;
        });
      }),
    ),
  );
  const persistManifest = Effect.fn("RollupStore.persistManifest")((m: Evidence.Manifest) =>
    atomic(
      Effect.gen(function* () {
        yield* Evidence.attempt(() => {
          validateManifest(m);
          Evidence.requireRollup(m.owner_id === scope.owner_id);
        });
        yield* active();
        const old =
          yield* sql`SELECT manifest_sha256 FROM jones_workstreams_rollup_manifests WHERE registry_instance_id=${scope.registry_instance_id}
      AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id} AND manifest_sha256=${m.manifest_sha256}`;
        if (old.length) {
          yield* manifest(m.workstream_id, m.manifest_sha256);
          return;
        }
        yield* registry.put(
          scope,
          "rollup_manifests",
          {
            metadata: {
              workstream_id: m.workstream_id,
              manifest_sha256: m.manifest_sha256,
              workstream_version: m.workstream_version,
              registry_version: m.registry_version,
            },
            value: m,
          },
          null,
        );
        for (const account of new Set(m.sources.map((s) => s.account_id))) {
          const s = m.sources.find((s) => s.account_id === account)!;
          yield* registry.appendPlain(scope, "rollup_manifest_sources", {
            workstream_id: m.workstream_id,
            manifest_sha256: m.manifest_sha256,
            provider_id: provider.id,
            account_id: account,
            authorization_policy_sha256: s.authorization_policy_sha256,
            authorization_principal_id: s.authorization_principal_id,
            authorization_grant_id: s.authorization_grant_id,
            authorization_decision_id: s.authorization_decision_id,
            authorization_account_generation: s.authorization_account_generation,
            authorization_revision: s.authorization_revision,
          });
        }
      }),
    ),
  );
  const queue = (id: string) =>
    sql<QueueRow>`SELECT * FROM jones_workstreams_rollup_queue WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${id}`;
  const enqueue = Effect.fn("RollupStore.enqueue")((m: Evidence.Manifest, now: string) =>
    atomic(
      Effect.gen(function* () {
        yield* Evidence.attempt(() => timestamp(now), "invalid_request");
        yield* persistManifest(m);
        const published =
          yield* sql`SELECT rollup_version FROM jones_workstreams_rollup_summaries WHERE registry_instance_id=${scope.registry_instance_id}
      AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id} AND manifest_sha256=${m.manifest_sha256}`;
        if (published.length) {
          // An imported overlap is logical absence, just as CP deletes only this matching queue input.
          yield* sql`UPDATE jones_workstreams_rollup_queue SET state='dead_letter',attempt_id=NULL,lease_expires_at=NULL,failure_code='published',failed_at=${now},updated_at=${now}
          WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id} AND manifest_sha256=${m.manifest_sha256}
          AND (failure_code IS NULL OR failure_code!='published' OR state!='dead_letter' OR attempt_id IS NOT NULL OR lease_expires_at IS NOT NULL)`;
          return "unchanged" as const;
        }
        const old = (yield* queue(m.workstream_id))[0];
        if (old?.manifest_sha256 === m.manifest_sha256) {
          if (
            old.state === "dead_letter" &&
            old.failure_code === "input_unavailable" &&
            old.fence < 3
          )
            yield* sql`UPDATE jones_workstreams_rollup_queue SET state='pending',attempt_id=NULL,lease_expires_at=NULL,failure_code=NULL,failed_at=NULL,updated_at=${now}
          WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id}`;
          return "coalesced" as const;
        }
        if (!old || old.failure_code === "published") {
          const count = (yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM jones_workstreams_rollup_queue WHERE failure_code IS NULL OR failure_code!='published'`)[0]!
            .count;
          if (count >= options.max_dirty_subjects) return "overloaded" as const;
        }
        if (!old) {
          yield* registry.appendPlain(scope, "rollup_queue", {
            workstream_id: m.workstream_id,
            manifest_sha256: m.manifest_sha256,
            state: "pending",
            attempt_id: null,
            fence: 0,
            lease_expires_at: null,
            failure_code: null,
            failed_at: null,
            updated_at: now,
          });
          return "enqueued" as const;
        }
        yield* sql`UPDATE jones_workstreams_rollup_queue SET manifest_sha256=${m.manifest_sha256},state='pending',attempt_id=NULL,lease_expires_at=NULL,
      failure_code=NULL,failed_at=NULL,updated_at=${now} WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id}`;
        return "coalesced" as const;
      }),
    ),
  );
  const claim = Effect.fn("RollupStore.claim")((now: string, leaseMs: number) =>
    atomic(
      Effect.gen(function* () {
        yield* Evidence.attempt(() => {
          timestamp(now);
          Evidence.requireRollup(
            Number.isSafeInteger(leaseMs) && leaseMs >= 1 && leaseMs <= 299000,
            "invalid_request",
          );
        }, "invalid_request");
        yield* active();
        const running = (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM jones_workstreams_rollup_queue WHERE state='running' AND lease_expires_at>${now}`)[0]!
          .count;
        if (running >= options.max_concurrent) return null;
        const row =
          (yield* sql<QueueRow>`SELECT * FROM jones_workstreams_rollup_queue WHERE registry_instance_id=${scope.registry_instance_id}
      AND owner_id=${scope.owner_id} AND state='pending' ORDER BY updated_at,workstream_id COLLATE BINARY LIMIT 1`)[0];
        if (!row) return null;
        const m = yield* manifest(row.workstream_id, row.manifest_sha256);
        const c: Evidence.Claim = {
          workstream_id: row.workstream_id,
          manifest: m,
          attempt_id: NodeCrypto.randomUUID(),
          fence: row.fence + 1,
          lease_expires_at: DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) + leaseMs)),
        };
        yield* sql`UPDATE jones_workstreams_rollup_queue SET state='running',attempt_id=${c.attempt_id},fence=${c.fence},lease_expires_at=${c.lease_expires_at},updated_at=${now}
      WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${row.workstream_id} AND state='pending'`;
        return Evidence.immutable(c);
      }),
    ),
  );
  const expireClaims = Effect.fn("RollupStore.expireClaims")((now: string) =>
    atomic(
      Effect.gen(function* () {
        yield* Evidence.attempt(() => timestamp(now), "invalid_request");
        yield* active();
        return (yield* sql`UPDATE jones_workstreams_rollup_queue SET state='dead_letter',attempt_id=NULL,lease_expires_at=NULL,failure_code='expired_claim',failed_at=${now},updated_at=${now}
      WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND state='running' AND lease_expires_at<=${now} RETURNING workstream_id`)
          .length;
      }),
    ),
  );
  const assertClaim = Effect.fn("RollupStore.assertClaim")(function* (c: Evidence.Claim) {
    yield* Evidence.attempt(() => {
      decodeWorkstreamsRollupClaim(c);
      validateManifest(c.manifest);
    }, "stale_claim");
    const row = (yield* queue(c.workstream_id))[0];
    if (
      c.manifest.owner_id !== scope.owner_id ||
      !row ||
      row.state !== "running" ||
      row.manifest_sha256 !== c.manifest.manifest_sha256 ||
      row.attempt_id !== c.attempt_id ||
      row.fence !== c.fence ||
      row.lease_expires_at !== c.lease_expires_at
    )
      return yield* new Evidence.RollupError({ code: "stale_claim" });
  });
  const nextRollupVersion = Effect.fn("RollupStore.nextRollupVersion")((c: Evidence.Claim) =>
    atomic(
      Effect.gen(function* () {
        yield* assertClaim(c);
        return (yield* sql<{
          version: number;
        }>`SELECT coalesce(max(rollup_version),0)+1 AS version FROM jones_workstreams_rollup_summaries
      WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${c.workstream_id}`)[0]!
          .version;
      }),
    ),
  );
  const failClaim = Effect.fn("RollupStore.failClaim")(
    (
      c: Evidence.Claim,
      code: "input_unavailable" | "provider_failure" | "provider_timeout",
      now: string,
    ) =>
      atomic(
        Effect.gen(function* () {
          yield* Evidence.attempt(() => timestamp(now), "invalid_request");
          yield* assertClaim(c);
          yield* sql`UPDATE jones_workstreams_rollup_queue SET state='dead_letter',attempt_id=NULL,lease_expires_at=NULL,failure_code=${code},failed_at=${now},updated_at=${now}
      WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${c.workstream_id}`;
        }),
      ),
  );
  const verifyAdmission = Effect.fn("RollupStore.verifyAdmission")((m: Evidence.Manifest) =>
    atomic(
      Effect.gen(function* () {
        yield* Evidence.attempt(() => validateManifest(m));
        if (m.owner_id !== scope.owner_id) return false;
        const context = yield* active(),
          configured = yield* bindings.list();
        const record = (yield* sql<{
          version: number;
        }>`SELECT version FROM jones_workstreams_records WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${m.workstream_id}`)[0];
        if (
          context.registry_version !== m.registry_version ||
          record?.version !== m.workstream_version ||
          !provider.configured
        )
          return false;
        const principals = yield* registry.listPlain(scope, "principals"),
          grants = yield* registry.listPlain(scope, "grants");
        for (const source of m.sources) {
          if (!principals.some((p) => p.principal_id === source.authorization_principal_id))
            return false;
          const grant = grants
            .filter((g) => g.principal_id === source.authorization_principal_id)
            .sort((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
          if (
            !grant ||
            grant.state !== "active" ||
            grant.grant_id !== source.authorization_grant_id ||
            grant.grant_version !== source.authorization_revision
          )
            return false;
          const permissions = yield* decodePermissions(String(grant.permissions));
          if (
            !permissions.includes("workstreams:read") ||
            !configured.some(
              (binding) =>
                binding.state === "active" &&
                binding.workstream_id === m.workstream_id &&
                binding.native_reference_id === source.native_reference_id &&
                binding.account_id === source.account_id &&
                binding.provider_id === provider.id,
            )
          )
            return false;
        }
        return true;
      }),
    ),
  );
  const completeClaim = Effect.fn("RollupStore.completeClaim")(
    (c: Evidence.Claim, s: Evidence.Summary, now: string) =>
      atomic(
        Effect.gen(function* () {
          yield* Evidence.attempt(() => {
            timestamp(now);
            validateSummary(c.manifest, s);
          });
          yield* assertClaim(c);
          const context = yield* active();
          const record = (yield* sql<{
            version: number;
          }>`SELECT version FROM jones_workstreams_records WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${c.workstream_id}`)[0];
          if (
            Date.parse(now) >= Date.parse(c.lease_expires_at) ||
            context.registry_version !== c.manifest.registry_version ||
            record?.version !== c.manifest.workstream_version ||
            s.rollup_version !== (yield* nextRollupVersion(c))
          )
            return yield* new Evidence.RollupError({ code: "stale_claim" });
          if (
            !(yield* verifyAdmission(c.manifest)) ||
            !(yield* provider.verifyCurrentAuthorization(c.manifest))
          )
            return yield* new Evidence.RollupError({ code: "input_unavailable" });
          const state = summaryState(c.manifest);
          yield* registry.put(
            scope,
            "rollup_summaries",
            {
              metadata: {
                workstream_id: s.workstream_id,
                rollup_version: s.rollup_version,
                manifest_sha256: s.manifest_sha256,
                output_sha256: s.output_sha256,
                generated_at: s.generated_at,
                ...state,
              },
              value: s,
            },
            null,
          );
          // Retained foundation rows require a tombstone, excluded from dirty capacity.
          yield* sql`UPDATE jones_workstreams_rollup_queue SET state='dead_letter',attempt_id=NULL,lease_expires_at=NULL,failure_code='published',failed_at=${now},updated_at=${now}
      WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${c.workstream_id}`;
        }),
      ),
  );
  const latestMetadata = Effect.fn("RollupStore.latestMetadata")((id: string) =>
    atomic(
      Effect.gen(function* () {
        const context = yield* active();
        const row = (yield* sql<{
          rollup_version: number;
          manifest_sha256: string;
          output_sha256: string;
          coverage: "complete" | "partial";
          freshness: Evidence.Freshness;
        }>`SELECT rollup_version,manifest_sha256,output_sha256,coverage,freshness
      FROM jones_workstreams_rollup_summaries WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${id} ORDER BY rollup_version DESC LIMIT 1`)[0];
        if (!row) return null;
        yield* decodeMetadata(row);
        const m = yield* manifest(id, row.manifest_sha256),
          account_ids = [...new Set(m.sources.map((s) => s.account_id))].sort();
        const record = (yield* sql<{
          version: number;
        }>`SELECT version FROM jones_workstreams_records WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${id}`)[0];
        if (!record || account_ids.length < 1 || account_ids.length > 100)
          return yield* new Evidence.RollupError({ code: "content_unavailable" });
        const freshness =
          (m.registry_version !== context.registry_version ||
            record.version !== m.workstream_version) &&
          row.freshness === "current"
            ? "stale"
            : row.freshness;
        return Evidence.immutable({
          ...row,
          owner_id: scope.owner_id,
          workstream_id: id,
          account_ids,
          freshness,
        });
      }),
    ),
  );
  const readSummary = Effect.fn("RollupStore.readSummary")((metadata: SummaryMetadata) =>
    atomic(
      Effect.gen(function* () {
        if (metadata.owner_id !== scope.owner_id)
          return yield* new Evidence.RollupError({ code: "not_found" });
        yield* manifest(metadata.workstream_id, metadata.manifest_sha256);
        const row = (yield* sql<{
          envelope: string;
          generated_at: string;
        }>`SELECT envelope,generated_at FROM jones_workstreams_rollup_summaries WHERE registry_instance_id=${scope.registry_instance_id}
      AND owner_id=${scope.owner_id} AND workstream_id=${metadata.workstream_id} AND rollup_version=${metadata.rollup_version} AND manifest_sha256=${metadata.manifest_sha256} AND output_sha256=${metadata.output_sha256}`)[0];
        if (!row) return yield* new Evidence.RollupError({ code: "content_unavailable" });
        const value = yield* db.openJson(
          {
            ...scope,
            purpose: "rollup_summary",
            record_id: recordKey([metadata.workstream_id, metadata.rollup_version]),
            revision: metadata.rollup_version,
          },
          row.envelope,
        );
        return yield* Evidence.attempt(() => {
          const s = decodeWorkstreamsRollupSummary(value, {
            onExcessProperty: "error",
          });
          Evidence.requireRollup(
            s.workstream_id === metadata.workstream_id &&
              s.rollup_version === metadata.rollup_version &&
              s.output_sha256 === metadata.output_sha256 &&
              s.generated_at === row.generated_at,
          );
          return Evidence.immutable(s);
        });
      }),
    ),
  );
  return RollupStore.of({
    verifyAdmission,
    persistManifest,
    manifest,
    enqueue,
    claim,
    expireClaims,
    nextRollupVersion,
    failClaim,
    completeClaim,
    latestMetadata,
    readSummary,
  });
});
/** @public Compose once with the home database and explicit provider ports. */
export const layer = Layer.effect(RollupStore, make);
