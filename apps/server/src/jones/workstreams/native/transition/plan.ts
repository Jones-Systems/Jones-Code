// @effect-diagnostics nodeBuiltinImport:off -- The operator supplies a public verification key, never a credential.
import type * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { WorkstreamReceipt } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Keys from "../crypto/keyProvider.ts";
import { canonical, digest, TransitionError } from "./canonical.ts";
import {
  decryptSnapshot,
  parseSnapshot,
  verifyPermit,
  type Snapshot,
  type SignedPermit,
} from "./snapshotReader.ts";
import { mappingDigest, validatePrincipalMap, type PrincipalMapping } from "./principalMap.ts";
export interface ImportPlan {
  readonly import_run_id: string;
  readonly snapshot: Snapshot;
  readonly home_environment_id: string;
  readonly active_key_id: string;
  readonly mappings: readonly PrincipalMapping[];
  readonly unresolved_sources: readonly string[];
  readonly plan_sha256: string;
}
export function manifestOf(p: Omit<ImportPlan, "plan_sha256">) {
  return {
    ...p.snapshot.header,
    import_run_id: p.import_run_id,
    snapshot_sha256: p.snapshot.snapshot_sha256,
    classes: p.snapshot.classes,
    home_environment_id: p.home_environment_id,
    active_key_id: p.active_key_id,
    mappings: p.mappings,
    unresolved_sources: p.unresolved_sources,
    mapping_sha256: digest(mappingDigest(p.mappings)),
  };
}
const decodeLegacyReceipt = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamReceipt));
export const plan = Effect.fn("WorkstreamsTransition.plan")(function* (input: {
  readonly import_run_id: string;
  readonly encrypted: string;
  readonly stream_key_id: string;
  readonly permit: SignedPermit;
  readonly permit_key_id: string;
  readonly permit_public_key: NodeCrypto.KeyObject;
  readonly home_environment_id: string;
  readonly active_key_id: string;
  readonly mappings: readonly PrincipalMapping[];
}) {
  const keys = yield* Keys.WorkstreamsKeyProvider,
    sql = yield* SqlClient.SqlClient;
  const key = yield* keys.get(input.stream_key_id);
  const snapshot = yield* Effect.try({
    try: () => {
      const s = parseSnapshot(decryptSnapshot(input.encrypted, input.stream_key_id, key));
      if (
        s.header.encryption.key_id !== input.stream_key_id ||
        input.permit.key_id !== input.permit_key_id
      )
        throw new TransitionError({ reason: "key_mismatch" });
      verifyPermit(s.header, input.permit, input.permit_public_key);
      return s;
    },
    catch: () => new TransitionError({ reason: "snapshot_authentication_or_validation" }),
  });
  for (const row of snapshot.rows.filter((r) => r.class === "workstream_command_receipts")) {
    const receipt = yield* decodeLegacyReceipt(row.row.payload).pipe(
      Effect.mapError(
        () => new TransitionError({ reason: "legacy_receipt_contract_incompatible" }),
      ),
    );
    if (
      receipt.command_id !== row.row.command_id ||
      receipt.owner_id !== row.row.owner_id ||
      receipt.actor.principal_id !== row.row.principal_id ||
      receipt.state !== row.row.state
    )
      return yield* new TransitionError({ reason: "legacy_receipt_metadata_conflict" });
  }
  if (
    !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
      input.import_run_id,
    ) ||
    !input.home_environment_id
  )
    return yield* new TransitionError({ reason: "invalid_plan_identity" });
  yield* keys.get(input.active_key_id);
  yield* keys.indexKey(snapshot.header.registry.registry_instance_id);
  const latestSources = new Map<string, { version: number; source: string; state: string }>();
  for (const r of snapshot.rows.filter((r) => r.class === "workstream_source_bindings")) {
    const id = canonical([r.row.owner_id, r.row.provider, r.row.source_instance_id]),
      version = Number(r.row.binding_version);
    if (version > (latestSources.get(id)?.version ?? 0))
      latestSources.set(id, {
        version,
        source: String(r.row.source_instance_id),
        state: String(r.row.state),
      });
  }
  const sources = new Set(
    [...latestSources.values()].filter((r) => r.state === "active").map((r) => r.source),
  );
  const principals = new Set(
    snapshot.rows.flatMap((r) =>
      r.row.principal_id === undefined ? [] : [String(r.row.principal_id)],
    ),
  );
  yield* Effect.try({
    try: () => validatePrincipalMap(input.mappings, principals, sources),
    catch: () => new TransitionError({ reason: "mapping_conflict" }),
  });
  const unresolved_sources = [...sources]
    .filter((s) => !input.mappings.some((m) => m.source_instance_id === s))
    .sort();
  const p = {
    import_run_id: input.import_run_id,
    snapshot,
    home_environment_id: input.home_environment_id,
    active_key_id: input.active_key_id,
    mappings: input.mappings,
    unresolved_sources,
  };
  const plan_sha256 = digest(canonical(manifestOf(p)));
  const prior = yield* sql<{
    snapshot_sha256: string;
  }>`SELECT snapshot_sha256 FROM jones_workstreams_import_runs WHERE import_run_id=${input.import_run_id}`;
  if (prior[0] && prior[0].snapshot_sha256 !== snapshot.snapshot_sha256)
    return yield* new TransitionError({ reason: "import_id_conflict" });
  const homes =
    yield* sql`SELECT owner_id FROM jones_workstreams_registries WHERE registry_instance_id=${snapshot.header.registry.registry_instance_id}`;
  if (homes.length && !prior.length)
    return yield* new TransitionError({ reason: "target_populated" });
  return { ...p, plan_sha256 } satisfies ImportPlan;
});
