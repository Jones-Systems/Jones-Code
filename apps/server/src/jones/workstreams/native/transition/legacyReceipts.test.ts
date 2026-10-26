// @effect-diagnostics nodeBuiltinImport:off -- Synthetic permits and exact fixture readback use Node crypto.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
  WorkstreamsLegacyReceiptLookupResult,
} from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
import vectors from "./fixtures/v1/vectors.json" with { type: "json" };
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import rollupMigration from "../../../persistence/Migrations/127_JonesWorkstreamsRollupAttempts.ts";
import { testLayer, pending } from "../store/sql.testkit.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Effects from "../store/effectStore.ts";
import { canonical, digest } from "./canonical.ts";
import { parseSnapshot, encryptSnapshot, encodeSnapshot } from "./snapshotReader.ts";
import { plan } from "./plan.ts";
import { apply } from "./apply.ts";
import { readback } from "./readback.ts";
import { prepareReverse } from "./reverseExport.ts";
import { activate } from "./activate.ts";
const key = Buffer.alloc(32, 7),
  registry = parseSnapshot(vectors.full_snapshot.canonical_jsonl).header.registry
    .registry_instance_id;
const keys = () =>
  new Map([
    ["workstreams-content-key-synthetic-transition-key", key],
    [`workstreams-index-key-${registry}`, Buffer.alloc(32, 8)],
  ]);
const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ed25519");
const prepare = (
  text = vectors.full_snapshot.canonical_jsonl,
  mapped = true,
  home = "synthetic.home",
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration;
    yield* rollupMigration;
    yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('synthetic-transition-key',${registry},'home','active','2026-10-25T12:00:00.000Z')`;
    const header = parseSnapshot(text).header;
    return yield* plan({
      import_run_id: "70000000-0000-4000-8000-000000000001",
      encrypted: encryptSnapshot(text, "synthetic-transition-key", key),
      stream_key_id: "synthetic-transition-key",
      permit_key_id: "operator",
      permit_public_key: publicKey,
      permit: {
        key_id: "operator",
        header_sha256: digest(canonical(header)),
        signature: NodeCrypto.sign(
          null,
          Buffer.from(canonical(["workstreams-snapshot-export-permit-v1", header])),
          privateKey,
        ).toString("base64"),
      },
      home_environment_id: home,
      active_key_id: "synthetic-transition-key",
      mappings: mapped
        ? [
            {
              principal_id: "20000000-0000-4000-8000-000000000001",
              environment_id: "synthetic.environment",
              source_instance_id: "synthetic.environment",
              evidence_sha256: "a".repeat(64),
            },
          ]
        : [],
    });
  });
it.effect("imports and reads back unchanged F-full receipt evidence", () =>
  Effect.gen(function* () {
    const p = yield* prepare();
    yield* apply(p);
    const result = yield* readback(p);
    expect(result.rows).toEqual(p.snapshot.rows);
    const receipts = yield* Receipts.ReceiptStore;
    const archive = yield* Archive.ArchiveStore;
    const scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
    const principal = "20000000-0000-4000-8000-000000000001";
    for (const row of p.snapshot.rows.filter((r) => r.class === "workstream_command_receipts")) {
      const request = {
        contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
        command_id: String(row.row.command_id),
        expected_server_generation: 7,
        request_sha256: "a".repeat(64),
      } as const;
      const found = yield* receipts.lookupLegacy(scope, principal, request);
      expect(found.result.kind).toBe("legacy_present");
      if (found.result.kind !== "legacy_present") throw new Error("missing retained proof");
      expect(found.result.proof.payload).toBe(row.row.payload);
      expect(found.result.proof.provenance.canonical_row_sha256).toBe(digest(canonical(row)));
      expect(found.result.proof.provenance.ciphertext_sha256).toBe(row.row.ciphertext_sha256);
      expect(found.result.proof.request_bytes).toBe("unknown");
      const codec = Schema.fromJsonString(WorkstreamsLegacyReceiptLookupResult);
      expect(
        yield* Schema.decodeUnknownEffect(codec)(yield* Schema.encodeEffect(codec)(found)),
      ).toEqual(found);
      expect(yield* archive.contains(scope, request.command_id)).toBe(true);
      expect(
        Exit.isFailure(yield* Effect.exit(receipts.lookup(scope, request.command_id, 7))),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(receipts.lookupLegacy(scope, "other.principal", request)),
        ),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            receipts.lookupLegacy(scope, principal, { ...request, request_sha256: "b".repeat(64) }),
          ),
        ),
      ).toBe(true);
      expect(
        (yield* receipts.lookupLegacy(scope, principal, {
          ...request,
          expected_server_generation: 8,
        })).result,
      ).toEqual({ kind: "unknown", reason: "generation_mismatch" });
    }
    expect(
      (yield* receipts.lookupLegacy(scope, principal, {
        contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
        command_id: "missing",
        expected_server_generation: 7,
      })).result,
    ).toEqual({ kind: "unknown", reason: "import_incomplete" });
    expect((yield* apply(p)).applied).toBe(0);
    const sql = yield* SqlClient.SqlClient;
    expect(
      (yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM jones_workstreams_external_reservations`)[0]!.count,
    ).toBe(0);
    const envelopes = yield* sql<{
      envelope: string;
    }>`SELECT envelope FROM jones_workstreams_legacy_archive UNION ALL SELECT envelope FROM jones_workstreams_legacy_receipts`;
    expect(
      envelopes.every((r) => !r.envelope.includes(principal) && !r.envelope.includes("Synthetic")),
    ).toBe(true);
    yield* readback(p);
    yield* activate(p, {
      activation_id: "activation:legacy",
      plan_sha256: p.plan_sha256,
      home_environment_id: p.home_environment_id,
      generations: p.snapshot.header.owners.map((o) => ({
        owner_id: o.owner_id,
        server_generation: o.server_generation,
      })),
    });
    expect(
      (yield* receipts.lookupLegacy(scope, principal, {
        contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
        command_id: "missing",
        expected_server_generation: 7,
      })).result.kind,
    ).toBe("proven_absent");
    yield* sql`UPDATE jones_workstreams_registries SET state='fenced'`;
    const reverse = yield* prepareReverse(p, {
      snapshot_id: "80000000-0000-4000-8000-000000000001",
      exported_at: "2026-10-25T13:00:00.000Z",
      revision: "07cfb3cff76d152f0240b8eac2e0cb1176e42dee",
      fence_epoch: 3,
      stream_key_id: "synthetic-transition-key",
      representable_principals: [principal, "20000000-0000-4000-8000-000000000002"],
    });
    for (const row of p.snapshot.rows.filter((r) => r.class === "workstream_command_receipts"))
      expect(
        reverse.snapshot.rows.find(
          (r) => r.class === row.class && canonical(r.key) === canonical(row.key),
        )?.row.payload,
      ).toBe(row.row.payload);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("unrepresentable open receipts retain unknown bytes and cannot activate or replay", () =>
  Effect.gen(function* () {
    const source = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
    const rows = source.rows.map((r) => {
      if (r.row.command_id !== "command:0") return r;
      if (r.class === "workstream_command_receipts") {
        const { completed_at: _at, error: _error, ...receipt } = JSON.parse(String(r.row.payload));
        return {
          ...r,
          row: {
            ...r.row,
            state: "pending",
            payload: JSON.stringify({ ...receipt, state: "pending", retry_after_seconds: 1 }),
          },
        };
      }
      if (r.class === "workstream_external_reservations")
        return { ...r, row: { ...r.row, state: "active" } };
      return r;
    });
    const p = yield* prepare(encodeSnapshot({ ...source.header, open_effects: 1 }, rows));
    yield* apply(p);
    yield* readback(p);
    const receipts = yield* Receipts.ReceiptStore;
    const scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
    const found = yield* receipts.lookupLegacy(scope, "20000000-0000-4000-8000-000000000001", {
      contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
      command_id: "command:0",
      expected_server_generation: 7,
    });
    expect(found.result.kind).toBe("legacy_present");
    if (found.result.kind !== "legacy_present") throw new Error("missing proof");
    expect(found.result.source).toBe("legacy_open");
    expect(found.result.proof.request_bytes).toBe("unknown");
    expect(found.result.proof.payload).toBe(
      rows.find(
        (r) => r.class === "workstream_command_receipts" && r.row.command_id === "command:0",
      )!.row.payload,
    );
    expect(Exit.isFailure(yield* Effect.exit(receipts.read(scope, "command:0")))).toBe(true);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          activate(p, {
            activation_id: "activation:open",
            plan_sha256: p.plan_sha256,
            home_environment_id: p.home_environment_id,
            generations: p.snapshot.header.owners.map((o) => ({
              owner_id: o.owner_id,
              server_generation: o.server_generation,
            })),
          }),
        ),
      ),
    ).toBe(true);
    expect((yield* apply(p)).applied).toBe(0);
    const sql = yield* SqlClient.SqlClient;
    const active = yield* sql<{
      state: string;
    }>`SELECT state FROM jones_workstreams_external_reservations`;
    expect(active).toEqual([{ state: "active" }]);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect(
  "a native-shaped ID with an unsupported terminal payload stays reserved independently of v1 decoding",
  () =>
    Effect.gen(function* () {
      const source = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
      const id = "command:legacy-retained-0000";
      // Explicit synthetic derivative: the unchanged F-full test above remains the preservation oracle.
      const rows = source.rows.map((r) => {
        const replaced = JSON.parse(JSON.stringify(r).replaceAll("command:0", id));
        if (replaced.class === "workstream_command_receipts" && replaced.row.command_id === id) {
          const { error: _error, ...payload } = JSON.parse(replaced.row.payload);
          replaced.row.payload = JSON.stringify(payload);
        }
        return replaced;
      });
      const p = yield* prepare(encodeSnapshot(source.header, rows));
      yield* apply(p);
      yield* readback(p);
      const receipts = yield* Receipts.ReceiptStore;
      const archive = yield* Archive.ArchiveStore;
      const scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
      expect(yield* archive.contains(scope, id)).toBe(true);
      const fresh = pending(id);
      const attempt = {
        ...fresh,
        receipt: { ...fresh.receipt, owner_id: scope.owner_id, server_generation: 7 },
      };
      yield* activate(p, {
        activation_id: "activation:reserved",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: p.snapshot.header.owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation,
        })),
      });
      const effects = yield* Effects.EffectStore;
      const replay = yield* effects
        .reserve(scope, attempt, {
          command_id: id,
          principal_id: attempt.receipt.actor.principal_id,
          native_reference_id: "synthetic.reference",
          effect: "verify_reference",
          server_generation: 7,
          admitted_registry_version: 7,
          intent: { synthetic: "must not dispatch" },
        })
        .pipe(Effect.flip);
      expect(replay).toMatchObject({ reason: "idempotency_conflict" });
      expect(yield* effects.active(scope)).toEqual([]);
      const conflict = yield* receipts.save(scope, attempt, null).pipe(Effect.flip);
      expect(conflict).toMatchObject({
        _tag: "WorkstreamsStoreError",
        reason: "idempotency_conflict",
      });
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM jones_workstreams_receipts`)[0]!.count,
      ).toBe(0);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect.each([
  ["owner", { owner_id: "other.owner" }],
  ["command", { command_id: "different:id" }],
  ["principal", { actor: { principal_id: "other.principal" } }],
  ["generation", { server_generation: 8 }],
] as const)("refuses conflicting legacy %s before import writes", ([, change]) =>
  Effect.gen(function* () {
    const source = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
    const rows = source.rows.map((r) =>
      r.class !== "workstream_command_receipts" || r.row.command_id !== "command:0"
        ? r
        : {
            ...r,
            row: {
              ...r.row,
              payload: JSON.stringify({ ...JSON.parse(String(r.row.payload)), ...change }),
            },
          },
    );
    expect(Exit.isFailure(yield* Effect.exit(prepare(encodeSnapshot(source.header, rows))))).toBe(
      true,
    );
    const sql = yield* SqlClient.SqlClient;
    expect(
      (yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM jones_workstreams_import_runs`)[0]!.count,
    ).toBe(0);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
