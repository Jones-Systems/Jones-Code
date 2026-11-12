// @effect-diagnostics nodeBuiltinImport:off -- Native file reads prove DB/WAL byte confidentiality; scoped fixtures remove only their captured roots.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as Envelope from "./envelope.ts";
import * as NativeSql from "../store/sql.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as ReceiptStore from "../store/receiptStore.ts";
import * as HistoryStore from "../store/historyStore.ts";
import * as EffectStore from "../store/effectStore.ts";
import * as ArchiveStore from "../store/archiveStore.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as OperationStore from "../store/operationStore.ts";
import {
  scope,
  seed,
  testLayer,
  secrets,
  keyBytes,
  now,
  hash,
  pending,
  terminal,
  binding,
} from "../store/sql.testkit.ts";

it.effect(
  "AES-GCM authenticates every AAD field and rejects ciphertext, nonce, tag and key-ID substitution",
  () => {
    const keys = secrets();
    keys.set("workstreams-content-key-alias", keyBytes);
    return Effect.gen(function* () {
      yield* seed;
      const crypto = yield* Envelope.WorkstreamsEnvelope;
      const context = { ...scope, purpose: "workstream_record", record_id: "ws:test", revision: 1 };
      const sealed = yield* crypto.seal(context, "protected λ text");
      expect(yield* crypto.open(context, sealed)).toBe("protected λ text");
      for (const changed of [
        { ...context, registry_instance_id: "other" },
        { ...context, owner_id: "other" },
        { ...context, purpose: "receipt" },
        { ...context, purpose: "account_record" },
        { ...context, record_id: "ws:other" },
        { ...context, revision: 2 },
      ])
        expect(Exit.isFailure(yield* Effect.exit(crypto.open(changed, sealed)))).toBe(true);
      const parsed = JSON.parse(sealed) as Record<string, string | number>;
      for (const field of ["ct", "nonce", "tag"]) {
        const value = String(parsed[field]);
        const corrupted = JSON.stringify({
          ...parsed,
          [field]: (value.startsWith("A") ? "B" : "A") + value.slice(1),
        });
        expect(Exit.isFailure(yield* Effect.exit(crypto.open(context, corrupted)))).toBe(true);
      }
      expect(
        Exit.isFailure(
          yield* Effect.exit(crypto.open(context, JSON.stringify({ ...parsed, key_id: "alias" }))),
        ),
      ).toBe(true);
      expect(Exit.isFailure(yield* Effect.exit(crypto.open(context, '{"v":1}')))).toBe(true);
    }).pipe(Effect.provide(testLayer({ keys })));
  },
);

it.effect(
  "protected records, exact recovery bytes and import material never appear in SQLite or its WAL",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const dir = yield* Effect.acquireRelease(
          Effect.tryPromise(() =>
            NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "nw-j1-at-rest-")),
          ),
          (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(dir, "native.sqlite");
        const sentinel = "NW_PROTECTED_SENTINEL_λ_9a7b6c";
        yield* Effect.gen(function* () {
          yield* seed;
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe("PRAGMA journal_mode=WAL");
          const registry = yield* RegistryStore.RegistryStore;
          const receipts = yield* ReceiptStore.ReceiptStore;
          const history = yield* HistoryStore.HistoryStore;
          const effects = yield* EffectStore.EffectStore;
          const archive = yield* ArchiveStore.ArchiveStore;
          const bindings = yield* BindingStore.BindingStore;
          const operations = yield* OperationStore.OperationStore;
          const db = yield* NativeSql.NativeSql;
          const protectedValue = {
            name: sentinel,
            notes: sentinel,
            etag: sentinel,
            disposition_text: sentinel,
          };
          yield* registry.put(
            scope,
            "references",
            {
              metadata: {
                native_reference_id: "ref:test",
                provider: "t3",
                resource_kind: "thread",
                id_kind: "internal",
                identity_index: hash,
                legacy_identity_sha256: null,
                attestation_version: 0,
                registration_state: "quarantined",
                created_by_principal: "principal:test",
                routing_source_instance_id: null,
                routing_native_id: null,
                routing_authority_namespace: null,
                routing_store_generation: null,
                routing_attested_at: null,
                routing_expires_at: null,
                routing_evidence_sha256: null,
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "declarations",
            {
              metadata: {
                declaration_id: "declaration:test",
                revision: 1,
                workstream_id: "ws:test",
                registry_version: 0,
                state: "active",
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "appearance_receipts",
            {
              metadata: {
                command_id: "appearance:receipt",
                principal_id: "principal:test",
                request_sha256: hash,
                origin: "native",
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "rollup_bindings",
            {
              metadata: {
                workstream_id: "ws:test",
                native_reference_id: "ref:test",
                binding_version: 1,
                provider_id: "synthetic",
                state: "active",
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "rollup_manifests",
            {
              metadata: {
                workstream_id: "ws:test",
                manifest_sha256: hash,
                workstream_version: 1,
                registry_version: 0,
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "rollup_summaries",
            {
              metadata: {
                workstream_id: "ws:test",
                rollup_version: 1,
                manifest_sha256: hash,
                output_sha256: hash,
                coverage: "complete",
                freshness: "current",
                generated_at: now,
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "records",
            {
              metadata: { workstream_id: "ws:test", version: 1, sort_order: 0 },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "memberships",
            {
              metadata: {
                membership_id: "membership:test",
                workstream_id: "ws:test",
                native_reference_id: "ref:test",
                kind: "primary",
                opened_registry_version: 0,
                closed_registry_version: null,
                closed_at: null,
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "pr_observations",
            {
              metadata: {
                native_reference_id: "ref:test",
                observation_version: 1,
                attempted_at: now,
                outcome: "observed",
              },
              value: protectedValue,
            },
            null,
          );
          yield* registry.put(
            scope,
            "appearance",
            { metadata: { workstream_id: "ws:test", version: 1 }, value: protectedValue },
            null,
          );
          yield* receipts.save(scope, pending("receipt:test-00000", sentinel), null);
          yield* history.append(scope, {
            event_id: "event:test",
            command_id: "receipt:test-00000",
            registry_version: 0,
            occurred_at: now,
            workstream_ids: ["ws:test"],
            value: protectedValue,
          });
          yield* effects.reserve(scope, pending("effect:test-000000", sentinel), {
            command_id: "effect:test-000000",
            principal_id: "principal:test",
            native_reference_id: "ref:test",
            effect: "request_native_t3_settlement",
            server_generation: 1,
            admitted_registry_version: 0,
            intent: protectedValue,
          });
          yield* effects.saveResult(scope, "effect:test-000000", protectedValue);
          yield* archive.startImport(scope, {
            import_run_id: "import:test",
            snapshot_id: "snapshot:test",
            snapshot_sha256: hash,
            producer: "synthetic",
            contract_version: "1.0.0",
            manifest: {
              registry: { registry_instance_id: scope.registry_instance_id },
              owners: [{ owner_id: scope.owner_id }],
              private: sentinel,
            },
          });
          yield* archive.addReceipt(
            scope,
            "import:test",
            terminal(pending("legacy:encrypted-receipt", sentinel)).receipt,
            hash,
            hash,
          );
          yield* archive.archive(scope, {
            import_run_id: "import:test",
            class: "workstreams",
            record_key: "ws:test",
            canonical_sha256: hash,
            value: protectedValue,
          });
          yield* bindings.bind(binding, null);
          yield* operations.accept({
            operation_id: "operation:test",
            kind: "set_primary",
            thread_environment_id: "source:test",
            thread_id: "thread:test",
            origin: { kind: "app" },
            destination: binding,
            child_command_ids: ["child:test"],
            request_body: sentinel,
            accepted_at: now,
          });
          yield* operations.advance("operation:test", "accepted", "dispatching", now, null);
          yield* operations.advance(
            "operation:test",
            "dispatching",
            "reconciling",
            now,
            null,
            sentinel,
          );
          const conflict = yield* db.sealJson(
            { ...scope, purpose: "import_conflict", record_id: "conflict:test", revision: 1 },
            protectedValue,
          );
          yield* sql`INSERT INTO jones_workstreams_import_classes(import_run_id,class,expected_count,expected_sha256,applied_count,state,conflict_envelope) VALUES ('import:test','workstreams',0,${hash},0,'conflict',${conflict})`;
          yield* sql.unsafe("PRAGMA wal_checkpoint(PASSIVE)");
          expect(NodeFS.existsSync(filename + "-wal")).toBe(true);
          expect(NodeFS.readFileSync(filename).includes(Buffer.from(sentinel))).toBe(false);
          expect(NodeFS.readFileSync(filename + "-wal").includes(Buffer.from(sentinel))).toBe(
            false,
          );
          expect((yield* registry.list(scope, "records"))[0]?.value).toEqual(protectedValue);
        }).pipe(Effect.provide(testLayer({ filename })));
      }),
    ),
);
