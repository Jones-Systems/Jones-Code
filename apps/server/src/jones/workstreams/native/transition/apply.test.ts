// @effect-diagnostics nodeBuiltinImport:off -- Ed25519 permit and AES-GCM conformance require Node crypto primitives.
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Transition from "./cli.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as EffectStore from "../store/effectStore.ts";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import vectors from "./fixtures/v1/vectors.json" with { type: "json" };
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { testLayer } from "../store/sql.testkit.ts";
import * as Receipts from "../store/receiptStore.ts";
import { canonical, digest } from "./canonical.ts";
import { parseSnapshot, encryptSnapshot, encodeSnapshot } from "./snapshotReader.ts";
import { plan } from "./plan.ts";
import { apply } from "./apply.ts";
import { readback } from "./readback.ts";
import { prepareReverse } from "./reverseExport.ts";
import {
  prepareTransfer,
  transfer,
  readTransfer,
  applyTransferExtensions,
  readbackTransfer,
  completeTransfer,
} from "./transfer.ts";
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
const compatibleFull = (() => {
  const s = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
  const rows = s.rows.map((r) =>
    JSON.parse(
      JSON.stringify(r).replace(/command:([012])/g, (_, n: string) => `command:000000000000${n}`),
    ),
  );
  return encodeSnapshot(s.header, rows);
})();
const prepare = (text = compatibleFull, mapped = true, home = "synthetic.home") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration;
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
it.effect(
  "imports all 26 classes, resumes interrupted batches, verifies native rows and replays legacy IDs",
  () =>
    Effect.gen(function* () {
      const p = yield* prepare();
      expect((yield* apply(p, 9)).state).toBe("applying");
      const receipts = yield* Receipts.ReceiptStore,
        scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
      expect((yield* receipts.lookup(scope, "missing", 7)).kind).toBe("unknown");
      expect((yield* apply(p)).state).toBe("applied");
      expect((yield* apply(p)).applied).toBe(0);
      const result = yield* readback(p);
      expect(result.snapshot_sha256).toBe(parseSnapshot(compatibleFull).snapshot_sha256);
      expect(result.classes).toHaveLength(26);
      expect((yield* receipts.lookup(scope, "command:0000000000000", 7)).kind).toBe("present");
      expect((yield* receipts.lookup(scope, "missing", 7)).kind).toBe("unknown");
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        envelope: string;
      }>`SELECT envelope FROM jones_workstreams_legacy_archive`;
      expect(
        rows.every((r) => !r.envelope.includes("Synthetic") && !r.envelope.includes("公開安全")),
      ).toBe(true);
      const contexts = yield* sql<{
        state: string;
        server_generation: number;
        registry_version: number;
      }>`SELECT state,server_generation,registry_version FROM jones_workstreams_registries WHERE owner_id='synthetic.owner'`;
      expect(contexts[0]).toEqual({
        state: "importing",
        server_generation: 7,
        registry_version: 7,
      });
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("empty registry has a durable encrypted manifest and 26 verified classes", () =>
  Effect.gen(function* () {
    const p = yield* prepare(vectors.empty_snapshot.canonical_jsonl, false);
    yield* apply(p);
    expect((yield* readback(p)).classes).toHaveLength(26);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("unmapped sources remain fenced and activation does not infer VPS ownership", () =>
  Effect.gen(function* () {
    const p = yield* prepare(undefined, false);
    yield* apply(p);
    yield* readback(p);
    expect(p.unresolved_sources).toContain("synthetic.environment");
    const result = yield* Effect.exit(
      activate(p, {
        activation_id: "activation:fixed",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: p.snapshot.header.owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation,
        })),
      }),
    );
    expect(Exit.isFailure(result)).toBe(true);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("activation refuses an incomplete import without changing its durable state", () =>
  Effect.gen(function* () {
    const p = yield* prepare();
    yield* apply(p, 9);
    const intent = {
      activation_id: "activation:original",
      plan_sha256: p.plan_sha256,
      home_environment_id: p.home_environment_id,
      generations: p.snapshot.header.owners.map((o) => ({
        owner_id: o.owner_id,
        server_generation: o.server_generation,
      })),
    };
    expect(Exit.isFailure(yield* Effect.exit(activate(p, intent)))).toBe(true);
    const sql = yield* SqlClient.SqlClient;
    expect(
      (yield* sql<{ state: string }>`SELECT state FROM jones_workstreams_import_runs`)[0]!.state,
    ).toBe("applying");
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect(
  "the immutable X1 full vector retains original receipt IDs without native command admission",
  () =>
    Effect.gen(function* () {
      const result = yield* prepare(vectors.full_snapshot.canonical_jsonl);
      expect(
        result.snapshot.rows
          .filter((r) => r.class === "workstream_command_receipts")
          .map((r) => r.row.command_id),
      ).toEqual(["command:0", "command:1", "command:2"]);
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM jones_workstreams_import_runs`)[0]!.count,
      ).toBe(0);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect(
  "the real registry guard permits activation without CAS changes; lost replies replay only the original intent",
  () =>
    Effect.gen(function* () {
      const p = yield* prepare(undefined, true);
      yield* apply(p);
      yield* readback(p);
      const intent = {
        activation_id: "activation:original",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: p.snapshot.header.owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation,
        })),
      };
      expect((yield* activate(p, intent)).replayed).toBe(false);
      expect((yield* activate(p, intent)).replayed).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(activate(p, { ...intent, activation_id: "activation:replacement" })),
        ),
      ).toBe(true);
      const receipts = yield* Receipts.ReceiptStore;
      expect(
        (yield* receipts.lookup(
          { registry_instance_id: registry, owner_id: "synthetic.owner" },
          "absent:0000000000000",
          7,
        )).kind,
      ).toBe("proven_absent");
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{
          server_generation: number;
          registry_version: number;
        }>`SELECT server_generation,registry_version FROM jones_workstreams_registries WHERE owner_id='synthetic.owner'`)[0],
      ).toEqual({ server_generation: 7, registry_version: 7 });
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect(
  "reverse/transfer retain exact private strings and fixed lineage at the next generation",
  () =>
    Effect.gen(function* () {
      const p = yield* prepare(undefined, true);
      yield* apply(p);
      yield* readback(p);
      yield* activate(p, {
        activation_id: "activation:original",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: p.snapshot.header.owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation,
        })),
      });
      const registryStore = yield* RegistryStore.RegistryStore;
      const scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
      const first = p.snapshot.rows.find((r) => r.class === "workstreams")!;
      const added = {
        ...JSON.parse(String(first.row.payload)),
        workstream_id: "workstream:native-added",
        sort_order: 10,
      };
      yield* registryStore.put(
        scope,
        "records",
        {
          metadata: { workstream_id: added.workstream_id, version: 1, sort_order: 10 },
          value: added,
        },
        null,
      );
      yield* registryStore.advance(scope, { server_generation: 7, registry_version: 7 });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE jones_workstreams_registries SET state='fenced'`;
      const input = {
        snapshot_id: "80000000-0000-4000-8000-000000000001",
        exported_at: "2026-10-25T13:00:00.000Z",
        revision: "6c1e515c57822e4333ce4e2b75024ebbe521661c",
        fence_epoch: 3,
        stream_key_id: "synthetic-transition-key",
        representable_principals: [
          "20000000-0000-4000-8000-000000000001",
          "20000000-0000-4000-8000-000000000002",
        ],
      };
      const reverse = yield* prepareReverse(p, input);
      for (const original of p.snapshot.rows.filter((r) => r.class === "workstreams"))
        expect(
          reverse.snapshot.rows.find(
            (r) => r.class === "workstreams" && r.row.workstream_id === original.row.workstream_id,
          )!.row.payload,
        ).toBe(original.row.payload);
      expect(
        reverse.snapshot.rows.some(
          (r) => r.class === "workstreams" && r.row.workstream_id === "workstream:native-added",
        ),
      ).toBe(true);
      expect(reverse.next_generations).toContainEqual({
        owner_id: "synthetic.owner",
        server_generation: 8,
      });
      const transferInput = {
          ...input,
          transfer_id: "transfer:fixed",
          destination_home: "synthetic.next-home",
        },
        bundle = yield* prepareTransfer(p, transferInput),
        bundleHash = digest(canonical(bundle));
      const permit = {
        key_id: "operator",
        bundle_sha256: bundleHash,
        signature: NodeCrypto.sign(
          null,
          Buffer.from(canonical(["jones-workstreams-transfer-permit-v1", bundleHash])),
          privateKey,
        ).toString("base64"),
      };
      const output = yield* transfer(p, transferInput, permit, "operator", publicKey),
        decoded = yield* readTransfer({
          encrypted: output.encrypted,
          stream_key_id: input.stream_key_id,
          destination_home: transferInput.destination_home,
          permit,
          permit_key_id: "operator",
          public_key: publicKey,
        });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const receiving = yield* prepare(decoded.snapshot, true, decoded.destination_home);
          yield* apply(receiving);
          yield* applyTransferExtensions(receiving, decoded);
          yield* readbackTransfer(receiving, decoded);
          yield* readback(receiving);
          yield* applyTransferExtensions(receiving, decoded);
          expect(
            (yield* activate(receiving, {
              activation_id: "activation:destination",
              plan_sha256: receiving.plan_sha256,
              home_environment_id: receiving.home_environment_id,
              generations: decoded.next_generations,
            })).state,
          ).toBe("activated");
        }).pipe(Effect.provide(Layer.fresh(testLayer({ keys: keys() })))),
      );
      expect(decoded.lineage.import_run_id).toBe(p.import_run_id);
      expect(decoded.extensions.some((x) => x.class === "jones.legacy_archive")).toBe(true);
      expect((yield* transfer(p, transferInput, permit, "operator", publicKey)).bundle_sha256).toBe(
        output.bundle_sha256,
      );
      expect(
        (yield* completeTransfer(p, {
          transfer_id: decoded.transfer_id,
          destination_home: decoded.destination_home,
          bundle_sha256: output.bundle_sha256,
          next_generations: decoded.next_generations,
        })).state,
      ).toBe("transferred_out");
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("private BOMs and JSON lexical whitespace are retained byte-for-byte", () =>
  Effect.gen(function* () {
    const source = parseSnapshot(compatibleFull),
      target = source.rows.find((r) => r.class === "workstreams")!;
    const payload = "\uFEFF  " + String(target.row.payload) + "  ";
    const text = encodeSnapshot(
      source.header,
      source.rows.map((r) => (r === target ? { ...r, row: { ...r.row, payload } } : r)),
    );
    const p = yield* prepare(text);
    yield* apply(p);
    const r = yield* readback(p);
    expect(r.rows.find((r) => r.class === "workstreams")!.row.payload).toBe(payload);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("a missing content key fails closed before any import run without creating secrets", () =>
  Effect.gen(function* () {
    expect(Exit.isFailure(yield* Effect.exit(prepare()))).toBe(true);
    const sql = yield* SqlClient.SqlClient;
    expect(
      (yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM jones_workstreams_import_runs`)[0]!.count,
    ).toBe(0);
  }).pipe(Effect.provide(testLayer({ keys: new Map() }))),
);

it.effect(
  "resumes from a closed SQLite connection and leaves no private sentinel in DB/WAL bytes",
  () =>
    Effect.gen(function* () {
      const cache = NodeURL.fileURLToPath(
        new URL("../../../../../../../node_modules/.cache/jones/j6/", import.meta.url),
      );
      NodeFS.mkdirSync(cache, { recursive: true });
      const directory = NodeFS.mkdtempSync(NodePath.join(cache, "restart-")),
        filename = NodePath.join(directory, "state.sqlite");
      const source = parseSnapshot(compatibleFull),
        target = source.rows.find((r) => r.class === "workstreams")!;
      const value = JSON.parse(String(target.row.payload));
      value.name = "PRIVATE_J6_DISK_SENTINEL_9f114b";
      const text = encodeSnapshot(
        source.header,
        source.rows.map((r) =>
          r === target ? { ...r, row: { ...r.row, payload: JSON.stringify(value) } } : r,
        ),
      );
      const p = yield* Effect.scoped(
        Effect.gen(function* () {
          const p = yield* prepare(text);
          yield* apply(p, 20);
          return p;
        }).pipe(Effect.provide(testLayer({ filename, keys: keys() }))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* apply(p);
          expect((yield* readback(p)).snapshot_sha256).toBe(p.snapshot.snapshot_sha256);
        }).pipe(Effect.provide(testLayer({ filename, keys: keys() }))),
      );
      for (const path of [filename, filename + "-wal"])
        if (NodeFS.existsSync(path))
          expect(NodeFS.readFileSync(path).includes(Buffer.from(value.name))).toBe(false);
    }),
);

it.effect(
  "open CP requests/intents/results are encrypted, retained and fenced without executing providers",
  () =>
    Effect.gen(function* () {
      const source = parseSnapshot(compatibleFull);
      const command = "command:0000000000000";
      const rows = source.rows.map((r) => {
        if (r.row.command_id !== command) return r;
        if (r.class === "workstream_command_receipts") {
          const old = JSON.parse(String(r.row.payload));
          const { completed_at: _completed, error: _error, ...rest } = old;
          return {
            ...r,
            row: {
              ...r.row,
              state: "pending",
              payload: JSON.stringify({ ...rest, state: "pending", retry_after_seconds: 1 }),
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
      const effects = yield* EffectStore.EffectStore,
        scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
      expect((yield* effects.active(scope))[0]!.command_id).toBe(command);
      const receipts = yield* Receipts.ReceiptStore;
      expect((yield* receipts.read(scope, command))!.request_body).toBe("");

      expect(yield* effects.result(scope, command)).toEqual({ native_outcome: "denied" });
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
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
it.effect("operator service exposes the same transactional empty-import API", () =>
  Effect.gen(function* () {
    const p = yield* prepare(vectors.empty_snapshot.canonical_jsonl, false);
    const service = yield* Transition.WorkstreamsTransition;
    yield* service.apply(p);
    expect((yield* service.readback(p)).state).toBe("verified");
  }).pipe(Effect.provide(Transition.layer.pipe(Layer.provideMerge(testLayer({ keys: keys() }))))),
);

it.effect(
  "reverse omits published queue tombstones while retaining archived and new dirty rows",
  () =>
    Effect.gen(function* () {
      const p = yield* prepare();
      yield* apply(p);
      yield* readback(p);
      yield* activate(p, {
        activation_id: "activation:rollup-reverse",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: p.snapshot.header.owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation,
        })),
      });
      const registryStore = yield* RegistryStore.RegistryStore;
      const scope = { registry_instance_id: registry, owner_id: "synthetic.owner" };
      const first = p.snapshot.rows.find((r) => r.class === "workstreams")!;
      for (const [i, id] of ["workstream:native-published", "workstream:native-dirty"].entries()) {
        yield* registryStore.put(
          scope,
          "records",
          {
            metadata: { workstream_id: id, version: 1, sort_order: 10 + i },
            value: {
              ...JSON.parse(String(first.row.payload)),
              workstream_id: id,
              sort_order: 10 + i,
            },
          },
          null,
        );
      }
      yield* registryStore.advance(scope, { server_generation: 7, registry_version: 7 });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE jones_workstreams_rollup_queue SET state='dead_letter', failure_code='published', failed_at='2026-10-25T12:10:00.000Z' WHERE workstream_id='workstream:0'`;
      yield* sql`INSERT INTO jones_workstreams_rollup_queue SELECT registry_instance_id,owner_id,'workstream:native-published',manifest_sha256,state,attempt_id,fence,lease_expires_at,failure_code,failed_at,updated_at FROM jones_workstreams_rollup_queue WHERE workstream_id='workstream:0'`;
      yield* sql`INSERT INTO jones_workstreams_rollup_queue SELECT registry_instance_id,owner_id,'workstream:native-dirty',manifest_sha256,'pending',NULL,fence,NULL,NULL,NULL,updated_at FROM jones_workstreams_rollup_queue WHERE workstream_id='workstream:0'`;
      yield* sql`UPDATE jones_workstreams_registries SET state='fenced'`;
      const input = {
        snapshot_id: "80000000-0000-4000-8000-000000000001",
        exported_at: "2026-10-25T13:00:00.000Z",
        revision: "0b1b4717897ca82c581251713b6b687375cea074",
        fence_epoch: 3,
        stream_key_id: "synthetic-transition-key",
        representable_principals: [
          "20000000-0000-4000-8000-000000000001",
          "20000000-0000-4000-8000-000000000002",
        ],
      };
      const result = yield* prepareReverse(p, input);
      const rows = result.snapshot.rows.filter((r) => r.class === "workstream_rollup_queue");
      expect(rows.map((r) => r.row.workstream_id).sort()).toEqual([
        "workstream:1",
        "workstream:2",
        "workstream:native-dirty",
      ]);
      expect(rows.some((r) => r.row.failure_code === "published")).toBe(false);
      for (const original of p.snapshot.rows.filter(
        (r) => r.class === "workstream_rollup_queue" && r.row.workstream_id !== "workstream:0",
      ))
        expect(rows.find((r) => r.row.workstream_id === original.row.workstream_id)).toEqual(
          original,
        );
      expect(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM jones_workstreams_rollup_queue`)[0]!.count,
      ).toBe(5);
      expect((yield* prepareReverse(p, input)).canonical_jsonl).toBe(result.canonical_jsonl);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
