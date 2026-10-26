import { WORKSTREAMS_SNAPSHOT_CLASSES } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as RegistryStore from "./registryStore.ts";
import * as ArchiveStore from "./archiveStore.ts";
import * as ReceiptStore from "./receiptStore.ts";
import { scope, seed, testLayer, hash, pending, terminal } from "./sql.testkit.ts";

it.effect(
  "legacy lookup remains three-state and a shared multiowner import cannot prove absence until activation and receipts verification",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const registry = yield* RegistryStore.RegistryStore;
      const archive = yield* ArchiveStore.ArchiveStore;
      const receipts = yield* ReceiptStore.ReceiptStore;
      const sql = yield* SqlClient.SqlClient;
      const imported = { ...scope, owner_id: "owner:imported" };
      const context = (yield* registry.context(scope))!;
      yield* registry.initialize({ ...context, ...imported, lineage: "imported" });
      expect(yield* receipts.lookup(imported, "missing", 1)).toEqual({
        kind: "unknown",
        reason: "import_incomplete",
      });
      yield* archive.startImport(scope, {
        import_run_id: "run:multi",
        snapshot_id: "snapshot:multi",
        snapshot_sha256: hash,
        producer: "control-plane",
        contract_version: "1.0.0",
        manifest: {
          registry: { registry_instance_id: scope.registry_instance_id },
          owners: [{ owner_id: scope.owner_id }, { owner_id: imported.owner_id }],
        },
      });
      const request = pending("legacy:command-000");
      const legacy = { ...terminal(request).receipt, owner_id: imported.owner_id };
      yield* archive.addReceipt(imported, "run:multi", legacy, hash, hash);
      expect(yield* receipts.lookup(imported, "legacy:command-000", 1)).toEqual({
        kind: "present",
        source: "legacy_archive",
        receipt: legacy,
      });
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            receipts.save(
              imported,
              { ...request, receipt: { ...request.receipt, owner_id: imported.owner_id } },
              null,
            ),
          ),
        ),
      ).toBe(true);
      yield* sql`INSERT INTO jones_workstreams_import_classes(import_run_id,class,expected_count,expected_sha256,applied_count,state) VALUES ('run:multi','workstreams',0,${hash},0,'planned')`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'applied'`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'verified', observed_sha256 = ${hash}`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'applying'`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'applied'`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'verified'`;
      expect(yield* receipts.lookup(imported, "missing", 1)).toEqual({
        kind: "unknown",
        reason: "import_incomplete",
      });
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_import_runs SET state = 'activated'`),
        ),
      ).toBe(true);
      for (const className of WORKSTREAMS_SNAPSHOT_CLASSES.filter((name) => name !== "workstreams"))
        yield* sql`INSERT INTO jones_workstreams_import_classes(import_run_id,class,expected_count,expected_sha256,applied_count,state) VALUES ('run:multi',${className},0,${hash},0,'planned')`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'applied' WHERE state = 'planned'`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'verified', observed_sha256 = ${hash} WHERE state = 'applied'`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'activated'`;
      expect(yield* receipts.lookup(imported, "missing", 1)).toEqual({
        kind: "proven_absent",
        witness: { server_generation: 1, registry_version: 0, archive: "complete" },
      });
    }).pipe(Effect.provide(testLayer())),
);

it.effect("a missing home content key produces unknown rather than an absence witness", () =>
  Effect.gen(function* () {
    yield* seed;
    const receipts = yield* ReceiptStore.ReceiptStore;
    expect(yield* receipts.lookup(scope, "missing", 1)).toEqual({
      kind: "unknown",
      reason: "key_unavailable",
    });
  }).pipe(Effect.provide(testLayer({ keys: new Map() }))),
);
