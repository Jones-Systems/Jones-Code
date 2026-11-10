import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Archive from "./archiveStore.ts";
import { seed, scope, testLayer, hash } from "./sql.testkit.ts";
import {
  readLegacyLedger,
  LEGACY_RECONCILIATION_CLASS,
  reconciliationDigest,
} from "./legacyReconciliationStore.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
it.effect("an encrypted but unqualified schema record cannot supply reconciliation coverage", () =>
  Effect.gen(function* () {
    yield* seed;
    const archive = yield* Archive.ArchiveStore;
    yield* archive.startImport(scope, {
      import_run_id: "run:proof",
      snapshot_id: "snapshot:proof",
      snapshot_sha256: hash,
      producer: "control-plane",
      contract_version: "1.0.0",
      manifest: {
        registry: { registry_instance_id: scope.registry_instance_id },
        owners: [{ owner_id: scope.owner_id }],
      },
    });
    const initial = yield* readLegacyLedger(scope, "run:proof");
    expect(initial.deltas).toEqual([]);
    const fake = { verified: true, result: { native_outcome: "committed" } };
    yield* archive.archive(scope, {
      import_run_id: "run:proof",
      class: LEGACY_RECONCILIATION_CLASS,
      record_key: canonical([scope.owner_id, "0000000000000001", "short"]),
      canonical_sha256: reconciliationDigest(fake),
      value: fake,
    });
    expect(Exit.isFailure(yield* Effect.exit(readLegacyLedger(scope, "run:proof")))).toBe(true);
  }).pipe(Effect.provide(testLayer())),
);
