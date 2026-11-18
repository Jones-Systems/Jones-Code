import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Shared from "./EnrollmentJournal.ts";
import * as Journal from "./Journal.ts";
import * as P from "./Protocol.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { createRuntimeUseSchema } from "../RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "../RuntimeUseStore.ts";
import deletionMigration from "../../persistence/Migrations/104_JonesDeletionAdmission.ts";
import { bindingFor, fixture } from "./fixtures.ts";

const entryFor = (
  storeId: string,
  scope: P.PhysicalScope,
  referenceId = "legacy-reference",
): Inventory.InventoryEntry => {
  const source = {
    domain: "synthetic-history",
    referenceId,
    revision: "r1",
    sha256: P.digest({ referenceId }),
  };
  return {
    itemKey: Inventory.itemKey(storeId, source),
    source,
    scopes: [scope],
    disposition: "pending",
    owner: null,
    terminalEvidenceSha256: null,
  };
};
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

describe("deterministic retained enrollment inventory", () => {
  it.effect("ordered manifest binds every source revision, scope and disposition", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = entryFor(f.b.identity.storeId, f.scope, "a");
        const b = entryFor(f.b.identity.storeId, f.unrelated, "b");
        yield* Inventory.validateEntries(f.b.identity.storeId, [a, b]);
        assert.strictEqual(Inventory.inventoryDigest([a, b]), Inventory.inventoryDigest([b, a]));
        assert.notStrictEqual(Inventory.inventoryDigest([a, b]), Inventory.inventoryDigest([a]));
        assert.notStrictEqual(
          Inventory.inventoryDigest([a]),
          Inventory.inventoryDigest([{ ...a, disposition: "entered_unknown" }]),
        );
        yield* refused(Inventory.validateEntries(f.b.identity.storeId, [a, a]));
        yield* refused(Inventory.validateEntries(f.b.identity.storeId, [{ ...a, scopes: [] }]));
        yield* refused(
          Inventory.validateEntries(f.b.identity.storeId, [
            { ...a, disposition: "validated_terminal" },
          ]),
        );
      }),
    ),
  );
  it.effect("production inventory refuses an incomplete domain scan", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* refused(
          Inventory.make.pipe(
            Effect.provide(f.a.context),
            Effect.provideService(Inventory.InventoryReaders, {
              coverageContract: "production-coverage",
              requiredDomains: ["ordinaryOwners"],
              readers: [
                {
                  domain: "ordinaryOwners",
                  readLocal: Effect.succeed({ snapshotDigest: P.digest([]), records: [] }),
                  locate: () => Effect.succeed([]),
                  revalidateLocal: () => Effect.void,
                },
              ],
            }),
          ),
        );
      }),
    ),
  );
  it.effect(
    "import uses dedicated historical rows; duplicates are exact and missing sources never delete them",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const identity = { ...f.b.identity, nonce: "history-staging" };
          const participant = P.Participant.of({ ...f.b.participant, identity });
          const ports = Context.make(P.Participant, participant).pipe(
            Context.add(Journal.JournalConnection, f.b.connection),
          );
          const shared = yield* Shared.make.pipe(
            Effect.provide(ports),
            Effect.provideService(Shared.LocalEnrollment, {
              assertClosed: participant.checkOutside,
              verifySeal: () => P.failure("synthetic_no_issued_seal"),
              validateTerminalReconciliation: () =>
                P.failure("synthetic_no_retained_terminal_proof"),
            }),
          );
          yield* shared.beginStore(identity, "history-enrollment", f.b.identity.nonce);
          const entry = entryFor(identity.storeId, f.scope);
          yield* shared.importBatch("history-enrollment", [entry]);
          yield* shared.importBatch("history-enrollment", [entry]);
          yield* shared.importBatch("history-enrollment", []);
          assert.lengthOf(yield* f.b.common`SELECT * FROM jones_common_enrollment_items`, 1);
          assert.lengthOf(yield* f.b.common`SELECT * FROM jones_common_reservations`, 0);
          yield* refused(
            shared.importBatch("history-enrollment", [
              { ...entry, disposition: "entered_unknown" },
            ]),
          );
          yield* refused(
            P.reserveScopes(
              bindingFor(f.a.identity.storeId, [f.scope], "historical-conflict"),
            ).pipe(Effect.provide(f.a.context)),
          );
          yield* P.reserveScopes(
            bindingFor(f.a.identity.storeId, [f.unrelated], "unrelated-history-progress"),
          ).pipe(Effect.provide(f.a.context));
          yield* refused(
            shared.reconcileTerminal(entry.itemKey, 0, {
              ...entry,
              disposition: "validated_terminal",
              terminalEvidenceSha256: P.digest("fabricated"),
            }),
          );
          assert.strictEqual(
            (yield* f.b.common<{
              reconciliation_revision: number;
            }>`SELECT reconciliation_revision FROM jones_common_enrollment_items`)[0]
              ?.reconciliation_revision,
            0,
          );
        }),
      ),
  );
  it.effect(
    "the actual runtime decoder exports retained terminal evidence and rejects caller JSON",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* deletionMigration.pipe(Effect.provideService(SqlClient.SqlClient, f.b.local));
          yield* f.b.local
            .withTransaction(createRuntimeUseSchema)
            .pipe(Effect.provideService(SqlClient.SqlClient, f.b.local));
          const store = yield* makeRuntimeUseStore.pipe(
            Effect.provideService(SqlClient.SqlClient, f.b.local),
          );
          const binding = {
            schema: "jones.workspace-runtime-use/v1",
            useId: "real-decoded-use",
            canonicalPath: f.bPath,
            threadId: "thread",
            directoryIdentity: f.unrelated.ancestors[0].identity,
            actor: {
              kind: "provider_open",
              provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
            },
            original: {
              commandId: null,
              runId: null,
              attemptId: null,
              outboxReference: null,
              nativeReference: null,
              claim: null,
            },
          } as const;
          yield* store.reserve(binding);
          yield* store.recordRetirement({
            schema: "jones.workspace-runtime-retirement/v1",
            useId: binding.useId,
            bindingSha256: P.digest(binding),
            outcome: "not_invoked",
            observationCount: 0,
          });
          const reader = yield* Inventory.makeRuntimeReader.pipe(Effect.provide(f.b.context));
          const snapshot = yield* f.b.local.withTransaction(reader.readLocal);
          const entries = yield* reader.locate(snapshot.records);
          assert.lengthOf(entries, 1);
          assert.strictEqual(entries[0]?.disposition, "validated_terminal");
          assert.isNotNull(entries[0]?.terminalEvidenceSha256);
          yield* Inventory.validateEntries(f.b.identity.storeId, entries);
          yield* refused(reader.locate(JSON.parse(JSON.stringify(snapshot.records))));
          yield* store.reserve({ ...binding, useId: "pending-same-path" });
          yield* f.b.local.withTransaction(
            refused(reader.revalidateLocal(snapshot.snapshotDigest)),
          );
          const newer = yield* f.b.local.withTransaction(reader.readLocal);
          assert.deepStrictEqual(
            (yield* reader.locate(newer.records)).map((entry) => entry.disposition),
            ["entered_unknown", "entered_unknown"],
          );
        }),
      ),
  );
});
