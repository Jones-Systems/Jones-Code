import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Shared from "./EnrollmentJournal.ts";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as P from "./Protocol.ts";
import { fixture, removalFor, bindingFor } from "./fixtures.ts";

const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

describe("shared enrollment manifest publication", () => {
  it.effect(
    "actual local seal plus the complete ordered manifest are both necessary; exact publish retries are idempotent",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const identity = { ...f.b.identity, nonce: "manifest-incarnation" };
          const participant = P.Participant.of({ ...f.b.participant, identity });
          const ports = Context.make(P.Participant, participant).pipe(
            Context.add(Journal.JournalConnection, f.b.connection),
          );
          const source = {
            domain: "synthetic-unknown",
            referenceId: "unlocatable",
            revision: "r1",
            sha256: P.digest("retained-unknown"),
          };
          const entry: Inventory.InventoryEntry = {
            itemKey: Inventory.itemKey(identity.storeId, source),
            source,
            scopes: [],
            disposition: "entered_unknown",
            owner: null,
            terminalEvidenceSha256: null,
          };
          yield* f.b
            .local`CREATE TABLE jones_fixture_manifest (item_key TEXT PRIMARY KEY,entry_json TEXT NOT NULL)`;
          yield* f.b
            .local`CREATE TABLE jones_fixture_seals (id TEXT PRIMARY KEY,seal_json TEXT NOT NULL)`;
          yield* f.b
            .local`INSERT INTO jones_fixture_manifest VALUES (${entry.itemKey},${P.canonical(entry)})`;
          const seal: Shared.EnrollmentSeal = {
            enrollmentId: "manifest-operation",
            storeId: identity.storeId,
            journalId: f.binding.journalId,
            participantNonce: identity.nonce,
            inventoryDigest: Inventory.inventoryDigest([entry]),
            itemCount: 1,
            schemaContractDigest: JournalSchema.contractDigest,
            coverageContract: identity.coverage,
            sourceSnapshotDigest: P.digest([entry]),
          };
          const local = Shared.LocalEnrollment.of({
            assertClosed: participant.checkOutside,
            verifySeal: Effect.fn("ManifestTest.verifyActualLocalSeal")(function* (expected) {
              yield* participant.checkOutside;
              yield* f.b.local
                .withTransaction(
                  Effect.gen(function* () {
                    yield* participant.validateLocal;
                    const rows = yield* f.b.local<{
                      seal_json: string;
                    }>`SELECT seal_json FROM jones_fixture_seals WHERE id=${expected.enrollmentId}`;
                    const actual = yield* f.b.local<{
                      entry_json: string;
                    }>`SELECT entry_json FROM jones_fixture_manifest ORDER BY item_key`;
                    if (
                      rows[0]?.seal_json !== P.canonical(expected) ||
                      actual.length !== 1 ||
                      actual[0]?.entry_json !== P.canonical(entry)
                    )
                      return yield* P.failure(
                        "manifest_actual_local_seal_missing_or_source_changed",
                      );
                  }),
                )
                .pipe(Effect.mapError(P.storageError));
            }),
            validateTerminalReconciliation: () => P.failure("manifest_no_terminal_custody"),
          });
          const shared = yield* Shared.make.pipe(
            Effect.provide(ports),
            Effect.provideService(Shared.LocalEnrollment, local),
          );
          yield* shared.beginStore(identity, seal.enrollmentId, f.b.identity.nonce);
          yield* refused(shared.publish(seal.enrollmentId, seal)); // Matching JSON has no local seal.
          yield* f.b.local.withTransaction(
            f.b
              .local`INSERT INTO jones_fixture_seals VALUES (${seal.enrollmentId},${P.canonical(seal)})`,
          );
          yield* refused(shared.publish(seal.enrollmentId, seal)); // Actual seal still lacks its shared manifest.
          yield* shared.importBatch(seal.enrollmentId, [entry]);
          yield* shared.publish(seal.enrollmentId, seal);
          yield* shared.publish(seal.enrollmentId, seal);
          const phase = yield* f.b.common<{
            phase: string;
          }>`SELECT phase FROM jones_common_enrollments WHERE enrollment_id=${seal.enrollmentId}`;
          assert.strictEqual(phase[0]?.phase, "published");
          // The incomplete-inventory barrier is gone, but this retained unknown still broadly fences destruction.
          yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
          yield* P.reserveScopes(
            bindingFor(f.a.identity.storeId, [f.unrelated], "non-destructive-with-unknown"),
          ).pipe(Effect.provide(f.a.context));
          yield* f.b.local`DELETE FROM jones_fixture_manifest`;
          yield* refused(shared.publish(seal.enrollmentId, seal));
          assert.lengthOf(yield* f.b.common`SELECT * FROM jones_common_enrollment_items`, 1);
        }),
      ),
  );
  it.effect.each(["exact", "wrong-owner-revision", "wrong-inode"] as const)(
    "historical reference discount remains bound to the exact owner and slot incarnation: %s",
    (variant) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const source = {
            domain: "synthetic-owning-reference",
            referenceId: "owner",
            revision: "r1",
            sha256: P.digest(f.slot.owner),
          };
          const reference: Inventory.InventoryEntry = {
            itemKey: Inventory.itemKey(f.a.identity.storeId, source),
            source,
            scopes: [
              variant === "wrong-inode"
                ? {
                    ...f.scope,
                    ancestors: [
                      {
                        ...f.scope.ancestors[0],
                        identity: {
                          ...f.scope.ancestors[0].identity,
                          inode: "different-incarnation",
                        },
                      },
                      ...f.scope.ancestors.slice(1),
                    ],
                  }
                : f.scope,
            ],
            disposition: "reference",
            owner: {
              ...f.slot.owner,
              targetRevision:
                variant === "wrong-owner-revision"
                  ? f.slot.owner.targetRevision + 1
                  : f.slot.owner.targetRevision,
            },
            terminalEvidenceSha256: null,
          };
          // Durable historical exclusion fixture only. No LocalCertificate, terminal receipt,
          // live invocation token or physical cleanup outcome is fabricated by this insertion.
          yield* f.a
            .common`INSERT INTO jones_common_enrollment_items (store_id,item_key,entry_json,entry_sha256)
        VALUES (${f.a.identity.storeId},${reference.itemKey},${P.canonical(reference)},${P.digest(reference)})`;
          const reserve = f.a.journal.reserveScopes({
            ...removalFor(f),
            participant: f.a.identity,
          });
          if (variant === "exact") assert.strictEqual((yield* reserve).phase, "pending");
          else yield* refused(reserve);
        }),
      ),
  );
});
