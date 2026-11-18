import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture checks native absence before infrastructure admission and creates the exact directory only after that barrier.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Enrollment from "./Enrollment.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Shared from "./EnrollmentJournal.ts";
import * as Journal from "./Journal.ts";
import * as P from "./Protocol.ts";
import { bindingFor, fixture, removalFor, type Fixture } from "./fixtures.ts";
const decodeActualSeal = Schema.decodeUnknownEffect(Schema.fromJsonString(Shared.EnrollmentSeal));

const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });
const startup = Effect.fn("EnrollmentTest.startup")(function* (
  f: Fixture,
  duringLocate: Effect.Effect<void, P.AdmissionError, Scope.Scope> = Effect.void,
) {
  const scope = yield* Effect.scope;
  const raw = P.Participant.of({
    ...f.b.participant,
    identity: { ...f.b.identity, nonce: "b-next-incarnation" },
  });
  const ports = Context.make(P.Participant, raw).pipe(
    Context.add(Journal.JournalConnection, f.b.connection),
    Context.add(P.ScopeObservation, f.b.observations),
  );
  yield* f.b
    .local`CREATE TABLE jones_fixture_inventory_source (id TEXT PRIMARY KEY, revision TEXT NOT NULL)`;
  yield* f.b.local`INSERT INTO jones_fixture_inventory_source VALUES ('retained-source','r1')`;
  yield* f.b
    .local`CREATE TABLE jones_fixture_actual_seals (id TEXT PRIMARY KEY, seal_json TEXT NOT NULL)`;
  const read = f.b.local<{
    id: string;
    revision: string;
  }>`SELECT id,revision FROM jones_fixture_inventory_source ORDER BY id`;
  const inventory = yield* Inventory.makeForSynthetic.pipe(
    Effect.provide(ports),
    Effect.provideService(Inventory.InventoryReaders, {
      coverageContract: raw.identity.coverage,
      requiredDomains: ["synthetic-source"],
      readers: [
        {
          domain: "synthetic-source",
          readLocal: Effect.gen(function* () {
            const records = yield* read.pipe(Effect.mapError(P.storageError));
            return { snapshotDigest: P.digest(records), records };
          }),
          locate: () => Effect.as(duringLocate, []).pipe(Effect.provideService(Scope.Scope, scope)),
          revalidateLocal: Effect.fn("EnrollmentTest.revalidate")(function* (snapshot) {
            if (P.digest(yield* read.pipe(Effect.mapError(P.storageError))) !== snapshot)
              return yield* P.failure("synthetic_source_drift");
          }),
        },
      ],
    }),
  );
  const service = yield* Enrollment.make(f.b.identity.nonce).pipe(
    Effect.provide(ports),
    Effect.provideService(Inventory.EnrollmentInventory, inventory),
    Effect.provideService(Enrollment.EnrollmentStore, {
      validateTerminalReconciliation: () => P.failure("synthetic_no_custody_validator"),
      commit: Effect.fn("EnrollmentTest.commit")((seal: Shared.EnrollmentSeal) =>
        f.b
          .local`INSERT INTO jones_fixture_actual_seals VALUES (${seal.enrollmentId},${P.canonical(seal)})`.pipe(
          Effect.asVoid,
          Effect.mapError(P.storageError),
        ),
      ),
      read: Effect.fn("EnrollmentTest.read")(function* (id) {
        const rows = yield* f.b.local<{
          seal_json: string;
        }>`SELECT seal_json FROM jones_fixture_actual_seals WHERE id=${id}`;
        return rows[0] === undefined ? null : yield* decodeActualSeal(rows[0].seal_json);
      }, Effect.mapError(P.storageError)),
    }),
  );
  const currentJournal = Context.get(
    yield* Layer.build(Journal.layer).pipe(
      Effect.provide(ports),
      Effect.provideService(P.Participant, service.participant),
    ),
    P.CommonJournal,
  );
  const context = f.b.context.pipe(
    Context.add(P.Participant, service.participant),
    Context.add(P.CommonJournal, currentJournal),
  );
  return { service, context, raw, ports };
});

describe("staged enrollment and actual local seal", () => {
  it.effect(
    "staging fences destructive reserve AND entry while unrelated non-destructive work progresses",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const token = yield* P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context));
          yield* P.commitLocalAcceptance(
            token,
            { receiptId: "before-stage", authorityRevision: "r1" },
            Effect.void,
          ).pipe(Effect.provide(f.a.context));
          let calls = 0;
          const during = Effect.gen(function* () {
            const retained = yield* f.a.journal.read(P.operationKey(removalFor(f).operation));
            if (retained === null) return yield* P.failure("missing_accepted_removal");
            yield* refused(f.a.journal.enterOnce(retained));
            // A different command reaches the global barrier before any new local acceptance.
            yield* refused(
              P.reserveScopes(removalFor(f, "second-remove")).pipe(Effect.provide(f.a.context)),
            );
            yield* P.reserveScopes(
              bindingFor(f.a.identity.storeId, [f.unrelated], "unrelated-progress"),
            ).pipe(Effect.provide(f.a.context));
            assert.strictEqual(calls, 0);
          }).pipe(Effect.mapError(P.storageError));
          const pending = yield* startup(f, during);
          yield* refused(
            P.reserveScopes(bindingFor(f.b.identity.storeId, [f.unrelated], "before-ready")).pipe(
              Effect.provide(pending.context),
            ),
          );
          assert.lengthOf(yield* f.b.local`SELECT * FROM jones_common_admission_bridge`, 0);
          yield* pending.service.prepareAndPublish;
          yield* pending.service.gate.assertOutside;
          yield* refused(
            P.reserveScopes(bindingFor(f.b.identity.storeId, [f.unrelated], "old-nonce")).pipe(
              Effect.provide(f.b.context),
            ),
          );
          yield* P.reserveScopes(bindingFor(f.b.identity.storeId, [f.unrelated], "new-nonce")).pipe(
            Effect.provide(pending.context),
          );
          yield* P.enterOnce(
            token,
            Effect.sync(() => {
              calls++;
            }),
          ).pipe(Effect.provide(f.a.context));
          assert.strictEqual(calls, 1);
        }),
      ),
  );
  it.effect(
    "already entered destructive work leaves joining enrollment draining, never ready",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const token = yield* P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context));
          yield* P.commitLocalAcceptance(
            token,
            { receiptId: "entered", authorityRevision: "r1" },
            Effect.void,
          ).pipe(Effect.provide(f.a.context));
          yield* P.enterOnce(token, Effect.void).pipe(Effect.provide(f.a.context));
          const next = yield* startup(f);
          yield* refused(next.service.prepareAndPublish);
          yield* refused(next.service.gate.assertOutside);
          const phase = yield* f.b.common<{
            phase: string;
          }>`SELECT phase FROM jones_common_enrollments WHERE nonce=${next.raw.identity.nonce}`;
          assert.strictEqual(phase[0]?.phase, "draining");
          assert.lengthOf(yield* f.b.local`SELECT * FROM jones_fixture_actual_seals`, 0);
        }),
      ),
  );
  it.effect("snapshot drift retains barrier and a matching caller seal cannot publish it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const next = yield* startup(
          f,
          f.b.local`UPDATE jones_fixture_inventory_source SET revision='r2'`.pipe(
            Effect.asVoid,
            Effect.mapError(P.storageError),
          ),
        );
        yield* refused(next.service.prepareAndPublish);
        yield* refused(next.service.gate.assertOutside);
        yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
        const outsider = yield* Shared.make.pipe(
          Effect.provide(next.ports),
          Effect.provideService(Shared.LocalEnrollment, {
            assertClosed: next.raw.checkOutside,
            verifySeal: () => P.failure("no_actual_retained_seal"),
            validateTerminalReconciliation: () => P.failure("no_actual_terminal_custody"),
          }),
        );
        const rows = yield* f.b.common<{
          enrollment_id: string;
        }>`SELECT enrollment_id FROM jones_common_enrollments WHERE nonce=${next.raw.identity.nonce}`;
        const id = rows[0]!.enrollment_id;
        yield* refused(
          outsider.publish(id, {
            enrollmentId: id,
            storeId: next.raw.identity.storeId,
            participantNonce: next.raw.identity.nonce,
            journalId: f.binding.journalId,
            inventoryDigest: P.digest([]),
            itemCount: 0,
            schemaContractDigest: "matching-json-is-not-authority",
            coverageContract: next.raw.identity.coverage,
            sourceSnapshotDigest: P.digest([]),
          }),
        );
      }),
    ),
  );
  it.effect(
    "no shared enrollment operation or bootstrap infrastructure runs across local SQL",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const next = yield* startup(f);
          yield* f.b.local.withTransaction(refused(next.service.prepareAndPublish));
          yield* refused(next.service.gate.assertOutside);
          assert.lengthOf(
            yield* f.b
              .common`SELECT * FROM jones_common_enrollments WHERE nonce=${next.raw.identity.nonce}`,
            0,
          );
        }),
      ),
  );
  it.effect("infrastructure is durably excluded before a prospective local store is opened", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const infrastructure = yield* Shared.makeInfrastructure.pipe(
          Effect.provideService(Journal.JournalConnection, f.a.connection),
        );
        const permit = yield* infrastructure.beginInfrastructure(f.unrelated);
        yield* Shared.validateInfrastructurePermit(permit, f.unrelated);
        yield* refused(
          Shared.validateInfrastructurePermit({} as Shared.InfrastructurePermit, f.unrelated),
        );
        yield* refused(
          P.reserveScopes(bindingFor(f.a.identity.storeId, [f.unrelated], "state-writer")).pipe(
            Effect.provide(f.a.context),
          ),
        );
        yield* P.reserveScopes(bindingFor(f.b.identity.storeId, [f.scope], "other-work")).pipe(
          Effect.provide(f.b.context),
        );
        yield* refused(infrastructure.beginInfrastructure(f.scope));
      }),
    ),
  );
  it.effect("a missing state directory retains its ancestor until exact creation is observed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const path = NodePath.join(f.directory, "prospective-state");
        const scope = yield* f.a.observations.capture(path);
        assert.isFalse(NodeFS.existsSync(path));
        const infrastructure = yield* Shared.makeInfrastructure.pipe(
          Effect.provideService(Journal.JournalConnection, f.a.connection),
        );
        const original = yield* infrastructure.beginInfrastructure(scope);
        yield* refused(
          P.reserveScopes(
            bindingFor(f.a.identity.storeId, [f.unrelated], "conservative-ancestor"),
          ).pipe(Effect.provide(f.a.context)),
        );
        NodeFS.mkdirSync(path);
        const observed = yield* infrastructure.observeCreatedInfrastructure(original);
        yield* Shared.validateInfrastructurePermit(observed.permit, observed.scope);
        yield* refused(Shared.validateInfrastructurePermit(original, scope));
        yield* P.reserveScopes(
          bindingFor(f.a.identity.storeId, [f.unrelated], "after-observed-creation"),
        ).pipe(Effect.provide(f.a.context));
        const escaped = yield* Effect.scoped(
          Effect.gen(function* () {
            const scoped = yield* Shared.makeInfrastructure.pipe(
              Effect.provideService(Journal.JournalConnection, f.a.connection),
            );
            return yield* scoped.beginInfrastructure(observed.scope);
          }),
        );
        yield* refused(Shared.validateInfrastructurePermit(escaped, observed.scope));
      }),
    ),
  );
});
