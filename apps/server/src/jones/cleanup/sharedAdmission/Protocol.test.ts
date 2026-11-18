import { assert, describe, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Protocol fixtures exercise exact owned physical removal, partial effects and inode replacement.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/sql/SqlClient";
import * as P from "./Protocol.ts";
import { bindingFor, fixture, makeGraph, removalFor, type Fixture } from "./fixtures.ts";
import * as Bridge from "./LocalBridge.ts";
import { JournalConnection } from "./Journal.ts";

const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(effect);
    assert.isTrue(Exit.isFailure(result));
  });
const accept = (
  token: P.LiveAdmission,
  request: Omit<P.ReservationBinding, "participant">,
  graph: Fixture["a"],
) =>
  P.commitLocalAcceptance(
    token,
    { receiptId: request.localReference, authorityRevision: "revision-1" },
    Effect.gen(function* () {
      yield* P.requireLocalAdmission(token, {
        operationKey: P.operationKey(request.operation),
        bindingSha256: P.digest({ ...request, participant: graph.identity }),
      });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO jones_fixture_acceptances VALUES (${request.operation.originalOperationId})`;
    }),
  );
const retired = (graph: Fixture["a"], certificate: P.LocalCertificate): P.TerminalEvidence => {
  const evidence: P.TerminalEvidence = {
    outcome: "retired",
    certificate,
    evidenceSha256: P.digest("exact-synthetic-custody-proof"),
  };
  graph.terminalEvidence.add(P.digest(evidence));
  return evidence;
};

describe("common admission ordering across independent application stores", () => {
  it.effect(
    "B-before-A: pending reserve blocks removal before any local B acceptance; unrelated scopes progress",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const request = bindingFor(f.b.identity.storeId, [f.scope]);
          yield* P.reserveScopes(request).pipe(Effect.provide(f.b.context));
          assert.lengthOf(yield* f.b.local`SELECT * FROM jones_common_admission_bridge`, 0);
          yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
          const other = bindingFor(f.a.identity.storeId, [f.unrelated], "unrelated");
          yield* Effect.gen(function* () {
            const token = yield* P.reserveScopes(other);
            const accepted = yield* accept(token, other, f.a);
            let calls = 0;
            yield* P.enterOnce(
              token,
              Effect.sync(() => {
                calls++;
              }),
            );
            yield* P.recordTerminalEvidence(token, retired(f.a, accepted.certificate));
            assert.strictEqual(calls, 1);
          }).pipe(Effect.provide(f.a.context));
          assert.strictEqual(
            (yield* f.b.journal.read(P.operationKey(request.operation)))?.phase,
            "pending",
          );
        }),
      ),
  );

  it.effect(
    "A-before-B: reserve and in-callback barriers both fence B before local mutation, while another path runs",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const reserved = yield* Deferred.make<void>();
          const continueAcceptance = yield* Deferred.make<void>();
          const entered = yield* Deferred.make<void>();
          const continueRemove = yield* Deferred.make<void>();
          let calls = 0;
          const worker = yield* Effect.gen(function* () {
            const request = removalFor(f);
            const token = yield* P.reserveScopes(request);
            yield* Deferred.succeed(reserved, undefined);
            yield* Deferred.await(continueAcceptance);
            const local = yield* accept(token, request, f.a);
            yield* P.enterOnce(
              token,
              Effect.gen(function* () {
                calls++;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(continueRemove);
                yield* Effect.sync(() => NodeFS.rmSync(f.aPath, { recursive: true }));
              }),
            );
            const absent = yield* f.a.observations.capture(f.aPath);
            const evidence: P.TerminalEvidence = {
              outcome: "removed",
              certificate: local.certificate,
              evidenceSha256: P.digest("exact-remove-completion-and-absence"),
              absent,
            };
            f.a.terminalEvidence.add(P.digest(evidence));
            yield* P.recordTerminalEvidence(token, evidence);
          }).pipe(Effect.provide(f.a.context), Effect.forkScoped);
          yield* Deferred.await(reserved);
          const blocked = bindingFor(f.b.identity.storeId, [f.scope]);
          yield* refused(P.reserveScopes(blocked).pipe(Effect.provide(f.b.context)));
          assert.lengthOf(yield* f.b.local`SELECT * FROM jones_fixture_acceptances`, 0);
          yield* Deferred.succeed(continueAcceptance, undefined);
          yield* Deferred.await(entered);
          yield* refused(P.reserveScopes(blocked).pipe(Effect.provide(f.b.context)));
          yield* P.reserveScopes(
            bindingFor(f.b.identity.storeId, [f.unrelated], "unrelated-b"),
          ).pipe(Effect.provide(f.b.context));
          yield* Deferred.succeed(continueRemove, undefined);
          yield* Fiber.join(worker);
          assert.strictEqual(calls, 1);
          assert.isFalse(NodeFS.existsSync(f.aPath));
          assert.strictEqual((yield* f.a.journal.readSlot(f.slot.slotId))?.phase, "removed");
          const absent = yield* f.b.observations.capture(f.aPath);
          yield* refused(
            P.reserveScopes(bindingFor(f.b.identity.storeId, [absent], "steal-absent-name")).pipe(
              Effect.provide(f.b.context),
            ),
          );
        }),
      ),
  );

  it.effect.each(["rollback", "lost-local-commit-reply", "lost-shared-ack"] as const)(
    "%s retains a blocking obligation across a fresh participant incarnation",
    (fault) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const request = bindingFor(f.b.identity.storeId, [f.scope]);
          const bridge =
            fault === "lost-local-commit-reply"
              ? P.LocalBridge.of({
                  ...f.b.bridge,
                  accept: (certificate, write) =>
                    f.b.bridge
                      .accept(certificate, write)
                      .pipe(Effect.andThen(P.failure("injected_lost_local_reply"))),
                })
              : f.b.bridge;
          const journal =
            fault === "lost-shared-ack"
              ? P.CommonJournal.of({
                  ...f.b.journal,
                  commitLocalAcceptance: (record, certificate) =>
                    f.b.journal
                      .commitLocalAcceptance(record, certificate)
                      .pipe(Effect.andThen(P.failure("injected_lost_shared_reply"))),
                })
              : f.b.journal;
          yield* Effect.gen(function* () {
            const token = yield* P.reserveScopes(request);
            yield* refused(
              P.commitLocalAcceptance(
                token,
                { receiptId: "receipt", authorityRevision: "revision" },
                fault === "rollback" ? P.failure("injected_rollback") : Effect.void,
              ),
            );
            yield* refused(P.enterOnce(token, Effect.die("must not execute")));
          }).pipe(
            Effect.provideService(P.LocalBridge, bridge),
            Effect.provideService(P.CommonJournal, journal),
            Effect.provide(f.b.context),
          );
          assert.lengthOf(
            yield* f.b.local`SELECT * FROM jones_common_admission_bridge`,
            fault === "rollback" ? 0 : 1,
          );
          f.b.closeOwner();
          const restarted = yield* makeGraph({
            ...f.b,
            identity: { ...f.b.identity, nonce: "b-incarnation-2" },
          });
          yield* refused(P.reserveScopes(request).pipe(Effect.provide(restarted.context)));
          yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
          assert.strictEqual(
            (yield* restarted.journal.read(P.operationKey(request.operation)))?.phase,
            fault === "lost-shared-ack" ? "accepted" : "pending",
          );
        }),
      ),
  );

  it.effect(
    "entered failures are unknown and cannot replay; locally checked terminal evidence can retire them",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* Effect.gen(function* () {
            const request = bindingFor(f.b.identity.storeId, [f.scope]);
            const token = yield* P.reserveScopes(request);
            const local = yield* accept(token, request, f.b);
            let calls = 0;
            yield* refused(
              P.enterOnce(
                token,
                Effect.sync(() => {
                  calls++;
                }).pipe(Effect.andThen(P.failure("partial_effect"))),
              ),
            );
            assert.strictEqual(
              (yield* f.b.journal.read(P.operationKey(request.operation)))?.phase,
              "unknown",
            );
            yield* refused(
              P.enterOnce(
                token,
                Effect.sync(() => {
                  calls++;
                }),
              ),
            );
            yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
            const evidence: P.TerminalEvidence = {
              outcome: "retired",
              certificate: local.certificate,
              evidenceSha256: P.digest("proof"),
            };
            yield* refused(P.recordTerminalEvidence(token, evidence));
            f.b.terminalEvidence.add(P.digest(evidence));
            yield* P.recordTerminalEvidence(token, evidence);
            yield* refused(P.reserveScopes(request));
            yield* P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context));
            assert.strictEqual(calls, 1);
          }).pipe(Effect.provide(f.b.context));
        }),
      ),
  );

  it.effect(
    "same command IDs in two stores remain distinct; copied database UUIDs are rejected",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          for (const graph of [f.a, f.b]) {
            const request = bindingFor(graph.identity.storeId, [f.unrelated]);
            yield* P.reserveScopes(request).pipe(Effect.provide(graph.context));
            assert.strictEqual(
              (yield* graph.journal.read(P.operationKey(request.operation)))?.binding.operation
                .storeId,
              graph.identity.storeId,
            );
          }
          yield* refused(
            makeGraph({
              ...f.b,
              identity: { ...f.b.identity, storeId: f.a.identity.storeId, nonce: "copy" },
            }),
          );
          const copied = yield* makeGraph({
            ...f.b,
            initialization: "existing",
            identity: { ...f.b.identity, storeId: f.a.identity.storeId, nonce: "copy" },
          });
          yield* refused(copied.journal.enroll(copied.identity));
          yield* refused(
            Bridge.pinLocalInstallation.pipe(
              Effect.provideService(P.Participant, {
                ...f.a.participant,
                identity: { ...f.a.identity, database: f.b.identity.database },
              }),
              Effect.provide(f.a.context),
            ),
          );
        }),
      ),
  );

  it.effect(
    "capabilities reject child fibers, wrong SqlClient, wrong binding, reuse, closed scope and closed owner",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          let escaped: P.LiveAdmission | undefined;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const request = bindingFor(f.a.identity.storeId, [f.scope]);
              const token = yield* P.reserveScopes(request);
              escaped = token;
              const child = yield* Effect.exit(
                P.commitLocalAcceptance(
                  token,
                  { receiptId: "child", authorityRevision: "r" },
                  Effect.void,
                ),
              ).pipe(Effect.forkScoped);
              assert.isTrue(Exit.isFailure(yield* Fiber.join(child)));
              const expected = {
                operationKey: P.operationKey(request.operation),
                bindingSha256: P.digest({ ...request, participant: f.a.identity }),
              };
              yield* refused(P.requireLocalAdmission(token, expected));
              yield* refused(
                f.a.local.withTransaction(
                  P.requireLocalAdmission(token, expected).pipe(
                    Effect.provideService(SqlClient.SqlClient, f.b.local),
                  ),
                ),
              );
              yield* refused(
                f.a.local.withTransaction(
                  P.reserveScopes(bindingFor(f.a.identity.storeId, [f.unrelated], "nested")),
                ),
              );
              yield* P.commitLocalAcceptance(
                token,
                { receiptId: request.localReference, authorityRevision: "revision-1" },
                Effect.gen(function* () {
                  yield* refused(
                    P.requireLocalAdmission(token, {
                      ...expected,
                      bindingSha256: P.digest("wrong"),
                    }),
                  );
                  yield* refused(
                    P.requireLocalAdmission(token, expected).pipe(
                      Effect.provideService(SqlClient.SqlClient, f.b.local),
                    ),
                  );
                  yield* refused(
                    P.requireLocalAdmission(
                      JSON.parse(JSON.stringify(token)) as P.LiveAdmission,
                      expected,
                    ),
                  );
                  yield* P.requireLocalAdmission(token, expected);
                }),
              );
              yield* refused(accept(token, request, f.a));
              yield* refused(f.a.local.withTransaction(P.requireLocalAdmission(token, expected)));
              let calls = 0;
              yield* P.enterOnce(
                token,
                Effect.sync(() => {
                  calls++;
                }),
              );
              yield* refused(
                P.enterOnce(
                  token,
                  Effect.sync(() => {
                    calls++;
                  }),
                ),
              );
              assert.strictEqual(calls, 1);
            }),
          ).pipe(Effect.provide(f.a.context));
          if (escaped === undefined) return yield* Effect.die("fixture did not capture token");
          yield* refused(P.enterOnce(escaped, Effect.die("escaped token ran")));
          f.a.closeOwner();
          yield* refused(
            P.reserveScopes(bindingFor(f.a.identity.storeId, [f.unrelated], "closed-owner")).pipe(
              Effect.provide(f.a.context),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a replaced physical target cannot enter, and a missing/replaced journal pin is never silently reset",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* Effect.gen(function* () {
            const request = bindingFor(f.a.identity.storeId, [f.scope]);
            const token = yield* P.reserveScopes(request);
            yield* accept(token, request, f.a);
            NodeFS.renameSync(f.aPath, NodePath.join(f.rootPath, "old-a"));
            NodeFS.mkdirSync(f.aPath);
            yield* refused(P.enterOnce(token, Effect.die("replaced target entered")));
            assert.strictEqual(
              (yield* f.a.journal.read(P.operationKey(request.operation)))?.phase,
              "accepted",
            );
          }).pipe(Effect.provide(f.a.context));
          const wrong = { ...f.a.connection, binding: { ...f.binding, journalId: "replacement" } };
          yield* refused(
            Bridge.pinLocalInstallation.pipe(
              Effect.provideService(JournalConnection, wrong),
              Effect.provide(f.a.context),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a fresh owner can publish exact retained terminal evidence without regaining the old live permit",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const request = bindingFor(f.b.identity.storeId, [f.scope]);
          const accepted = yield* Effect.gen(function* () {
            const token = yield* P.reserveScopes(request);
            const local = yield* accept(token, request, f.b);
            yield* refused(
              P.enterOnce(token, P.failure("synthetic_process_completion_reply_lost")),
            );
            return local.certificate;
          }).pipe(Effect.provide(f.b.context));
          f.b.closeOwner();
          const restarted = yield* makeGraph({
            ...f.b,
            identity: { ...f.b.identity, nonce: "b-new-owner" },
          });
          const retained = yield* restarted.journal.read(P.operationKey(request.operation));
          if (retained === null) return yield* Effect.die("missing retained use");
          const evidence = retired(restarted, accepted);
          yield* refused(
            P.reconcileTerminalEvidence(retained, evidence).pipe(Effect.provide(f.a.context)),
          );
          yield* P.reconcileTerminalEvidence(retained, evidence).pipe(
            Effect.provide(restarted.context),
          );
          yield* refused(P.reserveScopes(request).pipe(Effect.provide(restarted.context)));
          yield* P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context));
        }),
      ),
  );
});
