import { assert, describe, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The contender test retains the exact spawned process and native stream/close handles for interruption-safe joining.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Owned native fixtures exercise real destructive removal and independent-process journal coordination.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as P from "./Protocol.ts";
import { bindingFor, fixture, makeGraph, removalFor, type Fixture } from "./fixtures.ts";

const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });
const accept = (token: P.LiveAdmission, id: string) =>
  P.commitLocalAcceptance(
    token,
    { receiptId: `local-${id}`, authorityRevision: "r1" },
    Effect.void,
  );
const storeEvidence = (f: Fixture, evidence: P.TerminalEvidence) => {
  f.a.terminalEvidence.add(P.digest(evidence));
  return P.recordTerminalEvidence;
};

const contender = (f: Fixture, id: string) =>
  Effect.gen(function* () {
    const resource = yield* Effect.acquireRelease(
      Effect.sync(() => {
        const child = NodeChildProcess.spawn(
          process.execPath,
          [NodePath.join(import.meta.dirname, "contender.fixture.ts")],
          { stdio: ["pipe", "pipe", "pipe"], env: {} },
        );
        let stdout = "";
        let stderr = "";
        let spawnError: unknown;
        child.stdout.setEncoding("utf8").on("data", (part: string) => {
          stdout += part;
        });
        child.stderr.setEncoding("utf8").on("data", (part: string) => {
          stderr = (stderr + part).slice(-4000);
        });
        child.on("error", (cause) => {
          spawnError = cause;
        });
        child.stdin.on("error", (cause) => {
          spawnError = cause;
        });
        const closed = new Promise<number | null>((resolve) => {
          child.once("close", resolve);
        });
        child.stdin.end(
          JSON.stringify({
            localPath: f.bDb,
            commonPath: f.commonPath,
            binding: f.binding,
            identity: f.b.identity,
            reservation: {
              ...bindingFor(f.b.identity.storeId, [f.scope], id),
              participant: f.b.identity,
            },
          }),
        );
        return {
          child,
          closed,
          result: async () => {
            const code = await closed;
            if (spawnError !== undefined) throw spawnError;
            if (code !== 0) throw new Error(`synthetic contender failed (${code}): ${stderr}`);
            return JSON.parse(stdout) as { outcome: string; stage: string };
          },
        };
      }),
      (owned) =>
        Effect.gen(function* () {
          // Cancellation owns only this captured fixture child; join close before
          // releasing its SQLite files, including failure and interrupted tests.
          if (owned.child.exitCode === null && owned.child.signalCode === null)
            owned.child.kill("SIGTERM");
          yield* Effect.promise(() => owned.closed);
        }),
    );
    return yield* Effect.tryPromise({ try: resource.result, catch: P.storageError });
  });

describe("durable journal and slot CAS", () => {
  it.effect(
    "an independent process meets the actual SQLite writer lock, then reserves through the real journal",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* f.a.common.withTransaction(
            Effect.gen(function* () {
              yield* f.a
                .common`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`;
              assert.deepStrictEqual(yield* contender(f, "busy-process"), {
                outcome: "Failure",
                stage: "reserve",
              });
            }),
          );
          assert.isNull(
            yield* f.a.journal.read(
              P.operationKey(bindingFor(f.b.identity.storeId, [f.scope], "busy-process").operation),
            ),
          );
          assert.strictEqual((yield* contender(f, "independent-process")).outcome, "Success");
          yield* refused(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context)));
          assert.strictEqual(
            (yield* f.a.journal.read(
              P.operationKey(
                bindingFor(f.b.identity.storeId, [f.scope], "independent-process").operation,
              ),
            ))?.phase,
            "pending",
          );
        }),
      ),
  );

  it.effect(
    "multi-scope reservation is atomic: a conflict in one target leaves no reservation for either target",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context));
          const combined = bindingFor(
            f.b.identity.storeId,
            [f.unrelated, f.scope],
            "move-both-endpoints",
          );
          yield* refused(P.reserveScopes(combined).pipe(Effect.provide(f.b.context)));
          assert.isNull(yield* f.b.journal.read(P.operationKey(combined.operation)));
          yield* P.reserveScopes(bindingFor(f.b.identity.storeId, [f.unrelated], "other")).pipe(
            Effect.provide(f.b.context),
          );
        }),
      ),
  );

  it.effect.each([false, true])(
    "failed removal (partial=%s) enters once and retains unknown across restart",
    (partial) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          let calls = 0;
          yield* Effect.gen(function* () {
            const token = yield* P.reserveScopes(removalFor(f));
            yield* accept(token, "remove");
            yield* refused(
              P.enterOnce(
                token,
                Effect.gen(function* () {
                  calls++;
                  if (partial) NodeFS.rmSync(NodePath.join(f.aPath, "nested"), { recursive: true });
                  return yield* P.failure("injected_remove_failure");
                }),
              ),
            );
            yield* refused(
              P.enterOnce(
                token,
                Effect.sync(() => {
                  calls++;
                }),
              ),
            );
          }).pipe(Effect.provide(f.a.context));
          assert.strictEqual(calls, 1);
          assert.strictEqual(
            (yield* f.a.journal.read(P.operationKey(removalFor(f).operation)))?.phase,
            "unknown",
          );
          f.a.closeOwner();
          const restarted = yield* makeGraph({
            ...f.a,
            identity: { ...f.a.identity, nonce: "a-restarted" },
          });
          assert.strictEqual(
            (yield* restarted.journal.read(P.operationKey(removalFor(f).operation)))?.phase,
            "unknown",
          );
          yield* refused(
            P.reserveScopes(bindingFor(f.b.identity.storeId, [f.scope])).pipe(
              Effect.provide(f.b.context),
            ),
          );
          yield* refused(
            P.reserveScopes(removalFor(f, "retry-remove")).pipe(Effect.provide(restarted.context)),
          );
        }),
      ),
  );

  it.effect(
    "two complete removal/recreation cycles retain the name and advance the owner target once per consumed certificate",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          let slot = f.slot;
          let removeCalls = 0;
          let mkdirCalls = 0;
          yield* f.a
            .local`CREATE TABLE jones_fixture_owner (id INTEGER PRIMARY KEY, target_revision INTEGER NOT NULL)`;
          yield* f.a.local`INSERT INTO jones_fixture_owner VALUES (1,${slot.owner.targetRevision})`;
          yield* Effect.gen(function* () {
            for (const cycle of [1, 2]) {
              const removeRequest = removalFor(
                { ...f, slot, scope: slot.scope },
                `remove-${cycle}`,
              );
              const removeToken = yield* P.reserveScopes(removeRequest);
              const localRemoval = yield* accept(removeToken, `remove-${cycle}`);
              yield* P.enterOnce(
                removeToken,
                Effect.sync(() => {
                  removeCalls++;
                  NodeFS.rmSync(f.aPath, { recursive: true });
                }),
              );
              const absent = yield* f.a.observations.capture(f.aPath);
              const removal: P.TerminalEvidence = {
                outcome: "removed",
                certificate: localRemoval.certificate,
                evidenceSha256: P.digest({ removeCalls, completion: "returned", absent }),
                absent,
              };
              yield* storeEvidence(f, removal)(removeToken, removal);
              const removed = yield* f.a.journal.readSlot(slot.slotId);
              assert.isNotNull(removed);
              if (removed === null || removed.removalReceipt === null)
                return yield* Effect.die("missing removal receipt");
              yield* refused(
                P.reserveScopes({
                  ...removeRequest,
                  operation: {
                    ...removeRequest.operation,
                    originalOperationId: `remove-again-${cycle}`,
                  },
                }),
              );
              yield* refused(
                P.reserveScopes(bindingFor(f.b.identity.storeId, [absent], `steal-${cycle}`)).pipe(
                  Effect.provide(f.b.context),
                ),
              );
              const recreation: Omit<P.ReservationBinding, "participant"> = {
                ...bindingFor(f.a.identity.storeId, [absent], `recreate-${cycle}`),
                kind: "recreation",
                owner: slot.owner,
                slot: {
                  action: "recreate",
                  slotId: slot.slotId,
                  revision: removed.revision,
                  owner: slot.owner,
                  removalReceipt: removed.removalReceipt,
                },
              };
              const recreateToken = yield* P.reserveScopes(recreation);
              const localRecreation = yield* accept(recreateToken, `recreate-${cycle}`);
              yield* refused(P.reserveScopes(recreation));
              yield* P.enterOnce(
                recreateToken,
                Effect.sync(() => {
                  mkdirCalls++;
                  NodeFS.mkdirSync(f.aPath);
                }),
              );
              yield* refused(
                P.enterOnce(
                  recreateToken,
                  Effect.sync(() => {
                    mkdirCalls++;
                  }),
                ),
              );
              const successor = yield* f.a.observations.capture(f.aPath);
              const materialized: P.TerminalEvidence = {
                outcome: "materialized",
                certificate: localRecreation.certificate,
                evidenceSha256: P.digest({ mkdirCalls, completion: "returned-empty", successor }),
                successor,
              };
              yield* storeEvidence(f, materialized)(recreateToken, materialized);
              const materializedSlot = yield* f.a.journal.readSlot(slot.slotId);
              if (materializedSlot === null) return yield* Effect.die("missing materialized slot");
              const certificate: P.RecreationBindingCertificate = {
                ...localRecreation.certificate,
                receiptId: `consumed-rebind-${cycle}`,
                authorityRevision: `target-${slot.owner.targetRevision + 1}`,
                slotId: slot.slotId,
                slotRevision: materializedSlot.revision,
                ownerBefore: slot.owner,
                oldScope: slot.scope,
                successor,
              };
              yield* refused(P.publishRecreationBinding(certificate));
              yield* refused(
                P.reserveScopes(
                  bindingFor(f.b.identity.storeId, [successor], `early-new-use-${cycle}`),
                ).pipe(Effect.provide(f.b.context)),
              );
              f.a.recreationEvidence.add(P.digest(certificate));
              yield* f.a.bridge.commitRecreationBinding(
                certificate,
                f.a.local`
          UPDATE jones_fixture_owner SET target_revision=target_revision+1 WHERE id=1 AND target_revision=${slot.owner.targetRevision}`,
              );
              yield* refused(
                f.a.bridge.commitRecreationBinding(certificate, Effect.die("replayed owner CAS")),
              );
              slot = yield* P.publishRecreationBinding(certificate);
              assert.strictEqual(slot.owner.targetRevision, f.slot.owner.targetRevision + cycle);
              assert.deepStrictEqual(yield* P.publishRecreationBinding(certificate), slot);
              assert.deepStrictEqual(NodeFS.readdirSync(f.aPath), []);
              assert.strictEqual(slot.scope.canonicalPath, f.scope.canonicalPath);
            }
          }).pipe(Effect.provide(f.a.context));
          assert.strictEqual(removeCalls, 2);
          assert.strictEqual(mkdirCalls, 2);
          const rows = yield* f.a.local<{
            target_revision: number;
          }>`SELECT target_revision FROM jones_fixture_owner WHERE id=1`;
          assert.strictEqual(rows[0]?.target_revision, 3);
        }),
      ),
  );

  it.effect.each(["lost-evidence", "EEXIST"] as const)(
    "%s is unknown and never adopts or retries the directory",
    (mkdirFault) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* Effect.gen(function* () {
            const token = yield* P.reserveScopes(removalFor(f));
            const local = yield* accept(token, "remove");
            yield* P.enterOnce(
              token,
              Effect.sync(() => NodeFS.rmSync(f.aPath, { recursive: true })),
            );
            const absent = yield* f.a.observations.capture(f.aPath);
            const evidence: P.TerminalEvidence = {
              outcome: "removed",
              certificate: local.certificate,
              evidenceSha256: P.digest("returned-remove"),
              absent,
            };
            yield* storeEvidence(f, evidence)(token, evidence);
            const slot = yield* f.a.journal.readSlot(f.slot.slotId);
            if (slot === null || slot.removalReceipt === null)
              return yield* Effect.die("missing removed slot");
            const request: Omit<P.ReservationBinding, "participant"> = {
              ...bindingFor(f.a.identity.storeId, [absent], "recreate"),
              kind: "recreation",
              owner: slot.owner,
              slot: {
                action: "recreate",
                slotId: slot.slotId,
                revision: slot.revision,
                owner: slot.owner,
                removalReceipt: slot.removalReceipt,
              },
            };
            const recreate = yield* P.reserveScopes(request);
            yield* accept(recreate, "recreate");
            let calls = 0;
            yield* refused(
              P.enterOnce(
                recreate,
                Effect.gen(function* () {
                  calls++;
                  NodeFS.mkdirSync(f.aPath);
                  if (mkdirFault === "EEXIST")
                    return yield* Effect.try({
                      try: () => NodeFS.mkdirSync(f.aPath),
                      catch: P.storageError,
                    });
                  return yield* P.failure("completion_evidence_lost");
                }),
              ),
            );
            yield* refused(
              P.enterOnce(
                recreate,
                Effect.sync(() => {
                  calls++;
                  NodeFS.mkdirSync(f.aPath);
                }),
              ),
            );
            yield* refused(
              P.reserveScopes({
                ...request,
                operation: { ...request.operation, originalOperationId: "retry" },
              }),
            );
            assert.strictEqual(calls, 1);
            assert.strictEqual(
              (yield* f.a.journal.readSlot(slot.slotId))?.phase,
              "recreation_invoking",
            );
            assert.strictEqual(
              (yield* f.a.journal.read(P.operationKey(request.operation)))?.phase,
              "unknown",
            );
          }).pipe(Effect.provide(f.a.context));
        }),
      ),
  );
});
