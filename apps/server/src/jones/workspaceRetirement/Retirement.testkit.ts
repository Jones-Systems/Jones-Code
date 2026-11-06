// @effect-diagnostics nodeBuiltinImport:off -- Exact disposable fixtures execute production storage, qualification, reservation and unlink/removal paths.
import { canonical } from "./Filesystem.ts";
import * as Schema from "effect/Schema";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { DutyIdentityJson, NativeDutyOwnerJson, RetirementReceiptJson } from "./DurableCodecs.ts";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { fixture, command } from "./Conformance.testkit.ts";
import { Receipts, layer, RetirementError, RetirementReservationOwner } from "./Receipts.ts";
import { retire } from "./RetirementExecutor.ts";
import {
  acquireReservation,
  holderIdentity,
  releaseReservation,
  type ReservationRecord,
} from "./Reservation.ts";
import { binding, incarnation } from "./OutputAttestation.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { enumerateDisposal, disposePath } from "./Disposal.ts";
import { dutyIdentity } from "./DutyIdentity.ts";
import { captureNativeOwner, observeNativeOwner, type NativeOwnerIO } from "./NativeDutyOwner.ts";
import { nativePid } from "./Reservation.ts";
import { nativeServices, type ServiceIO } from "./NativeConsumers.ts";
import { events } from "./EventIntegration.ts";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { settlementAdmission } from "./SettlementAdmission.ts";

async function fixtureSql(
  path: string,
  after?: (query: string, values: unknown[]) => Promise<void>,
  migrate = true,
) {
  const db = new NodeSqlite.DatabaseSync(path);
  for (const name of migrate
    ? ["136_JonesWorkspaceProvenance.ts", "138_JonesWorkspaceRetirement.ts"]
    : []) {
    const source = await NodeFSP.readFile(
      new URL("../persistence/Migrations/" + name, import.meta.url),
      "utf8",
    );
    for (const statement of source.matchAll(/yield\* sql`([^`]+)`/g)) db.exec(statement[1]!);
  }
  let depth = 0;
  const sql = Object.assign(
    (parts: TemplateStringsArray, ...values: unknown[]) =>
      Effect.tryPromise({
        try: async () => {
          const query = parts.join("?");
          const rows = db.prepare(query).all(...(values as (string | number | null)[]));
          await after?.(query, values);
          return rows;
        },
        catch: (cause) => new SqlError({ reason: new UnknownError({ cause }) }),
      }),
    {
      withTransaction: <A, E, R>(body: Effect.Effect<A, E, R>) =>
        Effect.suspend(() => {
          const name = "test" + depth++,
            outer = depth === 1;
          db.exec(outer ? "BEGIN IMMEDIATE" : "SAVEPOINT " + name);
          return body.pipe(
            Effect.tap(() => Effect.sync(() => db.exec(outer ? "COMMIT" : "RELEASE " + name))),
            Effect.catchCause((cause) =>
              Effect.sync(() => db.exec(outer ? "ROLLBACK" : "ROLLBACK TO " + name)).pipe(
                Effect.andThen(Effect.failCause(cause)),
              ),
            ),
            Effect.ensuring(
              Effect.sync(() => {
                depth--;
              }),
            ),
          );
        }),
    },
  ) as unknown as SqlClient.SqlClient;
  return { sql, db, close: () => db.close() };
}
export const retirementCases: Record<string, () => Promise<void>> = {
  "production138 DDL is atomic in actual SQLite and retries without inventing duty identity": () =>
    fixture(async ({ base }) => {
      const f = await fixtureSql(NodePath.join(base, "migration.sqlite3"), undefined, false);
      const migration = (await import("../persistence/Migrations/138_JonesWorkspaceRetirement.ts"))
        .default;
      const run = (fail: boolean) =>
        Effect.runPromise(
          f.sql
            .withTransaction(
              migration.pipe(
                Effect.andThen(
                  fail ? Effect.fail(Error("controlled migration failure")) : Effect.void,
                ),
              ),
            )
            .pipe(Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
      try {
        await NodeAssert.rejects(run(true), /controlled migration failure/);
        NodeAssert.deepEqual(
          f.db.prepare("SELECT name FROM sqlite_master WHERE name LIKE 'jones_%'").all(),
          [],
        );
        await run(false);
        NodeAssert.deepEqual(
          f.db
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .all()
            .map((r) => r.name),
          [
            "jones_workspace_disposal_journal",
            "jones_workspace_duty_claims",
            "jones_workspace_duty_identity",
            "jones_workspace_retirement_intents",
            "jones_workspace_retirement_receipts",
            "jones_workspace_retirement_state",
          ],
        );
        NodeAssert.deepEqual(f.db.prepare("SELECT * FROM jones_workspace_duty_identity").all(), []);
      } finally {
        f.close();
      }
    }),
  "loaded native service observations use bounded manager projections and retain unknown coverage":
    async () => {
      const calls: string[][] = [];
      const linux: ServiceIO = {
        uid: () => 501,
        command: async (_file, args) => {
          calls.push(args);
          return args.includes("list-units")
            ? "fixture.service loaded inactive dead Fixture\n"
            : "Id=fixture.service\nLoadState=loaded\nWorkingDirectory=/fixture/checkout\nExecStart={ path=/outside/tool ; argv[]=/outside/tool ; }\n";
        },
      };
      const result = await nativeServices("/fixture/checkout", "linux", linux);
      NodeAssert.equal(result.status, "complete");
      NodeAssert.deepEqual(result.paths, ["/fixture/checkout"]);
      NodeAssert.equal(
        calls.some((args) => args.some((a) => a.includes("Environment"))),
        false,
      );
      const darwin: ServiceIO = {
        uid: () => 501,
        command: async (_file, args) =>
          args[1]?.split("/").length === 2
            ? "domain = {\n\tservices = {\n\t\t0 0 fixture.service\n\t}\n}\n"
            : "fixture = {\n program = /outside/tool\n working directory = /fixture/checkout\n environment = {\n SECRET = never-returned\n }\n}\n",
      };
      const projected = await nativeServices("/fixture/checkout", "darwin", darwin);
      NodeAssert.equal(projected.status, "complete");
      NodeAssert.deepEqual(projected.paths, ["/fixture/checkout"]);
      NodeAssert.equal(JSON.stringify(projected).includes("never-returned"), false);
      const unavailable = await nativeServices("/fixture/checkout", "darwin", {
        ...darwin,
        command: async () => {
          throw Error("controlled manager unavailable");
        },
      });
      NodeAssert.equal(unavailable.status, "unavailable");
      NodeAssert.equal(unavailable.paths.length, 0);
    },
  "event admission uses batch birth binding without eager projection lookups and releases actual terminal runs":
    () =>
      fixture(async ({ base, worktree }) => {
        const f = await fixtureSql(NodePath.join(base, "events.sqlite3"));
        f.db.exec(
          "CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)",
        );
        const occurredAt = DateTime.nowUnsafe();
        const birth = {
          type: "thread.created",
          id: "birth",
          threadId: "thread",
          occurredAt,
          payload: { id: "thread", worktreePath: worktree },
        };
        const queued = {
          type: "run.created",
          id: "queued",
          threadId: "thread",
          occurredAt,
          payload: { id: "run" },
        };
        try {
          await Effect.runPromise(
            f.sql.withTransaction(
              events(f.sql, [birth, queued] as unknown as OrchestrationV2DomainEvent[]),
            ),
          );
          NodeAssert.equal(
            f.db
              .prepare(
                "SELECT incarnation_id FROM jones_workspace_duty_claims WHERE claim_id='queued-turn:run'",
              )
              .get()?.incarnation_id !== null,
            true,
          );
          f.db
            .prepare("INSERT INTO orchestration_v2_projection_threads VALUES(?,?)")
            .run("thread", JSON.stringify({ worktreePath: worktree }));
          const terminal = {
            ...queued,
            type: "run.updated",
            payload: { id: "run", status: "completed" },
          };
          await Effect.runPromise(
            f.sql.withTransaction(
              events(f.sql, [terminal] as unknown as OrchestrationV2DomainEvent[]),
            ),
          );
          NodeAssert.equal(
            typeof f.db
              .prepare(
                "SELECT released_at FROM jones_workspace_duty_claims WHERE claim_id='queued-turn:run'",
              )
              .get()?.released_at,
            "string",
          );
          f.db
            .prepare("INSERT INTO jones_workspace_retirement_state VALUES(?,?,'retiring',?)")
            .run(worktree, "physical", "receipt");
          await NodeAssert.rejects(
            Effect.runPromise(
              f.sql.withTransaction(
                events(f.sql, [
                  { ...birth, id: "new-birth" },
                ] as unknown as OrchestrationV2DomainEvent[]),
              ),
            ),
            /retiring/,
          );
        } finally {
          f.close();
        }
      }),
  "real claim binding survives absent or malformed optional provenance and rejects replaced paths":
    () =>
      fixture(async ({ worktree, input, base }) => {
        const first = await dutyIdentity(worktree);
        NodeAssert.equal(first.status, "bound");
        await NodeFSP.writeFile(
          NodePath.join(input.expected.git_dir, "workspace-provenance.json"),
          "not optional JSON",
        );
        NodeAssert.deepEqual(await dutyIdentity(worktree), first);
        await NodeFSP.rename(worktree, NodePath.join(base, "old-checkout"));
        await NodeFSP.mkdir(worktree);
        NodeAssert.notDeepEqual(await dutyIdentity(worktree), first);
        await NodeFSP.rmdir(worktree);
        await NodeFSP.symlink(NodePath.join(base, "old-checkout"), worktree);
        NodeAssert.equal((await dutyIdentity(worktree)).status, "unbound");
      }),
  "native owner reconciliation retains children after launcher/server exit and never clears unknown":
    async () => {
      const host = {
        host: "fixture",
        boot_id: "boot",
        process_namespace: "fixture-pid-namespace",
        pid: nativePid,
        process_start: "server-start",
      };
      let listing = `${nativePid} server-session server-start\n12345 child-session child-start\n`;
      const io: NativeOwnerIO = {
        host: async () => host,
        processes: async () => listing,
        linuxIdentity: async () => ({ bootId: "boot", startTime: "100" }),
      };
      const owner = await captureNativeOwner(12345, undefined, io);
      NodeAssert.equal(owner.type, "native-session");
      listing = "12346 child-session descendant-start\n";
      NodeAssert.equal(await observeNativeOwner(owner, io), "present");
      NodeAssert.equal(
        await observeNativeOwner(owner, {
          ...io,
          processes: async () => {
            throw Error("unavailable");
          },
        }),
        "unknown",
      );
      NodeAssert.equal(
        await observeNativeOwner(
          { schema: "workspace-duty-owner/v1", type: "in-process", host, nonce: "handoff" },
          io,
        ),
        "unknown",
      );
      NodeAssert.equal(
        await observeNativeOwner(owner, {
          ...io,
          host: async () => ({ ...host, process_namespace: "different-namespace" }),
        }),
        "unknown",
      );
      listing = "12346 unrelated-session reused-pid\n";
      NodeAssert.equal(await observeNativeOwner(owner, io), "verified_absent");
      NodeAssert.equal(
        await observeNativeOwner(owner, {
          ...io,
          host: async () => ({ ...host, host: "other-host" }),
        }),
        "unknown",
      );
      NodeAssert.equal(
        await observeNativeOwner(owner, {
          ...io,
          host: async () => ({ ...host, boot_id: "new-boot" }),
        }),
        "verified_absent",
      );
      NodeAssert.equal(
        await observeNativeOwner(owner, { ...io, host: async () => ({ ...host, boot_id: "" }) }),
        "unknown",
      );
      listing = `${nativePid} shared server-start\n12345 shared child-start\n`;
      NodeAssert.equal((await captureNativeOwner(12345, undefined, io)).type, "unresolved");
    },
  "durable settlement refuses unfinished scripts at ten minutes and missing final checkpoints":
    async () => {
      NodeAssert.equal(
        settlementAdmission({
          script: "running",
          startedAt: 0,
          now: 600000,
          checkpoint: "ready",
          enabled: true,
        }).reason,
        "settle_script_unfinished",
      );
      NodeAssert.equal(
        settlementAdmission({
          script: "completed",
          startedAt: 0,
          now: 0,
          checkpoint: "pending",
          enabled: true,
        }).reason,
        "final_checkpoint_unfinished",
      );
      NodeAssert.equal(
        settlementAdmission({
          script: "completed",
          startedAt: 0,
          now: 0,
          checkpoint: "ready",
          enabled: false,
        }).decision,
        "propose",
      );
    },
  "common-store reservation excludes any participant until exact terminal release": () =>
    fixture(async ({ input }) => {
      const bound = input.expected,
        record: ReservationRecord = {
          schema: "workspace-reservation/v1",
          admin_id: bound.admin_id,
          incarnation: incarnation(bound),
          executor: "jones-fixture",
          receipt: "fixture-receipt",
          ...(await holderIdentity()),
        };
      await acquireReservation(bound, record);
      let writes = 0;
      try {
        await NodeAssert.rejects(
          participantGuard(bound, async () => {
            writes++;
          }),
          /retiring/,
        );
        await NodeAssert.rejects(
          acquireReservation(bound, { ...record, receipt: "second" }),
          /retiring/,
        );
        await NodeAssert.rejects(
          acquireReservation(
            bound,
            { ...record, receipt: "third" },
            { holderAbsent: async () => true, receiptReadback: async () => false },
          ),
          /reservation_recovery_required/,
        );
        NodeAssert.equal(writes, 0);
      } finally {
        await releaseReservation(bound, record);
      }
      await participantGuard(bound, async () => {
        writes++;
      });
      NodeAssert.equal(writes, 1);
    }),
  "receipt and retiring transaction reject claims; verified absence is required for reconciliation":
    () =>
      fixture(async ({ base, worktree }) => {
        const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"));
        try {
          await Effect.runPromise(
            Effect.gen(function* () {
              const store = yield* Receipts;
              yield* store.claim({
                id: "session",
                worktree,
                kind: "session",
                owner: "fixture",
                holder: '{"native":"fixture"}',
              });
              NodeAssert.equal((yield* store.claims(worktree)).length, 1);
              NodeAssert.equal(
                typeof f.db
                  .prepare(
                    "SELECT incarnation_id FROM jones_workspace_duty_claims WHERE claim_id='session'",
                  )
                  .get()?.incarnation_id,
                "string",
              );
              const facts = f.db
                .prepare(
                  "SELECT identity_json,native_owner_json FROM jones_workspace_duty_identity WHERE claim_id='session'",
                )
                .get()!;
              NodeAssert.equal(Schema.decodeSync(DutyIdentityJson)(String(facts.identity_json)).status, "bound");
              NodeAssert.equal(Schema.decodeSync(NativeDutyOwnerJson)(String(facts.native_owner_json)).type, "unresolved");
              const rejected = yield* store
                .reconcileClaim("session", '{"native":"fixture"}', false)
                .pipe(Effect.result);
              NodeAssert.equal(rejected._tag, "Failure");
              yield* store.reconcileClaim("session", '{"native":"different"}', true);
              NodeAssert.equal((yield* store.claims(worktree)).length, 1);
              yield* store.reconcileClaim("session", '{"native":"fixture"}', true);
              NodeAssert.equal((yield* store.claims(worktree)).length, 0);
            }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
          );
        } finally {
          f.close();
        }
      }),
  "production executor retires a clean fixture with durable receipt and retains branch": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"));
      try {
        const result = await Effect.runPromise(
          retire({
            cause: "sweep",
            projectRoot: repo,
            worktree,
            maxLevel: "branch-retained",
            admit: Effect.void,
            observe: Effect.succeed(input),
            revalidate: Effect.void,
            finalizeOutputs: () => Effect.void,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "retired");
        NodeAssert.equal(
          await NodeFSP.lstat(worktree).then(
            () => true,
            () => false,
          ),
          false,
        );
        NodeAssert.equal(
          f.db.prepare("SELECT state FROM jones_workspace_retirement_receipts").get()?.state,
          "retired",
        );
        NodeAssert.equal(
          (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
          true,
        );
        NodeAssert.equal(
          f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
          "retired",
        );
      } finally {
        f.close();
      }
    }),
  "executor readbacks carry only the exact live reservation and scope it away after removal": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "owned-reservation.sqlite3"));
      let reads = 0,
        mutations = 0;
      try {
        const result = await Effect.runPromise(
          retire({
            cause: "deletion",
            projectRoot: repo,
            worktree,
            maxLevel: "branch-retained",
            admit: Effect.void,
            observe: Effect.succeed(input),
            revalidate: Effect.gen(function* () {
              const own = yield* RetirementReservationOwner;
              if (own === null) return;
              reads++;
              NodeAssert.equal(own.worktree, worktree);
              const common = new NodeSqlite.DatabaseSync(
                NodePath.join(input.expected.git_common_dir, "workspace-lifecycle", "reservations.sqlite3"),
                { readOnly: true },
              );
              try {
                NodeAssert.equal(
                  common.prepare("SELECT record FROM reservations WHERE admin_id=?").get(own.record.admin_id)?.record,
                  canonical(own.record),
                );
              } finally {
                common.close();
              }
              const current = f.db
                .prepare("SELECT incarnation_id,receipt_id FROM jones_workspace_retirement_state WHERE worktree=?")
                .get(worktree);
              NodeAssert.equal(current?.incarnation_id, own.record.incarnation);
              NodeAssert.equal(current?.receipt_id, own.record.receipt);
            }),
            remove: Effect.promise(async () => {
              mutations++;
              await command(repo, ["worktree", "remove", "--", worktree]);
            }),
            finalizeOutputs: () => Effect.void,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "retired");
        NodeAssert.ok(reads >= 2);
        NodeAssert.equal(mutations, 1);
        NodeAssert.equal(
          await Effect.runPromise(Effect.gen(function* () {
            return yield* RetirementReservationOwner;
          })),
          null,
        );
      } finally {
        f.close();
      }
    }),
  "missing or replaced common-store ownership refuses before any native removal": async () => {
    for (const mode of ["missing", "different-receipt"] as const)
      await fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "lost-reservation.sqlite3"));
        let altered = false,
          mutations = 0;
        try {
          await NodeAssert.rejects(
            Effect.runPromise(
              retire({
                cause: "deletion",
                projectRoot: repo,
                worktree,
                maxLevel: "branch-retained",
                admit: Effect.void,
                observe: Effect.succeed(input),
                revalidate: Effect.gen(function* () {
                  const own = yield* RetirementReservationOwner;
                  if (own === null || altered) return;
                  altered = true;
                  const common = new NodeSqlite.DatabaseSync(
                    NodePath.join(input.expected.git_common_dir, "workspace-lifecycle", "reservations.sqlite3"),
                  );
                  try {
                    if (mode === "missing")
                      common.prepare("DELETE FROM reservations WHERE admin_id=?").run(own.record.admin_id);
                    else
                      common.prepare("UPDATE reservations SET record=? WHERE admin_id=?").run(
                        canonical({ ...own.record, receipt: "different-receipt" }),
                        own.record.admin_id,
                      );
                  } finally {
                    common.close();
                  }
                }),
                remove: Effect.sync(() => {
                  mutations++;
                }),
                finalizeOutputs: () => Effect.void,
              }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
            ),
          );
          NodeAssert.equal(altered, true);
          NodeAssert.equal(mutations, 0);
          const saved = f.db.prepare("SELECT receipt_json FROM jones_workspace_retirement_receipts").get();
          NodeAssert.deepEqual(
            Schema.decodeSync(RetirementReceiptJson)(String(saved?.receipt_json)).reasons,
            ["reservation_ownership_lost"],
          );
          NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
          NodeAssert.equal(
            f.db.prepare("SELECT COUNT(*) n FROM jones_workspace_disposal_journal").get()?.n,
            0,
          );
        } finally {
          f.close();
        }
      });
  },
  "a verified later physical checkout may work and retire without rewriting an older terminal receipt":
    () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "recreated.sqlite3"));
        const request = {
          cause: "sweep" as const,
          projectRoot: repo,
          worktree,
          maxLevel: "branch-retained" as const,
          admit: Effect.void,
          observe: Effect.succeed(input),
          revalidate: Effect.void,
          finalizeOutputs: () => Effect.void,
        };
        const run = <A, E>(effect: Effect.Effect<A, E, Receipts>) =>
          Effect.runPromise(
            effect.pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
          );
        try {
          NodeAssert.equal((await run(retire(request))).state, "retired");
          const old = String(
            f.db.prepare("SELECT receipt_json FROM jones_workspace_retirement_receipts").get()!
              .receipt_json,
          );
          await command(repo, ["worktree", "add", worktree, "task"]);
          input.expected = await binding(worktree);
          await run(
            Effect.gen(function* () {
              const store = yield* Receipts;
              const identity = yield* store.claim({
                id: "new-native-duty",
                worktree,
                kind: "terminal",
                owner: "new-owner",
                holder: "{}",
              });
              NodeAssert.equal(identity.status, "bound");
              yield* store.releaseClaim("new-native-duty");
            }),
          );
          NodeAssert.equal((await run(retire(request))).state, "retired");
          NodeAssert.equal(
            f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_retirement_receipts").get()?.n,
            2,
          );
          NodeAssert.equal(
            String(
              f.db
                .prepare(
                  "SELECT receipt_json FROM jones_workspace_retirement_receipts ORDER BY rowid LIMIT 1",
                )
                .get()!.receipt_json,
            ),
            old,
          );
        } finally {
          f.close();
        }
      }),
  "native unforced removal refusal retains actual diagnostics and durable terminal readback": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "native-refusal.sqlite3"));
      const remove = Effect.tryPromise({
        try: async () => {
          await command(repo, ["worktree", "lock", worktree]);
          await command(repo, ["worktree", "remove", "--", worktree]);
        },
        catch: (cause) =>
          new RetirementError({
            target: worktree,
            reason: "git_removal_refused",
            cause: cause instanceof Error ? cause.message : String(cause),
          }),
      });
      try {
        const result = await Effect.runPromise(
          retire({
            cause: "sweep",
            projectRoot: repo,
            worktree,
            maxLevel: "branch-retained",
            admit: Effect.void,
            observe: Effect.succeed(input),
            revalidate: Effect.void,
            finalizeOutputs: () => Effect.void,
            remove,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "not_retired");
        NodeAssert.deepEqual(result.reasons, ["git_removal_refused"]);
        const receipt = JSON.parse(
          String(
            f.db.prepare("SELECT receipt_json FROM jones_workspace_retirement_receipts").get()!
              .receipt_json,
          ),
        );
        NodeAssert.match(receipt.removalDiagnostic, /worktree.*remove/);
        NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
        NodeAssert.equal(
          f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_retirement_state").get()?.n,
          0,
        );
      } finally {
        f.close();
      }
    }),
  "tracked changes journal precedes no-follow symlink disposal and keeps the external target": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const external = NodePath.join(base, "outside");
      await NodeFSP.writeFile(external, "retained");
      await NodeFSP.symlink(external, NodePath.join(worktree, "link"));
      await NodeFSP.writeFile(NodePath.join(worktree, "untracked"), "covered");
      input.coverage = {
        keepMode: "tracked-changes",
        exactRoots: [NodePath.join(worktree, "link"), NodePath.join(worktree, "untracked")],
      };
      const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"));
      try {
        const result = await Effect.runPromise(
          retire({
            cause: "owner_action",
            projectRoot: repo,
            worktree,
            maxLevel: "branch-retained",
            admit: Effect.void,
            observe: Effect.succeed(input),
            revalidate: Effect.void,
            finalizeOutputs: () => Effect.void,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "retired");
        NodeAssert.equal(await NodeFSP.readFile(external, "utf8"), "retained");
        NodeAssert.deepEqual(
          f.db
            .prepare("SELECT state FROM jones_workspace_disposal_journal ORDER BY ordinal")
            .all()
            .map((r) => r.state),
          ["completed", "completed"],
        );
      } finally {
        f.close();
      }
    }),
  "partial disposal records actual paths and a second invocation cannot retry it": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const a = NodePath.join(worktree, "a"),
        b = NodePath.join(worktree, "b");
      await NodeFSP.writeFile(a, "a");
      await NodeFSP.writeFile(b, "b");
      input.coverage = { keepMode: "tracked-changes", exactRoots: [a, b] };
      const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"), async (query, values) => {
        if (
          query.includes("UPDATE jones_workspace_disposal_journal") &&
          values[0] === "completed" &&
          values[2] === 0
        )
          await NodeFSP.writeFile(b, "changed after the first disposal");
      });
      const request = {
        cause: "sweep" as const,
        projectRoot: repo,
        worktree,
        maxLevel: "branch-retained" as const,
        admit: Effect.void,
        observe: Effect.succeed(input),
        revalidate: Effect.void,
        finalizeOutputs: () => Effect.void,
      };
      try {
        const run = () =>
          Effect.runPromise(
            retire(request).pipe(
              Effect.provide(layer),
              Effect.provideService(SqlClient.SqlClient, f.sql),
            ),
          );
        const result = await run();
        NodeAssert.equal(result.state, "partial");
        NodeAssert.equal(
          await NodeFSP.lstat(a).then(
            () => true,
            () => false,
          ),
          false,
        );
        NodeAssert.equal(await NodeFSP.readFile(b, "utf8"), "changed after the first disposal");
        NodeAssert.equal(
          f.db.prepare("SELECT state FROM jones_workspace_retirement_receipts").get()?.state,
          "partial",
        );
        NodeAssert.deepEqual(
          f.db
            .prepare("SELECT state FROM jones_workspace_disposal_journal ORDER BY ordinal")
            .all()
            .map((r) => r.state),
          ["completed", "invoking"],
        );
        await NodeAssert.rejects(run(), /retiring/);
        NodeAssert.equal(await NodeFSP.readFile(b, "utf8"), "changed after the first disposal");
      } finally {
        f.close();
      }
    }),
  "a change before final checks aborts with no disposal and releases its reservation": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"));
      let observations = 0;
      try {
        const result = await Effect.runPromise(
          retire({
            cause: "sweep",
            projectRoot: repo,
            worktree,
            maxLevel: "branch-retained",
            admit: Effect.void,
            observe: Effect.tryPromise({
              try: async () => {
                if (++observations === 2)
                  await NodeFSP.writeFile(NodePath.join(worktree, "new"), "must remain");
                return input;
              },
              catch: (e) =>
                new RetirementError({
                  target: worktree,
                  reason: "fixture_observation_failed",
                  cause: e,
                }),
            }),
            revalidate: Effect.void,
            finalizeOutputs: () => Effect.void,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "not_retired");
        NodeAssert.equal(
          await NodeFSP.readFile(NodePath.join(worktree, "new"), "utf8"),
          "must remain",
        );
        NodeAssert.equal(
          f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_disposal_journal").get()?.n,
          0,
        );
        NodeAssert.equal(
          f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_retirement_state").get()?.n,
          0,
        );
        await participantGuard(input.expected, async () => {});
      } finally {
        f.close();
      }
    }),
  "settlement replay never starts another script and completion is durable": () =>
    fixture(async ({ base, worktree }) => {
      const f = await fixtureSql(NodePath.join(base, "jones.sqlite3"));
      f.db
        .prepare(
          "INSERT INTO jones_workspace_retirement_intents(event_id,thread_id,worktree,settled_at) VALUES(?,?,?,?)",
        )
        .run("event", "thread", worktree, "2026-11-06T00:00:00Z");
      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* Receipts;
            NodeAssert.equal(yield* store.startScript("event"), true);
            NodeAssert.equal(yield* store.startScript("event"), false);
            yield* store.finishScript("event", "completed", 0);
            NodeAssert.equal(
              (yield* store.settlement("thread", worktree, "2026-11-06T00:00:00Z"))?.script_state,
              "completed",
            );
            NodeAssert.equal(yield* store.startScript("event"), false);
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
      } finally {
        f.close();
      }
    }),
  "replacement parent symlinks cannot redirect a journaled unlink": () =>
    fixture(async ({ base, worktree, input }) => {
      await NodeFSP.mkdir(NodePath.join(worktree, "dir"));
      await NodeFSP.writeFile(NodePath.join(worktree, "dir/a"), "local");
      const journal = await enumerateDisposal(input.expected, "tracked-changes");
      const outside = NodePath.join(base, "outside");
      await NodeFSP.mkdir(outside);
      await NodeFSP.writeFile(NodePath.join(outside, "a"), "protected");
      await NodeFSP.rename(NodePath.join(worktree, "dir"), NodePath.join(worktree, "old"));
      await NodeFSP.symlink(outside, NodePath.join(worktree, "dir"));
      await NodeAssert.rejects(disposePath(input.expected, journal[0]!));
      NodeAssert.equal(await NodeFSP.readFile(NodePath.join(outside, "a"), "utf8"), "protected");
    }),
};
