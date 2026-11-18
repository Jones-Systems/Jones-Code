// @effect-diagnostics nodeBuiltinImport:off -- Exact disposable fixtures execute production storage, qualification, reservation and unlink/removal paths.
import { policyProbe } from "./LinuxInfrastructureEvidence.testkit.ts";
import { consumerDecision, CONSUMER_LIMITS } from "./ConsumerProbe.ts";
import { canonical } from "./Filesystem.ts";
import { ClaimAbsence } from "./ClaimReconciliation.ts";
import type { JournalEntry, RetirementReceipt } from "./Receipts.ts";
import { makeRootsWritable } from "./ReadOnlyRemoval.ts";
import { forgetWorktreeOutputs } from "./OutputFinalization.ts";
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
import {
  REMOVAL_TIMEOUT_MS,
  interruptedCheckoutAbsent,
  interruptedCheckoutUnregistered,
  nativeWorktreeRemoval,
  retire,
} from "./RetirementExecutor.ts";
import {
  acquireReservation,
  holderIdentity,
  releaseReservation,
  type ReservationRecord,
} from "./Reservation.ts";
import { binding, incarnation } from "./OutputAttestation.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { enumerateDisposal, disposePath } from "./Disposal.ts";
import { dutyIdentity, terminalWorkspace, revalidateTerminalWorkspace } from "./DutyIdentity.ts";
import { captureNativeOwner, observeNativeOwner, type NativeOwnerIO } from "./NativeDutyOwner.ts";
import { nativePid } from "./Reservation.ts";
import { events } from "./EventIntegration.ts";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { finalCheckpointSettled, settlementAdmission } from "./SettlementAdmission.ts";
import { qualify, type QualificationInput } from "./Qualification.ts";
import { readReservation } from "./Reservation.ts";
import * as NodeChildProcess from "node:child_process";

export async function fixtureSql(
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
const compiledMain3acaCodec14 = Schema.decodeSync(RetirementReceiptJson);

const compiledMain3acaCodec13 = Schema.decodeEffect(NativeDutyOwnerJson);

const compiledMain3acaCodec12 = Schema.decodeEffect(DutyIdentityJson);

const compiledMain3acaCodec11 = Schema.decodeEffect(RetirementReceiptJson);

const compiledMain3acaCodec10 = Schema.decodeEffect(RetirementReceiptJson);

const compiledMain3acaCodec9 = Schema.encodeEffect(RetirementReceiptJson);

type Holder = Pick<ReservationRecord, "host" | "boot_id" | "pid" | "process_start">;
const processStart = (pid: number) =>
  new Promise<string>((resolve, reject) =>
    NodeChildProcess.execFile(
      "/bin/ps",
      ["-p", String(pid), "-o", "lstart="],
      { encoding: "utf8", env: { ...process.env, LC_ALL: "C" } },
      (error, out) => (error ? reject(error) : resolve(out.trim())),
    ),
  );
// A PID whose process has exited, recorded with a start time it never had.
const exitedHolder = async (): Promise<Holder> => {
  const child = NodeChildProcess.spawn("/usr/bin/true");
  await new Promise((resolve) => child.once("exit", resolve));
  return {
    ...(await holderIdentity()),
    pid: child.pid!,
    process_start: "Mon Jan  1 00:00:00 2024",
  };
};
// The durable state an executor leaves when the server stops after begin and before any terminal write.
async function interruptedRetirement(
  f: Awaited<ReturnType<typeof fixtureSql>>,
  input: QualificationInput,
  holder: Holder,
  journal?: readonly JournalEntry[],
) {
  const bound = input.expected,
    id = "interrupted-receipt";
  const record: ReservationRecord = {
    schema: "workspace-reservation/v1",
    admin_id: bound.admin_id,
    incarnation: incarnation(bound),
    executor: "jones",
    receipt: id,
    ...holder,
  };
  await acquireReservation(bound, record);
  const receipt: RetirementReceipt = {
    schema: "worktree-retirement/v1",
    id,
    cause: "owner_action",
    state: "intent",
    binding: bound,
    incarnation: record.incarnation,
    qualification: await qualify(input),
    coverage: {},
    preservationRefs: [],
    journal: id,
    disposed: [],
    reasons: [],
    limits: CONSUMER_LIMITS,
    bytes: { logical: null, allocated: null, statvfs_delta: null },
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* Receipts;
      yield* store.begin(receipt);
      if (journal) yield* store.journal(id, journal);
      yield* store.finish(receipt);
    }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
  );
  return record;
}
// A Go-module-cache-shaped tree: `go mod download` leaves every directory 0555 and every file 0444.
async function goCacheTree(root: string) {
  const mod = NodePath.join(root, "mcpjungle-setup/gopath/pkg/mod");
  for (let n = 0; n < 12; n++)
    for (const module of [`github.com/a${n}/x@v1.0.0`, `golang.org/z${n}@v0.3.0/sub/deep`]) {
      const dir = NodePath.join(mod, module);
      await NodeFSP.mkdir(dir, { recursive: true });
      await NodeFSP.writeFile(NodePath.join(dir, "main.go"), "package x\n");
    }
  const lock = async (directory: string): Promise<void> => {
    for (const entry of await NodeFSP.readdir(directory, { withFileTypes: true })) {
      const path = NodePath.join(directory, entry.name);
      if (entry.isDirectory()) await lock(path);
      else await NodeFSP.chmod(path, 0o444);
    }
    await NodeFSP.chmod(directory, 0o555);
  };
  await lock(root);
}
const unlockTree = (path: string) =>
  new Promise<void>((resolve) =>
    NodeChildProcess.execFile("/bin/chmod", ["-R", "u+w", path], () => resolve()),
  );
const modeOf = async (path: string) => (await NodeFSP.lstat(path)).mode & 0o777;
// Git stops on the read-only tree after dropping the registration, leaving the directory behind.
// A superuser can delete read-only trees, so there is nothing to reproduce for one.
async function deregisterWithResidue(repo: string, worktree: string) {
  try {
    await command(repo, ["worktree", "remove", "--", worktree]);
    return false;
  } catch {
    const listed = await command(repo, ["worktree", "list", "--porcelain"]);
    NodeAssert.equal(listed.includes(worktree), false);
    return true;
  }
}
const retireRequest = (repo: string, worktree: string, input: QualificationInput) => ({
  cause: "owner_action" as const,
  projectRoot: repo,
  worktree,
  maxLevel: "branch-retained" as const,
  admit: Effect.void,
  observe: Effect.succeed(input),
  revalidate: Effect.void,
  finalizeOutputs: () => Effect.void,
});
export const receiptRow = (f: Awaited<ReturnType<typeof fixtureSql>>, id: string) => {
  const row = f.db
    .prepare(
      "SELECT state,receipt_json FROM jones_workspace_retirement_receipts WHERE receipt_id=?",
    )
    .get(id);
  return { state: row?.state, receipt: compiledMain3acaCodec14(String(row?.receipt_json)) };
};

export const retirementCases: Record<string, () => Promise<void>> = {
  "both final stages reject changed valid process basis; fresh timestamps alone permit retirement":
    async () => {
      for (const changeAt of [2, 3, 0])
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "policy.sqlite3"));
          let observations = 0,
            removals = 0;
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
                    observations++;
                    const process = await policyProbe(
                      changeAt && observations >= changeAt ? "b".repeat(64) : undefined,
                    );
                    process.observedAt -= observations;
                    return { ...input, consumers: { ...input.consumers, process } };
                  },
                  catch: (cause) =>
                    new RetirementError({ reason: "observation_failed", target: worktree, cause }),
                }),
                revalidate: Effect.void,
                finalizeOutputs: () => Effect.void,
                remove: Effect.tryPromise({
                  try: async () => {
                    removals++;
                    await command(repo, ["worktree", "remove", "--", worktree]);
                  },
                  catch: (cause) =>
                    new RetirementError({ reason: "remove_failed", target: worktree, cause }),
                }),
              }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
            );
            NodeAssert.equal(removals, changeAt ? 0 : 1);
            NodeAssert.equal(result.state, changeAt ? "not_retired" : "retired");
            if (changeAt)
              NodeAssert.ok(
                result.reasons.includes(
                  changeAt === 2 ? "final_check_conflict" : "remaining_fact_changed",
                ),
              );
          } finally {
            f.close();
          }
        });
    },
  "old and optional policy receipts roundtrip; malformed policy stays rejected": async () =>
    fixture(async ({ input }) => {
      const process = await policyProbe();
      const consumers = consumerDecision(
        input.worktree,
        { ...input.consumers, process },
        process.observedAt,
      );
      const receipt: RetirementReceipt = {
        schema: "worktree-retirement/v1",
        id: "policy",
        cause: "sweep",
        state: "not_retired",
        binding: input.expected,
        incarnation: incarnation(input.expected),
        qualification: {
          schema: "workspace-qualification/v1",
          authority_effect: "none",
          outcome: "unknown",
          level: null,
          reasons: [],
          decision: "unknown",
          observationMs: 0,
          consumers,
        },
        coverage: {},
        preservationRefs: [],
        journal: "policy",
        disposed: [],
        reasons: [],
        limits: [],
        bytes: { logical: null, allocated: null, statvfs_delta: null },
      };
      for (const policy of [undefined, process.processPolicy]) {
        if (policy) consumers.coverage.process!.processPolicy = policy;
        else delete consumers.coverage.process!.processPolicy;
        const decoded = await Effect.runPromise(
          compiledMain3acaCodec10(await Effect.runPromise(compiledMain3acaCodec9(receipt))),
        );
        NodeAssert.deepEqual(decoded, receipt);
        NodeAssert.deepEqual(decoded.qualification.consumers?.limits, CONSUMER_LIMITS);
      }
      const original = process.processPolicy!;
      for (const patch of [
        { policy: "wrong" },
        { basisDigest: "bad" },
        { deniedInstances: -1 },
        { verifiedExclusions: 0 },
        { unclassifiedInstances: 1 },
        { assurance: "non-use" },
        { reasons: { identity: -1 } },
      ]) {
        consumers.coverage.process!.processPolicy = { ...original, ...patch } as typeof original;
        await NodeAssert.rejects(Effect.runPromise(compiledMain3acaCodec10(canonical(receipt))));
      }
    }),
  "receipt codecs retain absent hold/unknown qualification evidence and reject invalid authority fields":
    () =>
      fixture(async ({ input }) => {
        for (const outcome of ["hold", "unknown"] as const) {
          const receipt: RetirementReceipt = {
            schema: "worktree-retirement/v1",
            id: "codec-fixture",
            cause: "sweep",
            state: "not_retired",
            binding: input.expected,
            incarnation: incarnation(input.expected),
            qualification: {
              schema: "workspace-qualification/v1",
              authority_effect: "none",
              outcome,
              level: null,
              reasons: ["native_binding_unavailable"],
              decision: outcome,
              observationMs: 0,
            },
            coverage: {},
            preservationRefs: [],
            journal: "codec-fixture",
            disposed: [],
            reasons: ["native_binding_unavailable"],
            limits: [],
            bytes: { logical: null, allocated: null, statvfs_delta: null },
          };
          const encoded = await Effect.runPromise(compiledMain3acaCodec9(receipt));
          const decoded = await Effect.runPromise(compiledMain3acaCodec10(encoded));
          NodeAssert.deepEqual(decoded, receipt);
          NodeAssert.equal(Object.hasOwn(decoded.qualification, "binding"), false);
          NodeAssert.equal(Object.hasOwn(decoded.qualification, "head"), false);
          await NodeAssert.rejects(
            Effect.runPromise(compiledMain3acaCodec11(canonical({ ...receipt, binding: null }))),
          );
        }
      }),
  "default absence decoding retains malformed and missing native holder proof": async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const absence = yield* ClaimAbsence;
        for (const native_owner_json of [null, "broken", '{"schema":"unexpected"}']) {
          const result = yield* absence.observe({
            claim_id: "synthetic-claim",
            worktree: "/synthetic/checkout",
            kind: "session",
            owner: "synthetic-owner",
            holder_json: '"unchanged-holder"',
            native_owner_json,
          });
          NodeAssert.deepEqual(result, { holder: '"unchanged-holder"', status: "unknown" });
        }
      }),
    );
  },
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
  "terminal native admission normalizes subdirectory roots and rejects hints or replaced cwd": () =>
    fixture(async ({ worktree, base }) => {
      const subdir = NodePath.join(worktree, "subdir");
      await NodeFSP.mkdir(subdir);
      const observed = await terminalWorkspace(subdir);
      NodeAssert.equal(observed.worktree, worktree);
      NodeAssert.equal(observed.identity?.status, "bound");
      if (observed.identity?.status === "bound")
        NodeAssert.equal(observed.identity.binding.path, worktree);
      await revalidateTerminalWorkspace(observed);
      await NodeAssert.rejects(terminalWorkspace(subdir, base), /terminal_worktree_hint_mismatch/);
      const ordinary = await terminalWorkspace(base);
      NodeAssert.equal(ordinary.worktree, null);
      NodeAssert.equal(ordinary.identity, null);
      await NodeFSP.rename(subdir, NodePath.join(worktree, "old-subdir"));
      await NodeFSP.symlink(NodePath.join(worktree, "old-subdir"), subdir);
      await NodeAssert.rejects(
        revalidateTerminalWorkspace(observed),
        /terminal_cwd_identity_changed/,
      );
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
      listing = "12345 replacement-session reused-parent-pid\n";
      NodeAssert.equal(await observeNativeOwner(owner, io), "unknown");
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
              NodeAssert.equal(
                (yield* compiledMain3acaCodec12(String(facts.identity_json))).status,
                "bound",
              );
              NodeAssert.equal(
                (yield* compiledMain3acaCodec13(String(facts.native_owner_json))).type,
                "unresolved",
              );
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
                NodePath.join(
                  input.expected.git_common_dir,
                  "workspace-lifecycle",
                  "reservations.sqlite3",
                ),
                { readOnly: true },
              );
              try {
                NodeAssert.equal(
                  common
                    .prepare("SELECT record FROM reservations WHERE admin_id=?")
                    .get(own.record.admin_id)?.record,
                  canonical(own.record),
                );
              } finally {
                common.close();
              }
              const current = f.db
                .prepare(
                  "SELECT incarnation_id,receipt_id FROM jones_workspace_retirement_state WHERE worktree=?",
                )
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
        NodeAssert.equal(await Effect.runPromise(RetirementReservationOwner), null);
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
                    NodePath.join(
                      input.expected.git_common_dir,
                      "workspace-lifecycle",
                      "reservations.sqlite3",
                    ),
                  );
                  try {
                    if (mode === "missing")
                      common
                        .prepare("DELETE FROM reservations WHERE admin_id=?")
                        .run(own.record.admin_id);
                    else
                      common
                        .prepare("UPDATE reservations SET record=? WHERE admin_id=?")
                        .run(
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
          const saved = f.db
            .prepare("SELECT receipt_json FROM jones_workspace_retirement_receipts")
            .get();
          NodeAssert.deepEqual(compiledMain3acaCodec14(String(saved?.receipt_json)).reasons, [
            "reservation_ownership_lost",
          ]);
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
  "removal timeout with the checkout still present is partial, never not_retired": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "removal-timeout.sqlite3"));
      // The driver's timeout, kill and spawn failures arrive as errors, not as a git exit code.
      const remove = Effect.fail(
        new RetirementError({
          target: worktree,
          reason: "removal_interrupted",
          cause: "Git command timed out.",
        }),
      );
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
            remove,
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "partial");
        NodeAssert.deepEqual(result.reasons, ["removal_interrupted"]);
        const receipt = JSON.parse(
          String(
            f.db.prepare("SELECT receipt_json FROM jones_workspace_retirement_receipts").get()!
              .receipt_json,
          ),
        );
        NodeAssert.equal(receipt.state, "partial");
        NodeAssert.match(receipt.removalDiagnostic, /timed out/);
        NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
      } finally {
        f.close();
      }
    }),
  "owner retirement passes an explicit removal timeout": async () => {
    const seen: { operation: string; args: readonly string[]; timeoutMs: number }[] = [];
    const driver = (exitCode: number, fail?: unknown) => ({
      execute: (request: {
        operation: string;
        args: readonly string[];
        allowNonZeroExit: boolean;
        timeoutMs: number;
      }) => {
        NodeAssert.equal(request.allowNonZeroExit, true);
        seen.push(request);
        return fail === undefined
          ? Effect.succeed({ exitCode, stderr: "fatal: refused\nsecond line" })
          : Effect.fail(fail);
      },
    });
    const call = async (d: ReturnType<typeof driver>) => {
      const outcome = await Effect.runPromise(
        nativeWorktreeRemoval(d as never, { projectRoot: "/project", worktree: "/project/w" }).pipe(
          Effect.result,
        ),
      );
      if (outcome._tag !== "Failure") throw new Error("expected the removal to fail");
      return outcome.failure;
    };
    NodeAssert.equal(REMOVAL_TIMEOUT_MS, 30 * 60 * 1000);
    await Effect.runPromise(
      nativeWorktreeRemoval(driver(0) as never, {
        projectRoot: "/project",
        worktree: "/project/w",
      }),
    );
    NodeAssert.equal(seen[0]?.operation, "WorkspaceRetirement.remove");
    NodeAssert.deepEqual(seen[0]?.args, ["worktree", "remove", "--", "/project/w"]);
    NodeAssert.equal(seen[0]?.timeoutMs, REMOVAL_TIMEOUT_MS);
    const refused = await call(driver(128));
    NodeAssert.equal(refused.reason, "git_removal_refused");
    NodeAssert.equal(refused.cause, "git worktree remove: fatal: refused");
    const timedOut = await call(driver(0, new Error("Git command timed out.")));
    NodeAssert.equal(timedOut.reason, "removal_interrupted");
    NodeAssert.equal(
      seen.every((r) => r.timeoutMs === REMOVAL_TIMEOUT_MS),
      true,
    );
  },
  "final checkpoint gate is unchanged: only runs with a ready checkpoint settle, failed and rolled_back still hold":
    async () => {
      // Checkpoint statuses are ready | missing | error | stale; none means "in progress", and capture
      // never runs for failed or rolled_back runs. Admitting those runs needs an owner decision.
      const ready = (runId: string) => [{ runId, status: "ready" }];
      for (const status of ["failed", "rolled_back"]) {
        NodeAssert.equal(finalCheckpointSettled({ id: "run", status }, []), false);
        NodeAssert.equal(finalCheckpointSettled({ id: "run", status }, ready("run")), false);
      }
      for (const status of ["completed", "cancelled", "interrupted"]) {
        NodeAssert.equal(finalCheckpointSettled({ id: "run", status }, ready("run")), true);
        NodeAssert.equal(finalCheckpointSettled({ id: "run", status }, ready("other")), false);
        NodeAssert.equal(
          finalCheckpointSettled({ id: "run", status }, [{ runId: "run", status: "stale" }]),
          false,
        );
      }
      NodeAssert.equal(
        finalCheckpointSettled({ id: "run", status: "running" }, ready("run")),
        false,
      );
      NodeAssert.equal(finalCheckpointSettled(undefined, []), true);
    },
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
  "absent holder with intact checkout reconciles to not_retired and a retry succeeds": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "interrupted-intact.sqlite3"));
      try {
        await interruptedRetirement(f, input, await exitedHolder());
        const result = await Effect.runPromise(
          retire(retireRequest(repo, worktree, input)).pipe(
            Effect.provide(layer),
            Effect.provideService(SqlClient.SqlClient, f.sql),
          ),
        );
        const stale = receiptRow(f, "interrupted-receipt");
        NodeAssert.equal(stale.state, "not_retired");
        NodeAssert.deepEqual(stale.receipt.reasons, ["interrupted_before_effect"]);
        NodeAssert.equal(result.state, "retired");
        NodeAssert.notEqual("id" in result ? result.id : null, "interrupted-receipt");
        NodeAssert.equal(
          await NodeFSP.lstat(worktree).then(
            () => true,
            () => false,
          ),
          false,
        );
        NodeAssert.equal(
          (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
          true,
        );
        NodeAssert.equal(await readReservation(input.expected), null);
      } finally {
        f.close();
      }
    }),
  "absent holder with removed checkout reconciles to retired": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "interrupted-removed.sqlite3"));
      let observed = 0;
      try {
        await interruptedRetirement(f, input, {
          ...(await holderIdentity()),
          boot_id: "a boot before the restart",
        });
        await command(repo, ["worktree", "remove", "--", worktree]);
        const result = await Effect.runPromise(
          retire({
            ...retireRequest(repo, worktree, input),
            observe: Effect.sync(() => {
              observed++;
              return input;
            }),
          }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
        NodeAssert.equal(result.state, "retired");
        NodeAssert.equal("id" in result ? result.id : null, "interrupted-receipt");
        NodeAssert.deepEqual(result.reasons, ["interrupted_after_removal"]);
        NodeAssert.equal(observed, 0);
        NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "retired");
        NodeAssert.equal(
          f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
          "retired",
        );
        NodeAssert.equal(
          (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
          true,
        );
        NodeAssert.equal(await readReservation(input.expected), null);
      } finally {
        f.close();
      }
    }),
  "half-removed checkout stays partial and holds": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "interrupted-half.sqlite3"));
      const run = () =>
        Effect.runPromise(
          retire(retireRequest(repo, worktree, input)).pipe(
            Effect.provide(layer),
            Effect.provideService(SqlClient.SqlClient, f.sql),
          ),
        );
      try {
        await interruptedRetirement(f, input, {
          ...(await holderIdentity()),
          boot_id: "a boot before the restart",
        });
        // An unforced removal deletes tracked files before Git drops the registration.
        await NodeFSP.unlink(NodePath.join(worktree, "tracked"));
        const result = await run();
        NodeAssert.equal(result.state, "partial");
        NodeAssert.deepEqual(result.reasons, ["interrupted_tracked_deletions"]);
        const stale = receiptRow(f, "interrupted-receipt");
        NodeAssert.equal(stale.state, "partial");
        NodeAssert.match(
          stale.receipt.qualification.details?.at(-1)?.nextAction ?? "",
          /do not force/,
        );
        NodeAssert.equal(
          f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
          "retiring",
        );
        NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
        await NodeAssert.rejects(run(), /retiring/);
        NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "partial");
        NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
      } finally {
        f.close();
      }
    }),
  "live holder is never recovered": async () => {
    const live = { ...(await holderIdentity()), pid: process.ppid };
    live.process_start = await processStart(process.ppid);
    for (const holder of [live, { ...live, host: "another-host.invalid" }])
      await fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "interrupted-live.sqlite3"));
        let removals = 0;
        try {
          const record = await interruptedRetirement(f, input, holder);
          await NodeAssert.rejects(
            Effect.runPromise(
              retire({
                ...retireRequest(repo, worktree, input),
                remove: Effect.sync(() => {
                  removals++;
                }),
              }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
            ),
            /retiring/,
          );
          NodeAssert.equal(removals, 0);
          NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "intent");
          NodeAssert.equal(canonical(await readReservation(input.expected)), canonical(record));
          NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true);
        } finally {
          f.close();
        }
      });
  },
  "a read-only Go-cache-shaped ignored tree no longer stops removal": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const build = NodePath.join(worktree, "build");
      await goCacheTree(build);
      input.coverage = { keepMode: "uncommitted-changes", exactRoots: [build] };
      const f = await fixtureSql(NodePath.join(base, "readonly-tree.sqlite3"));
      try {
        NodeAssert.equal(await modeOf(build), 0o555);
        const result = await Effect.runPromise(
          retire(retireRequest(repo, worktree, input)).pipe(
            Effect.provide(layer),
            Effect.provideService(SqlClient.SqlClient, f.sql),
          ),
        );
        NodeAssert.equal(result.state, "retired");
        NodeAssert.deepEqual(result.reasons, []);
        NodeAssert.equal(
          await NodeFSP.lstat(worktree).then(
            () => true,
            () => false,
          ),
          false,
        );
        NodeAssert.equal(
          (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
          true,
        );
      } finally {
        await unlockTree(worktree);
        f.close();
      }
    }),
  "making ignored roots writable never follows a symlink or leaves the target": () =>
    fixture(async ({ base, worktree, input }) => {
      const outside = NodePath.join(base, "outside"),
        build = NodePath.join(worktree, "build");
      await NodeFSP.mkdir(NodePath.join(outside, "inner"), { recursive: true });
      await NodeFSP.writeFile(NodePath.join(outside, "file"), "protected");
      await NodeFSP.chmod(NodePath.join(outside, "inner"), 0o555);
      await NodeFSP.chmod(outside, 0o555);
      await NodeFSP.mkdir(NodePath.join(build, "mod/sub"), { recursive: true });
      await NodeFSP.symlink(outside, NodePath.join(build, "mod/escape"));
      await NodeFSP.symlink(NodePath.join(outside, "file"), NodePath.join(build, "mod/filelink"));
      await NodeFSP.symlink(outside, NodePath.join(worktree, "linkroot"));
      for (const dir of ["mod/sub", "mod", ""])
        await NodeFSP.chmod(NodePath.join(build, dir), 0o555);
      try {
        const result = await makeRootsWritable(input.expected, [
          build,
          NodePath.join(worktree, "linkroot"),
          NodePath.join(worktree, "absent"),
        ]);
        NodeAssert.equal(result.changed, 3);
        NodeAssert.deepEqual(result.skipped, ["linkroot"]);
        for (const dir of ["mod/sub", "mod", ""])
          NodeAssert.equal(await modeOf(NodePath.join(build, dir)), 0o755);
        // Nothing the links point at was entered or changed.
        NodeAssert.equal(await modeOf(outside), 0o555);
        NodeAssert.equal(await modeOf(NodePath.join(outside, "inner")), 0o555);
        NodeAssert.equal(await modeOf(NodePath.join(outside, "file")), 0o644);
        NodeAssert.equal(
          (await NodeFSP.lstat(NodePath.join(build, "mod/escape"))).isSymbolicLink(),
          true,
        );
        // A root outside the target, the target itself and the checkout's own metadata are refused.
        for (const root of [outside, worktree, NodePath.join(worktree, ".git")])
          await NodeAssert.rejects(makeRootsWritable(input.expected, [root]), /identity_mismatch/);
        NodeAssert.equal(await modeOf(outside), 0o555);
      } finally {
        await unlockTree(outside);
        await unlockTree(build);
      }
    }),
  "a read-only tree reached through a symlink leaves the external target unchanged on removal":
    () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const outside = NodePath.join(base, "outside"),
          build = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(outside);
        await NodeFSP.writeFile(NodePath.join(outside, "file"), "protected");
        await NodeFSP.chmod(outside, 0o555);
        await goCacheTree(build);
        await NodeFSP.chmod(build, 0o755);
        await NodeFSP.symlink(outside, NodePath.join(build, "escape"));
        await NodeFSP.chmod(build, 0o555);
        input.coverage = { keepMode: "uncommitted-changes", exactRoots: [build] };
        const f = await fixtureSql(NodePath.join(base, "readonly-link.sqlite3"));
        try {
          const result = await Effect.runPromise(
            retire(retireRequest(repo, worktree, input)).pipe(
              Effect.provide(layer),
              Effect.provideService(SqlClient.SqlClient, f.sql),
            ),
          );
          NodeAssert.equal(result.state, "retired");
          NodeAssert.equal(await modeOf(outside), 0o555);
          NodeAssert.equal(
            await NodeFSP.readFile(NodePath.join(outside, "file"), "utf8"),
            "protected",
          );
        } finally {
          await unlockTree(outside);
          await unlockTree(worktree);
          f.close();
        }
      }),
  "interrupted probes match the canonical and a symlinked spelling of the checkout": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const alias = NodePath.join(base, "alias");
      await NodeFSP.symlink(base, alias);
      const spelled = NodePath.join(alias, "checkout");
      const f = await fixtureSql(NodePath.join(base, "interrupted-spelling.sqlite3"));
      const run = <A>(effect: Effect.Effect<A, RetirementError, Receipts>) =>
        Effect.runPromise(
          effect.pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
        );
      try {
        await interruptedRetirement(f, input, await exitedHolder());
        await command(repo, ["worktree", "remove", "--", worktree]);
        for (const name of [worktree, spelled])
          NodeAssert.equal(
            (await run(interruptedCheckoutAbsent(name)))?.receipt.id,
            "interrupted-receipt",
          );
        // A registered or present checkout is not an unregistered leftover under either spelling.
        for (const name of [worktree, spelled])
          NodeAssert.equal(await run(interruptedCheckoutUnregistered(name, repo)), null);
      } finally {
        f.close();
      }
    }),
  "the absent-checkout probe reports only an intent receipt whose checkout is gone": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "interrupted-probe.sqlite3"));
      const probe = () =>
        Effect.runPromise(
          interruptedCheckoutAbsent(worktree).pipe(
            Effect.provide(layer),
            Effect.provideService(SqlClient.SqlClient, f.sql),
          ),
        );
      try {
        NodeAssert.equal(await probe(), null);
        await interruptedRetirement(f, input, await exitedHolder());
        NodeAssert.equal(await probe(), null);
        await command(repo, ["worktree", "remove", "--", worktree]);
        NodeAssert.equal((await probe())?.receipt.id, "interrupted-receipt");
        f.db.exec("UPDATE jones_workspace_retirement_receipts SET state='partial'");
        NodeAssert.equal(await probe(), null);
      } finally {
        f.close();
      }
    }),
};

interface EvaluatedRetirement {
  readonly state: string;
  readonly reasons: readonly string[];
  readonly outcome: {
    readonly state: string;
    readonly reasonCodes: readonly string[];
    readonly receiptId: string | null;
    readonly fingerprint: string | null;
    readonly details: readonly { readonly nextAction: string }[];
  };
}
export interface OutputStub {
  readonly id: string;
  readonly path: string;
  readonly owner: string;
  readonly class: string;
  readonly state: string;
  readonly worktree: null;
}
/** The two registry operations a settled retirement's output finalization uses. */
export interface RegistryStub {
  readonly outputs: (worktree: string) => Effect.Effect<readonly OutputStub[]>;
  readonly forgetOutputs: (records: readonly OutputStub[]) => Effect.Effect<void>;
}
/** Settings evaluation for this fixture's checkout; the runner supplies the configured services. */
export type EvaluateRetirement = (
  sql: SqlClient.SqlClient,
  request: {
    readonly worktree: string;
    readonly projectRoot: string;
    readonly execute: boolean;
    readonly cause: RetirementReceipt["cause"];
    readonly ownerCoverage?: { readonly fingerprint: string; readonly paths: readonly string[] };
    readonly registry?: RegistryStub;
    // Runs where the executor admits a retirement, before it reconciles an interrupted receipt.
    readonly admit?: Effect.Effect<void>;
  },
) => Effect.Effect<EvaluatedRetirement, Error>;
// Durable retirement state that must not move unless the owner's Retire settles the receipt.
const durableState = async (f: Awaited<ReturnType<typeof fixtureSql>>, input: QualificationInput) =>
  canonical({
    receipt: receiptRow(f, "interrupted-receipt"),
    target: f.db.prepare("SELECT state,receipt_id FROM jones_workspace_retirement_state").all(),
    reservation: await readReservation(input.expected),
  });
// A checkout fully removed before the restart cannot be bound; these run through Settings evaluation.
export const absentCheckoutCases = (
  settingsEvaluation: EvaluateRetirement,
): Record<string, () => Promise<void>> => {
  const evaluate = (...args: Parameters<EvaluateRetirement>) =>
    Effect.runPromise(settingsEvaluation(...args));
  return {
    "owner Retire finishes an interrupted retirement whose checkout is gone": () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "absent-retire.sqlite3"));
        try {
          await interruptedRetirement(f, input, {
            ...(await holderIdentity()),
            boot_id: "a boot before the restart",
          });
          await command(repo, ["worktree", "remove", "--", worktree]);
          const proposal = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: false,
            cause: "owner_action",
          });
          const result = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: true,
            cause: "owner_action",
            ownerCoverage: { fingerprint: proposal.outcome.fingerprint!, paths: [] },
          });
          NodeAssert.equal(result.state, "retired");
          NodeAssert.deepEqual(result.reasons, ["interrupted_after_removal"]);
          NodeAssert.equal(result.outcome.state, "retired");
          NodeAssert.equal(result.outcome.receiptId, "interrupted-receipt");
          NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "retired");
          NodeAssert.equal(
            f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
            "retired",
          );
          NodeAssert.equal(await readReservation(input.expected), null);
          NodeAssert.equal(
            (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
            true,
          );
        } finally {
          f.close();
        }
      }),
    "Propose, an automatic run or a stale fingerprint leaves an interrupted absent checkout held":
      () =>
        fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "absent-propose.sqlite3"));
          try {
            await interruptedRetirement(f, input, {
              ...(await holderIdentity()),
              boot_id: "a boot before the restart",
            });
            await command(repo, ["worktree", "remove", "--", worktree]);
            const before = await durableState(f, input);
            const proposal = await evaluate(f.sql, {
              worktree,
              projectRoot: repo,
              execute: false,
              cause: "owner_action",
            });
            NodeAssert.equal(proposal.state, "held");
            NodeAssert.equal(proposal.outcome.state, "held");
            NodeAssert.deepEqual(proposal.outcome.reasonCodes, [
              "interrupted_retirement_checkout_absent",
            ]);
            NodeAssert.equal(proposal.outcome.fingerprint, "interrupted-receipt");
            NodeAssert.match(proposal.outcome.details[0]?.nextAction ?? "", /Run Retire to finish/);
            const automatic = await evaluate(f.sql, {
              worktree,
              projectRoot: repo,
              execute: true,
              cause: "settlement",
              ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
            });
            NodeAssert.deepEqual(automatic.reasons, ["interrupted_retirement_checkout_absent"]);
            const stale = await evaluate(f.sql, {
              worktree,
              projectRoot: repo,
              execute: true,
              cause: "owner_action",
              ownerCoverage: { fingerprint: "another-proposal", paths: [] },
            });
            NodeAssert.deepEqual(stale.reasons, ["proposal_changed"]);
            NodeAssert.equal(await durableState(f, input), before);
            NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "intent");
          } finally {
            f.close();
          }
        }),
    "owner Retire never recovers an absent checkout from a live holder": () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "absent-live.sqlite3"));
        try {
          const live = { ...(await holderIdentity()), pid: process.ppid };
          live.process_start = await processStart(process.ppid);
          await interruptedRetirement(f, input, live);
          await command(repo, ["worktree", "remove", "--", worktree]);
          const before = await durableState(f, input);
          await NodeAssert.rejects(
            evaluate(f.sql, {
              worktree,
              projectRoot: repo,
              execute: true,
              cause: "owner_action",
              ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
            }),
            /retiring/,
          );
          NodeAssert.equal(await durableState(f, input), before);
          NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "intent");
        } finally {
          f.close();
        }
      }),
    "owner Retire finishes a deregistered checkout left behind by read-only trees": () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "residue-retire.sqlite3"));
        const build = NodePath.join(worktree, "build");
        try {
          await goCacheTree(build);
          input.coverage = { keepMode: "uncommitted-changes", exactRoots: [build] };
          await interruptedRetirement(
            f,
            input,
            { ...(await holderIdentity()), boot_id: "a boot before the restart" },
            await enumerateDisposal(input.expected, "uncommitted-changes"),
          );
          if (!(await deregisterWithResidue(repo, worktree))) return;
          // Git deletes entries in directory order before it stops on the read-only tree, so which tracked
          // files survive depends on the filesystem. Leave one that matches HEAD, as macOS does.
          const tracked = NodePath.join(worktree, "tracked");
          if (
            !(await NodeFSP.lstat(tracked).then(
              () => true,
              () => false,
            ))
          )
            await NodeFSP.writeFile(tracked, "original\n");
          NodeAssert.equal(await modeOf(build), 0o555);
          const leftover = async () => (await NodeFSP.readdir(worktree)).sort();
          const leftoverBefore = await leftover();
          NodeAssert.ok(leftoverBefore.includes("build") && leftoverBefore.includes("tracked"));
          const before = await durableState(f, input);
          const proposal = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: false,
            cause: "owner_action",
          });
          NodeAssert.equal(proposal.state, "held");
          NodeAssert.deepEqual(proposal.outcome.reasonCodes, [
            "interrupted_retirement_checkout_unregistered",
          ]);
          NodeAssert.equal(proposal.outcome.fingerprint, "interrupted-receipt");
          NodeAssert.match(
            proposal.outcome.details[0]?.nextAction ?? "",
            /Anything else stays held/,
          );
          const automatic = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: true,
            cause: "settlement",
            ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
          });
          NodeAssert.deepEqual(automatic.reasons, ["interrupted_retirement_checkout_unregistered"]);
          const stale = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: true,
            cause: "owner_action",
            ownerCoverage: { fingerprint: "another-proposal", paths: [] },
          });
          NodeAssert.deepEqual(stale.reasons, ["proposal_changed"]);
          // Propose, an automatic run and a stale fingerprint delete and chmod nothing.
          NodeAssert.equal(await durableState(f, input), before);
          NodeAssert.equal(await modeOf(build), 0o555);
          NodeAssert.deepEqual(await leftover(), leftoverBefore);
          NodeAssert.equal(await NodeFSP.readFile(tracked, "utf8"), "original\n");
          const result = await evaluate(f.sql, {
            worktree,
            projectRoot: repo,
            execute: true,
            cause: "owner_action",
            ownerCoverage: { fingerprint: proposal.outcome.fingerprint!, paths: [] },
          });
          NodeAssert.equal(result.state, "retired");
          NodeAssert.deepEqual(result.reasons, ["interrupted_residue_removed"]);
          NodeAssert.equal(result.outcome.receiptId, "interrupted-receipt");
          NodeAssert.equal(
            await NodeFSP.lstat(worktree).then(
              () => true,
              () => false,
            ),
            false,
          );
          NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "retired");
          NodeAssert.equal(
            f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
            "retired",
          );
          NodeAssert.equal(await readReservation(input.expected), null);
          NodeAssert.equal(
            (await command(repo, ["show-ref", "--verify", "refs/heads/task"])).length > 0,
            true,
          );
        } finally {
          await unlockTree(worktree);
          f.close();
        }
      }),
    "owner Retire finishes a deregistered checkout named by a symlinked spelling": () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "residue-spelling.sqlite3"));
        const build = NodePath.join(worktree, "build");
        try {
          await goCacheTree(build);
          input.coverage = { keepMode: "uncommitted-changes", exactRoots: [build] };
          await interruptedRetirement(
            f,
            input,
            { ...(await holderIdentity()), boot_id: "a boot before the restart" },
            await enumerateDisposal(input.expected, "uncommitted-changes"),
          );
          if (!(await deregisterWithResidue(repo, worktree))) return;
          // On Linux Git removes `tracked` before it stops on the read-only tree; finish from that shape too.
          await NodeFSP.rm(NodePath.join(worktree, "tracked"), { force: true });
          const alias = NodePath.join(base, "alias");
          await NodeFSP.symlink(base, alias);
          const spelled = NodePath.join(alias, "checkout");
          const result = await evaluate(f.sql, {
            worktree: spelled,
            projectRoot: repo,
            execute: true,
            cause: "owner_action",
            ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
          });
          NodeAssert.equal(result.state, "retired");
          NodeAssert.deepEqual(result.reasons, ["interrupted_residue_removed"]);
          NodeAssert.equal(
            await NodeFSP.lstat(worktree).then(
              () => true,
              () => false,
            ),
            false,
          );
        } finally {
          await unlockTree(worktree);
          f.close();
        }
      }),
    "a deregistered checkout stays held, naming the path, when unrecognized content remains":
      async () => {
        const leftovers: Record<string, [(worktree: string) => Promise<void>, string]> = {
          "an untracked file": [
            (worktree) => NodeFSP.writeFile(NodePath.join(worktree, "notes.txt"), "keep me"),
            "notes.txt",
          ],
          "an edited tracked file": [
            (worktree) => NodeFSP.writeFile(NodePath.join(worktree, "tracked"), "edited\n"),
            "tracked",
          ],
          "a sensitive file inside a covered ignored root": [
            async (worktree) => {
              const build = NodePath.join(worktree, "build");
              await NodeFSP.chmod(build, 0o755);
              await NodeFSP.writeFile(NodePath.join(build, ".env"), "SECRET=1");
              await NodeFSP.chmod(build, 0o555);
            },
            "build (sensitive_ignored)",
          ],
          "a nested directory with untracked content": [
            async (worktree) => {
              await NodeFSP.mkdir(NodePath.join(worktree, "extra"));
              await NodeFSP.writeFile(NodePath.join(worktree, "extra/file"), "keep me");
            },
            "extra",
          ],
        };
        for (const [label, [change, named]] of Object.entries(leftovers))
          await fixture(async ({ base, repo, worktree, input }) => {
            const f = await fixtureSql(NodePath.join(base, "residue-held.sqlite3"));
            const build = NodePath.join(worktree, "build");
            try {
              await goCacheTree(build);
              input.coverage = { keepMode: "uncommitted-changes", exactRoots: [build] };
              await interruptedRetirement(
                f,
                input,
                { ...(await holderIdentity()), boot_id: "a boot before the restart" },
                await enumerateDisposal(input.expected, "uncommitted-changes"),
              );
              if (!(await deregisterWithResidue(repo, worktree))) return;
              await change(worktree);
              const result = await evaluate(f.sql, {
                worktree,
                projectRoot: repo,
                execute: true,
                cause: "owner_action",
                ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
              });
              NodeAssert.equal(result.state, "partial", label);
              NodeAssert.deepEqual(
                result.reasons,
                ["interrupted_residue_unrecognized_content"],
                label,
              );
              const next = result.outcome.details.map((d) => d.nextAction).join("\n");
              NodeAssert.ok(
                next.includes(`Paths: ${named}`) || next.includes(named),
                label + ": " + next,
              );
              // Nothing was made writable and nothing was deleted.
              NodeAssert.equal(await modeOf(build), 0o555, label);
              NodeAssert.equal((await NodeFSP.lstat(worktree)).isDirectory(), true, label);
              NodeAssert.equal(
                await NodeFSP.lstat(NodePath.join(build, "mcpjungle-setup")).then(
                  () => true,
                  () => false,
                ),
                true,
                label,
              );
              NodeAssert.equal(receiptRow(f, "interrupted-receipt").state, "partial", label);
              NodeAssert.equal(
                f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,
                "retiring",
                label,
              );
            } finally {
              await unlockTree(worktree);
              f.close();
            }
          });
      },
    "settling an interrupted retirement forgets the checkout's active non-evidence outputs on both paths":
      async () => {
        const forgotten: string[] = [];
        const records = (worktree: string): readonly OutputStub[] => [
          ...["active-build", "evidence-log", "retired-build"].map((id) => ({
            id,
            path: NodePath.join(worktree, "build", id),
            owner: "producer",
            class: id === "evidence-log" ? "evidence" : "build",
            state: id === "retired-build" ? "forgotten" : "active",
            worktree: null,
          })),
          {
            id: "other-worktree",
            path: NodePath.join(NodePath.dirname(worktree), "elsewhere", "build"),
            owner: "producer",
            class: "build",
            state: "active",
            worktree: null,
          },
        ];
        const registry = (worktree: string): RegistryStub => ({
          outputs: () => Effect.succeed(records(worktree)),
          forgetOutputs: (settled) =>
            Effect.sync(() => {
              forgotten.push(...settled.map((r) => r.id));
            }),
        });
        // The absent-checkout finish.
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "outputs-absent.sqlite3"));
          try {
            await interruptedRetirement(f, input, {
              ...(await holderIdentity()),
              boot_id: "a boot before the restart",
            });
            await command(repo, ["worktree", "remove", "--", worktree]);
            const result = await evaluate(f.sql, {
              worktree,
              projectRoot: repo,
              execute: true,
              cause: "owner_action",
              ownerCoverage: { fingerprint: "interrupted-receipt", paths: [] },
              registry: registry(worktree),
            });
            NodeAssert.equal(result.state, "retired");
            NodeAssert.deepEqual(forgotten, ["active-build"]);
          } finally {
            f.close();
          }
        });
        forgotten.length = 0;
        // Both settle paths share this finalization, and either spelling of the path finds the entries.
        await fixture(async ({ base, worktree, input }) => {
          const alias = NodePath.join(base, "alias");
          await NodeFSP.symlink(base, alias);
          const spelled = NodePath.join(alias, "checkout");
          const settled = { binding: input.expected } as RetirementReceipt;
          const run = (spellings: readonly string[]) =>
            Effect.runPromise(
              forgetWorktreeOutputs(registry(worktree) as never, spellings, settled),
            );
          await run([spelled]);
          NodeAssert.deepEqual(forgotten, ["active-build"]);
          forgotten.length = 0;
          await run([worktree, spelled]);
          NodeAssert.deepEqual(forgotten, ["active-build"]);
        });
      },
    "an absent checkout without an interrupted receipt keeps the binding failure": () =>
      fixture(async ({ base, repo, worktree }) => {
        const f = await fixtureSql(NodePath.join(base, "absent-plain.sqlite3"));
        try {
          await command(repo, ["worktree", "remove", "--", worktree]);
          for (const execute of [false, true])
            await NodeAssert.rejects(
              evaluate(f.sql, {
                worktree,
                projectRoot: repo,
                execute,
                cause: "owner_action",
                ownerCoverage: { fingerprint: "any", paths: [] },
              }),
              /observation_failed/,
            );
          NodeAssert.equal(
            f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_retirement_receipts").get()?.n,
            0,
          );
        } finally {
          f.close();
        }
      }),
  };
};
