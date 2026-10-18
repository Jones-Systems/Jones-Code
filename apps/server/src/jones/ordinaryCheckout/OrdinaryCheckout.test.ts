import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import {
  associationId,
  canonical,
  CheckoutError,
  operationId,
  type Binding,
  type Evidence,
  type Source,
  type Target,
} from "./CheckoutProtocol.ts";
import {
  CheckoutAcceptanceAuthority,
  CheckoutExecutorAuthority,
  CheckoutOwnershipAuthority,
  OrdinaryCheckout,
  type Accept,
} from "./OrdinaryCheckout.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";

const now = "2026-10-18T00:00:00.000Z";
const expiry = "2026-10-18T00:05:00.000Z";
const later = "2026-10-19T00:00:00.000Z";
const target: Target = {
  version: 1,
  projectId: "project",
  threadId: "thread",
  projectRoot: "/repo",
  worktreePath: null,
  threadBranch: "work",
  projectRevision: now,
  threadRevision: now,
  canonicalProjectRoot: "/repo",
  cwd: "/repo",
  checkoutKey: "/repo",
  directoryIdentity: { version: 1, device: "1", inode: "2", birth: "3" },
  gitCommonDirectory: "/repo/.git",
  gitDirectory: "/repo/.git",
  gitCommonIdentity: { version: 1, device: "1", inode: "git", birth: "3" },
  gitDirectoryIdentity: { version: 1, device: "1", inode: "git", birth: "3" },
  branch: "refs/heads/work",
};
const request = { type: "terminal.cleanup" };
const acceptedInput: Accept = {
  commandId: "command",
  commandType: "thread.test",
  receiptThreadId: "thread",
  subjectThreadId: "thread",
  command: { commandId: "command", type: "thread.test", text: "full content" },
  origin: { kind: "command", parentThreadId: null, requestId: null },
  admissionId: "admission",
  target,
  birth: { eventId: "birth", sequence: 1 },
  receiptSequence: 2,
  basisEventId: "basis",
  basisSequence: 2,
  relation: {
    runId: null,
    attemptId: null,
    nodeId: null,
    messageId: null,
    checkpointScopeId: null,
  },
  leaseId: "lease",
  now,
  expiresAt: expiry,
  expectedOwner: null,
  effects: [{ effectId: "effect", request, role: "launch" }],
};
const source: Source = {
  kind: "outbox",
  effectId: "effect",
  workerId: "worker",
  attempt: 1,
  claimExpiresAt: expiry,
};
const binding: Binding = {
  version: 1,
  operationId: operationId("admission", source),
  admissionId: "admission",
  executor: "outbox",
  issuerId: "test-executor",
  source,
  relation: acceptedInput.relation,
  provider: null,
};
const evidence = (outcome: Evidence["outcome"], observedAt = now): Evidence => ({
  version: 1,
  issuerId: "test-executor",
  reference: "test-owned-observation",
  outcome,
  coverage: "complete",
  observedAt,
});
// These are synthetic issuer ports, never installed in application composition.
const acceptance = CheckoutAcceptanceAuthority.of({
  assertNewAcceptance: () => Effect.void,
  assertDerivedLink: () => Effect.void,
});
const executor = CheckoutExecutorAuthority.of({
  assertCurrent: (b) =>
    b.issuerId === "test-executor"
      ? Effect.void
      : Effect.fail(new CheckoutError({ reason: "unavailable" })),
  reconcile: (b, e) =>
    b.issuerId === "test-executor" &&
    e.issuerId === "test-executor" &&
    e.reference === "test-owned-observation"
      ? Effect.void
      : Effect.fail(new CheckoutError({ reason: "identity" })),
});
const testLayer = Repository.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  Layer.provideMerge(NodePath.layer),
);
const provide = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | OrdinaryCheckout
    | SqlClient.SqlClient
    | CheckoutAcceptanceAuthority
    | CheckoutExecutorAuthority
    | CheckoutOwnershipAuthority
  >,
) =>
  effect.pipe(
    Effect.provideService(CheckoutOwnershipAuthority, { authorize: () => Effect.void }),
    Effect.provideService(CheckoutAcceptanceAuthority, acceptance),
    Effect.provideService(CheckoutExecutorAuthority, executor),
    Effect.provide(testLayer),
  );
const seed = Effect.gen(function* () {
  yield* runMigrations();
  const sql = yield* SqlClient.SqlClient;
  const payload = {
    id: "thread",
    projectId: "project",
    createdAt: now,
    updatedAt: now,
    deletedAt: null,
    worktreePath: null,
    branch: "work",
  };
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES('project','synthetic','/repo','[]',${now},${now})`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
    VALUES('thread','project','synthetic','codex','local','default',${now},${now},${canonical(payload)})`;
  for (const [sequence, id, type] of [
    [1, "birth", "thread.created"],
    [2, "basis", "thread.test"],
  ] as const)
    yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(${sequence},${id},'thread','thread',${sequence},${type},${now},${sequence === 1 ? null : "command"},'client',${canonical(payload)},'{}',2)`;
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
    VALUES('command','thread','thread','thread.test',${now},2,'accepted',NULL)`;
  yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at)
    VALUES('effect','command','thread','terminal.cleanup',${canonical(request)},'running',1,${now},'worker',${expiry},${now},${now})`;
  return { sql, checkout: yield* OrdinaryCheckout, payload };
});
const reserved = Effect.gen(function* () {
  const fixture = yield* seed;
  yield* fixture.checkout.accept(acceptedInput);
  const use = yield* fixture.checkout.reserve({
    admissionId: "admission",
    source,
    now,
    binding,
    evidence: evidence("active"),
  });
  return { ...fixture, use };
});
const started = Effect.gen(function* () {
  const fixture = yield* reserved;
  const use = yield* fixture.checkout.start({
    binding,
    evidence: evidence("active"),
    expectedRevision: fixture.use.revision,
    target,
  });
  return { ...fixture, use };
});

it.effect(
  "acceptance rollback shares the upstream transaction and leaves no partial Jones owner",
  () =>
    provide(
      Effect.gen(function* () {
        const { sql, checkout } = yield* seed;
        const before = yield* sql`SELECT * FROM orchestration_command_receipts`;
        const failed = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* checkout.accept(acceptedInput);
              yield* sql`UPDATE orchestration_command_receipts SET error='injected' WHERE command_id='command'`;
              return yield* new CheckoutError({ reason: "unavailable" });
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(failed));
        assert.strictEqual(yield* checkout.readOwner("/repo"), null);
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, before);
        assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_admissions`, []);
        assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_effect_links`, []);
      }),
    ),
);
it.effect(
  "exact replay writes nothing after expiry and target drift; changed content rejects",
  () =>
    provide(
      Effect.gen(function* () {
        const { sql, checkout, payload } = yield* seed;
        const original = yield* checkout.accept(acceptedInput);
        const owner = yield* checkout.readOwner("/repo");
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=${canonical({ ...payload, branch: "changed" })}`;
        const replay = yield* checkout.replay(acceptedInput);
        assert.strictEqual(replay.kind, "accepted");
        assert.deepEqual(
          yield* checkout.accept({
            ...acceptedInput,
            now: later,
            expiresAt: later,
            leaseId: "replacement",
          }),
          original,
        );
        assert.deepEqual(yield* checkout.readOwner("/repo"), owner);
        assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_uses`, []);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.replay({ ...acceptedInput, command: { ...request, text: "different" } }),
            ),
          ),
        );
      }),
    ),
);
it.effect(
  "historical receipt replay creates no owner and absent upstream acceptance is not executable",
  () =>
    provide(
      Effect.gen(function* () {
        const { sql, checkout } = yield* seed;
        assert.deepEqual(yield* checkout.replay(acceptedInput), {
          kind: "historical",
          receiptSequence: 2,
        });
        assert.strictEqual(yield* checkout.readOwner("/repo"), null);
        yield* checkout.accept(acceptedInput);
        yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id='command'`;
        assert.strictEqual(yield* checkout.readAdmission("admission"), null);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(checkout.replay(acceptedInput))));
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.reserve({
                admissionId: "admission",
                source,
                now,
                binding,
                evidence: evidence("active"),
              }),
            ),
          ),
        );
        assert.strictEqual((yield* checkout.readConflict("/repo")).length, 1);
      }),
    ),
);
it.effect.each(["worker", "attempt", "deadline", "issuer"] as const)(
  "rejects a stale or fabricated %s before invocation",
  (kind) =>
    provide(
      Effect.gen(function* () {
        const { checkout } = yield* seed;
        yield* checkout.accept(acceptedInput);
        const changed: Source = {
          ...source,
          ...(kind === "worker"
            ? { workerId: "foreign" }
            : kind === "attempt"
              ? { attempt: 2 }
              : kind === "deadline"
                ? { claimExpiresAt: later }
                : {}),
        };
        const candidate = {
          ...binding,
          source: changed,
          operationId: operationId("admission", changed),
          ...(kind === "issuer" ? { issuerId: "fabricated" } : {}),
        };
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.reserve({
                admissionId: "admission",
                source: changed,
                now,
                binding: candidate,
                evidence: evidence("active"),
              }),
            ),
          ),
        );
        assert.deepEqual(yield* checkout.listUnsettled(null, 10), []);
      }),
    ),
);
it.effect("outbox success and root retirement cannot settle joined background work", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, sql, use } = yield* started;
      const background: Binding = { ...binding, executor: "background" };
      const joined = yield* checkout.join({
        target,
        binding: background,
        evidence: evidence("active"),
        expectedRevision: use.revision,
      });
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL WHERE effect_id='effect'`;
      const rootDone = yield* checkout.reconcileEvidence({
        binding,
        evidence: evidence("quiescent"),
        expectedRevision: joined.revision,
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.complete({
              operationId: use.operation_id,
              expectedRevision: rootDone.revision,
              now,
              target,
            }),
          ),
        ),
      );
      const allDone = yield* checkout.reconcileEvidence({
        binding: background,
        evidence: evidence("quiescent", later),
        expectedRevision: rootDone.revision,
      });
      const complete = yield* checkout.complete({
        operationId: use.operation_id,
        expectedRevision: allDone.revision,
        now: later,
        target,
      });
      assert.strictEqual(complete.state, "completed");
      assert.strictEqual((yield* checkout.readOwner("/repo"))!.lease_id, "lease");
      assert.strictEqual((yield* checkout.readConflict("/repo/sub"))[0]!.kind, "ordinary");
    }),
  ),
);
it.effect(
  "restart records unknown, fences renewal and accepts only attributed old-executor retirement",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, use } = yield* started;
        const unknown = yield* checkout.markRestartUnknown({
          operationId: use.operation_id,
          expectedRevision: use.revision,
          now: later,
        });
        assert.strictEqual(unknown.state, "unknown");
        const journal = yield* checkout.events(use.operation_id);
        assert.strictEqual(journal.at(-1)!.event_kind, "unknown");
        assert.strictEqual(journal.at(-1)!.association_id, associationId(binding));
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.renew({
                target,
                binding,
                evidence: evidence("active", later),
                expectedRevision: unknown.revision,
                expectedOwnerRevision: 0,
                expiresAt: later,
              }),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.reconcileEvidence({
                binding,
                evidence: { ...evidence("quiescent", later), reference: "invented" },
                expectedRevision: unknown.revision,
              }),
            ),
          ),
        );
        const retired = yield* checkout.reconcileEvidence({
          binding,
          evidence: evidence("quiescent", later),
          expectedRevision: unknown.revision,
        });
        assert.strictEqual(
          (yield* checkout.complete({
            operationId: use.operation_id,
            expectedRevision: retired.revision,
            now: later,
            target,
          })).state,
          "completed",
        );
      }),
    ),
);
it.effect(
  "reserved no-effect requires revoked producer evidence; invocation cannot be relabeled no-effect",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, use } = yield* reserved;
        const retired = yield* checkout.reconcileEvidence({
          binding,
          evidence: evidence("not_invoked"),
          expectedRevision: use.revision,
        });
        assert.strictEqual(
          (yield* checkout.complete({
            operationId: use.operation_id,
            expectedRevision: retired.revision,
            now,
            target,
          })).state,
          "no_effect",
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.start({
                binding,
                evidence: evidence("active"),
                expectedRevision: use.revision,
                target,
              }),
            ),
          ),
        );
      }),
    ),
);
it.effect("partial coverage stays unknown and a stale completion cannot race a late join", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, use } = yield* started;
      const joined = yield* checkout.join({
        target,
        binding: { ...binding, executor: "checkpoint" },
        evidence: evidence("active"),
        expectedRevision: use.revision,
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.complete({
              operationId: use.operation_id,
              expectedRevision: use.revision,
              now,
              target,
            }),
          ),
        ),
      );
      const unknown = yield* checkout.reconcileEvidence({
        binding,
        evidence: { ...evidence("quiescent"), coverage: "partial" },
        expectedRevision: joined.revision,
      });
      assert.strictEqual(unknown.state, "unknown");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.complete({
              operationId: use.operation_id,
              expectedRevision: unknown.revision,
              now,
              target,
            }),
          ),
        ),
      );
    }),
  ),
);
it.effect("renewal is monotonic and preserves owner acquisition identity", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, use } = yield* started;
      const before = (yield* checkout.readOwner("/repo"))!;
      const renewed = yield* checkout.renew({
        target,
        binding,
        evidence: evidence("active", later),
        expectedRevision: use.revision,
        expectedOwnerRevision: 0,
        expiresAt: later,
      });
      const after = (yield* checkout.readOwner("/repo"))!;
      assert.strictEqual(after.lease_id, before.lease_id);
      assert.strictEqual(after.acquired_at, before.acquired_at);
      assert.strictEqual(after.owner_epoch, before.owner_epoch);
      assert.strictEqual(after.expires_at, later);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.renew({
              target,
              binding,
              evidence: evidence("active", later),
              expectedRevision: renewed.revision,
              expectedOwnerRevision: after.revision,
              expiresAt: expiry,
            }),
          ),
        ),
      );
    }),
  ),
);
it.effect("pruned effects are absent to consumers but retain ordinary exclusion history", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, sql } = yield* reserved;
      yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id='effect'`;
      assert.deepEqual(yield* checkout.readEffect("effect"), []);
      assert.strictEqual((yield* checkout.readConflict("/repo"))[0]!.checkoutKey, "/repo");
    }),
  ),
);
it.effect("native/deletion and expired legacy ownership are reciprocal admission fences", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, sql } = yield* seed;
      yield* sql`INSERT INTO worktree_ownership_leases VALUES('/repo/sub','legacy','other','birth',NULL,0,0,1)`;
      assert.strictEqual((yield* checkout.readConflict("/repo"))[0]!.kind, "legacy");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(checkout.accept(acceptedInput))));
      assert.strictEqual(yield* checkout.readOwner("/repo"), null);
    }),
  ),
);

const nextCommand = (otherThread = false) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = otherThread ? "other-thread" : "thread";
    const nextTarget = { ...target, threadId };
    if (otherThread) {
      const payload = {
        id: threadId,
        projectId: "project",
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
        worktreePath: null,
        branch: "work",
      };
      yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
      VALUES(${threadId},'project','synthetic','codex','local','default',${now},${now},${canonical(payload)})`;
      yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(3,'other-birth','thread',${threadId},1,'thread.created',${now},NULL,'client',${canonical(payload)},'{}',2)`;
    }
    yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES(4,'next-basis','thread',${threadId},4,'thread.test',${now},'next-command','client','{}','{}',2)`;
    yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
    VALUES('next-command','thread',${threadId},'thread.test',${now},4,'accepted',NULL)`;
    const input: Accept = {
      ...acceptedInput,
      commandId: "next-command",
      command: { type: "thread.test", commandId: "next-command" },
      admissionId: "next-admission",
      subjectThreadId: threadId,
      receiptThreadId: threadId,
      target: nextTarget,
      birth: otherThread ? { eventId: "other-birth", sequence: 3 } : acceptedInput.birth,
      basisEventId: "next-basis",
      basisSequence: 4,
      receiptSequence: 4,
      effects: [],
      now: later,
      expiresAt: later,
      leaseId: "new-lease",
    };
    return input;
  });
it.effect(
  "a completed same-owner command reuses the expired lease and its acquisition identity",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, use } = yield* started;
        const retired = yield* checkout.reconcileEvidence({
          binding,
          evidence: evidence("quiescent"),
          expectedRevision: use.revision,
        });
        yield* checkout.complete({
          operationId: use.operation_id,
          expectedRevision: retired.revision,
          now,
          target,
        });
        const before = (yield* checkout.readOwner("/repo"))!;
        const next = yield* nextCommand();
        const accepted = yield* checkout.accept({
          ...next,
          expectedOwner: { epoch: before.owner_epoch, revision: before.revision },
        });
        assert.strictEqual(accepted.lease_id, before.lease_id);
        assert.deepEqual(yield* checkout.readOwner("/repo"), before);
      }),
    ),
);
it.effect("another thread and a recreated birth cannot inherit an expired lease", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, sql, payload } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const next = yield* nextCommand(true);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.accept({ ...next, expectedOwner: { epoch: 0, revision: 0 } }),
          ),
        ),
      );
      yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES(5,'recreated-birth','thread','thread',5,'thread.created',${now},NULL,'client',${canonical(payload)},'{}',2)`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.reserve({
              admissionId: "admission",
              source,
              now,
              binding,
              evidence: evidence("active"),
            }),
          ),
        ),
      );
    }),
  ),
);
const transitionBinding: Binding = { ...binding, executor: "transition" };
const transitionStarted = Effect.gen(function* () {
  const fixture = yield* seed;
  yield* fixture.checkout.accept(acceptedInput);
  const reserved = yield* fixture.checkout.reserve({
    admissionId: "admission",
    source,
    now,
    binding: transitionBinding,
    evidence: evidence("active"),
  });
  const use = yield* fixture.checkout.start({
    binding: transitionBinding,
    evidence: evidence("active"),
    expectedRevision: reserved.revision,
    target,
  });
  return { ...fixture, use };
});
it.effect("release and exact handoff advance the epoch once; old callbacks stay fenced", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, use } = yield* transitionStarted;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.release({
              operationId: use.operation_id,
              expectedRevision: use.revision,
              target,
              binding: { ...binding, executor: "transition" },
              evidence: evidence("quiescent"),
              expectedOwner: { epoch: 0, revision: 0 },
            }),
          ),
        ),
      );
      const retired = yield* checkout.reconcileEvidence({
        binding: transitionBinding,
        evidence: evidence("quiescent"),
        expectedRevision: use.revision,
      });
      const done = yield* checkout.complete({
        operationId: use.operation_id,
        expectedRevision: retired.revision,
        now,
        target,
      });
      const transition = {
        operationId: use.operation_id,
        expectedRevision: done.revision,
        target,
        binding: { ...binding, executor: "transition" as const },
        evidence: evidence("quiescent"),
        expectedOwner: { epoch: 0, revision: 0 },
      };
      const released = yield* checkout.release(transition);
      assert.strictEqual(released.state, "released");
      const next = yield* nextCommand(true);
      const handed = yield* checkout.handoff({
        ...transition,
        expectedRevision: done.revision + 1,
        expectedOwner: { epoch: 0, revision: released.revision },
        next: { target: next.target, birth: next.birth, leaseId: "successor", expiresAt: expiry },
      });
      assert.strictEqual(handed.owner_epoch, 1);
      assert.strictEqual(handed.lease_id, "successor");
      assert.strictEqual(handed.owner_thread_id, "other-thread");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.reconcileEvidence({
              binding: transitionBinding,
              evidence: evidence("quiescent"),
              expectedRevision: done.revision,
            }),
          ),
        ),
      );
      assert.deepEqual(
        (yield* checkout.events(use.operation_id)).slice(-2).map((event) => event.event_kind),
        ["release", "handoff"],
      );
    }),
  ),
);
it.effect("a linked checkpoint stays pending until its own attributed participant retires", () =>
  provide(
    Effect.gen(function* () {
      const { checkout, sql, use } = yield* started;
      yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES(3,'terminal-event','thread','thread',3,'run.finished',${now},'derived','system','{}','{}',2)`;
      yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at)
    VALUES('checkpoint','derived','thread','checkpoint.capture','{"type":"checkpoint.capture"}','running',1,${now},'worker',${expiry},${now},${now})`;
      const linked = yield* checkout.linkDerived({
        operationId: use.operation_id,
        expectedRevision: use.revision,
        admissionId: "admission",
        basisEventId: "terminal-event",
        basisSequence: 3,
        now,
        link: {
          effectId: "checkpoint",
          request: { type: "checkpoint.capture" },
          role: "checkpoint",
        },
      });
      const retired = yield* checkout.reconcileEvidence({
        binding,
        evidence: evidence("quiescent"),
        expectedRevision: linked.revision,
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.complete({
              operationId: use.operation_id,
              expectedRevision: retired.revision,
              now,
              target,
            }),
          ),
        ),
      );
      const checkpoint: Binding = {
        ...binding,
        executor: "checkpoint",
        source: { ...source, effectId: "checkpoint" },
      };
      const joined = yield* checkout.join({
        target,
        binding: checkpoint,
        evidence: evidence("active"),
        expectedRevision: retired.revision,
      });
      const captured = yield* checkout.reconcileEvidence({
        binding: checkpoint,
        evidence: evidence("quiescent"),
        expectedRevision: joined.revision,
      });
      assert.strictEqual(
        (yield* checkout.complete({
          operationId: use.operation_id,
          expectedRevision: captured.revision,
          now,
          target,
        })).state,
        "completed",
      );
    }),
  ),
);

const scratch = process.env.JONES_CHECKOUT_TEST_SCRATCH;
it.effect.skipIf(!scratch)(
  "two independent SQLite connections accept exactly one competing owner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        // The verifier supplies an existing task-owned directory. This fixture owns only the unique child and cleans it after both connections close.
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          directory: scratch!,
          prefix: "ordinary-checkout-",
        });
        const connection = () =>
          Repository.layer.pipe(
            Layer.provideMerge(
              NodeSqliteClient.layer({
                filename: path.join(directory, "fixture.sqlite"),
                readerWorker: false,
                checkpointWorker: false,
              }),
            ),
            Layer.provideMerge(NodePath.layer),
          );
        const left = yield* Layer.build(connection());
        yield* seed.pipe(Effect.provide(left));
        const second = yield* nextCommand(true).pipe(Effect.provide(left));
        const right = yield* Layer.build(connection());
        const results = yield* Effect.all(
          [
            Effect.flatMap(OrdinaryCheckout, (checkout) => checkout.accept(acceptedInput)).pipe(
              Effect.provide(left),
              Effect.exit,
            ),
            Effect.flatMap(OrdinaryCheckout, (checkout) => checkout.accept(second)).pipe(
              Effect.provide(right),
              Effect.exit,
            ),
          ],
          { concurrency: 2 },
        );
        assert.strictEqual(results.filter(Exit.isSuccess).length, 1);
        const rows = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql`SELECT * FROM jones_ordinary_checkout_owners`;
        }).pipe(Effect.provide(left));
        assert.strictEqual(rows.length, 1);
      }),
    ).pipe(
      Effect.provideService(CheckoutAcceptanceAuthority, acceptance),
      Effect.provide(NodeServices.layer),
    ),
);

it.effect(
  "immutable admission/use/event identity rejects update, replace and out-of-order append",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql, use } = yield* reserved;
        const before = yield* checkout.events(use.operation_id);
        for (const mutation of [
          sql`UPDATE jones_ordinary_checkout_admissions SET command_digest=${"b".repeat(64)}`,
          sql`DELETE FROM jones_ordinary_checkout_admissions`,
          sql`INSERT OR REPLACE INTO jones_ordinary_checkout_effect_links SELECT * FROM jones_ordinary_checkout_effect_links`,
          sql`UPDATE jones_ordinary_checkout_uses SET worker_id='foreign',revision=revision+1`,
          sql`INSERT OR REPLACE INTO jones_ordinary_checkout_uses SELECT * FROM jones_ordinary_checkout_uses`,
          sql`UPDATE jones_ordinary_checkout_lifecycle_events SET evidence_json='{}'`,
          sql`DELETE FROM jones_ordinary_checkout_lifecycle_events`,
          sql`INSERT INTO jones_ordinary_checkout_lifecycle_events SELECT operation_id,ordinal+2,ordinal+1,association_id,event_kind,binding_json,evidence_json,recorded_at FROM jones_ordinary_checkout_lifecycle_events`,
        ])
          assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));
        assert.deepEqual(yield* checkout.events(use.operation_id), before);
      }),
    ),
);
it.effect(
  "branch transition requires quietness and records before/after without rotating lease identity",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql, payload, use } = yield* transitionStarted;
        const after = {
          ...target,
          branch: "refs/heads/next",
          threadBranch: "next",
          threadRevision: later,
        };
        const transition = {
          operationId: use.operation_id,
          expectedRevision: use.revision,
          target,
          after,
          binding: { ...binding, executor: "transition" as const },
          evidence: evidence("quiescent"),
          expectedOwner: { epoch: 0, revision: 0 },
        };
        assert.isTrue(Exit.isFailure(yield* Effect.exit(checkout.transitionBranch(transition))));
        const retired = yield* checkout.reconcileEvidence({
          binding: transitionBinding,
          evidence: evidence("quiescent"),
          expectedRevision: use.revision,
        });
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=${canonical({ ...payload, branch: "next", updatedAt: later })}`;
        const owner = yield* checkout.transitionBranch({
          ...transition,
          expectedRevision: retired.revision,
        });
        assert.strictEqual(owner.branch, after.branch);
        assert.strictEqual(owner.lease_id, "lease");
        assert.strictEqual(owner.target_revision, 1);
        assert.strictEqual(owner.acquired_at, now);
        const event = (yield* checkout.events(use.operation_id)).at(-1)!;
        assert.strictEqual(event.event_kind, "target_transition");
        assert.include(event.evidence_json, '"beforeBranch":"refs/heads/work"');
        assert.include(event.evidence_json, '"afterBranch":"refs/heads/next"');
      }),
    ),
);
it.effect(
  "prepared producers require their actual issuer and preserve the distinct source identity",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout } = yield* seed;
        yield* checkout.accept({ ...acceptedInput, effects: [] });
        const prepared: Source = { kind: "prepared_launch", producerId: "test-owned-producer" };
        const bound: Binding = {
          ...binding,
          source: prepared,
          operationId: operationId("admission", prepared),
          executor: "prepared",
        };
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.reserve({
                admissionId: "admission",
                source: prepared,
                now,
                binding: { ...bound, issuerId: "copied-json" },
                evidence: evidence("active"),
              }),
            ),
          ),
        );
        const reservation = yield* checkout.reserve({
          admissionId: "admission",
          source: prepared,
          now,
          binding: bound,
          evidence: evidence("active"),
        });
        assert.strictEqual(reservation.source_kind, "prepared_launch");
        assert.strictEqual(reservation.effect_id, null);
        const retired = yield* checkout.reconcileEvidence({
          binding: bound,
          evidence: evidence("not_invoked"),
          expectedRevision: reservation.revision,
        });
        assert.strictEqual(
          (yield* checkout.complete({
            operationId: bound.operationId,
            expectedRevision: retired.revision,
            now,
            target,
          })).state,
          "no_effect",
        );
      }),
    ),
);
it.effect(
  "an imported application birth uses the existing birth validator without native-creation authority",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql, payload } = yield* seed;
        const imported = { ...payload, historyOrigin: "v1_import" };
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=${canonical(imported)}`;
        yield* sql`UPDATE orchestration_events SET payload_json=${canonical(imported)} WHERE event_id='birth'`;
        assert.strictEqual((yield* checkout.accept(acceptedInput)).birth_event_id, "birth");
        assert.deepEqual(yield* sql`SELECT * FROM jones_native_creation_execution_acceptances`, []);
      }),
    ),
);
