import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import {
  makeRuntimeOperationPermit,
  RuntimePermitError,
  type RuntimeOperationIdentity,
  type ScopeObservation,
} from "../runtime/RuntimeOperationPermit.ts";
import { makeCheckoutPermitAdapter } from "./CheckoutPermitAdapter.ts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
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

// Exact synthetic issuer receipts bind every proof to its participant and outcome.
const proof = (bound: Binding, outcome: Evidence["outcome"]): Evidence => ({
  ...evidence(outcome),
  issuerId: bound.issuerId,
  reference: `${associationId(bound)}:${outcome}`,
});
const exactExecutor = (bindings: readonly Binding[]) =>
  CheckoutExecutorAuthority.of({
    assertCurrent: (bound) =>
      bindings.some((b) => canonical(b) === canonical(bound))
        ? Effect.void
        : Effect.fail(new CheckoutError({ reason: "stale" })),
    reconcile: (bound, observed) =>
      bindings.some((b) => canonical(b) === canonical(bound)) &&
      canonical(observed) === canonical(proof(bound, observed.outcome))
        ? Effect.void
        : Effect.fail(new CheckoutError({ reason: "identity" })),
  });
const withConnections = <A, E, R>(
  body: (
    left: OrdinaryCheckout["Service"],
    right: OrdinaryCheckout["Service"],
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: scratch!,
        prefix: "named-repairs-",
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
      const right = yield* Layer.build(connection());
      const leftCheckout = yield* OrdinaryCheckout.pipe(Effect.provide(left));
      const rightCheckout = yield* OrdinaryCheckout.pipe(Effect.provide(right));
      return yield* body(leftCheckout, rightCheckout).pipe(Effect.provide(left));
    }),
  ).pipe(
    Effect.provideService(CheckoutAcceptanceAuthority, acceptance),
    Effect.provideService(CheckoutExecutorAuthority, executor),
    Effect.provide(NodeServices.layer),
  );
const targetAt = (input: Accept, key: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath',${key}) WHERE thread_id=${input.subjectThreadId}`;
    return { ...input, target: { ...input.target, worktreePath: key, cwd: key, checkoutKey: key } };
  });
for (const child of ["/repo/😀/checkout", "/repo/\uffff/checkout", "/repo/\u{10ffff}/checkout"]) {
  for (const descendantFirst of [true, false]) {
    it.effect.skipIf(!scratch)(
      `DCA-COR-001 excludes ${child} descendantFirst=${descendantFirst}`,
      () =>
        withConnections((left, right) =>
          Effect.gen(function* () {
            const second = yield* nextCommand(true);
            const firstInput = yield* targetAt(acceptedInput, descendantFirst ? child : "/repo");
            const secondInput = yield* targetAt(second, descendantFirst ? "/repo" : child);
            yield* left.accept(firstInput);
            const failure = yield* right.accept(secondInput).pipe(Effect.flip);
            assert.strictEqual(failure.reason, "conflict");
            assert.deepEqual(
              (yield* right.readConflict(secondInput.target.checkoutKey)).map((c) => c.checkoutKey),
              [firstInput.target.checkoutKey],
            );
            assert.strictEqual(yield* right.readOwner(secondInput.target.checkoutKey), null);
          }),
        ),
    );
  }
}
for (const [firstKey, secondKey, conflict] of [
  ["/😀", "/", true],
  ["/", "/\uffff", true],
  ["/repo", "/repository", false],
  ["/Repo", "/repo", false],
] as const) {
  it.effect.skipIf(!scratch)(
    `DCA-COR-001 preserves path boundary ${firstKey} -> ${secondKey}`,
    () =>
      withConnections((left, right) =>
        Effect.gen(function* () {
          const second = yield* nextCommand(true);
          yield* left.accept(yield* targetAt(acceptedInput, firstKey));
          const secondInput = yield* targetAt(second, secondKey);
          if (conflict)
            assert.strictEqual(
              (yield* right.accept(secondInput).pipe(Effect.flip)).reason,
              "conflict",
            );
          else assert.strictEqual((yield* right.accept(secondInput)).checkout_key, secondKey);
        }),
      ),
  );
}
it.effect.skipIf(!scratch)(
  "DCA-COR-002 retires an exact uninvoked joined producer and preserves started history",
  () =>
    withConnections((left, right) =>
      Effect.gen(function* () {
        const background: Binding = {
          ...binding,
          issuerId: "background-issuer",
          executor: "background",
          source: { kind: "prepared_run", producerId: "uninvoked-background" },
        };
        yield* left.accept(acceptedInput);
        const use = yield* left.reserve({
          admissionId: "admission",
          source,
          now,
          binding,
          evidence: proof(binding, "active"),
        });
        const active = yield* left.start({
          binding,
          evidence: proof(binding, "active"),
          expectedRevision: use.revision,
          target,
        });
        const joined = yield* right.join({
          binding: background,
          evidence: proof(background, "active"),
          expectedRevision: active.revision,
          target,
        });
        const before = yield* right.events(use.operation_id);
        for (const invalid of [
          { ...proof(background, "not_invoked"), issuerId: binding.issuerId },
          { ...proof(background, "not_invoked"), reference: "invented" },
          { ...proof(background, "not_invoked"), coverage: "partial" as const },
        ]) {
          assert.strictEqual(
            (yield* right
              .reconcileEvidence({
                binding: background,
                evidence: invalid,
                expectedRevision: joined.revision,
              })
              .pipe(Effect.flip)).reason,
            "identity",
          );
          assert.deepEqual(yield* left.events(use.operation_id), before);
        }
        assert.strictEqual(
          (yield* left
            .reconcileEvidence({
              binding,
              evidence: proof(binding, "not_invoked"),
              expectedRevision: joined.revision,
            })
            .pipe(Effect.flip)).reason,
          "unknown",
        );
        const cancelled = yield* right.reconcileEvidence({
          binding: background,
          evidence: proof(background, "not_invoked"),
          expectedRevision: joined.revision,
        });
        assert.strictEqual(cancelled.state, "draining");
        assert.strictEqual(cancelled.started_at, active.started_at);
        assert.strictEqual(
          (yield* left
            .complete({
              operationId: use.operation_id,
              expectedRevision: cancelled.revision,
              now,
              target,
            })
            .pipe(Effect.flip)).reason,
          "unknown",
        );
        const retired = yield* left.reconcileEvidence({
          binding,
          evidence: proof(binding, "quiescent"),
          expectedRevision: cancelled.revision,
        });
        const complete = yield* right.complete({
          operationId: use.operation_id,
          expectedRevision: retired.revision,
          now,
          target,
        });
        assert.strictEqual(complete.state, "completed");
        assert.strictEqual(complete.started_at, now);
        const journal = yield* left.events(use.operation_id);
        assert.deepEqual(
          journal.map((e) => e.event_kind),
          ["bind", "activate", "join", "retire", "retire"],
        );
        assert.strictEqual(journal[3]!.binding_json, canonical(background));
        assert.strictEqual(journal[3]!.evidence_json, canonical(proof(background, "not_invoked")));
      }).pipe(
        Effect.provideService(
          CheckoutExecutorAuthority,
          exactExecutor([
            binding,
            {
              ...binding,
              issuerId: "background-issuer",
              executor: "background",
              source: { kind: "prepared_run", producerId: "uninvoked-background" },
            },
          ]),
        ),
      ),
    ),
);
for (const kind of ["outbox", "prepared_launch"] as const) {
  it.effect.skipIf(!scratch)(
    `DCA-COR-003 cancels ${kind} and every pending initial link without invocation`,
    () =>
      withConnections((left, right) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at)
        VALUES('second-effect','command','thread','terminal.cleanup',${canonical(request)},'pending',0,${now},${now},${now})`;
          const rootSource: Source =
            kind === "outbox" ? source : { kind, producerId: "prepared-producer" };
          const root: Binding = {
            ...binding,
            source: rootSource,
            executor: kind === "outbox" ? "outbox" : "prepared",
            operationId: operationId("admission", rootSource),
          };
          const pending: Binding = {
            ...binding,
            operationId: root.operationId,
            issuerId: "pending-issuer",
            source: { ...source, effectId: "second-effect", attempt: 0 },
          };
          const first: Binding = { ...binding, operationId: root.operationId };
          const competingSource: Source = {
            kind: "prepared_run",
            producerId: "competing-producer",
          };
          const competing: Binding = {
            ...binding,
            executor: "prepared",
            source: competingSource,
            operationId: operationId("admission", competingSource),
          };
          const run = Effect.gen(function* () {
            yield* left.accept({
              ...acceptedInput,
              effects: [
                ...acceptedInput.effects,
                { effectId: "second-effect", request, role: "checkpoint" },
              ],
            });
            const use = yield* left.reserve({
              admissionId: "admission",
              source: rootSource,
              now,
              binding: root,
              evidence: proof(root, "active"),
            });
            const retired = yield* left.reconcileEvidence({
              binding: root,
              evidence: proof(root, "not_invoked"),
              expectedRevision: use.revision,
            });
            assert.strictEqual(
              (yield* right
                .complete({
                  operationId: root.operationId,
                  expectedRevision: retired.revision,
                  now,
                  target,
                })
                .pipe(Effect.flip)).reason,
              "unknown",
            );
            const before = yield* left.events(root.operationId);
            assert.strictEqual(
              (yield* right
                .join({
                  binding: pending,
                  evidence: proof(pending, "active"),
                  expectedRevision: retired.revision,
                  target,
                })
                .pipe(Effect.flip)).reason,
              "stale",
            );
            assert.strictEqual(
              (yield* right
                .reserve({
                  admissionId: "admission",
                  source: competingSource,
                  now,
                  binding: competing,
                  evidence: proof(competing, "active"),
                })
                .pipe(Effect.flip)).reason,
              "conflict",
            );
            for (const invalid of [
              { ...proof(pending, "not_invoked"), reference: "invented" },
              { ...proof(pending, "not_invoked"), issuerId: root.issuerId },
              { ...proof(pending, "not_invoked"), coverage: "partial" as const },
              proof(pending, "quiescent"),
              proof(pending, "active"),
            ]) {
              const failure = yield* right
                .reconcileEvidence({
                  binding: pending,
                  evidence: invalid,
                  expectedRevision: retired.revision,
                })
                .pipe(Effect.flip);
              assert.strictEqual(failure.reason, "identity");
              assert.deepEqual(yield* left.events(root.operationId), before);
            }
            const cancelled = yield* right.reconcileEvidence({
              binding: pending,
              evidence: proof(pending, "not_invoked"),
              expectedRevision: retired.revision,
            });
            assert.strictEqual(cancelled.state, "reserved");
            assert.strictEqual(cancelled.started_at, null);
            for (const bound of [root, pending]) {
              assert.strictEqual(
                (yield* left
                  .start({
                    binding: bound,
                    evidence: proof(bound, "active"),
                    expectedRevision: cancelled.revision,
                    target,
                  })
                  .pipe(Effect.flip)).reason,
                "stale",
              );
            }
            let final = cancelled;
            if (kind === "prepared_launch") {
              assert.strictEqual(
                (yield* left
                  .complete({
                    operationId: root.operationId,
                    expectedRevision: cancelled.revision,
                    now,
                    target,
                  })
                  .pipe(Effect.flip)).reason,
                "unknown",
              );
              final = yield* left.reconcileEvidence({
                binding: first,
                evidence: proof(first, "not_invoked"),
                expectedRevision: cancelled.revision,
              });
            }
            assert.strictEqual(
              (yield* left
                .complete({
                  operationId: root.operationId,
                  expectedRevision: retired.revision,
                  now,
                  target,
                })
                .pipe(Effect.flip)).reason,
              "stale",
            );
            const complete = yield* right.complete({
              operationId: root.operationId,
              expectedRevision: final.revision,
              now,
              target,
            });
            assert.strictEqual(complete.state, "no_effect");
            assert.strictEqual(complete.started_at, null);
            assert.deepEqual(yield* left.listUnsettled(null, 10), []);
            const journal = yield* left.events(root.operationId);
            assert.deepEqual(
              journal.map((e) => e.event_kind),
              kind === "outbox"
                ? ["bind", "retire", "retire"]
                : ["bind", "retire", "retire", "retire"],
            );
            assert.strictEqual(journal[2]!.binding_json, canonical(pending));
            assert.strictEqual(journal[2]!.evidence_json, canonical(proof(pending, "not_invoked")));
            assert.deepEqual(
              yield* sql`SELECT status,attempt_count,lease_owner FROM orchestration_v2_effect_outbox WHERE effect_id='second-effect'`,
              [{ status: "pending", attempt_count: 0, lease_owner: null }],
            );
            const next = yield* nextCommand();
            const owner = (yield* left.readOwner("/repo"))!;
            assert.strictEqual(
              (yield* right.accept({
                ...next,
                expectedOwner: { epoch: owner.owner_epoch, revision: owner.revision },
              })).admission_id,
              next.admissionId,
            );
          });
          yield* run.pipe(
            Effect.provideService(
              CheckoutExecutorAuthority,
              exactExecutor([root, pending, first, competing]),
            ),
          );
        }),
      ),
  );
}

it.effect.each(["missing", "digest", "thread", "unlinked"] as const)(
  "DCA-COR-003 refuses a %s outbox link even with issuer-accepted cancellation proof",
  (mismatch) =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql, use } = yield* reserved;
        yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at)
      VALUES('pending','derived','thread','terminal.cleanup',${canonical(request)},'pending',0,${now},${now},${now})`;
        yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(3,'derived-event','thread','thread',3,'thread.test',${now},'derived','system','{}','{}',2)`;
        const linked =
          mismatch === "unlinked"
            ? use
            : yield* checkout.linkDerived({
                operationId: use.operation_id,
                expectedRevision: use.revision,
                admissionId: "admission",
                basisEventId: "derived-event",
                basisSequence: 3,
                now,
                link: { effectId: "pending", request, role: "checkpoint" },
              });
        if (mismatch === "missing")
          yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id='pending'`;
        if (mismatch === "digest")
          yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type":"changed"}' WHERE effect_id='pending'`;
        if (mismatch === "thread")
          yield* sql`UPDATE orchestration_v2_effect_outbox SET thread_id='foreign' WHERE effect_id='pending'`;
        const pending: Binding = {
          ...binding,
          source: { ...source, effectId: "pending", attempt: 0 },
        };
        const before = yield* checkout.events(use.operation_id);
        const failure = yield* checkout
          .reconcileEvidence({
            binding: pending,
            evidence: proof(pending, "not_invoked"),
            expectedRevision: linked.revision,
          })
          .pipe(
            Effect.provideService(CheckoutExecutorAuthority, exactExecutor([pending])),
            Effect.flip,
          );
        assert.strictEqual(failure.reason, "identity");
        assert.deepEqual(yield* checkout.events(use.operation_id), before);
        assert.deepEqual(yield* checkout.listUnsettled(null, 10), [linked]);
      }),
    ),
);
it.effect.skipIf(!scratch)(
  "DCA-COR-003 final coverage observes a late link from the other SQLite connection",
  () =>
    withConnections((left, right) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* left.accept(acceptedInput);
        const use = yield* left.reserve({
          admissionId: "admission",
          source,
          now,
          binding,
          evidence: evidence("active"),
        });
        const retired = yield* left.reconcileEvidence({
          binding,
          evidence: evidence("not_invoked"),
          expectedRevision: use.revision,
        });
        yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(3,'derived-event','thread','thread',3,'thread.test',${now},'derived','system','{}','{}',2)`;
        yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at)
      VALUES('pending','derived','thread','terminal.cleanup',${canonical(request)},'pending',0,${now},${now},${now})`;
        const linked = yield* right.linkDerived({
          operationId: use.operation_id,
          expectedRevision: retired.revision,
          admissionId: "admission",
          basisEventId: "derived-event",
          basisSequence: 3,
          now,
          link: { effectId: "pending", request, role: "checkpoint" },
        });
        for (const [revision, reason] of [
          [retired.revision, "stale"],
          [linked.revision, "unknown"],
        ] as const) {
          assert.strictEqual(
            (yield* left
              .complete({ operationId: use.operation_id, expectedRevision: revision, now, target })
              .pipe(Effect.flip)).reason,
            reason,
          );
          assert.deepEqual(yield* right.listUnsettled(null, 10), [linked]);
        }
        const pending: Binding = {
          ...binding,
          source: { ...source, effectId: "pending", attempt: 0 },
        };
        const cancelled = yield* left
          .reconcileEvidence({
            binding: pending,
            evidence: proof(pending, "not_invoked"),
            expectedRevision: linked.revision,
          })
          .pipe(Effect.provideService(CheckoutExecutorAuthority, exactExecutor([pending])));
        const complete = yield* right.complete({
          operationId: use.operation_id,
          expectedRevision: cancelled.revision,
          now,
          target,
        });
        assert.strictEqual(complete.state, "no_effect");
        assert.strictEqual(complete.started_at, null);
        assert.deepEqual(
          (yield* right.events(use.operation_id)).map((event) => event.event_kind),
          ["bind", "retire", "retire"],
        );
      }),
    ),
);
it.effect(
  "DCA-COR-002 partial joined cancellation and restart retain uncertainty and activation history",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, use } = yield* started;
        const background: Binding = { ...binding, executor: "background" };
        const joined = yield* checkout.join({
          binding: background,
          evidence: evidence("active"),
          expectedRevision: use.revision,
          target,
        });
        const partial = yield* checkout.reconcileEvidence({
          binding: background,
          evidence: { ...evidence("not_invoked"), coverage: "partial" },
          expectedRevision: joined.revision,
        });
        assert.strictEqual(partial.state, "unknown");
        assert.strictEqual(
          (yield* checkout.events(use.operation_id)).at(-1)!.event_kind,
          "unknown",
        );
        assert.strictEqual(
          (yield* checkout
            .complete({
              operationId: use.operation_id,
              expectedRevision: partial.revision,
              now,
              target,
            })
            .pipe(Effect.flip)).reason,
          "unknown",
        );
        const restart = yield* checkout.markRestartUnknown({
          operationId: use.operation_id,
          expectedRevision: partial.revision,
          now,
        });
        assert.strictEqual(
          (yield* checkout
            .reconcileEvidence({
              binding,
              evidence: evidence("not_invoked"),
              expectedRevision: restart.revision,
            })
            .pipe(Effect.flip)).reason,
          "unknown",
        );
        const cancelled = yield* checkout.reconcileEvidence({
          binding: background,
          evidence: evidence("not_invoked"),
          expectedRevision: restart.revision,
        });
        assert.strictEqual(cancelled.state, "unknown");
        const retired = yield* checkout.reconcileEvidence({
          binding,
          evidence: evidence("quiescent"),
          expectedRevision: cancelled.revision,
        });
        const complete = yield* checkout.complete({
          operationId: use.operation_id,
          expectedRevision: retired.revision,
          now,
          target,
        });
        assert.strictEqual(complete.state, "completed");
        assert.strictEqual(complete.started_at, now);
      }),
    ),
);

it.effect("authorization admits reserve, start and join without claiming observed activity", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const authorized: Evidence = { ...evidence("authorized"), coverage: "unknown" };
      const reservation = yield* checkout.reserve({
        admissionId: "admission",
        source,
        now,
        binding,
        evidence: authorized,
      });
      const active = yield* checkout.start({
        binding,
        evidence: authorized,
        expectedRevision: reservation.revision,
        target,
      });
      const joined = yield* checkout.join({
        binding: { ...binding, executor: "background" },
        evidence: authorized,
        expectedRevision: active.revision,
        target,
      });
      assert.equal(reservation.state, "reserved");
      assert.equal(active.state, "started");
      assert.equal(joined.state, "draining");
      const journal = yield* checkout.events(binding.operationId);
      assert.deepEqual(
        journal.map((event) => JSON.parse(event.evidence_json).outcome),
        ["authorized", "authorized", "authorized"],
      );
      assert.deepEqual(
        journal.map((event) => JSON.parse(event.evidence_json).coverage),
        ["unknown", "unknown", "unknown"],
      );
      const before = yield* checkout.listUnsettled(null, 10);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.reconcileEvidence({
              binding,
              evidence: authorized,
              expectedRevision: joined.revision,
            }),
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout.renew({
              binding,
              evidence: authorized,
              expectedRevision: joined.revision,
              target,
              expectedOwnerRevision: 0,
              expiresAt: later,
            }),
          ),
        ),
      );
      assert.deepEqual(yield* checkout.listUnsettled(null, 10), before);
      assert.deepEqual(yield* checkout.events(binding.operationId), journal);
    }),
  ),
);

it.effect("authorization cannot claim physical coverage at reserve, start or join", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      for (const coverage of ["complete", "partial"] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.reserve({
                admissionId: "admission",
                source,
                now,
                binding,
                evidence: { ...evidence("authorized"), coverage },
              }),
            ),
          ),
        );
      }
      assert.deepEqual(yield* checkout.listUnsettled(null, 10), []);
      const reservation = yield* checkout.reserve({
        admissionId: "admission",
        source,
        now,
        binding,
        evidence: { ...evidence("authorized"), coverage: "unknown" },
      });
      for (const coverage of ["complete", "partial"] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.start({
                binding,
                target,
                expectedRevision: reservation.revision,
                evidence: { ...evidence("authorized"), coverage },
              }),
            ),
          ),
        );
      }
      const active = yield* checkout.start({
        binding,
        target,
        expectedRevision: reservation.revision,
        evidence: { ...evidence("authorized"), coverage: "unknown" },
      });
      for (const coverage of ["complete", "partial"] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.join({
                binding: { ...binding, executor: "background" },
                target,
                expectedRevision: active.revision,
                evidence: { ...evidence("authorized"), coverage },
              }),
            ),
          ),
        );
      }
      assert.equal((yield* checkout.events(binding.operationId)).length, 2);
    }),
  ),
);

it.effect("authorization still requires the exact executor owner's reconciliation", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const authorized: Evidence = { ...evidence("authorized"), coverage: "unknown" };
      const denied = CheckoutExecutorAuthority.of({
        assertCurrent: () => Effect.void,
        reconcile: () => Effect.fail(new CheckoutError({ reason: "unavailable" })),
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout
              .reserve({
                admissionId: "admission",
                source,
                now,
                binding,
                evidence: authorized,
              })
              .pipe(Effect.provideService(CheckoutExecutorAuthority, denied)),
          ),
        ),
      );
      assert.deepEqual(yield* checkout.listUnsettled(null, 10), []);
      const reservation = yield* checkout.reserve({
        admissionId: "admission",
        source,
        now,
        binding,
        evidence: authorized,
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            checkout
              .start({
                binding,
                target,
                expectedRevision: reservation.revision,
                evidence: authorized,
              })
              .pipe(Effect.provideService(CheckoutExecutorAuthority, denied)),
          ),
        ),
      );
      assert.equal((yield* checkout.events(binding.operationId)).length, 1);
      const retired = yield* checkout.reconcileEvidence({
        binding,
        expectedRevision: reservation.revision,
        evidence: evidence("not_invoked"),
      });
      assert.equal(retired.state, "reserved");
    }),
  ),
);

function adapterFixture(
  native: Effect.Effect<void, string> = Effect.void,
  executorKind: Binding["executor"] = "outbox",
) {
  const ownedBinding: Binding = {
    ...binding,
    executor: executorKind,
    provider: {
      instanceId: "codex",
      sessionId: "session",
      threadId: "thread",
      driver: "codex",
      generation: "generation",
      evidenceRevision: 7,
    },
  };
  const identity: RuntimeOperationIdentity = {
    issuerId: ownedBinding.issuerId,
    operationId: ownedBinding.operationId,
    admissionId: ownedBinding.admissionId,
    participantId: associationId(ownedBinding),
    binding: {
      threadId: ThreadId.make("thread"),
      providerThreadId: ProviderThreadId.make("provider-thread"),
      providerSessionId: ProviderSessionId.make("session"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      nativeThreadId: "native",
      runtimeGeneration: "generation",
    },
    evidenceRevision: 7,
    evidenceReference: "owner-observation",
    writerReferences: ["synthetic-writer"],
  };
  let current = true;
  let expireOnLock = false;
  let calls = 0;
  let observation: ScopeObservation | (() => ScopeObservation) = {
    state: "active",
    coverage: "complete",
  };
  const availability = makeRuntimeOperationPermit({
    mode: "exclusive-operation",
    identity,
    isCurrent: Effect.sync(() => current),
    withCurrent: <V, F, R>(effect: Effect.Effect<V, F, R>) =>
      Effect.suspend((): Effect.Effect<V, F | RuntimePermitError, R> => {
        if (expireOnLock) current = false;
        return current ? effect : Effect.fail(new RuntimePermitError({ reason: "not_current" }));
      }),
    invoke: Effect.sync(() => {
      calls += 1;
    }).pipe(Effect.andThen(native)),
    observe: Effect.sync(() => (typeof observation === "function" ? observation() : observation)),
  });
  return {
    ownedBinding,
    identity,
    availability,
    adapter: makeCheckoutPermitAdapter(ownedBinding, identity, availability),
    calls: () => calls,
    expire: () => {
      current = false;
    },
    replaceAtLock: () => {
      expireOnLock = true;
    },
    observe: (next: ScopeObservation | (() => ScopeObservation)) => {
      observation = next;
    },
  };
}

it.effect("permit adapter journals authorization before the actual call and rejects replay", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const f = adapterFixture();
      const adapter = yield* f.adapter;
      assert.equal(adapter.status, "available");
      if (adapter.status !== "available") return;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.adapter)));
      const reservedUse = yield* adapter.reserve(now);
      assert.equal(f.calls(), 0);
      assert.equal((yield* adapter.observe(now)).outcome, "unknown");
      yield* adapter.start({ target, now, expectedRevision: reservedUse.revision });
      assert.equal(f.calls(), 1);
      const events = yield* checkout.events(binding.operationId);
      assert.deepEqual(
        events.map((e) => e.event_kind),
        ["bind", "activate"],
      );
      assert.deepEqual(
        events.map((e) => JSON.parse(e.evidence_json).outcome),
        ["authorized", "authorized"],
      );
      assert.equal((yield* adapter.observe(now)).outcome, "active");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            adapter.start({ target, now, expectedRevision: reservedUse.revision }),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(adapter.reserve(now))));
      assert.equal(f.calls(), 1);
      const use = (yield* checkout.listUnsettled(null, 10))[0]!;
      yield* adapter.renew({
        target,
        now,
        expectedRevision: use.revision,
        expectedOwnerRevision: 0,
        expiresAt: later,
      });
    }),
  ),
);

it.effect(
  "permit adapter revocation before start proves no effect without requiring currentness",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql } = yield* seed;
        yield* checkout.accept(acceptedInput);
        const f = adapterFixture();
        const adapter = yield* f.adapter;
        if (adapter.status !== "available") return assert.fail("available fixture");
        const use = yield* adapter.reserve(now);
        yield* adapter.revoke;
        f.expire();
        assert.equal((yield* adapter.observe(now)).outcome, "not_invoked");
        const retired = yield* adapter.reconcile(use.revision, now);
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL WHERE effect_id='effect'`;
        const done = yield* checkout.complete({
          operationId: use.operation_id,
          expectedRevision: retired.revision,
          now,
          target,
        });
        assert.equal(done.state, "no_effect");
        assert.equal(f.calls(), 0);
      }),
    ),
);

it.effect("permit adapter retains unknown after commit failure or native response loss", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const f = adapterFixture(Effect.fail("response lost"));
      const adapter = yield* f.adapter;
      if (adapter.status !== "available") return assert.fail("available fixture");
      const use = yield* adapter.reserve(now);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(adapter.start({ target, now, expectedRevision: use.revision })),
        ),
      );
      assert.equal(f.calls(), 1);
      assert.equal((yield* adapter.observe(now)).outcome, "unknown");
      const currentUse = (yield* checkout.listUnsettled(null, 10))[0]!;
      assert.equal(currentUse.state, "started");
      const unknown = yield* adapter.reconcile(currentUse.revision, now);
      assert.equal(unknown.state, "unknown");
    }),
  ),
);

it.effect("permit adapter cannot infer not-invoked from a failed durable start attempt", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const f = adapterFixture();
      const adapter = yield* f.adapter;
      if (adapter.status !== "available") return assert.fail("available fixture");
      const use = yield* adapter.reserve(now);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(adapter.start({ target, now, expectedRevision: use.revision + 1 })),
        ),
      );
      assert.equal(f.calls(), 0);
      assert.equal((yield* adapter.observe(now)).outcome, "unknown");
      assert.equal((yield* adapter.reconcile(use.revision, now)).state, "unknown");
    }),
  ),
);

it.effect("permit adapter retires a complete old scope after replacement without invoking", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const f = adapterFixture();
      const adapter = yield* f.adapter;
      if (adapter.status !== "available") return assert.fail("available fixture");
      const use = yield* adapter.reserve(now);
      f.replaceAtLock();
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(adapter.start({ target, now, expectedRevision: use.revision })),
        ),
      );
      assert.equal(f.calls(), 0);
      assert.equal((yield* adapter.observe(now)).outcome, "unknown");
      f.observe({ state: "quiescent", coverage: "complete" });
      const currentUse = (yield* checkout.listUnsettled(null, 10))[0]!;
      assert.equal((yield* adapter.reconcile(currentUse.revision, now)).state, "draining");
    }),
  ),
);

it.effect("permit adapter preserves unavailable and rejects full identity mismatches", () =>
  Effect.gen(function* () {
    const f = adapterFixture();
    for (const mode of ["warm_runtime", "unsupported", "restart"] as const) {
      assert.deepEqual(
        yield* makeCheckoutPermitAdapter(
          f.ownedBinding,
          f.identity,
          makeRuntimeOperationPermit({ mode }),
        ),
        { status: "unavailable", reason: mode },
      );
    }
    for (const altered of [
      { ...f.identity, issuerId: "other" },
      { ...f.identity, participantId: "other" },
      { ...f.identity, evidenceRevision: 8 },
      { ...f.identity, binding: { ...f.identity.binding, nativeThreadId: "other" } },
      {
        ...f.identity,
        binding: { ...f.identity.binding, providerThreadId: ProviderThreadId.make("other") },
      },
      { ...f.identity, writerReferences: ["other"] },
    ])
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(makeCheckoutPermitAdapter(f.ownedBinding, altered, f.availability)),
        ),
      );
    assert.equal(f.calls(), 0);
  }),
);

it.effect(
  "permit adapter keeps joined partial writers unsettled until each owner proves retirement",
  () =>
    provide(
      Effect.gen(function* () {
        const { checkout, sql } = yield* seed;
        yield* checkout.accept(acceptedInput);
        const root = adapterFixture();
        const child = adapterFixture(Effect.void, "background");
        const a = yield* root.adapter;
        const b = yield* child.adapter;
        if (a.status !== "available" || b.status !== "available")
          return assert.fail("available fixtures");
        const reservedUse = yield* a.reserve(now);
        yield* a.start({ target, now, expectedRevision: reservedUse.revision });
        const startedUse = (yield* checkout.listUnsettled(null, 10))[0]!;
        yield* b.join({ target, now, expectedRevision: startedUse.revision });
        assert.equal(child.calls(), 1);
        assert.deepEqual(
          (yield* checkout.events(binding.operationId)).map((e) => e.event_kind),
          ["bind", "activate", "join"],
        );
        const joinedUse = (yield* checkout.listUnsettled(null, 10))[0]!;
        child.observe({ state: "quiescent", coverage: "partial" });
        assert.equal((yield* b.observe(now)).outcome, "unknown");
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              b.renew({
                target,
                now,
                expectedRevision: joinedUse.revision,
                expectedOwnerRevision: 0,
                expiresAt: later,
              }),
            ),
          ),
        );
        root.observe({ state: "quiescent", coverage: "complete" });
        const rootDone = yield* a.reconcile(joinedUse.revision, now);
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL WHERE effect_id='effect'`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              checkout.complete({
                operationId: binding.operationId,
                expectedRevision: rootDone.revision,
                now,
                target,
              }),
            ),
          ),
        );
        child.observe({ state: "quiescent", coverage: "complete" });
        child.expire();
        const childDone = yield* b.reconcile(rootDone.revision, later);
        assert.equal(
          (yield* checkout.complete({
            operationId: binding.operationId,
            expectedRevision: childDone.revision,
            now: later,
            target,
          })).state,
          "completed",
        );
      }),
    ),
);

it.effect("permit adapter revokes an interrupted invocation without claiming no effect", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const entered = yield* Deferred.make<void>();
      const f = adapterFixture(
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      );
      const adapter = yield* f.adapter;
      if (adapter.status !== "available" || f.availability.status !== "available")
        return assert.fail("available fixture");
      const use = yield* adapter.reserve(now);
      const running = yield* adapter
        .start({ target, now, expectedRevision: use.revision })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(running);
      assert.equal(f.calls(), 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.availability.permit.assertCurrent)));
      assert.equal((yield* adapter.observe(now)).outcome, "unknown");
      const activeUse = (yield* checkout.listUnsettled(null, 10))[0]!;
      assert.equal((yield* adapter.reconcile(activeUse.revision, now)).state, "unknown");
    }),
  ),
);

it.effect("permit adapter rejects retirement when a fresh owner observation changes", () =>
  provide(
    Effect.gen(function* () {
      const { checkout } = yield* seed;
      yield* checkout.accept(acceptedInput);
      const f = adapterFixture();
      const adapter = yield* f.adapter;
      if (adapter.status !== "available") return assert.fail("available fixture");
      const use = yield* adapter.reserve(now);
      yield* adapter.start({ target, now, expectedRevision: use.revision });
      const activeUse = (yield* checkout.listUnsettled(null, 10))[0]!;
      let reads = 0;
      f.observe(() => ({ state: ++reads === 1 ? "quiescent" : "active", coverage: "complete" }));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(adapter.reconcile(activeUse.revision, now))));
      assert.equal(reads, 2);
      assert.equal((yield* checkout.events(binding.operationId)).length, 2);
      assert.equal((yield* checkout.listUnsettled(null, 10))[0]!.revision, activeUse.revision);
    }),
  ),
);
