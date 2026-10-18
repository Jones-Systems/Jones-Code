import { assert, it } from "@effect/vitest";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  ProjectId, ThreadId, NativeCreationHistoricalBinding,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Native from "../nativeCreation/NativeCreationRepository.ts";
import * as NativeSqlite from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import * as Authority from "../nativeCreation/NativeCreationAuthority.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import {
  NativePreparationBinding, nativeCreationCanonicalJson, nativeCreationSha256,
  nativePreparationCommand, validateNativeCreationPreparation,
} from "../nativeCreation/NativeCreationPreparation.ts";
import {
  canonical, CheckoutError, operationId,
  type Binding, type Evidence, type Source, type Target,
} from "./CheckoutProtocol.ts";
import {
  CheckoutAcceptanceAuthority, CheckoutExecutorAuthority, CheckoutOwnershipAuthority,
  OrdinaryCheckout, type Accept,
} from "./OrdinaryCheckout.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";
import * as Targets from "./CheckoutTarget.ts";
import * as Integration from "./CheckoutConflictIntegration.ts";
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
  executor: "transition",
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

const observation = Targets.CheckoutTargetObservation.of({
  directory: (value) => Effect.succeed({
    realPath: value === "/alias" ? "/repo" : value,
    identity: target.directoryIdentity,
  }),
  git: (cwd) => Effect.succeed(cwd === "/repo/subproject" ? {
    root: "/repo", commonDirectory: "/repo/.git", gitDirectory: "/repo/.git",
    commonIdentity: target.directoryIdentity, directoryIdentity: target.directoryIdentity,
    branch: "refs/heads/work", registeredWorktrees: ["/repo"],
  } : null),
});
const files = Physical.NativeWorkspaceFileObservation.of({
  inspect: (value) => Effect.succeed(value === "/alias/new-worktree" ? null : {
    realPath: value === "/alias" ? "/repo" : value,
    birth: "synthetic-birth", kind: "directory" as const,
  }),
  readFile: () => Effect.succeed(""),
});
const testLayer = (filename = ":memory:") => {
  const database = NodeSqliteClient.layer({ filename, readerWorker: false, checkpointWorker: false });
  const owners = Layer.mergeAll(Repository.layer, NativeSqlite.layer, Receipts.layer).pipe(
    Layer.provideMerge(database), Layer.provideMerge(NodePath.layer),
  );
  return Integration.layer.pipe(
    Layer.provideMerge(owners),
    Layer.provide(Layer.succeed(Targets.CheckoutTargetObservation, observation)),
    Layer.provide(Layer.succeed(Physical.NativeWorkspaceFileObservation, files)),
  );
};
const layer = testLayer();
const provide = <A, E>(effect: Effect.Effect<A, E,
  Integration.CheckoutConflictIntegration | OrdinaryCheckout | SqlClient.SqlClient |
  CheckoutAcceptanceAuthority | CheckoutExecutorAuthority | CheckoutOwnershipAuthority
>) => effect.pipe(
  Effect.provideService(CheckoutAcceptanceAuthority, acceptance),
  Effect.provideService(CheckoutExecutorAuthority, executor),
  Effect.provideService(CheckoutOwnershipAuthority, { authorize: () => Effect.void }),
  Effect.provide(layer),
);
const accept = (input: Accept = acceptedInput) => Effect.gen(function* () {
  const integration = yield* Integration.CheckoutConflictIntegration;
  return yield* (yield* integration.prepareOrdinary([input]));
});
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

const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const nativeFixture = Effect.fnUntraced(function* (
  operationId = "fixture-operation",
  text = "Synthetic prompt",
  path = "/repo",
  project = "/fixture/project",
) {
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: project,
    account_ref: "fixture-account",
    runtime_mode: "full-access" as const,
    interaction_mode: "default" as const,
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "fixture-model" },
  });
  const command = nativePreparationCommand(
    operationId,
    binding,
    text,
    "Synthetic thread",
    timestamp,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: operationId,
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
      }),
    ),
  );
  const historical = decodeFixtureHistory({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "qualified-fixture-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const input: Native.NativeCreationClaimInput = {
    preparation,
    resources: {
      projectCwd: binding.project_cwd,
      branch: historical.requestedBranch,
      worktreePath: path,
    },
    claimId: `claim-${operationId}`,
    claimedBootId: "fixture-boot",
    claimedAt: timestamp,
    actorSessionId: "fixture-session",
    grantId: "fixture-grant",
    grantRevision: 1,
  };
  return { input, historical, preparation, authorize: Effect.succeed(historical) };
});

for (const [label, worktree, project] of [
  ["same checkout", "/repo", "/native-project"],
  ["nested target", "/repo/child", "/native-project"],
  ["ancestor target", "/", "/native-project"],
  ["project root", "/native-worktree", "/repo"],
  ["project inside Git checkout", "/native-worktree", "/repo/subproject"],
  ["prospective path under an aliased parent", "/alias/new-worktree", "/native-project"],
  ["physical alias", "/alias", "/native-project"],
] as const) {
  it.effect(`ordinary owner excludes native ${label} without acquiring another claim`, () => provide(
    Effect.gen(function* () {
      const { sql } = yield* seed;
      yield* accept();
      const integration = yield* Integration.CheckoutConflictIntegration;
      const value = yield* nativeFixture("native-competitor", "Synthetic", worktree, project);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(integration.native.claim(value.input, value.authorize))));
      assert.deepEqual(yield* sql`SELECT claim_id FROM native_creation_intents`, []);
      assert.strictEqual((yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`).length, 1);
    }),
  ));
}

for (const [label, worktree, project] of [
  ["worktree", "/repo", "/native-project"],
  ["nested worktree", "/repo/child", "/native-project"],
  ["project root with a different project ID", "/native-worktree", "/repo"],
  ["project inside Git checkout", "/native-worktree", "/repo/subproject"],
] as const) {
  it.effect(`native ${label} reciprocally excludes ordinary acceptance`, () => provide(
    Effect.gen(function* () {
      const { sql } = yield* seed;
      const integration = yield* Integration.CheckoutConflictIntegration;
      const value = yield* nativeFixture("native-first", "Synthetic", worktree, project);
      yield* integration.native.claim(value.input, value.authorize);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(accept())));
      assert.deepEqual(yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`, []);
      assert.deepEqual(yield* sql`SELECT admission_id FROM jones_ordinary_checkout_admissions`, []);
    }),
  ));
}

it.effect("retained physical aliases are excluded without adopting or rewriting native history", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    yield* sql`INSERT INTO native_creation_intents(claim_id,operation_id,preparation_id,command_id,thread_id,message_id,project_cwd,branch,worktree_path,canonical_preparation,intent_json)
      VALUES('foreign','foreign-op','foreign-prep','foreign-cmd','foreign-thread','foreign-message','/native-project','native','/alias','{}','{}')`;
    const before = yield* sql`SELECT * FROM native_creation_intents`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(accept())));
    assert.deepEqual(yield* sql`SELECT * FROM native_creation_intents`, before);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, []);
  }),
));

it.effect("a claim arriving after physical preflight invalidates the ordinary commit snapshot", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const commit = yield* integration.prepareOrdinary([acceptedInput]);
    const value = yield* nativeFixture();
    yield* integration.native.claim(value.input, value.authorize);
    const denied = yield* commit.pipe(Effect.flip);
    assert.strictEqual(denied.reason, "stale");
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, []);
  }),
));

it.effect("concurrent ordinary and native admission commits at most one reciprocal owner", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture();
    const results = yield* Effect.all([
      Effect.exit(accept()), Effect.exit(integration.native.claim(value.input, value.authorize)),
    ], { concurrency: "unbounded" });
    assert.strictEqual(results.filter(Exit.isSuccess).length, 1);
    const ordinary = yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`;
    const native = yield* sql`SELECT claim_id FROM native_creation_intents`;
    assert.strictEqual(ordinary.length + native.length, 1);
  }),
));

it.effect("disjoint native and ordinary checkouts retain independent ownership", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture("independent", "Synthetic", "/native-worktree", "/native-project");
    yield* integration.native.claim(value.input, value.authorize);
    yield* accept();
    assert.strictEqual((yield* sql`SELECT claim_id FROM native_creation_intents`).length, 1);
    assert.strictEqual((yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`).length, 1);
  }),
));

it.effect("prepared ordinary acceptance rolls back with its enclosing receipt transaction", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const commit = yield* integration.prepareOrdinary([acceptedInput]);
    const receipts = yield* sql`SELECT * FROM orchestration_command_receipts`;
    const result = yield* Effect.exit(sql.withTransaction(Effect.gen(function* () {
      yield* commit;
      yield* sql`UPDATE orchestration_command_receipts SET error='injected' WHERE command_id='command'`;
      return yield* new CheckoutError({ reason: "unavailable" });
    })));
    assert.isTrue(Exit.isFailure(result));
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, receipts);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, []);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_admissions`, []);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_effect_links`, []);
  }),
));

it.effect("native authority refusal is preserved and cannot leave a sparse claim", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture();
    const denial = new Authority.NativeCreationAuthorityError({ code: "stale_grant", message: "Synthetic denial" });
    assert.strictEqual(yield* integration.native.claim(value.input, Effect.fail(denial)).pipe(Effect.flip), denial);
    assert.deepEqual(yield* sql`SELECT * FROM native_creation_intents`, []);
  }),
));

it.effect("native workspace admission still requires receiving migration 100/101 provenance", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture("workspace", "Synthetic", "/native-worktree", "/native-project");
    yield* integration.native.claim(value.input, value.authorize);
    yield* sql`DELETE FROM jones_sql_migrations WHERE migration_id=101`;
    const result = yield* integration.native.admitWorkspace!(value.input.claimId, {
      bootId: value.input.claimedBootId,
      projectId: value.historical.projectId,
      projectCwd: value.input.resources.projectCwd,
      projectBirth: "synthetic-project-birth",
      gitCommonDirectory: "/native-project/.git",
      physicalGitIdentity: "synthetic-git",
      worktreePath: value.input.resources.worktreePath,
      parentBirth: "synthetic-parent-birth",
      producerId: "synthetic-producer",
      baseRef: value.historical.baseBranch,
      setupDefinition: null,
      configuredSubmodulesDefinition: "[]",
      baseConfigurationDefinition: "{}",
    }).pipe(Effect.flip);
    assert.strictEqual(result.code, "unresolved_claim");
    assert.deepEqual(yield* sql`SELECT * FROM jones_native_workspace_admissions`, []);
  }),
));

const deletionInput = {
  threadId: ThreadId.make("deleted-thread"),
  target: { projectId: ProjectId.make("deleted-project"), projectRoot: "/deleted-project", path: "/repo", branch: "work", force: false },
  rules: {
    worktreeKeepWhen: "uncommitted-changes" as const, worktreeAfterDays: null,
    worktreeSettledAfterDays: null, worktreeOnDelete: true, worktreeOnMerge: false,
    worktreeUnchanged: false,
  },
  currentLive: Effect.succeed(true),
};
const deletionFixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const payload = {
    id: deletionInput.threadId, projectId: deletionInput.target.projectId,
    createdAt: now, deletedAt: now, worktreePath: "/repo", branch: "work",
  };
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES('deleted-project','synthetic','/deleted-project','[]',${now},${now})`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,deleted_at,payload_json)
    VALUES('deleted-thread','deleted-project','synthetic','codex','local','default',${now},${now},${now},${canonical(payload)})`;
  for (const [sequence, id, type, body] of [
    [3, "deleted-birth", "thread.created", { ...payload, deletedAt: null }],
    [4, "deleted-event", "thread.deleted", payload],
  ] as const) {
    yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(${sequence},${id},'thread','deleted-thread',${sequence},${type},${now},${sequence === 4 ? 'delete-command' : null},'client',${canonical(body)},'{}',2)`;
  }
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
    VALUES('delete-command','thread','deleted-thread','thread.delete',${now},4,'accepted',NULL)`;
  return { ...deletionInput, currentRules: Effect.succeed(deletionInput.rules) };
});

it.effect("ordinary ownership prevents deletion admission before any execution permit exists", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const input = yield* deletionFixture;
    yield* accept();
    const integration = yield* Integration.CheckoutConflictIntegration;
    const denial = yield* integration.deletion.start(input).pipe(Effect.flip);
    assert.strictEqual(denial.reason, "ordinary_checkout_original_owner_handoff_required");
    assert.deepEqual(yield* sql`SELECT * FROM jones_deletion_worktree_admissions`, []);
  }),
));

it.effect("deletion's retained start reciprocally excludes ordinary acquisition and preserves one-shot revalidation", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const input = yield* deletionFixture;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const first = yield* integration.deletion.start(input);
    assert.strictEqual(first.status, "start_now");
    assert.isTrue(Exit.isFailure(yield* Effect.exit(accept())));
    yield* integration.deletion.revalidate(first.start, first.ordinal, input.currentRules, input.currentLive);
    const repeat = yield* integration.deletion.revalidate(first.start, first.ordinal, input.currentRules, input.currentLive).pipe(Effect.flip);
    assert.strictEqual(repeat.reason, "original_start_observation_only");
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, []);
  }),
));

it.effect("deletion admission and ordinary acceptance racing on the same target retain one owner", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const input = yield* deletionFixture;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const results = yield* Effect.all([
      Effect.exit(accept()), Effect.exit(integration.deletion.start(input)),
    ], { concurrency: "unbounded" });
    assert.strictEqual(results.filter(Exit.isSuccess).length, 1);
    const ordinary = yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`;
    const deletion = yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`;
    assert.strictEqual(ordinary.length + deletion.length, 1);
  }),
));

for (const state of ["active", "unknown"] as const) {
  it.effect(`release and branch transition refuse ${state} participants without dropping reciprocal exclusion`, () => provide(
    Effect.gen(function* () {
      const { checkout, use: startedUse } = yield* started;
      const use = state === "unknown" ? yield* checkout.markRestartUnknown({
        operationId: startedUse.operation_id, expectedRevision: startedUse.revision, now,
      }) : startedUse;
      const before = yield* checkout.readOwner("/repo");
      const input = {
        operationId: use.operation_id, expectedRevision: use.revision, target,
        binding, evidence: evidence("quiescent"), expectedOwner: { epoch: 0, revision: 0 },
      };
      const release = yield* checkout.release(input).pipe(Effect.flip);
      const transition = yield* checkout.transitionBranch({ ...input, after: {
        ...target, branch: "refs/heads/next", threadBranch: "next", threadRevision: later,
      } }).pipe(Effect.flip);
      assert.strictEqual(release.reason, state === "active" ? "unknown" : "stale");
      assert.strictEqual(transition.reason, state === "active" ? "unknown" : "stale");
      assert.deepEqual(yield* checkout.readOwner("/repo"), before);
      const integration = yield* Integration.CheckoutConflictIntegration;
      const value = yield* nativeFixture();
      assert.isTrue(Exit.isFailure(yield* Effect.exit(integration.native.claim(value.input, value.authorize))));
    }),
  ));
}

it.effect("settled use keeps its ordinary owner until explicit release allows native acquisition", () => provide(
  Effect.gen(function* () {
    const { checkout, use } = yield* started;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture("after-release");
    const retired = yield* checkout.reconcileEvidence({ binding, evidence: evidence("quiescent"), expectedRevision: use.revision });
    const complete = yield* checkout.complete({ operationId: use.operation_id, expectedRevision: retired.revision, now, target });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(integration.native.claim(value.input, value.authorize))));
    yield* checkout.release({
      operationId: use.operation_id, expectedRevision: complete.revision, target, binding,
      evidence: evidence("quiescent"), expectedOwner: { epoch: 0, revision: 0 },
    });
    assert.strictEqual((yield* integration.native.claim(value.input, value.authorize)).status, "claimed");
  }),
));

it.effect("exact ordinary replay bypasses changed physical preflight and adds no writes", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const first = yield* accept();
    const before = yield* sql`SELECT * FROM jones_ordinary_checkout_owners`;
    // Retained foreign history cannot turn an existing immutable receipt into new admission.
    yield* sql`INSERT INTO native_creation_intents(claim_id,operation_id,preparation_id,command_id,thread_id,message_id,project_cwd,branch,worktree_path,canonical_preparation,intent_json)
      VALUES('foreign','foreign-op','foreign-prep','foreign-cmd','foreign-thread','foreign-message','/native-project','native','/repo','{}','{}')`;
    assert.deepEqual(yield* accept(), first);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, before);
    assert.strictEqual((yield* sql`SELECT * FROM jones_ordinary_checkout_admissions`).length, 1);
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_uses`, []);
  }),
));

it.effect("deletion policy refusal leaves no claim and does not block ordinary ownership", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const input = yield* deletionFixture;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const denied = yield* integration.deletion.start({ ...input, currentRules: Effect.succeed({ ...input.rules, worktreeOnDelete: false }) }).pipe(Effect.flip);
    assert.strictEqual(denied.reason, "original_worktree_policy_changed");
    assert.deepEqual(yield* sql`SELECT * FROM jones_deletion_worktree_admissions`, []);
    yield* accept();
  }),
));

it.effect("a rolled-back native claim leaves ordinary admission available", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const value = yield* nativeFixture("rollback-native");
    const failed = yield* Effect.exit(sql.withTransaction(Effect.gen(function* () {
      yield* integration.native.claim(value.input, value.authorize);
      return yield* new CheckoutError({ reason: "unavailable" });
    })));
    assert.isTrue(Exit.isFailure(failed));
    assert.deepEqual(yield* sql`SELECT * FROM native_creation_intents`, []);
    yield* accept();
  }),
));

it.effect("a rolled-back deletion start retains no durable claim or executable readback", () => provide(
  Effect.gen(function* () {
    const { sql } = yield* seed;
    const input = yield* deletionFixture;
    const integration = yield* Integration.CheckoutConflictIntegration;
    const failed = yield* Effect.exit(sql.withTransaction(Effect.gen(function* () {
      yield* integration.deletion.start(input);
      return yield* new CheckoutError({ reason: "unavailable" });
    })));
    assert.isTrue(Exit.isFailure(failed));
    assert.deepEqual(yield* sql`SELECT * FROM jones_deletion_worktree_admissions`, []);
    assert.isNull(yield* integration.deletion.readTarget(input.threadId, input.target));
    yield* accept();
  }),
));

const scratch = process.env.JONES_CHECKOUT_TEST_SCRATCH;
for (const competitor of ["native", "deletion"] as const) {
  it.effect.skipIf(!scratch)(`two independent SQLite connections exclude ordinary versus ${competitor}`, () =>
    Effect.scoped(Effect.gen(function* () {
      // The verifier owns the private 0700 cache parent. This scope owns only its
      // unique child; both SQL connections finalize before that child is removed.
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      if (!scratch || !path.isAbsolute(scratch) || scratch === "/tmp" || scratch.startsWith("/tmp/"))
        return yield* new CheckoutError({ reason: "identity" });
      const directory = yield* fs.makeTempDirectoryScoped({ directory: scratch, prefix: "checkout-conflicts-" });
      yield* fs.chmod(directory, 0o700);
      const filename = path.join(directory, "fixture.sqlite");
      const left = yield* Layer.build(testLayer(filename));
      yield* seed.pipe(Effect.provide(left));
      const deletion = competitor === "deletion" ? yield* deletionFixture.pipe(Effect.provide(left)) : null;
      const value = yield* nativeFixture("two-connections");
      const right = yield* Layer.build(testLayer(filename));
      const competing = Effect.gen(function* () {
        const integration = yield* Integration.CheckoutConflictIntegration;
        if (deletion !== null) return yield* integration.deletion.start(deletion);
        return yield* integration.native.claim(value.input, value.authorize);
      });
      const results = yield* Effect.all([
        accept().pipe(Effect.provide(left), Effect.exit),
        competing.pipe(Effect.provide(right), Effect.exit),
      ], { concurrency: 2 });
      assert.strictEqual(results.filter(Exit.isSuccess).length, 1);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const ordinary = yield* sql`SELECT checkout_key FROM jones_ordinary_checkout_owners`;
        const native = yield* sql`SELECT claim_id FROM native_creation_intents`;
        const removal = yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`;
        assert.strictEqual(ordinary.length + native.length + removal.length, 1);
      }).pipe(Effect.provide(left));
    })).pipe(
      Effect.provideService(CheckoutAcceptanceAuthority, acceptance),
      Effect.provide(NodeServices.layer),
    ),
  );
}
