import { assert, it } from "@effect/vitest";
import {
  NativeCreationHistoricalBinding,
  OrchestrationV2Command, ProviderThreadId, ProviderSessionId, ProviderInstanceId, RunAttemptId,
  ThreadId,
  AuthSessionId,
  EventId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { NativeCreationAuthorityError } from "../../orchestration-v2/NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "../../orchestration-v2/NativeCreationPreparation.ts";
import { runMigrations } from "../Migrations.ts";
import migration from "../Migrations/003_JonesNativeCreationIntents.ts";
import identityMigration from "../Migrations/004_JonesNativeCreationCommandIdentities.ts";
import receiptMigration from "../Migrations/002_OrchestrationCommandReceipts.ts";
import {
  NativeCreationRepository, NativeCreationCommand,
  type NativeCreationClaimInput,
} from "../Services/NativeCreationRepository.ts";
import { layer, make } from "./NativeCreationRepository.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const database = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* migration;
    yield* receiptMigration;
    yield* identityMigration;
  }),
).pipe(Layer.provideMerge(memory));
const repositoryLayer = layer.pipe(Layer.provideMerge(database));
const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureCommand = Schema.decodeUnknownSync(NativeCreationCommand);
const enrolledSessionId = AuthSessionId.make("fixture-enrolled-session");
const fixture = Effect.fnUntraced(function* (
  operationId = "fixture-operation",
  text = "Synthetic prompt",
  path = "/fixture/worktree",
) {
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: "/fixture/project",
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
  const input: NativeCreationClaimInput = {
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

it.effect(
  "commits one invocation claim under concurrent identical submissions and never replaces it",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      const results = yield* Effect.all(
        Array.from({ length: 8 }, (_, index) =>
          repository.claim({ ...value.input, claimId: `request-${index}` }, value.authorize),
        ),
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter((result) => result.status === "claimed").length, 1);
      assert.strictEqual(results.filter((result) => result.status === "duplicate").length, 7);
      assert.strictEqual(new Set(results.map((result) => result.history.intent.claimId)).size, 1);
      assert.isNull(results[0]!.history.normalizedCommandDigest);
      const old = results[0]!.history.intent;
      const duplicate = yield* repository.claim(
        {
          ...value.input,
          claimedBootId: "later-boot",
          claimedAt: "2099-01-01T00:00:00Z",
          grantRevision: 2,
        },
        value.authorize,
      );
      assert.deepEqual(duplicate.history.intent, old);
      assert.strictEqual(duplicate.status, "duplicate");
      const observed = yield* repository.readHistory(value.preparation.command.commandId);
      assert.isTrue(Option.isSome(observed));
      assert.deepEqual(Option.getOrThrow(observed).intent, old);
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("reads exact permanent enrollment membership and rejects malformed present markers", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    assert.isFalse(yield* repository.hasAutomationEnrollment(enrolledSessionId));
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES (${enrolledSessionId}, ${timestamp})`;
    assert.isTrue(yield* repository.hasAutomationEnrollment(enrolledSessionId));
    assert.isFalse(
      yield* repository.hasAutomationEnrollment(AuthSessionId.make("other-native-session")),
    );
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES ('malformed-native-session', 'not-a-timestamp')`;
    assert.strictEqual(
      (yield* repository
        .hasAutomationEnrollment(AuthSessionId.make("malformed-native-session"))
        .pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("missing native membership table is unknown rather than an absent marker", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    assert.strictEqual(
      (yield* repository.hasAutomationEnrollment(enrolledSessionId).pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(layer.pipe(Layer.provide(memory)))),
);

it.effect(
  "rejects changed immutable intent, competing path and duplicate identities without partial claims",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const changed = yield* fixture("fixture-operation", "Changed prompt");
      const otherPath = yield* fixture("other-operation");
      const otherClaimId = yield* fixture(
        "third-operation",
        "Synthetic prompt",
        "/fixture/other-worktree",
      );
      const wrongBranch = {
        ...value.input,
        claimId: "other-claim",
        resources: { ...value.input.resources, branch: "other-branch" },
      };
      for (const [input, authorize] of [
        [changed.input, changed.authorize],
        [otherPath.input, otherPath.authorize],
        [{ ...otherClaimId.input, claimId: value.input.claimId }, otherClaimId.authorize],
        [wrongBranch, value.authorize],
      ] as const) {
        assert.strictEqual(
          (yield* repository.claim(input, authorize).pipe(Effect.flip)).code,
          "conflict",
        );
      }
      assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_intents`, [
        { count: 1 },
      ]);
      assert.isTrue(Option.isNone(yield* repository.readHistory("missing-command")));
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("authority denial creates no claim or started effect", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    const denied = Effect.fail(
      new NativeCreationAuthorityError({ code: "stale_grant", message: "Synthetic revoked grant" }),
    );
    assert.strictEqual(
      (yield* repository.claim(value.input, denied).pipe(Effect.flip)).code,
      "stale_grant",
    );
    for (const changed of [
      { backendInstance: "other-backend" },
      { environmentId: "other-environment" },
      { accountRef: "other-account" },
      { runSetupScript: true },
      {
        providerModelSelection: {
          ...value.historical.providerModelSelection,
          model: "other-model",
        },
      },
    ]) {
      assert.strictEqual(
        (yield* repository
          .claim(value.input, Effect.succeed({ ...value.historical, ...changed }))
          .pipe(Effect.flip)).code,
        "conflict",
      );
    }
    assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_intents`, [
      { count: 0 },
    ]);
    yield* repository.claim(value.input, value.authorize);
    const fact = {
      kind: "fetch" as const,
      phase: "started" as const,
      effectId: "fixture-fetch",
      timestamp,
      projectCwd: value.input.resources.projectCwd,
      baseRef: "main",
    };
    assert.strictEqual(
      (yield* repository.startEffect(value.input.claimId, fact, denied).pipe(Effect.flip)).code,
      "stale_grant",
    );
    assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_effect_facts`, [
      { count: 0 },
    ]);
    const persisted = yield* repository.startEffect(value.input.claimId, fact, value.authorize);
    const history = Option.getOrThrow(
      yield* repository.readHistory(value.preparation.command.commandId),
    );
    assert.deepEqual(history.effects, [persisted]);
    assert.strictEqual(persisted.ordinal, 0);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("keeps original digest separate and reserves immutable native commands", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const command = decodeFixtureCommand({
      ...value.preparation.command,
      modelSelection: value.preparation.binding.provider_model_selection,
    });
    yield* repository.reserveCommandIdentities(value.input.claimId, [command.commandId]);
    yield* repository.recordNormalizedCommand(value.input.claimId, command);
    yield* repository.recordNormalizedCommand(value.input.claimId, command);
    const history = Option.getOrThrow(
      yield* repository.readHistory(value.preparation.command.commandId),
    );
    assert.strictEqual(history.intent.commandDigest, value.preparation.commandDigest);
    assert.strictEqual(
      history.normalizedCommandDigest,
      nativeCreationSha256(nativeCreationCanonicalJson(command)),
    );
    assert.notStrictEqual(history.normalizedCommandDigest, history.intent.commandDigest);
    const reserved = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
    assert.strictEqual(reserved.claimId, value.input.claimId);
    assert.strictEqual(reserved.commandDigest, history.normalizedCommandDigest);
    if (command.type !== "thread.turn.start")
      return yield* Effect.die("Fixture command type changed");
    assert.strictEqual(
      (yield* repository
        .recordNormalizedCommand(value.input.claimId, {
          ...command,
          message: { ...command.message, text: "Changed" },
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    const second = yield* fixture("other-operation", "Synthetic prompt", "/fixture/other-worktree");
    yield* repository.claim(second.input, second.authorize);
    assert.strictEqual(
      (yield* repository
        .reserveCommand(second.input.claimId, {
          ...command,
          threadId: ThreadId.make(second.preparation.command.threadId),
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    const future = yield* fixture(
      "future-operation",
      "Synthetic prompt",
      "/fixture/future-worktree",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(value.input.claimId, {
          ...command,
          commandId: decodeFixtureCommand(future.preparation.command).commandId,
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommandIdentities(value.input.claimId, [
          command.commandId,
          future.preparation.command.commandId,
        ])
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual((yield* repository.claim(future.input, future.authorize)).status, "claimed");
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("orders append-only typed facts and preserves external gaps and failed results", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const started = {
      kind: "fetch" as const,
      phase: "started" as const,
      effectId: "fixture-fetch",
      timestamp,
      projectCwd: value.input.resources.projectCwd,
      baseRef: "main",
    };
    const completed = { ...started, phase: "completed" as const, result: "failed" as const };
    assert.strictEqual(
      (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
      "conflict",
    );
    const persistedStart = yield* repository.startEffect(
      value.input.claimId,
      started,
      value.authorize,
    );
    assert.strictEqual(
      (yield* repository
        .startEffect(value.input.claimId, started, value.authorize)
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .completeEffect(value.input.claimId, { ...completed, baseRef: "other" })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart],
    );
    const persistedCompletion = yield* repository.completeEffect(value.input.claimId, completed);
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart, persistedCompletion],
    );
    assert.strictEqual(persistedStart.ordinal, 0);
    assert.strictEqual(persistedCompletion.ordinal, 1);
    assert.strictEqual(
      (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .startEffect(
          value.input.claimId,
          { ...started, effectId: "next", projectCwd: "/other/project" },
          value.authorize,
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    yield* repository.startEffect(
      value.input.claimId,
      {
        kind: "setup",
        phase: "started",
        effectId: "fixture-setup",
        timestamp,
        worktreePath: value.input.resources.worktreePath,
        terminalId: "known-terminal",
      },
      value.authorize,
    );
    assert.strictEqual(
      (yield* repository
        .completeEffect(value.input.claimId, {
          kind: "setup",
          phase: "completed",
          effectId: "fixture-setup",
          timestamp,
          worktreePath: value.input.resources.worktreePath,
          terminalId: "other-terminal",
          exitCode: 0,
          result: "succeeded",
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    yield* repository.completeEffect(value.input.claimId, {
      kind: "setup",
      phase: "completed",
      effectId: "fixture-setup",
      timestamp,
      worktreePath: value.input.resources.worktreePath,
      terminalId: "known-terminal",
      exitCode: 0,
      result: "succeeded",
    });
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("fact completion joins an enclosing engine transaction and rolls back with it", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const started = {
      kind: "lifecycle" as const,
      phase: "started" as const,
      effectId: "fixture-normalization",
      timestamp,
      threadId: ThreadId.make(value.preparation.command.threadId),
      action: "normalization" as const,
    };
    const persistedStart = yield* repository.startEffect(
      value.input.claimId,
      started,
      value.authorize,
    );
    const completed = { ...started, phase: "completed" as const, result: "succeeded" as const };
    yield* sql`CREATE TABLE synthetic_engine_receipts (command_id TEXT PRIMARY KEY)`;
    const aborted = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO synthetic_engine_receipts (command_id) VALUES ('synthetic-command')`;
          yield* repository.completeEffect(value.input.claimId, completed);
          return yield* Effect.fail("synthetic-transaction-abort");
        }),
      )
      .pipe(Effect.flip);
    assert.strictEqual(aborted, "synthetic-transaction-abort");
    assert.deepEqual(yield* sql`SELECT * FROM synthetic_engine_receipts`, []);
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart],
    );
    yield* repository.completeEffect(value.input.claimId, completed);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("assigns distinct fact ordinals when asynchronous completions arrive concurrently", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const starts = yield* Effect.all(
      ["first", "second"].map((effectId) =>
        repository.startEffect(
          value.input.claimId,
          {
            kind: "lifecycle",
            phase: "started",
            effectId,
            timestamp,
            threadId: ThreadId.make(value.preparation.command.threadId),
            action: "git_status_refresh",
          },
          value.authorize,
        ),
      ),
      { concurrency: "unbounded" },
    );
    const completions = yield* Effect.all(
      starts.map((fact) => {
        if (fact.kind !== "lifecycle") return Effect.die("Fixture lifecycle fact changed");
        const { ordinal: _ordinal, ...started } = fact;
        return repository.completeEffect(value.input.claimId, {
          ...started,
          phase: "completed",
          result: "succeeded",
        });
      }),
      { concurrency: "unbounded" },
    );
    assert.deepEqual(starts.map((fact) => fact.ordinal).sort(), [0, 1]);
    assert.deepEqual(completions.map((fact) => fact.ordinal).sort(), [2, 3]);
    assert.deepEqual(
      Option.getOrThrow(
        yield* repository.readHistory(value.preparation.command.commandId),
      ).effects.map((fact) => fact.ordinal),
      [0, 1, 2, 3],
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

const identityInventory = (commandId: string) => [
  commandId,
  ...[
    "bootstrap-thread-create",
    "bootstrap-thread-message",
    "bootstrap-thread-preparing",
    "bootstrap-thread-meta-update",
    "bootstrap-thread-preparing-failed",
    "bootstrap-thread-delete",
    "setup-script-requested",
    "setup-script-started",
    "setup-script-failed",
    "worktree-setup-running",
    "worktree-setup-done",
    "worktree-setup-failed",
    "worktree-setup-cancelled",
  ].map((suffix) => `${commandId}:${suffix}`),
];

it.effect(
  "reserves the whole closed inventory without inventing bodies and binds real bodies later",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const ids = identityInventory(value.preparation.command.commandId);
      yield* repository.reserveCommandIdentities(value.input.claimId, ids);
      yield* repository.reserveCommandIdentities(value.input.claimId, [...ids].reverse());
      for (const commandId of ids) {
        assert.deepEqual(
          Option.getOrThrow(yield* repository.getReservedCommandIdentity(commandId)),
          {
            claimId: value.input.claimId,
            commandId,
            threadId: value.preparation.command.threadId,
          },
        );
        assert.isTrue(Option.isNone(yield* repository.getReservedCommand(commandId)));
      }
      const command = decodeFixtureCommand({
        ...value.preparation.command,
        modelSelection: value.preparation.binding.provider_model_selection,
        createdAt: "2026-10-02T12:35:00Z",
      });
      yield* repository.recordNormalizedCommand(value.input.claimId, command);
      const body = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
      assert.strictEqual(body.canonicalCommand, nativeCreationCanonicalJson(command));
      const history = yield* repository.readHistoryByClaim(value.input.claimId);
      assert.deepEqual(
        history,
        Option.getOrThrow(yield* repository.readHistory(command.commandId)),
      );
      assert.strictEqual(history.normalizedCommandDigest, body.commandDigest);
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("invalid inventories and unowned bodies reject without reserving any IDs", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const id = value.preparation.command.commandId;
    for (const ids of [
      [],
      [id, id],
      [id, ""],
      [id, " padded "],
      [id, `${id}:bootstrap-arbitrary`],
      [`${id}:bootstrap-thread-create`],
    ])
      assert.strictEqual(
        (yield* repository.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.result))
          ._tag,
        "Failure",
      );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(
          value.input.claimId,
          yield* Schema.decodeEffect(NativeCreationCommand)(value.preparation.command),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepEqual(yield* sql`SELECT command_id FROM native_creation_reserved_commands`, []);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("native receipts collide before whole-inventory reservation or claim insertion", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const ids = identityInventory(value.preparation.command.commandId);
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${ids[ids.length - 1]!}, 'thread', ${value.preparation.command.threadId}, ${timestamp}, 1, 'accepted')`;
    assert.strictEqual(
      (yield* repository.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      [],
    );
    const other = yield* fixture(
      "receipt-operation",
      "Synthetic prompt",
      "/fixture/receipt-worktree",
    );
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${other.preparation.command.commandId}, 'thread', ${other.preparation.command.threadId}, ${timestamp}, 2, 'accepted')`;
    assert.strictEqual(
      (yield* repository.claim(other.input, other.authorize).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.isTrue(
      Option.isNone(yield* repository.readHistory(other.preparation.command.commandId)),
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("different claims cannot bind or reserve another owner's commands", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const first = yield* fixture();
    const second = yield* fixture(
      "second-operation",
      "Synthetic prompt",
      "/fixture/second-worktree",
    );
    yield* repository.claim(first.input, first.authorize);
    yield* repository.claim(second.input, second.authorize);
    const ids = identityInventory(first.preparation.command.commandId);
    yield* repository.reserveCommandIdentities(first.input.claimId, ids);
    assert.strictEqual(
      (yield* repository
        .reserveCommandIdentities(second.input.claimId, [
          second.preparation.command.commandId,
          ids[1]!,
        ])
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(
          second.input.claimId,
          decodeFixtureCommand({
            ...first.preparation.command,
            threadId: second.preparation.command.threadId,
          }),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT COUNT(*) AS count FROM native_creation_reserved_command_identities`,
      [{ count: ids.length }],
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect(
  "a new repository instance preserves identity ownership and cannot extend partial reservations",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      const claimed = yield* repository.claim(value.input, value.authorize);
      const ids = identityInventory(value.preparation.command.commandId);
      yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
      VALUES (${ids[0]!}, ${value.input.claimId}, ${value.preparation.command.threadId})`;
      const restarted = yield* make;
      assert.strictEqual(
        (yield* restarted.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      const duplicate = yield* restarted.claim(
        { ...value.input, claimId: "takeover-claim", claimedBootId: "later-boot" },
        value.authorize,
      );
      assert.strictEqual(duplicate.status, "duplicate");
      assert.deepEqual(duplicate.history.intent, claimed.history.intent);
      assert.deepEqual(
        yield* sql`SELECT claim_id FROM native_creation_reserved_command_identities`,
        [{ claim_id: value.input.claimId }],
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect(
  "missing or malformed identity lookup remains unresolved, while a valid absence is empty",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const id = value.preparation.command.commandId;
      assert.isTrue(Option.isNone(yield* repository.getReservedCommandIdentity(id)));
      yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
      VALUES (${id}, ${value.input.claimId}, 'wrong-thread')`;
      assert.strictEqual(
        (yield* repository.getReservedCommandIdentity(id).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* repository.readHistoryByClaim("missing-claim").pipe(Effect.flip)).code,
        "unresolved_claim",
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("an unavailable identity table fails closed", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    assert.strictEqual(
      (yield* repository.getReservedCommandIdentity("fixture-command").pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(layer.pipe(Layer.provide(memory)))),
);

it.effect(
  "activity command facts bind actual immutable bodies and preserve repeated fact rejection",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      yield* repository.reserveCommandIdentities(
        value.input.claimId,
        identityInventory(value.preparation.command.commandId),
      );
      const command = decodeFixtureCommand({
        type: "thread.activity.append",
        commandId: `${value.preparation.command.commandId}:worktree-setup-done`,
        threadId: ThreadId.make(value.preparation.command.threadId),
        activity: {
          id: "fixture-native-activity",
          tone: "info",
          kind: "worktree.setup.done",
          summary: "Setup completed",
          payload: { exitCode: 0, terminalId: "real-fixture-terminal" },
          turnId: null,
          createdAt: "2026-10-02T12:36:00Z",
        },
        createdAt: "2026-10-02T12:36:00Z",
      });
      yield* repository.reserveCommand(value.input.claimId, command);
      const reserved = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
      const start = {
        kind: "native_command" as const,
        phase: "started" as const,
        effectId: "fixture-activity-command",
        timestamp: "2026-10-02T12:36:01Z",
        commandId: command.commandId,
        threadId: ThreadId.make(value.preparation.command.threadId),
        commandType: "thread.activity.append" as const,
        commandDigest: reserved.commandDigest,
      };
      yield* repository.startEffect(value.input.claimId, start, value.authorize);
      assert.strictEqual(
        (yield* repository
          .startEffect(value.input.claimId, start, value.authorize)
          .pipe(Effect.flip)).code,
        "conflict",
      );
      const completed = {
        ...start,
        phase: "completed" as const,
        timestamp: "2026-10-02T12:36:02Z",
        eventId: EventId.make("fixture-native-event"),
        sequence: 5,
      };
      assert.strictEqual(
        (yield* repository
          .completeEffect(value.input.claimId, { ...completed, commandDigest: "0".repeat(64) })
          .pipe(Effect.flip)).code,
        "conflict",
      );
      yield* repository.completeEffect(value.input.claimId, completed);
      assert.strictEqual(
        (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
        "conflict",
      );
      assert.strictEqual(
        (yield* repository
          .reserveCommand(
            value.input.claimId,
            decodeFixtureCommand({ ...command, createdAt: "2026-10-02T12:37:00Z" }),
          )
          .pipe(Effect.flip)).code,
        "conflict",
      );
      const history = yield* repository.readHistoryByClaim(value.input.claimId);
      assert.strictEqual(history.effects.length, 2);
      assert.deepEqual(
        yield* Schema.decodeEffect(Schema.fromJsonString(NativeCreationCommand))(
          reserved.canonicalCommand,
        ),
        command,
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("a receipt appearing after identity reservation prevents later body binding", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    yield* repository.reserveCommandIdentities(
      value.input.claimId,
      identityInventory(value.preparation.command.commandId),
    );
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${value.preparation.command.commandId}, 'thread', ${value.preparation.command.threadId}, ${timestamp}, 1, 'accepted')`;
    assert.strictEqual(
      (yield* repository
        .recordNormalizedCommand(
          value.input.claimId,
          yield* Schema.decodeEffect(NativeCreationCommand)(value.preparation.command),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.isTrue(
      Option.isNone(yield* repository.getReservedCommand(value.preparation.command.commandId)),
    );
    assert.isNull(
      (yield* repository.readHistoryByClaim(value.input.claimId)).normalizedCommandDigest,
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

const v2RepositoryLayer = layer.pipe(Layer.provideMerge(
  Layer.effectDiscard(runMigrations()).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" }))),
));
const acceptedStageFixture = Effect.fnUntraced(function* () {
  const repository = yield* NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const value = yield* fixture();
  yield* repository.claim(value.input, value.authorize);
  const command = Schema.decodeUnknownSync(OrchestrationV2Command)({
    type: "prepared-run.release", commandId: value.preparation.command.commandId,
    threadId: value.preparation.command.threadId, runId: "run:native-repository",
  });
  if (command.type !== "prepared-run.release") return yield* Effect.die("Expected release fixture");
  yield* repository.reserveCommandIdentities(value.input.claimId, [command.commandId]);
  yield* repository.recordNormalizedCommand(value.input.claimId, command);
  const digest = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId)).commandDigest;
  const eventId = EventId.make("event:native-repository");
  const events = yield* sql<{ sequence: number }>`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, payload_json, occurred_at, command_id, actor_kind, metadata_json, application_event_version)
    VALUES (${eventId}, 'thread', ${command.threadId}, 1, 'run.updated', '{}', ${timestamp}, ${command.commandId}, 'system', '{}', 2) RETURNING sequence`;
  const sequence = events[0]!.sequence;
  yield* sql`INSERT INTO orchestration_command_receipts (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
    VALUES (${command.commandId}, 'thread', ${command.threadId}, ${command.type}, ${timestamp}, ${sequence}, 'accepted')`;
  yield* sql`INSERT INTO orchestration_v2_native_command_identities
    (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
    VALUES (${command.commandId}, 'native_creation_stage', 2, ${command.type}, 'thread', ${command.threadId}, ${digest}, ${value.preparation.bindingDigest})`;
  const reference = { version: 2 as const, claimId: value.input.claimId, stageCommandId: command.commandId,
    effectId: `effect:${command.commandId}:provider-turn.start:${command.runId}`, stage: "native_command" as const };
  yield* sql`INSERT INTO orchestration_v2_effect_outbox (effect_id, command_id, thread_id, effect_type, payload_json,
    status, attempt_count, available_at, created_at, updated_at)
    VALUES (${reference.effectId}, ${command.commandId}, ${command.threadId}, 'provider-turn.start',
      ${nativeCreationCanonicalJson({ request: { type: "provider-turn.start", runId: command.runId }, nativeCreationExecutionReference: reference })},
      'pending', 0, ${timestamp}, ${timestamp}, ${timestamp})`;
  return { repository, sql, value, command, digest, eventId, sequence, reference };
});

it.effect("reads bounded V2 history without converting the original V1 preparation", () =>
  Effect.gen(function* () {
    const { repository, value, command, digest, eventId, sequence } = yield* acceptedStageFixture();
    const history = yield* repository.readBoundedHistoryByThread(command.threadId);
    assert.strictEqual(history?.commandDigest, value.preparation.commandDigest);
    assert.strictEqual(history?.normalizedCommandDigest, digest);
    assert.deepEqual(history?.effectsV1, []);
    assert.deepEqual(history?.effectsV2, []);
    assert.strictEqual(history?.stageCommands[0]?.commandType, "prepared-run.release");
    assert.deepEqual(history?.stageCommands[0]?.event, { eventId, sequence });
    assert.strictEqual(history?.overflow, false);
    assert.isNull(yield* repository.readBoundedHistoryByThread(ThreadId.make("missing")));
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect("requires exact V2 reservation, receipt, identity and event before execution", () =>
  Effect.gen(function* () {
    const { repository, value, command, digest, eventId, sequence, reference } = yield* acceptedStageFixture();
    const acceptance = { commandId: command.commandId, threadId: command.threadId, commandType: command.type,
      commandDigest: digest, bindingDigest: value.preparation.bindingDigest, eventId, sequence };
    yield* repository.validateCommandAcceptanceV2(acceptance);
    for (const changed of [
      { ...acceptance, bindingDigest: "0".repeat(64) }, { ...acceptance, commandDigest: "0".repeat(64) },
      { ...acceptance, eventId: EventId.make("other") }, { ...acceptance, sequence: sequence + 1 },
      { ...acceptance, threadId: ThreadId.make("other") },
    ]) assert.strictEqual((yield* repository.validateCommandAcceptanceV2(changed).pipe(Effect.result))._tag, "Failure");
    const resolved = yield* repository.readExecutionReference(reference);
    assert.deepEqual(resolved.command, command);
    assert.strictEqual(resolved.preparation.canonicalText, value.preparation.canonicalText);
    assert.strictEqual((yield* repository.readExecutionReference({ ...reference, claimId: "other" }).pipe(Effect.result))._tag, "Failure");
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect("commits one authorized V2 start and preserves unknown prior effects on retry", () =>
  Effect.gen(function* () {
    const { repository, value, reference, eventId, sequence } = yield* acceptedStageFixture();
    const denied = Effect.fail(new NativeCreationAuthorityError({ code: "unsupported_authority", message: "revoked" }));
    assert.strictEqual((yield* repository.startEffectV2(reference, timestamp, denied).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual((yield* repository.readHistoryByClaim(reference.claimId)).effectsV2, []);
    const started = yield* repository.startEffectV2(reference, timestamp, value.authorize);
    assert.strictEqual(started.fact.ordinal, 0);
    assert.strictEqual((yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* repository.startEffectV2({ ...reference, effectId: "replacement" }, timestamp, value.authorize).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* repository.completeEffectV2(reference, { timestamp, eventId, sequence: sequence + 1 }).pipe(Effect.result))._tag, "Failure");
    const completed = yield* repository.completeEffectV2(reference, { timestamp, eventId, sequence });
    assert.strictEqual(completed.ordinal, 1);
    assert.strictEqual((yield* repository.completeEffectV2(reference, { timestamp, eventId, sequence }).pipe(Effect.result))._tag, "Failure");
    const history = yield* repository.readHistoryByClaim(reference.claimId);
    assert.deepEqual(history.effects, []);
    assert.deepEqual(history.effectsV2, [started.fact, completed]);
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect("rolls back a V2 effect start when the containing acceptance transaction fails", () =>
  Effect.gen(function* () {
    const { repository, sql, value, reference } = yield* acceptedStageFixture();
    yield* sql.withTransaction(Effect.gen(function* () {
      yield* repository.startEffectV2(reference, timestamp, value.authorize);
      return yield* Effect.fail("outer transaction rejected");
    })).pipe(Effect.result);
    assert.deepEqual((yield* repository.readHistoryByClaim(reference.claimId)).effectsV2, []);
    yield* repository.startEffectV2(reference, timestamp, value.authorize);
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect("reserves exact thread recovery without weakening the immutable stage inventory", () =>
  Effect.gen(function* () {
    const { repository, value, command, eventId, sequence } = yield* acceptedStageFixture();
    const resource = { kind: "thread" as const, threadId: command.threadId, incarnation: { eventId, sequence } };
    const cleanup = yield* repository.startEffect(value.input.claimId, { kind: "cleanup", phase: "started",
      effectId: "cleanup:repository", timestamp, recoveryScopeId: "scope:repository", resource }, value.authorize);
    const recoveryCommand = Schema.decodeUnknownSync(OrchestrationV2Command)({ type: "thread.delete",
      commandId: `${value.preparation.command.commandId}:bootstrap-thread-delete`, threadId: command.threadId });
    if (recoveryCommand.type !== "thread.delete") return yield* Effect.die("Expected deletion fixture");
    const recovery = { version: 2 as const, claimId: value.input.claimId, commandId: recoveryCommand.commandId,
      threadId: command.threadId, commandType: "thread.delete" as const, canonicalCommand: recoveryCommand,
      commandDigest: nativeCreationSha256(nativeCreationCanonicalJson(recoveryCommand)), commandStartEffectId: "delete:repository",
      cleanupStartEffectId: cleanup.effectId, cleanupStartOrdinal: cleanup.ordinal, recoveryScopeId: "scope:repository", resource };
    assert.strictEqual((yield* repository.reserveThreadRecoveryCommand({ ...recovery, cleanupStartOrdinal: 1 }).pipe(Effect.result))._tag, "Failure");
    assert.isNull(yield* repository.readThreadRecoveryCommand(recovery.commandId));
    yield* repository.reserveThreadRecoveryCommand(recovery);
    yield* repository.reserveThreadRecoveryCommand(recovery);
    assert.deepEqual(yield* repository.readThreadRecoveryCommand(recovery.commandId), recovery);
    assert.strictEqual((yield* repository.reserveThreadRecoveryCommand({ ...recovery, commandStartEffectId: "another" }).pipe(Effect.result))._tag, "Failure");
    yield* repository.startEffect(value.input.claimId, { kind: "native_command", phase: "started", effectId: recovery.commandStartEffectId,
      timestamp, commandId: recovery.commandId, commandType: "thread.delete", threadId: command.threadId,
      commandDigest: recovery.commandDigest }, value.authorize);
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect("native success acknowledgement atomically commits proof, completion and outbox success", () =>
  Effect.gen(function* () {
    const { repository, sql, value, command, reference } = yield* acceptedStageFixture();
    yield* repository.startEffectV2(reference, timestamp, value.authorize);
    const binding = { threadId: command.threadId, providerThreadId: ProviderThreadId.make("provider:repository"),
      providerSessionId: ProviderSessionId.make("session:repository"), instanceId: ProviderInstanceId.make("codex"),
      runtimeGeneration: "generation:repository", nativeThreadId: "native:repository" };
    const attemptId = RunAttemptId.make("attempt:repository");
    yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = 1,
      lease_owner = 'worker:repository', lease_expires_at = '2099-01-01T00:00:00Z' WHERE effect_id = ${reference.effectId}`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads
      (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json)
      VALUES (${command.threadId}, 'fixture-project', 'Synthetic', 'codex', 'full-access', 'default', ${timestamp}, ${timestamp},
        ${JSON.stringify({ activeProviderThreadId: binding.providerThreadId, modelSelection: { instanceId: binding.instanceId } })})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions
      (provider_session_id, thread_id, provider, driver, provider_instance_id, status, updated_at, payload_json)
      VALUES (${binding.providerSessionId}, ${command.threadId}, 'codex', 'codex', 'codex', 'ready', ${timestamp}, '{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads
      (provider_thread_id, thread_id, provider, driver, provider_instance_id, provider_session_id, status, updated_at, payload_json)
      VALUES (${binding.providerThreadId}, ${command.threadId}, 'codex', 'codex', 'codex', ${binding.providerSessionId}, 'running', ${timestamp},
        ${JSON.stringify({ nativeThreadRef: { nativeId: binding.nativeThreadId } })})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings (provider_session_id, thread_id)
      VALUES (${binding.providerSessionId}, ${command.threadId})`;
    yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
      (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, evidence_revision, registered_at)
      VALUES (${command.threadId}, ${binding.providerThreadId}, ${binding.providerSessionId}, 'codex', 'codex', ${binding.nativeThreadId}, ${binding.runtimeGeneration}, 1, ${timestamp})`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at, payload_json)
      VALUES (${command.runId}, ${command.threadId}, 1, 'codex', ${binding.providerThreadId}, 'running', ${timestamp},
        ${JSON.stringify({ activeAttemptId: attemptId })})`;
    yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
      (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider, provider_thread_id, status, payload_json)
      VALUES (${attemptId}, ${command.threadId}, ${command.runId}, 1, 'node:repository', 'codex', ${binding.providerThreadId}, 'running', '{}')`;
    const input = { effectId: reference.effectId, workerId: "worker:repository", expectedAttempt: 1,
      runId: command.runId, attemptId, binding, expectedEvidenceRevision: 1,
      evidence: { operationId: reference.effectId, operation: "start_turn" as const, outcome: "confirmed_success" as const,
        threadId: binding.threadId, providerThreadId: binding.providerThreadId, providerSessionId: binding.providerSessionId,
        instanceId: binding.instanceId, runtimeGeneration: binding.runtimeGeneration, attemptId } };
    for (const invalid of [{ ...input, expectedAttempt: 2 }, { ...input, workerId: "other" },
      { ...input, expectedEvidenceRevision: 2 }, { ...input, evidence: { ...input.evidence, outcome: "unknown" as const } }])
      assert.strictEqual((yield* repository.recordNativeEffectConfirmation(invalid).pipe(Effect.result))._tag, "Failure");
    yield* sql`CREATE TRIGGER fixture_confirmation_failure BEFORE INSERT ON orchestration_v2_native_effect_confirmations
      BEGIN SELECT RAISE(ABORT, 'injected acknowledgement failure'); END`;
    assert.strictEqual((yield* repository.recordNativeEffectConfirmation(input).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* repository.readHistoryByClaim(reference.claimId)).effectsV2.length, 1);
    assert.isNull(yield* repository.readNativeEffectConfirmation(reference.effectId));
    assert.deepEqual(yield* sql`SELECT status FROM orchestration_v2_effect_outbox WHERE effect_id = ${reference.effectId}`, [{ status: "running" }]);
    yield* sql`DROP TRIGGER fixture_confirmation_failure`;
    const proof = yield* repository.recordNativeEffectConfirmation(input);
    assert.deepEqual(yield* repository.readNativeEffectConfirmation(reference.effectId), proof);
    assert.deepEqual(yield* repository.recordNativeEffectConfirmation(input), proof);
    assert.strictEqual((yield* repository.readHistoryByClaim(reference.claimId)).effectsV2.length, 2);
    assert.deepEqual(yield* sql`SELECT status FROM orchestration_v2_effect_outbox WHERE effect_id = ${reference.effectId}`, [{ status: "succeeded" }]);
  }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);
