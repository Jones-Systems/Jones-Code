import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ThreadId,
  EventId,
  NativeCreationHistoricalBinding,
  OrchestrationCommand,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { runMigrations } from "../persistence/Migrations.ts";
import * as NativeCreationRepositoryLayer from "../persistence/Layers/NativeCreationRepository.ts";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
  nativeCreationCommandDigest,
} from "./NativeCreationPreparation.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { makeCommandObservationQuery } from "./CommandObservation.ts";
import { nativeBootstrapCommandIds } from "../ws.ts";

const database = Layer.effectDiscard(runMigrations()).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const projection = Layer.mock(ProjectionSnapshotQuery)({
  getSnapshotSequence: () => Effect.succeed({ snapshotSequence: 0 }),
  getThreadShellById: () => Effect.succeedNone,
});
const layer = Layer.mergeAll(database, projection);
const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
  backend_instance: "synthetic-backend",
  environment_id: "synthetic-env",
  project_id: "synthetic-project",
  project_cwd: "/synthetic/project",
  account_ref: "synthetic-account",
  runtime_mode: "full-access",
  interaction_mode: "default",
  base_branch: "main",
  start_from_origin: false,
  run_setup_script: false,
  provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
});
const raw = nativePreparationCommand(
  "synthetic-observation",
  binding,
  "SYNTHETIC_PRIVATE_TEXT_NOT_HISTORY",
  "Synthetic thread",
  "2026-10-02T12:00:00Z",
);
const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
  backendInstance: binding.backend_instance,
  environmentId: binding.environment_id,
  projectId: binding.project_id,
  projectCwd: binding.project_cwd,
  accountRef: binding.account_ref,
  accountBindingId: "synthetic-binding",
  accountBindingRevision: 1,
  providerModelSelection: binding.provider_model_selection,
  runtimeMode: binding.runtime_mode,
  interactionMode: binding.interaction_mode,
  baseBranch: "main",
  startFromOrigin: false,
  runSetupScript: false,
  requestedBranch: raw.bootstrap.prepareWorktree.branch,
});
const timestamp = "2026-10-02T12:00:01Z";
const queryInput = {
  threadId: ThreadId.make(raw.threadId),
  commandId: CommandId.make(raw.commandId),
  messageId: MessageId.make(raw.message.messageId),
};
const fixture = Effect.gen(function* () {
  const repository = yield* NativeCreationRepositoryLayer.make;
  const query = yield* makeCommandObservationQuery();
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "synthetic-observation",
        preparation_id: raw.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding,
        command: raw,
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(raw.message.text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(raw)),
      }),
    ),
  );
  const claimId = "synthetic-observation-claim";
  yield* repository.claim(
    {
      claimId,
      preparation,
      resources: {
        projectCwd: binding.project_cwd,
        branch: historical.requestedBranch,
        worktreePath: "/synthetic/worktree",
      },
      claimedBootId: "synthetic-boot",
      claimedAt: timestamp,
      actorSessionId: "synthetic-session",
      grantId: "synthetic-grant",
      grantRevision: 1,
    },
    Effect.succeed(historical),
  );
  yield* repository.reserveCommandIdentities(claimId, nativeBootstrapCommandIds(raw.commandId));
  const decoded = yield* Schema.decodeUnknownEffect(OrchestrationCommand)(raw);
  if (decoded.type !== "thread.turn.start") return yield* Effect.die("Invalid synthetic fixture");
  const { bootstrap: _bootstrap, ...finalCommand } = decoded;
  return { repository, query, claimId, finalCommand };
});

it.effect("claimed history remains absent until normalized command digest is durable", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const observed = yield* f.query.observe(queryInput);
    assert.isUndefined(observed.creation);
    assert.equal(observed.commandStatus, "not_found");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "normalized intent with no creation chain cannot become complete or leak original bytes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.repository.recordNormalizedCommand(f.claimId, f.finalCommand);
      const observed = yield* f.query.observe(queryInput);
      assert.isDefined(observed.creation);
      assert.equal(observed.creation?.outcome, "in_progress");
      assert.isNull(observed.creation?.incarnation);
      const encoded = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
        observed,
      );
      assert.notInclude(encoded, raw.message.text);
      assert.notInclude(encoded, "canonicalPreparation");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "unresolved external starts stay unknown without receipts, replay, or regenerated identity",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.repository.recordNormalizedCommand(f.claimId, f.finalCommand);
      yield* f.repository.startEffect(
        f.claimId,
        {
          kind: "fetch",
          phase: "started",
          effectId: "synthetic-fetch",
          timestamp,
          projectCwd: binding.project_cwd,
          baseRef: "main",
        },
        Effect.succeed(historical),
      );
      const observed = yield* f.query.observe(queryInput);
      assert.equal(observed.creation?.outcome, "unknown");
      assert.deepEqual(observed.creation?.unresolvedEffects, ["synthetic-fetch"]);
      assert.equal(observed.creation?.claimId, f.claimId);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "command completion facts without matching native event and receipt cannot attest creation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.repository.recordNormalizedCommand(f.claimId, f.finalCommand);
      const create = yield* Schema.decodeUnknownEffect(OrchestrationCommand)({
        type: "thread.create",
        commandId: `${raw.commandId}:bootstrap-thread-create`,
        threadId: raw.threadId,
        ...raw.bootstrap.createThread,
      });
      if (create.type !== "thread.create")
        return yield* Effect.die("Invalid synthetic create fixture");
      yield* f.repository.reserveCommand(f.claimId, create);
      const start = yield* f.repository.startEffect(
        f.claimId,
        {
          kind: "native_command",
          phase: "started",
          effectId: "synthetic-create",
          timestamp,
          commandId: create.commandId,
          threadId: create.threadId,
          commandType: "thread.create",
          commandDigest: nativeCreationCommandDigest(create),
        },
        Effect.succeed(historical),
      );
      if (start.kind !== "native_command")
        return yield* Effect.die("Invalid synthetic effect fixture");
      yield* f.repository.completeEffect(f.claimId, {
        ...start,
        phase: "completed",
        eventId: EventId.make("synthetic-event-without-native-row"),
        sequence: 1,
      });
      const observed = yield* f.query.observe(queryInput);
      assert.equal(observed.creation?.outcome, "unknown");
      assert.isNotNull(observed.creation?.incarnation);
    }).pipe(Effect.provide(layer)),
);
