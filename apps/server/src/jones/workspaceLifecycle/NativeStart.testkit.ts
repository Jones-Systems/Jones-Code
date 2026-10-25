import {
  AuthSessionId,
  CommandId,
  EventId,
  MessageId,
  ThreadId,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Repository from "../nativeCreation/NativeCreationRepository.ts";
import * as RepositorySqlite from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import * as Authority from "../nativeCreation/NativeCreationAuthority.ts";
import { NativeCreationExecutionReferenceV2 } from "../nativeCreation/NativeCreationExecutionTypes.ts";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  validateNativeCreationPreparation,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { projectId, selection, type fixture } from "./ManagedConsumer.testkit.ts";

const decodePreparationBinding = Schema.decodeUnknownEffect(NativePreparationBinding);
const decodeHistoricalBinding = Schema.decodeUnknownEffect(NativeCreationHistoricalBinding);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);
const decodeExecutionReference = Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2);
const decodeSession = Schema.decodeUnknownEffect(AuthSessions.AuthSessionRecord);
const encodeEffectPayload = Schema.encodeEffect(
  Schema.fromJsonString(Outbox.NativeOrchestrationEffectPayloadV2),
);

// Fixture-only grant/binding resolvers; production NativeCreationAuthority, durable native100 repository and private guard remain real.
export const nativeStartFixture = (f: Effect.Success<typeof fixture>) =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const deadline = "2099-01-01T00:00:00.000Z";
    const actor = AuthSessionId.make("synthetic-native-actor");
    const sql = yield* SqlClient.SqlClient;
    const repository = yield* Repository.NativeCreationRepository.pipe(
      Effect.provide(RepositorySqlite.layer),
    );
    const binding = yield* decodePreparationBinding({
      backend_instance: "sql-fixture-backend",
      environment_id: "synthetic-environment",
      project_id: projectId,
      project_cwd: f.repo,
      account_ref: "sql-fixture-account",
      runtime_mode: "full-access",
      interaction_mode: "default",
      base_branch: "main",
      start_from_origin: false,
      run_setup_script: false,
      provider_model_selection: { instanceId: "codex", model: selection.model },
    });
    const prepared = nativePreparationCommand(
      "sql-native-execution",
      binding,
      "Synthetic prompt",
      "Synthetic title",
      now,
    );
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(
        nativeCreationCanonicalJson({
          schema: "voice.t3-bootstrap-preparation/v1",
          operation_id: "sql-native-execution",
          binding,
          command: prepared,
          preparation_id: prepared.commandId.replace("voice-command-", "voice-bootstrap-"),
          binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
          prompt_digest: nativeCreationSha256(prepared.message.text),
          command_digest: nativeCreationSha256(nativeCreationCanonicalJson(prepared)),
        }),
      ),
    );
    const historical = yield* decodeHistoricalBinding({
      backendInstance: binding.backend_instance,
      environmentId: binding.environment_id,
      projectId: binding.project_id,
      projectCwd: binding.project_cwd,
      accountRef: binding.account_ref,
      accountBindingId: "sql-qualified-account",
      accountBindingRevision: 1,
      providerModelSelection: binding.provider_model_selection,
      runtimeMode: binding.runtime_mode,
      interactionMode: binding.interaction_mode,
      baseBranch: binding.base_branch,
      startFromOrigin: false,
      runSetupScript: false,
      requestedBranch: prepared.bootstrap.prepareWorktree.branch,
    });
    const resources = {
      projectCwd: binding.project_cwd,
      branch: historical.requestedBranch,
      worktreePath: f.linked,
    };
    yield* f.runGit(["branch", "-m", "work", resources.branch]);
    yield* f.seedThread(ThreadId.make(prepared.threadId), resources.branch);
    yield* f.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("native-real-run"),
      threadId: ThreadId.make(prepared.threadId),
      createdBy: "user",
      creationSource: "server",
      messageId: MessageId.make(prepared.message.messageId),
      text: prepared.message.text,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
    });
    const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(
      ThreadId.make(prepared.threadId),
    );
    const nativeRunId = projection.runs[0]!.id;
    const claimId = "sql-native-claim";
    yield* repository.claim(
      {
        preparation,
        resources,
        claimId,
        claimedBootId: "sql-boot",
        claimedAt: now,
        actorSessionId: actor,
        grantId: "sql-grant",
        grantRevision: 1,
      },
      Effect.succeed(historical),
    );
    yield* sql`INSERT INTO native_creation_automation_enrollments(session_id,enrolled_at) VALUES(${actor},${now})`;
    const command = yield* decodeCommand({
      type: "prepared-run.release",
      commandId: `${prepared.commandId}:native:v2:message`,
      threadId: prepared.threadId,
      runId: nativeRunId,
    });
    if (command.type !== "prepared-run.release")
      return yield* Effect.die(new Error("Synthetic release has wrong type"));
    yield* repository.reserveExecutionCommandIdentities!(claimId, [
      prepared.commandId,
      command.commandId,
    ]);
    const normalized = yield* decodeCommand({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "server",
      commandId: prepared.commandId,
      threadId: prepared.threadId,
      messageId: prepared.message.messageId,
      text: prepared.message.text,
      attachments: [],
      modelSelection: binding.provider_model_selection,
      dispatchMode: { type: "start_immediately" },
    });
    yield* repository.recordNormalizedCommand(claimId, normalized);
    yield* repository.reserveCommand(claimId, command);
    const reference = yield* decodeExecutionReference({
      version: 2,
      claimId,
      stageCommandId: command.commandId,
      effectId: `effect:${command.commandId}:provider-turn.start:${command.runId}`,
      stage: "native_command",
    });
    const eventId = EventId.make("sql-release-event");
    const appendReceipt = Effect.gen(function* () {
      const events = yield* sql<{
        sequence: number;
      }>`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(${eventId},'thread',${command.threadId},(SELECT coalesce(max(stream_version),0)+1 FROM orchestration_events WHERE stream_id=${command.threadId}),'run.updated',${now},${command.commandId},'system',${nativeCreationCanonicalJson(projection.runs[0])},'{}',2) RETURNING sequence`;
      const sequence = events[0]!.sequence;
      yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type)
      VALUES(${command.commandId},'thread',${command.threadId},${now},${sequence},'accepted',NULL,${command.type})`;
      return sequence;
    });
    const accept = (actual: OrchestrationV2Command = command) =>
      sql.withTransaction(
        Effect.gen(function* () {
          const sequence = yield* appendReceipt;
          yield* repository.recordExecutionAcceptance!({
            claimId,
            command: actual,
            eventId,
            sequence,
          });
          return sequence;
        }),
      );
    const enqueue = Effect.gen(function* () {
      const payload = yield* encodeEffectPayload({
        request: { type: "provider-turn.start", runId: command.runId },
        nativeCreationExecutionReference: reference,
      });
      yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at)
      VALUES(${reference.effectId},${command.commandId},${command.threadId},'provider-turn.start',${payload},'running',1,${now},'sql-worker',${deadline},${now},${now})`;
    });
    const session = yield* decodeSession({
      sessionId: actor,
      subject: "Synthetic native actor",
      scopes: ["orchestration:operate"],
      method: "bearer-access-token",
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "bot",
        os: null,
        browser: null,
      },
      issuedAt: now,
      expiresAt: deadline,
      revokedAt: null,
      lastConnectedAt: null,
    });
    const grant: Authority.NativeCreationGrant = {
      grantId: "sql-grant",
      revision: 1,
      actorSessionId: actor,
      issuerId: "sql-issuer",
      expiresAt: DateTime.makeUnsafe(deadline),
      revoked: false,
      operationId: preparation.operationId,
      preparationId: preparation.preparationId,
      preparationSha256: preparation.preparationSha256,
      bindingDigest: preparation.bindingDigest,
      binding: historical,
      resources,
      allowedStages: ["native_command", "fetch"],
      recoveryScopes: [],
    };
    let revoked = false;
    const revoke = Effect.sync(() => {
      revoked = true;
    });
    const authorityLayer = Layer.fresh(Authority.NativeCreationAuthorityLive).pipe(
      Layer.provide(Layer.succeed(Repository.NativeCreationRepository, repository)),
      Layer.provide(
        Layer.succeed(
          AuthSessions.AuthSessionRepository,
          AuthSessions.AuthSessionRepository.of({
            getById: () => Effect.succeed(Option.some(session)),
            create: () => Effect.void,
            createReplacingActive: () => Effect.succeed([]),
            createIfAbsent: () => Effect.void,
            listActive: () => Effect.succeed([]),
            revoke: () => Effect.succeed(false),
            revokeAllExcept: () => Effect.succeed([]),
            setLastConnectedAt: () => Effect.void,
            setClientConnection: () => Effect.void,
          }),
        ),
      ),
      Layer.provide(
        Layer.succeed(Authority.NativeCreationGrantResolver, {
          resolveCurrent: () =>
            Effect.succeed({
              enrolledSessionId: actor,
              trustedIssuerId: "sql-issuer",
              grant: { ...grant, revoked },
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(Authority.NativeCreationBindingResolver, {
          resolveCurrent: () => Effect.succeed(historical),
        }),
      ),
    );
    const issue = Effect.gen(function* () {
      const authority = yield* Authority.NativeCreationAuthority;
      return yield* authority.issueExecution!({ reference, timestamp: now });
    }).pipe(Effect.provide(authorityLayer));
    yield* accept();
    yield* enqueue;
    return { repository, command, reference, resources, issue, revoke };
  });
