import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import { OrchestrationV2ThreadStreamItem } from "@t3tools/contracts";
import { subscribeOrchestrationV2Thread } from "../ws.ts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  GitCommandError,
  ChatAttachmentId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as PtyAdapter from "../terminal/PtyAdapter.ts";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as CommandReceipts from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as Management from "./ThreadManagementService.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import {
  LegacyNoTerminalControl,
  type LegacyPreparation,
  type LegacyPreparationUpdate,
} from "./RecordedTypes.ts";
import {
  legacyBootstrapCreateCommandId,
  legacyBootstrapBirth,
  legacyPreparationGeneration,
  legacyPreparationEffectId,
  canonicalLegacyPayload,
  legacyPayloadHash,
} from "./LegacyBootstrap.ts";
import { projectThreadProjectionForWire, projectDomainEventForWire } from "./WireProjection.ts";

type LegacyPreparationStep = LegacyPreparation["steps"][number];

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No native provider entry in workspace failure qualification"),
} as ProviderAdapterV2Shape;

class FailureFixturePty implements PtyAdapter.PtyProcess {
  readonly pid = 92811;
  readonly writes: string[] = [];
  readonly kills: (string | undefined)[] = [];
  private readonly exits = new Set<(event: PtyAdapter.PtyExitEvent) => void>();
  write(data: string) {
    this.writes.push(data);
  }
  resize() {}
  kill(signal?: string) {
    this.kills.push(signal);
    for (const exit of this.exits) exit({ exitCode: 0, signal: 15 });
  }
  onData(_callback: (data: string) => void) {
    return () => {};
  }
  onExit(callback: (event: PtyAdapter.PtyExitEvent) => void) {
    this.exits.add(callback);
    return () => {
      this.exits.delete(callback);
    };
  }
}

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Legacy absent C actual owner and SQL",
  (it) => {
    it.effect.each([
      "opted_out",
      "no_script",
      "intent_lost",
      "outcome_lost",
      "entered_partial",
      "boolean_only",
      "control_present",
      "delete_failed",
      "lost_reply",
      "C_accepted",
      "C_rejected",
    ] as const)(
      "authenticates never-invoked failure D and retains uploaded bytes for %s",
      (scenario) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const root = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-known-failure-" });
          const cwd = `${root}/repo`;
          const common = `${cwd}/.git`;
          const worktreesDir = `${root}/worktrees`;
          const plannedBranch = scenario === "entered_partial" ? "legacy/valid" : "legacy/invalid?";
          yield* fs.makeDirectory(common, { recursive: true });
          yield* fs.makeDirectory(worktreesDir);
          const target = nativeWorktreePath({
            worktreesDir: yield* fs.realPath(worktreesDir),
            cwd,
            branch: plannedBranch,
          });
          const path = yield* Path.Path;
          yield* fs.makeDirectory(path.dirname(target));
          const uploaded = `${root}/attachments/original-upload.png`;
          yield* fs.makeDirectory(`${root}/attachments`);
          const originalBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]);
          yield* fs.writeFile(uploaded, originalBytes);
          const controlProcess = new FailureFixturePty();
          const manager = yield* TerminalManager.makeWithOptions({
            logsDir: `${root}/terminal-logs`,
            env: {},
            shellResolver: () => "/bin/sh",
            processTable: Effect.succeed([]),
            processKillGraceMs: 1,
            subprocessInspector: () =>
              Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
            ptyAdapter: {
              spawn: () =>
                scenario === "control_present"
                  ? Effect.succeed(controlProcess)
                  : Effect.die("Actual no-control D must never spawn"),
            },
          }).pipe(Effect.provide(ProcessRunner.layer));
          const database = makeSqlitePersistenceLive(`${root}/journal.sqlite`).pipe(
            Layer.provide(NodeServices.layer),
          );
          const registry = ProviderAdapterRegistry.makeLayer([adapter]);
          const runtime = makeOrchestratorV2ReplayLayerWithRegistry(
            { name: `legacy-failure-${scenario}` },
            registry,
            {
              databaseLayer: database,
              runEffectWorker: false,
              checkoutFixture: { projects: [], resolvePath: () => undefined, worktreesDir },
            },
          ).pipe(Layer.provide(Layer.succeed(TerminalManager.TerminalManager, manager)));
          const management = Management.layer.pipe(Layer.provide(runtime));
          const stores = Layer.mergeAll(
            CommandReceipts.layer,
            EffectOutbox.layer,
            EventStore.layer,
            ProjectionStore.layer,
          ).pipe(Layer.provide(database));
          const maintenance = ProjectionMaintenance.layer.pipe(
            Layer.provide(Layer.mergeAll(stores, database)),
          );
          const layer = Layer.mergeAll(
            runtime,
            management,
            stores,
            database,
            maintenance,
            OrchestrationEventStoreLive.pipe(Layer.provide(database)),
          );
          const projectId = ProjectId.make(`known-failure:P:${scenario}`);
          const threadId = ThreadId.make(`known-failure:T:${scenario}`);
          const releaseCommandId = CommandId.make(`known-failure:C:${scenario}`);
          const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
          const birthCommandId = CommandId.make(`${createCommandId}:initial-message`);
          const messageId = MessageId.make(`known-failure:M:${scenario}`);
          const policy = {
            version: 1 as const,
            createCommandId,
            birthCommandId,
            releaseCommandId,
            projectId,
            threadId,
            messageId,
            payloadHash: legacyPayloadHash("exact frozen receiving prompt"),
            ownsNewThread: true,
          };
          const commands: string[][] = [];
          const spawner = ChildProcessSpawner.make((command) =>
            Effect.gen(function* () {
              if (!ChildProcess.isStandardCommand(command))
                return yield* Effect.die("Unexpected synthetic pipeline");
              const args = [...command.args];
              commands.push(args);
              const entered = args.includes("add");
              if (entered) {
                yield* fs.makeDirectory(target);
                yield* fs.writeFileString(`${target}/partial`, "unknown material");
              }
              const stdout = args.includes("--git-common-dir")
                ? `${common}\n`
                : args.includes("rev-parse")
                  ? `${"9".repeat(40)}\n`
                  : "";
              return ChildProcessSpawner.makeHandle({
                pid: ChildProcessSpawner.ProcessId(1),
                exitCode: Effect.succeed(
                  ChildProcessSpawner.ExitCode(entered || args.includes("--get-regexp") ? 1 : 0),
                ),
                isRunning: Effect.succeed(false),
                kill: () => Effect.void,
                unref: Effect.succeed(Effect.void),
                stdin: Sink.drain,
                stdout: Stream.encodeText(Stream.make(stdout)),
                stderr: entered
                  ? Stream.encodeText(Stream.make("synthetic entered failure"))
                  : Stream.empty,
                all: Stream.empty,
                getInputFd: () => Sink.drain,
                getOutputFd: () => Stream.empty,
              });
            }),
          );
          const driver = yield* makeGitVcsDriverCore().pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provide(ServerConfig.layerTest(root, { prefix: "legacy-failure-driver-" })),
          );
          const first = yield* Effect.scoped(
            Effect.gen(function* () {
              const sink = yield* EventSink.EventSinkV2;
              const threads = yield* Management.ThreadManagementService;
              const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
              const outbox = yield* EffectOutbox.EffectOutboxV2;
              const collect = (commandId: CommandId) =>
                sink.readByCommandId({ commandId }).pipe(
                  Stream.runCollect,
                  Effect.map((events) => Array.from(events)),
                );
              const projectCommand = CommandId.make(`known-failure:project:${scenario}`);
              yield* sink.commitProjectCommand({
                commandId: projectCommand,
                projectId,
                commandType: "project.create",
                acceptedAt: yield* DateTime.now,
                event: {
                  eventId: EventId.make(`${projectCommand}:event`),
                  aggregateKind: "project",
                  aggregateId: projectId,
                  occurredAt: "2026-10-05T00:00:00.000Z",
                  commandId: projectCommand,
                  causationEventId: null,
                  correlationId: null,
                  metadata: {},
                  type: "project.created",
                  payload: {
                    projectId,
                    title: "Original failure",
                    workspaceRoot: cwd,
                    defaultModelSelection: modelSelection,
                    scripts: [],
                    createdAt: "2026-10-05T00:00:00.000Z",
                    updatedAt: "2026-10-05T00:00:00.000Z",
                  },
                },
              });
              yield* threads.dispatch({
                type: "thread.create",
                commandId: createCommandId,
                threadId,
                projectId,
                title: "Original failure",
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: plannedBranch,
                worktreePath: null,
                createdBy: "user",
                creationSource: "web",
                legacyBootstrap: policy,
              });
              const birthResult = yield* threads.dispatch({
                type: "message.dispatch",
                commandId: birthCommandId,
                threadId,
                messageId,
                text: "Original uploaded prompt",
                attachments: [
                  {
                    type: "image",
                    id: ChatAttachmentId.make("original-upload"),
                    name: "original-upload.png",
                    mimeType: "image/png",
                    sizeBytes: originalBytes.length,
                  },
                ],
                modelSelection,
                createdBy: "user",
                creationSource: "web",
                legacyBootstrap: policy,
                dispatchMode: {
                  type: "defer_start",
                  workspaceStrategy: { type: "worktree", baseRef: "main", startFromOrigin: false },
                  runSetupScript: false,
                },
              });
              const birth = birthResult.storedEvents.find(
                (event) => event.event.type === "run.created",
              );
              assert.equal(birth?.event.type, "run.created");
              if (birth?.event.type !== "run.created")
                return yield* Effect.die("Missing authentic birth");
              const runId = birth.event.payload.id;
              const receivingPolicy = { ...policy, runId };
              const proof = legacyBootstrapBirth({
                policy: receivingPolicy,
                claimEvents: yield* collect(createCommandId),
                birthEvents: yield* collect(birthCommandId),
              });
              assert.equal(proof.type, "valid");
              if (proof.type !== "valid") return yield* Effect.die("Missing authentic claim");
              const claimReceipt = yield* receipts.getByCommandId(createCommandId);
              assert.isTrue(Option.isSome(claimReceipt));
              if (Option.isNone(claimReceipt)) return yield* Effect.die("Missing claim receipt");
              const generation = legacyPreparationGeneration({
                runId,
                birthEventId: proof.birthEventId,
                birthSequence: proof.sequence,
              });
              let preparation: LegacyPreparation = {
                version: 1,
                policy: receivingPolicy,
                claimEventId: proof.claimEventId,
                claimSequence: proof.claimSequence,
                claimReceiptSequence: claimReceipt.value.resultSequence,
                birthEventId: proof.birthEventId,
                birthSequence: proof.sequence,
                birthReceiptSequence: birthResult.sequence,
                generation,
                projectWorkspaceRoot: cwd,
                commonDirectory: common,
                setup: { status: scenario === "no_script" ? "unresolved" : "opted_out" },
                steps: [],
              };
              const journal = (commandId: CommandId, update: LegacyPreparationUpdate) =>
                Effect.gen(function* () {
                  const result = yield* threads.dispatch({
                    type: "prepared-run.progress",
                    commandId,
                    threadId,
                    runId,
                    phase: "worktree",
                    legacyPreparationUpdate: update,
                  });
                  const recorded = yield* collect(commandId);
                  const receipt = yield* receipts.getByCommandId(commandId);
                  assert.isTrue(Option.isSome(receipt));
                  assert.equal(recorded.length, 1);
                  assert.equal(recorded[0]?.sequence, result.sequence);
                  assert.equal(recorded[0]?.event.type, "run.updated");
                  if (
                    recorded[0]?.event.type !== "run.updated" ||
                    recorded[0].event.payload.legacyPreparation === undefined
                  )
                    return yield* Effect.die("Journal readback missing");
                  preparation = recorded[0].event.payload.legacyPreparation;
                });
              yield* journal(
                CommandId.make(`${createCommandId}:preparation:${generation}:initialize`),
                { type: "initialize", preparation },
              );
              if (scenario === "no_script")
                yield* journal(
                  CommandId.make(`${createCommandId}:preparation:${generation}:setup-policy`),
                  { type: "setup-policy", setup: { status: "no_script" } },
                );
              let intentStep: LegacyPreparationStep | undefined;
              const refused = yield* driver
                .createWorktree(
                  {
                    cwd,
                    refName: "main",
                    newRefName: plannedBranch,
                    path: target,
                  },
                  {
                    legacyPreparation: {
                      beforeEffect: (planned) =>
                        Effect.gen(function* () {
                          const { kind, ...input } = planned;
                          const effect = { kind, input };
                          const effectId = legacyPreparationEffectId({ generation, effect });
                          const intentId = CommandId.make(
                            `${createCommandId}:preparation:${generation}:${effectId}:intent`,
                          );
                          const step: LegacyPreparationStep = {
                            effect,
                            effectId,
                            inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
                            intentCommandId: intentId,
                            intentEventId: EventId.make(`${intentId}:event`),
                            state: "intent",
                          };
                          if (scenario === "intent_lost" || scenario === "boolean_only")
                            return yield* new GitCommandError({
                              operation: "fixture.intent",
                              command: "git",
                              cwd,
                              detail: "Intent acceptance unavailable",
                            });
                          yield* journal(intentId, { type: "intent", step });
                          intentStep = step;
                        }),
                      neverInvoked: (planned, reason) =>
                        Effect.gen(function* () {
                          assert.isDefined(intentStep);
                          assert.isFalse(commands.some((args) => args.includes("add")));
                          const { kind, ...input } = planned;
                          assert.equal(
                            canonicalLegacyPayload(intentStep?.effect),
                            canonicalLegacyPayload({ kind, input }),
                          );
                          if (scenario === "outcome_lost")
                            return yield* new GitCommandError({
                              operation: "fixture.outcome",
                              command: "git",
                              cwd,
                              detail: "Outcome acceptance unavailable",
                            });
                          const outcomeId = CommandId.make(
                            `${createCommandId}:preparation:${generation}:${intentStep!.effectId}:outcome`,
                          );
                          yield* journal(outcomeId, {
                            type: "outcome",
                            step: {
                              ...intentStep!,
                              state: "known_no_effect_failure",
                              outcomeCommandId: outcomeId,
                              outcomeEventId: EventId.make(`${outcomeId}:event`),
                              evidence: { type: "never_invoked", owner: "git", reason },
                            },
                          });
                        }),
                      afterEffect: () =>
                        Effect.gen(function* () {
                          assert.equal(scenario, "entered_partial");
                          const outcomeId = CommandId.make(
                            `${createCommandId}:preparation:${generation}:${intentStep!.effectId}:outcome`,
                          );
                          yield* journal(outcomeId, {
                            type: "outcome",
                            step: {
                              ...intentStep!,
                              state: "unknown",
                              outcomeCommandId: outcomeId,
                              outcomeEventId: EventId.make(`${outcomeId}:event`),
                              evidence: { type: "unknown", reason: "partial_material" },
                            },
                          });
                        }),
                    },
                  },
                )
                .pipe(Effect.result);
              assert.isTrue(Result.isFailure(refused));
              const failed = yield* threads.dispatch({
                type: "prepared-run.fail",
                commandId: CommandId.make(`${createCommandId}:fail`),
                threadId,
                runId,
                ...(scenario === "boolean_only" ? { legacyPreparationFailureKnown: true } : {}),
                failure: {
                  class: "validation_error",
                  code: null,
                  message: "worktree exploded",
                  retryable: false,
                },
              });
              const projection = yield* threads.getThreadProjection(threadId);
              const run = projection.runs.find((run) => run.id === runId)!;
              const qualifiedFailure = ![
                "intent_lost",
                "outcome_lost",
                "entered_partial",
                "boolean_only",
              ].includes(scenario);
              const positive =
                scenario === "opted_out" || scenario === "no_script" || scenario === "lost_reply";
              assert.equal(run.status, "failed");
              assert.isNull(run.startedAt);
              assert.equal(run.legacyPreparationFailureDecision !== undefined, qualifiedFailure);
              assert.isTrue(Option.isNone(yield* receipts.getByCommandId(releaseCommandId)));
              assert.isTrue(Option.isNone(yield* receipts.getProjectByCommandId(releaseCommandId)));
              if (scenario === "C_accepted")
                yield* threads.dispatch({
                  type: "thread.visit",
                  commandId: releaseCommandId,
                  threadId,
                  visitedAt: "2026-10-05T00:00:00.000Z",
                });
              if (scenario === "C_rejected") {
                const rejected = yield* threads
                  .dispatch({
                    type: "prepared-run.release",
                    commandId: releaseCommandId,
                    threadId,
                    runId,
                    legacyBootstrap: receivingPolicy,
                  })
                  .pipe(Effect.result);
                assert.isTrue(Result.isFailure(rejected));
                const release = yield* receipts.getByCommandId(releaseCommandId);
                assert.isTrue(Option.isSome(release));
                if (Option.isSome(release)) assert.equal(release.value.status, "rejected");
              }
              if (scenario === "control_present")
                yield* manager.open({
                  threadId,
                  terminalId: "unexpected-native-control",
                  cwd,
                  cols: 80,
                  rows: 24,
                });
              const sql = yield* SqlClient.SqlClient;
              if (scenario === "delete_failed")
                yield* sql`
          CREATE TEMP TRIGGER fail_owned_failure_D BEFORE INSERT ON orchestration_events
          WHEN NEW.event_type = 'thread.deleted'
          BEGIN SELECT RAISE(ABORT, 'synthetic atomic D persistence failure'); END`;
              const commandId = CommandId.make(`${createCommandId}:failure-delete`);
              const control = yield* Schema.decodeEffect(LegacyNoTerminalControl)({
                version: 1,
                type: "no_control",
                policy: receivingPolicy,
                threadId,
                runId,
                claimEventId: proof.claimEventId,
                claimSequence: proof.claimSequence,
                claimReceiptSequence: claimReceipt.value.resultSequence,
                birthEventId: proof.birthEventId,
                birthSequence: proof.sequence,
                birthReceiptSequence: birthResult.sequence,
                preparationGeneration: generation,
                workspacePath: cwd,
                projectWorkspaceRoot: cwd,
              });
              assert.isDefined(threads.dispatchLegacyFailureDelete);
              const deleted = yield* threads.dispatchLegacyFailureDelete!({
                type: "legacy-bootstrap.failure-delete",
                commandId,
                threadId,
                runId,
                legacyBootstrap: receivingPolicy,
                legacyNoControl: control,
              }).pipe(
                Effect.flatMap((result) =>
                  scenario === "lost_reply"
                    ? new Orchestrator.OrchestratorDispatchError({
                        commandId,
                        commandType: "legacy-bootstrap.failure-delete",
                        cause: "Synthetic lost reply after commit",
                      })
                    : Effect.succeed(result),
                ),
                Effect.result,
                Effect.ensuring(
                  scenario === "delete_failed"
                    ? sql`DROP TRIGGER fail_owned_failure_D`.pipe(Effect.orDie, Effect.asVoid)
                    : Effect.void,
                ),
              );
              assert.equal(Result.isSuccess(deleted), positive && scenario !== "lost_reply");
              if (scenario === "control_present") {
                assert.deepEqual(controlProcess.writes, []);
                assert.deepEqual(controlProcess.kills, []);
              }

              assert.deepEqual(yield* fs.readFile(uploaded), originalBytes);
              assert.deepEqual(yield* fs.readDirectory(`${root}/attachments`), [
                "original-upload.png",
              ]);
              assert.equal(
                commands.some((args) => args.includes("add")),
                scenario === "entered_partial",
              );
              const deletionReceipt = yield* receipts.getByCommandId(commandId);
              assert.equal(
                Option.isSome(deletionReceipt) && deletionReceipt.value.status === "accepted",
                positive,
              );
              if (!positive && Option.isSome(deletionReceipt))
                assert.equal(deletionReceipt.value.status, "rejected");
              const deletionEvents = yield* collect(commandId);
              if (positive) {
                assert.deepEqual(
                  deletionEvents.map((stored) => stored.event.type),
                  ["run.updated", "thread.deleted"],
                );
                assert.isTrue(Option.isSome(deletionReceipt));
                assert.equal(
                  deletionEvents[1]?.sequence,
                  Option.isSome(deletionReceipt) ? deletionReceipt.value.resultSequence : -1,
                );
                assert.equal(deletionEvents[0]!.sequence + 1, deletionEvents[1]!.sequence);
                assert.equal(
                  deletionEvents[0]?.event.type === "run.updated"
                    ? deletionEvents[0].event.payload.legacyPreparationFailureDecision?.deletion
                        ?.failureReceiptSequence
                    : -1,
                  failed.sequence,
                );
                assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
                const publicEvents = deletionEvents.map((event) =>
                  projectDomainEventForWire(event.event),
                );
                assert.notInclude(
                  yield* encodeJson(publicEvents),
                  "legacyPreparationFailureDecision",
                );
                const replayed = yield* threads.dispatchLegacyFailureDelete!({
                  type: "legacy-bootstrap.failure-delete",
                  commandId,
                  threadId,
                  runId,
                  legacyBootstrap: receivingPolicy,
                  legacyNoControl: control,
                });
                assert.equal(
                  replayed.sequence,
                  Option.isSome(deletionReceipt) ? deletionReceipt.value.resultSequence : -1,
                );
                const changed = yield* threads.dispatchLegacyFailureDelete!({
                  type: "legacy-bootstrap.failure-delete",
                  commandId,
                  threadId,
                  runId,
                  legacyBootstrap: receivingPolicy,
                  legacyNoControl: { ...control, workspacePath: `${cwd}/changed` },
                }).pipe(Effect.result);
                assert.isTrue(Result.isFailure(changed));
                if (scenario === "opted_out") {
                  const store = yield* EventStore.EventStoreV2;
                  const raw = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect));
                  const created = raw[0];
                  if (created?.event.type !== "thread.created")
                    return yield* Effect.die("Real stream lacks shell birth");
                  let memory = ProjectionStore.emptyProjection(created.event);
                  for (const recorded of raw.slice(1))
                    memory = ProjectionStore.applyToProjection(memory, recorded.event);
                  assert.deepEqual(
                    memory.runs[0]?.legacyPreparationFailureDecision,
                    run.legacyPreparationFailureDecision === undefined
                      ? undefined
                      : {
                          ...run.legacyPreparationFailureDecision,
                          deletion:
                            deletionEvents[0]?.event.type === "run.updated"
                              ? deletionEvents[0].event.payload.legacyPreparationFailureDecision
                                  ?.deletion
                              : undefined,
                        },
                  );
                  const acceptedRun = memory.runs[0]!;
                  const { legacyPreparationFailureDecision: _privateDecision, ...stale } =
                    acceptedRun;
                  const ordinary = {
                    type: "run.updated" as const,
                    id: EventId.make("known-failure:ordinary-stale-run"),
                    threadId,
                    runId,
                    providerInstanceId: acceptedRun.providerInstanceId,
                    occurredAt: yield* DateTime.now,
                    payload: stale,
                  };
                  const replayFrames = Array.from(
                    yield* Stream.unwrap(
                      subscribeOrchestrationV2Thread({
                        threadId,
                        afterSequence: failed.sequence,
                        requestCompletionMarker: true,
                      }),
                    ).pipe(Stream.take(3), Stream.runCollect, Effect.timeout("5 seconds")),
                  );
                  assert.deepEqual(
                    replayFrames.map((frame) => frame.kind),
                    ["event", "event", "synchronized"],
                  );
                  assert.deepEqual(
                    replayFrames
                      .filter((frame) => frame.kind === "event")
                      .map((frame) => frame.sequence),
                    deletionEvents.map((event) => event.sequence),
                  );
                  const snapshotFrames = Array.from(
                    yield* Stream.unwrap(
                      subscribeOrchestrationV2Thread({
                        threadId,
                        requestCompletionMarker: true,
                      }),
                    ).pipe(Stream.take(2), Stream.runCollect, Effect.timeout("5 seconds")),
                  );
                  assert.deepEqual(
                    snapshotFrames.map((frame) => frame.kind),
                    ["snapshot", "synchronized"],
                  );
                  const ready = yield* Deferred.make<void>();
                  const live = yield* Stream.unwrap(
                    subscribeOrchestrationV2Thread({
                      threadId,
                      afterSequence: deletionEvents[1]!.sequence,
                      requestCompletionMarker: true,
                    }),
                  ).pipe(
                    Stream.tap((frame) =>
                      frame.kind === "synchronized"
                        ? Deferred.succeed(ready, undefined)
                        : Effect.void,
                    ),
                    Stream.take(2),
                    Stream.runCollect,
                    Effect.timeout("5 seconds"),
                    Effect.ensuring(Deferred.succeed(ready, undefined)),
                    Effect.forkChild,
                  );
                  yield* Effect.race(
                    Deferred.await(ready),
                    Fiber.join(live).pipe(
                      Effect.andThen(
                        Effect.die("Live subscription ended before the ordinary event"),
                      ),
                    ),
                  );
                  const staleMemory = ProjectionStore.applyToProjection(memory, ordinary);
                  assert.deepEqual(
                    staleMemory.runs[0]?.legacyPreparationFailureDecision,
                    acceptedRun.legacyPreparationFailureDecision,
                  );
                  const staleCommit = yield* sink.commitCommand({
                    commandId: CommandId.make("known-failure:ordinary-stale"),
                    threadId,
                    commandType: "synthetic.stale-native-update",
                    acceptedAt: yield* DateTime.now,
                    events: [ordinary],
                    effects: [],
                  });
                  assert.deepEqual(
                    (yield* threads.getThreadProjection(threadId)).runs[0]
                      ?.legacyPreparationFailureDecision,
                    acceptedRun.legacyPreparationFailureDecision,
                  );
                  const liveFrames = Array.from(yield* Fiber.join(live));
                  assert.deepEqual(
                    liveFrames.map((frame) => frame.kind),
                    ["synchronized", "event"],
                  );
                  assert.equal(
                    liveFrames[1]!.kind === "event" ? liveFrames[1]!.sequence : -1,
                    staleCommit.receipt.resultSequence,
                  );
                  for (const frames of [replayFrames, snapshotFrames, liveFrames]) {
                    const publicFrames = yield* Effect.forEach(frames, (frame) =>
                      Schema.encodeEffect(OrchestrationV2ThreadStreamItem)(frame),
                    );
                    const publicJson = yield* encodeJson(publicFrames);
                    assert.notInclude(publicJson, "legacyPreparationFailureDecision");
                    assert.notInclude(publicJson, "legacyReleaseDecision");
                    assert.notInclude(publicJson, "legacyPreparation");
                    assert.notInclude(publicJson, receivingPolicy.payloadHash);
                  }
                  assert.deepEqual(
                    (yield* threads.getThreadProjection(threadId)).runs[0]
                      ?.legacyPreparationFailureDecision,
                    acceptedRun.legacyPreparationFailureDecision,
                  );
                  const decision = acceptedRun.legacyPreparationFailureDecision!;
                  const deletion = decision.deletion!;
                  const provenance = deletion.provenance;
                  if (provenance.type !== "no_control")
                    return yield* Effect.die("Expected no-control proof");
                  const tampered = {
                    ...ordinary,
                    id: EventId.make("known-failure:tampered-proof"),
                    payload: {
                      ...acceptedRun,
                      legacyPreparationFailureDecision: {
                        ...decision,
                        deletion: {
                          ...deletion,
                          provenance: {
                            ...provenance,
                            control: { ...provenance.control, workspacePath: `${cwd}/replaced` },
                          },
                        },
                      },
                    },
                  };
                  assert.throws(
                    () => ProjectionStore.applyToProjection(memory, tampered),
                    /Malformed or replaced/,
                  );
                  const tamperId = CommandId.make("known-failure:tamper-attempt");
                  const rejectedTamper = yield* sink
                    .commitCommand({
                      commandId: tamperId,
                      threadId,
                      commandType: "synthetic.tamper",
                      acceptedAt: yield* DateTime.now,
                      events: [tampered],
                      effects: [],
                    })
                    .pipe(Effect.result);
                  assert.isTrue(Result.isFailure(rejectedTamper));
                  assert.isTrue(Option.isNone(yield* receipts.getByCommandId(tamperId)));
                  assert.deepEqual(yield* collect(tamperId), []);
                  assert.deepEqual(
                    (yield* threads.getThreadProjection(threadId)).runs[0]
                      ?.legacyPreparationFailureDecision,
                    acceptedRun.legacyPreparationFailureDecision,
                  );
                  const projectionMaintenance =
                    yield* ProjectionMaintenance.ProjectionMaintenanceV2;
                  yield* projectionMaintenance.rebuild;
                  assert.deepEqual(
                    (yield* threads.getThreadProjection(threadId)).runs[0]
                      ?.legacyPreparationFailureDecision,
                    acceptedRun.legacyPreparationFailureDecision,
                  );
                }
              } else {
                assert.deepEqual(deletionEvents, []);
                const survivor = yield* threads.getThreadProjection(threadId);
                assert.isNull(survivor.thread.deletedAt);
                assert.isUndefined(survivor.runs[0]?.legacyPreparationFailureDecision?.deletion);
                assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
                if (scenario === "entered_partial")
                  assert.equal(yield* fs.readFileString(`${target}/partial`), "unknown material");
              }
              return {
                positive,
                commandId,
                runId,
                control,
                receivingPolicy,
                failedSequence: failed.sequence,
              };
            }).pipe(Effect.provide(layer)),
          );
          if (first.positive)
            yield* Effect.scoped(
              Effect.gen(function* () {
                const threads = yield* Management.ThreadManagementService;
                const projection = yield* threads.getThreadProjection(threadId);
                assert.isNotNull(projection.thread.deletedAt);
                const run = projection.runs.find((entry) => entry.id === first.runId)!;
                assert.equal(
                  run.legacyPreparationFailureDecision?.deletion?.failureReceiptSequence,
                  first.failedSequence,
                );
                assert.notInclude(
                  yield* encodeJson(projectThreadProjectionForWire(projection)),
                  "legacyPreparationFailureDecision",
                );
                const replayed = yield* threads.dispatchLegacyFailureDelete!({
                  type: "legacy-bootstrap.failure-delete",
                  commandId: first.commandId,
                  threadId,
                  runId: first.runId,
                  legacyBootstrap: first.receivingPolicy,
                  legacyNoControl: first.control,
                });
                assert.isAbove(replayed.sequence, first.failedSequence);
                assert.deepEqual(yield* fs.readFile(uploaded), originalBytes);
              }).pipe(Effect.provide(layer)),
            );
        }),
    );
  },
);
