import { assert, it } from "@effect/vitest";
import { CommandId, EnvironmentId, MessageId, NodeId, OrchestratorMcpFailure, RunId,
  type OrchestrationV2Subagent, type OrchestratorMcpThreadSendResult } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Links from "../../peer/PeerLinks.ts";
import * as Peers from "../../peer/PeerMcpClient.ts";
import { fixture as parentFixture } from "../taskContinuation/testkit.ts";
import { layer, envelope as legacyEnvelope, commandId as legacyCommand } from "./RemoteDelegation.testkit.ts";
import * as Store from "./IntentStore.ts";
import * as FollowUp from "./FollowUp.ts";

// Real production mechanism + released SQLite ledger. Only the authoritative
// core/peer ports are fakes: these do not qualify native locking or socket behavior.
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.makeFollowUps;
  const base = parentFixture();
  const destination = EnvironmentId.make("destination");
  const oldTask: OrchestrationV2Subagent = {...base.task, driver: "remote",
    remoteChild: {environmentId: destination, threadId: base.child.thread.id, runId: base.run.id, label: "Peer"}};
  let tasks = [oldTask];
  let runs = [...base.parent.runs];
  let generation = "link-generation-1";
  let locked = false;
  let authenticated = true;
  let supported = true;
  let receipt: typeof FollowUp.FollowUpReceipt.Type | undefined;
  let sends = 0;
  let stopDebt = false;
  let failBind = false;
  let withdrawals = 0;
  let withdrawalStatus = "dispatching";
  const readCalls: Array<{tool: string; arguments: Record<string, unknown>; generation?: string}> = [];
  const receipts = new Map<string, {storedEvents: ReadonlyArray<{event: {type: string; payload: unknown}}>}>();
  const commandOwner: FollowUp.FollowUpCommandOwner["Service"] = {
    inParentTransaction: (_id, effect) => Effect.acquireUseRelease(
      Effect.sync(() => { assert.isFalse(locked); locked = true; }),
      () => sql.withTransaction(effect).pipe(Effect.mapError((error) => Schema.is(OrchestratorMcpFailure)(error) ? error :
        new OrchestratorMcpFailure({code: "orchestration_error", message: "injected transaction failure"}))),
      () => Effect.sync(() => { locked = false; })),
    assertParentCaller: (scope, runId) => Effect.gen(function* () {
      assert.isTrue(locked);
      if (!authenticated || scope.thread.providerSessionId !== base.scope.thread!.providerSessionId ||
          !runs.some((run) => run.id === runId && Threads.isActiveRun(run)))
        return yield* new OrchestratorMcpFailure({code: "capability_denied", message: "parent session ended"});
    }),
    dispatch: (command) => Effect.gen(function* () {
      if (failBind && command.type === "delegated_task.remote.bind")
        return yield* new OrchestratorMcpFailure({code: "orchestration_error", message: "injected bind failure"});
      return yield* Effect.sync(() => {
        assert.isTrue(locked);
        const replay = receipts.get(command.commandId);
        if (replay) return replay;
        let storedEvents: Array<{event: {type: string; payload: unknown}}> = [];
        if (command.type === "delegated_task.remote.request") {
          const {remoteChild: _oldBinding, ...unbound} = oldTask;
          const next: OrchestrationV2Subagent = {...unbound, id: NodeId.make(`reserved:${receipts.size}`),
            result: null, status: "pending", completedAt: null,
            runId: command.parentRunId, prompt: command.task};
          tasks = [...tasks, next];
          storedEvents = [{event: {type: "subagent.updated", payload: next}}];
        } else if (command.type === "delegated_task.remote.bind") {
          tasks = tasks.map((task) => {
            if (task.id !== command.taskId) return task;
            if (task.result !== null) stopDebt = true;
            return {...task, remoteChild: command.remoteChild};
          });
        } else if (command.type === "delegated_task.remote.complete") {
          tasks = tasks.map((task) => task.id !== command.taskId || task.result !== null ? task :
            {...task, result: command.result, status: command.status});
        }
        const result = {storedEvents};
        receipts.set(command.commandId, result);
        return result;
      });
    }),
  };
  const threadService = {getThreadRecords: () => Effect.sync(() => ({...base.parent, runs, subagents: tasks}))} as unknown as Threads.ThreadManagementService["Service"];
  const linkService = {
    get: () => Effect.succeed(Option.some({linkedAt: base.now, expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00.000Z"), label: "Peer"})),
    currentGeneration: () => Effect.sync(() => Option.some(generation)),
  } as unknown as Links.PeerLinks["Service"];
  const peerService: Peers.PeerMcpClient["Service"] = {call: (input) => Effect.gen(function* () {
    assert.isFalse(locked, "network must be outside parent transaction");
    readCalls.push({tool: input.tool, arguments: input.arguments, ...(input.expectedGeneration === undefined ? {} : {generation: input.expectedGeneration})});
    let result: unknown;
    if (input.tool === "orchestrator_capabilities") result = {features: supported ? {
      remoteFollowUpSendVersion: FollowUp.receiptVersion, remoteFollowUpReceiptVersion: FollowUp.receiptVersion} : {}};
    else if (input.tool === "t3_thread_send_receipt_lookup") {
      result = receipt ?? {version: FollowUp.receiptVersion, environmentId: destination,
        ...input.arguments, outcome: "unknown", sent: null};
    } else if (input.tool === "t3_remote_admission_cancel") {
      withdrawals++;
      result = {...input.arguments, status: withdrawalStatus, commandId: null,
        createdAt: DateTime.formatIso(base.now), updatedAt: DateTime.formatIso(base.now)};
    } else throw new Error(`Unexpected peer mutation: ${input.tool}`);
    return Schema.decodeUnknownSync(input.success)(result);
  })};
  const make = (withOwner = true) => {
    const effect = FollowUp.make.pipe(
      Effect.provideService(Threads.ThreadManagementService, threadService),
      Effect.provideService(Links.PeerLinks, linkService), Effect.provideService(Peers.PeerMcpClient, peerService));
    return withOwner ? effect.pipe(Effect.provideService(FollowUp.FollowUpCommandOwner, commandOwner)) : effect;
  };
  const service = yield* make();
  const capture: Store.FollowUpEnvelope["send"] = {
    arguments: {threadId: base.child.thread.id, message: "Follow up", mode: "queue", clientRequestId: "stable-original-key"},
    limits: {runtimeMode: base.parent.thread.runtimeMode, interactionMode: base.parent.thread.interactionMode},
    origin: {schemaVersion: "t3.linked-message-origin/v1", environmentId: base.scope.environmentId,
      thread: {threadId: base.parent.thread.id}},
  };
  const input = {hasStableClientRequestId: true, scope: base.scope, environmentId: destination, capture, requestDigest: "a".repeat(64),
    targetCreatedAt: DateTime.formatIso(base.now), targetModes: capture.limits};
  const sent: OrchestratorMcpThreadSendResult = {threadId: base.child.thread.id, messageId: MessageId.make("exact-sent-message"),
    runId: RunId.make("exact-new-remote-run"), status: "starting", delivery: "queued"};
  const network = (result = sent) => Effect.gen(function* () {
    assert.isFalse(locked);
    sends++;
    const [row] = yield* store.pending;
    assert.isTrue(row?.launchStarted, "potential-effect marker must commit before SEND");
    assert.isNotNull(row?.taskId, "fresh task must commit before SEND");
    assert.notEqual(row?.taskId, oldTask.id);
    assert.equal(tasks.find((task) => task.id === row?.taskId)?.result, null);
    return result;
  });
  return {service, store, sql, input, oldTask, sent, network, make,
    tasks: () => tasks, counts: () => ({sends, withdrawals, stopDebt}), readCalls,
    supported: (value: boolean) => { supported = value; },
    bindFailure: (value: boolean) => { failBind = value; },
    withdrawal: (value: string) => { withdrawalStatus = value; },
    authenticate: (value: boolean) => { authenticated = value; },
    replaceLink: () => { generation = "replacement-generation"; },
    nextParentRun: () => { runs = runs.map((run) => ({...run, id: RunId.make("parent-wake-run"), ordinal: run.ordinal + 1})); },
    endParent: () => { runs = runs.map((run) => ({...run, status: "completed"})); },
    receipt: (intent: Store.FollowUpIntent, outcome: "accepted" | "rejected" | "unknown", result = sent) => {
      receipt = {version: FollowUp.receiptVersion, environmentId: destination, threadId: sent.threadId,
        clientRequestId: String(intent.envelope.send.arguments.clientRequestId), requestDigest: intent.envelope.requestDigest,
        outcome, sent: outcome === "accepted" ? result : null};
    },
  };
});

it.effect("reserves under the command transaction before SEND; binds exact new run and keeps old result immutable", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    assert.isFalse(intent.launchStarted);
    assert.equal(intent.envelope.previousRemoteRunId, f.oldTask.remoteChild!.runId);
    const result = yield* f.service.send(intent, () => f.network());
    assert.equal(result.taskId, intent.taskId);
    assert.equal(f.tasks().find((task) => task.id === intent.taskId)?.remoteChild?.runId, f.sent.runId);
    assert.deepEqual(f.tasks()[0], f.oldTask);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "recorded");
    const replay = (yield* f.service.prepare(f.input))!;
    assert.deepEqual(yield* f.service.send(replay, () => f.network()), result);
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("same key recovers original generation and changed message or steering payload refuses", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const original = (yield* f.service.prepare(f.input))!;
    f.replaceLink();
    assert.deepEqual(yield* f.service.prepare({...f.input, targetCreatedAt: "replaced-thread"}), original);
    for (const patch of [{message: "changed"}, {mode: "steer"}]) {
      const failed = yield* f.service.prepare({...f.input, capture: {...f.input.capture,
        arguments: {...f.input.capture.arguments, ...patch}}}).pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
    }
    assert.equal((yield* f.service.send(original, () => f.network()).pipe(Effect.result))._tag, "Failure");
    assert.equal(f.counts().sends, 0);
  }).pipe(Effect.provide(layer)));

it.effect("timeout or lost ACK survives restart as unknown and never redispatches SEND", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "lost ACK"}))))).pipe(Effect.result);
    const restarted = yield* f.make();
    yield* restarted.start;
    const pending = (yield* f.store.get(intent.commandId))!;
    assert.isTrue(pending.launchStarted);
    assert.equal(pending.status, "pending");
    yield* restarted.send(pending, () => f.network()).pipe(Effect.result);
    assert.equal(f.counts().sends, 1);
    const lookups = f.readCalls.filter((call) => call.tool === "t3_thread_send_receipt_lookup");
    assert.isAbove(lookups.length, 0);
    assert.deepEqual(lookups[0]?.arguments, {threadId: f.sent.threadId,
      clientRequestId: intent.envelope.send.arguments.clientRequestId, requestDigest: intent.envelope.requestDigest});
  }).pipe(Effect.provide(layer)));

it.effect("accepted binding write failure recovers the original exact receipt without SEND", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.sql`CREATE TRIGGER injected_accept_failure BEFORE UPDATE OF launched_json ON jones_remote_delegation_intents
      BEGIN SELECT RAISE(FAIL, 'injected accepted-response failure'); END`;
    assert.equal((yield* f.service.send(intent, () => f.network()).pipe(Effect.result))._tag, "Failure");
    yield* f.sql`DROP TRIGGER injected_accept_failure`;
    assert.isNull((yield* f.store.get(intent.commandId))?.launched);
    f.receipt(intent, "accepted");
    const restarted = yield* f.make();
    yield* restarted.start;
    assert.equal((yield* f.store.get(intent.commandId))?.launched?.messageId, f.sent.messageId);
    assert.equal(f.tasks().find((task) => task.id === intent.taskId)?.remoteChild?.runId, f.sent.runId);
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("old peers and missing core transaction owner fail before SEND", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const absent = yield* f.make(false);
    assert.equal((yield* absent.prepare(f.input).pipe(Effect.result))._tag, "Failure");
    assert.equal((yield* f.store.pending).length, 0);
    const intent = (yield* f.service.prepare(f.input))!;
    f.supported(false);
    assert.equal((yield* f.service.send(intent, () => f.network()).pipe(Effect.result))._tag, "Failure");
    assert.isFalse((yield* f.store.get(intent.commandId))?.launchStarted);
    assert.equal(f.counts().sends, 0);
  }).pipe(Effect.provide(layer)));

it.effect("Stop while unknown retains reservation, then exact receipt binds cancellation debt after restart", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "interrupted socket"}))))).pipe(Effect.result);
    assert.isTrue(yield* f.service.cancel(intent.taskId!, "Stopped"));
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    f.receipt(intent, "accepted");
    yield* (yield* f.make()).start;
    assert.isTrue(f.counts().stopDebt);
    assert.equal(f.tasks().find((task) => task.id === intent.taskId)?.result, "Stopped");
    assert.deepEqual(f.tasks()[0], f.oldTask);
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("definite exact refusal releases only new reservation; explicit steering allocates none", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    assert.isUndefined(yield* f.service.prepare({...f.input, capture: {...f.input.capture,
      arguments: {...f.input.capture.arguments, mode: "steer"}}}));
    assert.equal(f.tasks().length, 1);
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "unknown response"}))))).pipe(Effect.result);
    f.receipt(intent, "rejected");
    yield* (yield* f.make()).start;
    assert.equal((yield* f.store.get(intent.commandId))?.status, "denied");
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));

it.effect("startup cancels a committed unstarted reservation; ended parent or stale session never SENDs", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    f.endParent();
    yield* f.service.send(intent, () => f.network()).pipe(Effect.result);
    assert.equal(f.counts().sends, 0);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "denied");
    const g = yield* fixture;
    // Distinct immutable key in the same real database.
    const next = (yield* g.service.prepare({...g.input, capture: {...g.input.capture,
      arguments: {...g.input.capture.arguments, clientRequestId: "second-key"}}}))!;
    g.authenticate(false);
    assert.equal((yield* g.service.send(next, () => g.network()).pipe(Effect.result))._tag, "Failure");
    yield* (yield* g.make()).start;
    assert.equal((yield* g.store.get(next.commandId))?.status, "denied");
    assert.equal(g.counts().sends, 0);
  }).pipe(Effect.provide(layer)));

it.effect("legacy launch recovery excludes tagged follow-ups; corrupt tagged rows skip safely", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const legacy = yield* Store.make;
    yield* legacy.prepare(legacyCommand, {launch: true}, legacyEnvelope);
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.sql`INSERT INTO jones_remote_delegation_intents(command_id, request_json, envelope_json, status, created_at)
      VALUES ('corrupt-follow-up', '{}', '{"kind":"remote-followup/v1"}', 'pending', '2026-01-01T00:00:00Z')`;
    assert.deepEqual((yield* legacy.pending).map((row) => row.commandId), [legacyCommand]);
    assert.deepEqual((yield* f.store.pending).map((row) => row.commandId), [intent.commandId]);
    const [retained] = yield* f.sql<{n: number}>`SELECT count(*) AS n FROM jones_remote_delegation_intents`;
    assert.equal(retained?.n, 3);
    assert.equal((yield* f.store.get(CommandId.make("corrupt-follow-up")).pipe(Effect.result))._tag, "Failure");
  }).pipe(Effect.provide(layer)));

it.effect("Stop withdraws original held admission and restart retains debt until definite cancellation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(new OrchestratorMcpFailure({
      code: "remote_admission_pending", message: "held", remoteAdmission: {
        id: "original-admission", bindingDigest: "a".repeat(64), status: "held"}}))))).pipe(Effect.result);
    yield* f.service.cancel(intent.taskId!, "Stopped");
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    assert.equal(f.counts().withdrawals, 1);
    f.withdrawal("cancelled");
    yield* (yield* f.make()).start;
    assert.equal((yield* f.store.get(intent.commandId))?.status, "stopped");
    assert.equal(f.counts().sends, 1);
    const withdrawal = f.readCalls.find((call) => call.tool === "t3_remote_admission_cancel");
    assert.deepEqual(withdrawal?.arguments, {id: "original-admission", bindingDigest: "a".repeat(64)});
    assert.equal(withdrawal?.generation, intent.envelope.linkGeneration);
  }).pipe(Effect.provide(layer)));

it.effect("a late steer releases the reservation and cannot masquerade as a new task", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    const result = yield* f.service.send(intent, () => f.network({...f.sent, delivery: "steered",
      taskId: NodeId.make("untrusted-peer-task")}));
    assert.isUndefined(result.taskId);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "denied");
    assert.isUndefined(f.tasks().find((task) => task.id === intent.taskId)?.remoteChild);
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));

it.effect("an accepted historical run cannot be relabeled as the new follow-up", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    const result = yield* f.service.send(intent, () => f.network({...f.sent, runId: f.oldTask.remoteChild!.runId!})).pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    assert.isNotNull((yield* f.store.get(intent.commandId))?.launched);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    assert.isUndefined(f.tasks().find((task) => task.id === intent.taskId)?.remoteChild);
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));

it.effect("a missing stable key refuses before reservation instead of inventing replay identity", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    assert.equal((yield* f.service.prepare({...f.input, hasStableClientRequestId: false}).pipe(Effect.result))._tag, "Failure");
    assert.equal(f.tasks().length, 1);
    assert.equal((yield* f.store.pending).length, 0);
    assert.equal(f.counts().sends, 0);
  }).pipe(Effect.provide(layer)));

it.effect("a completion wake captures the current parent run while preserving the old child's run", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.nextParentRun();
    const intent = (yield* f.service.prepare(f.input))!;
    assert.equal(intent.envelope.request.parentRunId, "parent-wake-run");
    assert.equal(intent.envelope.previousRemoteRunId, f.oldTask.remoteChild!.runId);
    yield* f.service.send(intent, () => f.network());
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));

it.effect("an interrupted potentially effective SEND remains unknown through restart", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    const entered = yield* Deferred.make<void>();
    const sending = yield* f.service.send(intent, () => f.network().pipe(
      Effect.andThen(Deferred.succeed(entered, undefined)), Effect.andThen(Effect.never),
    )).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(sending);
    yield* (yield* f.make()).start;
    const stored = (yield* f.store.get(intent.commandId))!;
    assert.isTrue(stored.launchStarted);
    assert.isNull(stored.launched);
    assert.equal(stored.status, "pending");
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("bind command failure retains accepted evidence and retries only local binding", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    f.bindFailure(true);
    assert.equal((yield* f.service.send(intent, () => f.network()).pipe(Effect.result))._tag, "Failure");
    assert.equal((yield* f.store.get(intent.commandId))?.launched?.runId, f.sent.runId);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    f.bindFailure(false);
    yield* (yield* f.make()).start;
    assert.equal((yield* f.store.get(intent.commandId))?.status, "recorded");
    assert.equal(f.counts().sends, 1);
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));

it.effect("tagged envelope and request bytes stay immutable after reservation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    assert.equal((yield* f.sql`UPDATE jones_remote_delegation_intents SET request_json = '{}' WHERE command_id = ${intent.commandId}`
      .pipe(Effect.result))._tag, "Failure");
    assert.equal((yield* f.sql`UPDATE jones_remote_delegation_intents SET envelope_json = '{}' WHERE command_id = ${intent.commandId}`
      .pipe(Effect.result))._tag, "Failure");
    assert.deepEqual((yield* f.store.get(intent.commandId))?.envelope, intent.envelope);
  }).pipe(Effect.provide(layer)));

it.effect("downgraded peer after unknown SEND stays pending without receipt inference or replay", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "lost receipt"}))))).pipe(Effect.result);
    f.supported(false);
    yield* (yield* f.make()).start;
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    assert.equal(f.readCalls.filter((call) => call.tool === "t3_thread_send_receipt_lookup").length, 0);
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("an unknown original command fences a new-key replacement to the same child", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "unknown original effect"}))))).pipe(Effect.result);
    const replacement = yield* f.service.prepare({...f.input, capture: {...f.input.capture,
      arguments: {...f.input.capture.arguments, clientRequestId: "replacement-key"}}}).pipe(Effect.result);
    assert.equal(replacement._tag, "Failure");
    assert.equal(f.tasks().length, 2);
    assert.equal((yield* f.store.pending).length, 1);
    assert.equal(f.counts().sends, 1);
  }).pipe(Effect.provide(layer)));

it.effect("foreground status leaves a live unstarted reservation alone and reconciles only exact evidence", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const intent = (yield* f.service.prepare(f.input))!;
    assert.isNull((yield* f.service.refresh(intent.taskId!))?.result);
    assert.equal((yield* f.store.get(intent.commandId))?.status, "pending");
    yield* f.service.send(intent, () => f.network().pipe(Effect.andThen(Effect.fail(
      new OrchestratorMcpFailure({code: "orchestration_error", message: "unknown ACK"}))))).pipe(Effect.result);
    f.receipt(intent, "accepted");
    const refreshed = yield* f.service.refresh(intent.taskId!);
    assert.equal(refreshed?.remoteChild?.runId, f.sent.runId);
    assert.equal(f.counts().sends, 1);
    assert.deepEqual(f.tasks()[0], f.oldTask);
  }).pipe(Effect.provide(layer)));
