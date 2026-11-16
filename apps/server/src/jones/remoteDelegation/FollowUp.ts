import {
  CommandId, NodeId, ThreadId, OrchestratorMcpFailure, OrchestratorMcpThreadSendResult,
  type OrchestrationV2ServerCommand, EnvironmentId, type RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Links from "../../peer/PeerLinks.ts";
import * as Peers from "../../peer/PeerMcpClient.ts";
import { followUpOwnerRun } from "../../orchestration-v2/CommandPolicy.ts";
import { refuseUnsolicitedSupervision } from "../mcp/delegatedTaskSupervision/DelegatedTaskSupervision.ts";
import * as Store from "./IntentStore.ts";
import { runtimeModeRank, interactionModeRank } from "../../mcp/delegatedTaskModes.ts";
import { CancelRemoteAdmissionTool } from "../remoteAdmissionIntegration/tools.ts";

/** Supplied by the authoritative command owner. The callback owns the SAME
 * parent command lock and SQLite transaction as dispatch, which must not reacquire
 * that lock. Neither method may run provider/network side effects in the callback.
 */
export class FollowUpCommandOwner extends Context.Service<FollowUpCommandOwner, {
  readonly inParentTransaction: <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | OrchestratorMcpFailure, R>;
  readonly assertParentCaller: (scope: Store.FollowUpEnvelope["scope"], runId: RunId) => Effect.Effect<void, OrchestratorMcpFailure>;
  readonly dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<{
    readonly storedEvents: ReadonlyArray<{readonly event: {readonly type: string; readonly payload: unknown}}>;
  }, OrchestratorMcpFailure>;
}>()("jones/remoteDelegation/FollowUpCommandOwner") {}

export const receiptVersion = "jones.remote-followup-receipt/v1";
// Local strict negotiation until contracts owner adopts the minimal fragment.
export const FollowUpReceipt = Schema.Struct({
  version: Schema.Literal(receiptVersion),
  environmentId: EnvironmentId, threadId: ThreadId,
  clientRequestId: Schema.NonEmptyString, requestDigest: Schema.NonEmptyString,
  outcome: Schema.Literals(["accepted", "rejected", "unknown"]),
  sent: Schema.NullOr(OrchestratorMcpThreadSendResult),
});
const CapabilityReceipt = Schema.Struct({features: Schema.Record(Schema.String, Schema.Unknown)});
const pending = () => new OrchestratorMcpFailure({code: "orchestration_error",
  message: "Remote follow-up is durably pending exact receipt reconciliation. Do not resend or launch a replacement."});
const denied = (message: string) => new OrchestratorMcpFailure({code: "capability_denied", message});
type SendCapture = Store.FollowUpEnvelope["send"];

export const make = Effect.gen(function* () {
  const store = yield* Store.makeFollowUps;
  const threads = yield* Threads.ThreadManagementService;
  const links = yield* Links.PeerLinks;
  const peers = yield* Peers.PeerMcpClient;
  const ownerOption = yield* Effect.serviceOption(FollowUpCommandOwner);
  const owner = () => Option.isSome(ownerOption) ? Effect.succeed(ownerOption.value) :
    Effect.fail(denied("Durable follow-up command transactions are unavailable; nothing was sent."));
  const taskFor = (intent: Store.FollowUpIntent) => threads.getThreadRecords(intent.envelope.request.parentThreadId, ["subagents"]).pipe(
    Effect.map(({subagents}) => subagents.find((task) => task.id === intent.taskId)), Effect.mapError(pending));
  const originalLink = (intent: Store.FollowUpIntent) => Effect.gen(function* () {
    const generation = yield* links.currentGeneration(intent.envelope.environmentId).pipe(Effect.mapError(pending));
    if (Option.isNone(generation) || generation.value !== intent.envelope.linkGeneration) return yield* pending();
  });
  const finishReservation = (intent: Store.FollowUpIntent, reason: string) => Effect.gen(function* () {
    const port = yield* owner();
    yield* port.dispatch({type: "delegated_task.remote.complete", commandId: CommandId.make(`${intent.commandId}:release`),
      parentThreadId: intent.envelope.request.parentThreadId, taskId: intent.taskId!, status: "cancelled", result: reason});
    yield* store.finish(intent.commandId, "denied");
  });
  const bind = Effect.fn("RemoteFollowUp.bind")(function* (intent: Store.FollowUpIntent) {
    const port = yield* owner();
    return yield* port.inParentTransaction(intent.envelope.request.parentThreadId, Effect.gen(function* () {
      const current = yield* store.get(intent.commandId);
      if (current === undefined || current.taskId === null || current.launched === null) return yield* pending();
      const sent = current.launched;
      if (sent.threadId !== current.envelope.send.arguments.threadId) return yield* pending();
      if (sent.delivery === "steered") {
        if (current.status === "pending") yield* finishReservation(current, "The message steered an existing run; no new delegated task was started.");
        const {taskId: _remoteTaskId, ...steered} = sent;
        return steered;
      }
      if (sent.runId === current.envelope.previousRemoteRunId) return yield* pending();
      if (current.status !== "recorded") {
        if (current.status !== "pending") return yield* pending();
        yield* port.dispatch({type: "delegated_task.remote.bind", commandId: CommandId.make(`${intent.commandId}:bind`),
          parentThreadId: current.envelope.request.parentThreadId, taskId: current.taskId,
          remoteChild: {environmentId: current.envelope.environmentId, threadId: sent.threadId,
            label: current.envelope.label, runId: sent.runId}});
        // Core bind preserves a cancelled task's result and establishes stop debt.
        yield* store.finish(current.commandId, "recorded");
      }
      return {...sent, taskId: current.taskId};
    }));
  });
  const reconcile = Effect.fn("RemoteFollowUp.reconcile")(function* (intent: Store.FollowUpIntent) {
    if (intent.launched !== null) return yield* bind(intent);
    if (intent.status !== "pending") return yield* denied("This follow-up reservation was released or stopped; the original key cannot send again.");
    if (!intent.launchStarted) return yield* pending();
    yield* originalLink(intent);
    const capture = intent.envelope.send;
    const capabilities = yield* peers.call({environmentId: intent.envelope.environmentId,
      expectedGeneration: intent.envelope.linkGeneration, tool: "orchestrator_capabilities", arguments: {},
      limits: capture.limits, origin: capture.origin, success: CapabilityReceipt});
    if (capabilities.features.remoteFollowUpReceiptVersion !== receiptVersion) return yield* pending();
    const receipt = yield* Peers.lookupSendReceipt(peers, {
      environmentId: intent.envelope.environmentId, expectedGeneration: intent.envelope.linkGeneration,
      origin: capture.origin, limits: capture.limits,
      request: {threadId: ThreadId.make(String(capture.arguments.threadId)),
        clientRequestId: String(capture.arguments.clientRequestId), requestDigest: intent.envelope.requestDigest},
      success: FollowUpReceipt,
    });
    if (receipt.environmentId !== intent.envelope.environmentId || receipt.threadId !== capture.arguments.threadId ||
        receipt.clientRequestId !== capture.arguments.clientRequestId || receipt.requestDigest !== intent.envelope.requestDigest)
      return yield* pending();
    if (receipt.outcome === "accepted" && receipt.sent !== null) {
      const port = yield* owner();
      yield* port.inParentTransaction(intent.envelope.request.parentThreadId, store.accepted(intent.commandId, receipt.sent));
      return yield* bind((yield* store.get(intent.commandId))!);
    }
    if (receipt.outcome === "rejected" && receipt.sent === null) {
      const port = yield* owner();
      yield* port.inParentTransaction(intent.envelope.request.parentThreadId,
        finishReservation(intent, "The exact remote command was refused before acceptance."));
      return yield* denied("The exact remote command was refused before acceptance; its reservation was released.");
    }
    // Unknown/absent is never evidence of not applied, and never retries SEND.
    return yield* pending();
  });
  const prepare = Effect.fn("RemoteFollowUp.prepare")(function* (input: {
    readonly scope: McpInvocationScope; readonly environmentId: EnvironmentId;
    readonly capture: SendCapture; readonly requestDigest: string; readonly hasStableClientRequestId: boolean;
    readonly targetCreatedAt: string;
    readonly targetModes: {readonly runtimeMode: Store.FollowUpEnvelope["send"]["limits"]["runtimeMode"];
      readonly interactionMode: Store.FollowUpEnvelope["send"]["limits"]["interactionMode"]};
  }) {
    const {scope, environmentId, capture} = input;
    if (scope.thread === undefined) return undefined;
    const commandId = CommandId.make(`command:remote-followup-v1:${encodeURIComponent(scope.requestNamespace)}:${encodeURIComponent(environmentId)}:${encodeURIComponent(String(capture.arguments.clientRequestId))}`);
    const request = {environmentId, arguments: capture.arguments};
    const existing = yield* store.find(commandId, request);
    if (existing !== undefined) {
      const port = yield* owner();
      return yield* port.inParentTransaction(scope.thread.threadId, Effect.gen(function* () {
        yield* port.assertParentCaller(existing.envelope.scope, existing.envelope.request.parentRunId);
        return (yield* store.find(commandId, request))!;
      }));
    }
    if (capture.arguments.mode === "steer") return undefined;
    if (yield* store.pendingForTarget(scope.thread.threadId, environmentId, String(capture.arguments.threadId)))
      return yield* pending();
    const candidate = yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(Effect.mapError(pending));
    const isThere = (task: (typeof candidate.subagents)[number]) => task.origin === "app_owned" &&
      task.remoteChild?.environmentId === environmentId && task.remoteChild.threadId === capture.arguments.threadId;
    if (!candidate.subagents.some((task) => isThere(task) && task.result !== null)) return undefined;
    if (!input.hasStableClientRequestId) return yield* denied("Follow-ups require a stable clientRequestId before reservation; nothing was sent.");
    const port = yield* owner();
    return yield* port.inParentTransaction(scope.thread.threadId, Effect.gen(function* () {
      const replay = yield* store.find(commandId, request);
      if (replay !== undefined) {
        yield* port.assertParentCaller(replay.envelope.scope, replay.envelope.request.parentRunId);
        return replay;
      }
      const outstanding = yield* store.pendingForTarget(scope.thread!.threadId, environmentId, String(capture.arguments.threadId));
      if (outstanding !== undefined) return yield* pending();
      const parent = yield* threads.getThreadRecords(scope.thread!.threadId, ["runs", "subagents"]).pipe(Effect.mapError(pending));
      const run = followUpOwnerRun(parent.runs);
      if (parent.subagents.some((task) => isThere(task) && task.result === null)) return yield* pending();
      const previous = parent.subagents.filter(isThere).toSorted((a,b) => DateTime.toEpochMillis(b.updatedAt) - DateTime.toEpochMillis(a.updatedAt))[0];
      if (parent.thread.archivedAt !== null || parent.thread.deletedAt !== null || run?.rootNodeId == null ||
          run.providerInstanceId !== scope.thread!.providerInstanceId || !Threads.isActiveRun(run) ||
          previous?.result == null || previous.model === null || previous.driver !== "remote" || previous.remoteChild?.runId === undefined) return yield* denied("Follow-up requires the current owning parent run and its finished child; nothing was sent.");
      yield* port.assertParentCaller({environmentId: scope.environmentId, requestNamespace: scope.requestNamespace, thread: scope.thread!}, run.id);
      yield* refuseUnsolicitedSupervision(parent.thread, input.targetModes);
      const link = yield* links.get(environmentId).pipe(Effect.mapError(pending));
      const generation = yield* links.currentGeneration(environmentId).pipe(Effect.mapError(pending));
      if (Option.isNone(link) || Option.isNone(generation) || DateTime.isLessThanOrEqualTo(link.value.expiresAt, yield* DateTime.now)) return yield* denied("The original child link is unavailable; nothing was sent.");
      const envelope: Store.FollowUpEnvelope = {
        kind: "remote-followup/v1", scope: {environmentId: scope.environmentId, requestNamespace: scope.requestNamespace, thread: scope.thread!},
        environmentId, linkedAt: DateTime.formatIso(link.value.linkedAt), label: link.value.label,
        previousTaskId: previous.id, previousRemoteRunId: previous.remoteChild!.runId, linkGeneration: generation.value, requestDigest: input.requestDigest, targetCreatedAt: input.targetCreatedAt,
        send: {...capture, arguments: {...capture.arguments, expectedThreadCreatedAt: input.targetCreatedAt}},
        request: {type: "delegated_task.remote.request", commandId, parentThreadId: parent.thread.id,
          parentRunId: run.id, parentNodeId: run.rootNodeId, task: String(capture.arguments.message),
          ...(previous.title === null ? {} : {title: previous.title}), driver: previous.driver,
          modelSelection: previous.modelSelection ?? {instanceId: previous.providerInstanceId, model: previous.model}, completionWake: "always"},
      };
      yield* store.prepare(commandId, request, envelope);
      const recorded = yield* port.dispatch(envelope.request);
      const event = recorded.storedEvents.find(({event}) => event.type === "subagent.updated")?.event;
      const taskId = event !== undefined && typeof event.payload === "object" && event.payload !== null && "id" in event.payload && typeof event.payload.id === "string" ? NodeId.make(event.payload.id) : undefined;
      if (taskId === undefined || taskId === previous.id) return yield* pending();
      yield* store.reserved(commandId, taskId);
      return (yield* store.get(commandId))!;
    }));
  });
  const send = Effect.fn("RemoteFollowUp.send")(function* (intent: Store.FollowUpIntent,
    network: (capture: SendCapture, generation: string) => Effect.Effect<typeof OrchestratorMcpThreadSendResult.Type, OrchestratorMcpFailure>) {
    if (intent.launched !== null || intent.launchStarted || intent.status !== "pending") return yield* reconcile(intent);
    const port = yield* owner();
    yield* originalLink(intent);
    const capabilities = yield* peers.call({environmentId: intent.envelope.environmentId,
      expectedGeneration: intent.envelope.linkGeneration, tool: "orchestrator_capabilities", arguments: {},
      limits: intent.envelope.send.limits, origin: intent.envelope.send.origin, success: CapabilityReceipt});
    if (capabilities.features.remoteFollowUpSendVersion !== receiptVersion || capabilities.features.remoteFollowUpReceiptVersion !== receiptVersion)
      return yield* denied("The peer cannot fence follow-ups to the captured thread generation; nothing was sent. The reservation remains held.");
    const admitted = yield* port.inParentTransaction(intent.envelope.request.parentThreadId, Effect.gen(function* () {
      yield* originalLink(intent);
      const task = yield* taskFor(intent);
      const parent = yield* threads.getThreadRecords(intent.envelope.request.parentThreadId, ["runs"]).pipe(Effect.mapError(pending));
      const run = parent.runs.find((run) => run.id === intent.envelope.request.parentRunId);
      if (task === undefined || task.result !== null || run === undefined || !Threads.isActiveRun(run) ||
          parent.thread.archivedAt !== null || parent.thread.deletedAt !== null ||
          runtimeModeRank(intent.envelope.send.limits.runtimeMode) > runtimeModeRank(parent.thread.runtimeMode) ||
          interactionModeRank(intent.envelope.send.limits.interactionMode) > interactionModeRank(parent.thread.interactionMode)) {
        if (task !== undefined) yield* finishReservation(intent, "Follow-up cancelled before SEND.");
        return false;
      }
      yield* port.assertParentCaller(intent.envelope.scope, intent.envelope.request.parentRunId);
      return yield* store.beginSend(intent.commandId, intent.taskId!);
    }));
    if (!admitted) return yield* pending();
    // No retry/fork around SEND. The committed marker survives timeout, Stop,
    // crash, interrupted response and accepted-response persistence failure.
    const sent = yield* network(intent.envelope.send, intent.envelope.linkGeneration).pipe(
      Effect.tapError((error) => error.remoteAdmission === undefined ? Effect.void :
        port.inParentTransaction(intent.envelope.request.parentThreadId, store.admission(intent.commandId, error.remoteAdmission))));
    yield* port.inParentTransaction(intent.envelope.request.parentThreadId, store.accepted(intent.commandId, sent));
    return yield* bind((yield* store.get(intent.commandId))!);
  });
  const recoverOne = Effect.fn("RemoteFollowUp.recoverOne")(function* (intent: Store.FollowUpIntent) {
    if (!intent.launchStarted && intent.launched === null) {
      // A crashed pre-SEND reservation can be safely cancelled, never auto-sent.
      const port = yield* owner();
      yield* port.inParentTransaction(intent.envelope.request.parentThreadId, Effect.gen(function* () {
        const current = yield* store.get(intent.commandId);
        if (current?.status === "pending" && !current.launchStarted && current.launched === null)
          yield* finishReservation(current, "Recovered an unstarted follow-up reservation; nothing was sent.");
      }));
      return;
    }
    const task = yield* taskFor(intent);
    if (task?.result !== null && task !== undefined && intent.admission !== null && intent.launched === null) {
      yield* originalLink(intent);
      const capture = intent.envelope.send;
      const withdrawn = yield* peers.call({environmentId: intent.envelope.environmentId,
        expectedGeneration: intent.envelope.linkGeneration, tool: CancelRemoteAdmissionTool.name,
        arguments: {id: intent.admission.id, bindingDigest: intent.admission.bindingDigest},
        limits: capture.limits, origin: capture.origin, success: CancelRemoteAdmissionTool.successSchema});
      if (withdrawn.status === "cancelled" || withdrawn.status === "denied") {
        const port = yield* owner();
        yield* port.inParentTransaction(intent.envelope.request.parentThreadId, store.finish(intent.commandId, "stopped"));
        return;
      }
    }
    yield* reconcile(intent);
  });
  return {prepare, send, reconcile,
    // Foreground status can reconcile an uncertain receipt without replaying SEND
    // or treating a live, unstarted reservation as a startup orphan.
    refresh: Effect.fn("RemoteFollowUp.refresh")(function* (taskId: NodeId) {
      const row = yield* store.byTask(taskId);
      if (row === undefined) return undefined;
      if (row.status === "pending" && (row.launchStarted || row.launched !== null))
        yield* reconcile(row).pipe(Effect.ignore);
      return yield* taskFor(row);
    }),
    cancel: Effect.fn("RemoteFollowUp.cancel")(function* (taskId: NodeId, reason?: string) {
      const intent = yield* store.byTask(taskId);
      if (intent === undefined) return false;
      const port = yield* owner();
      yield* port.inParentTransaction(intent.envelope.request.parentThreadId, port.dispatch({type: "delegated_task.remote.complete",
        commandId: CommandId.make(`${intent.commandId}:cancel`), parentThreadId: intent.envelope.request.parentThreadId,
        taskId, status: "cancelled", result: reason ?? "Cancelled; exact remote reconciliation and stop debt remain pending."}));
      yield* recoverOne(intent).pipe(Effect.ignore);
      return true;
    }),
    start: store.pending.pipe(Effect.flatMap((rows) => Effect.forEach(rows, (row) => recoverOne(row).pipe(
      Effect.catchCause(() => Effect.logWarning("Remote follow-up remains pending exact reconciliation", {commandId: row.commandId}))), {discard: true}))),
  };
});
