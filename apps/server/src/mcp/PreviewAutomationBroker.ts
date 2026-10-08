import {
  PREVIEW_AUTOMATION_V1_OPERATIONS,
  PreviewAutomationClientDisconnectedError,
  PreviewAutomationControlInterruptedError,
  PreviewAutomationControlReason,
  PreviewAutomationExecutionError,
  SERVER_BROWSER_AUTOMATION_CLIENT_ID,
  PreviewAutomationInvalidSelectorError,
  PreviewAutomationMalformedResponseError,
  PreviewAutomationNoAvailableHostError,
  PreviewAutomationRemoteUnavailableError,
  PreviewAutomationRecordingTransferError,
  PreviewAutomationRecordingDesktopUpdateRequiredError,
  PreviewAutomationRecordingTooLargeError,
  PreviewAutomationRecordingDeadlineExpiredError,
  PreviewAutomationRequestQueueClosedError,
  PreviewAutomationResultTooLargeError,
  PreviewAutomationTabNotFoundError,
  PreviewAutomationTargetNotEditableError,
  PreviewAutomationTimeoutError,
  PreviewAutomationUnsupportedClientError,
  PreviewTabId,
  type PreviewAutomationError,
  type PreviewAutomationRuntimeIdentity,
  type PreviewAutomationOperation,
  type PreviewAutomationHost,
  type PreviewAutomationHostFocus,
  type PreviewAutomationResponse,
  type PreviewAutomationStreamEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import * as McpInvocationContext from "./McpInvocationContext.ts";

export interface PreviewAutomationInvokeInput {
  /** Preview tabs belong to a thread, so only thread callers reach the broker. */
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly operation: PreviewAutomationOperation;
  readonly input: unknown;
  readonly tabId?: PreviewTabId;
  readonly timeoutMs?: number;
  /** Background metadata reads must not change the agent's current tab. */
  readonly updateCurrentTab?: boolean;
  readonly failurePolicy?: "check_liveness" | "request_only";
  /** Capture the routed tab before another request changes the current assignment. */
  readonly onTargetTab?: (tabId: PreviewTabId | undefined) => void;
}

export interface PreviewAutomationRuntimeEvidence {
  readonly runtimeIdentity: PreviewAutomationRuntimeIdentity | null;
  readonly attachmentGeneration: number;
}

export interface PreviewAutomationConnectOptions {
  readonly resolveRuntimeEvidence?: (target: {
    readonly threadId: string;
    readonly tabId?: PreviewTabId;
  }) => Effect.Effect<PreviewAutomationRuntimeEvidence | null>;
  /**
   * New agent work goes to a preferred host before any desktop. The server's
   * own headless browser registers this way so a standalone environment keeps
   * browsing when every desktop disconnects.
   */
  readonly preferred?: boolean;
}

export class PreviewAutomationBroker extends Context.Service<
  PreviewAutomationBroker,
  {
    readonly connect: (
      host: PreviewAutomationHost,
      options?: PreviewAutomationConnectOptions,
    ) => Effect.Effect<Stream.Stream<PreviewAutomationStreamEvent>>;
    readonly focusHost: (host: PreviewAutomationHostFocus) => Effect.Effect<void>;
    readonly respond: (
      response: PreviewAutomationResponse,
    ) => Effect.Effect<void, PreviewAutomationError>;
    readonly invoke: <A = unknown>(
      request: PreviewAutomationInvokeInput,
    ) => Effect.Effect<A, PreviewAutomationError>;
  }
>()("t3/mcp/PreviewAutomationBroker") {}

interface ClientConnection {
  readonly clientId: string;
  readonly connectionId: string;
  readonly environmentId: PreviewAutomationHost["environmentId"];
  readonly runtimeIdentity: PreviewAutomationHost["runtimeIdentity"];
  readonly resolveRuntimeEvidence: PreviewAutomationConnectOptions["resolveRuntimeEvidence"];
  readonly supportedOperations: ReadonlySet<PreviewAutomationOperation>;
  readonly supportsSnapshotBarrier: boolean;
  readonly lastReplySequence: number;
  readonly focused: boolean;
  readonly preferred: boolean;
  readonly liveTabs: NonNullable<PreviewAutomationHostFocus["liveTabs"]>;
  readonly focusOrder: number;
  readonly queue: Queue.Queue<PreviewAutomationStreamEvent, Cause.Done>;
}

interface PendingRequest {
  readonly runtimeIdentity: ClientConnection["runtimeIdentity"];
  readonly runtimeEvidence?: PreviewAutomationRuntimeEvidence | null;
  readonly resolveRuntimeEvidence?: PreviewAutomationConnectOptions["resolveRuntimeEvidence"];
  readonly queue: ClientConnection["queue"];
  readonly deferred: Deferred.Deferred<unknown, PreviewAutomationError>;
  readonly context: PreviewAutomationRequestErrorContext;
  readonly requestSequence: number;
  readonly replySequenceAtDispatch: number;
  readonly dispatched: boolean;
}

/**
 * Live leases are independent of MCP credential expiry. Stream loss starts a
 * bounded affinity hold so reconnecting cannot silently migrate cookie/DOM state.
 */
interface HostAssignment {
  readonly clientId: ClientConnection["clientId"];
  readonly connectionId: ClientConnection["connectionId"];
  readonly queue: ClientConnection["queue"];
  readonly tabId?: PreviewTabId;
  readonly tabSequence?: number;
  readonly runtimeIdentity: ClientConnection["runtimeIdentity"];
  readonly affinityUntil?: number;
}

interface PreviewAutomationRequestErrorContext {
  readonly operation: PreviewAutomationOperation;
  readonly environmentId: McpInvocationContext.McpInvocationScope["environmentId"];
  readonly threadId: McpInvocationContext.McpThreadCaller["threadId"];
  readonly providerSessionId: string;
  readonly providerInstanceId: McpInvocationContext.McpThreadCaller["providerInstanceId"];
  readonly clientId: string;
  readonly connectionId: ClientConnection["connectionId"];
  readonly requestId: string;
  readonly tabId?: PreviewTabId;
  readonly timeoutMs: number;
  readonly outcome?: "unknown" | "reported" | "not_started";
  readonly selectorKind?: "locator" | "selector";
  readonly selectorLength?: number;
}

interface BrokerState {
  readonly clients: ReadonlyMap<string, ClientConnection>;
  readonly assignments: ReadonlyMap<string, HostAssignment>;
  readonly pending: ReadonlyMap<string, PendingRequest>;
  readonly requestSequence: number;
  readonly focusSequence: number;
  readonly replySequence: number;
  readonly tombstones: ReadonlyMap<
    string,
    PendingRequest & { readonly expiresAt: number; readonly reconciled?: boolean }
  >;
  readonly quarantine: ReadonlyMap<string, PendingRequest>;
  readonly liveness: ReadonlySet<string>;
}

const removeConnectionFromState = (
  current: BrokerState,
  clientId: string,
  queue: ClientConnection["queue"],
  now: number,
): { readonly state: BrokerState; readonly disconnected: ReadonlyArray<PendingRequest> } => {
  const clients = new Map(current.clients);
  const assignments = new Map(current.assignments);
  const pending = new Map(current.pending);
  const disconnected: PendingRequest[] = [];
  const tombstones = new Map(
    Array.from(current.tombstones).filter(([, entry]) => entry.expiresAt > now),
  );
  const quarantine = new Map(current.quarantine);
  if (current.clients.get(clientId)?.queue === queue) clients.delete(clientId);
  for (const [assignmentKey, assignment] of assignments) {
    if (assignment.queue === queue)
      assignments.set(assignmentKey, { ...assignment, affinityUntil: now + 30_000 });
  }
  for (const [requestId, entry] of pending) {
    if (entry.queue !== queue) continue;
    pending.delete(requestId);
    disconnected.push(entry);
    if (entry.dispatched) {
      tombstones.set(requestId, { ...entry, expiresAt: now + 60_000 });
      if (!readOnlyOperations.has(entry.context.operation)) quarantine.set(requestId, entry);
    }
  }
  while (tombstones.size > 256) tombstones.delete(tombstones.keys().next().value!);
  return {
    state: { ...current, clients, assignments, pending, tombstones, quarantine },
    disconnected,
  };
};

const selectorDiagnosticsFromInput = (
  input: unknown,
): Pick<PreviewAutomationRequestErrorContext, "selectorKind" | "selectorLength"> => {
  if (typeof input !== "object" || input === null) return {};
  if ("locator" in input && typeof input.locator === "string") {
    return { selectorKind: "locator", selectorLength: input.locator.length };
  }
  if ("selector" in input && typeof input.selector === "string") {
    return { selectorKind: "selector", selectorLength: input.selector.length };
  }
  return {};
};

const hostAssignmentKey = (scope: McpInvocationContext.McpThreadInvocationScope): string =>
  `${scope.environmentId}\u0000${scope.thread.providerSessionId}`;

const sameRuntime = (
  left: NonNullable<ClientConnection["runtimeIdentity"]>,
  right: NonNullable<ClientConnection["runtimeIdentity"]>,
) =>
  left.schemaVersion === right.schemaVersion &&
  left.runtimeKind === right.runtimeKind &&
  left.runtimeInstanceId === right.runtimeInstanceId &&
  left.appVersion === right.appVersion &&
  left.buildCommit === right.buildCommit;

const readOnlyOperations = new Set<PreviewAutomationOperation>([
  "status",
  "snapshot",
  "waitFor",
  "ping",
]);
const controlledMutations = new Set<PreviewAutomationOperation>([
  "click",
  "type",
  "press",
  "scroll",
  "evaluate",
]);
const sameTab = (
  left: PreviewAutomationRequestErrorContext,
  right: PreviewAutomationRequestErrorContext,
) =>
  left.environmentId === right.environmentId &&
  left.providerSessionId === right.providerSessionId &&
  left.tabId !== undefined &&
  left.tabId === right.tabId;

const isPreviewTabId = Schema.is(PreviewTabId);
const decodeControlReason = Schema.decodeUnknownOption(PreviewAutomationControlReason);

const readResultTabId = (result: unknown): PreviewTabId | null | undefined => {
  if (typeof result !== "object" || result === null || !("tabId" in result)) return undefined;
  const tabId = result.tabId;
  return tabId === null || isPreviewTabId(tabId) ? tabId : undefined;
};

const supportsOperation = (
  connection: ClientConnection,
  operation: PreviewAutomationOperation,
): boolean => connection.supportedOperations.has(operation);

type RemoteDetailKind = "null" | "array" | "object" | "string" | "number" | "boolean";

function remoteDetailKind(detail: unknown): RemoteDetailKind {
  if (detail === null) return "null";
  if (Array.isArray(detail)) return "array";
  switch (typeof detail) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    default:
      return "object";
  }
}

/** Enough for a browser's own error line, such as a refused connection and its URL. */
const MAX_REASON_CHARS = 500;

const classifyResponseError = (
  context: PreviewAutomationRequestErrorContext,
  error: NonNullable<PreviewAutomationResponse["error"]>,
): PreviewAutomationError => {
  const remoteDiagnostics = {
    outcome: error.outcome ?? "reported",
    remoteTag: error._tag,
    remoteMessageLength: error.message.length,
    ...(error.detail === undefined ? {} : { remoteDetailKind: remoteDetailKind(error.detail) }),
    cause: error,
  };
  switch (error._tag) {
    case "PreviewAutomationRecordingDesktopUpdateRequiredError":
      return new PreviewAutomationRecordingDesktopUpdateRequiredError({
        threadId: context.threadId,
        cause: error,
      });
    case "PreviewAutomationRecordingTooLargeError":
      return new PreviewAutomationRecordingTooLargeError({
        threadId: context.threadId,
        cause: error,
      });
    case "PreviewAutomationRecordingDeadlineExpiredError":
      return new PreviewAutomationRecordingDeadlineExpiredError({
        threadId: context.threadId,
        cause: error,
      });
    case "PreviewAutomationRecordingTransferError":
      return new PreviewAutomationRecordingTransferError({
        threadId: context.threadId,
        cause: error,
      });
    case "PreviewAutomationNoAvailableHostError":
      return new PreviewAutomationNoAvailableHostError({
        ...context,
        ...remoteDiagnostics,
      });
    case "PreviewAutomationUnsupportedClientError":
      return new PreviewAutomationUnsupportedClientError({
        ...context,
        ...remoteDiagnostics,
      });
    case "PreviewAutomationTabNotFoundError":
      return new PreviewAutomationTabNotFoundError({
        ...context,
        ...remoteDiagnostics,
      });
    case "PreviewAutomationTimeoutError":
      return new PreviewAutomationTimeoutError({
        ...context,
        ...remoteDiagnostics,
      });
    case "PreviewAutomationControlInterruptedError": {
      const reason = decodeControlReason(error.detail);
      return new PreviewAutomationControlInterruptedError({
        ...context,
        ...remoteDiagnostics,
        ...(Option.isSome(reason) ? { reason: reason.value } : {}),
      });
    }
    case "PreviewAutomationInvalidSelectorError": {
      const staleRef =
        typeof error.detail === "object" &&
        error.detail !== null &&
        "staleRef" in error.detail &&
        error.detail.staleRef === true;
      return new PreviewAutomationInvalidSelectorError({
        ...context,
        ...remoteDiagnostics,
        ...(staleRef ? { staleRef } : {}),
      });
    }
    case "PreviewAutomationTargetNotEditableError": {
      const detail =
        typeof error.detail === "object" && error.detail !== null ? error.detail : undefined;
      const remoteSelectorKind =
        detail &&
        "selectorKind" in detail &&
        (detail.selectorKind === "focused-element" ||
          detail.selectorKind === "locator" ||
          detail.selectorKind === "selector")
          ? detail.selectorKind
          : undefined;
      const remoteSelectorLength =
        detail &&
        "selectorLength" in detail &&
        typeof detail.selectorLength === "number" &&
        Number.isInteger(detail.selectorLength) &&
        detail.selectorLength >= 0
          ? detail.selectorLength
          : undefined;
      return new PreviewAutomationTargetNotEditableError({
        ...context,
        ...remoteDiagnostics,
        ...(remoteSelectorKind === undefined && context.selectorKind === undefined
          ? {}
          : { selectorKind: remoteSelectorKind ?? context.selectorKind }),
        ...(remoteSelectorLength === undefined && context.selectorLength === undefined
          ? {}
          : { selectorLength: remoteSelectorLength ?? context.selectorLength }),
      });
    }
    case "PreviewAutomationResultTooLargeError": {
      const detail =
        typeof error.detail === "object" && error.detail !== null ? error.detail : undefined;
      const maximumBytes =
        detail &&
        "maximumBytes" in detail &&
        typeof detail.maximumBytes === "number" &&
        Number.isInteger(detail.maximumBytes) &&
        detail.maximumBytes > 0
          ? detail.maximumBytes
          : undefined;
      return new PreviewAutomationResultTooLargeError({
        ...context,
        ...remoteDiagnostics,
        ...(maximumBytes === undefined ? {} : { maximumBytes }),
      });
    }
    case "PreviewAutomationUnavailableError":
      return new PreviewAutomationRemoteUnavailableError({
        ...context,
        ...remoteDiagnostics,
      });
    default:
      return new PreviewAutomationExecutionError({
        ...context,
        ...remoteDiagnostics,
        // The server's own browser writes these; other hosts' text stays out of the agent's context.
        ...(context.clientId === SERVER_BROWSER_AUTOMATION_CLIENT_ID
          ? { reason: error.message.slice(0, MAX_REASON_CHARS) }
          : {}),
      });
  }
};

export const make = Effect.gen(function* PreviewAutomationBrokerMake() {
  const crypto = yield* Crypto.Crypto;
  const state = yield* SynchronizedRef.make<BrokerState>({
    clients: new Map(),
    assignments: new Map(),
    pending: new Map(),
    requestSequence: 0,
    focusSequence: 0,
    replySequence: 0,
    tombstones: new Map(),
    quarantine: new Map(),
    liveness: new Set(),
  });

  const closeConnection = Effect.fn("PreviewAutomationBroker.closeConnection")(function* (
    queue: ClientConnection["queue"],
    disconnected: ReadonlyArray<PendingRequest>,
    completeStream = false,
  ) {
    if (completeStream) {
      // Discard this generation's commands and complete the RPC stream so a
      // responsive desktop can re-register after a timeout eviction.
      yield* Queue.clear(queue);
      yield* Queue.end(queue);
    } else {
      // Replaced registrations must not reconnect and displace their successor.
      yield* Queue.shutdown(queue);
    }
    yield* Effect.forEach(
      disconnected,
      ({ deferred, context, dispatched }) =>
        Deferred.fail(
          deferred,
          new PreviewAutomationClientDisconnectedError({
            ...context,
            outcome: dispatched ? "unknown" : "not_started",
          }),
        ),
      { discard: true },
    );
  });

  const disconnect = Effect.fn("PreviewAutomationBroker.disconnect")(function* (
    clientId: string,
    queue: ClientConnection["queue"],
    completeStream = false,
    replySequenceLimit?: number,
  ) {
    const now = yield* Clock.currentTimeMillis;
    yield* SynchronizedRef.modifyEffect(state, (current) => {
      // Retired generations were already closed by their replacement or eviction.
      if (
        current.clients.get(clientId)?.queue !== queue ||
        (replySequenceLimit !== undefined &&
          current.clients.get(clientId)!.lastReplySequence > replySequenceLimit)
      ) {
        return Effect.succeed([undefined, current] as const);
      }
      const removed = removeConnectionFromState(current, clientId, queue, now);
      return closeConnection(queue, removed.disconnected, completeStream).pipe(
        Effect.as([undefined, removed.state] as const),
      );
    });
  });

  const acquireConnection = Effect.fn("PreviewAutomationBroker.acquireConnection")(function* (
    host: PreviewAutomationHost,
    options: PreviewAutomationConnectOptions | undefined,
  ) {
    const clientId = host.clientId;
    const queue = yield* Queue.unbounded<PreviewAutomationStreamEvent, Cause.Done>();
    const connectionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    yield* Queue.offer(queue, { type: "connected", connectionId });
    const connection: ClientConnection = {
      clientId,
      connectionId,
      environmentId: host.environmentId,
      runtimeIdentity: host.runtimeIdentity,
      resolveRuntimeEvidence: options?.resolveRuntimeEvidence,
      supportedOperations: new Set<PreviewAutomationOperation>([
        ...(host.supportedOperations ?? PREVIEW_AUTOMATION_V1_OPERATIONS),
        ...(host.supportsPing ? ["ping" as const] : []),
      ]),
      supportsSnapshotBarrier: host.supportsSnapshotBarrier === true,
      lastReplySequence: 0,
      focused: false,
      preferred: options?.preferred ?? false,
      liveTabs: [],
      focusOrder: 0,
      queue,
    };
    const now = yield* Clock.currentTimeMillis;
    const registration = yield* SynchronizedRef.modify(state, (current) => {
      const previousConnection = current.clients.get(clientId);
      const removed = previousConnection
        ? removeConnectionFromState(current, clientId, previousConnection.queue, now)
        : { state: current, disconnected: [] };
      const clients = new Map(removed.state.clients);
      const focusSequence = removed.state.focusSequence + 1;
      const registeredConnection = { ...connection, focusOrder: focusSequence };
      clients.set(clientId, registeredConnection);
      return [
        {
          previousConnection,
          disconnected: removed.disconnected,
          registeredConnection,
        },
        { ...removed.state, clients, focusSequence },
      ] as const;
    });
    if (registration.previousConnection) {
      yield* closeConnection(registration.previousConnection.queue, registration.disconnected);
    }
    return registration.registeredConnection;
  });

  const connect: PreviewAutomationBroker["Service"]["connect"] = Effect.fn(
    "PreviewAutomationBroker.connect",
  )((host, options) =>
    Effect.succeed(
      Stream.unwrap(
        Effect.acquireRelease(acquireConnection(host, options), (connection) =>
          disconnect(connection.clientId, connection.queue),
        ).pipe(Effect.map((connection) => Stream.fromQueue(connection.queue))),
      ),
    ),
  );

  const focusHost: PreviewAutomationBroker["Service"]["focusHost"] = Effect.fn(
    "PreviewAutomationBroker.focusHost",
  )(function* (host) {
    yield* SynchronizedRef.update(state, (current) => {
      const currentHost = current.clients.get(host.clientId);
      if (
        !currentHost ||
        currentHost.environmentId !== host.environmentId ||
        currentHost.connectionId !== host.connectionId
      ) {
        return current;
      }
      const clients = new Map(current.clients);
      const focusSequence = host.focused ? current.focusSequence + 1 : current.focusSequence;
      clients.set(host.clientId, {
        ...currentHost,
        focused: host.focused,
        liveTabs: host.liveTabs ?? currentHost.liveTabs,
        focusOrder: host.focused ? focusSequence : currentHost.focusOrder,
      });
      return { ...current, clients, focusSequence };
    });
  });

  const respond: PreviewAutomationBroker["Service"]["respond"] = Effect.fn(
    "PreviewAutomationBroker.respond",
  )(function* (response) {
    const now = yield* Clock.currentTimeMillis;
    const pending = yield* SynchronizedRef.modify(state, (current) => {
      const entry =
        current.pending.get(response.requestId) ?? current.tombstones.get(response.requestId);
      if (
        !entry ||
        entry.context.clientId !== response.clientId ||
        entry.context.connectionId !== response.connectionId ||
        ("expiresAt" in entry && typeof entry.expiresAt === "number" && entry.expiresAt <= now)
      )
        return [undefined, current] as const;
      const next = new Map(current.pending);
      const tombstones = new Map(current.tombstones);
      const quarantine = new Map(current.quarantine);
      const clients = new Map(current.clients);
      next.delete(response.requestId);

      const validReply = response.ok || response.error !== undefined;
      if (validReply && response.error?.outcome !== "unknown")
        tombstones.delete(response.requestId);
      else {
        tombstones.set(response.requestId, {
          ...entry,
          expiresAt:
            "expiresAt" in entry && typeof entry.expiresAt === "number"
              ? entry.expiresAt
              : now + 60_000,
        });
        while (tombstones.size > 256) tombstones.delete(tombstones.keys().next().value!);
      }
      if (validReply && response.error?.outcome !== "unknown")
        quarantine.delete(response.requestId);
      if (
        (!validReply || response.error?.outcome === "unknown") &&
        !("reconciled" in entry && entry.reconciled === true) &&
        !readOnlyOperations.has(entry.context.operation)
      )
        quarantine.set(response.requestId, entry);
      // Snapshot orders only controlled actions dispatched before that snapshot.
      if (
        response.ok &&
        entry.context.operation === "snapshot" &&
        current.clients.get(response.clientId)?.connectionId === response.connectionId &&
        current.clients.get(response.clientId)?.supportsSnapshotBarrier === true
      ) {
        for (const [id, unknown] of quarantine) {
          if (
            sameTab(entry.context, unknown.context) &&
            entry.context.connectionId === unknown.context.connectionId &&
            entry.context.clientId === unknown.context.clientId &&
            unknown.requestSequence < entry.requestSequence &&
            controlledMutations.has(unknown.context.operation)
          ) {
            quarantine.delete(id);
            const tombstone = tombstones.get(id);
            if (tombstone) tombstones.set(id, { ...tombstone, reconciled: true });
          }
        }
      }
      const connection = clients.get(response.clientId);
      if (validReply && connection?.connectionId === response.connectionId) {
        clients.set(response.clientId, {
          ...connection,
          lastReplySequence: current.replySequence + 1,
        });
      }
      return [
        entry,
        {
          ...current,
          pending: next,
          tombstones,
          quarantine,
          clients,
          replySequence: current.replySequence + Number(validReply),
        },
      ] as const;
    });
    if (!pending) return;
    if ("expiresAt" in pending) {
      yield* Effect.logInfo("Late preview automation reply", {
        requestId: response.requestId,
        connectionId: response.connectionId,
        ok: response.ok,
        outcome: response.error?.outcome ?? "reported",
      });
      return;
    }
    if (response.ok) {
      const result = response.result;
      const currentEvidence =
        pending.resolveRuntimeEvidence === undefined
          ? undefined
          : yield* pending.resolveRuntimeEvidence({
              threadId: pending.context.threadId,
              ...(pending.context.tabId === undefined ? {} : { tabId: pending.context.tabId }),
            });
      const currentConnection = (yield* SynchronizedRef.get(state)).clients.get(
        pending.context.clientId,
      );
      const runtimeIdentity =
        currentConnection?.queue !== pending.queue
          ? null
          : pending.resolveRuntimeEvidence === undefined
            ? (pending.runtimeIdentity ?? null)
            : currentEvidence !== null &&
                currentEvidence !== undefined &&
                currentEvidence.attachmentGeneration ===
                  pending.runtimeEvidence?.attachmentGeneration &&
                currentEvidence.runtimeIdentity?.runtimeInstanceId ===
                  pending.runtimeEvidence?.runtimeIdentity?.runtimeInstanceId
              ? (pending.runtimeEvidence?.runtimeIdentity ?? null)
              : null;
      yield* Deferred.succeed(
        pending.deferred,
        pending.context.operation === "status" &&
          typeof result === "object" &&
          result !== null &&
          !Array.isArray(result)
          ? {
              ...result,
              selectedClient: {
                clientId: pending.context.clientId,
                connectionId: pending.context.connectionId,
                requestId: pending.context.requestId,
                completedAt: DateTime.formatIso(yield* DateTime.now),
                runtimeIdentity,
              },
            }
          : result,
      );
    } else {
      yield* Deferred.fail(
        pending.deferred,
        response.error
          ? classifyResponseError(pending.context, response.error)
          : new PreviewAutomationMalformedResponseError({ ...pending.context, outcome: "unknown" }),
      );
    }
  });

  const checkLiveness = Effect.fn("PreviewAutomationBroker.checkLiveness")(function* (
    connection: ClientConnection,
    expired: PendingRequest,
  ) {
    const ping = yield* Deferred.make<unknown, PreviewAutomationError>();
    const selected = yield* SynchronizedRef.modify(state, (current) => {
      const host = current.clients.get(connection.clientId);
      if (
        host?.queue !== connection.queue ||
        host.lastReplySequence > expired.replySequenceAtDispatch ||
        current.liveness.has(connection.connectionId)
      )
        return [undefined, current] as const;
      const liveness = new Set(current.liveness);
      liveness.add(connection.connectionId);
      const requestId = `preview-${current.requestSequence}`;
      const pending = new Map(current.pending);
      if (supportsOperation(host, "ping"))
        pending.set(requestId, {
          ...expired,
          runtimeIdentity: host.runtimeIdentity,
          deferred: ping,
          requestSequence: current.requestSequence,
          context: { ...expired.context, requestId, operation: "ping", timeoutMs: 1000 },
        });
      return [
        { requestId, supported: supportsOperation(host, "ping") },
        { ...current, liveness, pending, requestSequence: current.requestSequence + 1 },
      ] as const;
    });
    if (!selected) return;
    yield* Effect.gen(function* () {
      if (selected.supported) {
        yield* Queue.offer(connection.queue, {
          type: "request",
          connectionId: connection.connectionId,
          request: {
            requestId: selected.requestId,
            threadId: expired.context.threadId,
            operation: "ping",
            input: {},
            timeoutMs: 1000,
          },
        });
        yield* Deferred.await(ping).pipe(
          Effect.timeoutOption(1000),
          Effect.orElseSucceed(() => Option.none()),
        );
      }
      const live = yield* SynchronizedRef.get(state);
      if (
        live.clients.get(connection.clientId)?.queue === connection.queue &&
        live.clients.get(connection.clientId)!.lastReplySequence <= expired.replySequenceAtDispatch
      )
        yield* disconnect(
          connection.clientId,
          connection.queue,
          true,
          expired.replySequenceAtDispatch,
        );
    }).pipe(
      Effect.ensuring(
        SynchronizedRef.update(state, (current) => {
          const liveness = new Set(current.liveness);
          liveness.delete(connection.connectionId);
          const pending = new Map(current.pending);
          pending.delete(selected.requestId);
          return { ...current, liveness, pending };
        }),
      ),
    );
  });

  const invoke = Effect.fn("PreviewAutomationBroker.invoke")(function* <A = unknown>(
    input: Parameters<PreviewAutomationBroker["Service"]["invoke"]>[0],
  ): Effect.fn.Return<A, PreviewAutomationError> {
    const now = yield* Clock.currentTimeMillis;
    const deferred = yield* Deferred.make<unknown, PreviewAutomationError>();
    type Route =
      | {
          connection: ClientConnection;
          requestId: string;
          requestContext: PreviewAutomationRequestErrorContext;
          requestSequence: number;
        }
      | { blocked: PreviewAutomationRequestErrorContext }
      | { reconnecting: true }
      | undefined;
    const route = yield* SynchronizedRef.modify(state, (current): readonly [Route, BrokerState] => {
      const assignments = new Map(
        Array.from(current.assignments).filter(
          ([, assignment]) =>
            assignment.affinityUntil === undefined || assignment.affinityUntil > now,
        ),
      );
      const assignmentKey = hostAssignmentKey(input.scope);
      const assigned = assignments.get(assignmentKey);
      const candidate = assigned ? current.clients.get(assigned.clientId) : undefined;
      const assignedConnection =
        candidate &&
        (candidate.queue === assigned?.queue ||
          (assigned?.runtimeIdentity !== undefined &&
            candidate.runtimeIdentity !== undefined &&
            sameRuntime(assigned.runtimeIdentity, candidate.runtimeIdentity)))
          ? candidate
          : undefined;
      const hasLiveAssignment = assignedConnection?.environmentId === input.scope.environmentId;
      // Affinity prevents cookie and DOM state from silently migrating during reconnect.
      const ownsTargetTab = (host: ClientConnection, visibleOnly = false) =>
        host.liveTabs.some(
          (tab) =>
            tab.threadId === input.scope.thread.threadId &&
            (!visibleOnly || tab.visible === true) &&
            (input.tabId === undefined || tab.tabId === input.tabId),
        );
      const connection =
        hasLiveAssignment && supportsOperation(assignedConnection, input.operation)
          ? assignedConnection
          : hasLiveAssignment
            ? undefined
            : assigned
              ? undefined
              : Array.from(current.clients.values())
                  .filter(
                    (host) =>
                      host.environmentId === input.scope.environmentId &&
                      supportsOperation(host, input.operation),
                  )
                  .sort(
                    (left, right) =>
                      Number(input.tabId !== undefined && ownsTargetTab(right)) -
                        Number(input.tabId !== undefined && ownsTargetTab(left)) ||
                      Number(right.preferred) - Number(left.preferred) ||
                      Number(ownsTargetTab(right, true)) - Number(ownsTargetTab(left, true)) ||
                      Number(ownsTargetTab(right)) - Number(ownsTargetTab(left)) ||
                      Number(right.focused) - Number(left.focused) ||
                      right.focusOrder - left.focusOrder,
                  )[0];
      if (!connection) {
        if (!assigned) assignments.delete(assignmentKey);
        return [
          assigned && !hasLiveAssignment ? { reconnecting: true as const } : undefined,
          { ...current, assignments },
        ] as const;
      }
      // The environment host may install Chromium on its first open (up to
      // ten minutes). Keep that request alive without replaying its effects.
      const timeoutMs =
        input.timeoutMs ?? (input.operation === "open" && connection.preferred ? 660_000 : 15_000);
      const canReuseAssignedTab =
        assigned !== undefined &&
        (assigned.queue === connection.queue ||
          connection.liveTabs.some(
            (tab) => tab.tabId === assigned.tabId && tab.threadId === input.scope.thread.threadId,
          ));
      assignments.set(assignmentKey, {
        clientId: connection.clientId,
        connectionId: connection.connectionId,
        queue: connection.queue,
        runtimeIdentity:
          connection.resolveRuntimeEvidence === undefined
            ? connection.runtimeIdentity
            : assigned?.runtimeIdentity,
        ...(assigned?.affinityUntil === undefined ? {} : { affinityUntil: assigned.affinityUntil }),
        ...(canReuseAssignedTab && assigned.tabId !== undefined ? { tabId: assigned.tabId } : {}),
        ...(canReuseAssignedTab && assigned.tabSequence !== undefined
          ? { tabSequence: assigned.tabSequence }
          : {}),
      });

      const requestSequence = current.requestSequence;
      const requestId = `preview-${requestSequence}`;
      const tabId = input.tabId ?? (canReuseAssignedTab ? assigned.tabId : undefined);
      const selectorDiagnostics = selectorDiagnosticsFromInput(input.input);
      const context: PreviewAutomationRequestErrorContext = {
        operation: input.operation,
        environmentId: input.scope.environmentId,
        threadId: input.scope.thread.threadId,
        providerSessionId: input.scope.thread.providerSessionId,
        providerInstanceId: input.scope.thread.providerInstanceId,
        clientId: connection.clientId,
        connectionId: connection.connectionId,
        requestId,
        ...(tabId === undefined ? {} : { tabId }),
        timeoutMs,
        ...selectorDiagnostics,
      };
      const blocked =
        !readOnlyOperations.has(input.operation) &&
        Array.from(current.quarantine.values()).some(
          (entry) =>
            entry.context.providerSessionId === context.providerSessionId &&
            entry.context.environmentId === context.environmentId &&
            (entry.context.tabId === undefined || entry.context.tabId === context.tabId),
        );
      if (blocked) return [{ blocked: context }, current] as const;
      const pending = new Map(current.pending);
      pending.set(requestId, {
        queue: connection.queue,
        runtimeIdentity: connection.runtimeIdentity,
        deferred,
        context,
        requestSequence,
        replySequenceAtDispatch: current.replySequence,
        dispatched: false,
      });
      return [
        { connection, requestId, requestContext: context, requestSequence },
        { ...current, assignments, pending, requestSequence: current.requestSequence + 1 },
      ] as const;
    });
    if (!route || "reconnecting" in route) {
      return yield* new PreviewAutomationNoAvailableHostError({
        operation: input.operation,
        environmentId: input.scope.environmentId,
        threadId: input.scope.thread.threadId,
        providerSessionId: input.scope.thread.providerSessionId,
        providerInstanceId: input.scope.thread.providerInstanceId,
        reconnecting: route !== undefined,
      });
    }
    if ("blocked" in route)
      return yield* new PreviewAutomationTimeoutError({
        ...route.blocked,
        outcome: "not_started",
        unreconciled: true,
      });
    const { connection, requestId, requestContext, requestSequence } = route;
    const { timeoutMs } = requestContext;
    input.onTargetTab?.(requestContext.tabId);
    const removePending = SynchronizedRef.update(state, (next) => {
      if (!next.pending.has(requestId)) return next;
      const pending = new Map(next.pending);
      pending.delete(requestId);
      return { ...next, pending };
    });
    const awaitResponse = Effect.fn("PreviewAutomationBroker.awaitResponse")(function* () {
      const offered = yield* SynchronizedRef.modifyEffect(state, (current) => {
        // A route can outlive its generation while another request evicts it.
        // Serialize the live-generation check and offer with queue closure.
        if (
          current.clients.get(connection.clientId)?.queue !== connection.queue ||
          !current.pending.has(requestId)
        ) {
          return Effect.succeed([false, current] as const);
        }
        return Effect.gen(function* () {
          const runtimeEvidence =
            connection.resolveRuntimeEvidence === undefined
              ? undefined
              : yield* connection.resolveRuntimeEvidence({
                  threadId: requestContext.threadId,
                  ...(requestContext.tabId === undefined ? {} : { tabId: requestContext.tabId }),
                });
          const assignments = new Map(current.assignments);
          const assignmentKey = hostAssignmentKey(input.scope);
          const assignment = assignments.get(assignmentKey);
          if (
            connection.resolveRuntimeEvidence !== undefined &&
            assignment?.runtimeIdentity !== undefined &&
            (runtimeEvidence?.runtimeIdentity == null ||
              !sameRuntime(assignment.runtimeIdentity, runtimeEvidence.runtimeIdentity))
          ) {
            const now = yield* Clock.currentTimeMillis;
            assignments.set(assignmentKey, {
              ...assignment,
              affinityUntil: assignment.affinityUntil ?? now + 30_000,
            });
            return [false, { ...current, assignments }] as const;
          }
          if (assignment && runtimeEvidence?.runtimeIdentity != null) {
            const { affinityUntil: _expiredHold, ...liveAssignment } = assignment;
            assignments.set(assignmentKey, {
              ...liveAssignment,
              runtimeIdentity: runtimeEvidence.runtimeIdentity,
            });
          }
          const dispatchAt = yield* Clock.currentTimeMillis;
          const remainingTimeoutMs = Math.max(1, timeoutMs - (dispatchAt - now));
          if (dispatchAt - now >= timeoutMs) return [false, current] as const;
          const offered = yield* Queue.offer(connection.queue, {
            type: "request",
            connectionId: connection.connectionId,
            request: {
              requestId,
              threadId: input.scope.thread.threadId,
              tabId: requestContext.tabId,
              tabIdExplicit: input.tabId !== undefined,
              agentSessionId: hostAssignmentKey(input.scope),
              operation: input.operation,
              input: input.input,
              timeoutMs: remainingTimeoutMs,
            },
          }).pipe(
            Effect.map((offered) => {
              const pending = new Map(current.pending);
              const entry = pending.get(requestId)!;
              if (offered)
                pending.set(requestId, {
                  ...entry,
                  runtimeIdentity:
                    connection.resolveRuntimeEvidence === undefined
                      ? entry.runtimeIdentity
                      : (runtimeEvidence?.runtimeIdentity ?? undefined),
                  ...(runtimeEvidence === undefined ? {} : { runtimeEvidence }),
                  ...(connection.resolveRuntimeEvidence === undefined
                    ? {}
                    : { resolveRuntimeEvidence: connection.resolveRuntimeEvidence }),
                  dispatched: true,
                  replySequenceAtDispatch: current.replySequence,
                });
              return [offered, { ...current, pending, assignments }] as const;
            }),
          );
          return offered;
        });
      });
      if (!offered) {
        const completion = yield* Deferred.poll(deferred);
        if (Option.isSome(completion)) {
          return (yield* completion.value) as A;
        }
        return yield* new PreviewAutomationRequestQueueClosedError({
          ...requestContext,
          outcome: "not_started",
        });
      }
      const awaitStart = yield* Clock.currentTimeMillis;
      const result = yield* Deferred.await(deferred).pipe(
        Effect.timeoutOption(Math.max(1, timeoutMs - (awaitStart - now))),
      );
      return yield* Option.match(result, {
        onNone: () =>
          Effect.gen(function* () {
            const expiredAt = yield* Clock.currentTimeMillis;
            const expired = yield* SynchronizedRef.modify(state, (current) => {
              const entry = current.pending.get(requestId);
              if (!entry) return [undefined, current] as const;
              const pending = new Map(current.pending);
              pending.delete(requestId);
              const tombstones = new Map(
                Array.from(current.tombstones).filter(([, value]) => value.expiresAt > expiredAt),
              );
              tombstones.set(requestId, { ...entry, expiresAt: expiredAt + 60_000 });
              while (tombstones.size > 256) tombstones.delete(tombstones.keys().next().value!);
              const quarantine = new Map(current.quarantine);
              if (!readOnlyOperations.has(input.operation)) quarantine.set(requestId, entry);
              return [entry, { ...current, pending, tombstones, quarantine }] as const;
            });
            if (expired && input.failurePolicy !== "request_only")
              yield* checkLiveness(connection, expired).pipe(Effect.forkDetach);
            return yield* new PreviewAutomationTimeoutError({
              ...requestContext,
              outcome: "unknown",
            });
          }),
        onSome: (value) => Effect.succeed(value as A),
      });
    });
    const result = yield* awaitResponse().pipe(Effect.ensuring(removePending));
    if (input.updateCurrentTab === false) return result;
    const responseTabId = readResultTabId(result);
    const resultTabId = responseTabId === undefined ? input.tabId : responseTabId;
    if (resultTabId === undefined) return result;
    const assignmentKey = hostAssignmentKey(input.scope);
    yield* SynchronizedRef.update(state, (current) => {
      const assignment = current.assignments.get(assignmentKey);
      if (
        !assignment ||
        assignment.connectionId !== connection.connectionId ||
        assignment.queue !== connection.queue ||
        (assignment.tabSequence ?? -1) > requestSequence
      ) {
        return current;
      }
      const assignments = new Map(current.assignments);
      if (resultTabId === null) {
        const { tabId: _tabId, ...withoutTabId } = assignment;
        assignments.set(assignmentKey, { ...withoutTabId, tabSequence: requestSequence });
      } else {
        assignments.set(assignmentKey, {
          ...assignment,
          ...(resultTabId === undefined ? {} : { tabId: resultTabId }),
          tabSequence: requestSequence,
        });
      }
      return { ...current, assignments };
    });
    return result;
  });

  return PreviewAutomationBroker.of({ connect, focusHost, respond, invoke });
}).pipe(Effect.withSpan("PreviewAutomationBroker.make"));

export const layer = Layer.effect(PreviewAutomationBroker, make);
