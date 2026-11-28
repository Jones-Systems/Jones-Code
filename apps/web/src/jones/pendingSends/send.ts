import { AsyncResult } from "effect/reactivity";
import {
  AccountAutoRoutingSessionError,
  type PreparedThreadTurn,
  type StartThreadTurnInput,
} from "@t3tools/client-runtime/operations";
import {
  settlePromise,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { readPreparedThreadTurnDeliveryEvidence } from "@t3tools/client-runtime/jones/deliveryEvidence";
import { pendingSends, type createPendingSendStore } from "./store";

function bindPreparedTurn<A, D>(args: {
  store: ReturnType<typeof createPendingSendStore>;
  environmentId: EnvironmentId;
  threadId: ThreadId;
  messageId: MessageId;
  key: string;
  prepared: PreparedThreadTurn;
  dispatch: (prepared: PreparedThreadTurn) => Promise<AtomCommandResult<A, D>>;
}) {
  const { store, key, prepared } = args;
  const send = async (firstAttempt: boolean) => {
    store.issuing(key);
    const result = await args.dispatch(prepared);
    if (result._tag === "Success") {
      store.finish(key, "accepted");
    } else {
      const error = squashAtomCommandFailure(result);
      // A session refusing a retry cannot settle an earlier attempt.
      store.finish(
        key,
        firstAttempt && error instanceof AccountAutoRoutingSessionError
          ? "rejected"
          : readPreparedThreadTurnDeliveryEvidence(error, prepared),
      );
    }
    return result;
  };
  store.prepared(key, prepared, async () => {
    const entry = store.get(args.environmentId, args.threadId, args.messageId);
    if (!entry || entry.inFlight || entry.delivery !== "uncertain") return;
    return send(false);
  });
  return send;
}

export function bindRecoveredPendingTurn<A, D>(args: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  messageId: MessageId;
  key: string;
  prepared: PreparedThreadTurn;
  dispatch: (prepared: PreparedThreadTurn) => Promise<AtomCommandResult<A, D>>;
}) {
  bindPreparedTurn({ ...args, store: pendingSends });
}

export async function sendPendingTurn<A, P, D>(args: {
  environmentId: EnvironmentId;
  input: StartThreadTurnInput;
  prepare: () => Promise<AtomCommandResult<PreparedThreadTurn, P>>;
  dispatch: (prepared: PreparedThreadTurn) => Promise<AtomCommandResult<A, D>>;
  beforeDispatch?: (prepared: PreparedThreadTurn) => Promise<void>;
  store?: ReturnType<typeof createPendingSendStore>;
}): Promise<AtomCommandResult<A, P | D>> {
  const { environmentId, input } = args;
  const store = args.store ?? pendingSends;
  const createdAt = input.createdAt ?? new Date().toISOString();
  const key = store.admit(environmentId, input.threadId, {
    id: input.message.messageId,
    role: "user",
    text: input.message.text,
    runId: null,
    createdAt,
    updatedAt: createdAt,
    streaming: false,
    ...(input.dispatchMode === "queue" ? { inputIntent: "queued_turn" as const } : {}),
  });
  const prepared = await args.prepare();
  if (prepared._tag === "Failure") {
    store.finish(key, false);
    return AsyncResult.failure<A, P | D>(prepared.cause);
  }
  const beforeDispatch = args.beforeDispatch;
  if (beforeDispatch) {
    const saved = await settlePromise(() => beforeDispatch(prepared.value));
    if (saved._tag === "Failure") {
      store.failBeforeDispatch(environmentId, input.threadId, input.message.messageId);
      return AsyncResult.failure<A, P | D>(saved.cause);
    }
    store.retainDurably(key);
  }
  const firstAttempt = !store.get(environmentId, input.threadId, input.message.messageId)?.issued;
  const send = bindPreparedTurn({
    store,
    environmentId,
    threadId: input.threadId,
    messageId: input.message.messageId,
    key,
    prepared: prepared.value,
    dispatch: args.dispatch,
  });
  return send(firstAttempt);
}
