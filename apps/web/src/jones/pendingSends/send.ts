import { AsyncResult } from "effect/reactivity";
import type { PreparedThreadTurn, StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import { pendingSends, type createPendingSendStore } from "./store";

export async function sendPendingTurn<A, P, D>(args: {
  environmentId: EnvironmentId;
  input: StartThreadTurnInput;
  prepare: () => Promise<AtomCommandResult<PreparedThreadTurn, P>>;
  dispatch: (prepared: PreparedThreadTurn) => Promise<AtomCommandResult<A, D>>;
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
  const send = async () => {
    const entry = store.get(environmentId, input.threadId, input.message.messageId);
    if (!entry || entry.inFlight || entry.delivery === "accepted") return;
    store.issuing(key);
    const result = await args.dispatch(prepared.value);
    store.finish(key, result._tag === "Success");
    return result;
  };
  store.prepared(key, prepared.value, send);
  store.issuing(key);
  const result = await args.dispatch(prepared.value);
  store.finish(key, result._tag === "Success");
  return result;
}
