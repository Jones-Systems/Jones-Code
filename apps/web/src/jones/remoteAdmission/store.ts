import type {
  RemoteAdmissionDecision,
  RemoteAdmissionPolicyUpdate,
  RemoteAdmissionRecord,
  RemoteAdmissionSnapshot,
} from "@t3tools/contracts";
import {
  normalizeRemoteAdmissionError,
  type RemoteAdmissionFailureKind,
} from "@t3tools/client-runtime/jones/remote-admission";

export interface AdmissionState {
  readonly snapshot: RemoteAdmissionSnapshot | null;
  readonly loadFailed: boolean;
  readonly loadFailure: RemoteAdmissionFailureKind | null;
  readonly loading: boolean;
  readonly busy: boolean;
  readonly error: string | null;
  readonly unconfirmed: ReadonlySet<string>;
  readonly atLimit: boolean;
}

type Request =
  | { readonly action: "snapshot" }
  | { readonly action: "policy"; readonly input: RemoteAdmissionPolicyUpdate }
  | { readonly action: "decide"; readonly input: RemoteAdmissionDecision };
type Response = RemoteAdmissionSnapshot | RemoteAdmissionRecord | { readonly count: number };

export const admissionBindingKey = (record: Pick<RemoteAdmissionRecord, "id" | "bindingDigest">) =>
  JSON.stringify([record.id, record.bindingDigest]);

export function requestBlockedReason(state: AdmissionState, bindingKey: string) {
  return state.busy
    ? "A decision or settings change is in progress."
    : state.loadFailed
      ? "Owner review is unavailable. Refresh server state before deciding."
      : state.loading
        ? "Refreshing owner review. Wait for current server state before deciding."
        : state.unconfirmed.has(bindingKey)
          ? "This decision outcome is unconfirmed. Await server confirmation; do not resend."
          : null;
}

export function actionableAdmissions(state: AdmissionState) {
  if (state.loadFailure === "unauthorized" || state.loadFailure === "unsupported") return [];
  return (
    state.snapshot?.pending.filter(
      (record) => record.status === "held" && !state.unconfirmed.has(admissionBindingKey(record)),
    ) ?? []
  );
}

export function admissionAttention(state: AdmissionState) {
  if (state.loadFailure === "unauthorized" || state.loadFailure === "unsupported")
    return { held: 0, unknown: false, capped: false };
  return {
    held: actionableAdmissions(state).length,
    unknown: state.loadFailed || !state.snapshot,
    capped: state.atLimit,
  };
}

export function createAdmissionStore(request: (input: Request) => Promise<Response>) {
  let state: AdmissionState = {
    snapshot: null,
    loadFailed: false,
    loadFailure: null,
    loading: false,
    busy: false,
    error: null,
    unconfirmed: new Set(),
    atLimit: false,
  };
  const listeners = new Set<() => void>();
  const decisions = new Map<string, RemoteAdmissionRecord>();
  let reading: Promise<void> | null = null;
  let epoch = 0;
  let refreshFailures = 0;
  let retryAt = 0;
  const publish = (patch: Partial<AdmissionState>) => {
    state = { ...state, ...patch };
    for (const listener of listeners) listener();
  };
  const accept = (snapshot: RemoteAdmissionSnapshot) => ({
    ...snapshot,
    pending: snapshot.pending
      .map((record) =>
        record.status === "held" ? (decisions.get(admissionBindingKey(record)) ?? record) : record,
      )
      .filter((record) => record.status !== "denied"),
  });
  const refresh = (): Promise<void> => {
    if (reading) return reading;
    if (state.busy) return Promise.resolve();
    const started = epoch;
    publish({ loading: true });
    reading = (async () => {
      try {
        const result = await request({ action: "snapshot" });
        if (!("pending" in result)) throw new Error("Unexpected snapshot response");
        if (started === epoch) {
          refreshFailures = 0;
          retryAt = 0;
          const unconfirmed = new Set(state.unconfirmed);
          for (const record of result.pending) {
            if (record.status !== "held") unconfirmed.delete(admissionBindingKey(record));
          }
          publish({
            snapshot: accept(result),
            loadFailed: false,
            loadFailure: null,
            unconfirmed,
            atLimit: result.pending.length >= 200,
            ...(state.unconfirmed.size > 0 && unconfirmed.size === 0 ? { error: null } : {}),
          });
        }
      } catch (error) {
        if (started === epoch) {
          refreshFailures = Math.min(refreshFailures + 1, 6);
          retryAt = Date.now() + Math.min(10_000 * 2 ** (refreshFailures - 1), 300_000);
          publish({ loadFailed: true, loadFailure: normalizeRemoteAdmissionError(error).kind });
        }
      } finally {
        reading = null;
        publish({ loading: false });
      }
    })();
    return reading;
  };
  const mutate = async (input: Exclude<Request, { action: "snapshot" }>) => {
    if (state.busy || state.loadFailed || !state.snapshot) return;
    if (input.action === "policy" && state.loading) return;
    if (input.action === "decide") {
      const record = state.snapshot.pending.find(
        (record) =>
          record.id === input.input.id && record.bindingDigest === input.input.bindingDigest,
      );
      if (
        !record ||
        record.status !== "held" ||
        state.unconfirmed.has(admissionBindingKey(record)) ||
        (input.input.decision === "approve" &&
          record.envelope.receiverPolicy.revision !== state.snapshot.policy.revision)
      )
        return;
    }
    ++epoch;
    publish({ busy: true, error: null });
    try {
      const result = await request(input);
      if (input.action === "policy") {
        if (!("pending" in result)) throw new Error("Unexpected policy response");
        publish({ snapshot: accept(result), atLimit: result.pending.length >= 200 });
      } else {
        if (
          !("bindingDigest" in result) ||
          result.id !== input.input.id ||
          result.bindingDigest !== input.input.bindingDigest ||
          result.status === "held"
        ) {
          throw new Error("Unconfirmed decision response");
        }
        decisions.set(admissionBindingKey(result), result);
        if (state.snapshot) publish({ snapshot: accept(state.snapshot) });
      }
    } catch (error) {
      const failure = normalizeRemoteAdmissionError(error).kind;
      const unconfirmed = new Set(state.unconfirmed);
      if (input.action === "decide") unconfirmed.add(admissionBindingKey(input.input));
      publish({
        unconfirmed,
        ...(failure === "unauthorized" || failure === "unsupported"
          ? { loadFailed: true, loadFailure: failure }
          : {}),
        error:
          input.action === "decide"
            ? failure === "unauthorized" || failure === "unsupported"
              ? "Decision could not be confirmed. Owner review is unavailable; this request cannot be retried here until its outcome is confirmed."
              : "Decision could not be confirmed. Refreshing server state; this request cannot be retried here until its outcome is confirmed."
            : "Evaluator settings could not be saved. Refresh and try again.",
      });
    } finally {
      publish({ busy: false });
      await reading;
      if (state.loadFailure !== "unauthorized" && state.loadFailure !== "unsupported")
        await refresh();
    }
  };
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    refresh,
    refreshWhenDue: () => {
      if (
        state.loadFailure === "unauthorized" ||
        state.loadFailure === "unsupported" ||
        Date.now() < retryAt
      )
        return Promise.resolve();
      return refresh();
    },
    decide: (input: RemoteAdmissionDecision) => mutate({ action: "decide", input }),
    savePolicy: (input: RemoteAdmissionPolicyUpdate) => mutate({ action: "policy", input }),
  };
}
