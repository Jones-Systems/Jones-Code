import type {
  RemoteAdmissionDecision,
  RemoteAdmissionPolicyUpdate,
  RemoteAdmissionRecord,
  RemoteAdmissionSnapshot,
} from "@t3tools/contracts";

export interface AdmissionState {
  readonly snapshot: RemoteAdmissionSnapshot | null;
  readonly loadFailed: boolean;
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
      : state.unconfirmed.has(bindingKey)
        ? "This decision outcome is unconfirmed. Await server confirmation; do not resend."
        : null;
}

export function actionableAdmissions(state: AdmissionState) {
  return (
    state.snapshot?.pending.filter(
      (record) => record.status === "held" && !state.unconfirmed.has(admissionBindingKey(record)),
    ) ?? []
  );
}

export function createAdmissionStore(request: (input: Request) => Promise<Response>) {
  let state: AdmissionState = {
    snapshot: null,
    loadFailed: false,
    busy: false,
    error: null,
    unconfirmed: new Set(),
    atLimit: false,
  };
  const listeners = new Set<() => void>();
  const decisions = new Map<string, RemoteAdmissionRecord>();
  let reading: Promise<void> | null = null;
  let epoch = 0;
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
    reading = (async () => {
      try {
        const result = await request({ action: "snapshot" });
        if (!("pending" in result)) throw new Error("Unexpected snapshot response");
        if (started === epoch) {
          const unconfirmed = new Set(state.unconfirmed);
          for (const record of result.pending) {
            if (record.status !== "held") unconfirmed.delete(admissionBindingKey(record));
          }
          publish({
            snapshot: accept(result),
            loadFailed: false,
            unconfirmed,
            atLimit: result.pending.length >= 200,
            ...(state.unconfirmed.size > 0 && unconfirmed.size === 0 ? { error: null } : {}),
          });
        }
      } catch {
        if (started === epoch) publish({ loadFailed: true });
      } finally {
        reading = null;
      }
    })();
    return reading;
  };
  const mutate = async (input: Exclude<Request, { action: "snapshot" }>) => {
    if (state.busy || state.loadFailed || !state.snapshot) return;
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
    } catch {
      const unconfirmed = new Set(state.unconfirmed);
      if (input.action === "decide") unconfirmed.add(admissionBindingKey(input.input));
      publish({
        unconfirmed,
        error:
          input.action === "decide"
            ? "Decision could not be confirmed. Refreshing server state; this request cannot be retried here until its outcome is confirmed."
            : "Evaluator settings could not be saved. Refresh and try again.",
      });
    } finally {
      publish({ busy: false });
      await reading;
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
    decide: (input: RemoteAdmissionDecision) => mutate({ action: "decide", input }),
    savePolicy: (input: RemoteAdmissionPolicyUpdate) => mutate({ action: "policy", input }),
  };
}
