import type {
  PreviewAutomationHost,
  PreviewAutomationRequest,
  PreviewAutomationResponse,
  PreviewAutomationStreamEvent,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import {
  PreviewAutomationOperationError,
  type PreviewAutomationOperationContext,
  serializePreviewAutomationHostError,
} from "./previewAutomationErrors";

type AutomationStreamResult<E> = AsyncResult.AsyncResult<PreviewAutomationStreamEvent, E>;

export function serializePreviewAutomationError(
  error: unknown,
  context: PreviewAutomationOperationContext,
): NonNullable<PreviewAutomationResponse["error"]> {
  if (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    error._tag === "PreviewAutomationNotStartedError"
  ) {
    return {
      _tag: "PreviewAutomationTimeoutError",
      outcome: "not_started",
      message: `Preview ${context.operation} did not start within the request budget.`,
      detail: { ...context },
    };
  }
  return serializePreviewAutomationHostError(
    PreviewAutomationOperationError.fromCause({ ...context, cause: error }),
  );
}

export function createPreviewAutomationRequestConsumerAtom<E>(options: {
  readonly requestsAtom: Atom.Atom<AutomationStreamResult<E>>;
  readonly clientId: PreviewAutomationHost["clientId"];
  readonly connectionAtom: Atom.Writable<PreviewAutomationStreamEvent["connectionId"] | null>;
  readonly environmentId: PreviewAutomationHost["environmentId"];
  readonly requestHandlerAtom: Atom.Atom<{
    readonly handle: (request: PreviewAutomationRequest) => Promise<unknown>;
  }>;
  readonly respond: (response: PreviewAutomationResponse) => Promise<unknown>;
  readonly label: string;
}): Atom.Atom<void> {
  return Atom.make((get) => {
    get.mount(options.connectionAtom);
    get.mount(options.requestHandlerAtom);
    let disposed = false;
    let activeConnectionId: PreviewAutomationStreamEvent["connectionId"] | null = null;
    let connectionExplicitlyAnnounced = false;
    let reportedConnectionId: PreviewAutomationStreamEvent["connectionId"] | null = null;
    let requestsVersion = 0;
    const controlled = new Map<string, Set<Promise<unknown>>>();
    const controlledOperations = new Set(["click", "type", "press", "scroll", "evaluate"]);

    const consume = (result: AutomationStreamResult<E>) => {
      if (!AsyncResult.isSuccess(result)) return;
      const event = result.value;
      if (event.type === "connected") {
        activeConnectionId = event.connectionId;
        connectionExplicitlyAnnounced = true;
      } else if (activeConnectionId === null) {
        activeConnectionId = event.connectionId;
      } else if (activeConnectionId !== event.connectionId) {
        if (connectionExplicitlyAnnounced) return;
        activeConnectionId = event.connectionId;
      }
      if (reportedConnectionId !== event.connectionId) {
        reportedConnectionId = event.connectionId;
        get.set(options.connectionAtom, event.connectionId);
      }
      if (event.type === "connected") {
        return;
      }
      const request = event.request;
      const deadlineMs = Date.now() + request.timeoutMs;
      const deliver = async (response: PreviewAutomationResponse) => {
        while (!disposed && activeConnectionId === event.connectionId) {
          let timer: ReturnType<typeof setTimeout> | undefined;
          try {
            await Promise.race([
              options.respond(response),
              new Promise<never>((_, reject) => {
                timer = setTimeout(
                  () => reject(new Error("Response deadline elapsed")),
                  Math.max(0, deadlineMs - Date.now()),
                );
              }),
            ]);
            return;
          } catch {
            if (Date.now() >= deadlineMs) return;
            await new Promise<void>((resolve) =>
              setTimeout(resolve, Math.min(100, Math.max(0, deadlineMs - Date.now()))),
            );
          } finally {
            clearTimeout(timer);
          }
        }
      };
      const key = `${event.connectionId}:${request.threadId}:${request.tabId ?? ""}`;
      const predecessors = request.operation === "snapshot" ? [...(controlled.get(key) ?? [])] : [];
      const execution =
        request.operation === "ping"
          ? Promise.resolve({ alive: true })
          : Promise.resolve().then(async () => {
              if (predecessors.length > 0) {
                let timer: ReturnType<typeof setTimeout> | undefined;
                try {
                  await Promise.race([
                    Promise.allSettled(predecessors),
                    new Promise<never>((_, reject) => {
                      timer = setTimeout(
                        () => reject({ _tag: "PreviewAutomationNotStartedError" }),
                        Math.max(0, deadlineMs - Date.now()),
                      );
                    }),
                  ]);
                } finally {
                  clearTimeout(timer);
                }
              }
              return get.once(options.requestHandlerAtom).handle({
                ...request,
                timeoutMs: Math.max(0, deadlineMs - Date.now()),
              });
            });
      if (controlledOperations.has(request.operation)) {
        const pending = controlled.get(key) ?? new Set<Promise<unknown>>();
        pending.add(execution);
        controlled.set(key, pending);
        const settled = () => {
          pending.delete(execution);
          if (pending.size === 0 && controlled.get(key) === pending) controlled.delete(key);
        };
        void execution.then(settled, settled);
      }
      void execution.then(
        (value) =>
          deliver({
            clientId: options.clientId,
            connectionId: event.connectionId,
            requestId: request.requestId,
            ok: true,
            ...(value === undefined ? {} : { result: value }),
          }),
        (error) =>
          deliver({
            clientId: options.clientId,
            connectionId: event.connectionId,
            requestId: request.requestId,
            ok: false,
            error: serializePreviewAutomationError(error, {
              requestId: request.requestId,
              operation: request.operation,
              environmentId: options.environmentId,
              threadId: request.threadId,
              tabId: request.tabId ?? null,
            }),
          }),
      );
    };

    get.addFinalizer(() => {
      disposed = true;
    });
    const initialRequest = get.once(options.requestsAtom);
    if (AsyncResult.isSuccess(initialRequest)) {
      activeConnectionId = initialRequest.value.connectionId;
      connectionExplicitlyAnnounced = initialRequest.value.type === "connected";
      if (initialRequest.value.type === "connected") {
        reportedConnectionId = initialRequest.value.connectionId;
        get.set(options.connectionAtom, initialRequest.value.connectionId);
      }
    }
    get.subscribe(options.requestsAtom, (result) => {
      requestsVersion += 1;
      consume(result);
    });
    queueMicrotask(() => {
      const initialConnectionWasSkipped =
        AsyncResult.isSuccess(initialRequest) &&
        initialRequest.value.connectionId === activeConnectionId &&
        initialRequest.value.connectionId !== reportedConnectionId;
      if (!disposed && (requestsVersion === 0 || initialConnectionWasSkipped)) {
        consume(initialRequest);
      }
    });
  }).pipe(Atom.setIdleTTL(0), Atom.withLabel(options.label));
}
