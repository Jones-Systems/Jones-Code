import {
  CompanionDown,
  ThreadId,
  PREVIEW_COMPANION_PROTOCOL,
  PREVIEW_COMPANION_WS_PATH,
  type CompanionUp,
  type DesktopBrowserEvent,
  type DesktopBrowserCommand,
  type DesktopCompanionConfig,
  type DesktopCompanionState,
  type DesktopCompanionTicketRequest,
  type DesktopCompanionTicketResponse,
  type PreviewAutomationRuntimeIdentity,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  CompanionFrameDecoder,
  CompanionFrameError,
  encodeFrames,
  MAX_MESSAGE_BYTES,
} from "@t3tools/shared/jones/previewCompanionFraming";

type TabKey = { readonly threadId: string; readonly tabId: string };
export interface CompanionSocket {
  readonly bufferedAmount: number;
  send(frame: string): void;
  close(code: number): void;
  onOpen(listener: () => void): () => void;
  onMessage(listener: (frame: unknown) => void): () => void;
  onClose(listener: (code: number) => void): () => void;
  onError(listener: (status?: number) => void): () => void;
}
export interface CompanionUplinkPorts {
  readonly browserOnlyLocked?: boolean;
  readonly runtimeIdentity: PreviewAutomationRuntimeIdentity;
  readonly platform: string;
  readonly now: () => number;
  readonly random: () => number;
  readonly schedule: (delay: number, task: () => void) => () => void;
  readonly connect: (url: string) => CompanionSocket;
  readonly state: (state: DesktopCompanionState) => void;
  readonly requestTicket: (request: DesktopCompanionTicketRequest) => void;
  readonly replaceAssignments: (keys: ReadonlyArray<TabKey>) => void;
  readonly command: (command: DesktopBrowserCommand, isCurrent: () => boolean) => void;
  readonly subscribeBrowser: (
    listener: (event: DesktopBrowserEvent) => void,
    isAssigned: (key: TabKey) => boolean,
  ) => () => void;
  readonly preventSuspension: () => () => void;
  readonly unknownMessage: () => void;
}
const keyOf = ({ threadId, tabId }: TabKey) => JSON.stringify([threadId, tabId]);
const decode = Schema.decodeUnknownOption(CompanionDown);

export function isCompanionTicketUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      (url.protocol === "ws:" || url.protocol === "wss:") &&
      url.pathname === PREVIEW_COMPANION_WS_PATH &&
      url.username === "" &&
      url.password === "" &&
      url.hash === "" &&
      url.searchParams.getAll("wsTicket").length === 1 &&
      (url.searchParams.get("wsTicket")?.length ?? 0) > 0 &&
      [...url.searchParams.keys()].every((key) => key === "wsTicket")
    );
  } catch {
    return false;
  }
}

export function createCompanionUplink(
  initial: DesktopCompanionConfig,
  ports: CompanionUplinkPorts,
) {
  let state: DesktopCompanionState = {
    ...(ports.browserOnlyLocked ? { browserOnlyLocked: true } : {}),
    config: initial,
    status: initial.enabled ? "awaiting_ticket" : "disabled",
    connectionGeneration: null,
    assignments: [],
  };
  let ready = false;
  let disposed = false;
  let epoch = 0;
  let attempt = 0;
  let sequence = 0;
  let socket: CompanionSocket | undefined;
  let pending: DesktopCompanionTicketRequest | undefined;
  let cancelRetry: (() => void) | undefined;
  let cancelDeadline: (() => void) | undefined;
  let cancelHeartbeat: (() => void) | undefined;
  let stopBrowser: (() => void) | undefined;
  let stopPower: (() => void) | undefined;
  let subscriptions: Array<() => void> = [];
  let assigned = new Set<string>();
  let lastSeen = 0;
  let onlineSince = 0;
  let warnedUnknown = false;
  const emit = (status = state.status) => {
    state = { ...state, status };
    ports.state(state);
  };
  const isAssigned = (key: TabKey) => assigned.has(keyOf(key));
  const replace = (keys: ReadonlyArray<TabKey>) => {
    const tabs = [
      ...new Map(
        keys.map(({ threadId, tabId }) => [
          keyOf({ threadId, tabId }),
          { threadId: ThreadId.make(threadId), tabId },
        ]),
      ).values(),
    ];
    // Host validates ordinary-tab collisions before mutating ownership or exposing mounts.
    ports.replaceAssignments(tabs);
    assigned = new Set(tabs.map(keyOf));
    state = { ...state, assignments: tabs };
    if (tabs.length && !stopPower) stopPower = ports.preventSuspension();
    if (!tabs.length && stopPower) {
      stopPower();
      stopPower = undefined;
    }
    emit();
  };
  const clear = (code = 1000) => {
    epoch++;
    pending = undefined;
    cancelRetry?.();
    cancelRetry = undefined;
    cancelDeadline?.();
    cancelDeadline = undefined;
    cancelHeartbeat?.();
    cancelHeartbeat = undefined;
    stopBrowser?.();
    stopBrowser = undefined;
    for (const unsubscribe of subscriptions) unsubscribe();
    subscriptions = [];
    const previous = socket;
    socket = undefined;
    try {
      previous?.close(code);
    } catch {
      /* A closed socket has no further work to perform. */
    }
    state = { ...state, connectionGeneration: null };
    replace([]);
  };
  const retryAfter = (status: DesktopCompanionState["status"], delay: number) => {
    clear();
    emit(status);
    if (!ready || disposed || !state.config.enabled) return;
    const current = epoch;
    cancelRetry = ports.schedule(delay, () => {
      if (current === epoch) request();
    });
  };
  const failed = (code?: number) => {
    if (code === 4401 || code === 401 || code === 403) {
      clear();
      emit("auth_required");
      return;
    }
    if (code === 4409) {
      clear();
      emit("superseded");
      return;
    }
    if (code === 4426 || code === 404) {
      retryAfter("unsupported", 300000);
      return;
    }
    if (onlineSince && ports.now() - onlineSince >= 45000) attempt = 0;
    const ceiling = Math.min(30000, 1000 * 2 ** Math.min(attempt++, 5));
    retryAfter("reconnecting", Math.max(1000, Math.floor(ceiling * (0.8 + ports.random() * 0.2))));
  };
  const send = (message: CompanionUp) => {
    const currentSocket = socket;
    if (!currentSocket) return;
    try {
      for (const frame of encodeFrames(message, `desktop-${++sequence}`)) {
        if (
          currentSocket.bufferedAmount + new TextEncoder().encode(frame).byteLength >
          MAX_MESSAGE_BYTES
        ) {
          clear(4413);
          emit("unavailable");
          return;
        }
        currentSocket.send(frame);
      }
    } catch {
      failed();
    }
  };
  const request = () => {
    if (disposed || !ready || !state.config.enabled || state.config.environmentId === null) return;
    clear();
    emit("awaiting_ticket");
    pending = {
      requestId: `companion-${epoch}-${++sequence}`,
      environmentId: state.config.environmentId,
    };
    const current = epoch;
    cancelDeadline = ports.schedule(15000, () => {
      if (current === epoch) failed();
    });
    ports.requestTicket(pending);
  };
  const open = (url: string) => {
    const current = epoch;
    const active = () => current === epoch && !disposed;
    let connection: CompanionSocket;
    try {
      connection = ports.connect(url);
    } catch {
      failed();
      return;
    }
    socket = connection;
    const frames = new CompanionFrameDecoder();
    let welcomed = false;
    let reconciled = false;
    onlineSince = 0;
    emit("connecting");
    cancelDeadline = ports.schedule(20000, () => {
      if (active()) failed();
    });
    subscriptions = [
      connection.onOpen(() => {
        if (!active()) return;
        send({
          type: "hello",
          protocol: PREVIEW_COMPANION_PROTOCOL,
          hostId: state.config.hostId,
          label: state.config.label,
          platform: ports.platform,
          runtimeIdentity: ports.runtimeIdentity,
          capabilities: {
            cdp: true,
            clipboardText: true,
            uploads: false,
            downloads: false,
            recording: false,
          },
        });
      }),
      connection.onClose((code) => {
        if (active()) failed(code);
      }),
      connection.onError((status) => {
        if (active()) failed(status);
      }),
      connection.onMessage((frame) => {
        if (!active()) return;
        try {
          if (typeof frame !== "string") throw new CompanionFrameError(4426);
          const raw = frames.accept(frame);
          if (raw === undefined) return;
          const message = decode(raw);
          if (Option.isNone(message)) {
            if (
              typeof raw === "object" &&
              raw !== null &&
              "type" in raw &&
              raw.type === "welcome"
            ) {
              failed(4426);
              return;
            }
            if (!warnedUnknown) {
              warnedUnknown = true;
              ports.unknownMessage();
            }
            return;
          }
          const value = message.value;
          lastSeen = ports.now();
          if (value.type === "welcome") {
            if (welcomed || value.environmentId !== state.config.environmentId) {
              clear(4426);
              emit("unavailable");
              return;
            }
            welcomed = true;
            onlineSince = lastSeen;
            state = { ...state, connectionGeneration: value.connectionGeneration };
            // The deadman is fixed by protocol V1; a peer cannot extend it through heartbeatMs.
            const beat = () => {
              if (!active()) return;
              if (ports.now() - lastSeen >= 45000) {
                clear(4408);
                failed();
                return;
              }
              send({ type: "heartbeat", sentAt: ports.now() });
              if (active()) cancelHeartbeat = ports.schedule(15000, beat);
            };
            cancelHeartbeat = ports.schedule(15000, beat);
            return;
          }
          if (!welcomed) {
            clear(4426);
            emit("unavailable");
            return;
          }
          if (value.type === "assignments") {
            replace(value.tabs);
            if (!reconciled) {
              reconciled = true;
              cancelDeadline?.();
              cancelDeadline = undefined;
              stopBrowser = ports.subscribeBrowser(
                (event) => {
                  if (!active() || !isAssigned(event)) return;
                  send({ type: "browser", event });
                  if (event.type === "attached")
                    send({
                      type: "mounted",
                      threadId: ThreadId.make(event.threadId),
                      tabId: event.tabId,
                    });
                },
                (key) => active() && isAssigned(key),
              );
            }
            emit("online");
          } else if (value.type === "mount" && reconciled) {
            replace([...state.assignments, { threadId: value.threadId, tabId: value.tabId }]);
          } else if (value.type === "unmount" && reconciled) {
            replace(state.assignments.filter((key) => keyOf(key) !== keyOf(value)));
            send({ type: "unmountedAck", threadId: value.threadId, tabId: value.tabId });
          } else if (value.type === "browser" && reconciled && isAssigned(value.command)) {
            ports.command(value.command, () => active() && isAssigned(value.command));
          }
        } catch (error) {
          clear(error instanceof CompanionFrameError ? error.code : 4426);
          emit("unavailable");
        }
      }),
    ];
  };
  return {
    getState: () => state,
    configure: (config: DesktopCompanionConfig, restartRequired = false) => {
      clear();
      state = { ...state, config };
      attempt = 0;
      emit(restartRequired ? "restart_required" : config.enabled ? "awaiting_ticket" : "disabled");
      if (!restartRequired) request();
    },
    setReady: (value: boolean) => {
      if (disposed || ready === value) return;
      ready = value;
      if (!ready) {
        clear();
        emit(state.config.enabled ? "awaiting_ticket" : "disabled");
      } else if (state.status !== "restart_required") request();
    },
    completeTicket: (response: DesktopCompanionTicketResponse) => {
      if (
        !pending ||
        response.requestId !== pending.requestId ||
        response.environmentId !== pending.environmentId
      )
        return;
      pending = undefined;
      cancelDeadline?.();
      cancelDeadline = undefined;
      if (response.result._tag !== "ready") {
        if (response.result._tag === "unsupported") failed(404);
        else if (response.result._tag === "auth_required") failed(4401);
        else retryAfter("unavailable", 30000);
        return;
      }
      if (!isCompanionTicketUrl(response.result.url)) {
        clear();
        emit("auth_required");
        return;
      }
      open(response.result.url);
    },
    retry: () => {
      if (state.status !== "restart_required") {
        attempt = 0;
        request();
      }
    },
    dispose: () => {
      disposed = true;
      ready = false;
      clear();
    },
  };
}
