import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  type DesktopCompanionBridge,
  type DesktopCompanionState,
  type DesktopCompanionTicketRequest,
} from "@t3tools/contracts";
import { subscribeCompanionController } from "./controller.ts";
const environmentId = EnvironmentId.make("env");
const initial: DesktopCompanionState = {
  config: { enabled: true, environmentId, hostId: "mini", label: "Mini", browserOnly: true },
  status: "awaiting_ticket",
  assignments: [],
  connectionGeneration: null,
};
function fixture() {
  let stateListener!: (state: DesktopCompanionState) => void;
  let ticketListener!: (request: DesktopCompanionTicketRequest) => void;
  let read!: (state: DesktopCompanionState) => void;
  const cleanup = vi.fn();
  const bridge: DesktopCompanionBridge = {
    getState: () =>
      new Promise((resolve) => {
        read = resolve;
      }),
    configure: async () => initial,
    retry: async () => {},
    setTicketProviderReady: vi.fn(async () => {}),
    completeTicket: vi.fn(async () => {}),
    onState: (listener) => {
      stateListener = listener;
      return cleanup;
    },
    onTicketRequest: (listener) => {
      ticketListener = listener;
      return cleanup;
    },
    onNotice: () => cleanup,
  };
  const state = vi.fn();
  return {
    bridge,
    state,
    cleanup,
    read: (value: DesktopCompanionState) => read(value),
    emit: (value: DesktopCompanionState) => stateListener(value),
    request: (requestId: string) => ticketListener({ requestId, environmentId }),
  };
}
const flush = async () => {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
};
describe("renderer companion controller", () => {
  it("subscribes before initial read and rejects an older read overtaken by events", async () => {
    const f = fixture();
    const stop = subscribeCompanionController({
      ...f,
      ticket: async () => ({ _tag: "unavailable" }),
      notice: vi.fn(),
    });
    const newer = { ...initial, status: "online" as const, connectionGeneration: 2 };
    f.emit(newer);
    f.read(initial);
    await flush();
    expect(f.state).toHaveBeenLastCalledWith(newer);
    expect(f.bridge.setTicketProviderReady).toHaveBeenCalledWith(true);
    stop();
    expect(f.cleanup).toHaveBeenCalledTimes(3);
    expect(f.bridge.setTicketProviderReady).toHaveBeenLastCalledWith(false);
  });
  it("accepts a fresh ticket across serialized status updates of the same config", async () => {
    const f = fixture();
    let resolve!: (value: { _tag: "ready"; url: string }) => void;
    const stop = subscribeCompanionController({
      ...f,
      ticket: () =>
        new Promise((done) => {
          resolve = done;
        }),
      notice: vi.fn(),
    });
    f.read(initial);
    await flush();
    f.request("first");
    f.emit({ ...initial, config: { ...initial.config }, status: "connecting" });
    resolve({
      _tag: "ready",
      url: "wss://fixture.test/api/jones/preview-companion/ws?wsTicket=fixture",
    });
    await flush();
    expect(f.bridge.completeTicket).toHaveBeenCalledOnce();
    stop();
  });
  it("drops results after config changes, a newer request, or cleanup", async () => {
    const f = fixture();
    const pending: Array<(value: { _tag: "unavailable" }) => void> = [];
    const stop = subscribeCompanionController({
      ...f,
      ticket: () => new Promise((resolve) => pending.push(resolve)),
      notice: vi.fn(),
    });
    f.read(initial);
    await flush();
    f.request("first");
    f.request("second");
    pending[0]!({ _tag: "unavailable" });
    await flush();
    expect(f.bridge.completeTicket).not.toHaveBeenCalled();
    f.emit({ ...initial, config: { ...initial.config, enabled: false } });
    pending[1]!({ _tag: "unavailable" });
    await flush();
    expect(f.bridge.completeTicket).not.toHaveBeenCalled();
    f.emit(initial);
    f.request("third");
    stop();
    pending[2]!({ _tag: "unavailable" });
    await flush();
    expect(f.bridge.completeTicket).not.toHaveBeenCalled();
  });
});
