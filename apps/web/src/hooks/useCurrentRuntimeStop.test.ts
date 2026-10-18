import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { StopCurrentThreadRuntimeInput } from "@t3tools/contracts";
vi.mock("../state/threads", () => ({
  threadEnvironment: {
    readCurrentRuntimeStopTarget: "capture",
    stopCurrentThreadRuntime: "submit",
    observeCurrentThreadRuntimeStop: "observe",
  },
}));
vi.mock("../state/use-atom-command", () => ({
  useAtomCommand: () => {
    throw new Error("No hook invocation in pointer tests.");
  },
}));
import { currentRuntimeStopPointerStore } from "./useCurrentRuntimeStop";
const input = Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)({
  commandId: "command:stop",
  threadId: "thread:stop",
  target: {
    binding: {
      threadId: "thread:stop",
      providerThreadId: "provider-thread:stop",
      providerSessionId: "session:stop",
      providerInstanceId: "codex",
      driver: "codex",
      nativeThreadId: "native:stop",
      runtimeGeneration: "physical:stop",
    },
    evidenceRevision: 1,
  },
});
const fixture = (overrides: Partial<Storage> = {}) => {
  const values = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    key: (i) => [...values.keys()][i] ?? null,
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => {
      values.set(key, value);
    },
    removeItem: (key) => {
      values.delete(key);
    },
    ...overrides,
  };
  vi.stubGlobal("window", { localStorage: storage });
  return {
    storage,
    store: currentRuntimeStopPointerStore("jones.current-runtime-stop.v1:fixture"),
  };
};
afterEach(() => {
  vi.unstubAllGlobals();
});
describe("retained runtime stop pointer", () => {
  it("reads exact request after remount and refuses overwrite until exact completion", () => {
    const f = fixture();
    f.store.reserve(input);
    const remount = currentRuntimeStopPointerStore("jones.current-runtime-stop.v1:fixture");
    expect(remount.readPointer()).toEqual(input);
    expect(() => remount.reserve(input)).toThrow();
    expect(() =>
      remount.clear({
        ...input,
        commandId: Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)({
          ...input,
          commandId: "command:other",
        }).commandId,
      }),
    ).toThrow();
    expect(remount.readPointer()).toEqual(input);
    remount.clear(input);
    expect(f.store.readPointer()).toBeNull();
  });
  it("failed storage write or readback prevents successful reservation", () => {
    const f = fixture({
      setItem: () => {
        throw new Error("quota");
      },
    });
    expect(() => f.store.reserve(input)).toThrow();
    const g = fixture({ setItem: () => {} });
    expect(() => g.store.reserve(input)).toThrow();
  });
  it("malformed retained correlation holds rather than being silently replaced", () => {
    const f = fixture();
    f.storage.setItem("jones.current-runtime-stop.v1:fixture", '{"commandId":"partial"}');
    expect(() => f.store.readPointer()).toThrow();
    expect(() => f.store.reserve(input)).toThrow();
  });
});
