import { EnvironmentId, type AuthSessionState } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const seam = vi.hoisted(() => ({ read: vi.fn<() => Promise<AuthSessionState>>() }));
vi.mock("../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/session", async () => {
  const { Atom } = await import("effect/reactivity");
  const Effect = await import("effect/Effect");
  const Data = await import("effect/Data");
  class SessionTestError extends Data.TaggedError("SessionTestError")<{
    readonly cause: unknown;
  }> {}
  return {
    createEnvironmentSessionAtoms: () => ({
      sessionStateAtom: Atom.family((_id: string) =>
        Atom.make(
          Effect.tryPromise({
            try: () => seam.read(),
            catch: (cause) => new SessionTestError({ cause }),
          }),
        ),
      ),
    }),
  };
});

import { appAtomRegistry } from "../rpc/atomRegistry";
import {
  environmentSession,
  ensureEnvironmentScope,
  readEnvironmentScope,
  readEnvironmentScopeAccess,
  refreshEnvironmentSession,
} from "./session";

const session: AuthSessionState = {
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "synthetic",
  },
  scopes: ["orchestration:operate"],
};
let sequence = 0;
const disposers: Array<() => void> = [];
afterEach(() => {
  for (const dispose of disposers.splice(0)) dispose();
  vi.useRealTimers();
  seam.read.mockReset();
});
function mount() {
  vi.useFakeTimers();
  const id = EnvironmentId.make(`scope-test-${sequence++}`);
  disposers.push(appAtomRegistry.mount(environmentSession.sessionStateAtom(id)));
  return id;
}
const flush = () => vi.advanceTimersByTimeAsync(0);

describe("environment scope access recovery", () => {
  it("shares one fresh check across simultaneous menu checks and scopes", async () => {
    seam.read.mockRejectedValueOnce(new Error("synthetic transport failure"));
    seam.read.mockResolvedValue(session);
    const id = mount();
    await flush();
    expect(readEnvironmentScopeAccess(id, "orchestration:operate").status).toBe("check_failed");
    const first = ensureEnvironmentScope(id, "orchestration:operate");
    const second = ensureEnvironmentScope(id, "orchestration:read");
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
    await flush();
    expect(await first).toEqual({ status: "granted" });
    expect(await second).toEqual({ status: "scope_missing" });
    expect(seam.read).toHaveBeenCalledTimes(2);
  });

  it("awaits the initial request without restarting it", async () => {
    let complete!: (value: AuthSessionState) => void;
    seam.read.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const id = mount();
    const result = ensureEnvironmentScope(id, "orchestration:operate");
    await flush();
    expect(readEnvironmentScopeAccess(id, "orchestration:operate")).toEqual({ status: "checking" });
    expect(seam.read).toHaveBeenCalledTimes(1);
    complete(session);
    await flush();
    expect(await result).toEqual({ status: "granted" });
  });

  it.each([
    [{ ...session, authenticated: false }, "signed_out"],
    [{ ...session, scopes: [] }, "scope_missing"],
  ] as const)("keeps a resolved denial strict without retrying", async (body, status) => {
    seam.read.mockResolvedValue(body);
    const id = mount();
    await flush();
    expect(await ensureEnvironmentScope(id, "orchestration:operate")).toEqual({ status });
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
    expect(seam.read).toHaveBeenCalledTimes(1);
  });

  it("returns a failed check when explicit recovery also fails", async () => {
    seam.read.mockRejectedValue(new Error("synthetic unavailable"));
    const id = mount();
    await flush();
    const result = ensureEnvironmentScope(id, "orchestration:operate");
    await flush();
    expect(await result).toEqual({ status: "check_failed" });
    expect(seam.read).toHaveBeenCalledTimes(2);
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
  });

  it("bounds a hung check at three seconds and never grants early", async () => {
    seam.read.mockImplementation(() => new Promise(() => {}));
    const id = mount();
    const result = ensureEnvironmentScope(id, "orchestration:operate");
    const resolved = vi.fn();
    void result.then(resolved);
    await vi.advanceTimersByTimeAsync(2_999);
    expect(resolved).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await result).toEqual({ status: "checking" });
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
    expect(seam.read).toHaveBeenCalledTimes(1);
  });

  it("explicit Refresh revokes a stale success while checking and on failure", async () => {
    seam.read.mockResolvedValueOnce(session).mockRejectedValue(new Error("synthetic unavailable"));
    const id = mount();
    await flush();
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(true);
    const refreshed = refreshEnvironmentSession(id);
    await Promise.resolve();
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
    await flush();
    await refreshed;
    expect(readEnvironmentScopeAccess(id, "orchestration:operate")).toEqual({
      status: "check_failed",
    });
    expect(readEnvironmentScope(id, "orchestration:operate")).toBe(false);
  });
});
