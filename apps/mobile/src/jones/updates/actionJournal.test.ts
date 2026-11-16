import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import { createJonesActionJournal } from "@t3tools/client-runtime/jones/updates/action-journal";
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("./actionJournal", async () => {
  const { createJonesActionJournal } =
    await import("@t3tools/client-runtime/jones/updates/action-journal");
  return {
    mobileJonesJournal: createJonesActionJournal({ getItem: () => null, setItem: () => {} }),
  };
});
vi.mock("@t3tools/client-runtime/jones/updates", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/jones/updates")>()),
  createJonesUpdateAtoms: () => ({}),
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
vi.mock("../../lib/uuid", () => {
  let next = 1;
  return { uuidv4: () => `${String(next++).padStart(8, "0")}-1111-4111-8111-111111111111` };
});
import { createMobileJonesRuntime, type MobileJonesTransport } from "./jonesUpdates";

const environmentId = EnvironmentId.make("mobile-process-journal"),
  targetSource = "a".repeat(40);
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  environmentId,
  currentVersion: "old",
  revision: 0,
  capability: { check: true, download: true, install: true },
  provenance: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: targetSource,
    sourceTree: "b".repeat(40),
    workflow: "synthetic",
    runId: 1,
    runAttempt: 1,
    artifactId: 2,
    artifactDigest: "d".repeat(64),
    platform: "linux",
    architecture: "x64",
  },
};
function disk() {
  let bytes: string | null = null;
  return {
    getItem: () => bytes,
    setItem: (_key: string, value: string) => {
      bytes = value;
    },
  };
}
function transport(): MobileJonesTransport {
  return {
    legacy: vi.fn(async () => {
      throw new Error("lost reply");
    }),
    start: vi.fn(),
    preflight: vi.fn(),
    read: vi.fn(),
    stop: vi.fn(),
  };
}

describe("mobile process recreation", () => {
  it("restores a lost legacy dispatch before enabling controls, with no operations request to the old host", async () => {
    const storage = disk(),
      first = createJonesActionJournal(storage),
      target = transport();
    const owner = createMobileJonesRuntime(first);
    const input = {
      environmentId,
      transport: target,
      readLatest: () => ({ observation: { state, freshness: "fresh" as const }, canOperate: true }),
    };
    const actions = owner.actions(input);
    await first.load();
    await actions.run("download");
    expect(target.legacy).toHaveBeenCalledTimes(1);
    const second = createJonesActionJournal(storage),
      restored = createMobileJonesRuntime(second);
    const restartedActions = restored.actions(input);
    expect(restored.session(environmentId).getSnapshot().ready).toBe(false);
    await second.load();
    expect(restored.session(environmentId).getSnapshot().uncertainty).toBe("legacy");
    expect(restored.session(environmentId).getSnapshot().operationId).toBeDefined();
    await restartedActions.run("download");
    await restartedActions.check();
    await restartedActions.read();
    expect(target.legacy).toHaveBeenCalledTimes(1);
    expect(target.start).not.toHaveBeenCalled();
    expect(target.read).not.toHaveBeenCalled();
  });

  it("releases only an exactly fenced operation and admits a later explicit action", async () => {
    const storage = disk(),
      first = createJonesActionJournal(storage),
      target = transport();
    let sent!: JonesOperationStartInput;
    vi.mocked(target.start).mockImplementation(async (request) => {
      sent = request;
      throw new Error("accepted, reply lost");
    });
    const modern = {
      ...state,
      capability: { ...state.capability, operations: 1 as const },
      installedSource: {
        repository: "Jones-Systems/Jones-Code" as const,
        sha: "c".repeat(40),
        tree: "d".repeat(40),
      },
    };
    const input = {
      environmentId,
      transport: target,
      readLatest: () => ({
        observation: { state: modern, freshness: "fresh" as const },
        canOperate: true,
      }),
    };
    const owner = createMobileJonesRuntime(first),
      actions = owner.actions(input);
    await first.load();
    await actions.run("download");
    await first.reserve({
      id: "99999999-1111-4111-8111-111111111111",
      environmentId: EnvironmentId.make("other-mobile-host"),
      kind: "legacy",
      action: "download",
      targetSource,
    });
    const second = createJonesActionJournal(storage),
      restored = createMobileJonesRuntime(second),
      next = restored.actions(input);
    await second.load();
    let answer: JonesOperationReceipt = {
      protocol: 1,
      ...sent,
      phase: "fenced",
      updatedAt: "2026-10-27T04:00:00Z",
      operationId: "88888888-1111-4111-8111-111111111111",
    };
    vi.mocked(target.read).mockImplementation(async () => ({
      receipts: [{ operationId: sent.operationId, receipt: answer }],
    }));
    await next.read();
    expect(restored.session(environmentId).getSnapshot().uncertainty).toBe("start");
    answer = { ...answer, operationId: sent.operationId };
    await next.read();
    expect(restored.session(environmentId).getSnapshot().uncertainty).toBeUndefined();
    expect(second.getSnapshot().duties.map((d) => d.environmentId)).toEqual(["other-mobile-host"]);
    await next.run("download");
    expect(target.start).toHaveBeenCalledTimes(2);
  });
});
