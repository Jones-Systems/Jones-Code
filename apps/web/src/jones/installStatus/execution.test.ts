import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { prepareReviewedInstall } from "@t3tools/client-runtime/jones/updates/executor";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import { createWebJonesJournal } from "./executionJournal";
import { createWebJonesExecutor, stopWebJonesDownload } from "./execution";
import { bindInstallPlanServices, type InstallPlanServices } from "./services";

const environmentId = EnvironmentId.make("web-duty-host"),
  targetSource = "a".repeat(40);
const id = (n: number) => `${String(n).padStart(8, "0")}-1111-4111-8111-111111111111`;
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
function storage() {
  let disk: string | null = null;
  return {
    getItem: () => disk,
    setItem: (_key: string, value: string) => {
      disk = value;
    },
  };
}
function host(legacy = vi.fn()): JonesExecutorHost {
  return {
    readLatest: () => ({ observation: { state, freshness: "fresh" }, canOperate: true }),
    legacy,
    start: vi.fn(),
    confirm: async () => true,
  };
}
afterEach(() => vi.unstubAllGlobals());

describe("web durable execution", () => {
  it("releases an exact supported fence, retains an unrelated hold, and requires a fresh explicit install review", async () => {
    const journal = createWebJonesJournal(storage()),
      execute = createWebJonesExecutor(journal);
    let current: JonesUpdateState = {
      ...state,
      capability: { ...state.capability, operations: 1 },
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: "c".repeat(40),
        tree: "d".repeat(40),
      },
    };
    let captured!: JonesOperationStartInput;
    const start = vi.fn(
      async (request: JonesOperationStartInput): Promise<JonesOperationReceipt> => {
        captured = request;
        throw new Error("reply lost");
      },
    );
    const confirm = vi.fn(async () => true);
    const endpoint: JonesExecutorHost = {
      readLatest: () => ({ observation: { state: current, freshness: "fresh" }, canOperate: true }),
      legacy: vi.fn(),
      start,
      confirm,
    };
    const now = () => Date.parse("2026-10-27T04:00:00Z");
    const input = {
      environmentId,
      targetSource,
      operationId: id(1),
      action: { kind: "download" as const, artifactId: 2, sourceSha: targetSource },
      host: endpoint,
      now,
    };
    expect((await execute(input)).kind).toBe("unknown");
    await journal.reserve({
      id: id(4),
      environmentId: EnvironmentId.make("other-web-environment"),
      kind: "legacy",
      action: "download",
      targetSource,
    });
    expect((await execute({ ...input, operationId: id(2) })).kind).toBe("unknown");
    expect(start).toHaveBeenCalledTimes(1);
    await journal.reconcileReceipt({
      protocol: 1,
      ...captured,
      phase: "fenced",
      updatedAt: "2026-10-27T04:00:00Z",
    });
    current = { ...current, phase: "staged", stagedHandle: "fresh-stage" };
    const reviewed = await prepareReviewedInstall({
      environmentId,
      targetSource,
      label: "host",
      host: endpoint,
      now,
    });
    expect(reviewed.kind).toBe("ready");
    if (reviewed.kind !== "ready") throw new Error("No explicit install review");
    start.mockImplementation(async (request) => ({
      protocol: 1,
      ...request,
      phase: "running",
      acceptedAt: "2026-10-27T04:00:00Z",
      updatedAt: "2026-10-27T04:00:00Z",
    }));
    expect(
      (
        await execute({
          ...input,
          operationId: id(3),
          action: reviewed.action,
          review: reviewed.review,
        })
      ).kind,
    ).toBe("accepted");
    expect(confirm).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledTimes(2);
    expect(
      journal.getSnapshot().duties.some((d) => d.environmentId === "other-web-environment"),
    ).toBe(true);
  });
  it("records a legacy dispatch before transport and blocks a fresh runtime after a lost response and revision reset", async () => {
    const disk = storage(),
      journal = createWebJonesJournal(disk);
    const legacy = vi.fn(async () => {
      expect(disk.getItem()).toContain(id(1));
      throw new Error("lost reply");
    });
    const input = {
      environmentId,
      targetSource,
      operationId: id(1),
      action: { kind: "download" as const, artifactId: 2, sourceSha: targetSource },
      host: host(legacy),
    };
    expect((await createWebJonesExecutor(journal)(input)).kind).toBe("unknown");
    const restarted = createWebJonesExecutor(createWebJonesJournal(disk));
    expect((await restarted({ ...input, operationId: id(2) })).kind).toBe("unknown");
    expect(legacy).toHaveBeenCalledTimes(1);
  });

  it("treats a lost preliminary native read as not sent, without an immortal duty", async () => {
    const journal = createWebJonesJournal(storage());
    const installPlan = vi.fn(async () => {
      throw new Error("preliminary read lost");
    });
    const unbind = bindInstallPlanServices({
      bridge: { installPlan },
    } as unknown as InstallPlanServices);
    vi.stubGlobal("window", { desktopBridge: { installPlan } });
    try {
      const result = await createWebJonesExecutor(journal)({
        environmentId,
        targetSource,
        operationId: id(1),
        action: { kind: "download", artifactId: 2, sourceSha: targetSource },
        host: host(),
      });
      expect(result.kind).toBe("refused");
      expect(journal.getSnapshot().duties).toEqual([]);
      expect(installPlan.mock.calls).toHaveLength(1);
    } finally {
      unbind();
    }
  });

  it("retains exactly one Stop through repeated calls, lost reply, fresh runtime and mismatched proof", async () => {
    const disk = storage(),
      journal = createWebJonesJournal(disk);
    const receipt: JonesOperationReceipt = {
      protocol: 1,
      environmentId,
      operationId: id(1),
      expectedInstalledSource: "c".repeat(40),
      action: { kind: "download-exact", targetSource },
      phase: "running",
      acceptedAt: "2026-10-27T04:00:00Z",
      updatedAt: "2026-10-27T04:00:00Z",
    };
    const uuid = vi.fn(() => id(2)),
      send = vi.fn(async () => {
        throw new Error("Stop reply lost");
      });
    const input = { environmentId, receipt, uuid, send, canOperate: () => true };
    await Promise.all([stopWebJonesDownload(input, journal), stopWebJonesDownload(input, journal)]);
    const restored = createWebJonesJournal(disk);
    await stopWebJonesDownload(input, restored);
    expect(uuid).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(1);
    expect(await restored.reconcileReceipt({ ...receipt, phase: "stopped" })).toBe(false);
    expect(
      await restored.reconcileReceipt({
        ...receipt,
        phase: "stopped",
        stop: { stopId: id(3), requestedAt: receipt.updatedAt, result: "stopped" },
      }),
    ).toBe(false);
    expect(
      await restored.reconcileReceipt({
        ...receipt,
        phase: "stopped",
        stop: { stopId: id(2), requestedAt: receipt.updatedAt, result: "stopped" },
      }),
    ).toBe(true);
    await stopWebJonesDownload(input, restored);
    expect(uuid).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(1);
  });
});
