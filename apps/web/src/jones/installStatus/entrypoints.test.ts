import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type { JonesInstallPlanRequest, JonesInstallPlanResult, JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan";
import { routeInstallStatusPill, routeLaptopDownload, routeLaptopInstall, partitionUpdateEverything } from "./entrypoints";
import { createInstallStatusStore, installStatusStore } from "./panelUi";
import { bindInstallPlanServices, type InstallPlanServices } from "./services";

const environmentId = EnvironmentId.make("laptop-test");
const source = "a".repeat(40);
const target = { repository: "Jones-Systems/Jones-Code" as const, sourceSha: source, sourceTree: "b".repeat(40), version: "0.1.2" };
const plan = startPlan({ planId: "11111111-1111-4111-8111-111111111111", origin: "update-button", target,
  from: { version: "0.1.1" }, laptopEnvironmentId: environmentId, select: [], now: "2026-10-27T04:00:00.000Z" });
const native = (current: JonesInstallPlanState["current"], last: JonesInstallPlanState["last"] = null): JonesInstallPlanState => ({ schema: 1, current, last,
  laptop: { environmentId, observedAt: "2026-10-27T04:00:00.000Z", activationBlocked: false }, view: { complete: false } });
const desktop: DesktopUpdateState = {
  enabled: true, status: "available", channel: "latest", currentVersion: "0.1.1", availableVersion: "0.1.2", downloadedVersion: null,
  hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false, releaseNotes: [], omittedReleaseCount: 0, downloadPercent: null,
  checkedAt: null, message: null, errorContext: null, canRetry: false,
  jones: { source: "jones-actions", channel: "jones-main", phase: "available", capability: { check: true, download: true, install: true },
    provenance: { repository: "Jones-Systems/Jones-Code", sourceSha: source, sourceTree: target.sourceTree, version: target.version,
      workflow: "fixture-only", runId: 1, runAttempt: 1, artifactId: 99, artifactDigest: "c".repeat(64), platform: "darwin", architecture: "arm64" } },
};

describe("install entrypoint routing", () => {
  it.each(["downloading", "preparing", "installing", "blocked"] as const)("opens a current plan during %s without a Laptop or host mutation", async (phase) => {
    const requests: JonesInstallPlanRequest[] = [];
    const bridge = { installPlan: vi.fn(async (request: JonesInstallPlanRequest) => { requests.push(request); return { state: native(plan) }; }) };
    expect(await routeInstallStatusPill(bridge, { ...desktop, jones: { ...desktop.jones!, phase } }, true)).toBe("opened");
    expect(requests).toEqual([{ type: "read" }]);
    expect(installStatusStore.get().open).toBe(true);
  });
  it("retains the old bridge route and opens the preview only before other updates", async () => {
    expect(await routeInstallStatusPill({}, desktop, false)).toBe("legacy");
    const bridge = { installPlan: vi.fn(async () => ({ state: native(null) })) };
    expect(await routeInstallStatusPill(bridge, desktop, true)).toBe("legacy");
    expect(await routeInstallStatusPill(bridge, desktop, false)).toBe("opened");
    expect(installStatusStore.get().previewSelection).toEqual([]);
  });
  it("reopens last read-only and leaves the existing Check action to the caller", async () => {
    const bridge = { installPlan: vi.fn(async () => ({ state: native(null, { ...plan, dismissedAt: "2026-10-27T04:10:00.000Z" }) })) };
    const { provenance: _provenance, ...jones } = desktop.jones!;
    expect(await routeInstallStatusPill(bridge, { ...desktop, jones: { ...jones, phase: "no-new" } }, false)).toBe("last-and-check");
    expect(installStatusStore.get().showLast).toBe(true);
    expect(bridge.installPlan).toHaveBeenCalledExactlyOnceWith({ type: "read" });
  });
  it("starts Laptop through native plan intent and never prepares a fleet campaign", async () => {
    const requests: JonesInstallPlanRequest[] = [];
    const bridge = { installPlan: vi.fn(async (request: JonesInstallPlanRequest) => { requests.push(request); return { state: native(request.type === "start" ? plan : null) }; }) };
    await routeLaptopDownload(bridge, desktop);
    expect(requests.map((r) => r.type)).toEqual(["read", "start"]);
    expect(requests[1]).toMatchObject({ type: "start", select: [], laptop: { action: "download" }, target });
  });
  it.each(["clone-required", null] as const)("reviews a cold Laptop stage without creating a plan before recovery choice %s", async (policy) => {
    const requests: JonesInstallPlanRequest[] = [];
    const staged: DesktopUpdateState = { ...desktop, status: "downloaded", downloadedVersion: target.version,
      jones: { ...desktop.jones!, phase: "staged", stagedHandle: "cold-stage", currentVersion: desktop.currentVersion,
        installedSource: { repository: "Jones-Systems/Jones-Code", sha: "c".repeat(40), tree: "d".repeat(40) },
        capability: { ...desktop.jones!.capability, recoveryPolicy: 1 } } };
    const measured: JonesRecoveryPreflight = { protocol: 1, preflightId: "22222222-2222-4222-8222-222222222222",
      measuredAt: "2026-10-27T04:00:00.000Z", expiresAt: "2026-10-27T04:05:00.000Z",
      binding: { environmentId, stagedHandle: "cold-stage", currentVersion: desktop.currentVersion, expectedInstalledSource: "c".repeat(40) },
      firstInstall: false, stateBytes: 1024, volumes: [], clone: { status: "proved" },
      policies: { cloneOrCopy: { admissible: false }, cloneRequired: { admissible: true } } };
    const installPlan = vi.fn(async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
      requests.push(request);
      if (request.type === "recovery-preflight") return { state: native(null), preflight: measured };
      if (request.type === "read") return { state: native(null) };
      if (request.type === "start") return { state: native(plan) };
      throw new Error("Unexpected plan mutation before review");
    });
    const unused = async () => { throw new Error("Unexpected host dispatch"); };
    const services: InstallPlanServices = {
      bridge: { installPlan, getUpdateState: async () => staged, onUpdateState: () => () => {} },
      devices: () => [], observe: unused, legacy: unused, operation: unused, stop: unused, host: unused,
      confirm: vi.fn(async () => true), chooseRecovery: vi.fn(async () => policy), now: () => 1793073600000,
    };
    const unbind = bindInstallPlanServices(services, createInstallStatusStore());
    try {
      const result = routeLaptopInstall(services.bridge, staged);
      if (policy === null) await expect(result).rejects.toThrow("Nothing was sent.");
      else await expect(result).resolves.toMatchObject({ accepted: true, completed: false });
      expect(services.confirm).toHaveBeenCalledOnce();
      expect(services.chooseRecovery).toHaveBeenCalledExactlyOnceWith(measured);
      expect(requests.map((request) => request.type)).toEqual(policy === null
        ? ["read", "recovery-preflight"] : ["read", "recovery-preflight", "read", "start"]);
      expect(requests[1]).toEqual({ type: "recovery-preflight", input: measured.binding });
      if (policy !== null) expect(requests[3]).toMatchObject({ type: "start", laptop: { action: "install",
        review: { recovery: { policy, preflightId: measured.preflightId }, stagedHandle: "cold-stage" } } });
    } finally { unbind(); }
  });
  it("partitions all Jones machines without dropping current, offline or unsupported targets", () => {
    const machines = [
      { environmentId: EnvironmentId.make("vps"), label: "VPS", jones: true, current: true },
      { environmentId: EnvironmentId.make("mini"), label: "Mini", jones: true, offline: true },
      { environmentId: EnvironmentId.make("desktop"), label: "Desktop", jones: true, supported: false },
      { environmentId: EnvironmentId.make("npm"), label: "npm", jones: false },
    ];
    const result = partitionUpdateEverything(machines, (machine) => machine.jones);
    expect(result.jones.map((m) => m.label)).toEqual(["VPS", "Mini", "Desktop"]);
    expect(result.other.map((m) => m.label)).toEqual(["npm"]);
  });
});
