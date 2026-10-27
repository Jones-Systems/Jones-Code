// @effect-diagnostics nodeBuiltinImport:off - Synthetic settings files under the user's cache.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  JonesBackgroundPreparation,
  fileBackgroundPreparationStore,
  type BackgroundPreparationController,
  type BackgroundPreparationRecord,
  type BackgroundPreparationStore,
} from "./backgroundPreparation.ts";
import type {
  JonesDesktopPreparationState,
  PreparationHold,
} from "./backgroundPreparationPolicy.ts";
import type { JonesSupersedeResult } from "./JonesDesktopUpdates.ts";
import type { JonesReclaimResult } from "./stagingJobs.ts";

const SETTLE = 10 * 60_000;
const BACKOFF = 15 * 60_000;

function build(n: number): JonesActionsCandidate {
  return {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source: String(n).repeat(40),
    tree: "c".repeat(40),
    installedSource: "0".repeat(40),
    workflow: ".github/workflows/artifact-desktop-mac.yml",
    workflowId: 10,
    runId: 10 + n,
    runAttempt: 1,
    ciRunId: 20 + n,
    artifactId: 30 + n,
    artifactName: "qualified-mac",
    artifactDigest: "d".repeat(64),
    artifactBytes: 100,
    expiresAt: "2030-01-01T00:00:00Z",
    version: `0.0.44-preview.20261020.${n}.1`,
    platform: "darwin",
    architecture: "arm64",
  };
}
const handleOf = (candidate: JonesActionsCandidate) => candidate.source.slice(0, 1).repeat(64);

/** The controller surface the scheduler drives, with the published build list as input. */
class FakeController implements BackgroundPreparationController {
  busy: string | null = null;
  latestAvailable: JonesActionsCandidate | undefined;
  stagedBuild: { handle: string; source: string; version: string } | undefined;
  preparationHold: PreparationHold | undefined;
  published: JonesActionsCandidate | undefined;
  statuses: JonesDesktopPreparationState[] = [];
  downloads: string[] = [];
  supersedes: Array<{ handle: string; next: string }> = [];
  discards: string[] = [];
  downloadFails = false;
  supersedeOutcome: JonesSupersedeResult = "replaced";

  get status() {
    return this.statuses.at(-1);
  }

  stage(candidate: JonesActionsCandidate) {
    this.stagedBuild = {
      handle: handleOf(candidate),
      source: candidate.source,
      version: candidate.version,
    };
  }

  check = async () => {
    this.latestAvailable = this.published;
    return true;
  };

  download = async (selection: { artifactId: number; sourceSha: string }) => {
    const target = this.latestAvailable;
    if (target === undefined || target.source !== selection.sourceSha) throw new Error("Mismatch.");
    this.downloads.push(target.version);
    if (this.downloadFails) return { completed: false };
    this.stage(target);
    return { completed: true };
  };

  discard = async (handle: string) => {
    this.discards.push(handle);
    if (this.stagedBuild?.handle !== handle) return { accepted: false, completed: false };
    this.stagedBuild = undefined;
    return { accepted: true, completed: true };
  };

  supersede = async (
    handle: string,
    next: JonesActionsCandidate,
    reclaim: (handle: string) => Promise<void>,
  ) => {
    this.supersedes.push({ handle, next: next.version });
    const outcome = this.supersedeOutcome;
    if (outcome === "refused") return outcome;
    if (outcome === "uncertain") {
      this.preparationHold = {
        kind: "blocked",
        message: "The discard outcome needs reconciliation.",
      };
      return outcome;
    }
    this.stagedBuild = undefined;
    await reclaim(handle).catch(() => undefined);
    if (outcome === "download-failed") return outcome;
    this.stage(next);
    return outcome;
  };

  reclaimExclusive = async (reclaim: () => Promise<void>) => {
    if (this.busy !== null) return false;
    await reclaim();
    return true;
  };

  setPreparationStatus = async (status: JonesDesktopPreparationState) => {
    this.statuses.push(status);
  };
}

function memoryStore(initial?: BackgroundPreparationRecord) {
  const state = {
    record: initial,
    writes: [] as BackgroundPreparationRecord[],
    readFails: false,
    writeFails: false,
  };
  const store: BackgroundPreparationStore = {
    read: async () => {
      if (state.readFails) throw new Error("Unreadable.");
      return state.record ?? { enabled: false, unreclaimed: [] };
    },
    write: async (record) => {
      if (state.writeFails) throw new Error("Read-only.");
      state.writes.push(record);
      state.record = record;
    },
  };
  return { state, store };
}

function harness(initial?: BackgroundPreparationRecord) {
  const clock = { now: 1_000_000 };
  const controller = new FakeController();
  const { state, store } = memoryStore(initial);
  const reclaims: string[] = [];
  const reclaim = {
    fails: false,
    result: { status: "reclaimed", bytes: 10 } as JonesReclaimResult,
  };
  const background = new JonesBackgroundPreparation({
    controller,
    store,
    now: () => clock.now,
    settleMs: SETTLE,
    backoffMs: BACKOFF,
    reclaim: async (handle) => {
      reclaims.push(handle);
      if (reclaim.fails) throw new Error("Payload removal failed.");
      return reclaim.result;
    },
  });
  /** A build first seen now, then checked again once it has settled. */
  const publishSettled = async (candidate: JonesActionsCandidate) => {
    controller.published = candidate;
    await background.check();
    await background.idle();
    clock.now += SETTLE;
    await background.check();
    await background.idle();
  };
  return { background, clock, controller, reclaim, reclaims, publishSettled, store: state };
}

describe("JonesBackgroundPreparation", () => {
  it("stays off by default and never prepares a checked build", async () => {
    const h = harness();
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(1));
    expect(h.controller.downloads).toEqual([]);
    expect(h.controller.status).toEqual({ enabled: false, phase: "off" });
    expect(h.store.writes).toEqual([]);
  });

  it("downloads a newer build once it has stayed the latest for the settle period", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    await h.background.start();
    await h.background.idle();
    h.controller.published = build(1);
    await h.background.check();
    await h.background.idle();
    expect(h.controller.status).toMatchObject({
      phase: "settling",
      targetVersion: build(1).version,
    });
    expect(h.controller.downloads).toEqual([]);

    h.clock.now += SETTLE - 1;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([]);
    h.clock.now += 1;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([build(1).version]);
    expect(h.controller.status).toEqual({
      enabled: true,
      phase: "ready",
      targetVersion: build(1).version,
    });
  });

  it("restarts the settle period when a newer build replaces the latest", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    await h.background.start();
    await h.background.idle();
    h.controller.published = build(1);
    await h.background.check();
    await h.background.idle();
    h.clock.now += SETTLE - 1;
    h.controller.published = build(2);
    await h.background.check();
    await h.background.idle();
    h.clock.now += 1;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([]);
    expect(h.controller.status).toMatchObject({
      phase: "settling",
      targetVersion: build(2).version,
    });
  });

  it("replaces an unaccepted stage, recording the replaced handle until its payload is removed", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.stage(build(1));
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(2));

    expect(h.controller.supersedes).toEqual([
      { handle: handleOf(build(1)), next: build(2).version },
    ]);
    expect(h.store.writes.map((write) => write.unreclaimed)).toEqual([[handleOf(build(1))], []]);
    expect(h.reclaims).toEqual([handleOf(build(1))]);
    expect(h.controller.stagedBuild?.version).toBe(build(2).version);
    expect(h.controller.status).toMatchObject({ phase: "ready", targetVersion: build(2).version });
  });

  it("keeps only the latest: an intermediate build is never prepared", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.stage(build(1));
    await h.background.start();
    await h.background.idle();
    h.controller.published = build(2);
    await h.background.check();
    await h.background.idle();
    h.controller.published = build(3);
    await h.background.check();
    await h.background.idle();
    h.clock.now += SETTLE;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.supersedes).toEqual([
      { handle: handleOf(build(1)), next: build(3).version },
    ]);
  });

  it("pauses replacement after repeated removal failures and retries removal later", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.reclaim.fails = true;
    h.controller.stage(build(1));
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(2));
    await h.publishSettled(build(3));
    expect(h.store.record?.unreclaimed).toEqual([handleOf(build(1)), handleOf(build(2))]);

    await h.publishSettled(build(4));
    expect(h.controller.supersedes).toHaveLength(2);
    expect(h.controller.status).toMatchObject({
      phase: "blocked",
      targetVersion: build(4).version,
      reason: expect.stringContaining(`${build(3).version} can still be installed`),
    });

    // Removal is retried no sooner than its hourly backoff, then replacement resumes.
    h.reclaim.fails = false;
    h.clock.now += 60 * 60_000;
    await h.background.check();
    await h.background.idle();
    expect(h.store.record?.unreclaimed).toEqual([]);
    expect(h.controller.supersedes.at(-1)).toEqual({
      handle: handleOf(build(3)),
      next: build(4).version,
    });
  });

  it("does not remove payloads while an install or reconciliation holds the controller", async () => {
    const h = harness({ enabled: true, unreclaimed: ["9".repeat(64)] });
    h.controller.preparationHold = { kind: "install" };
    await h.background.start();
    await h.background.idle();
    expect(h.reclaims).toEqual([]);
    h.controller.preparationHold = undefined;
    await h.background.tick();
    await h.background.idle();
    expect(h.reclaims).toEqual(["9".repeat(64)]);
    expect(h.store.record?.unreclaimed).toEqual([]);
  });

  it("pins an accepted install's stage and names the newer build it is holding back", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.stage(build(1));
    h.controller.preparationHold = { kind: "install" };
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(2));
    expect(h.controller.supersedes).toEqual([]);
    expect(h.controller.status).toEqual({
      enabled: true,
      phase: "pinned",
      targetVersion: build(2).version,
    });
  });

  it("forgets a handle whose discard was refused and backs off that build", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.supersedeOutcome = "refused";
    h.controller.stage(build(1));
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(2));
    expect(h.store.record?.unreclaimed).toEqual([]);
    expect(h.controller.stagedBuild?.version).toBe(build(1).version);
    expect(h.controller.status).toMatchObject({
      phase: "backoff",
      targetVersion: build(2).version,
    });
  });

  it("stops at an uncertain discard and keeps its handle recorded", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.supersedeOutcome = "uncertain";
    h.controller.stage(build(1));
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(2));
    expect(h.store.record?.unreclaimed).toEqual([handleOf(build(1))]);
    expect(h.reclaims).toEqual([]);
    expect(h.controller.status).toMatchObject({ phase: "blocked" });
  });

  it("backs off a failed download and retries the same build after the backoff", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.controller.downloadFails = true;
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(1));
    expect(h.controller.downloads).toEqual([build(1).version]);
    expect(h.controller.status).toMatchObject({ phase: "backoff" });

    h.controller.downloadFails = false;
    h.clock.now += BACKOFF - 1;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toHaveLength(1);
    h.clock.now += 1;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([build(1).version, build(1).version]);
    expect(h.controller.status).toMatchObject({ phase: "ready" });
  });

  it("never re-prepares a build the user discarded", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(1));
    await h.background.discard(handleOf(build(1)));
    await h.background.idle();
    expect(h.store.record?.declinedSource).toBe(build(1).source);
    h.clock.now += SETTLE;
    await h.background.check();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([build(1).version]);
    expect(h.controller.status).toMatchObject({ phase: "watching" });

    await h.publishSettled(build(2));
    expect(h.controller.downloads).toEqual([build(1).version, build(2).version]);
  });

  it("persists the setting and acts on the next decision when enabled", async () => {
    const h = harness();
    await h.background.start();
    await h.background.idle();
    h.controller.published = build(1);
    await h.background.check();
    await h.background.idle();
    h.clock.now += SETTLE;
    await h.background.setEnabled(true);
    await h.background.idle();
    expect(h.store.record).toEqual({ enabled: true, unreclaimed: [] });
    expect(h.controller.downloads).toEqual([build(1).version]);

    await h.background.setEnabled(false);
    await h.background.idle();
    expect(h.store.record?.enabled).toBe(false);
    expect(h.controller.status).toEqual({ enabled: false, phase: "off" });
  });

  it("refuses to overwrite unreadable settings and stays off", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    h.store.readFails = true;
    await h.background.start();
    await h.background.idle();
    await h.publishSettled(build(1));
    expect(h.controller.downloads).toEqual([]);
    expect(h.controller.status).toMatchObject({ enabled: false, phase: "blocked" });
    await expect(h.background.setEnabled(true)).rejects.toThrow("could not be read");
    expect(h.store.writes).toEqual([]);
  });

  it("returns from a check and a setting change while a download is still running", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    const release = Promise.withResolvers<void>();
    const download = h.controller.download;
    h.controller.download = async (selection) => {
      h.controller.busy = "download";
      await release.promise;
      h.controller.busy = null;
      return download(selection);
    };
    await h.background.start();
    h.controller.published = build(1);
    await h.background.check();
    await h.background.idle();
    h.clock.now += SETTLE;
    await h.background.check();
    await vi.waitFor(() => expect(h.controller.status).toMatchObject({ phase: "preparing" }));

    await h.background.setEnabled(false);
    expect(h.controller.status).toEqual({ enabled: false, phase: "off" });
    release.resolve();
    await h.background.idle();
    expect(h.controller.downloads).toEqual([build(1).version]);
    expect(h.controller.status).toEqual({ enabled: false, phase: "off" });
  });

  it("does nothing after close", async () => {
    const h = harness({ enabled: true, unreclaimed: [] });
    await h.background.start();
    await h.background.idle();
    h.background.close();
    await h.publishSettled(build(1));
    expect(h.controller.downloads).toEqual([]);
  });
});

describe("fileBackgroundPreparationStore", () => {
  const roots = new Set<string>();
  afterEach(async () => {
    for (const root of roots) {
      await NodeFSP.rm(root, { recursive: true, force: true });
      roots.delete(root);
    }
  });

  async function settingsPath() {
    const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
    await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "background-settings-"));
    roots.add(root);
    return NodePath.join(root, "state", "jones-desktop-updates.json");
  }

  it("defaults to off, then round-trips an owner-only document", async () => {
    const path = await settingsPath();
    const store = fileBackgroundPreparationStore(path);
    expect(await store.read()).toEqual({ enabled: false, unreclaimed: [] });
    const record = {
      enabled: true,
      declinedSource: "a".repeat(40),
      unreclaimed: ["b".repeat(64)],
    };
    await store.write(record);
    expect(await store.read()).toEqual(record);
    expect((await NodeFSP.stat(path)).mode & 0o777).toBe(0o600);
    expect(await NodeFSP.readdir(NodePath.dirname(path))).toEqual(["jones-desktop-updates.json"]);
  });

  it("rejects malformed or oversized settings instead of defaulting them", async () => {
    const path = await settingsPath();
    const store = fileBackgroundPreparationStore(path);
    await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
    await NodeFSP.writeFile(path, JSON.stringify({ schema: 2, backgroundPreparation: {} }));
    await expect(store.read()).rejects.toThrow();
    await NodeFSP.writeFile(path, Buffer.alloc(70_000, 32));
    await expect(store.read()).rejects.toThrow("bounded file");
    await expect(store.write({ enabled: true, unreclaimed: ["../escape"] })).rejects.toThrow();
  });
});
