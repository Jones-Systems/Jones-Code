import { describe, expect, it, vi } from "vite-plus/test";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Exercises the native record's replacement, mode bits and bounds on an isolated real filesystem.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths belong to the native filesystem fixture consumed by the Promise adapter.
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type { JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import { JonesUpdater, type JonesUpdaterHost } from "./JonesUpdater.ts";
import {
  createProgressRecorder,
  publishJonesUpdateProgress,
  readJonesUpdateProgress,
  reconcileProgressAtStartup,
  startProgress,
  advanceProgress,
  stampProgressReportedAt,
} from "./progress.ts";
import { sanitizeCheckFailure } from "./checkRecord.ts";

const candidate: JonesActionsCandidate = {
  schema: 1,
  repository: "Jones-Systems/Jones-Code",
  source: "b".repeat(40),
  tree: "c".repeat(40),
  installedSource: "a".repeat(40),
  workflow: ".github/workflows/artifact-cli-linux.yml",
  workflowId: 1,
  runId: 2,
  runAttempt: 1,
  ciRunId: 3,
  artifactId: 4,
  artifactName: "fixture",
  artifactDigest: `sha256:${"d".repeat(64)}`,
  artifactBytes: 10,
  expiresAt: "2099-01-01T00:00:00Z",
  version: "0.0.0-preview.20261002.2.1",
  platform: "linux",
  architecture: "x64",
};
const environmentId = EnvironmentId.make("fixture");
const currentVersion = "0.0.0-preview.20261002.1";
const install = { stagedHandle: "fixed-handle", environmentId, currentVersion };

function fixture(
  overrides: Partial<JonesUpdaterHost> = {},
  check: () => Promise<unknown> = async () => ({ state: "available", candidate }),
) {
  const states: JonesUpdateState[] = [];
  const host: JonesUpdaterHost = {
    initialState: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "no-new",
      environmentId,
      currentVersion,
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: "a".repeat(40),
        tree: "9".repeat(40),
      },
      capability: { check: true, download: true, install: true },
    },
    installedSource: async () => "a".repeat(40),
    platform: "linux",
    architecture: "x64",
    prepare: async (_candidate, { onVerifying }) => {
      onVerifying();
      return {
        stagedHandle: "fixed-handle",
        version: candidate.version,
        payloadSha256: "e".repeat(64),
      };
    },
    install: async () => undefined,
    stateChanged: (state) => states.push(state),
    ...overrides,
  };
  // The Actions client is an injected seam; results are synthetic fixtures.
  const updater = new JonesUpdater(host, { check: check as never });
  return { updater, states };
}

const stepKeys = (states: ReadonlyArray<JonesUpdateState>) =>
  states
    .map((state) => state.progress && `${state.progress.phase}:${state.progress.stepKey}`)
    .filter((entry, index, all) => entry !== undefined && entry !== all[index - 1]);

describe("updater progress record", () => {
  it("records the download steps in order and finishes when staged", async () => {
    const f = fixture();
    await f.updater.check();
    const staged = await f.updater.download({ artifactId: 4, sourceSha: candidate.source });
    expect(stepKeys(f.states)).toEqual([
      "download:download",
      "verify:verify-stage",
      "done:verify-stage",
    ]);
    expect(staged.progress).toMatchObject({
      schema: 1,
      kind: "download",
      phase: "done",
      stepIndex: 2,
      stepCount: 2,
      stepLabel: "Verifying and staging the download",
    });
    expect(staged.progress?.steps?.every((step) => step.endedAt !== undefined)).toBe(true);
  });

  it("records install steps through the launcher handoff, with marking only when continuing", async () => {
    for (const continueRunningThreads of [true, false]) {
      const f = fixture({
        install: async (input, reportStep) => {
          for (const step of ["marking", "verifying", "preflight", "capturing", "handoff"] as const)
            if (step !== "marking" || input.continueRunningThreads) reportStep?.(step);
          return { updateId: "11111111-1111-4111-8111-111111111111" };
        },
      });
      await f.updater.stageExact(candidate.source);
      f.states.length = 0;
      const state = await f.updater.install({ ...install, continueRunningThreads });
      expect(stepKeys(f.states)).toEqual([
        ...(continueRunningThreads ? ["prep:mark-threads"] : []),
        "prep:verify-runtime",
        "prep:startup-readiness",
        "prep:capture-continuity",
        "install:launcher-handoff",
        "install:restart",
      ]);
      expect(state.progress).toMatchObject({
        kind: "install",
        operationId: "11111111-1111-4111-8111-111111111111",
        stepCount: continueRunningThreads ? 7 : 6,
        stepIndex: continueRunningThreads ? 6 : 5,
        stepKey: "restart",
      });
    }
  });

  it("marks the step that failed and keeps its error bounded", async () => {
    const f = fixture({
      install: async (_input, reportStep) => {
        reportStep?.("verifying");
        reportStep?.("preflight");
        throw new Error(`bootstrap-required: ${"x".repeat(400)}`);
      },
    });
    await f.updater.stageExact(candidate.source);
    const state = await f.updater.install(install);
    expect(state.phase).toBe("blocked");
    expect(state.progress).toMatchObject({
      phase: "failed",
      stepKey: "startup-readiness",
      error: { code: "bootstrap-required" },
    });
    expect(state.progress?.error?.message.length).toBe(240);
  });

  it("a check never touches the progress record, including a failed check", async () => {
    let fail = false;
    const f = fixture({}, async () => {
      if (fail) throw new Error("network down");
      return { state: "available", candidate };
    });
    await f.updater.stageExact(candidate.source);
    const before = f.updater.snapshot().progress;
    expect(before?.phase).toBe("done");
    await f.updater.check();
    fail = true;
    await f.updater.check();
    expect(f.updater.snapshot().progress).toEqual(before);
    // A check failure on an unstaged host must not open or fail a record either.
    const idle = fixture({}, async () => {
      throw new Error("network down");
    });
    expect((await idle.updater.check()).progress).toBeUndefined();
  });
});

describe("check record", () => {
  it("dates each check, logs a sanitized failure, and clears it on the next success", async () => {
    let fail = true;
    const logged: unknown[] = [];
    const f = fixture({ checkFailed: (context) => logged.push(context) }, async () => {
      if (fail)
        throw new Error(
          "GET https://api.github.com/repos/x?access_token=abc failed: Bearer ghs_abcdefghijklmnop",
        );
      return { state: "available", candidate };
    });
    await f.updater.stageExact(candidate.source).catch(() => undefined);
    fail = false;
    await f.updater.stageExact(candidate.source);
    fail = true;
    const failed = await f.updater.check();
    expect(failed.phase).toBe("staged");
    expect(failed.lastCheck).toMatchObject({ outcome: "failed" });
    expect(failed.checkClassification?.kind).toBe("failure");
    expect(failed.lastCheck?.reason).not.toContain("ghs_");
    expect(failed.lastCheck?.reason).not.toContain("access_token");
    expect(logged.at(-1)).toMatchObject({ staged: true, reason: failed.lastCheck?.reason });
    fail = false;
    const recovered = await f.updater.check();
    expect(recovered.lastCheck).toEqual({ at: recovered.checkedAt, outcome: "succeeded" });
    expect(recovered.checkClassification).toBeUndefined();
  });

  it("redacts credentials and query strings", () => {
    expect(
      sanitizeCheckFailure(
        new Error("https://user:pw@host/p?token=1 token abcdefghijk github_pat_ABCDEFGHIJKLMNOP"),
      ),
    ).toBe("https://host/p token [redacted] [redacted]");
  });

  it("flags a staged build whose source or tree is already running", async () => {
    const f = fixture({}, async () => ({
      state: "available",
      candidate: { ...candidate, tree: "9".repeat(40) },
    }));
    await f.updater.stageExact(candidate.source);
    expect(f.updater.snapshot()).toMatchObject({ phase: "staged", stagedAlreadyRunning: true });
    const pending = fixture();
    await pending.updater.stageExact(candidate.source);
    expect(pending.updater.snapshot().stagedAlreadyRunning).toBeUndefined();
  });
});

describe("restart reconciliation", () => {
  const at = "2026-11-10T00:00:00.000Z";
  const restarting = () => {
    const started = startProgress("install", at, { operationId: "op-1" });
    return advanceProgress(advanceProgress(started, "launcher-handoff", at), "restart", at)!;
  };
  const later = Date.parse("2026-11-10T00:01:00.000Z");

  it("finishes a matching install from the launcher's committed outcome", () => {
    const settled = reconcileProgressAtStartup(
      restarting(),
      { status: "committed", id: "op-1" },
      later,
    );
    expect(settled).toMatchObject({
      phase: "done",
      stepKey: "confirm-restart",
      stepIndex: 6,
      updatedAt: "2026-11-10T00:01:00.000Z",
    });
    expect(
      settled?.steps?.every((step) => step.startedAt === undefined || step.endedAt !== undefined),
    ).toBe(true);
  });

  it("fails a rolled-back install at the confirmation step", () => {
    expect(
      reconcileProgressAtStartup(restarting(), { status: "rolled-back", id: "op-1" }, later),
    ).toMatchObject({
      phase: "failed",
      stepKey: "confirm-restart",
      error: { code: "rolled-back" },
    });
  });

  it("does not adopt another operation's outcome, and fails interrupted records", () => {
    expect(
      reconcileProgressAtStartup(restarting(), { status: "committed", id: "op-2" }, later),
    ).toMatchObject({
      phase: "failed",
      stepKey: "restart",
      error: { code: "outcome-unavailable" },
    });
    expect(
      reconcileProgressAtStartup(startProgress("download", at), undefined, later),
    ).toMatchObject({ phase: "failed", stepKey: "download", error: { code: "interrupted" } });
    const done = reconcileProgressAtStartup(
      restarting(),
      { status: "committed", id: "op-1" },
      later,
    );
    expect(reconcileProgressAtStartup(done, undefined, later + 1)).toEqual(done);
  });

  it("stamps the served record with the response clock only", () => {
    const state = { progress: startProgress("download", at) } as JonesUpdateState;
    expect(stampProgressReportedAt(state, later).progress?.reportedAt).toBe(
      "2026-11-10T00:01:00.000Z",
    );
    expect(state.progress?.reportedAt).toBe(at);
  });
});

describe("durable progress record", () => {
  const withBase = async (body: (baseDir: string) => Promise<void>) => {
    const baseDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-progress-"));
    try {
      await body(baseDir);
    } finally {
      await NodeFSP.rm(baseDir, { recursive: true, force: true });
    }
  };

  it("atomically replaces an owner-only record and reads it back", () =>
    withBase(async (baseDir) => {
      const first = startProgress("install", "2026-11-10T00:00:00.000Z", { operationId: "op-1" });
      await publishJonesUpdateProgress(baseDir, first);
      const next = advanceProgress(first, "startup-readiness", "2026-11-10T00:00:05.000Z")!;
      await publishJonesUpdateProgress(baseDir, next);
      const target = NodePath.join(baseDir, "runtime", "jones-update-progress.json");
      expect((await NodeFSP.stat(target)).mode & 0o777).toBe(0o600);
      expect(await NodeFSP.readdir(NodePath.dirname(target))).toEqual([
        "jones-update-progress.json",
      ]);
      expect(await readJonesUpdateProgress(baseDir)).toEqual(next);
      await NodeFSP.chmod(target, 0o644);
      expect(await readJonesUpdateProgress(baseDir)).toBeUndefined();
    }));

  it("keeps the record within 4 KiB, dropping step detail before skipping a write", () =>
    withBase(async (baseDir) => {
      const record = startProgress("install", "2026-11-10T00:00:00.000Z", {
        continueRunningThreads: true,
      });
      const wide = {
        ...record,
        operationId: "o".repeat(128),
        steps: (record.steps ?? []).map((step) => ({
          ...step,
          label: "l".repeat(80),
          startedAt: "s".repeat(500),
        })),
      };
      await publishJonesUpdateProgress(baseDir, wide);
      const target = NodePath.join(baseDir, "runtime", "jones-update-progress.json");
      expect((await NodeFSP.stat(target)).size).toBeLessThanOrEqual(4096);
      expect((await readJonesUpdateProgress(baseDir))?.steps).toBeUndefined();
      await NodeFSP.writeFile(target, "x".repeat(5000), { mode: 0o600 });
      expect(await readJonesUpdateProgress(baseDir)).toBeUndefined();
    }));

  it("persists only changed records and swallows write failures", async () => {
    const write = vi.fn(async () => {
      throw new Error("disk full");
    });
    const recorder = createProgressRecorder("/unused", write);
    const record = startProgress("download", "2026-11-10T00:00:00.000Z");
    recorder.persist({ progress: record });
    recorder.persist({ progress: { ...record, reportedAt: "2026-11-10T00:00:09.000Z" } });
    recorder.persist({});
    await recorder.flushed();
    expect(write).toHaveBeenCalledTimes(1);
    const f = fixture({ stateChanged: (state) => recorder.persist(state) });
    await f.updater.stageExact(candidate.source);
    await recorder.flushed();
    expect(f.updater.snapshot()).toMatchObject({ phase: "staged", progress: { phase: "done" } });
  });
});
