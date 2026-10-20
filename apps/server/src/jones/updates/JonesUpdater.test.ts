import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, JonesUpdateState } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type {
  JonesActionsCandidate,
  JonesActionsCheckResult,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { JonesUpdater, type JonesUpdaterHost } from "./JonesUpdater.ts";
import { AutomaticActivationWithdrawn } from "./followerPolicy.ts";

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
function fixture(overrides: Partial<JonesUpdaterHost> = {}) {
  const effects: string[] = [];
  let available = candidate;
  const host: JonesUpdaterHost = {
    initialState: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "no-new",
      environmentId: EnvironmentId.make("fixture"),
      currentVersion: "0.0.0-preview.20261002.1",
      capability: { check: true, download: true, install: true },
    },
    installedSource: async () => "a".repeat(40),
    platform: "linux",
    architecture: "x64",
    cacheRoot: "fixture",
    stage: async () => {
      effects.push("stage");
      return { stagedHandle: "fixed-handle", version: candidate.version };
    },
    install: async () => {
      effects.push("install");
    },
  };
  const updater = new JonesUpdater(
    { ...host, ...overrides },
    {
      check: async () => ({ state: "available", candidate: available }),
      stage: async () => ({ receipt: { sha256: "e".repeat(64) } }) as JonesStagedArtifact,
    },
  );
  return {
    updater,
    effects,
    newer: () => {
      available = { ...candidate, artifactId: 5, source: "f".repeat(40) };
    },
  };
}
describe("host-owned Jones updater", () => {
  it("stages only the pinned source and retains a conflicting existing handle", async () => {
    const f = fixture();
    const wrong = await f.updater.stageExact("f".repeat(40));
    expect(wrong.stagedHandle).toBeUndefined();
    expect(f.effects).toEqual([]);
    expect((await f.updater.stageExact(candidate.source)).stagedHandle).toBe("fixed-handle");
    f.newer();
    expect((await f.updater.stageExact("f".repeat(40))).phase).toBe("blocked");
    expect(f.updater.snapshot().stagedHandle).toBe("fixed-handle");
    expect(f.effects).toEqual(["stage"]);
  });
  it("decodes older update state without inventing installed source identity", () => {
    const decoded = Schema.decodeUnknownSync(JonesUpdateState)(fixture().updater.snapshot());
    expect(decoded.installedSource).toBeUndefined();
  });
  it("keeps authenticated installed source separate from offered and staged candidates", async () => {
    const installedSource = {
      repository: "Jones-Systems/Jones-Code" as const,
      sha: "a".repeat(40),
      tree: "9".repeat(40),
    };
    const original = fixture().updater.snapshot();
    const f = fixture({ initialState: { ...original, installedSource } });
    expect(
      Schema.decodeUnknownSync(JonesUpdateState)(f.updater.snapshot()).installedSource,
    ).toEqual(installedSource);
    await f.updater.check();
    expect(f.updater.snapshot().installedSource).toEqual(installedSource);
    expect(f.updater.snapshot().provenance?.sourceSha).toBe(candidate.source);
    await f.updater.download({ artifactId: candidate.artifactId, sourceSha: candidate.source });
    f.newer();
    await f.updater.check();
    expect(f.updater.snapshot().installedSource).toEqual(installedSource);
    expect(f.updater.snapshot().provenance?.sourceSha).toBe(candidate.source);
  });
  it("Download stages without invoking install and a later check retains the fixed handle", async () => {
    const f = fixture();
    await f.updater.check();
    const staged = await f.updater.download({
      artifactId: candidate.artifactId,
      sourceSha: candidate.source,
    });
    expect(staged.phase).toBe("staged");
    expect(f.effects).toEqual(["stage"]);
    f.newer();
    const checked = await f.updater.check();
    expect(checked.stagedHandle).toBe("fixed-handle");
    expect(checked.provenance?.sourceSha).toBe(candidate.source);
    expect(checked.phase).toBe("staged");
  });
  it("refuses stale selection and wrong environment/version/handle without native effects", async () => {
    const f = fixture();
    await f.updater.check();
    await f.updater.download({ artifactId: 99, sourceSha: candidate.source });
    expect(f.effects).toEqual([]);
    await f.updater.download({ artifactId: 4, sourceSha: candidate.source });
    const base = {
      stagedHandle: "fixed-handle",
      environmentId: EnvironmentId.make("fixture"),
      currentVersion: "0.0.0-preview.20261002.1",
    };
    for (const patch of [
      { stagedHandle: "other" },
      { environmentId: EnvironmentId.make("other") },
      { currentVersion: "1.2.3" },
    ]) {
      expect((await f.updater.install({ ...base, ...patch })).phase).toBe("blocked");
    }
    expect(f.effects).toEqual(["stage"]);
    await f.updater.install(base);
    expect(f.effects).toEqual(["stage", "install"]);
  });
  it("notifies subscriptions on a state transition and cancels idle observers", async () => {
    const f = fixture();
    const observed = f.updater.observe(f.updater.snapshot().revision);
    await f.updater.check();
    expect((await observed).phase).toBe("checking");
    const abort = new AbortController();
    const cancelled = f.updater.observe(f.updater.snapshot().revision, abort.signal);
    abort.abort();
    await expect(cancelled).rejects.toThrow("closed");
  });
  it("serializes a check and refuses overlapping downloads", async () => {
    const f = fixture();
    const check = f.updater.check();
    await f.updater.download({ artifactId: 4, sourceSha: candidate.source });
    await check;
    expect(f.effects).toEqual([]);
  });
});

it("retains the native terminal outcome identity after a subsequent build check", async () => {
  const terminal = {
    id: "update-fixture",
    status: "rolled-back" as const,
    fromVersion: "0.0.0-preview.20261002.1",
    targetVersion: candidate.version,
    reason: "trial failed",
  };
  const f = fixture({ startupOutcome: () => terminal });
  await f.updater.check();
  expect(f.updater.snapshot()).toMatchObject({
    updateId: terminal.id,
    outcome: {
      status: "rolled-back",
      reason: "trial failed",
      fromVersion: terminal.fromVersion,
      targetVersion: terminal.targetVersion,
    },
  });
});

it("keeps the accepted native update ID while refusing a second install", async () => {
  const effects: string[] = [];
  const f = fixture({
    install: async () => {
      effects.push("install");
      return { updateId: "native-id" };
    },
  });
  await f.updater.check();
  await f.updater.download({ artifactId: 4, sourceSha: candidate.source });
  const input = {
    stagedHandle: "fixed-handle",
    environmentId: EnvironmentId.make("fixture"),
    currentVersion: "0.0.0-preview.20261002.1",
  };
  expect(await f.updater.install(input)).toMatchObject({
    phase: "installing",
    updateId: "native-id",
  });
  await f.updater.install(input);
  await f.updater.check();
  expect(f.updater.snapshot().phase).toBe("installing");
  expect(effects).toEqual(["install"]);
});

describe("exact unaccepted stage retirement", () => {
  it("clears only the matching slot and permits a different frozen source afterwards", async () => {
    const retired: string[] = [];
    const f = fixture({
      retireStaged: async (input) => {
        retired.push(input.stagedHandle);
      },
    });
    await f.updater.stageExact(candidate.source);
    const input = {
      operationId: "11111111-1111-4111-8111-111111111111",
      environmentId: "fixture",
      currentVersion: "0.0.0-preview.20261002.1",
      expectedInstalledSource: "a".repeat(40),
      targetSource: candidate.source,
      stagedHandle: "fixed-handle",
    };
    expect(
      (await f.updater.retireStagedOperation({ ...input, targetSource: "f".repeat(40) })).retired,
    ).toBe(false);
    expect(retired).toEqual([]);
    expect((await f.updater.retireStagedOperation(input)).retired).toBe(true);
    expect(f.updater.snapshot().stagedHandle).toBeUndefined();
    f.newer();
    expect((await f.updater.stageExact("f".repeat(40))).provenance?.sourceSha).toBe("f".repeat(40));
    expect(retired).toEqual(["fixed-handle"]);
    expect(f.effects).toEqual(["stage", "stage"]);
  });
  it("preserves the selection when native absence cannot be proved", async () => {
    const f = fixture({
      retireStaged: async () => {
        throw new Error("Native acceptance is unknown.");
      },
    });
    await f.updater.stageExact(candidate.source);
    const result = await f.updater.retireStagedOperation({
      operationId: "11111111-1111-4111-8111-111111111111",
      environmentId: "fixture",
      currentVersion: "0.0.0-preview.20261002.1",
      expectedInstalledSource: "a".repeat(40),
      targetSource: candidate.source,
      stagedHandle: "fixed-handle",
    });
    expect(result).toEqual({ retired: false, reason: "Native acceptance is unknown." });
    expect(f.updater.snapshot().stagedHandle).toBe("fixed-handle");
  });
});

describe("exact-source staging for this host", () => {
  // Each case scripts what the Actions check returns for the requested source.
  function exact(
    check: (targetSource: string | undefined) => Promise<JonesActionsCheckResult>,
    download: (artifactId: number) => Promise<void> = async () => {},
  ) {
    const effects: string[] = [];
    const checks: Array<string | undefined> = [];
    const downloads: number[] = [];
    const phases: string[] = [];
    const updater = new JonesUpdater(
      {
        initialState: {
          source: "jones-actions",
          channel: "jones-main",
          phase: "no-new",
          environmentId: EnvironmentId.make("fixture"),
          currentVersion: "0.0.0-preview.20261002.1",
          capability: { check: true, download: true, install: true },
        },
        installedSource: async () => "a".repeat(40),
        platform: "linux",
        architecture: "x64",
        cacheRoot: "fixture",
        stage: async () => {
          effects.push("stage");
          return { stagedHandle: "fixed-handle", version: candidate.version };
        },
        install: async () => {
          effects.push("install");
        },
        stateChanged: (state) => {
          phases.push(state.phase);
        },
      },
      {
        check: async (input) => {
          checks.push(input.targetSource);
          return check(input.targetSource);
        },
        stage: async (staged) => {
          downloads.push(staged.artifactId);
          await download(staged.artifactId);
          return { receipt: { sha256: "e".repeat(64) } } as JonesStagedArtifact;
        },
      },
    );
    return { updater, effects, checks, downloads, phases };
  }
  function gate() {
    const held = { release: () => {} };
    const promise = new Promise<void>((resolve) => (held.release = resolve));
    return { promise, release: () => held.release() };
  }
  /** Lets every already-queued promise continuation run. */
  const settle = async () => {
    for (let turn = 0; turn < 10; turn++) await Promise.resolve();
  };
  const offered = async (): Promise<JonesActionsCheckResult> => ({
    state: "available",
    candidate,
  });

  it("downloads exactly the artifact its check returned for the target, no install", async () => {
    const f = exact(offered);
    const staged = await f.updater.stageExact(candidate.source);
    expect(f.checks).toEqual([candidate.source]);
    expect(f.downloads).toEqual([candidate.artifactId]);
    expect(staged).toMatchObject({
      phase: "staged",
      stagedHandle: "fixed-handle",
      provenance: { sourceSha: candidate.source, artifactId: candidate.artifactId },
    });
    expect(f.effects).toEqual(["stage"]);
  });

  it("never downloads a build whose source differs from the target", async () => {
    const other = { ...candidate, artifactId: 9, source: "f".repeat(40) };
    const f = exact(async () => ({ state: "available", candidate: other }));
    const result = await f.updater.stageExact(candidate.source);
    expect(result.stagedHandle).toBeUndefined();
    expect(result.phase).toBe("available");
    expect(result.message).toContain("not available from the current check");
    expect(f.downloads).toEqual([]);
    expect(f.effects).toEqual([]);
  });

  it("refuses to start a check or stage while a download owns the host", async () => {
    const hold = gate();
    const f = exact(offered, () => hold.promise);
    await f.updater.check();
    const downloading = f.updater.download({
      artifactId: candidate.artifactId,
      sourceSha: candidate.source,
    });
    await settle();
    const result = await f.updater.stageExact(candidate.source);
    expect(result.phase).toBe("downloading");
    expect(f.checks).toEqual([undefined]);
    expect(f.downloads).toEqual([candidate.artifactId]);
    hold.release();
    expect((await downloading).phase).toBe("staged");
    expect(f.effects).toEqual(["stage"]);
  });

  it("waits for an in-flight latest-main check, then stages only its own target run", async () => {
    const hold = gate();
    const latest = { ...candidate, artifactId: 77 };
    const f = exact(async (target) => {
      if (target === undefined) {
        await hold.promise;
        return { state: "available", candidate: latest };
      }
      return { state: "available", candidate };
    });
    const background = f.updater.check();
    await settle();
    const staging = f.updater.stageExact(candidate.source);
    await settle();
    // The exact request never reads the running latest-main check and has not started its own.
    expect(f.checks).toEqual([undefined]);
    hold.release();
    const staged = await staging;
    await background;
    expect(f.checks).toEqual([undefined, candidate.source]);
    expect(f.downloads).toEqual([candidate.artifactId]);
    expect(staged).toMatchObject({
      phase: "staged",
      stagedHandle: "fixed-handle",
      provenance: { sourceSha: candidate.source, artifactId: candidate.artifactId },
    });
  });

  it("never stages an in-flight run's candidate when the target's own run differs", async () => {
    for (const own of [
      { state: "available", candidate: { ...candidate, artifactId: 9, source: "f".repeat(40) } },
      { state: "no-new" },
      { state: "blocked", reason: "unqualified", retryAfterMs: 0 },
      { state: "building", retryAfterMs: 0 },
    ] as const) {
      const hold = gate();
      // The latest-main run offers the target's own source; the target's run says otherwise.
      const f = exact(async (target) => {
        if (target === undefined) {
          await hold.promise;
          return { state: "available", candidate };
        }
        return own;
      });
      const background = f.updater.check();
      await settle();
      const staging = f.updater.stageExact(candidate.source);
      hold.release();
      const result = await staging;
      await background;
      expect(f.checks).toEqual([undefined, candidate.source]);
      expect(result.stagedHandle).toBeUndefined();
      expect(f.downloads).toEqual([]);
      expect(f.effects).toEqual([]);
    }
  });

  it("joins concurrent same-target checks into one run with one completed snapshot", async () => {
    const hold = gate();
    const f = exact(async () => {
      await hold.promise;
      return { state: "available", candidate };
    });
    const first = f.updater.check();
    await settle();
    const joined = [f.updater.check(), f.updater.check()];
    await settle();
    hold.release();
    const results = await Promise.all([first, ...joined]);
    expect(f.checks).toEqual([undefined]);
    expect(results[1]).toEqual(results[0]);
    expect(results[2]).toEqual(results[0]);
    expect(results[0]).toMatchObject({ phase: "available" });
    expect(f.phases.filter((phase) => phase === "available")).toHaveLength(1);
  });

  it("joins concurrent exact requests for one target into one check and one download", async () => {
    const hold = gate();
    const f = exact(async () => {
      await hold.promise;
      return { state: "available", candidate };
    });
    const requests = [
      f.updater.stageExact(candidate.source),
      f.updater.stageExact(candidate.source),
    ];
    await settle();
    hold.release();
    const results = await Promise.all(requests);
    expect(f.checks).toEqual([candidate.source]);
    expect(f.downloads).toEqual([candidate.artifactId]);
    expect(f.effects).toEqual(["stage"]);
    expect(results.map((result) => result.stagedHandle ?? result.phase)).toContain("fixed-handle");
    expect(f.updater.snapshot()).toMatchObject({ phase: "staged", stagedHandle: "fixed-handle" });
  });

  it("sequences concurrent checks for different targets one run at a time", async () => {
    const holds = [gate(), gate()];
    const pending = [...holds];
    const f = exact(async () => {
      await pending.shift()!.promise;
      return { state: "no-new" };
    });
    const first = f.updater.check();
    await settle();
    const second = f.updater.check("f".repeat(40));
    const third = f.updater.check("f".repeat(40));
    await settle();
    expect(f.checks).toEqual([undefined]);
    holds[0]!.release();
    await first;
    await settle();
    expect(f.checks).toEqual([undefined, "f".repeat(40)]);
    holds[1]!.release();
    await Promise.all([second, third]);
    expect(f.checks).toEqual([undefined, "f".repeat(40)]);
  });

  it("keeps the last settled result visible while a repeat check runs", async () => {
    const hold = gate();
    let calls = 0;
    const f = exact(async () => {
      if (calls++ === 1) await hold.promise;
      return calls === 1 ? { state: "available", candidate } : { state: "no-new" };
    });
    await f.updater.check();
    expect(f.phases).toEqual(["checking", "available"]);
    const revision = f.updater.snapshot().revision ?? 0;
    expect(revision).toBeGreaterThan(0);
    const repeat = f.updater.check();
    await settle();
    // No transitional state: the prior result, provenance and revision stay as they were.
    expect(f.updater.snapshot()).toMatchObject({
      phase: "available",
      revision,
      provenance: { sourceSha: candidate.source },
    });
    expect(f.phases).toEqual(["checking", "available"]);
    // The latch still refuses a download while the check owns the host.
    await f.updater.download({ artifactId: candidate.artifactId, sourceSha: candidate.source });
    expect(f.downloads).toEqual([]);
    hold.release();
    expect(await repeat).toMatchObject({ phase: "no-new", revision: revision + 1 });
    expect(f.phases).toEqual(["checking", "available", "no-new"]);
  });

  it("starts a fresh run after a failed one instead of replaying its failure", async () => {
    let calls = 0;
    const f = exact(async () => {
      if (calls++ === 0) throw new Error("Actions unavailable");
      return { state: "available", candidate };
    });
    expect(await f.updater.check()).toMatchObject({
      phase: "blocked",
      message: "Actions unavailable",
    });
    expect(await f.updater.check()).toMatchObject({ phase: "available" });
    expect(f.checks).toEqual([undefined, undefined]);
  });

  it("reports an already-running source as no-new instead of downloading", async () => {
    const f = exact(async () => ({ state: "no-new" }));
    const result = await f.updater.stageExact("a".repeat(40));
    expect(result.phase).toBe("no-new");
    expect(result.message).toBe("This host already runs the requested source.");
    expect(f.downloads).toEqual([]);
  });

  it("surfaces an unqualified or blocked target and a check failure without staging", async () => {
    const blocked = exact(async () => ({
      state: "blocked",
      reason: "unqualified",
      retryAfterMs: 0,
    }));
    expect((await blocked.updater.stageExact(candidate.source)).phase).toBe("blocked");
    const failed = exact(async () => {
      throw new Error("Actions unavailable");
    });
    expect(await failed.updater.stageExact(candidate.source)).toMatchObject({
      phase: "blocked",
      message: "Actions unavailable",
    });
    expect(blocked.downloads.concat(failed.downloads)).toEqual([]);
  });

  it("is idempotent for the staged source and never replaces another staged source", async () => {
    const f = exact(offered);
    await f.updater.stageExact(candidate.source);
    const checks = f.checks.length;
    const again = await f.updater.stageExact(candidate.source);
    expect(again).toMatchObject({ phase: "staged", stagedHandle: "fixed-handle" });
    const refused = await f.updater.stageExact("f".repeat(40));
    expect(refused.phase).toBe("blocked");
    expect(refused.message).toContain(candidate.source.slice(0, 12));
    expect(f.updater.snapshot()).toMatchObject({
      phase: "staged",
      stagedHandle: "fixed-handle",
      provenance: { sourceSha: candidate.source },
    });
    // Neither repeat consulted Actions or staged again.
    expect(f.checks).toHaveLength(checks);
    expect(f.downloads).toEqual([candidate.artifactId]);
    expect(f.effects).toEqual(["stage"]);
  });

  it("keeps the staged provenance through later latest-main checks", async () => {
    const newer = { ...candidate, artifactId: 5, source: "f".repeat(40) };
    const f = exact(async (target) => ({
      state: "available",
      candidate: target === undefined ? newer : candidate,
    }));
    await f.updater.stageExact(candidate.source);
    const checked = await f.updater.check();
    expect(f.checks).toEqual([candidate.source, undefined]);
    expect(checked).toMatchObject({
      phase: "staged",
      stagedHandle: "fixed-handle",
      provenance: { sourceSha: candidate.source, artifactId: candidate.artifactId },
    });
  });
});

it("waits for an in-flight check before installing and rechecks the selection", async () => {
  let release!: (value: string) => void;
  let hold = false;
  const f = fixture({
    installedSource: () =>
      hold
        ? new Promise<string>((resolve) => {
            release = resolve;
          })
        : Promise.resolve("a".repeat(40)),
  });
  await f.updater.stageExact(candidate.source);
  hold = true;
  const check = f.updater.check();
  const install = f.updater.install({
    stagedHandle: "fixed-handle",
    environmentId: EnvironmentId.make("fixture"),
    currentVersion: "0.0.0-preview.20261002.1",
  });
  expect(f.effects).toEqual(["stage"]);
  release("a".repeat(40));
  await check;
  expect((await install).phase).toBe("installing");
  expect(f.effects).toEqual(["stage", "install"]);
});

it.each([false, true])(
  "publishes ordered install steps and clears its heartbeat on failure=%s",
  async (fail) => {
    vi.useFakeTimers();
    let finish!: () => void;
    const messages: string[] = [];
    const f = fixture({
      stateChanged: (state) => {
        if (state.message) messages.push(state.message);
      },
      install: async (_input, reportStep) => {
        for (const step of ["marking", "verifying", "preflight", "capturing", "handoff"] as const)
          reportStep?.(step);
        await new Promise<void>((resolve) => {
          finish = resolve;
        });
        if (fail) throw new Error("synthetic definite rejection");
      },
    });
    try {
      await f.updater.stageExact(candidate.source);
      const install = f.updater.install({
        stagedHandle: "fixed-handle",
        environmentId: EnvironmentId.make("fixture"),
        currentVersion: "0.0.0-preview.20261002.1",
      });
      await vi.advanceTimersByTimeAsync(15_000);
      expect(messages.slice(-6)).toEqual([
        "Marking running threads for continuation… (0s elapsed)",
        "Verifying the selected runtime… (0s elapsed)",
        "Checking candidate startup readiness… (0s elapsed)",
        "Capturing planned update continuity… (0s elapsed)",
        "Waiting for the native launcher… (0s elapsed)",
        "Waiting for the native launcher… (15s elapsed)",
      ]);
      finish();
      expect((await install).phase).toBe(fail ? "blocked" : "installing");
      const count = messages.length;
      await vi.advanceTimersByTimeAsync(30_000);
      expect(messages).toHaveLength(count);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      finish?.();
      vi.useRealTimers();
    }
  },
);

it("rechecks install guards after the in-flight check settles", async () => {
  let release!: (value: string) => void;
  let hold = false;
  const f = fixture({
    installedSource: () =>
      hold
        ? new Promise<string>((resolve) => {
            release = resolve;
          })
        : Promise.resolve("a".repeat(40)),
  });
  await f.updater.stageExact(candidate.source);
  hold = true;
  const check = f.updater.check();
  const install = f.updater.install({
    stagedHandle: "wrong-handle",
    environmentId: EnvironmentId.make("fixture"),
    currentVersion: "0.0.0-preview.20261002.1",
  });
  release("a".repeat(40));
  await check;
  expect((await install).message).toContain("does not match");
  expect(f.effects).toEqual(["stage"]);
});

describe("follower policy on the host updater", () => {
  const environmentId = EnvironmentId.make("fixture");
  const currentVersion = "0.0.0-preview.20261002.1";
  const leader = candidate.source;
  const other = "f".repeat(40);
  const following = (leaderSource: string) =>
    ({ role: "follower", leaderSource, leaderTree: candidate.tree }) as const;
  function follower(overrides: Partial<JonesUpdaterHost> = {}) {
    const checks: Array<string | undefined> = [];
    const installs: string[] = [];
    let offered = candidate;
    const updater = new JonesUpdater(
      {
        initialState: {
          source: "jones-actions",
          channel: "jones-main",
          phase: "no-new",
          environmentId,
          currentVersion,
          capability: { check: true, download: true, install: true },
        },
        installedSource: async () => "a".repeat(40),
        platform: "linux",
        architecture: "x64",
        cacheRoot: "fixture",
        stage: async () => ({ stagedHandle: "fixed-handle", version: offered.version }),
        install: async (request) => {
          installs.push(request.stagedHandle);
        },
        ...overrides,
      },
      {
        check: async (input): Promise<JonesActionsCheckResult> => {
          checks.push(input.targetSource);
          return { state: "available", candidate: offered };
        },
        stage: async () => ({ receipt: { sha256: "e".repeat(64) } }) as JonesStagedArtifact,
      },
    );
    return {
      updater,
      checks,
      installs,
      offer: (source: string) => {
        offered = { ...candidate, source, artifactId: candidate.artifactId + 1 };
      },
    };
  }
  const install = { stagedHandle: "fixed-handle", environmentId, currentVersion };

  it("holds every manual path until the laptop reports its installed build", async () => {
    const f = follower();
    await f.updater.check();
    expect(f.updater.snapshot().phase).toBe("available");
    const held = f.updater.setPolicy({ role: "follower", leaderSource: null });
    expect(held).toMatchObject({ phase: "no-new", policy: { leaderSource: null } });
    expect(held.provenance).toBeUndefined();
    expect((await f.updater.check()).message).toContain("Waiting for the laptop");
    expect(f.checks).toEqual([undefined]);
    const download = await f.updater.download({
      artifactId: candidate.artifactId,
      sourceSha: leader,
    });
    expect(download.phase).toBe("blocked");
    expect(f.updater.snapshot().phase).toBe("no-new");
    // A campaign from an older laptop app carries no source proof the host can verify.
    const campaign = await f.updater.stageExact(leader);
    expect(campaign.phase).toBe("blocked");
    expect(campaign.message).toContain("predates following");
    expect(f.updater.snapshot().stagedHandle).toBeUndefined();
    expect((await f.updater.install(install)).phase).toBe("blocked");
    expect(f.installs).toEqual([]);
  });

  it("holds a leader record that predates source trees until the laptop reports again", async () => {
    const f = follower();
    f.updater.setPolicy({ role: "follower", leaderSource: leader });
    expect((await f.updater.check(leader)).phase).toBe("blocked");
    expect((await f.updater.stageExact(leader)).phase).toBe("blocked");
    expect(f.updater.snapshot().stagedHandle).toBeUndefined();
  });

  it("offers, stages and installs only the laptop's committed source", async () => {
    const f = follower();
    f.updater.setPolicy(following(leader));
    await f.updater.check();
    expect(f.checks).toEqual([leader]);
    expect(f.updater.snapshot().provenance?.sourceSha).toBe(leader);
    expect((await f.updater.stageExact(other)).message).toContain("bbbbbbb");
    expect((await f.updater.stageExact(leader)).stagedHandle).toBe("fixed-handle");
    expect((await f.updater.install(install)).phase).toBe("installing");
    expect(f.installs).toEqual(["fixed-handle"]);
  });

  it("does not offer or stage the laptop's commit when the repository records another tree", async () => {
    const f = follower();
    f.updater.setPolicy({ role: "follower", leaderSource: leader, leaderTree: "9".repeat(40) });
    const checked = await f.updater.check();
    expect(checked).toMatchObject({ phase: "blocked" });
    expect(checked.message).toContain("source tree");
    expect(checked.provenance).toBeUndefined();
    const staged = await f.updater.stageExact(leader);
    expect(staged.phase).toBe("blocked");
    expect(staged.message).toContain("source tree");
    expect(f.updater.snapshot().stagedHandle).toBeUndefined();
    expect(f.installs).toEqual([]);
  });

  it("refuses a staged build whose tree the laptop's report no longer matches", async () => {
    const f = follower();
    f.updater.setPolicy(following(leader));
    await f.updater.stageExact(leader);
    f.updater.setPolicy({ role: "follower", leaderSource: leader, leaderTree: "9".repeat(40) });
    const refused = await f.updater.install(install);
    expect(refused).toMatchObject({ phase: "blocked" });
    expect(refused.message).toContain("source tree");
    expect(f.installs).toEqual([]);
  });

  it("refuses a staged build that is not the laptop's current build, even an older one", async () => {
    const f = follower();
    await f.updater.stageExact(leader);
    f.updater.setPolicy(following(other));
    expect(f.updater.snapshot().stagedHandle).toBe("fixed-handle");
    for (const origin of ["manual", "fleet"] as const) {
      const refused = await f.updater.install({ ...install, origin });
      expect(refused).toMatchObject({ phase: "blocked" });
      expect(refused.message).toContain("fffffff");
    }
    expect(f.installs).toEqual([]);
    expect(f.updater.snapshot().phase).toBe("staged");
  });

  it("keeps the stage ready when an automatic activation is withdrawn before the handoff", async () => {
    let withdraw = true;
    const f = follower({
      install: async () => {
        if (!withdraw) return;
        withdraw = false;
        throw new Error("launcher not contacted", {
          cause: new AutomaticActivationWithdrawn("Input arrived."),
        });
      },
    });
    f.updater.setPolicy(following(leader));
    await f.updater.stageExact(leader);
    const withdrawn = await f.updater.install({ ...install, origin: "fleet" });
    expect(withdrawn).toMatchObject({ phase: "staged", stagedHandle: "fixed-handle" });
    expect(withdrawn.message).toContain("Input arrived.");
    expect(withdrawn.outcome).toBeUndefined();
    expect((await f.updater.install(install)).phase).toBe("installing");
  });
});
