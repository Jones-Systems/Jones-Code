import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, JonesUpdateState } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type {
  JonesActionsCandidate,
  JonesActionsCheckResult,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { JonesUpdater, type JonesUpdaterHost } from "./JonesUpdater.ts";

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
  function exact(check: (targetSource: string | undefined) => Promise<JonesActionsCheckResult>) {
    const effects: string[] = [];
    const checks: Array<string | undefined> = [];
    const downloads: number[] = [];
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
      },
      {
        check: async (input) => {
          checks.push(input.targetSource);
          return check(input.targetSource);
        },
        stage: async (staged) => {
          downloads.push(staged.artifactId);
          return { receipt: { sha256: "e".repeat(64) } } as JonesStagedArtifact;
        },
      },
    );
    return { updater, effects, checks, downloads };
  }
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

  it("ignores a candidate left by an earlier check when this check is skipped", async () => {
    const gate = { release: () => {} };
    const hold = new Promise<void>((resolve) => (gate.release = resolve));
    let call = 0;
    const f = exact(async () => {
      if (call++ === 1) await hold;
      return { state: "available", candidate };
    });
    // The first check populates the offered candidate with the target's own source, then a
    // second one stays in flight so the exact request's check is skipped as busy.
    await f.updater.check();
    const pending = f.updater.check();
    const result = await f.updater.stageExact(candidate.source);
    expect(result.message).toContain("not available from the current check");
    expect(f.downloads).toEqual([]);
    gate.release();
    await pending;
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
