import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesActionsCandidate,
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
    const f = fixture({ retireStaged: async (input) => { retired.push(input.stagedHandle); } });
    await f.updater.stageExact(candidate.source);
    const input = { operationId: "11111111-1111-4111-8111-111111111111", environmentId: "fixture", currentVersion: "0.0.0-preview.20261002.1", expectedInstalledSource: "a".repeat(40), targetSource: candidate.source, stagedHandle: "fixed-handle" };
    expect((await f.updater.retireStagedOperation({ ...input, targetSource: "f".repeat(40) })).retired).toBe(false);
    expect(retired).toEqual([]);
    expect((await f.updater.retireStagedOperation(input)).retired).toBe(true);
    expect(f.updater.snapshot().stagedHandle).toBeUndefined();
    f.newer();
    expect((await f.updater.stageExact("f".repeat(40))).provenance?.sourceSha).toBe("f".repeat(40));
    expect(retired).toEqual(["fixed-handle"]);
    expect(f.effects).toEqual(["stage", "stage"]);
  });
  it("preserves the selection when native absence cannot be proved", async () => {
    const f = fixture({ retireStaged: async () => { throw new Error("Native acceptance is unknown."); } });
    await f.updater.stageExact(candidate.source);
    const result = await f.updater.retireStagedOperation({ operationId: "11111111-1111-4111-8111-111111111111", environmentId: "fixture", currentVersion: "0.0.0-preview.20261002.1", expectedInstalledSource: "a".repeat(40), targetSource: candidate.source, stagedHandle: "fixed-handle" });
    expect(result).toEqual({ retired: false, reason: "Native acceptance is unknown." });
    expect(f.updater.snapshot().stagedHandle).toBe("fixed-handle");
  });
});
