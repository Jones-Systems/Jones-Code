// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { jonesCandidateHandle } from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  QualifiedRuntimeBlockedError,
  verifyStagedQualifiedRuntime,
} from "../../cloud/qualifiedRuntime.ts";
import {
  preparationJobsDirectory,
  type PreparationJob,
  type PreparationPhase,
} from "./protocol.ts";
import { adoptPreparedStage, adoptVerifiedStage } from "./rebind.ts";
import { makePreparationSupervisor, type PreparationWorkerExit } from "./supervisor.ts";
import {
  actionsFixture,
  BASELINE,
  candidateEntry,
  fixtureHost,
  INSTALLED,
  makePreparationHome,
  SOURCE,
  sourceWorkerCommand,
  VERSION,
  type ActionsFixture,
  type PreparationHome,
} from "./Preparation.testkit.ts";

const homes: PreparationHome[] = [];
afterEach(async () => {
  for (const home of homes.splice(0)) await home.remove();
});
async function home(): Promise<PreparationHome> {
  const created = await makePreparationHome();
  homes.push(created);
  return created;
}
const stageJob = (home: PreparationHome, actions: ActionsFixture): PreparationJob => ({
  kind: "stage",
  candidate: actions.candidate,
  cacheRoot: home.cacheRoot,
  dbPath: home.dbPath,
  activeVersion: BASELINE,
  host: fixtureHost,
});
const blocked =
  (reason: QualifiedRuntimeBlockedError["reason"], message?: RegExp) => (cause: unknown) =>
    cause instanceof QualifiedRuntimeBlockedError &&
    cause.reason === reason &&
    (message === undefined || message.test(cause.message));
const listing = (path: string) => NodeFSP.readdir(path).catch(() => []);

describe("off-process stage job", () => {
  it("downloads, extracts, preflights and stages a candidate, and the parent adopts only its durable receipt", async () => {
    const fixture = await home();
    const actions = await actionsFixture(fixture);
    const phases: PreparationPhase[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      env: actions.env,
    });
    const outcome = await supervisor.run(stageJob(fixture, actions), {
      onPhase: (phase) => phases.push(phase),
    });
    if (outcome.kind !== "stage") throw new Error("Expected a stage outcome.");
    expect(phases).toEqual(["downloading", "verifying"]);
    expect(outcome.leftover).toBeUndefined();
    expect(outcome.migrationPlan).toBeUndefined();
    const adoption = {
      baseDir: fixture.base,
      dbPath: fixture.dbPath,
      activeVersion: BASELINE,
      candidate: actions.candidate,
      host: fixtureHost,
      staged: outcome.staged,
    };
    const adopted = await adoptPreparedStage(adoption);
    expect(adopted).toEqual(outcome.staged);
    expect(adopted.receipt).toMatchObject({
      version: VERSION,
      sourceSha: SOURCE,
      installedSourceSha: INSTALLED,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      runId: 101,
      runAttempt: 2,
      artifactId: 301,
      artifactDigest: `sha256:${actions.candidate.artifactDigest}`,
      platform: fixtureHost.platform,
      architecture: fixtureHost.architecture,
    });
    // The staged runtime is the candidate's own payload, and activation's anchor still holds.
    expect(
      await NodeFSP.readFile(
        NodePath.join(fixture.base, "runtime", "versions", VERSION, "t3"),
        "utf8",
      ),
    ).toBe(candidateEntry());
    await expect(
      verifyStagedQualifiedRuntime(fixture.base, BASELINE, adopted.stagedHandle, fixtureHost),
    ).resolves.toEqual(adopted);
    expect(await NodeFSP.readFile(fixture.dbPath, "utf8")).toBe("untouched state");

    expect(await listing(preparationJobsDirectory(fixture.base))).toEqual([]);
    // The finished download stays selected for a re-stage; the sweep keeps it.
    await expect(supervisor.sweep()).resolves.toMatchObject({
      kind: "sweep",
      removed: 0,
      failed: 0,
    });
    expect(await listing(NodePath.join(fixture.cacheRoot, "completed"))).toEqual([
      `${jonesCandidateHandle(actions.candidate)}.json`,
    ]);
    expect(await listing(NodePath.join(fixture.cacheRoot, "attempts"))).toHaveLength(1);

    await expect(
      adoptPreparedStage({ ...adoption, candidate: { ...actions.candidate, runAttempt: 3 } }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /selected candidate's provenance/));
    await expect(
      adoptPreparedStage({
        ...adoption,
        host: {
          ...fixtureHost,
          architecture: fixtureHost.architecture === "x64" ? "arm64" : "x64",
        },
      }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /selected candidate's provenance/));
    await expect(
      adoptPreparedStage({ ...adoption, activeVersion: "0.0.0-preview.20261002.99" }),
    ).rejects.toSatisfy(blocked("binding-mismatch"));
    await expect(
      adoptPreparedStage({
        ...adoption,
        staged: {
          ...outcome.staged,
          receipt: { ...outcome.staged.receipt, payloadSha256: "f".repeat(64) },
        },
      }),
    ).rejects.toSatisfy(blocked("integrity-mismatch"));
    await expect(
      adoptPreparedStage({
        ...adoption,
        staged: { ...outcome.staged, stagedHandle: NodeCrypto.randomUUID() },
      }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /no valid durable receipt/));
    await expect(
      adoptPreparedStage({ ...adoption, staged: { ...outcome.staged, stagedHandle: "../escape" } }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /Invalid staged update handle/));
    await expect(
      adoptVerifiedStage({
        baseDir: fixture.base,
        activeVersion: BASELINE,
        stagedHandle: NodeCrypto.randomUUID(),
        staged: outcome.staged,
      }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /verified a different handle/));
  });

  it("refuses a candidate whose packaged preparation worker cannot start and stages nothing", async () => {
    const fixture = await home();
    const actions = await actionsFixture(fixture, { worker: false });
    const exits: PreparationWorkerExit[] = [];
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      env: actions.env,
      observe: (exit) => exits.push(exit),
    });
    await expect(supervisor.run(stageJob(fixture, actions))).rejects.toSatisfy(
      blocked("invalid-artifact", /cannot run its off-process update preparation worker/),
    );
    await vi.waitFor(() => expect(exits.map((exit) => exit.kind)).toEqual(["stage", "sweep"]), {
      timeout: 20_000,
    });
    await supervisor.sweep();
    expect(await listing(NodePath.join(fixture.base, "runtime", "versions"))).toEqual([BASELINE]);
    expect(await listing(NodePath.join(fixture.base, "runtime", "staged-updates"))).toEqual([]);
    expect(await listing(preparationJobsDirectory(fixture.base))).toEqual([]);
  });
});
