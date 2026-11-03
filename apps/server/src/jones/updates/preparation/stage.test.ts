// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
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
import type { extractQualifiedDarwinRuntime } from "../../cloud/qualifiedDarwinRuntime.ts";
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

it.each([false, true])(
  "preserves bounded Darwin cancellation cleanup evidence (proved: %s)",
  async (cleanupProved) => {
    const fixture = await home();
    const actions = await actionsFixture(fixture);
    const controller = new AbortController();
    const cleanup = new AbortController();
    const primary = new Error("Synthetic preparation cancellation.");
    const mount = NodePath.join(fixture.base, "runtime", "owned-mount", "volume");
    const written: string[] = [];
    const effects: string[] = [];
    let sequence = 0;
    const spawn = vi.fn(() => {
      const index = ++sequence;
      const child = Object.assign(new NodeEvents.EventEmitter(), {
        stdin: new NodeStream.PassThrough(),
        stdout: new NodeStream.PassThrough(),
        stderr: new NodeStream.PassThrough(),
        pid: 12345,
        kill: vi.fn(),
      });
      child.stdin.on("data", (data: Buffer) => written.push(data.toString("utf8")));
      queueMicrotask(() => {
        child.stdout.end('{"images":[]}');
        child.stderr.end(
          index === 3
            ? Buffer.alloc(64 * 1024 + 1, 120)
            : index === 4
              ? Buffer.from([0xff])
              : Buffer.from("native command diagnostic"),
        );
        if (index === 5) child.stdin.emit("error", new Error("Synthetic EPIPE"));
        child.emit("close", 0);
      });
      return child;
    });
    vi.resetModules();
    vi.doMock("node:child_process", () => ({ spawn }));
    vi.doMock("@t3tools/shared/jones/jonesActions", async (importOriginal) => ({
      ...(await importOriginal<typeof import("@t3tools/shared/jones/jonesActions")>()),
      validateJonesStagedArtifact: async () => ({
        payloadPath: NodePath.join(fixture.cacheRoot, "candidate.dmg"),
        candidate: { ...actions.candidate, platform: "darwin", architecture: "arm64" },
      }),
    }));
    vi.doMock("../../cloud/qualifiedRuntime.ts", async (importOriginal) => ({
      ...(await importOriginal<typeof import("../../cloud/qualifiedRuntime.ts")>()),
      currentQualifiedRuntimeBinding: async () => ({
        baseDir: fixture.base,
        dbPath: fixture.dbPath,
      }),
    }));
    vi.doMock("../../cloud/qualifiedDarwinRuntime.ts", () => ({
      extractQualifiedDarwinRuntime: async (
        input: Parameters<typeof extractQualifiedDarwinRuntime>[0],
      ) => {
        controller.abort(primary);
        const detached = await input.run({
          command: "/usr/bin/hdiutil",
          args: ["detach", mount],
          signal: cleanup.signal,
        });
        expect(detached.stderr).toBe("native command diagnostic");
        const converted = await input.run({
          command: "/usr/bin/plutil",
          args: ["-convert", "json", "-o", "-", "-"],
          stdin: "synthetic plist",
          signal: cleanup.signal,
        });
        expect(converted.stdout).toBe('{"images":[]}');
        const limited = await input.run({
          command: "/usr/bin/hdiutil",
          args: ["info", "-plist"],
          signal: cleanup.signal,
        });
        expect(limited.truncated).toBe(true);
        expect(Buffer.byteLength(limited.stderr ?? "")).toBe(64 * 1024);
        const invalid = await input.run({
          command: "/usr/bin/hdiutil",
          args: ["info", "-plist"],
          signal: cleanup.signal,
        });
        expect(invalid.invalidUtf8).toBe(true);
        const pipe = await input.run({
          command: "/usr/bin/plutil",
          args: ["-convert", "json", "-o", "-", "-"],
          stdin: "synthetic plist",
          signal: cleanup.signal,
        });
        expect(pipe.inputFailed).toBe(true);
        if (!cleanupProved) throw primary;
      },
    }));
    try {
      const { stageQualifiedArtifact } = await import("./stageArtifact.ts");
      await expect(
        stageQualifiedArtifact({
          artifact: {
            payloadPath: NodePath.join(fixture.cacheRoot, "candidate.dmg"),
          } as Parameters<typeof stageQualifiedArtifact>[0]["artifact"],
          baseDir: fixture.base,
          dbPath: fixture.dbPath,
          activeVersion: BASELINE,
          host: { platform: "darwin", architecture: "arm64" },
          scratchParent: fixture.root,
          signal: controller.signal,
          onNativeEffects: async (phase) => {
            effects.push(phase);
          },
        }),
      ).rejects.toBe(primary);
      expect(effects).toEqual(["native-effects", ...(cleanupProved ? ["clean"] : [])]);
      expect(spawn).toHaveBeenCalledTimes(5);
      expect(spawn.mock.calls[0]).toEqual([
        "/usr/bin/hdiutil",
        ["detach", mount],
        {
          stdio: ["ignore", "pipe", "pipe"],
        },
      ]);
      expect(written).toEqual(["synthetic plist", "synthetic plist"]);
      expect(spawn.mock.results[0]!.value.kill).not.toHaveBeenCalled();
      expect(
        (await NodeFSP.readdir(fixture.root)).filter((name) => name.startsWith(".jones-extract-")),
      ).toEqual([]);
    } finally {
      vi.doUnmock("node:child_process");
      vi.doUnmock("@t3tools/shared/jones/jonesActions");
      vi.doUnmock("../../cloud/qualifiedRuntime.ts");
      vi.doUnmock("../../cloud/qualifiedDarwinRuntime.ts");
      vi.resetModules();
    }
  },
);
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

  it("round-trips historical enrollment through the worker and rebinds to the current source", async () => {
    const fixture = await home();
    const actions = await actionsFixture(fixture);
    const supervisor = makePreparationSupervisor({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      command: sourceWorkerCommand,
      env: actions.env,
    });
    const first = await supervisor.run(stageJob(fixture, actions));
    if (first.kind !== "stage") throw new Error("Expected initial stage.");
    const receiptPath = NodePath.join(
      fixture.base,
      "runtime",
      "versions",
      VERSION,
      ".jones-runtime-receipt.json",
    );
    const historical = JSON.stringify({
      ...first.staged.receipt,
      installedSourceSha: "9".repeat(40),
    });
    await NodeFSP.writeFile(receiptPath, historical);
    const recovered = await supervisor.run(stageJob(fixture, actions));
    if (recovered.kind !== "stage") throw new Error("Expected recovered stage.");
    expect(recovered.staged.protocol).toBe(2);
    const adoption = {
      baseDir: fixture.base,
      dbPath: fixture.dbPath,
      activeVersion: BASELINE,
      candidate: actions.candidate,
      host: fixtureHost,
      staged: recovered.staged,
    };
    await expect(adoptPreparedStage(adoption)).resolves.toEqual(recovered.staged);
    await expect(
      adoptPreparedStage({
        ...adoption,
        candidate: { ...actions.candidate, installedSource: "9".repeat(40) },
      }),
    ).rejects.toSatisfy(blocked("invalid-artifact", /selected candidate's provenance/));
    expect(await NodeFSP.readFile(receiptPath, "utf8")).toBe(historical);
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
