// @effect-diagnostics nodeBuiltinImport:off
// Hosted-only reproduction for Jones-Code #873. Both cases use the same synthetic
// installed binding and exact qualified Actions payload.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { JonesActionsClient } from "@t3tools/shared/jones/jonesActions";
import { qualifiedRuntimeArtifactFromJonesStage } from "../../cloud/qualifiedRuntime.ts";
import {
  decodePreparationResult,
  PREPARATION_LIMITS,
  preparationJobsDirectory,
  readBoundedJson,
  RESULT_FILE,
  type PreparationPhase,
} from "./protocol.ts";
import { isLowestWorkerPriority, makePreparationHome } from "./Preparation.testkit.ts";
import {
  makePreparationSupervisor,
  PreparationWorkerError,
  type PreparationWorkerExit,
} from "./supervisor.ts";

class ReproError extends Error {}

const producer = {
  source: "8dd90ff9e3d41e7ba9ac1c81bceb4f973734314b",
  tree: "61cba9fb99501a49332e5cfe19c94bf0d97c6b70",
  runId: 42147193520,
  artifactId: 13471135731,
  artifactDigest: "eb84d344bf4e2fcf25189674bdbb58369a62ba567aa762ceef646ac0c6a9358a",
};
const target = {
  source: "245409b7feefd2509b12f2fb7e48e6ec5e8d0b50",
  tree: "3dd4d16816fcdac08796e1b82a01850dc5fa49f3",
  runId: 42315079345,
  artifactId: 13550452169,
  artifactDigest: "6e8148eed1312da02cba33c8bededee24ecee8ef0dad4fcd5e2f811da853fe67",
};
const host = { platform: "darwin", architecture: "arm64" } as const;

async function exactCandidate(
  client: JonesActionsClient,
  installedSource: string,
  expected: typeof target,
) {
  const check = await client.check({ ...host, installedSource, targetSource: expected.source });
  if (check.state !== "available")
    throw new ReproError(`Exact artifact discovery returned ${check.state}.`);
  const candidate = check.candidate;
  if (
    candidate.source !== expected.source ||
    candidate.tree !== expected.tree ||
    candidate.runId !== expected.runId ||
    candidate.runAttempt !== 1 ||
    candidate.artifactId !== expected.artifactId ||
    candidate.artifactDigest !== expected.artifactDigest ||
    candidate.workflow !== ".github/workflows/artifact-desktop-mac.yml"
  )
    throw new ReproError("The exact artifact binding did not match.");
  return candidate;
}

const native = NodeUtil.promisify(NodeChildProcess.execFile);
async function runNative(file: string, args: string[]) {
  try {
    return (await native(file, args, { timeout: 60_000, maxBuffer: 2 * 1024 * 1024 })).stdout;
  } catch {
    throw new ReproError(`Hosted fixture ${NodePath.basename(file)} ${args[0]} failed.`);
  }
}

async function confirmImageAbsent(image: string, mount: string) {
  const plist = await runNative("/usr/bin/hdiutil", ["info", "-plist"]);
  const inventory = NodeChildProcess.spawnSync(
    "/usr/bin/plutil",
    ["-convert", "json", "-o", "-", "-"],
    {
      input: plist,
      encoding: "utf8",
      timeout: 10_000,
      maxBuffer: 2 * 1024 * 1024,
    },
  );
  if (inventory.status !== 0)
    throw new ReproError("Hosted fixture inventory could not be decoded.");
  const value = JSON.parse(inventory.stdout) as { images?: unknown };
  if (
    !Array.isArray(value.images) ||
    value.images.some(
      (item) =>
        typeof item?.["image-path"] !== "string" ||
        item["image-path"] === image ||
        !Array.isArray(item["system-entities"]) ||
        item["system-entities"].some(
          (entity: { "mount-point"?: unknown }) => entity?.["mount-point"] === mount,
        ),
    )
  )
    throw new ReproError("Hosted fixture attachment absence could not be proved.");
}

const { values } = NodeUtil.parseArgs({
  options: { "worker-dmg": { type: "string" } },
});

async function main() {
  if (
    process.env.GITHUB_ACTIONS !== "true" ||
    process.env.RUNNER_ENVIRONMENT !== "github-hosted" ||
    process.platform !== host.platform ||
    process.arch !== host.architecture
  )
    throw new ReproError("The exact stage reproduction requires a hosted Apple Silicon runner.");

  const diagnostic = values["worker-dmg"] !== undefined;
  const mode = diagnostic ? "diagnostic" : "original";
  const workerSource = diagnostic ? process.env.JONES_SOURCE_SHA : producer.source;
  if (workerSource === undefined || !/^[a-f0-9]{40}$/.test(workerSource))
    throw new ReproError("The packaged worker source was not bound.");

  const client = new JonesActionsClient();
  const sourceCandidate = await exactCandidate(
    client,
    "2dd92e767b0317bf5eefd8848bbedaf20dd0e83f",
    producer,
  );
  const candidate = await exactCandidate(client, producer.source, target);
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-stage-repro-873-")),
  );
  const mount = NodePath.join(root, "producer");
  let image: string | undefined;
  let attached = false;
  let releaseProved = true;
  try {
    const baseline = await client.stage(sourceCandidate, NodePath.join(root, "baseline"));
    image = await NodeFSP.realpath(values["worker-dmg"] ?? baseline.payloadPath);
    await NodeFSP.mkdir(mount);
    await confirmImageAbsent(image, mount);
    await runNative("/usr/bin/hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-noautoopen",
      "-mountpoint",
      mount,
      image,
    ]);
    attached = true;
    const apps = (await NodeFSP.readdir(mount)).filter((name) => name.endsWith(".app"));
    if (apps.length !== 1) throw new ReproError("The worker DMG must contain exactly one app.");
    const contents = NodePath.join(mount, apps[0]!, "Contents");
    const executable = (
      await runNative("/usr/bin/plutil", [
        "-extract",
        "CFBundleExecutable",
        "raw",
        "-o",
        "-",
        NodePath.join(contents, "Info.plist"),
      ])
    ).trim();
    if (!/^[a-zA-Z0-9 ._-]+$/.test(executable) || executable === "." || executable === "..")
      throw new ReproError("The worker bundle executable name is invalid.");
    const worker = NodePath.join(contents, "MacOS", executable);
    const entry = NodePath.join(contents, "Resources", "app.asar", "apps/server/dist/bin.mjs");

    const { payloadDirectory: _payloadDirectory, ...binding } =
      qualifiedRuntimeArtifactFromJonesStage(baseline, root);
    const home = await makePreparationHome({ protocol: 1, ...binding });
    try {
      const payload = await client.stage(candidate, home.cacheRoot);
      const jobId = NodeCrypto.randomUUID();
      const phases = new Set<PreparationPhase>();
      const observation: { exit?: PreparationWorkerExit } = {};
      const supervisor = makePreparationSupervisor({
        baseDir: home.base,
        cacheRoot: home.cacheRoot,
        command: { file: worker, args: [entry] },
        env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
        observe: (observed) => {
          if (observed.jobId === jobId) observation.exit = observed;
        },
      });
      releaseProved = false;
      const job = supervisor.start(
        {
          kind: "stage",
          candidate,
          cacheRoot: home.cacheRoot,
          dbPath: home.dbPath,
          activeVersion: binding.version,
          host,
        },
        { jobId, onPhase: (phase) => phases.add(phase) },
      );
      const result = await job.outcome.then(
        (outcome) => ({ outcome }) as const,
        (error: unknown) => ({ error }) as const,
      );
      const join = await job.settled;
      releaseProved =
        join.kind === "completed" || join.kind === "failed" || join.kind === "not-started";
      const retained = !releaseProved
        ? decodePreparationResult(
            await readBoundedJson(
              NodePath.join(preparationJobsDirectory(home.base), jobId, RESULT_FILE),
              PREPARATION_LIMITS.resultBytes,
            ).catch(() => undefined),
            jobId,
            "stage",
          )
        : undefined;
      const failure =
        retained?.ok === false
          ? retained.error
          : "error" in result && result.error instanceof Error
            ? result.error
            : undefined;
      const message = failure?.message ?? "";
      const mountHeld = message.startsWith("DMG mount ownership needs reconciliation");
      const labels =
        /Staging=(attach|inspect-mounted-app|copy-app|verify-copied-app|completed) cleanup=(before-detach|after-ordinary-detach|after-forced-detach)\/(read|hdiutil-info|size-bound|plutil-conversion|inventory-shape|ownership|attachment-remains) failed-detach=(none|ordinary|forced|ordinary,forced)\.$/.exec(
          message,
        );
      const reason = failure && "reason" in failure ? failure.reason : undefined;
      const observed = observation.exit;
      const isolated =
        observed?.reaped === true &&
        observed.pid !== undefined &&
        observed.pid !== process.pid &&
        isLowestWorkerPriority(observed.priority, "darwin");
      console.log(
        JSON.stringify({
          mode,
          workerSource,
          host: { ...host, release: NodeOS.release() },
          target,
          runAttempt: 1,
          dmgSha256: payload.receipt.sha256,
          producerDmgSha256: baseline.receipt.sha256,
          jobId,
          phases: [...phases],
          join: join.kind,
          isolated,
          result: "outcome" in result ? "staged" : mountHeld ? "mount-ownership-held" : "failed",
          reason,
          diagnostic:
            labels === null
              ? undefined
              : {
                  staging: labels[1],
                  cleanupRound: labels[2],
                  cleanupStep: labels[3],
                  failedDetach: labels[4],
                },
          supervisorReason:
            "error" in result && result.error instanceof PreparationWorkerError
              ? result.error.reason
              : undefined,
        }),
      );
      if (!releaseProved)
        throw new ReproError("Native or worker release is unconfirmed. Scratch was retained.");
      releaseProved = false;
      const sweep = await supervisor.sweep();
      if (sweep.failed !== 0 || (sweep.retainedJobIds?.length ?? 0) !== 0)
        throw new ReproError("The hosted preparation sweep could not confirm cleanup.");
      releaseProved = true;
      if (!isolated || !phases.has("verifying"))
        throw new ReproError("The packaged stage was not observed.");
      if ("outcome" in result) {
        const receipt = result.outcome.staged.receipt;
        if (receipt.sourceSha !== target.source || receipt.archiveSha256 !== payload.receipt.sha256)
          throw new ReproError("The staged runtime receipt did not match the bound payload.");
      } else if (!mountHeld)
        throw new ReproError("The stage did not reach the expected incident outcome.");
    } finally {
      if (releaseProved) await home.remove();
    }
  } finally {
    if (releaseProved) {
      if (attached) await runNative("/usr/bin/hdiutil", ["detach", mount]);
      if (image !== undefined) await confirmImageAbsent(image, mount);
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  }
}

await main().catch((error: unknown) => {
  console.error(error instanceof ReproError ? error.message : "Hosted reproduction failed.");
  process.exitCode = 1;
});
