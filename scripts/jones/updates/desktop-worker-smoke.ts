// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off - Hosted packaging qualification, outside the application runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";
import {
  createJonesStageAttempt,
  JONES_ACTIONS_RECEIPT_FILE,
  JonesActionsClient,
  publishJonesCompletedStage,
  type JonesActionsCandidate,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";

const REPOSITORY = "Jones-Systems/Jones-Code";
const WORKER_ENTRY = "apps/desktop/dist-electron/jones/updates/jonesStagingWorker.cjs";
const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
// Exercise protocol v1 as an external caller, without loading the unpackaged desktop runtime.
type JonesStagingJob =
  | { kind: "reclaim"; updatesRoot: string; handle: string }
  | { kind: "validate-artifact"; directory: string }
  | {
      kind: "prepare";
      candidate: JonesActionsCandidate;
      roots: { artifacts: string; apps: string };
      platform: "darwin";
    };
type JonesStagingWorkerMessage =
  | { type: "phase"; phase: "verifying" }
  | { type: "result"; value: unknown }
  | { type: "error"; error: { name: string; reason?: string } };
type JonesPreparedStage = {
  artifact: JonesStagedArtifact;
  app: { sourceSha: string; sourceTree: string; version: string; appPath: string };
};
type Status = "pass" | "fail" | "unknown";
type Observation = { status: Status; detail: string };
type Exit = { code: number | null; signal: string | null };
type JobEvidence = {
  kind: JonesStagingJob["kind"];
  pid?: number;
  priority?: number;
  phases: string[];
  reply?: "result" | "error";
  reason?: string;
  exit?: Exit;
  childAbsence?: Status;
  groupAbsence?: Status;
  timedOut: boolean;
  forcedCleanup: boolean;
};

function requireProof(condition: unknown, detail: string): asserts condition {
  if (!condition) throw new Error(detail);
}

function record(value: unknown): Record<string, unknown> {
  requireProof(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "Expected an object.",
  );
  return value as Record<string, unknown>;
}

export function verifyPackagedIdentity(
  value: unknown,
  expected: { source: string; tree: string; version: string },
): void {
  const probe = record(value);
  const metadata = record(probe.metadata);
  const source = record(metadata.jonesSource);
  requireProof(
    /^[a-f0-9]{40}$/.test(expected.source) &&
      /^[a-f0-9]{40}$/.test(expected.tree) &&
      metadata.version === expected.version &&
      source.repository === REPOSITORY &&
      source.sha === expected.source &&
      source.tree === expected.tree &&
      typeof probe.electron === "string" &&
      probe.electron.length > 0 &&
      probe.platform === "darwin" &&
      probe.arch === "arm64" &&
      probe.workerEntry === WORKER_ENTRY &&
      typeof probe.workerSha256 === "string" &&
      /^[a-f0-9]{64}$/.test(probe.workerSha256),
    "Packaged Electron, desktop worker, version, source or tree binding failed.",
  );
}

export function processAbsence(
  pid: number,
  probe: (pid: number, signal: 0) => unknown = process.kill,
): Status {
  try {
    probe(pid, 0);
    return "fail";
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "ESRCH" ? "pass" : "unknown";
  }
}

export function qualificationCancellation(
  target: Pick<NodeJS.EventEmitter, "once" | "removeListener"> = process,
): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("Hosted qualification cancelled."));
  target.once("SIGTERM", cancel);
  target.once("SIGINT", cancel);
  return {
    signal: controller.signal,
    dispose: () => {
      target.removeListener("SIGTERM", cancel);
      target.removeListener("SIGINT", cancel);
    },
  };
}

export async function withSmokeScratch<T>(
  parent: string,
  operation: (root: string) => Promise<T>,
  beforeRemove: (root: string) => Promise<void> = async () => {},
): Promise<T> {
  const canonicalParent = await NodeFSP.realpath(parent);
  const root = await NodeFSP.mkdtemp(NodePath.join(canonicalParent, "desktop-worker-"));
  const identity = await NodeFSP.lstat(root);
  let result: T | undefined;
  let failure: unknown;
  try {
    result = await operation(root);
  } catch (cause) {
    failure = cause;
  }
  try {
    await beforeRemove(root);
    const current = await NodeFSP.lstat(root);
    requireProof(
      current.isDirectory() &&
        current.ino === identity.ino &&
        current.dev === identity.dev &&
        (await NodeFSP.realpath(root)) === root,
      "Scratch ownership changed; preserved.",
    );
    await NodeFSP.rm(root, { recursive: true });
    requireProof(!NodeFS.existsSync(root), "Task scratch remains after removal.");
  } catch (cleanup) {
    throw failure === undefined
      ? cleanup
      : new AggregateError([failure, cleanup], "Qualification failed and scratch cleanup failed.");
  }
  if (failure !== undefined) throw failure;
  return result as T;
}

async function command(
  file: string,
  args: string[],
  env?: NodeJS.ProcessEnv,
  signal?: AbortSignal,
) {
  const result = await execFile(file, args, {
    encoding: "utf8",
    maxBuffer: 2 * 1024 * 1024,
    timeout: 60_000,
    ...(env === undefined ? {} : { env }),
    ...(signal === undefined ? {} : { signal }),
  });
  return result.stdout;
}

export async function probePackagedIdentity(
  executable: string,
  asar: string,
  env: NodeJS.ProcessEnv,
  signal?: AbortSignal,
): Promise<unknown> {
  let probe: string;
  try {
    probe = await command(
      executable,
      [
        "-e",
        `
        const fs = require('node:fs'), crypto = require('node:crypto');
        const asar = process.argv[1], entry = process.argv[2];
        process.stdout.write(JSON.stringify({
          metadata: JSON.parse(fs.readFileSync(asar + '/package.json', 'utf8')),
          electron: process.versions.electron, platform: process.platform, arch: process.arch,
          workerEntry: entry, workerSha256: crypto.createHash('sha256').update(fs.readFileSync(asar + '/' + entry)).digest('hex')
        }));`,
        asar,
        WORKER_ENTRY,
      ],
      env,
      signal,
    );
  } catch (cause) {
    const failure = cause as { code?: unknown; signal?: unknown; stderr?: unknown };
    const stderr = typeof failure.stderr === "string" ? failure.stderr.trim().slice(0, 512) : "";
    throw new Error(
      `Packaged identity probe failed (${String(failure.code ?? failure.signal ?? "unknown")}): ${stderr}`,
      { cause },
    );
  }
  return JSON.parse(probe) as unknown;
}

function taskMounts(root: string): string[] {
  const plist = NodeChildProcess.execFileSync("/usr/bin/hdiutil", ["info", "-plist"], {
    encoding: "utf8",
    maxBuffer: 2 * 1024 * 1024,
    timeout: 30_000,
  });
  const info = JSON.parse(
    NodeChildProcess.execFileSync("/usr/bin/plutil", ["-convert", "json", "-o", "-", "-"], {
      input: plist,
      encoding: "utf8",
      maxBuffer: 2 * 1024 * 1024,
      timeout: 30_000,
    }),
  ) as { images?: Array<{ "system-entities"?: Array<{ "mount-point"?: string }> }> };
  requireProof(Array.isArray(info.images), "Cannot prove task mount absence.");
  return info.images.flatMap((image) =>
    (image["system-entities"] ?? []).flatMap((entity) => {
      const mount = entity["mount-point"];
      return typeof mount === "string" && mount.startsWith(`${root}${NodePath.sep}`) ? [mount] : [];
    }),
  );
}

async function waitUntil(check: () => boolean, milliseconds: number): Promise<boolean> {
  const deadline = Date.now() + milliseconds;
  do {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 25));
  } while (Date.now() < deadline);
  return check();
}

async function runJob(
  runtime: { executable: string; entry: string; env: NodeJS.ProcessEnv },
  job: JonesStagingJob,
  jobs: JobEvidence[],
  signal: AbortSignal,
): Promise<JonesStagingWorkerMessage> {
  signal.throwIfAborted();
  const evidence: JobEvidence = {
    kind: job.kind,
    phases: [],
    timedOut: false,
    forcedCleanup: false,
  };
  jobs.push(evidence);
  // Match the production fork boundary; this driver does not qualify the client's queue or close grace.
  const child = NodeChildProcess.fork(runtime.entry, [], {
    execPath: runtime.executable,
    execArgv: [],
    detached: true,
    env: runtime.env,
    stdio: ["ignore", "ignore", "ignore", "ipc"],
  });
  if (child.pid !== undefined) evidence.pid = child.pid;
  let failure: Error | undefined;
  let reply: JonesStagingWorkerMessage | undefined;
  let terminalReplies = 0;
  child.on("error", () => {
    failure = new Error("Packaged worker spawn or IPC failed.");
  });
  child.on("exit", (code, exitSignal) => {
    evidence.exit = { code, signal: exitSignal };
  });
  child.on("message", (raw) => {
    try {
      const message = raw as JonesStagingWorkerMessage;
      if (message.type === "phase" && message.phase === "verifying" && evidence.phases.length < 4)
        evidence.phases.push(message.phase);
      else if (message.type === "result" || message.type === "error") {
        terminalReplies++;
        reply = message;
        evidence.reply = message.type;
        if (message.type === "error") evidence.reason = message.error.reason ?? message.error.name;
      } else throw new Error("Unknown packaged worker IPC message.");
    } catch {
      failure = new Error("Unknown packaged worker IPC message.");
    }
  });
  const pid = child.pid;
  try {
    requireProof(pid !== undefined && pid !== process.pid, "No distinct packaged worker PID.");
    const ready = await waitUntil(() => {
      if (failure || evidence.exit || signal.aborted) return true;
      try {
        evidence.priority = NodeOS.getPriority(pid);
      } catch {
        return false;
      }
      return evidence.priority >= NodeOS.constants.priority.PRIORITY_LOW && evidence.priority <= 20;
    }, 15_000);
    signal.throwIfAborted();
    requireProof(
      ready &&
        !failure &&
        evidence.exit === undefined &&
        evidence.priority !== undefined &&
        evidence.priority >= NodeOS.constants.priority.PRIORITY_LOW &&
        evidence.priority <= 20,
      "Packaged worker low priority was not observed before dispatch.",
    );
    child.send({ protocol: 1, job }, (error) => {
      if (error) failure = new Error("Packaged worker request delivery failed.");
    });
    const completed = await waitUntil(
      () => Boolean(failure || evidence.exit || signal.aborted),
      job.kind === "prepare" ? 20 * 60_000 : 60_000,
    );
    if (!completed) evidence.timedOut = true;
    signal.throwIfAborted();
    if (failure) throw failure;
    const exit = evidence.exit as Exit | undefined;
    requireProof(
      completed && exit?.code === 0 && exit.signal === null,
      "Packaged worker did not exit normally inside the job deadline.",
    );
    requireProof(
      terminalReplies === 1 && reply !== undefined,
      "Expected exactly one terminal IPC reply.",
    );
    evidence.childAbsence = processAbsence(pid);
    evidence.groupAbsence = processAbsence(-pid);
    requireProof(
      evidence.childAbsence === "pass" && evidence.groupAbsence === "pass",
      "Normal exit did not prove captured child and process-group absence.",
    );
    return reply;
  } finally {
    if (pid !== undefined) {
      evidence.childAbsence = processAbsence(pid);
      evidence.groupAbsence = processAbsence(-pid);
      if (evidence.groupAbsence === "fail") {
        // Harness emergency cleanup is recorded separately and can never make a lifecycle assertion pass.
        evidence.forcedCleanup = true;
        try {
          process.kill(-pid, "SIGTERM");
        } catch {}
        if (!(await waitUntil(() => processAbsence(-pid) === "pass", 12_000))) {
          try {
            process.kill(-pid, "SIGKILL");
          } catch {}
          await waitUntil(() => processAbsence(-pid) === "pass", 5_000);
        }
      }
    }
  }
}

async function qualifiedCandidate(env: NodeJS.ProcessEnv, signal: AbortSignal) {
  const api = async (endpoint: string) =>
    JSON.parse(
      await command(
        "gh",
        ["api", "--hostname", "github.com", "--method", "GET", `repos/${REPOSITORY}/${endpoint}`],
        env,
        signal,
      ),
    ) as unknown;
  try {
    const runs = record(
      await api(
        "actions/workflows/artifact-desktop-mac.yml/runs?branch=main&event=push&status=success&per_page=10",
      ),
    );
    const anchor = Array.isArray(runs.workflow_runs) ? runs.workflow_runs.at(-1) : undefined;
    if (anchor === undefined)
      return { state: "blocked", reason: "no-successful-main-artifact-run" } as const;
    const source = record(anchor).head_sha;
    requireProof(
      typeof source === "string" && /^[a-f0-9]{40}$/.test(source),
      "Invalid source from Actions.",
    );
    const commit = record(await api(`commits/${source}`));
    const parent = Array.isArray(commit.parents) ? record(commit.parents[0]).sha : undefined;
    requireProof(
      typeof parent === "string" && /^[a-f0-9]{40}$/.test(parent),
      "No public predecessor for the staging fixture.",
    );
    // The public predecessor is synthetic installed identity. check() and the packaged stage revalidate all trust.
    return await new JonesActionsClient({
      transport: {
        api: (endpoint) => {
          requireProof(
            endpoint.startsWith(`repos/${REPOSITORY}/`),
            "Unexpected qualification endpoint.",
          );
          return api(endpoint.slice(`repos/${REPOSITORY}/`.length));
        },
        download: async () => {
          throw new Error("Only the packaged worker may stage the artifact.");
        },
      },
    }).check({
      installedSource: parent,
      platform: "darwin",
      architecture: "arm64",
    });
  } catch {
    return { state: "blocked", reason: "candidate-discovery-unavailable" } as const;
  }
}

async function main() {
  const { values } = NodeUtil.parseArgs({
    options: Object.fromEntries(
      ["dmg", "source", "tree", "version", "scratch-parent", "report"].map((name) => [
        name,
        { type: "string" as const },
      ]),
    ),
    strict: true,
    allowPositionals: false,
  });
  const required = (name: string) => {
    const value = values[name];
    requireProof(typeof value === "string" && value.length > 0, `Missing --${name}.`);
    return value;
  };
  const expected = {
    source: required("source"),
    tree: required("tree"),
    version: required("version"),
  };
  const dmg = NodePath.resolve(required("dmg"));
  const reportPath = NodePath.resolve(required("report"));
  requireProof(
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Hosted smoke admission observes its physical OS before spawning the packaged runtime.
    NodeOS.platform() === "darwin" &&
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Hosted smoke admission observes its physical architecture before spawning the packaged runtime.
      NodeOS.arch() === "arm64" &&
      process.env.GITHUB_ACTIONS === "true" &&
      process.env.GITHUB_REPOSITORY === REPOSITORY &&
      process.env.GITHUB_SHA === expected.source,
    "This qualification runs only on the exact-source hosted Apple Silicon job.",
  );
  const report = {
    schema: "jones-desktop-worker-smoke/v1",
    expected,
    dmg,
    workerEntry: WORKER_ENTRY,
    identity: undefined as unknown,
    jobs: [] as JobEvidence[],
    core: { status: "unknown", detail: "Not completed." } as Observation,
    staging: { status: "unknown", detail: "Not attempted." } as Observation,
    candidate: undefined as unknown,
    failure: undefined as string | undefined,
    cleanup: { status: "unknown", detail: "Not completed." } as Observation,
    held: {
      cancellationAndQueue:
        "unproved: production client grace and queue are outside this normal-job proof",
      signing: "unproved: no signing, signature repair, Gatekeeper or GUI acceptance",
    },
  };
  const abort = qualificationCancellation();
  let failed = false;
  let scratch: string | undefined;
  try {
    await withSmokeScratch(
      required("scratch-parent"),
      async (root) => {
        scratch = root;
        const mount = NodePath.join(root, "input-dmg");
        const updates = NodePath.join(root, "updates");
        const roots = {
          artifacts: NodePath.join(updates, "artifacts"),
          apps: NodePath.join(updates, "apps"),
        };
        const ghConfig = NodePath.join(root, "gh");
        const temporary = NodePath.join(root, "tmp");
        for (const directory of [mount, ghConfig, temporary, roots.artifacts, roots.apps])
          await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
        const env: NodeJS.ProcessEnv = {
          PATH: process.env.PATH,
          TMPDIR: temporary,
          GH_CONFIG_DIR: ghConfig,
          GH_PROMPT_DISABLED: "1",
          ...(process.env.GH_TOKEN ? { GH_TOKEN: process.env.GH_TOKEN } : {}),
          ELECTRON_RUN_AS_NODE: "1",
        };
        await command(
          "/usr/bin/hdiutil",
          ["attach", "-readonly", "-nobrowse", "-noautoopen", "-mountpoint", mount, dmg],
          env,
          abort.signal,
        );
        requireProof(taskMounts(root).includes(mount), "Exact task DMG mount was not observed.");
        const apps = (await NodeFSP.readdir(mount, { withFileTypes: true })).filter(
          (entry) => entry.isDirectory() && entry.name.endsWith(".app"),
        );
        requireProof(apps.length === 1, "Expected one packaged app in the DMG.");
        const app = NodePath.join(mount, apps[0]!.name);
        const executableName = (
          await command("/usr/bin/plutil", [
            "-extract",
            "CFBundleExecutable",
            "raw",
            "-o",
            "-",
            NodePath.join(app, "Contents/Info.plist"),
          ])
        ).trim();
        requireProof(/^[^/\\]+$/.test(executableName), "Invalid packaged executable name.");
        const executable = NodePath.join(app, "Contents/MacOS", executableName);
        const asar = NodePath.join(app, "Contents/Resources/app.asar");
        const entry = NodePath.join(asar, WORKER_ENTRY);
        const identity = await probePackagedIdentity(executable, asar, env, abort.signal);
        verifyPackagedIdentity(identity, expected);
        const parsedIdentity = record(identity);
        report.identity = {
          ...expected,
          electron: parsedIdentity.electron,
          workerSha256: parsedIdentity.workerSha256,
        };
        const runtime = { executable, entry, env };
        const run = (job: JonesStagingJob) => runJob(runtime, job, report.jobs, abort.signal);
        const handle = NodeCrypto.createHash("sha256")
          .update("desktop-worker-reclaim-fixture")
          .digest("hex");
        const attempt = await createJonesStageAttempt(roots.artifacts, handle);
        await NodeFSP.writeFile(
          NodePath.join(attempt.directory, "owned-payload"),
          "qualification fixture",
        );
        await NodeFSP.writeFile(NodePath.join(attempt.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
        await publishJonesCompletedStage(
          roots.artifacts,
          handle,
          attempt.directory,
          JONES_ACTIONS_RECEIPT_FILE,
        );
        const reclaimed = await run({ kind: "reclaim", updatesRoot: updates, handle });
        requireProof(
          reclaimed.type === "result" &&
            record(reclaimed.value).status === "reclaimed" &&
            !NodeFS.existsSync(attempt.attemptRoot),
          "Packaged worker did not reclaim its exact fixture payload.",
        );
        const absent = await run({ kind: "reclaim", updatesRoot: updates, handle });
        requireProof(
          absent.type === "result" && record(absent.value).status === "absent",
          "Reclaimed payload is not absent.",
        );
        const refused = await run({ kind: "validate-artifact", directory: roots.artifacts });
        requireProof(
          refused.type === "error" && refused.error.reason === "occupied-cache",
          "Packaged worker accepted an invalid artifact stage.",
        );
        report.core = {
          status: "pass",
          detail:
            "Exact packaged source/entry, low priority, reclaim/absence, invalid-stage refusal, normal IPC/exit and group absence.",
        };
        if (!env.GH_TOKEN) {
          report.staging.detail = "No ephemeral job token; canonical-main staging unproved.";
          return;
        }
        const selection = await qualifiedCandidate(env, abort.signal);
        abort.signal.throwIfAborted();
        if (selection.state !== "available") {
          report.staging.detail = `Canonical-main staging unproved: ${selection.state}${"reason" in selection ? `/${selection.reason}` : ""}.`;
          return;
        }
        report.candidate = selection.candidate;
        report.staging = {
          status: "fail",
          detail: "Qualified candidate selected; prepare has not completed.",
        };
        const prepared = await run({
          kind: "prepare",
          candidate: selection.candidate,
          roots,
          platform: "darwin",
        });
        if (
          prepared.type === "error" &&
          prepared.error.reason !== undefined &&
          [
            "gh-missing",
            "authentication-required",
            "rate-limited",
            "unavailable",
            "unqualified",
          ].includes(prepared.error.reason)
        ) {
          report.staging = {
            status: "unknown",
            detail: `Canonical-main staging unproved; packaged trust/transport refusal: ${prepared.error.reason}.`,
          };
          return;
        }
        requireProof(
          prepared.type === "result",
          `Canonical-main prepare failed: ${prepared.type === "error" ? (prepared.error.reason ?? prepared.error.name) : "no result"}.`,
        );
        const stage = prepared.value as JonesPreparedStage;
        requireProof(
          stage.app.sourceSha === selection.candidate.source &&
            stage.app.sourceTree === selection.candidate.tree &&
            stage.app.version === selection.candidate.version &&
            stage.artifact.candidate.artifactId === selection.candidate.artifactId &&
            report.jobs.at(-1)?.phases.includes("verifying") &&
            taskMounts(root).every((path) => path === mount),
          "Prepared app identity, verifying phase or stage mount cleanup failed.",
        );
        const checked = await run({
          kind: "validate-artifact",
          directory: NodePath.dirname(stage.artifact.payloadPath),
        });
        requireProof(
          checked.type === "result",
          "Packaged revalidation of the canonical stage failed.",
        );
        const removed = await run({
          kind: "reclaim",
          updatesRoot: updates,
          handle: stage.artifact.stagedHandle,
        });
        requireProof(
          removed.type === "result" &&
            record(removed.value).status === "reclaimed" &&
            !NodeFS.existsSync(stage.app.appPath) &&
            !NodeFS.existsSync(stage.artifact.payloadPath),
          "Canonical stage reclaim failed.",
        );
        report.staging = {
          status: "pass",
          detail:
            "Normal trust validation, canonical-main download, app staging, revalidation, reclaim and stage mount absence observed.",
        };
      },
      async (root) => {
        for (const job of report.jobs)
          if (job.pid !== undefined)
            requireProof(
              processAbsence(job.pid) === "pass" && processAbsence(-job.pid) === "pass",
              "Captured worker/group remains or absence is unknown; scratch preserved.",
            );
        for (const mount of taskMounts(root)) await command("/usr/bin/hdiutil", ["detach", mount]);
        requireProof(taskMounts(root).length === 0, "Task DMG mount remains; scratch preserved.");
      },
    );
    report.cleanup = {
      status: "pass",
      detail: "Captured workers/groups and exact task mounts absent; exact scratch root removed.",
    };
  } catch (cause) {
    failed = true;
    const detail = cause instanceof Error ? cause.message.slice(0, 512) : "Qualification failed.";
    report.failure = detail;
    if (report.core.status !== "pass") report.core = { status: "fail", detail };
    else if (report.staging.status === "fail") report.staging.detail = detail;
    if (scratch !== undefined && !NodeFS.existsSync(scratch))
      report.cleanup = {
        status: "pass",
        detail: "Exact scratch root removed after failure; mount/group cleanup guard completed.",
      };
    else
      report.cleanup = {
        status: "unknown",
        detail: "Exact cleanup not proved; task scratch preserved on disposable runner.",
      };
  } finally {
    abort.dispose();
    await NodeFSP.mkdir(NodePath.dirname(reportPath), { recursive: true });
    await NodeFSP.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, { flag: "wx" });
    const summary = `Desktop worker: core=${report.core.status}, canonical-main staging=${report.staging.status}, cleanup=${report.cleanup.status}. Cancellation/queue and signing remain unproved.\n`;
    process.stdout.write(summary);
    if (process.env.GITHUB_STEP_SUMMARY)
      await NodeFSP.appendFile(process.env.GITHUB_STEP_SUMMARY, summary);
    if (report.staging.status === "unknown")
      process.stdout.write(`::warning::${report.staging.detail}\n`);
  }
  process.exitCode = failed ? 1 : 0;
}

if (
  process.argv[1] !== undefined &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
)
  await main();
