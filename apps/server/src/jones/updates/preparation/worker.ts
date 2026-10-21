// @effect-diagnostics nodeBuiltinImport:off globalTimers:off
// The preparation worker is its own process: it owns its signals, stdio and exit. `bin.ts`
// dispatches here before the CLI module graph (and its signal handling) loads.
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { JonesActionsClient } from "@t3tools/shared/jones/jonesActions";
import {
  verifyStagedQualifiedRuntime,
  withQualifiedRuntimeLock,
} from "../../cloud/qualifiedRuntime.ts";
import {
  decodePreparationRequest,
  describePreparationFailure,
  PREPARATION_LIMITS,
  PREPARATION_LOCK,
  PREPARATION_PROTOCOL,
  readBoundedJson,
  REQUEST_FILE,
  RESULT_FILE,
  writeDurableJson,
  type PreparationEvent,
  type PreparationOutcome,
  type PreparationPhase,
  type PreparationRequest,
  type PreparationResult,
} from "./protocol.ts";
import { stageQualifiedArtifact } from "./stageArtifact.ts";
import { sweepPreparationLeftovers } from "./sweep.ts";

const HEARTBEAT_MS = 2_000;
/** Below the supervisor's grace, so a worker that cannot unwind still ends its own group first. */
const HARD_EXIT_MS = 20_000;

class PreparationAborted extends Error {
  readonly reason: "cancelled" | "parent-exited" | "deadline";
  constructor(reason: PreparationAborted["reason"]) {
    super(
      reason === "deadline"
        ? "Update preparation exceeded its deadline and was stopped."
        : reason === "parent-exited"
          ? "Update preparation stopped because its server exited."
          : "Update preparation was cancelled.",
    );
    this.name = "PreparationAborted";
    this.reason = reason;
  }
}

/** Heartbeats are droppable: a full pipe skips them until it drains instead of buffering. */
function eventWriter(stream: NodeJS.WritableStream) {
  let blocked = false;
  let broken = false;
  stream.on("drain", () => {
    blocked = false;
  });
  stream.on("error", () => {
    broken = true;
  });
  return (event: PreparationEvent) => {
    if (blocked || broken) return;
    blocked = !stream.write(`${JSON.stringify(event)}\n`);
  };
}

async function runJob(
  request: PreparationRequest,
  jobDir: string,
  signal: AbortSignal,
  phase: (next: PreparationPhase) => void,
): Promise<PreparationOutcome> {
  const { job } = request;
  if (job.kind === "sweep") {
    phase("sweeping");
    return {
      kind: "sweep",
      ...(await sweepPreparationLeftovers({
        baseDir: request.baseDir,
        cacheRoot: job.cacheRoot,
        keepJob: request.jobId,
      })),
    };
  }
  if (job.kind === "verify") {
    phase("verifying");
    const staged = await verifyStagedQualifiedRuntime(
      request.baseDir,
      job.activeVersion,
      job.stagedHandle,
      job.host,
      { signal },
    );
    return { kind: "verify", staged };
  }
  phase("downloading");
  const artifact = await new JonesActionsClient().stage(job.candidate, job.cacheRoot);
  signal.throwIfAborted();
  phase("verifying");
  const { value, leftover } = await stageQualifiedArtifact({
    artifact,
    baseDir: request.baseDir,
    dbPath: job.dbPath,
    activeVersion: job.activeVersion,
    host: job.host,
    scratchParent: jobDir,
    signal,
  });
  const { migrationPlan, ...staged } = value;
  return {
    kind: "stage",
    staged,
    ...(migrationPlan === undefined ? {} : { migrationPlan }),
    ...(leftover === undefined ? {} : { leftover: leftover.path }),
  };
}

/**
 * Runs the job described by `<jobDir>/request.json` and publishes `<jobDir>/result.json`.
 * Returns the exit code: 0 once a result is durable, otherwise non-zero.
 */
async function runPreparationWorker(jobDir: string): Promise<number> {
  try {
    NodeOS.setPriority(NodeOS.constants.priority.PRIORITY_LOW);
  } catch {
    // The supervisor also lowers the priority; neither is a CPU cap.
  }
  let priority: number | null = null;
  try {
    priority = NodeOS.getPriority();
  } catch {
    priority = null;
  }
  let request: PreparationRequest | undefined;
  try {
    request = decodePreparationRequest(
      await readBoundedJson(NodePath.join(jobDir, REQUEST_FILE), PREPARATION_LIMITS.requestBytes),
      jobDir,
    );
  } catch {
    request = undefined;
  }
  if (request === undefined) return 65;
  const processGroup = request.processGroup;
  const emit = eventWriter(process.stdout);
  const controller = new AbortController();
  const timers: NodeJS.Timeout[] = [];
  const signalGroup = (signal: NodeJS.Signals) => {
    try {
      process.kill(-process.pid, signal);
    } catch {
      // The group is already gone.
    }
  };
  const abort = (reason: PreparationAborted["reason"]) => {
    if (controller.signal.aborted) return;
    controller.abort(new PreparationAborted(reason));
    // gh, the candidate preflight and Darwin tools run in this group; stop them with the job.
    if (processGroup) signalGroup("SIGTERM");
    const hardExit = setTimeout(() => {
      if (processGroup) signalGroup("SIGKILL");
      process.exit(75);
    }, HARD_EXIT_MS);
    timers.push(hardExit);
  };
  process.on("SIGTERM", () => abort("cancelled"));
  process.on("SIGINT", () => abort("cancelled"));
  process.on("SIGHUP", () => abort("parent-exited"));
  // The parent holds the write end of stdin for the worker's whole life; EOF means it exited.
  process.stdin.on("end", () => abort("parent-exited"));
  process.stdin.on("error", () => abort("parent-exited"));
  process.stdout.on("error", () => abort("parent-exited"));
  process.stdin.resume();

  let phase: PreparationPhase = "waiting";
  emit({ type: "ready", protocol: PREPARATION_PROTOCOL, pid: process.pid, priority });
  timers.push(setInterval(() => emit({ type: "heartbeat", phase }), HEARTBEAT_MS));
  timers.push(setTimeout(() => abort("deadline"), request.deadlineMs));
  const setPhase = (next: PreparationPhase) => {
    phase = next;
    emit({ type: "phase", phase: next });
  };

  let result: PreparationResult;
  try {
    const outcome = await withQualifiedRuntimeLock(request.baseDir, PREPARATION_LOCK, () =>
      runJob(request, jobDir, controller.signal, setPhase),
    );
    result = { protocol: PREPARATION_PROTOCOL, jobId: request.jobId, ok: true, outcome };
  } catch (error) {
    result = {
      protocol: PREPARATION_PROTOCOL,
      jobId: request.jobId,
      ok: false,
      error: describePreparationFailure(
        controller.signal.aborted ? controller.signal.reason : error,
      ),
    };
  }
  try {
    await writeDurableJson(
      NodePath.join(jobDir, RESULT_FILE),
      result,
      PREPARATION_LIMITS.resultBytes,
    );
    return 0;
  } catch {
    return 70;
  } finally {
    for (const timer of timers) clearTimeout(timer);
  }
}

/** `bin.ts` entry: `--protocol` answers the qualification probe; a job directory runs one job. */
export async function runPreparationWorkerCommand(args: ReadonlyArray<string>): Promise<void> {
  if (args.length === 1 && args[0] === "--protocol") {
    process.stdout.write(`${JSON.stringify({ protocol: PREPARATION_PROTOCOL })}\n`);
    return;
  }
  const jobDir = args.length === 1 ? args[0] : undefined;
  process.exitCode =
    jobDir !== undefined && NodePath.isAbsolute(jobDir) ? await runPreparationWorker(jobDir) : 64;
  // stdin stays open to observe the parent; the result is durable, so exit now.
  process.exit();
}
