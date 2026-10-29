// Manual source diagnostic only. No result from this entry is qualification evidence.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import { classifyCpuEnvelope, classifyResourceInterval } from "../../../apps/server/src/jones/serverParallelism/diagnosticObservation.ts";

const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const schema = "jones.server-parallelism.observer-diagnostic/v1";
const positive = (value) => Number.isFinite(value) && value > 0;
const outputPaths = (output) => Object.fromEntries(
  ["raw", "summary", "error", "trace", "cleanup"].map((kind) => [kind, `${output}.${kind}.json`]),
);

export function validateDiagnosticAdmission(input) {
  const { manifest, baseline, mode, source, sourceTree, build, entry, executable, platform, parallelism, prior, baselineHash } = input;
  if (platform !== "linux" || parallelism < 2 || !["full", "cpu-stat-only"].includes(mode) || !/^[0-9a-f]{40}$/.test(source ?? "") || !/^[0-9a-f]{40}$/.test(sourceTree ?? ""))
    throw new Error("Diagnostic requires Linux source N2/W3/A500/F512 and an exact source/tree");
  if (!manifest || manifest.schema !== "jones.server-parallelism.command/v1" || manifest.runtimeForm !== "source" || manifest.file !== executable || !NodePath.isAbsolute(entry ?? "") || !entry.endsWith("/apps/server/src/bin.ts") || JSON.stringify(manifest.args) !== JSON.stringify(["--max-old-space-size=256", entry, "__jones-server-compute"]) || !/^[a-f0-9]{64}$/.test(build ?? "") || manifest.build !== build || manifest.identityResolver !== NodePath.join(NodePath.dirname(entry), "jones/serverCompute/workerCommand.ts"))
    throw new Error("Unbound source command/build");
  if (!baseline || baseline.schema !== "jones.server-parallelism.baseline/v1" || baseline.source !== source || baseline.profile !== "laptop" || baseline.count !== 2 || baseline.files !== 512 || baseline.acquisitions !== 500 || baseline.windows?.length !== 3 || baseline.churnWindows?.length !== 2 || JSON.stringify(baseline.churnWindows.map((row) => row.workingSet)) !== "[9,16]" || baseline.readerCount !== 1 || baseline.computePlacement !== "main-thread" || baseline.outcome !== "passed" || baseline.nativeScopeClosed !== true || baseline.capturedExit !== true || baseline.cleanupProved !== true || ![baseline.writer?.p95, baseline.writer?.p99, baseline.loop?.p99, ...baseline.churnWindows.flatMap((row) => [row.durationMs, row.latency?.p99])].every(positive))
    throw new Error("Unbound or incomplete unchanged baseline");
  if (mode === "full" && prior !== undefined) throw new Error("Full run cannot consume a prior diagnostic");
  if (mode === "cpu-stat-only" && (!prior || prior.schema !== schema || prior.mode !== "full" || prior.eligible !== false || prior.outcome !== "recorded" || prior.complete !== true || prior.source !== source || prior.sourceTree !== sourceTree || prior.build !== build || prior.baselineHash !== baselineHash || prior.cleanupProved !== true || prior.runtimeForm !== "source" || prior.profile !== "laptop" || prior.bounds?.count !== 2 || prior.bounds.windows !== 3 || prior.bounds.acquisitions !== 500 || prior.bounds.files !== 512 || prior.bounds.fullRuns !== 1 || prior.bounds.controlRunsMaximum !== 1))
    throw new Error("Control requires the separately inspected full run and its unchanged baseline");
}

export function diagnosticCompletion({ report, trace, cleanup, elapsedMs, error, aborted }) {
  return !error && !aborted && elapsedMs <= 600_000 && report?.eligible === false && report.diagnostic?.eligible === false && report.shutdownProved === true && report.retention?.workload?.count === 2 && report.retention.workload.windows === 3 && report.retention.workload.acquisitions === 500 && report.retention.workload.files === 512 && report.retention.workload.workloadMs === 600_000 && [report.retention.observations, report.retention.executions, report.retention.resources, trace, report.diagnostic?.cpuRetention].every((row) => row && row.overflow === false && row.dropped === 0) && cleanup?.shutdownProved === true && cleanup.fixtureRemoved === true;
}

export function summarizeDiagnostic(report, events) {
  const batches = events.filter((row) => row.kind === "resource" && row.stage === "batch");
  const begins = new Map(batches.filter((row) => row.edge === "begin").map((row) => [row.readId, row]));
  const spans = batches.filter((row) => row.edge === "end").map((row) => ({
    readId: row.readId, phase: row.phase, startMs: begins.get(row.readId)?.atMs ?? null,
    endMs: row.atMs, cpu: row.cpu ?? null,
  }));
  const starts = spans.flatMap((row) => row.startMs === null ? [] : [row.startMs]);
  const cadenceMs = starts.slice(1).map((value, index) => value - starts[index]);
  const active = new Map(), writers = [], controls = [], loopStalls = [];
  const correlations = (startMs, endMs) => {
    const matches = events.flatMap((row, index) => row.atMs >= startMs && row.atMs <= endMs && (row.kind === "lifecycle" || (row.kind === "resource" && (row.stage === "batch" || row.edge === "error")) || (row.kind === "qualification" && !["request-begin", "request-end", "request-timing"].includes(row.event))) ? [index] : []);
    return { attempted: matches.length, retained: Math.min(128, matches.length), dropped: Math.max(0, matches.length - 128), eventIndexes: matches.slice(0, 128) };
  };
  for (const row of events) {
    const key = `${row.phase}:${row.index}:${row.event?.split("-")[0]}`;
    if (row.kind === "combined" && ["writer-begin", "control-begin"].includes(row.event)) active.set(key, row.atMs);
    if (row.kind === "combined" && ["writer-end", "control-end"].includes(row.event)) {
      const startMs = active.get(key);
      if (startMs !== undefined) {
        const interval = { phase: row.phase, index: row.index, startMs, endMs: row.atMs, durationMs: row.atMs - startMs };
        (row.event === "writer-end" ? writers : controls).push(interval);
        active.delete(key);
      }
    }
    if (row.kind === "qualification" && row.event === "loop-window-end" && row.loop?.maxMs >= 100) {
      const begin = events.find((item) => item.phase === row.phase && item.kind === "qualification" && item.event === "loop-window-begin");
      loopStalls.push({ phase: row.phase, startMs: begin?.atMs ?? null, endMs: row.atMs, ...row.loop, location: "unknown within official window envelope", correlations: begin ? correlations(begin.atMs, row.atMs) : [] });
    }
  }
  return {
    eligible: false, windows: report?.windows?.map((row) => ({ readerQueue: row.readerQueue, readerExecute: row.readerExecute, writer: row.writer, control: row.control, loop: row.loop, diagnosticCpu: row.diagnosticCpu })) ?? null, churnWindows: report?.churnWindows ?? null,
    nominalSamplingGapMs: 10, cadenceMs, resourceSpans: spans, writerIntervals: writers, controlIntervals: controls,
    writerStalls: writers.filter((row) => row.durationMs >= 100).map((row) => ({ ...row, correlations: correlations(row.startMs, row.endMs) })),
    loopEnvelopeStalls: loopStalls,
    ...classifyDiagnosticReads(report, events),
    correlationMeaning: "Time alignment is a hypothesis, not causal attribution. Resource wall spans and process/thread CPU include concurrent parent work; no subtraction from official results.",
    unavailable: report?.diagnostic?.mode === "cpu-stat-only" ? ["rssBytes", "threads", "inotifyDescriptors", "inotifyWatches"] : [],
  };
}

function classifyDiagnosticReads(report, events) {
  const bindings = new Map(), stages = new Map(), identities = new Map(), expectedStarts = new Map(), pidBrackets = new Map();
  const identityKey = (row) => JSON.stringify([row.pid, row.bootstrap?.version, row.bootstrap?.build, row.bootstrap?.authorityId, row.bootstrap?.slotId, row.bootstrap?.slotGeneration, row.bootstrap?.nonce]);
  for (const row of events) {
    if (row.kind === "resource" && row.stage === "pid") {
      const key = `${row.readId}:${row.pid}`, bracket = pidBrackets.get(key) ?? {};
      if (row.edge === "begin") bracket.startMs = row.atMs;
      if (row.edge === "end") bracket.endMs = row.atMs;
      pidBrackets.set(key, bracket);
    }
    if (row.kind === "resource" && row.stage === "stat" && row.edge === "end") stages.set(`${row.readId}:${row.pid}`, row);
    if (row.bootstrap) {
      const key = identityKey(row), list = identities.get(key) ?? [];
      list.push(row); identities.set(key, list);
    }
  }
  const executions = events.flatMap((row) => row.kind === "qualification" && row.event === "request-timing" && row.timing?.worker?.executionOutcome === "ok" && row.timing.delivery && row.bootstrap ? [{
    pid: row.pid, generation: row.bootstrap.slotGeneration, nonce: row.bootstrap.nonce, requestId: row.requestId, phase: row.phase,
    startMs: row.timing.worker.executionStartedMs + row.timing.delivery.offsetUpperMs,
    endMs: row.timing.worker.executionCompletedMs + row.timing.delivery.offsetLowerMs,
  }] : []);
  const sourceRows = report?.diagnostic?.mode === "cpu-stat-only" ? report.diagnostic.cpuResources.map((row) => ({ ...row, at: row.atMs, finishedAt: row.finishedAtMs })) : report?.resources ?? [];
  const resourceIntervals = [], cpuEnvelopes = [];
  for (const row of sourceRows) {
    for (const binding of row.generations) {
      const key = identityKey(binding), records = identities.get(key) ?? [];
      const stage = stages.get(`${row.readId}:${binding.pid}`), process = row.sample.processes.find((item) => item.pid === binding.pid);
      const observed = stage?.startTimeTicks, previous = bindings.get(key);
      const bracket = pidBrackets.get(`${row.readId}:${binding.pid}`);
      const startMs = bracket?.startMs ?? row.at, endMs = bracket?.endMs ?? row.finishedAt;
      const milestone = (kind, event) => records.find((item) => item.kind === kind && item.event === event)?.atMs;
      resourceIntervals.push({
        readId: row.readId, pid: binding.pid, bootstrap: binding.bootstrap, phase: row.phase,
        startMs, endMs, spanBasis: bracket ? "per-PID collector return; late stages remain separate" : "batch fallback; per-PID bracket unavailable",
        ...classifyResourceInterval({ startMs, endMs, available: process !== undefined, startTimeTicks: observed, expectedStartTimeTicks: expectedStarts.get(key),
          retirementStartedMs: milestone("qualification", "retire-begin"), capturedExitMs: milestone("lifecycle", "captured-exit"), capturedCloseMs: milestone("lifecycle", "captured-close"), retireResolvedMs: milestone("qualification", "retire-resolved"), journalRetiredMs: milestone("qualification", "retired-end") }),
      });
      if (process && observed) {
        if (!expectedStarts.has(key)) expectedStarts.set(key, observed);
        const point = { pid: binding.pid, generation: binding.bootstrap.slotGeneration, nonce: binding.bootstrap.nonce, startTimeTicks: observed, atMs: row.at, finishedAtMs: row.finishedAt, cpuMs: process.cpuMs, phase: row.phase };
        if (expectedStarts.get(key) === observed && previous && previous.phase === row.phase) cpuEnvelopes.push({ pid: binding.pid, generation: point.generation, nonce: point.nonce, phase: row.phase, startMs: previous.atMs, endMs: point.finishedAtMs, ...classifyCpuEnvelope(previous, point, executions.filter((item) => item.phase === row.phase)) });
        if (expectedStarts.get(key) === observed) bindings.set(key, point);
        else {
          cpuEnvelopes.push({ pid: binding.pid, generation: point.generation, nonce: point.nonce, phase: row.phase, readId: row.readId, relation: "identity-mismatch", requestId: null });
          bindings.delete(key);
        }
      } else {
        cpuEnvelopes.push({ pid: binding.pid, generation: binding.bootstrap.slotGeneration, nonce: binding.bootstrap.nonce, phase: row.phase, readId: row.readId, relation: "unavailable", requestId: null });
        bindings.delete(key);
      }
    }
  }
  return { resourceIntervals, cpuEnvelopes, cpuEnvelopeMeaning: "Whole batch brackets; one request only; diagnostic classification never changes official predicates or charge ownership." };
}

export function diagnosticLifecycleComplete(events) {
  const spawns = events.filter((row) => row.kind === "lifecycle" && row.event === "spawn");
  return spawns.length > 0 && spawns.every((spawn) => ["captured-exit", "captured-close", "retired-end"].every((event) => events.some((row) => row.event === event && row.pid === spawn.pid && row.bootstrap?.nonce === spawn.bootstrap?.nonce && row.bootstrap?.slotGeneration === spawn.bootstrap?.slotGeneration)));
}

export async function verifyDiagnosticArtifacts(receipt, output) {
  try {
    if (receipt?.schema !== schema || receipt.eligible !== false || receipt.complete !== true || receipt.outcome !== "recorded" || receipt.cleanupProved !== true) return false;
    const paths = outputPaths(output);
    for (const [kind, path] of Object.entries(paths)) {
      const bytes = await NodeFSP.readFile(path);
      if (receipt.artifacts?.[kind]?.sha256 !== digest(bytes)) return false;
    }
    const raw = JSON.parse(await NodeFSP.readFile(paths.raw, "utf8"));
    const trace = JSON.parse(await NodeFSP.readFile(paths.trace, "utf8"));
    const cleanup = JSON.parse(await NodeFSP.readFile(paths.cleanup, "utf8"));
    const error = JSON.parse(await NodeFSP.readFile(paths.error, "utf8"));
    return trace.retention.retained === trace.events.length && trace.retention.attempted === trace.retention.retained + trace.retention.dropped && diagnosticLifecycleComplete(trace.events) && raw.build === receipt.build && diagnosticCompletion({ report: raw, trace: trace.retention, cleanup, elapsedMs: receipt.elapsedMs, error, aborted: receipt.aborted });
  } catch { return false; }
}

export async function assertNewDiagnosticPaths(output, inputs) {
  const paths = outputPaths(output);
  const destinations = [output, ...Object.values(paths)];
  const canonical = async (path) => NodePath.join(await NodeFSP.realpath(NodePath.dirname(path)), NodePath.basename(path));
  const resolved = await Promise.all([...destinations, ...inputs].map(canonical));
  if (new Set(resolved).size !== resolved.length) throw new Error("Duplicate input/output paths");
  for (const path of destinations) {
    try { await NodeFSP.lstat(path); throw new Error("Diagnostic output already exists"); }
    catch (cause) { if (cause.code !== "ENOENT") throw cause; }
  }
}

async function main(args) {
  const [manifestPath, output, baselinePath, mode, priorPath] = args;
  if ((mode === "full" ? args.length !== 4 : args.length !== 5) || !["full", "cpu-stat-only"].includes(mode) || ![manifestPath, output, baselinePath, ...(priorPath ? [priorPath] : [])].every((path) => typeof path === "string" && NodePath.isAbsolute(path)) || !output.endsWith(".json"))
    throw new Error("observer-diagnostic.mjs <absolute source manifest> <absolute output.json> <absolute bound baseline> <full|cpu-stat-only> [absolute inspected full receipt]");
  const root = NodePath.resolve(import.meta.dirname, "../../.."), entry = NodePath.join(root, "apps/server/src/bin.ts");
  const required = JSON.parse(await NodeFSP.readFile(NodePath.join(root, "package.json"), "utf8")).engines.node;
  if (process.versions.node !== required) throw new Error(`Diagnostic requires Node ${required}`);
  const source = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const sourceTree = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD^{tree}"], { cwd: root, encoding: "utf8" }).trim();
  if (source !== process.env.JONES_SP_SOURCE_SHA || sourceTree !== process.env.JONES_SP_SOURCE_TREE) throw new Error("Source/tree mismatch");
  NodeChildProcess.execFileSync("git", ["diff", "--quiet", "HEAD", "--"], { cwd: root, stdio: "ignore" });
  const paths = outputPaths(output);
  await assertNewDiagnosticPaths(output, [manifestPath, baselinePath, ...(priorPath ? [priorPath] : [])]);
  const manifestBytes = await NodeFSP.readFile(manifestPath), baselineBytes = await NodeFSP.readFile(baselinePath);
  const manifest = JSON.parse(manifestBytes), baseline = JSON.parse(baselineBytes);
  const prior = priorPath ? JSON.parse(await NodeFSP.readFile(priorPath, "utf8")) : undefined;
  if (priorPath && !(await verifyDiagnosticArtifacts(prior, priorPath))) throw new Error("Full diagnostic artifacts are incomplete or changed");
  const HostProcess = await import("../../../packages/shared/src/HostProcess.ts");
  const { currentBuildIdentity, minimalWorkerEnvironment } = await import("../../../apps/server/src/jones/serverCompute/workerCommand.ts");
  const argvEntry = process.argv[1];
  let build;
  try { process.argv[1] = entry; build = await currentBuildIdentity(); }
  finally { process.argv[1] = argvEntry; }
  validateDiagnosticAdmission({ manifest, baseline, mode, source, sourceTree, build, entry, executable: process.execPath, platform: HostProcess.Platform.defaultValue(), parallelism: NodeOS.availableParallelism(), prior, baselineHash: digest(baselineBytes) });
  const inputHashes = {};
  for (const path of ["package.json", "pnpm-lock.yaml", "patches/@ff-labs__fff-node@0.11.0.patch", "apps/server/src/jones/serverParallelism/diagnosticObservation.ts", "apps/server/src/jones/serverParallelism/resourceSamples.ts", "apps/server/src/jones/serverParallelism/nativeQualification.ts", "apps/server/src/jones/serverParallelism/combinedQualification.ts", "apps/server/src/jones/serverCompute/processSlot.ts", "scripts/jones/serverParallelism/observer-diagnostic.mjs"]) inputHashes[path] = digest(await NodeFSP.readFile(NodePath.join(root, path)));
  if (prior && (prior.manifestHash !== digest(manifestBytes) || JSON.stringify(prior.inputHashes) !== JSON.stringify(inputHashes))) throw new Error("Control diagnostic input bytes changed");
  const { diagnosticTrace, diagnosticError } = await import("../../../apps/server/src/jones/serverParallelism/diagnosticObservation.ts");
  const { runNativeQualification } = await import("../../../apps/server/src/jones/serverParallelism/nativeQualification.ts");
  const { qualificationDecision, requiredNativePredicates } = await import("./qualificationResult.mjs");
  const trace = diagnosticTrace(1_000_000), controller = new AbortController();
  const scratch = await NodeFSP.realpath(await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sp-observer-")));
  let report = null, error = null, shutdownProved = false;
  const cleanup = { scratch, shutdownProved: false, fixtureRemoved: false, error: null };
  const stop = () => controller.abort();
  process.once("SIGTERM", stop); process.once("SIGINT", stop);
  const started = performance.now();
  try {
    report = await runNativeQualification({
      count: 2, windows: 3, acquisitionsPerWindow: 500, filesPerKey: 512,
      directory: NodePath.join(scratch, "fixture"),
      command: { file: manifest.file, args: manifest.args, build, env: minimalWorkerEnvironment(process.env, false, false) },
      signal: controller.signal, diagnostic: { mode, sink: trace.append },
      onShutdown: () => { shutdownProved = true; },
      baselineWriterP95Ms: baseline.writer.p95, baselineWriterP99Ms: baseline.writer.p99,
      baselineLoopP99Ms: baseline.loop.p99, baselineChurn: baseline.churnWindows,
    });
  } catch (cause) { error = diagnosticError(cause); }
  finally {
    process.removeListener("SIGTERM", stop); process.removeListener("SIGINT", stop);
    cleanup.shutdownProved = shutdownProved;
    if (shutdownProved) {
      try {
        const parent = await NodeFSP.realpath(NodeOS.tmpdir());
        if (NodePath.dirname(scratch) !== parent || !NodePath.basename(scratch).startsWith("jones-sp-observer-") || (await NodeFSP.lstat(scratch)).isSymbolicLink()) throw new Error("Diagnostic scratch ownership mismatch");
        await NodeFSP.rm(scratch, { recursive: true });
        try { await NodeFSP.lstat(scratch); }
        catch (cause) { if (cause.code === "ENOENT") cleanup.fixtureRemoved = true; else throw cause; }
      } catch (cause) { cleanup.error = diagnosticError(cause); }
    }
  }
  const elapsedMs = performance.now() - started, retention = trace.report();
  const complete = diagnosticLifecycleComplete(trace.rows) && diagnosticCompletion({ report, trace: retention, cleanup, elapsedMs, error, aborted: controller.signal.aborted });
  const values = { raw: report, trace: { retention, events: trace.rows }, summary: summarizeDiagnostic(report, trace.rows), error, cleanup };
  const artifacts = {};
  for (const [kind, value] of Object.entries(values)) {
    const bytes = JSON.stringify(value, null, 2) + "\n";
    await NodeFSP.writeFile(paths[kind], bytes, { flag: "wx", mode: 0o600 });
    artifacts[kind] = { path: paths[kind], sha256: digest(bytes) };
  }
  const receipt = {
    schema, eligible: false, passed: false, outcome: complete ? "recorded" : "incomplete", complete,
    source, sourceTree, build, mode, runtimeForm: "source", profile: "laptop",
    manifestHash: digest(manifestBytes), baselineHash: digest(baselineBytes), inputHashes,
    priorFullHash: priorPath ? digest(await NodeFSP.readFile(priorPath)) : null,
    bounds: { count: 2, windows: 3, acquisitions: 500, files: 512, churnSets: [9, 16], churnRounds: 3, workloadMs: 600_000, fullRuns: 1, controlRunsMaximum: 1 },
    host: { platform: HostProcess.Platform.defaultValue(), architecture: HostProcess.Architecture.defaultValue(), kernel: NodeOS.release(), availableParallelism: NodeOS.availableParallelism(), node: process.versions },
    clock: { monotonic: "performance.now", wallOriginMs: performance.timeOrigin, units: { trace: "milliseconds", cpu: "microseconds", procStart: "CLK_TCK ticks since boot" } },
    elapsedMs, aborted: controller.signal.aborted, cleanupProved: shutdownProved && cleanup.fixtureRemoved,
    traceRetention: retention, artifacts,
    official: { predicates: report?.predicates ?? null, decision: report ? qualificationDecision(report.predicates, requiredNativePredicates) : null },
    meaning: "Permanently ineligible diagnostic. A favorable control cannot replace resource qualification, its full observation or its unchanged baseline.",
  };
  await NodeFSP.writeFile(output, JSON.stringify(receipt, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  if (!complete) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href)
  await main(process.argv.slice(2));
