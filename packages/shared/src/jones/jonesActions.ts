// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Node-only launcher/desktop transport and receipt boundary; clock is injected for qualification tests.
import { parseJonesBuildStartedAt } from "./buildTime.ts";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeZlib from "node:zlib";

import {
  createJonesStageAttempt,
  findJonesCompletedStage,
  publishJonesCompletedStage,
} from "./jonesActionsStage.ts";
export {
  createJonesStageAttempt,
  findJonesCompletedStage,
  publishJonesCompletedStage,
  requireJonesStageDirectory,
} from "./jonesActionsStage.ts";

const JONES_ACTIONS_REPOSITORY = "Jones-Systems/Jones-Code";
export const JONES_ACTIONS_RECEIPT_FILE = "jones-actions-stage.json";
const MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024;
const SHA = /^[a-f0-9]{40}$/;
const HASH = /^[a-f0-9]{64}$/;
const ANCESTRY_MEMO_LIMIT = 256;
const PREVIEW = /^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/;
const API_ROOT = `repos/${JONES_ACTIONS_REPOSITORY}`;

export type JonesActionsPlatform = "linux" | "darwin";
export type JonesActionsArchitecture = "x64" | "arm64";
export type JonesActionsBlockedReason =
  | "gh-missing"
  | "authentication-required"
  | "rate-limited"
  | "unavailable"
  | "unqualified"
  | "integrity"
  | "occupied-cache"
  | "unsupported-platform";

/** Messages deliberately exclude gh stderr, credentials and artifact redirect URLs. */
export class JonesActionsError extends Error {
  readonly reason: JonesActionsBlockedReason;
  constructor(reason: JonesActionsBlockedReason) {
    super(`Jones main update blocked: ${reason}.`);
    this.name = "JonesActionsError";
    this.reason = reason;
  }
}

export interface JonesActionsCandidate {
  readonly schema: 1;
  readonly repository: typeof JONES_ACTIONS_REPOSITORY;
  readonly source: string;
  readonly tree: string;
  readonly installedSource: string;
  readonly workflow: string;
  readonly workflowId: number;
  readonly runId: number;
  readonly runAttempt: number;
  readonly ciRunId: number;
  readonly artifactId: number;
  readonly artifactName: string;
  readonly artifactDigest: string;
  readonly artifactBytes: number;
  readonly expiresAt: string;
  readonly version: string;
  readonly platform: JonesActionsPlatform;
  readonly architecture: JonesActionsArchitecture;
}

export interface JonesArtifactReceipt {
  readonly schema: 1;
  readonly repository: string;
  readonly source: string;
  readonly tree: string;
  readonly workflow: string;
  readonly event: "push" | "schedule";
  readonly ref: "refs/heads/main";
  readonly runId: string;
  readonly runAttempt: string;
  readonly version: string;
  readonly platform: JonesActionsPlatform;
  readonly architecture: JonesActionsArchitecture;
  readonly artifact: string;
  readonly sha256: string;
}

export interface JonesStagedArtifact {
  readonly schema: 1;
  readonly source: "jones-actions";
  readonly channel: "jones-main";
  readonly stagedHandle: string;
  readonly candidate: JonesActionsCandidate;
  readonly receipt: JonesArtifactReceipt;
  readonly payloadPath: string;
}

export type JonesActionsCheckResult = { readonly classification?: JonesCheckClassification } & (
  | {
      readonly state: "available";
      readonly candidate: JonesActionsCandidate;
      readonly buildStartedAt?: string;
    }
  | { readonly state: "no-new" }
  | { readonly state: "building"; readonly retryAfterMs: number }
  | {
      readonly state: "blocked";
      readonly reason: JonesActionsBlockedReason;
      readonly retryAfterMs: number;
    }
);

export type JonesSourceRelationKind = "ahead" | "behind" | "identical" | "diverged";

interface JonesActionsCheckInput {
  readonly installedSource: string;
  readonly targetSource?: string;
  readonly desktopBundleIdentifier?: string;
  readonly platform: JonesActionsPlatform;
  readonly architecture: JonesActionsArchitecture;
}

type CheckOperation =
  | "input"
  | "main"
  | "ancestry"
  | "producer-list"
  | "producer-detail"
  | "ci-list"
  | "artifact-list"
  | "commit";
type PendingBasis = "producer-not-completed" | "ci-not-completed" | "ci-not-observed";
type SourceAssurance = "listed-main-only" | "ancestry-proved";
type PendingStatus = "queued" | "in_progress" | "requested" | "waiting" | "pending" | "unknown";
type FailedConclusion =
  | "failure"
  | "cancelled"
  | "timed_out"
  | "action_required"
  | "neutral"
  | "skipped"
  | "stale"
  | "startup_failure";
interface CheckWitness {
  readonly sourceSha?: string;
  readonly runId?: number;
  readonly sourceAssurance: SourceAssurance;
}
interface PendingWitness extends CheckWitness {
  readonly reportedStatus?: PendingStatus;
}
interface FailedWitness extends CheckWitness {
  readonly stage: "producer-list" | "producer-detail" | "ci-list";
  readonly conclusion: FailedConclusion;
}
interface WitnessCount<Witness> {
  count: number;
  first?: Witness;
}
interface RunFrontier {
  readonly runId: number;
  readonly createdAt?: string;
}
interface DiscoveryStream {
  readonly event: string;
  pagesRead: number;
  lastRawRowCount?: number;
  end: "not-read" | "more-possible" | "short-page" | "page-limit" | "error";
  bufferedRunCount: number;
  lastYielded?: RunFrontier;
  nextBuffered?: RunFrontier;
}
type DiscoveryLimit = "producer-pages" | "ci-page" | "artifact-page" | "unknown-terminal-evidence";
interface CheckDiscovery {
  termination:
    | "not-started"
    | "target-proved"
    | "candidate-selected"
    | "window-ended"
    | "interrupted";
  frontierWithinBudgetSatisfied: boolean;
  unresolvedLimits: DiscoveryLimit[];
  streams: DiscoveryStream[];
}
interface CheckEvidence {
  readonly scope: {
    readonly platform: JonesActionsPlatform;
    readonly architecture: JonesActionsArchitecture;
    readonly selection: "latest" | "exact";
    installedSource?: string;
    requestedSource?: string;
    mainSource?: string;
  };
  readonly checkedAt: string;
  readonly mainObservedAt?: string;
  readonly pending: Record<PendingBasis, WitnessCount<PendingWitness>>;
  readonly failedQualification: WitnessCount<FailedWitness>;
  readonly rejections: {
    installedSource: number;
    noncanonicalOrWrongTarget: number;
    noMatchingArtifact: number;
    expiredArtifact: number;
    bindingMismatch: number;
  };
  readonly discovery: CheckDiscovery;
}
interface CheckFailureContext {
  operation: CheckOperation;
  origin: "request" | "validation" | "unknown";
}

type JonesActionsCheckObservation = CheckEvidence &
  (
    | {
        readonly kind: "available";
        readonly candidate: JonesActionsCandidate;
        readonly buildStartedAt?: string;
      }
    | {
        readonly kind:
          | "target-already-running"
          | "qualification-pending"
          | "qualification-failed"
          | "no-eligible-package-in-window"
          | "incomplete-discovery";
      }
    | {
        readonly kind: "failure";
        readonly failure: {
          readonly reason: JonesActionsBlockedReason;
          readonly operation: CheckOperation;
          readonly origin: "request" | "validation" | "unknown";
        };
      }
  );

/** Additive display evidence; the established selector and legacy phase projection stay intact. */
export interface JonesCheckClassification {
  readonly kind: JonesActionsCheckObservation["kind"];
  readonly checkedAt: string;
  readonly discovery: {
    readonly termination: CheckDiscovery["termination"];
    readonly bounded: boolean;
    readonly unresolvedLimits: readonly DiscoveryLimit[];
  };
  readonly pendingCount: number;
  readonly failedQualificationCount: number;
}

export function classifyJonesActionsCheck(
  observation: JonesActionsCheckObservation,
): JonesCheckClassification {
  return {
    kind: observation.kind,
    checkedAt: observation.checkedAt,
    discovery: {
      termination: observation.discovery.termination,
      bounded: observation.discovery.frontierWithinBudgetSatisfied,
      unresolvedLimits: [...observation.discovery.unresolvedLimits],
    },
    pendingCount: Object.values(observation.pending).reduce(
      (sum, witness) => sum + witness.count,
      0,
    ),
    failedQualificationCount: observation.failedQualification.count,
  };
}

function hasPending(evidence: Pick<CheckEvidence, "pending">): boolean {
  return Object.values(evidence.pending).some((witness) => witness.count > 0);
}

function projectJonesActionsCheck(
  observation: JonesActionsCheckObservation,
): JonesActionsCheckResult {
  switch (observation.kind) {
    case "available":
      return {
        state: "available",
        candidate: observation.candidate,
        ...(observation.buildStartedAt === undefined
          ? {}
          : { buildStartedAt: observation.buildStartedAt }),
      };
    case "target-already-running":
      return { state: "no-new" };
    case "qualification-pending":
      return { state: "building", retryAfterMs: 240000 };
    case "incomplete-discovery":
      if (hasPending(observation)) return { state: "building", retryAfterMs: 240000 };
      return { state: "blocked", reason: "unavailable", retryAfterMs: 240000 };
    case "qualification-failed":
    case "no-eligible-package-in-window":
      return { state: "blocked", reason: "unavailable", retryAfterMs: 240000 };
    case "failure":
      return { state: "blocked", reason: observation.failure.reason, retryAfterMs: 240000 };
    default: {
      const unreachable: never = observation;
      return unreachable;
    }
  }
}

function addWitness<Witness>(bucket: WitnessCount<Witness>, witness: Witness): void {
  bucket.count++;
  bucket.first ??= witness;
}
function addLimit(discovery: CheckDiscovery, limit: DiscoveryLimit): void {
  if (!discovery.unresolvedLimits.includes(limit)) discovery.unresolvedLimits.push(limit);
}
function checkWitness(
  run: Record<string, unknown>,
  sourceAssurance: SourceAssurance,
): CheckWitness {
  return {
    sourceAssurance,
    ...(typeof run.head_sha === "string" && SHA.test(run.head_sha)
      ? { sourceSha: run.head_sha }
      : {}),
    ...(typeof run.id === "number" && Number.isSafeInteger(run.id) && run.id > 0
      ? { runId: run.id }
      : {}),
  };
}
function pendingStatus(value: unknown): PendingStatus {
  switch (value) {
    case "queued":
    case "in_progress":
    case "requested":
    case "waiting":
    case "pending":
      return value;
    default:
      return "unknown";
  }
}
function failedConclusion(value: unknown): FailedConclusion | undefined {
  switch (value) {
    case "failure":
    case "cancelled":
    case "timed_out":
    case "action_required":
    case "neutral":
    case "skipped":
    case "stale":
    case "startup_failure":
      return value;
    default:
      return undefined;
  }
}
function runFrontier(run: Record<string, unknown>): RunFrontier | undefined {
  const witness = checkWitness(run, "listed-main-only");
  if (witness.runId === undefined) return undefined;
  const createdAt = parseJonesBuildStartedAt(run.created_at);
  return { runId: witness.runId, ...(createdAt === undefined ? {} : { createdAt }) };
}

/** Adapters own authentication and redirect following; neither enters the updater model. */
export interface JonesActionsTransport {
  api(endpoint: string): Promise<unknown>;
  download(artifactId: number, destination: string): Promise<void>;
}

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new JonesActionsError("unqualified");
  return value as Record<string, unknown>;
}
function rows(value: unknown, key: string): Array<Record<string, unknown>> {
  const list = record(value)[key];
  if (!Array.isArray(list) || list.length > 100) throw new JonesActionsError("unqualified");
  return list.map(record);
}
function positive(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0)
    throw new JonesActionsError("unqualified");
  return value;
}
function count(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new JonesActionsError("unqualified");
  return value;
}
function text(value: unknown): string {
  if (typeof value !== "string") throw new JonesActionsError("unqualified");
  return value;
}
function source(value: unknown): string {
  const result = text(value);
  if (!SHA.test(result)) throw new JonesActionsError("unqualified");
  return result;
}
function digest(value: unknown): string {
  const result = text(value).replace(/^sha256:/, "");
  if (!HASH.test(result)) throw new JonesActionsError("unqualified");
  return result;
}
const PRODUCTION_DESKTOP_ID = "com.jonessystems.jonescode";
const PRODUCTION_DESKTOP_WORKFLOW = ".github/workflows/jones-desktop-signing.yml";

function spec(
  platform: JonesActionsPlatform,
  architecture: JonesActionsArchitecture,
  desktopBundleIdentifier?: string,
) {
  if (
    desktopBundleIdentifier !== undefined &&
    (platform !== "darwin" ||
      !["com.t3tools.t3code", PRODUCTION_DESKTOP_ID].includes(desktopBundleIdentifier))
  )
    throw new JonesActionsError("unqualified");
  if (
    platform === "darwin" &&
    architecture === "arm64" &&
    desktopBundleIdentifier === PRODUCTION_DESKTOP_ID
  )
    return {
      workflow: PRODUCTION_DESKTOP_WORKFLOW,
      name: "desktop-mac-production-arm64",
      events: ["push"],
    };
  if (platform === "darwin" && architecture === "arm64")
    return {
      workflow: ".github/workflows/artifact-desktop-mac.yml",
      name: "desktop-mac-arm64",
      events: ["push", "schedule"],
    };
  if (platform === "linux" && (architecture === "x64" || architecture === "arm64"))
    return {
      workflow: ".github/workflows/artifact-cli-linux.yml",
      name: `jones-code-cli-linux-${architecture}`,
      events: ["push"],
    };
  throw new JonesActionsError("unsupported-platform");
}
function canonicalRun(run: Record<string, unknown>, workflow: string): boolean {
  return (
    run.path === workflow &&
    run.head_branch === "main" &&
    record(run.repository).full_name === JONES_ACTIONS_REPOSITORY &&
    record(run.head_repository).full_name === JONES_ACTIONS_REPOSITORY
  );
}
function artifactEvent(approved: ReturnType<typeof spec>, event: unknown): boolean {
  return typeof event === "string" && approved.events.includes(event);
}
function canonicalArtifactRun(
  run: Record<string, unknown>,
  approved: ReturnType<typeof spec>,
): boolean {
  return canonicalRun(run, approved.workflow) && artifactEvent(approved, run.event);
}
function canonicalCiRun(run: Record<string, unknown>): boolean {
  return canonicalRun(run, ".github/workflows/ci.yml") && run.event === "push";
}
function newestArtifactRun(left: Record<string, unknown>, right: Record<string, unknown>): number {
  const createdAt = (run: Record<string, unknown>) =>
    typeof run.created_at === "string" ? Date.parse(run.created_at) || 0 : 0;
  return createdAt(right) - createdAt(left) || positive(right.id) - positive(left.id);
}

async function* discoverArtifactRuns(
  transport: JonesActionsTransport,
  approved: ReturnType<typeof spec>,
  targetSource: string | undefined,
  evidence: Pick<CheckEvidence, "discovery" | "rejections">,
  context: CheckFailureContext,
): AsyncGenerator<Record<string, unknown>> {
  const streams = approved.events.map((event) => {
    const observation: DiscoveryStream = {
      event,
      pagesRead: 0,
      end: "not-read",
      bufferedRunCount: 0,
    };
    evidence.discovery.streams.push(observation);
    return {
      event,
      page: 0,
      complete: false,
      runs: [] as Array<Record<string, unknown>>,
      observation,
    };
  });
  const updateBuffer = (stream: (typeof streams)[number]) => {
    stream.observation.bufferedRunCount = stream.runs.length;
    const next = stream.runs[0];
    const pointer = next === undefined ? undefined : runFrontier(next);
    if (pointer === undefined) delete stream.observation.nextBuffered;
    else stream.observation.nextBuffered = pointer;
  };
  const seen = new Set<number>();
  while (true) {
    context.operation = "producer-list";
    context.origin = "validation";
    for (const stream of streams) {
      if (stream.runs.length > 0 || stream.complete) continue;
      try {
        context.origin = "request";
        const response = await transport.api(
          `${API_ROOT}/actions/workflows/${NodePath.posix.basename(approved.workflow)}/runs?branch=main&event=${stream.event}${targetSource === undefined ? "" : `&head_sha=${targetSource}`}&per_page=50&page=${++stream.page}`,
        );
        context.origin = "validation";
        const runs = rows(response, "workflow_runs");
        stream.observation.pagesRead++;
        stream.observation.lastRawRowCount = runs.length;
        stream.observation.end =
          runs.length < 50 ? "short-page" : stream.page === 3 ? "page-limit" : "more-possible";
        if (stream.observation.end === "page-limit") addLimit(evidence.discovery, "producer-pages");
        stream.complete = runs.length < 50 || stream.page === 3;
        stream.runs = runs
          .filter((run) => {
            const accepted =
              canonicalArtifactRun(run, approved) &&
              run.event === stream.event &&
              (targetSource === undefined || run.head_sha === targetSource);
            if (!accepted) evidence.rejections.noncanonicalOrWrongTarget++;
            return accepted;
          })
          .sort(newestArtifactRun);
        updateBuffer(stream);
      } catch (error) {
        stream.observation.end = "error";
        throw error;
      }
    }
    // Refill every drained frontier before choosing: its next page may precede another stream.
    if (streams.some((stream) => stream.runs.length === 0 && !stream.complete)) continue;
    const frontier = streams
      .filter((stream) => stream.runs.length > 0)
      .sort((left, right) => newestArtifactRun(left.runs[0]!, right.runs[0]!))[0];
    if (frontier === undefined) return;
    const run = frontier.runs.shift()!;
    updateBuffer(frontier);
    let id: number;
    try {
      id = positive(run.id);
    } catch (error) {
      frontier.observation.end = "error";
      throw error;
    }
    if (seen.has(id)) continue;
    seen.add(id);
    const yielded = runFrontier(run);
    if (yielded !== undefined) frontier.observation.lastYielded = yielded;
    evidence.discovery.frontierWithinBudgetSatisfied = true;
    yield run;
  }
}

function safeTransportError(error: unknown): JonesActionsError {
  return error instanceof JonesActionsError ? error : new JonesActionsError("unavailable");
}

function classifyGh(stderr: string, missing = false): JonesActionsError {
  if (missing) return new JonesActionsError("gh-missing");
  if (/rate limit|HTTP 429/i.test(stderr)) return new JonesActionsError("rate-limited");
  if (/HTTP 401|authentication|gh auth login|not logged/i.test(stderr))
    return new JonesActionsError("authentication-required");
  return new JonesActionsError("unavailable");
}

/** Fixed host and repository. gh alone reads its auth and follows the download redirect. */
function createGhJonesActionsTransport(): JonesActionsTransport {
  const invoke = async (endpoint: string, destination?: string): Promise<unknown> => {
    if (!endpoint.startsWith(`${API_ROOT}/`) || /[\r\n]/.test(endpoint))
      throw new JonesActionsError("unqualified");
    const child = NodeChildProcess.spawn(
      "gh",
      ["api", "--hostname", "github.com", "--method", "GET", endpoint],
      {
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
        timeout: destination ? 15 * 60 * 1000 : 30000,
      },
    );
    let stderr = "";
    child.stderr.on("data", (chunk: Buffer) => {
      if (stderr.length < 16384) stderr += chunk.toString("utf8");
    });
    const completion = new Promise<void>((resolve, reject) => {
      child.once("error", (error: NodeJS.ErrnoException) =>
        reject(classifyGh("", error.code === "ENOENT")),
      );
      child.once("close", (code) => (code === 0 ? resolve() : reject(classifyGh(stderr))));
    });
    let count = 0;
    const limit = new NodeStream.Transform({
      transform(chunk: Buffer, _encoding, callback) {
        count += chunk.length;
        callback(
          count > (destination ? MAX_ARCHIVE_BYTES : 8 * 1024 * 1024)
            ? new JonesActionsError("integrity")
            : null,
          chunk,
        );
      },
    });
    try {
      if (destination) {
        await Promise.all([
          NodeStreamPromises.pipeline(
            child.stdout,
            limit,
            NodeFS.createWriteStream(destination, { flags: "wx", mode: 0o600 }),
          ),
          completion,
        ]);
        return undefined;
      }
      const chunks: Buffer[] = [];
      limit.on("data", (chunk: Buffer) => chunks.push(chunk));
      await Promise.all([NodeStreamPromises.pipeline(child.stdout, limit), completion]);
      return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    } catch (error) {
      // This PID was spawned here; stop bounded failed transports rather than orphaning them.
      child.kill();
      throw safeTransportError(error);
    }
  };
  return {
    api: (endpoint) => invoke(endpoint),
    download: async (artifactId, destination) => {
      await invoke(`${API_ROOT}/actions/artifacts/${positive(artifactId)}/zip`, destination);
    },
  };
}

export class JonesActionsClient {
  private readonly transport: JonesActionsTransport;
  private readonly now: () => number;
  /** Immutable `base...head` ancestry answers, oldest first; bounded by `ANCESTRY_MEMO_LIMIT`. */
  private readonly ancestry = new Map<string, boolean>();
  constructor(
    options: { readonly transport?: JonesActionsTransport; readonly now?: () => number } = {},
  ) {
    this.transport = options.transport ?? createGhJonesActionsTransport();
    this.now = options.now ?? Date.now;
  }

  async check(input: JonesActionsCheckInput): Promise<JonesActionsCheckResult> {
    const observation = await this.observeCheck(input);
    return {
      ...projectJonesActionsCheck(observation),
      classification: classifyJonesActionsCheck(observation),
    };
  }

  async observeCheck(input: JonesActionsCheckInput): Promise<JonesActionsCheckObservation> {
    const scope: CheckEvidence["scope"] = {
      platform: input.platform,
      architecture: input.architecture,
      selection: input.targetSource === undefined ? "latest" : "exact",
    };
    const pending: CheckEvidence["pending"] = {
      "producer-not-completed": { count: 0 },
      "ci-not-completed": { count: 0 },
      "ci-not-observed": { count: 0 },
    };
    const failedQualification: CheckEvidence["failedQualification"] = { count: 0 };
    const rejections: CheckEvidence["rejections"] = {
      installedSource: 0,
      noncanonicalOrWrongTarget: 0,
      noMatchingArtifact: 0,
      expiredArtifact: 0,
      bindingMismatch: 0,
    };
    const discovery: CheckDiscovery = {
      termination: "not-started",
      frontierWithinBudgetSatisfied: false,
      unresolvedLimits: [],
      streams: [],
    };
    let mainObservedAt: string | undefined;
    const completedEvidence = (): CheckEvidence => ({
      scope,
      pending,
      failedQualification,
      rejections,
      discovery,
      checkedAt: new Date(this.now()).toISOString(),
      ...(mainObservedAt === undefined ? {} : { mainObservedAt }),
    });
    const context: CheckFailureContext = { operation: "input", origin: "validation" };
    const read = async (operation: CheckOperation, endpoint: string): Promise<unknown> => {
      context.operation = operation;
      context.origin = "request";
      const response = await this.transport.api(endpoint);
      context.origin = "validation";
      return response;
    };
    const descends = async (base: string, head: string): Promise<boolean> => {
      context.operation = "ancestry";
      context.origin = "unknown";
      const proven = await this.descends(base, head);
      context.origin = "validation";
      return proven;
    };
    const observeTerminal = (
      run: Record<string, unknown>,
      stage: FailedWitness["stage"],
      sourceAssurance: SourceAssurance,
      unresolved = true,
    ) => {
      if (run.status !== "completed" || run.conclusion === "success") return;
      const conclusion = failedConclusion(run.conclusion);
      if (conclusion === undefined) {
        if (unresolved) addLimit(discovery, "unknown-terminal-evidence");
      } else
        addWitness(failedQualification, {
          ...checkWitness(run, sourceAssurance),
          stage,
          conclusion,
        });
    };
    try {
      scope.installedSource = source(input.installedSource);
      const targetSource =
        input.targetSource === undefined ? undefined : source(input.targetSource);
      if (targetSource !== undefined) scope.requestedSource = targetSource;
      const approved = spec(input.platform, input.architecture, input.desktopBundleIdentifier);
      const main = record(await read("main", `${API_ROOT}/commits/main`));
      const mainSource = source(main.sha);
      scope.mainSource = mainSource;
      mainObservedAt = new Date(this.now()).toISOString();
      // Installed source must itself be on canonical main, including after a force rewrite.
      if (!(await descends(input.installedSource, mainSource)))
        throw new JonesActionsError("unqualified");
      if (
        targetSource !== undefined &&
        (!(await descends(input.installedSource, targetSource)) ||
          !(await descends(targetSource, mainSource)))
      )
        throw new JonesActionsError("unqualified");
      if ((targetSource ?? mainSource) === input.installedSource) {
        discovery.termination = "target-proved";
        return { ...completedEvidence(), kind: "target-already-running" };
      }
      const ciObservations = new Map<string, Array<Record<string, unknown>>>();
      for await (const listed of discoverArtifactRuns(
        this.transport,
        approved,
        targetSource,
        { discovery, rejections },
        context,
      )) {
        if (listed.status !== "completed") {
          addWitness(pending["producer-not-completed"], {
            ...checkWitness(listed, "listed-main-only"),
            reportedStatus: pendingStatus(listed.status),
          });
          continue;
        }
        observeTerminal(listed, "producer-list", "listed-main-only");
        if (listed.conclusion !== "success") continue;
        if (listed.head_sha === input.installedSource) {
          rejections.installedSource++;
          continue;
        }
        context.operation = "producer-detail";
        const run = record(
          await read("producer-detail", `${API_ROOT}/actions/runs/${positive(listed.id)}`),
        );
        if (!canonicalArtifactRun(run, approved)) {
          rejections.noncanonicalOrWrongTarget++;
          continue;
        }
        if (targetSource === undefined || run.head_sha === targetSource) {
          observeTerminal(run, "producer-detail", "listed-main-only");
        }
        if (run.status !== "completed" || run.conclusion !== "success") continue;
        const candidateSource = source(run.head_sha);
        if (
          (targetSource !== undefined && candidateSource !== targetSource) ||
          candidateSource === input.installedSource ||
          !(await descends(input.installedSource, candidateSource)) ||
          !(await descends(candidateSource, mainSource))
        ) {
          rejections.noncanonicalOrWrongTarget++;
          continue;
        }
        const ciEndpoint = `${API_ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${candidateSource}&per_page=100&page=1`;
        context.operation = "ci-list";
        context.origin = "validation";
        let ciRuns = ciObservations.get(ciEndpoint);
        const freshCi = ciRuns === undefined;
        if (ciRuns === undefined) {
          ciRuns = rows(await read("ci-list", ciEndpoint), "workflow_runs");
          ciObservations.set(ciEndpoint, ciRuns);
        }
        const sourceCi = ciRuns.filter(
          (item) => canonicalCiRun(item) && item.head_sha === candidateSource,
        );
        const ci = sourceCi.find(
          (item) => item.status === "completed" && item.conclusion === "success",
        );
        if (freshCi) {
          for (const item of sourceCi)
            observeTerminal(item, "ci-list", "ancestry-proved", ci === undefined);
        }
        if (!ci) {
          if (ciRuns.length === 100) addLimit(discovery, "ci-page");
          // Count each inspected CI row once, even when several producers share its source.
          if (freshCi) {
            if (sourceCi.length === 0)
              addWitness(pending["ci-not-observed"], {
                sourceSha: candidateSource,
                sourceAssurance: "ancestry-proved",
              });
            for (const item of sourceCi) {
              if (item.status !== "completed")
                addWitness(pending["ci-not-completed"], {
                  ...checkWitness(item, "ancestry-proved"),
                  reportedStatus: pendingStatus(item.status),
                });
            }
          }
          continue;
        }
        context.operation = "artifact-list";
        const runId = positive(run.id),
          runAttempt = positive(run.run_attempt);
        const artifacts = rows(
          await read(
            "artifact-list",
            `${API_ROOT}/actions/runs/${runId}/artifacts?per_page=100&page=1`,
          ),
          "artifacts",
        );
        if (artifacts.length === 0) rejections.noMatchingArtifact++;
        for (const artifact of artifacts) {
          const name = text(artifact.name);
          const prefix = `${approved.name}-${runId}-${runAttempt}--`;
          if (!name.startsWith(prefix)) {
            rejections.noMatchingArtifact++;
            continue;
          }
          const version = name.slice(prefix.length);
          if (!PREVIEW.test(version) || !version.endsWith(`.${runId}.${runAttempt}`)) {
            rejections.noMatchingArtifact++;
            continue;
          }
          const expiresAt = text(artifact.expires_at);
          if (
            artifact.expired !== false ||
            !Number.isFinite(Date.parse(expiresAt)) ||
            Date.parse(expiresAt) <= this.now()
          ) {
            rejections.expiredArtifact++;
            continue;
          }
          const artifactRun = record(artifact.workflow_run);
          if (
            artifactRun.id !== runId ||
            artifactRun.head_sha !== candidateSource ||
            artifactRun.head_branch !== "main" ||
            artifactRun.repository_id !== record(run.repository).id ||
            artifactRun.head_repository_id !== record(run.repository).id
          ) {
            rejections.bindingMismatch++;
            continue;
          }
          const commit = record(await read("commit", `${API_ROOT}/git/commits/${candidateSource}`));
          if (commit.sha !== candidateSource) throw new JonesActionsError("unqualified");
          context.operation = "artifact-list";
          const artifactBytes = positive(artifact.size_in_bytes);
          if (artifactBytes > MAX_ARCHIVE_BYTES) throw new JonesActionsError("integrity");
          const buildStartedAt = parseJonesBuildStartedAt(run.run_started_at);
          context.operation = "commit";
          const candidate: JonesActionsCandidate = {
            schema: 1,
            repository: JONES_ACTIONS_REPOSITORY,
            source: candidateSource,
            tree: source(record(commit.tree).sha),
            installedSource: input.installedSource,
            workflow: approved.workflow,
            workflowId: positive(run.workflow_id),
            runId,
            runAttempt,
            ciRunId: positive(ci.id),
            artifactId: positive(artifact.id),
            artifactName: name,
            artifactDigest: digest(artifact.digest),
            artifactBytes,
            expiresAt,
            version,
            platform: input.platform,
            architecture: input.architecture,
          };
          discovery.termination = "candidate-selected";
          return {
            ...completedEvidence(),
            kind: "available",
            candidate,
            ...(buildStartedAt === undefined ? {} : { buildStartedAt }),
          };
        }
        if (artifacts.length === 100) addLimit(discovery, "artifact-page");
      }
      discovery.termination = "window-ended";
      const kind =
        discovery.unresolvedLimits.length > 0
          ? "incomplete-discovery"
          : hasPending({ pending })
            ? "qualification-pending"
            : failedQualification.count > 0
              ? "qualification-failed"
              : "no-eligible-package-in-window";
      return { ...completedEvidence(), kind };
    } catch (error) {
      discovery.termination = "interrupted";
      return {
        ...completedEvidence(),
        kind: "failure",
        failure: { reason: safeTransportError(error).reason, ...context },
      };
    }
  }

  /**
   * Ancestry of two exact commits in the fixed repository: "ahead" when `base` is an ancestor of
   * `head`, "behind" for the reverse. It says nothing about main membership or channel order.
   * Throws a sanitized `JonesActionsError` when GitHub cannot prove one relation for this pair.
   */
  async relation(base: string, head: string): Promise<JonesSourceRelationKind> {
    if (source(base) === source(head)) return "identical";
    try {
      const comparison = record(
        await this.transport.api(`${API_ROOT}/compare/${base}...${head}?per_page=1`),
      );
      const mergeBase = source(record(comparison.merge_base_commit).sha);
      const aheadBy = count(comparison.ahead_by),
        behindBy = count(comparison.behind_by);
      if (source(record(comparison.base_commit).sha) !== base)
        throw new JonesActionsError("unqualified");
      if (comparison.status === "ahead" && mergeBase === base && aheadBy > 0 && behindBy === 0)
        return "ahead";
      if (comparison.status === "behind" && mergeBase === head && behindBy > 0 && aheadBy === 0)
        return "behind";
      if (
        comparison.status === "diverged" &&
        mergeBase !== base &&
        mergeBase !== head &&
        aheadBy > 0 &&
        behindBy > 0
      )
        return "diverged";
      throw new JonesActionsError("unqualified");
    } catch (error) {
      throw safeTransportError(error);
    }
  }

  /**
   * Whether `base` is an ancestor of `head` and the merge base is `base`. Ancestry between two
   * exact commits never changes, so a parsed answer is remembered for this client; main
   * membership is still proven on every check because the fresh main SHA is part of the pair.
   * Transport failures and unparsable comparisons are never remembered.
   */
  private async descends(base: string, head: string): Promise<boolean> {
    if (base === head) return true;
    const pair = `${source(base)}...${source(head)}`;
    const known = this.ancestry.get(pair);
    if (known !== undefined) return known;
    const comparison = record(await this.transport.api(`${API_ROOT}/compare/${pair}?per_page=1`));
    if (comparison.status === "ahead") {
      const proven = record(comparison.merge_base_commit).sha === base;
      if (proven) this.remember(pair, true);
      return proven;
    }
    if (comparison.status === "behind" || comparison.status === "diverged")
      this.remember(pair, false);
    return false;
  }

  private remember(pair: string, descends: boolean): void {
    if (this.ancestry.size >= ANCESTRY_MEMO_LIMIT) {
      const oldest = this.ancestry.keys().next();
      if (!oldest.done) this.ancestry.delete(oldest.value);
    }
    this.ancestry.set(pair, descends);
  }

  /** Download changes only the staging cache; it never extracts or launches the candidate app/runtime. */
  async stage(candidate: JonesActionsCandidate, cacheRoot: string): Promise<JonesStagedArtifact> {
    validateJonesActionsCandidate(candidate);
    if (Date.parse(candidate.expiresAt) <= this.now()) throw new JonesActionsError("unavailable");
    const run = record(await this.transport.api(`${API_ROOT}/actions/runs/${candidate.runId}`));
    const ci = record(await this.transport.api(`${API_ROOT}/actions/runs/${candidate.ciRunId}`));
    const commit = record(await this.transport.api(`${API_ROOT}/git/commits/${candidate.source}`));
    const main = record(await this.transport.api(`${API_ROOT}/commits/main`));
    if (
      !canonicalArtifactRun(run, candidateSpec(candidate)) ||
      run.status !== "completed" ||
      run.conclusion !== "success" ||
      run.head_sha !== candidate.source ||
      run.run_attempt !== candidate.runAttempt ||
      run.workflow_id !== candidate.workflowId ||
      !canonicalCiRun(ci) ||
      ci.status !== "completed" ||
      ci.conclusion !== "success" ||
      ci.head_sha !== candidate.source ||
      commit.sha !== candidate.source ||
      record(commit.tree).sha !== candidate.tree ||
      !(await this.descends(candidate.installedSource, candidate.source)) ||
      !(await this.descends(candidate.source, source(main.sha)))
    )
      throw new JonesActionsError("unqualified");
    const artifact = record(
      await this.transport.api(`${API_ROOT}/actions/artifacts/${candidate.artifactId}`),
    );
    if (
      artifact.id !== candidate.artifactId ||
      artifact.name !== candidate.artifactName ||
      digest(artifact.digest) !== candidate.artifactDigest ||
      artifact.expired !== false ||
      artifact.size_in_bytes !== candidate.artifactBytes ||
      !Number.isFinite(Date.parse(text(artifact.expires_at))) ||
      Date.parse(text(artifact.expires_at)) <= this.now() ||
      record(artifact.workflow_run).id !== candidate.runId ||
      record(artifact.workflow_run).head_sha !== candidate.source ||
      record(artifact.workflow_run).head_branch !== "main" ||
      record(artifact.workflow_run).repository_id !== record(run.repository).id ||
      record(artifact.workflow_run).head_repository_id !== record(run.repository).id
    ) {
      throw new JonesActionsError("unqualified");
    }
    const stagedHandle = jonesCandidateHandle(candidate);
    await NodeFSP.mkdir(NodePath.resolve(cacheRoot), { recursive: true, mode: 0o700 });
    const root = await NodeFSP.realpath(NodePath.resolve(cacheRoot));
    try {
      const completed = await findJonesCompletedStage(
        root,
        stagedHandle,
        JONES_ACTIONS_RECEIPT_FILE,
      );
      if (completed !== undefined) {
        const cached = await validateJonesStagedArtifact(completed, candidate);
        if (cached.receipt.event !== run.event) throw new JonesActionsError("unqualified");
        return cached;
      }
    } catch (error) {
      throw error instanceof JonesActionsError ? error : new JonesActionsError("occupied-cache");
    }
    const attempt = await createJonesStageAttempt(root, stagedHandle);
    const working = attempt.directory;
    // Once publication can have happened, keep this attempt on an unknown result.
    let retainAttempt = false;
    let failure: unknown;
    try {
      const archive = NodePath.join(working, "github-artifact.zip");
      await this.transport.download(candidate.artifactId, archive);
      const archiveStat = await NodeFSP.lstat(archive);
      if (
        !archiveStat.isFile() ||
        archiveStat.size !== candidate.artifactBytes ||
        (await hashFile(archive)) !== candidate.artifactDigest
      )
        throw new JonesActionsError("integrity");
      const entries = await readArtifactZip(archive);
      const receiptEntry = entries.find((entry) => entry.name === "ARTIFACT.json");
      if (!receiptEntry || receiptEntry.bytes > 65536) throw new JonesActionsError("integrity");
      const receiptPath = NodePath.join(working, "ARTIFACT.json");
      await extractZipEntry(archive, receiptEntry, receiptPath);
      const receipt = parseJonesArtifactReceipt(
        JSON.parse(await NodeFSP.readFile(receiptPath, "utf8")) as unknown,
        candidate,
      );
      if (receipt.event !== run.event) throw new JonesActionsError("unqualified");
      const permitted = new Set([
        "ARTIFACT.json",
        "SOURCE_COMMIT",
        receipt.artifact,
        candidate.platform === "linux" ? "SHA256SUMS" : `${receipt.artifact}.sha256`,
      ]);
      if (entries.length !== permitted.size || entries.some((entry) => !permitted.has(entry.name)))
        throw new JonesActionsError("integrity");
      for (const entry of entries) {
        if (entry.name !== "ARTIFACT.json")
          await extractZipEntry(archive, entry, NodePath.join(working, entry.name));
      }
      if (
        (await NodeFSP.readFile(NodePath.join(working, "SOURCE_COMMIT"), "utf8")).trim() !==
        candidate.source
      )
        throw new JonesActionsError("integrity");
      if ((await hashFile(NodePath.join(working, receipt.artifact))) !== receipt.sha256)
        throw new JonesActionsError("integrity");
      const staged: JonesStagedArtifact = {
        schema: 1,
        source: "jones-actions",
        channel: "jones-main",
        stagedHandle,
        candidate,
        receipt,
        payloadPath: NodePath.join(working, receipt.artifact),
      };
      await NodeFSP.writeFile(
        NodePath.join(working, JONES_ACTIONS_RECEIPT_FILE),
        JSON.stringify(staged, null, 2) + "\n",
        { flag: "wx", mode: 0o600 },
      );
      const verified = await validateJonesStagedArtifact(working, candidate);
      for (const name of await NodeFSP.readdir(working)) {
        const file = await NodeFSP.open(NodePath.join(working, name), "r");
        try {
          await file.sync();
        } finally {
          await file.close();
        }
      }
      retainAttempt = true;
      const selected = await publishJonesCompletedStage(
        root,
        stagedHandle,
        working,
        JONES_ACTIONS_RECEIPT_FILE,
      );
      if (selected === working) return verified;
      const winner = await validateJonesStagedArtifact(selected, candidate);
      if (winner.receipt.event !== run.event) throw new JonesActionsError("unqualified");
      retainAttempt = false;
      return winner;
    } catch (cause) {
      failure = cause;
      throw cause;
    } finally {
      if (!retainAttempt) {
        try {
          await NodeFSP.rm(attempt.attemptRoot, { recursive: true, force: true });
        } catch (cleanup) {
          if (failure !== undefined)
            throw new AggregateError([failure, cleanup], "Artifact staging and cleanup failed.");
          throw cleanup;
        }
      }
    }
  }
}

export function jonesCandidateHandle(candidate: JonesActionsCandidate): string {
  return NodeCrypto.createHash("sha256")
    .update(
      JSON.stringify([
        candidate.schema,
        candidate.repository,
        candidate.source,
        candidate.tree,
        candidate.installedSource,
        candidate.workflow,
        candidate.workflowId,
        candidate.runId,
        candidate.runAttempt,
        candidate.ciRunId,
        candidate.artifactId,
        candidate.artifactName,
        candidate.artifactDigest,
        candidate.artifactBytes,
        candidate.expiresAt,
        candidate.version,
        candidate.platform,
        candidate.architecture,
      ]),
    )
    .digest("hex");
}

function candidateSpec(candidate: JonesActionsCandidate) {
  return spec(
    candidate.platform,
    candidate.architecture,
    candidate.workflow === PRODUCTION_DESKTOP_WORKFLOW ? PRODUCTION_DESKTOP_ID : undefined,
  );
}

function validateJonesActionsCandidate(candidate: JonesActionsCandidate): void {
  const approved = candidateSpec(candidate);
  if (
    candidate.schema !== 1 ||
    candidate.repository !== JONES_ACTIONS_REPOSITORY ||
    candidate.workflow !== approved.workflow ||
    !PREVIEW.test(candidate.version) ||
    !candidate.version.endsWith(`.${candidate.runId}.${candidate.runAttempt}`) ||
    candidate.artifactName !==
      `${approved.name}-${candidate.runId}-${candidate.runAttempt}--${candidate.version}` ||
    candidate.source === candidate.installedSource ||
    !Number.isFinite(Date.parse(candidate.expiresAt))
  )
    throw new JonesActionsError("unqualified");
  source(candidate.source);
  source(candidate.tree);
  source(candidate.installedSource);
  digest(candidate.artifactDigest);
  for (const value of [
    candidate.runId,
    candidate.runAttempt,
    candidate.workflowId,
    candidate.ciRunId,
    candidate.artifactId,
    candidate.artifactBytes,
  ])
    positive(value);
  if (candidate.artifactBytes > MAX_ARCHIVE_BYTES) throw new JonesActionsError("integrity");
}

function parseJonesArtifactReceipt(
  value: unknown,
  candidate: JonesActionsCandidate,
): JonesArtifactReceipt {
  const item = record(value);
  const expectedArtifact =
    candidate.platform === "linux"
      ? `t3-${candidate.version}-linux-${candidate.architecture}.tar.gz`
      : `T3-Code-${candidate.version}-arm64.dmg`;
  if (
    item.schema !== 1 ||
    item.repository !== candidate.repository ||
    item.source !== candidate.source ||
    item.tree !== candidate.tree ||
    item.workflow !== candidate.workflow ||
    !artifactEvent(candidateSpec(candidate), item.event) ||
    item.ref !== "refs/heads/main" ||
    item.runId !== String(candidate.runId) ||
    item.runAttempt !== String(candidate.runAttempt) ||
    item.version !== candidate.version ||
    item.platform !== candidate.platform ||
    item.architecture !== candidate.architecture ||
    item.artifact !== expectedArtifact
  )
    throw new JonesActionsError("unqualified");
  if (
    candidate.workflow === PRODUCTION_DESKTOP_WORKFLOW &&
    (item.signed !== true ||
      item.notarized !== true ||
      item.bundleIdentifier !== PRODUCTION_DESKTOP_ID ||
      item.teamId !== "2CUPSQS986")
  )
    throw new JonesActionsError("unqualified");
  digest(item.sha256);
  return item as unknown as JonesArtifactReceipt;
}

/** Revalidates fixed candidate binding and both payload hashes at the final host install boundary. */
export async function validateJonesStagedArtifact(
  directory: string,
  expected?: JonesActionsCandidate,
): Promise<JonesStagedArtifact> {
  try {
    const dir = NodePath.resolve(directory);
    if (!(await NodeFSP.lstat(dir)).isDirectory() || (await NodeFSP.realpath(dir)) !== dir)
      throw new JonesActionsError("occupied-cache");
    const receiptFile = NodePath.join(dir, JONES_ACTIONS_RECEIPT_FILE);
    if (
      !(await NodeFSP.lstat(receiptFile)).isFile() ||
      (await NodeFSP.stat(receiptFile)).size > 131072
    )
      throw new JonesActionsError("occupied-cache");
    const item = record(JSON.parse(await NodeFSP.readFile(receiptFile, "utf8")) as unknown);
    const candidate = item.candidate as JonesActionsCandidate;
    validateJonesActionsCandidate(candidate);
    const receipt = parseJonesArtifactReceipt(item.receipt, candidate);
    if (
      item.schema !== 1 ||
      item.source !== "jones-actions" ||
      item.channel !== "jones-main" ||
      item.stagedHandle !== jonesCandidateHandle(candidate) ||
      NodePath.basename(dir) !== item.stagedHandle ||
      (expected && item.stagedHandle !== jonesCandidateHandle(expected)) ||
      item.payloadPath !== NodePath.join(dir, receipt.artifact)
    )
      throw new JonesActionsError("occupied-cache");
    for (const [name, expectedHash] of [
      [receipt.artifact, receipt.sha256],
      ["github-artifact.zip", candidate.artifactDigest],
    ] as const) {
      const file = NodePath.join(dir, name);
      const stat = await NodeFSP.lstat(file);
      if (
        !stat.isFile() ||
        stat.size > MAX_ARCHIVE_BYTES ||
        (name === "github-artifact.zip" && stat.size !== candidate.artifactBytes) ||
        (await hashFile(file)) !== expectedHash
      )
        throw new JonesActionsError("integrity");
    }
    const original = parseJonesArtifactReceipt(
      JSON.parse(
        (await selectArtifactMetadata(NodePath.join(dir, "github-artifact.zip"))).toString("utf8"),
      ) as unknown,
      candidate,
    );
    if (original.sha256 !== receipt.sha256 || original.event !== receipt.event)
      throw new JonesActionsError("integrity");
    return item as unknown as JonesStagedArtifact;
  } catch (error) {
    throw error instanceof JonesActionsError ? error : new JonesActionsError("occupied-cache");
  }
}

async function hashFile(file: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(file)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

interface ZipEntry {
  readonly name: string;
  readonly bytes: number;
  readonly compressed: number;
  readonly offset: number;
  readonly method: number;
  /** Central directory CRC-32; authoritative even when a data descriptor zeroes the local copy. */
  readonly crc: number;
}

/**
 * Exact bounded ARTIFACT.json bytes from a flat Actions envelope, without an external unzip.
 * Parsing grants no authenticity: callers first compare the archive's size and SHA-256 with
 * the GitHub artifact record, and reopening by pathname is not an immutable-file identity.
 */
export async function readJonesArtifactMetadata(archive: string): Promise<Buffer> {
  try {
    return await selectArtifactMetadata(archive);
  } catch (error) {
    throw error instanceof JonesActionsError ? error : new JonesActionsError("integrity");
  }
}

async function selectArtifactMetadata(archive: string): Promise<Buffer> {
  // Duplicate names are refused by the envelope parser, so this selection is exact.
  const entry = (await readArtifactZip(archive)).find((value) => value.name === "ARTIFACT.json");
  if (!entry || entry.bytes > 65536 || entry.compressed > 131072)
    throw new JonesActionsError("integrity");
  return await readZipMetadata(archive, entry);
}

async function readZipMetadata(file: string, entry: ZipEntry): Promise<Buffer> {
  const handle = await NodeFSP.open(file, "r");
  try {
    const compressed = Buffer.alloc(entry.compressed);
    const read = await handle.read(compressed, 0, compressed.length, entry.offset);
    if (read.bytesRead !== compressed.length) throw new JonesActionsError("integrity");
    const data =
      entry.method === 8
        ? NodeZlib.inflateRawSync(compressed, { maxOutputLength: 65536 })
        : compressed;
    // Matches unzip's corruption rejection; CRC-32 detects damage, not substitution.
    if (data.length !== entry.bytes || NodeZlib.crc32(data) !== entry.crc)
      throw new JonesActionsError("integrity");
    return data;
  } finally {
    await handle.close();
  }
}

/** Only flat regular-file artifact envelopes are accepted. Inner app/archive extraction is host-specific. */
async function readArtifactZip(file: string): Promise<ZipEntry[]> {
  const handle = await NodeFSP.open(file, "r");
  try {
    const size = (await handle.stat()).size;
    if (size < 22 || size > MAX_ARCHIVE_BYTES) throw new JonesActionsError("integrity");
    const tail = Buffer.alloc(Math.min(size, 65557));
    if ((await handle.read(tail, 0, tail.length, size - tail.length)).bytesRead !== tail.length)
      throw new JonesActionsError("integrity");
    let end = tail.length - 22;
    while (end >= 0 && tail.readUInt32LE(end) !== 0x06054b50) end--;
    if (
      end < 0 ||
      end + 22 + tail.readUInt16LE(end + 20) !== tail.length ||
      tail.readUInt16LE(end + 4) !== 0 ||
      tail.readUInt16LE(end + 6) !== 0
    )
      throw new JonesActionsError("integrity");
    const count = tail.readUInt16LE(end + 10),
      bytes = tail.readUInt32LE(end + 12),
      offset = tail.readUInt32LE(end + 16);
    if (
      count < 1 ||
      count > 8 ||
      count !== tail.readUInt16LE(end + 8) ||
      bytes > 65536 ||
      offset + bytes !== size - tail.length + end
    )
      throw new JonesActionsError("integrity");
    const central = Buffer.alloc(bytes);
    if ((await handle.read(central, 0, bytes, offset)).bytesRead !== bytes)
      throw new JonesActionsError("integrity");
    const result: ZipEntry[] = [];
    let cursor = 0;
    let expanded = 0;
    for (let index = 0; index < count; index++) {
      if (cursor + 46 > central.length || central.readUInt32LE(cursor) !== 0x02014b50)
        throw new JonesActionsError("integrity");
      const nameLength = central.readUInt16LE(cursor + 28),
        extra = central.readUInt16LE(cursor + 30),
        comment = central.readUInt16LE(cursor + 32);
      if (cursor + 46 + nameLength + extra + comment > central.length)
        throw new JonesActionsError("integrity");
      const name = central.subarray(cursor + 46, cursor + 46 + nameLength).toString("utf8");
      const mode = central.readUInt32LE(cursor + 38) >>> 16;
      const flags = central.readUInt16LE(cursor + 8),
        method = central.readUInt16LE(cursor + 10);
      const crc = central.readUInt32LE(cursor + 16),
        compressed = central.readUInt32LE(cursor + 20),
        entryBytes = central.readUInt32LE(cursor + 24),
        localOffset = central.readUInt32LE(cursor + 42);
      expanded += entryBytes;
      if (
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,240}$/.test(name) ||
        result.some((entry) => entry.name === name) ||
        ((mode & 0o170000) !== 0 && (mode & 0o170000) !== 0o100000) ||
        (central.readUInt32LE(cursor + 38) & 0x10) !== 0 ||
        flags & 1 ||
        (method !== 0 && method !== 8) ||
        expanded > MAX_ARCHIVE_BYTES ||
        localOffset + 30 + nameLength + compressed > offset
      )
        throw new JonesActionsError("integrity");
      const local = Buffer.alloc(30 + nameLength);
      if ((await handle.read(local, 0, local.length, localOffset)).bytesRead !== local.length)
        throw new JonesActionsError("integrity");
      if (
        local.readUInt32LE(0) !== 0x04034b50 ||
        local.readUInt16LE(6) !== flags ||
        local.readUInt16LE(8) !== method ||
        local.readUInt16LE(26) !== nameLength ||
        local.subarray(30).toString("utf8") !== name ||
        // A data descriptor (bit 3) may leave local CRC and sizes zero; the central record governs.
        ((flags & 0x08) === 0 &&
          (local.readUInt32LE(14) !== crc ||
            local.readUInt32LE(18) !== compressed ||
            local.readUInt32LE(22) !== entryBytes))
      )
        throw new JonesActionsError("integrity");
      const dataOffset = localOffset + 30 + nameLength + local.readUInt16LE(28);
      if (dataOffset + compressed > offset) throw new JonesActionsError("integrity");
      result.push({ name, bytes: entryBytes, compressed, offset: dataOffset, method, crc });
      cursor += 46 + nameLength + extra + comment;
    }
    if (cursor !== central.length) throw new JonesActionsError("integrity");
    return result;
  } finally {
    await handle.close();
  }
}

async function extractZipEntry(
  archive: string,
  entry: ZipEntry,
  destination: string,
): Promise<void> {
  let bytes = 0;
  const limit = new NodeStream.Transform({
    transform(chunk: Buffer, _encoding, callback) {
      bytes += chunk.length;
      callback(bytes > entry.bytes ? new JonesActionsError("integrity") : null, chunk);
    },
  });
  const output = NodeFS.createWriteStream(destination, { flags: "wx", mode: 0o600 });
  if (entry.compressed === 0) {
    output.end();
    await new Promise<void>((resolve, reject) => {
      output.once("finish", resolve);
      output.once("error", reject);
    });
  } else {
    const input = NodeFS.createReadStream(archive, {
      start: entry.offset,
      end: entry.offset + entry.compressed - 1,
    });
    if (entry.method === 8)
      await NodeStreamPromises.pipeline(input, NodeZlib.createInflateRaw(), limit, output);
    else await NodeStreamPromises.pipeline(input, limit, output);
  }
  if (bytes !== entry.bytes) throw new JonesActionsError("integrity");
}
