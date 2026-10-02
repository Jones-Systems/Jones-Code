// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Node-only launcher/desktop transport and receipt boundary; clock is injected for qualification tests.
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeZlib from "node:zlib";

const JONES_ACTIONS_REPOSITORY = "Jones-Systems/Jones-Code";
export const JONES_ACTIONS_RECEIPT_FILE = "jones-actions-stage.json";
const MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024;
const SHA = /^[a-f0-9]{40}$/;
const HASH = /^[a-f0-9]{64}$/;
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
  readonly event: "push";
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

export type JonesActionsCheckResult =
  | { readonly state: "available"; readonly candidate: JonesActionsCandidate }
  | { readonly state: "no-new" }
  | { readonly state: "building"; readonly retryAfterMs: number }
  | {
      readonly state: "blocked";
      readonly reason: JonesActionsBlockedReason;
      readonly retryAfterMs: number;
    };

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
function spec(platform: JonesActionsPlatform, architecture: JonesActionsArchitecture) {
  if (platform === "darwin" && architecture === "arm64")
    return {
      workflow: ".github/workflows/artifact-desktop-mac.yml",
      name: "desktop-mac-arm64",
    };
  if (platform === "linux" && (architecture === "x64" || architecture === "arm64"))
    return {
      workflow: ".github/workflows/artifact-cli-linux.yml",
      name: `jones-code-cli-linux-${architecture}`,
    };
  throw new JonesActionsError("unsupported-platform");
}
function canonicalRun(run: Record<string, unknown>, workflow: string): boolean {
  return (
    run.path === workflow &&
    run.event === "push" &&
    run.head_branch === "main" &&
    record(run.repository).full_name === JONES_ACTIONS_REPOSITORY &&
    record(run.head_repository).full_name === JONES_ACTIONS_REPOSITORY
  );
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
  constructor(
    options: { readonly transport?: JonesActionsTransport; readonly now?: () => number } = {},
  ) {
    this.transport = options.transport ?? createGhJonesActionsTransport();
    this.now = options.now ?? Date.now;
  }

  async check(input: {
    readonly installedSource: string;
    readonly platform: JonesActionsPlatform;
    readonly architecture: JonesActionsArchitecture;
  }): Promise<JonesActionsCheckResult> {
    try {
      source(input.installedSource);
      const approved = spec(input.platform, input.architecture);
      const main = record(await this.transport.api(`${API_ROOT}/commits/main`));
      const mainSource = source(main.sha);
      if (mainSource === input.installedSource) return { state: "no-new" };
      // Installed source must itself be on canonical main, including after a force rewrite.
      if (!(await this.descends(input.installedSource, mainSource)))
        throw new JonesActionsError("unqualified");
      let building = false;
      for (let page = 1; page <= 3; page++) {
        const runs = rows(
          await this.transport.api(
            `${API_ROOT}/actions/workflows/${NodePath.posix.basename(approved.workflow)}/runs?branch=main&event=push&per_page=50&page=${page}`,
          ),
          "workflow_runs",
        );
        for (const listed of runs) {
          if (!canonicalRun(listed, approved.workflow)) continue;
          if (listed.status !== "completed") {
            building = true;
            continue;
          }
          if (listed.conclusion !== "success") continue;
          const run = record(
            await this.transport.api(`${API_ROOT}/actions/runs/${positive(listed.id)}`),
          );
          if (
            !canonicalRun(run, approved.workflow) ||
            run.status !== "completed" ||
            run.conclusion !== "success"
          )
            continue;
          const candidateSource = source(run.head_sha);
          if (
            candidateSource === input.installedSource ||
            !(await this.descends(input.installedSource, candidateSource)) ||
            !(await this.descends(candidateSource, mainSource))
          )
            continue;
          const ciRuns = rows(
            await this.transport.api(
              `${API_ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${candidateSource}&per_page=100&page=1`,
            ),
            "workflow_runs",
          );
          const ci = ciRuns.find(
            (item) =>
              canonicalRun(item, ".github/workflows/ci.yml") &&
              item.head_sha === candidateSource &&
              item.status === "completed" &&
              item.conclusion === "success",
          );
          if (!ci) {
            building = true;
            continue;
          }
          const runId = positive(run.id),
            runAttempt = positive(run.run_attempt);
          const artifacts = rows(
            await this.transport.api(
              `${API_ROOT}/actions/runs/${runId}/artifacts?per_page=100&page=1`,
            ),
            "artifacts",
          );
          for (const artifact of artifacts) {
            const name = text(artifact.name);
            const prefix = `${approved.name}-${runId}-${runAttempt}--`;
            if (!name.startsWith(prefix)) continue;
            const version = name.slice(prefix.length);
            if (!PREVIEW.test(version) || !version.endsWith(`.${runId}.${runAttempt}`)) continue;
            const expiresAt = text(artifact.expires_at);
            if (
              artifact.expired !== false ||
              !Number.isFinite(Date.parse(expiresAt)) ||
              Date.parse(expiresAt) <= this.now()
            )
              continue;
            const artifactRun = record(artifact.workflow_run);
            if (
              artifactRun.id !== runId ||
              artifactRun.head_sha !== candidateSource ||
              artifactRun.head_branch !== "main" ||
              artifactRun.repository_id !== record(run.repository).id ||
              artifactRun.head_repository_id !== record(run.repository).id
            )
              continue;
            const commit = record(
              await this.transport.api(`${API_ROOT}/git/commits/${candidateSource}`),
            );
            if (commit.sha !== candidateSource) throw new JonesActionsError("unqualified");
            const artifactBytes = positive(artifact.size_in_bytes);
            if (artifactBytes > MAX_ARCHIVE_BYTES) throw new JonesActionsError("integrity");
            return {
              state: "available",
              candidate: {
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
              },
            };
          }
        }
        if (runs.length < 50) break;
      }
      return building
        ? { state: "building", retryAfterMs: 240000 }
        : { state: "blocked", reason: "unavailable", retryAfterMs: 240000 };
    } catch (error) {
      return { state: "blocked", reason: safeTransportError(error).reason, retryAfterMs: 240000 };
    }
  }

  private async descends(base: string, head: string): Promise<boolean> {
    if (base === head) return true;
    const comparison = record(
      await this.transport.api(`${API_ROOT}/compare/${source(base)}...${source(head)}`),
    );
    return comparison.status === "ahead" && record(comparison.merge_base_commit).sha === base;
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
      !canonicalRun(run, candidate.workflow) ||
      run.status !== "completed" ||
      run.conclusion !== "success" ||
      run.head_sha !== candidate.source ||
      run.run_attempt !== candidate.runAttempt ||
      run.workflow_id !== candidate.workflowId ||
      !canonicalRun(ci, ".github/workflows/ci.yml") ||
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
    const root = await NodeFSP.realpath(NodePath.resolve(cacheRoot)),
      final = NodePath.join(root, stagedHandle);
    try {
      await NodeFSP.lstat(final);
      return await validateJonesStagedArtifact(final, candidate);
    } catch (error) {
      if (!isMissing(error))
        throw error instanceof JonesActionsError ? error : new JonesActionsError("occupied-cache");
    }
    const working = await NodeFSP.mkdtemp(NodePath.join(root, ".download-"));
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
        payloadPath: NodePath.join(final, receipt.artifact),
      };
      await NodeFSP.writeFile(
        NodePath.join(working, JONES_ACTIONS_RECEIPT_FILE),
        JSON.stringify(staged, null, 2) + "\n",
        { flag: "wx", mode: 0o600 },
      );
      // The exclusive reservation protects occupied paths even on platforms where rename replaces empty directories.
      try {
        await NodeFSP.mkdir(final, { mode: 0o700 });
      } catch {
        throw new JonesActionsError("occupied-cache");
      }
      try {
        for (const name of await NodeFSP.readdir(working))
          await NodeFSP.rename(NodePath.join(working, name), NodePath.join(final, name));
      } catch (error) {
        // A partial owned stage remains visibly invalid. Never recycle it as a completed cache entry.
        throw safeTransportError(error);
      }
      return await validateJonesStagedArtifact(final, candidate);
    } finally {
      await NodeFSP.rm(working, { recursive: true, force: true });
    }
  }
}

function isMissing(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
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

function validateJonesActionsCandidate(candidate: JonesActionsCandidate): void {
  const approved = spec(candidate.platform, candidate.architecture);
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
    item.event !== "push" ||
    item.ref !== "refs/heads/main" ||
    item.runId !== String(candidate.runId) ||
    item.runAttempt !== String(candidate.runAttempt) ||
    item.version !== candidate.version ||
    item.platform !== candidate.platform ||
    item.architecture !== candidate.architecture ||
    item.artifact !== expectedArtifact
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
    const archive = NodePath.join(dir, "github-artifact.zip");
    const entries = await readArtifactZip(archive);
    const entry = entries.find((value) => value.name === "ARTIFACT.json");
    if (!entry || entry.bytes > 65536 || entry.compressed > 131072)
      throw new JonesActionsError("integrity");
    const original = parseJonesArtifactReceipt(
      JSON.parse((await readZipMetadata(archive, entry)).toString("utf8")) as unknown,
      candidate,
    );
    if (original.sha256 !== receipt.sha256) throw new JonesActionsError("integrity");
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
    if (data.length !== entry.bytes) throw new JonesActionsError("integrity");
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
    await handle.read(tail, 0, tail.length, size - tail.length);
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
    await handle.read(central, 0, bytes, offset);
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
      const compressed = central.readUInt32LE(cursor + 20),
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
      await handle.read(local, 0, local.length, localOffset);
      if (
        local.readUInt32LE(0) !== 0x04034b50 ||
        local.readUInt16LE(6) !== flags ||
        local.readUInt16LE(8) !== method ||
        local.readUInt16LE(26) !== nameLength ||
        local.subarray(30).toString("utf8") !== name
      )
        throw new JonesActionsError("integrity");
      const dataOffset = localOffset + 30 + nameLength + local.readUInt16LE(28);
      if (dataOffset + compressed > offset) throw new JonesActionsError("integrity");
      result.push({ name, bytes: entryBytes, compressed, offset: dataOffset, method });
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
