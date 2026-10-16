// @effect-diagnostics nodeBuiltinImport:off globalFetch:off - The one-shot CI weights picker reads the Actions REST API and writes step outputs before any Effect runtime exists.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeZlib from "node:zlib";

import {
  MAX_WEIGHTS_BYTES,
  RUN_ID_PATTERN,
  parseWeightsEnvelope,
  weightsDigest,
  type WeightsSource,
} from "./envelope.ts";

export const WORKFLOW_PATH = ".github/workflows/ci.yml";
export const SOURCE_BRANCH = "main";
export const DEFAULT_SHARD_COUNT = 6;
const ALLOWED_SHARD_COUNTS = [6, 8];
/** How many of the newest successful main pushes to look through. */
export const MAX_SOURCE_RUNS = 20;
/** The combined weights file inside each `server-shard-weights-a<attempt>` artifact. */
export const WEIGHTS_FILE_NAME = "server-shard-weights.json";
const WEIGHTS_ARTIFACT_PATTERN = /^server-shard-weights-a([1-9][0-9]{0,3})$/;
/**
 * Shards receive the weights through an environment variable, and Linux caps a
 * single one at 128 KiB. About 600 files take roughly 40 KB.
 */
export const MAX_TRANSPORT_BYTES = 120_000;

/** The fields of a workflow run this picker relies on, as the REST API returns them. */
export interface WorkflowRun {
  id: number;
  run_attempt?: number;
  event: string;
  head_branch: string | null;
  head_sha: string;
  path: string;
  status: string | null;
  conclusion: string | null;
  repository: { full_name: string };
  head_repository: { full_name: string } | null;
}

export interface RunArtifact {
  id: number;
  name: string;
  size_in_bytes: number;
  expired: boolean;
  workflow_run?: { id?: number; head_sha?: string } | null;
}

/** The Actions API calls the picker makes. Every method throws when the request fails. */
export interface ActionsClient {
  /** Newest first: this workflow's successful push runs on the source branch. */
  listSourceRuns(limit: number): Promise<WorkflowRun[]>;
  getRun(runId: string): Promise<WorkflowRun>;
  listArtifacts(runId: string): Promise<RunArtifact[]>;
  /** The artifact's zip archive, at most `maxBytes` long. */
  downloadArtifact(artifactId: number, maxBytes: number): Promise<Uint8Array>;
}

export type Selection =
  | { mode: "checked-in"; reason: string; skipped: string[] }
  | {
      mode: "runtime";
      reason: string;
      skipped: string[];
      sourceRunId: string;
      artifactName: string;
      text: string;
      digest: string;
    };

/** A pinned source that cannot be used. The run must fail rather than switch weights. */
export class SelectionRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SelectionRefusal";
  }
}

type Candidate = { ok: true; artifactName: string; text: string } | { ok: false; reason: string };

/** Why a run cannot be a source, or undefined when it can. */
export function disqualify(
  run: WorkflowRun,
  { repository, currentRunId }: { repository: string; currentRunId: string },
): string | undefined {
  if (String(run.id) === currentRunId) return "it is the current run";
  if (run.repository.full_name !== repository) return "it ran in another repository";
  if (run.head_repository?.full_name !== repository) return "its head is from another repository";
  if (run.path !== WORKFLOW_PATH && !run.path.startsWith(`${WORKFLOW_PATH}@`)) {
    return `it ran ${run.path}`;
  }
  if (run.event !== "push") return `it was a ${run.event} run`;
  if (run.head_branch !== SOURCE_BRANCH) return `it ran on ${run.head_branch ?? "no branch"}`;
  if (run.status !== "completed" || run.conclusion !== "success") {
    return `it ended ${run.conclusion ?? run.status ?? "unknown"}`;
  }
  return undefined;
}

function invalid(message: string): never {
  throw new Error(`Invalid weights archive: ${message}`);
}

/**
 * Reads the single file of a zip archive. Accepts only an unencrypted stored or
 * deflated entry with the expected name, and checks its size and CRC-32.
 */
export function readSingleZipEntry(
  archive: Uint8Array,
  { name, maxBytes }: { name: string; maxBytes: number },
): string {
  const bytes = Buffer.from(archive.buffer, archive.byteOffset, archive.byteLength);
  // The end record is the last 22 bytes, before a comment of at most 64 KiB.
  const earliest = Math.max(0, bytes.length - 22 - 0xffff);
  let end = -1;
  for (let offset = bytes.length - 22; offset >= earliest; offset--) {
    if (bytes.readUInt32LE(offset) === 0x06054b50) {
      end = offset;
      break;
    }
  }
  if (end < 0) invalid("no end of central directory");
  if (bytes.readUInt16LE(end + 10) !== 1) invalid("expected exactly one file");
  const central = bytes.readUInt32LE(end + 16);
  if (central + 46 > bytes.length || bytes.readUInt32LE(central) !== 0x02014b50) {
    invalid("bad central directory");
  }
  const flags = bytes.readUInt16LE(central + 8);
  const method = bytes.readUInt16LE(central + 10);
  const crc = bytes.readUInt32LE(central + 16);
  const compressedSize = bytes.readUInt32LE(central + 20);
  const size = bytes.readUInt32LE(central + 24);
  const nameLength = bytes.readUInt16LE(central + 28);
  const local = bytes.readUInt32LE(central + 42);
  if (flags & 1) invalid("encrypted entry");
  if (bytes.toString("utf8", central + 46, central + 46 + nameLength) !== name) {
    invalid(`expected ${name}`);
  }
  if (size > maxBytes || compressedSize > bytes.length) invalid("entry exceeds the size limit");
  if (local + 30 > bytes.length || bytes.readUInt32LE(local) !== 0x04034b50) {
    invalid("bad local header");
  }
  const start = local + 30 + bytes.readUInt16LE(local + 26) + bytes.readUInt16LE(local + 28);
  if (start + compressedSize > bytes.length) invalid("truncated entry");
  const stored = bytes.subarray(start, start + compressedSize);
  let data: Buffer;
  if (method === 0) data = stored;
  else if (method === 8) data = NodeZlib.inflateRawSync(stored, { maxOutputLength: maxBytes });
  else return invalid(`unsupported compression method ${method}`);
  if (data.length !== size || NodeZlib.crc32(data) !== crc) {
    invalid("entry does not match its size and CRC");
  }
  return data.toString("utf8");
}

async function candidate(
  client: ActionsClient,
  run: WorkflowRun,
  repository: string,
): Promise<Candidate> {
  const artifacts = (await client.listArtifacts(String(run.id)))
    .map((artifact) => ({
      artifact,
      attempt: Number(WEIGHTS_ARTIFACT_PATTERN.exec(artifact.name)?.[1]),
    }))
    .filter(
      ({ artifact, attempt }) =>
        Number.isSafeInteger(attempt) &&
        attempt === run.run_attempt &&
        !artifact.expired &&
        (artifact.workflow_run?.id === undefined || artifact.workflow_run.id === run.id),
    )
    .toSorted((a, b) => b.attempt - a.attempt);
  const newest = artifacts[0];
  if (newest === undefined) return { ok: false, reason: "it has no unexpired combined weights" };
  const { artifact, attempt } = newest;
  if (artifact.size_in_bytes > MAX_WEIGHTS_BYTES) {
    return { ok: false, reason: `${artifact.name} is too large` };
  }
  // A failed download is an API problem; a bad archive or file only rules out this run.
  const archive = await client.downloadArtifact(artifact.id, MAX_WEIGHTS_BYTES);
  let text: string;
  let source: WeightsSource;
  try {
    text = readSingleZipEntry(archive, { name: WEIGHTS_FILE_NAME, maxBytes: MAX_WEIGHTS_BYTES });
    ({ source } = parseWeightsEnvelope(text));
  } catch (error) {
    return { ok: false, reason: describe(error) };
  }
  const mismatch = [
    source.repository === repository ? undefined : "repository",
    source.workflowRef === `${repository}/${WORKFLOW_PATH}@refs/heads/${SOURCE_BRANCH}`
      ? undefined
      : "workflow",
    source.runId === String(run.id) ? undefined : "run ID",
    source.runAttempt === attempt ? undefined : "attempt",
    source.headSha === run.head_sha ? undefined : "head SHA",
    source.event === "push" ? undefined : "event",
    source.ref === `refs/heads/${SOURCE_BRANCH}` ? undefined : "ref",
  ].filter((field) => field !== undefined);
  if (mismatch.length > 0) {
    return { ok: false, reason: `${artifact.name} records a different ${mismatch.join(", ")}` };
  }
  if (Buffer.byteLength(text, "utf8") > MAX_TRANSPORT_BYTES) {
    return { ok: false, reason: `${artifact.name} is too large to hand to the shards` };
  }
  return { ok: true, artifactName: artifact.name, text };
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function runtime(
  run: WorkflowRun,
  found: { artifactName: string; text: string },
  skipped: string[],
) {
  return {
    mode: "runtime",
    reason: `using ${found.artifactName} from run ${run.id}`,
    skipped,
    sourceRunId: String(run.id),
    artifactName: found.artifactName,
    text: found.text,
    digest: weightsDigest(found.text),
  } as const;
}

/**
 * Picks one weights source for the whole run. Without a pin it takes the newest
 * qualifying main push with valid combined weights, and falls back to the
 * checked-in map, with the reason, when the API fails or nothing qualifies. A
 * pinned source must qualify, or this throws `SelectionRefusal`.
 */
export async function selectWeights(
  client: ActionsClient,
  options: { repository: string; currentRunId: string; pinnedRunId?: string | undefined },
): Promise<Selection> {
  const { repository, pinnedRunId } = options;
  if (pinnedRunId) {
    if (!RUN_ID_PATTERN.test(pinnedRunId)) {
      throw new SelectionRefusal("pinned run ID is not a run ID");
    }
    let run: WorkflowRun;
    let found: Candidate;
    try {
      run = await client.getRun(pinnedRunId);
      const reason = disqualify(run, options);
      if (reason !== undefined) throw new SelectionRefusal(`pinned run ${pinnedRunId}: ${reason}`);
      found = await candidate(client, run, repository);
    } catch (error) {
      if (error instanceof SelectionRefusal) throw error;
      throw new SelectionRefusal(`pinned run ${pinnedRunId}: ${describe(error)}`);
    }
    if (!found.ok) throw new SelectionRefusal(`pinned run ${pinnedRunId}: ${found.reason}`);
    return runtime(run, found, []);
  }

  const skipped: string[] = [];
  try {
    const runs = (await client.listSourceRuns(MAX_SOURCE_RUNS)).slice(0, MAX_SOURCE_RUNS);
    for (const run of runs) {
      const reason = disqualify(run, options);
      const found = reason === undefined ? await candidate(client, run, repository) : undefined;
      if (found?.ok) return runtime(run, found, skipped);
      skipped.push(`run ${run.id}: ${reason ?? (found && !found.ok ? found.reason : "unusable")}`);
    }
  } catch (error) {
    return { mode: "checked-in", reason: `Actions API: ${describe(error)}`, skipped };
  }
  return {
    mode: "checked-in",
    reason: `no usable weights in the newest ${MAX_SOURCE_RUNS} successful ${SOURCE_BRANCH} pushes`,
    skipped,
  };
}

export function parseShardCount(input: string | undefined): number {
  if (!input) return DEFAULT_SHARD_COUNT;
  const count = Number(input);
  if (!ALLOWED_SHARD_COUNTS.includes(count)) {
    throw new Error(`server_shards must be one of ${ALLOWED_SHARD_COUNTS.join(", ")}`);
  }
  return count;
}

/**
 * Step outputs for the shards. The weights go in a multiline value whose
 * delimiter is random and never a line of the value. The value drops the file's
 * final newline, which the shards restore before checking the digest.
 */
export function formatOutputs(
  selection: Selection,
  shardCount: number,
  randomHex: () => string = () => NodeCrypto.randomBytes(16).toString("hex"),
): string {
  const oneLine = (value: string) => value.replaceAll(/[\r\n]+/g, " ");
  const lines = [
    `mode=${selection.mode}`,
    `reason=${oneLine(selection.reason)}`,
    `source_run_id=${selection.mode === "runtime" ? selection.sourceRunId : ""}`,
    `digest=${selection.mode === "runtime" ? selection.digest : ""}`,
    `shard_count=${shardCount}`,
    `shards=${JSON.stringify(Array.from({ length: shardCount }, (_, i) => i + 1))}`,
  ];
  if (selection.mode === "runtime") {
    const valueLines = selection.text.split("\n");
    let delimiter = `JONES_WEIGHTS_${randomHex()}`;
    for (let attempt = 0; valueLines.includes(delimiter); attempt++) {
      if (attempt >= 5) throw new Error("could not choose an output delimiter");
      delimiter = `JONES_WEIGHTS_${randomHex()}`;
    }
    if (!selection.text.endsWith("\n")) throw new Error("weights text must end with a newline");
    lines.push(`weights<<${delimiter}`, `${selection.text}${delimiter}`);
  } else {
    lines.push("weights=");
  }
  return `${lines.join("\n")}\n`;
}

export function formatSummary(selection: Selection, shardCount: number): string {
  const lines = [
    "### Server shard weights",
    "",
    `- Mode: \`${selection.mode}\` (${selection.reason})`,
    `- Shards: ${shardCount}`,
  ];
  if (selection.mode === "runtime") {
    lines.push(`- Source run: ${selection.sourceRunId}`, `- Digest: \`${selection.digest}\``);
  }
  if (selection.skipped.length > 0) {
    lines.push("", "Skipped sources:", ...selection.skipped.map((reason) => `- ${reason}`));
  }
  return `${lines.join("\n")}\n`;
}

/** The Actions REST API through `fetch`, authenticated with the job's token. */
export function restActionsClient({
  apiUrl,
  repository,
  token,
}: {
  apiUrl: string;
  repository: string;
  token: string;
}): ActionsClient {
  const headers = {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${token}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
  const json = async <T>(path: string): Promise<T> => {
    const response = await fetch(`${apiUrl}/repos/${repository}${path}`, { headers });
    if (!response.ok) throw new Error(`${path} returned ${response.status}`);
    return (await response.json()) as T;
  };
  const bounded = async (response: Response, maxBytes: number): Promise<Uint8Array> => {
    if (!response.ok) throw new Error(`artifact download returned ${response.status}`);
    if (Number(response.headers.get("content-length") ?? 0) > maxBytes) {
      throw new Error("artifact archive exceeds the size limit");
    }
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > maxBytes) throw new Error("artifact archive exceeds the size limit");
    return bytes;
  };
  const workflowFile = WORKFLOW_PATH.split("/").at(-1);
  return {
    listSourceRuns: async (limit) => {
      const query = new URLSearchParams({
        branch: SOURCE_BRANCH,
        event: "push",
        status: "success",
        exclude_pull_requests: "true",
        per_page: String(limit),
      });
      const page = await json<{ workflow_runs: WorkflowRun[] }>(
        `/actions/workflows/${workflowFile}/runs?${query}`,
      );
      return page.workflow_runs;
    },
    getRun: (runId) => json<WorkflowRun>(`/actions/runs/${runId}`),
    listArtifacts: async (runId) =>
      (await json<{ artifacts: RunArtifact[] }>(`/actions/runs/${runId}/artifacts?per_page=100`))
        .artifacts,
    downloadArtifact: async (artifactId, maxBytes) => {
      // The API answers with a short-lived redirect to blob storage, which
      // must not receive the token.
      const response = await fetch(
        `${apiUrl}/repos/${repository}/actions/artifacts/${artifactId}/zip`,
        { headers, redirect: "manual" },
      );
      const location = response.headers.get("location");
      if (response.status >= 300 && response.status < 400 && location) {
        return bounded(await fetch(location), maxBytes);
      }
      return bounded(response, maxBytes);
    },
  };
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

async function main(): Promise<void> {
  const repository = required("GITHUB_REPOSITORY");
  const shardCount = parseShardCount(process.env.INPUT_SERVER_SHARDS);
  const selection = await selectWeights(
    restActionsClient({
      apiUrl: process.env.GITHUB_API_URL ?? "https://api.github.com",
      repository,
      token: required("GH_TOKEN"),
    }),
    {
      repository,
      currentRunId: required("GITHUB_RUN_ID"),
      pinnedRunId: process.env.INPUT_WEIGHTS_SOURCE_RUN_ID?.trim(),
    },
  );
  NodeFS.appendFileSync(required("GITHUB_OUTPUT"), formatOutputs(selection, shardCount));
  const summary = formatSummary(selection, shardCount);
  if (process.env.GITHUB_STEP_SUMMARY) {
    NodeFS.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
  }
  process.stdout.write(summary);
}

if (import.meta.main) {
  main().catch((error: unknown) => {
    process.stderr.write(`Server shard weights selection failed: ${describe(error)}\n`);
    process.exitCode = 1;
  });
}
