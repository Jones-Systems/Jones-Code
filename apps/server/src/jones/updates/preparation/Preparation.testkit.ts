// @effect-diagnostics nodeBuiltinImport:off globalDate:off
// Synthetic homes, Actions artifacts and stand-in tools for the preparation tests and the packaged
// worker smoke. Nothing here reads real state, credentials or the network.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeZlib from "node:zlib";
import type { JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  QUALIFIED_RUNTIME_RECEIPT,
  stageQualifiedRuntime,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "../../cloud/qualifiedRuntime.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../../cloud/serviceProtocol.ts";
import type { PreparationHost } from "./protocol.ts";
import type { PreparationWorkerCommand } from "./supervisor.ts";

export const BASELINE = "0.0.0-preview.20261002.100";
export const VERSION = "0.0.0-preview.20261002.101.2";
export const INSTALLED = "c".repeat(40);
export const SOURCE = "a".repeat(40);
const TREE = "b".repeat(40);
export const fixtureHost: PreparationHost = {
  platform: "linux",
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Fixture payloads run on this host's architecture with an injected Linux platform.
  architecture: NodeOS.arch() === "arm64" ? "arm64" : "x64",
};

/** This checkout's own server entry under the running Node, exactly as a script install starts it. */
export const sourceWorkerCommand: PreparationWorkerCommand = {
  file: process.execPath,
  args: [NodeURL.fileURLToPath(new URL("../../../bin.ts", import.meta.url))],
};

// oxlint-disable-next-line t3code/no-global-process-runtime -- A real worker's lowest priority is bounded by this host's kernel; tests inject each platform.
const hostPlatform = NodeOS.platform();
/** Darwin's `PRIO_MAX`. Linux clamps niceness at `PRIORITY_LOW` instead. */
const DARWIN_LOWEST_PRIORITY = 20;

/**
 * Whether a worker reported a lowest priority. It requests `PRIORITY_LOW`, but `nice -n 19` adds
 * to an inherited niceness, so Darwin can clamp it at 20, which no unprivileged process can undo.
 * Normal, raised, missing and non-integer priorities all fail.
 */
export function isLowestWorkerPriority(
  priority: number | null | undefined,
  platform: NodeJS.Platform = hostPlatform,
): boolean {
  const lowest = NodeOS.constants.priority.PRIORITY_LOW;
  const ceiling = platform === "darwin" ? DARWIN_LOWEST_PRIORITY : lowest;
  return (
    typeof priority === "number" &&
    Number.isSafeInteger(priority) &&
    priority >= lowest &&
    priority <= ceiling
  );
}

const sha256 = (value: Buffer | string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");

const receipt = (
  version: string,
  sourceSha: string,
): Omit<QualifiedRuntimeReceipt, "payloadSha256"> => ({
  protocol: 1,
  repository: "Jones-Systems/Jones-Code",
  channel: "jones-main",
  version,
  sourceSha,
  sourceTree: TREE,
  installedSourceSha: INSTALLED,
  runId: 101,
  runAttempt: 2,
  artifactId: 301,
  workflow: ".github/workflows/artifact-cli-linux.yml",
  artifactDigest: `sha256:${"d".repeat(64)}`,
  archiveSha256: "e".repeat(64),
  platform: fixtureHost.platform,
  architecture: fixtureHost.architecture,
});

export interface PreparationHome {
  /** Scratch root holding the home and every stand-in tool; removed by `remove`. */
  readonly root: string;
  readonly base: string;
  readonly dbPath: string;
  readonly cacheRoot: string;
  readonly tools: string;
  readonly remove: () => Promise<void>;
}

/** Writes a runtime payload: an executable `t3` and a client, padded to at least `bytes`. */
async function writePayload(
  directory: string,
  options: { readonly t3?: string; readonly bytes?: number } = {},
): Promise<void> {
  await NodeFSP.mkdir(NodePath.join(directory, "client"), { recursive: true });
  await NodeFSP.writeFile(NodePath.join(directory, "t3"), options.t3 ?? "#!/bin/sh\nexit 0\n", {
    mode: 0o700,
  });
  await NodeFSP.writeFile(NodePath.join(directory, "client", "index.html"), "fixture");
  const chunk = 8 * 1024 * 1024;
  for (let written = 0, index = 0; written < (options.bytes ?? 0); written += chunk, index += 1)
    await NodeFSP.writeFile(
      NodePath.join(directory, "client", `asset-${index}.bin`),
      Buffer.alloc(chunk, index + 1),
    );
}

/** A qualified home on `BASELINE` (installed from `INSTALLED`) with untouched synthetic state. */
export async function makePreparationHome(): Promise<PreparationHome> {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-preparation-test-")),
  );
  const base = NodePath.join(root, "home");
  const active = NodePath.join(base, "runtime", "versions", BASELINE);
  await writePayload(active);
  await NodeFSP.writeFile(
    NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
    JSON.stringify({
      ...receipt(BASELINE, INSTALLED),
      payloadSha256: await qualifiedPayloadDigest(active),
    }),
    { mode: 0o600 },
  );
  const userdata = NodePath.join(base, "userdata");
  await NodeFSP.mkdir(userdata);
  await NodeFSP.writeFile(NodePath.join(userdata, "statev2.sqlite"), "untouched state");
  await NodeFSP.writeFile(NodePath.join(userdata, "environment-id"), "fixture-environment");
  const tools = NodePath.join(root, "tools");
  await NodeFSP.mkdir(tools);
  return {
    root,
    base,
    dbPath: NodePath.join(userdata, "statev2.sqlite"),
    cacheRoot: NodePath.join(base, "runtime", "jones-actions"),
    tools,
    remove: () => NodeFSP.rm(root, { recursive: true, force: true }),
  };
}

/** Stages a candidate runtime in-process, as a finished stage job would have left it. */
export async function stageFixtureRuntime(
  home: PreparationHome,
  options: { readonly bytes?: number } = {},
): Promise<StagedQualifiedRuntime> {
  const payload = NodePath.join(home.root, "candidate");
  await writePayload(payload, options);
  try {
    const { protocol: _protocol, ...metadata } = receipt(VERSION, SOURCE);
    return await stageQualifiedRuntime({
      host: fixtureHost,
      artifact: { ...metadata, payloadDirectory: payload },
      binding: await currentQualifiedRuntimeBinding(home.base, BASELINE, fixtureHost),
      validate: async () => {},
    });
  } finally {
    await NodeFSP.rm(payload, { recursive: true, force: true });
  }
}

/** A candidate `t3` that passes its service preflight and, unless told otherwise, its worker probe. */
export function candidateEntry(options: { readonly worker?: boolean } = {}): string {
  const ready = JSON.stringify({
    status: "ready",
    version: VERSION,
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    startupGateProtocol: 1,
  });
  const worker =
    options.worker === false
      ? "exit 64"
      : `[ "$2" = "--protocol" ] && printf '%s\\n' '{"protocol":1}'`;
  return `#!/bin/sh\ncase "$1" in\n  __service-preflight) printf '%s\\n' '${ready}' ;;\n  __jones-preparation-worker) ${worker} ;;\n  *) exit 64 ;;\nesac\n`;
}

function tar(entries: ReadonlyArray<{ readonly name: string; readonly data?: string }>): Buffer {
  const buffers: Buffer[] = [];
  for (const entry of entries) {
    const bytes = Buffer.from(entry.data ?? "");
    const header = Buffer.alloc(512);
    header.write(entry.name, 0, 100);
    header.write("0000700\0", 100);
    header.write(`${bytes.length.toString(8).padStart(11, "0")}\0`, 124);
    header.fill(32, 148, 156);
    header.write(entry.data === undefined ? "5" : "0", 156);
    header.write("ustar\0", 257);
    const sum = header.reduce((total, byte) => total + byte, 0);
    header.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148);
    buffers.push(header, bytes, Buffer.alloc((512 - (bytes.length % 512)) % 512));
  }
  return NodeZlib.gzipSync(Buffer.concat([...buffers, Buffer.alloc(1024)]));
}

/** Stored-entry ZIP with real CRC-32s, the envelope GitHub serves for an Actions artifact. */
function zip(entries: ReadonlyArray<{ readonly name: string; readonly content: Buffer }>): Buffer {
  const locals: Buffer[] = [];
  const centrals: Buffer[] = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name);
    const crc = NodeZlib.crc32(entry.content);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(entry.content.length, 18);
    local.writeUInt32LE(entry.content.length, 22);
    local.writeUInt16LE(name.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(0x0314, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(entry.content.length, 20);
    central.writeUInt32LE(entry.content.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE((0o100600 << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, name, entry.content);
    centrals.push(central, name);
    offset += local.length + name.length + entry.content.length;
  }
  const central = Buffer.concat(centrals);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}

const FAKE_GH = `import * as fs from "node:fs";
const fixture = JSON.parse(fs.readFileSync(process.env.FAKE_GH_FIXTURE, "utf8"));
const endpoint = process.argv.at(-1);
if (fixture.hang) {
  fs.appendFileSync(fixture.pids, \`\${process.pid} \${process.ppid}\\n\`);
  setInterval(() => {}, 1000);
} else if (endpoint in fixture.files) process.stdout.write(fs.readFileSync(fixture.files[endpoint]));
else if (endpoint in fixture.api) process.stdout.write(JSON.stringify(fixture.api[endpoint]));
else {
  process.stderr.write(\`unexpected endpoint \${endpoint}\\n\`);
  process.exitCode = 1;
}
`;

export interface ActionsFixture {
  readonly candidate: JonesActionsCandidate;
  /** Environment that puts the stand-in `gh` first on `PATH`. */
  readonly env: NodeJS.ProcessEnv;
  /** Where a hanging `gh` records `<gh pid> <worker pid>`. */
  readonly pids: string;
}

/**
 * A canonical main-branch Linux CLI artifact for `candidateEntry`, served by a stand-in `gh` that
 * answers only the fixed Actions endpoints. `hang` makes every `gh` call wait until it is killed.
 */
export async function actionsFixture(
  home: PreparationHome,
  options: { readonly worker?: boolean; readonly hang?: boolean } = {},
): Promise<ActionsFixture> {
  const repo = "repos/Jones-Systems/Jones-Code";
  const workflow = ".github/workflows/artifact-cli-linux.yml";
  const artifact = `t3-${VERSION}-linux-${fixtureHost.architecture}.tar.gz`;
  const payload = tar([
    { name: "runtime/" },
    { name: "runtime/t3", data: candidateEntry(options) },
    { name: "runtime/client/index.html", data: "candidate" },
  ]);
  const artifactReceipt = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source: SOURCE,
    tree: TREE,
    workflow,
    event: "push",
    ref: "refs/heads/main",
    runId: "101",
    runAttempt: "2",
    version: VERSION,
    platform: "linux",
    architecture: fixtureHost.architecture,
    artifact,
    sha256: sha256(payload),
  };
  const envelope = zip([
    { name: "ARTIFACT.json", content: Buffer.from(JSON.stringify(artifactReceipt)) },
    { name: "SOURCE_COMMIT", content: Buffer.from(SOURCE) },
    { name: "SHA256SUMS", content: Buffer.from(`${sha256(payload)}  ${artifact}\n`) },
    { name: artifact, content: payload },
  ]);
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  const candidate: JonesActionsCandidate = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source: SOURCE,
    tree: TREE,
    installedSource: INSTALLED,
    workflow,
    workflowId: 5,
    runId: 101,
    runAttempt: 2,
    ciRunId: 99,
    artifactId: 301,
    artifactName: `jones-code-cli-linux-${fixtureHost.architecture}-101-2--${VERSION}`,
    artifactDigest: sha256(envelope),
    artifactBytes: envelope.length,
    expiresAt,
    version: VERSION,
    platform: "linux",
    architecture: fixtureHost.architecture,
  };
  const run = {
    path: workflow,
    id: 101,
    workflow_id: 5,
    run_attempt: 2,
    head_sha: SOURCE,
    head_branch: "main",
    event: "push",
    status: "completed",
    conclusion: "success",
    repository: { id: 77, full_name: "Jones-Systems/Jones-Code" },
    head_repository: { id: 77, full_name: "Jones-Systems/Jones-Code" },
  };
  const zipPath = NodePath.join(home.tools, "artifact.zip");
  await NodeFSP.writeFile(zipPath, envelope);
  const pids = NodePath.join(home.tools, "gh-pids");
  const fixture = NodePath.join(home.tools, "gh-fixture.json");
  await NodeFSP.writeFile(
    fixture,
    JSON.stringify({
      hang: options.hang === true,
      pids,
      files: { [`${repo}/actions/artifacts/301/zip`]: zipPath },
      api: {
        [`${repo}/actions/runs/101`]: run,
        [`${repo}/actions/runs/99`]: { ...run, id: 99, path: ".github/workflows/ci.yml" },
        [`${repo}/git/commits/${SOURCE}`]: { sha: SOURCE, tree: { sha: TREE } },
        [`${repo}/commits/main`]: { sha: SOURCE },
        [`${repo}/compare/${INSTALLED}...${SOURCE}?per_page=1`]: {
          status: "ahead",
          merge_base_commit: { sha: INSTALLED },
        },
        [`${repo}/actions/artifacts/301`]: {
          id: 301,
          name: candidate.artifactName,
          size_in_bytes: envelope.length,
          expired: false,
          expires_at: expiresAt,
          digest: `sha256:${candidate.artifactDigest}`,
          workflow_run: {
            id: 101,
            repository_id: 77,
            head_repository_id: 77,
            head_branch: "main",
            head_sha: SOURCE,
          },
        },
      },
    }),
  );
  const bin = NodePath.join(home.tools, "bin");
  await NodeFSP.mkdir(bin, { recursive: true });
  const script = NodePath.join(home.tools, "fake-gh.mjs");
  await NodeFSP.writeFile(script, FAKE_GH);
  await NodeFSP.writeFile(
    NodePath.join(bin, "gh"),
    `#!/bin/sh\nexec "${process.execPath}" "${script}" "$@"\n`,
    { mode: 0o700 },
  );
  return {
    candidate,
    env: {
      ...process.env,
      PATH: `${bin}${NodePath.delimiter}${process.env.PATH ?? ""}`,
      FAKE_GH_FIXTURE: fixture,
    },
    pids,
  };
}

/**
 * A stand-in worker for supervisor failure modes, selected by `FAKE_WORKER_MODE`. Modes that
 * leave a SIGTERM-ignoring grandchild record its PID in `<FAKE_WORKER_RECORD>/grandchild`.
 */
const FAKE_WORKER = `import * as fs from "node:fs";
import * as path from "node:path";
import { spawn } from "node:child_process";
const jobDir = process.argv.at(-1);
const mode = process.env.FAKE_WORKER_MODE;
const request = JSON.parse(fs.readFileSync(path.join(jobDir, "request.json"), "utf8"));
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\\n");
const keep = setInterval(() => {}, 1000);
if (mode === "stall" || mode === "orphan") {
  if (mode === "stall") process.on("SIGTERM", () => {});
  const child = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{});setInterval(()=>{},1000)"], { stdio: "ignore" });
  fs.writeFileSync(path.join(process.env.FAKE_WORKER_RECORD, "grandchild"), String(child.pid));
}
emit({ type: "ready", protocol: 1, pid: mode === "bad-ready" ? process.pid + 1 : process.pid, priority: 19 });
if (mode === "flood") {
  const block = (JSON.stringify({ type: "heartbeat", phase: "waiting" }) + "\\n").repeat(2048);
  const pump = () => { while (process.stdout.write(block)) {} process.stdout.once("drain", pump); };
  pump();
} else if (mode === "long-line") process.stdout.write("x".repeat(16 * 1024));
else if (mode === "crash") process.exit(3);
else if (mode === "heartbeat") setInterval(() => emit({ type: "heartbeat", phase: "waiting" }), 20);
else if (mode === "orphan") {
  const result = { protocol: 1, jobId: request.jobId, ok: true, outcome: { kind: "sweep", removed: 0, retained: 0, failed: 0 } };
  fs.writeFileSync(path.join(jobDir, "result.json"), JSON.stringify(result), { mode: 0o600 });
  clearInterval(keep);
  process.exit(0);
}
`;

export async function fakeWorkerCommand(home: PreparationHome): Promise<PreparationWorkerCommand> {
  const script = NodePath.join(home.tools, "fake-worker.mjs");
  await NodeFSP.writeFile(script, FAKE_WORKER);
  return { file: process.execPath, args: [script] };
}

/** Whether `pid` still exists; signal 0 only probes. */
export function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "EPERM";
  }
}
