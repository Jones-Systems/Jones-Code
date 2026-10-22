// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off globalFetch:off - This standalone gated native harness owns Node children, local HTTP fixtures and bounded timing without loading the server Effect runtime.
/**
 * Source-owned XCUITest fallback. Native qualification requires a separately
 * admitted Release build and the sentinel run; --plan executes no native tools.
 */
import * as NodeAssert from "node:assert/strict";
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  configureTestRun,
  fileDigest,
  nativeEnvironment,
  ownedCommand,
  testArgv,
  treeDigest,
} from "./xcuitest.ts";
import type { ChildReceipt } from "./xcuitest.ts";

const sha256 = (value: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const id = () => NodeCrypto.randomUUID();
const record = (value: unknown): Record<string, unknown> => {
  NodeAssert.ok(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "Expected object",
  );
  return value as Record<string, unknown>;
};
const message = (error: unknown) => (error instanceof Error ? error.message : "Unknown failure");

export function redact(value: string, secrets: readonly string[]): string {
  return secrets
    .filter(Boolean)
    .reduce((text, secret) => text.replaceAll(secret, "[REDACTED]"), value);
}

export interface ThreadState {
  id: string;
  pinnedAt: string | null;
  sequence: number;
}
export interface PairState {
  target: ThreadState;
  control: ThreadState;
}
export function parseThread(value: unknown, expectedId: string): ThreadState {
  const response = record(value);
  const thread = record(record(response.projection).thread);
  NodeAssert.equal(thread.id, expectedId, "thread readback identity mismatch");
  NodeAssert.ok(
    Number.isSafeInteger(response.snapshotSequence) && Number(response.snapshotSequence) >= 0,
    "snapshot sequence unavailable",
  );
  NodeAssert.ok(
    thread.pinnedAt === null ||
      (typeof thread.pinnedAt === "string" && Number.isFinite(Date.parse(thread.pinnedAt))),
    "pinnedAt unavailable",
  );
  return {
    id: expectedId,
    pinnedAt: thread.pinnedAt as string | null,
    sequence: Number(response.snapshotSequence),
  };
}

export function assertTransition(before: PairState, after: PairState, pinned: boolean) {
  NodeAssert.notEqual(before.target.id, before.control.id, "control must be a different thread");
  NodeAssert.equal(after.target.id, before.target.id, "target identity changed");
  NodeAssert.equal(after.control.id, before.control.id, "control identity changed");
  NodeAssert.equal(after.control.pinnedAt, before.control.pinnedAt, "control pin changed");
  NodeAssert.ok(after.target.sequence > before.target.sequence, "target sequence did not advance");
  NodeAssert.equal(
    after.target.pinnedAt !== null,
    pinned,
    pinned ? "target was not pinned" : "target was not unpinned",
  );
}

export function assertPersistence(before: PairState, after: PairState) {
  for (const role of ["target", "control"] as const) {
    NodeAssert.equal(after[role].id, before[role].id, "relaunch changed thread identity");
    NodeAssert.equal(
      after[role].pinnedAt,
      before[role].pinnedAt,
      "relaunch changed server pin state",
    );
    NodeAssert.ok(
      after[role].sequence >= before[role].sequence,
      "relaunch snapshot sequence regressed",
    );
  }
}

export interface WorkflowIO {
  pin(): Promise<void>;
  unpin(): Promise<void>;
  menu(label: "Pin" | "Unpin"): Promise<void>;
  read(): Promise<PairState>;
  relaunch(): Promise<void>;
}

export async function awaitTransition(
  read: () => Promise<PairState>,
  before: PairState,
  pinned: boolean,
  options: { timeoutMs?: number; intervalMs?: number; signal?: AbortSignal } = {},
) {
  const deadline = Date.now() + (options.timeoutMs ?? 15_000);
  let last: unknown;
  do {
    options.signal?.throwIfAborted();
    const after = await read();
    // Control drift is a failure immediately, even while the target converges.
    NodeAssert.equal(after.control.id, before.control.id, "control identity changed");
    NodeAssert.equal(after.control.pinnedAt, before.control.pinnedAt, "control pin changed");
    try {
      assertTransition(before, after, pinned);
      return after;
    } catch (error) {
      last = error;
    }
    if (Date.now() >= deadline) break;
    await NodeTimersPromises.setTimeout(options.intervalMs ?? 100, undefined, {
      signal: options.signal,
    });
  } while (Date.now() <= deadline);
  throw new Error(`Pin oracle did not converge: ${message(last)}`);
}

async function exercise(io: WorkflowIO, noAction = false) {
  const initial = await io.read();
  NodeAssert.equal(initial.target.pinnedAt, null, "target must start unpinned");
  if (!noAction) await io.pin();
  const pinned = await awaitTransition(io.read, initial, true, {
    timeoutMs: noAction ? 0 : 15_000,
  });
  await io.relaunch();
  assertPersistence(pinned, await io.read());
  await io.menu("Unpin");
  await io.unpin();
  const unpinned = await awaitTransition(io.read, pinned, false);
  await io.relaunch();
  assertPersistence(unpinned, await io.read());
  await io.menu("Pin");
  return {
    initial,
    pinned,
    unpinned,
    uiAfterPinnedRelaunch: "Unpin",
    uiAfterUnpinnedRelaunch: "Pin",
  };
}

export async function runInjected(io: WorkflowIO, noAction = false, cleanup = new OwnedCleanup()) {
  const outcome = await exercise(io, noAction).then(
    (oracle) => ({ ok: true as const, oracle }),
    (error: unknown) => ({ ok: false as const, error }),
  );
  const result = await cleanup.finish(outcome.ok ? undefined : message(outcome.error));
  if (!outcome.ok) {
    if (outcome.error instanceof Error) Object.assign(outcome.error, { cleanup: result });
    throw outcome.error;
  }
  if (result.retainScratch) throw new Error("Injected cleanup unresolved");
  return {
    schema: "jones-ios-workflow-receipt/2",
    runner: "injected",
    tier: "B-rehearsal",
    nativeClaim: false,
    oracle: outcome.oracle,
  } as const;
}

export class OwnedCleanup {
  private tasks: { label: string; destructive: boolean; run: () => Promise<void> }[] = [];
  add(label: string, run: () => Promise<void>, destructive = false) {
    this.tasks.push({ label, run, destructive });
  }
  async finish(primary?: string) {
    const cleanup: { label: string; status: "passed" | "failed" | "held"; error?: string }[] = [];
    let unknown = false;
    for (const task of this.tasks.toReversed()) {
      if (unknown && task.destructive) {
        cleanup.push({ label: task.label, status: "held" });
        continue;
      }
      try {
        await task.run();
        cleanup.push({ label: task.label, status: "passed" });
      } catch (error) {
        unknown = true;
        cleanup.push({ label: task.label, status: "failed", error: message(error) });
      }
    }
    this.tasks = [];
    return {
      cleanup,
      outcome: unknown ? "unknown" : primary ? "fail" : "pass",
      primary: primary ?? null,
      retainScratch: unknown,
    };
  }
}

export function acceptCreatedDevice(
  udid: string,
  previous: readonly { udid: string; name: string }[],
  name: string,
) {
  NodeAssert.match(
    udid,
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    "create returned no owned UDID; prior effect unknown",
  );
  NodeAssert.ok(
    !previous.some((device) => device.udid === udid || device.name === name),
    "device already existed; ownership not established",
  );
  return udid;
}

interface Options {
  source: string;
  sourceSha: string;
  buildRecord: string;
  developerDir: string;
  runtime: string;
  deviceType: string;
  scratchParent: string;
  receipt: string;
  admission: string;
  controlPinned: boolean;
  noAction: boolean;
  signal?: AbortSignal;
  children: ChildReceipt[];
}
async function command(
  executable: string,
  args: readonly string[],
  cwd: string,
  env: NodeJS.ProcessEnv = { PATH: process.env.PATH ?? "/usr/bin:/bin" },
  input?: string,
  options?: Pick<Options, "signal" | "children">,
) {
  return ownedCommand(executable, args, {
    cwd,
    env,
    ...(input === undefined ? {} : { input }),
    ...(options?.signal === undefined ? {} : { signal: options.signal }),
    ...(options?.children === undefined ? {} : { receipts: options.children }),
  }).result;
}
async function checked(
  executable: string,
  args: readonly string[],
  cwd: string,
  env?: NodeJS.ProcessEnv,
  input?: string,
  options?: Pick<Options, "signal" | "children">,
) {
  const result = await command(executable, args, cwd, env, input, options);
  NodeAssert.equal(
    result.code,
    0,
    `${NodePath.basename(executable)} ${args[0]} failed (${result.code})`,
  );
  return result.stdout.trim();
}
async function pathFact(path: string) {
  try {
    await NodeFSP.lstat(path);
    return { kind: "present" };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { kind: "absent" };
    throw error;
  }
}

export function assertAdmission(value: unknown, sourceSha: string, now = Date.now()) {
  const admission = record(value);
  NodeAssert.equal(admission.sourceSha, sourceSha, "admission source mismatch");
  NodeAssert.equal(admission.hostname, NodeOS.hostname(), "admission host mismatch");
  NodeAssert.ok(
    typeof admission.serialSlot === "string" && admission.serialSlot.length > 0,
    "root serial slot required",
  );
  NodeAssert.ok(
    typeof admission.measuredAt === "number" &&
      now >= admission.measuredAt &&
      now - admission.measuredAt < 60_000,
    "fresh root admission required",
  );
  NodeAssert.ok(
    typeof admission.cpuUsed === "number" && admission.cpuUsed >= 0 && admission.cpuUsed < 85,
    "CPU admission held",
  );
  NodeAssert.ok(
    typeof admission.availableMemoryBytes === "number" && admission.availableMemoryBytes > 0,
    "memory evidence required",
  );
  NodeAssert.equal(admission.memoryPressure, "normal", "memory pressure held");
  NodeAssert.ok(
    typeof admission.freeDiskBytes === "number" && admission.freeDiskBytes >= 30 * 1024 ** 3,
    "30 GiB free disk required",
  );
  return admission;
}

async function preflight(options: Options) {
  NodeAssert.equal(HostProcess.Platform.defaultValue(), "darwin", "macOS required");
  NodeAssert.match(options.sourceSha, /^[0-9a-f]{40}$/);
  const head = await checked("git", ["rev-parse", "HEAD"], options.source);
  NodeAssert.equal(head, options.sourceSha, "source changed");
  NodeAssert.equal(
    await checked("git", ["status", "--porcelain"], options.source),
    "",
    "source must be clean",
  );
  const branch = await checked("git", ["branch", "--show-current"], options.source);
  NodeAssert.ok(branch && !["main", "master", "HEAD"].includes(branch), "task branch required");
  NodeAssert.equal(
    (await command("git", ["check-ignore", "apps/mobile/ios/"], options.source)).code,
    0,
    "generated ios must be ignored",
  );
  await NodeFSP.access(NodePath.join(options.source, "apps/server/dist/bin.mjs"));
  const admission = assertAdmission(
    JSON.parse(await NodeFSP.readFile(options.admission, "utf8")),
    head,
  );
  const stat = await NodeFSP.statfs(options.scratchParent);
  NodeAssert.ok(stat.bavail * stat.bsize >= 30 * 1024 ** 3, "fresh disk gate failed");
  const build = record(JSON.parse(await NodeFSP.readFile(options.buildRecord, "utf8")));
  NodeAssert.equal(build.sourceSha, head, "build source mismatch");
  NodeAssert.equal(build.developerDir, options.developerDir, "Xcode identity mismatch");
  NodeAssert.ok(
    Array.isArray(build.buildArgv) && build.buildArgv.every((arg) => typeof arg === "string"),
  );
  const argv = build.buildArgv as string[];
  for (const flag of [
    "build-for-testing",
    "Release",
    "CODE_SIGNING_ALLOWED=NO",
    "-derivedDataPath",
  ])
    NodeAssert.ok(argv.includes(flag), `build missing ${flag}`);
  NodeAssert.ok(
    !argv.includes("-allowProvisioningUpdates") &&
      !argv.includes("-allowProvisioningDeviceRegistration"),
  );
  NodeAssert.ok(
    Array.isArray(build.children) && build.children.length > 0,
    "captured build child receipts required",
  );
  const buildChild = build.children
    .map(record)
    .find(
      (child) =>
        child.executable === NodePath.join(options.developerDir, "usr/bin/xcodebuild") &&
        JSON.stringify(child.argv) === JSON.stringify(argv),
    );
  NodeAssert.ok(
    buildChild &&
      Number.isSafeInteger(buildChild.pid) &&
      Number(buildChild.pid) > 0 &&
      buildChild.pgid === buildChild.pid &&
      buildChild.code === 0 &&
      buildChild.groupGone === true,
    "successful owned build receipt required",
  );
  NodeAssert.ok(
    typeof build.ownedRoot === "string" && NodePath.isAbsolute(build.ownedRoot),
    "owned build root required",
  );
  const buildRoot = await NodeFSP.realpath(build.ownedRoot);
  const artifacts = record(build.artifacts);
  for (const key of ["app", "testHost", "testBundle", "xctestrun"]) {
    const artifact = record(artifacts[key]);
    NodeAssert.ok(typeof artifact.path === "string" && NodePath.isAbsolute(artifact.path));
    NodeAssert.ok(
      (await NodeFSP.realpath(artifact.path)).startsWith(`${buildRoot}${NodePath.sep}`),
      "artifact outside owned build root",
    );
    NodeAssert.equal(
      key === "xctestrun" ? await fileDigest(artifact.path) : await treeDigest(artifact.path),
      artifact.sha256,
      `${key} bytes changed`,
    );
  }
  const app = String(record(artifacts.app).path);
  const plist = async (name: string) =>
    record(
      JSON.parse(
        await checked(
          "/usr/bin/plutil",
          ["-convert", "json", "-o", "-", NodePath.join(app, name)],
          options.source,
        ),
      ),
    );
  const info = await plist("Info.plist"),
    expo = await plist("Expo.plist");
  NodeAssert.equal(expo.EXUpdatesEnabled, false);
  NodeAssert.ok(
    Array.isArray(info.CFBundleSupportedPlatforms) &&
      info.CFBundleSupportedPlatforms.includes("iPhoneSimulator"),
  );
  NodeAssert.equal(typeof info.CFBundleIdentifier, "string");
  NodeAssert.equal(await fileDigest(NodePath.join(app, "main.jsbundle")), build.bundleSha256);
  NodeAssert.equal(
    await checked("git", ["rev-parse", "HEAD"], options.source),
    head,
    "source moved during preflight",
  );
  return { admission, build, bundleId: String(info.CFBundleIdentifier), artifacts };
}

function fixtureEnvironment(root: string): NodeJS.ProcessEnv {
  return {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    HOME: NodePath.join(root, "home"),
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CONFIG_HOME: NodePath.join(root, "home/config"),
    XDG_CACHE_HOME: NodePath.join(root, "home/cache"),
    XDG_DATA_HOME: NodePath.join(root, "home/data"),
    T3CODE_DISABLE_TELEMETRY: "1",
    CI: "true",
    NO_COLOR: "1",
  };
}
async function freePort() {
  const server = NodeNet.createServer();
  server.listen(0, "127.0.0.1");
  await NodeEvents.EventEmitter.once(server, "listening");
  const address = server.address();
  NodeAssert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}
async function portClosed(port: number) {
  await new Promise<void>((resolve, reject) => {
    const socket = NodeNet.connect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      reject(new Error("owned server port still open"));
    });
    socket.once("error", (error: NodeJS.ErrnoException) =>
      error.code === "ECONNREFUSED" ? resolve() : reject(error),
    );
    socket.setTimeout(2_000, () => {
      socket.destroy();
      reject(new Error("port state unknown"));
    });
  });
}
async function rpcDispatch(
  origin: string,
  api: (path: string, payload?: unknown) => Promise<unknown>,
  payload: unknown,
) {
  const descriptor = record(await api("/.well-known/t3/environment"));
  const ticket = record(await api("/api/auth/websocket-ticket", {}));
  const socket = new WebSocket(
    `${origin.replace("http:", "ws:")}/ws?orchestrationProtocol=${descriptor.orchestrationProtocolVersion}&wsTicket=${encodeURIComponent(String(ticket.ticket))}`,
  );
  const requestId = id();
  try {
    await NodeEvents.EventEmitter.once(socket, "open", { signal: AbortSignal.timeout(10_000) });
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("thread command timed out; effect unknown")),
        15_000,
      );
      const finish = (error?: Error) => {
        clearTimeout(timer);
        if (error) reject(error);
        else resolve();
      };
      socket.addEventListener("error", () => finish(new Error("RPC transport failed")), {
        once: true,
      });
      socket.addEventListener(
        "close",
        () => finish(new Error("RPC closed before command receipt")),
        { once: true },
      );
      socket.addEventListener("message", ({ data }) => {
        try {
          const decoded: unknown = JSON.parse(String(data));
          for (const raw of Array.isArray(decoded) ? decoded : [decoded]) {
            const frame = record(raw);
            if (frame.requestId !== requestId) continue;
            if (frame._tag === "Chunk") socket.send(JSON.stringify({ _tag: "Ack", requestId }));
            if (frame._tag === "Exit")
              finish(
                record(frame.exit)._tag === "Success"
                  ? undefined
                  : new Error("thread command rejected"),
              );
          }
        } catch {
          finish(new Error("Invalid RPC response"));
        }
      });
      socket.send(
        JSON.stringify({
          _tag: "Request",
          id: requestId,
          tag: "orchestration.dispatchCommand",
          payload,
          headers: [],
        }),
      );
    });
  } finally {
    socket.close();
  }
}

async function startFixture(
  options: Options,
  root: string,
  cleanup: OwnedCleanup,
  secrets: string[],
) {
  const env = fixtureEnvironment(root);
  await NodeFSP.mkdir(env.HOME!, { recursive: true });
  await NodeFSP.mkdir(env.TMPDIR!, { recursive: true });
  const baseDir = NodePath.join(root, "server");
  const binary = NodePath.join(options.source, "apps/server/dist/bin.mjs");
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  cleanup.add("server-port-closed", () => portClosed(port));
  const server = ownedCommand(
    process.execPath,
    [binary, "serve", "--base-dir", baseDir, "--host", "127.0.0.1", "--port", String(port)],
    {
      cwd: root,
      env,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
      timeoutMs: 600_000,
      receipts: options.children,
    },
  );
  const child = server.child;
  void server.result.catch(() => {});
  cleanup.add("server-child-exited", async () => {
    await server.stop();
    await server.result.catch((error) => {
      if (!server.receipt.groupGone) throw error;
    });
  });
  await NodeEvents.EventEmitter.once(child, "spawn");
  const until = Date.now() + 30_000;
  let environmentId = "";
  while (Date.now() < until) {
    options.signal?.throwIfAborted();
    NodeAssert.equal(child.exitCode, null, "fixture server exited before readiness");
    try {
      const response = await fetch(`${origin}/.well-known/t3/environment`, {
        signal: AbortSignal.timeout(1_000),
      });
      const descriptor = record(await response.json());
      if (response.ok && typeof descriptor.environmentId === "string") {
        const source = record(descriptor.jonesSource);
        NodeAssert.equal(
          source.repository,
          "Jones-Systems/Jones-Code",
          "server repository mismatch",
        );
        NodeAssert.equal(source.sha, options.sourceSha, "server source mismatch");
        NodeAssert.equal(
          (
            await NodeFSP.readFile(NodePath.join(baseDir, "userdata/environment-id"), "utf8")
          ).trim(),
          descriptor.environmentId,
          "fixture endpoint identity mismatch",
        );
        environmentId = descriptor.environmentId;
        break;
      }
    } catch {
      /* Readiness is bounded; it never repeats a mutating request. */
    }
    await NodeTimersPromises.setTimeout(100);
  }
  NodeAssert.ok(environmentId, "fixture environment not ready");
  const pair = async () => {
    const output = await checked(
      process.execPath,
      [binary, "pair", "--base-dir", baseDir],
      root,
      env,
      undefined,
      options,
    );
    const matches = [...output.matchAll(/^Token: ([^\s]+)\r?$/gm)];
    NodeAssert.equal(matches.length, 1, "pairing output unavailable");
    const token = matches[0]![1]!;
    secrets.push(token);
    return token;
  };
  const response = await fetch(`${origin}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential: await pair() }),
    signal: AbortSignal.timeout(10_000),
  });
  NodeAssert.equal(response.status, 200, "fixture browser pairing failed");
  const cookie = response.headers
    .getSetCookie()
    .map((value) => value.split(";")[0])
    .join("; ");
  NodeAssert.ok(cookie, "fixture browser cookie missing");
  secrets.push(cookie);
  const api = async (path: string, payload?: unknown): Promise<unknown> => {
    const result = await fetch(`${origin}${path}`, {
      method: payload === undefined ? "GET" : "POST",
      headers: { cookie, "content-type": "application/json", "x-t3-orchestration-protocol": "2" },
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
      signal: AbortSignal.timeout(10_000),
    });
    NodeAssert.ok(result.ok, `fixture API failed (${result.status})`);
    return result.json();
  };
  const projectId = id(),
    targetId = id(),
    controlId = id();
  const title = `Pin target ${targetId}`;
  const workspace = NodePath.join(root, "synthetic-project");
  await NodeFSP.mkdir(workspace);
  await api("/api/projects/mutate", {
    type: "project.create",
    commandId: id(),
    projectId,
    title: "iOS pin workflow",
    workspaceRoot: workspace,
  });
  for (const [threadId, threadTitle] of [
    [targetId, title],
    [controlId, `Control ${controlId}`],
  ]) {
    await rpcDispatch(origin, api, {
      type: "thread.create",
      commandId: id(),
      threadId,
      projectId,
      createdBy: "user",
      creationSource: "web",
      title: threadTitle,
      modelSelection: { instanceId: "codex", model: "gpt-5" },
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
  }
  if (options.controlPinned)
    await rpcDispatch(origin, api, { type: "thread.pin", commandId: id(), threadId: controlId });
  return {
    environmentId,
    port,
    origin,
    title,
    pairingToken: await pair(),
    read: async (): Promise<PairState> => ({
      target: parseThread(await api(`/api/orchestration/threads/${targetId}`), targetId),
      control: parseThread(await api(`/api/orchestration/threads/${controlId}`), controlId),
    }),
  };
}

export const PHASES = [
  "initial",
  "pinned",
  "pinnedRelaunch",
  "unpinned",
  "unpinnedRelaunch",
  "complete",
] as const;
export class PinPhases {
  readonly observations: { phase: string; state: PairState }[] = [];
  private next = 0;
  private failed: unknown;
  private readonly read: () => Promise<PairState>;
  private readonly signal: AbortSignal | undefined;
  constructor(read: () => Promise<PairState>, signal?: AbortSignal) {
    this.read = read;
    this.signal = signal;
  }
  async accept(phase: string) {
    if (this.failed) throw this.failed;
    try {
      NodeAssert.equal(phase, PHASES[this.next], "unexpected or duplicate UI phase");
      const before = this.observations.at(-1)?.state;
      let state: PairState;
      if (phase === "pinned" || phase === "unpinned") {
        NodeAssert.ok(before);
        state = await awaitTransition(
          this.read,
          before,
          phase === "pinned",
          this.signal === undefined ? {} : { signal: this.signal },
        );
      } else {
        state = await this.read();
        if (phase === "initial")
          NodeAssert.equal(state.target.pinnedAt, null, "target must start unpinned");
        else {
          NodeAssert.ok(before);
          assertPersistence(before, state);
        }
      }
      this.observations.push({ phase, state });
      this.next++;
    } catch (error) {
      this.failed = error;
      throw error;
    }
  }
  assertComplete() {
    if (this.failed) throw this.failed;
    NodeAssert.equal(this.next, PHASES.length, "native UI journey incomplete");
  }
}

async function nativeRun(options: Options, facts: Awaited<ReturnType<typeof preflight>>) {
  await NodeFSP.writeFile(
    options.receipt,
    JSON.stringify({ outcome: "unknown", nativeClaim: false, stage: "allocating" }),
    { flag: "wx", mode: 0o600 },
  );
  const root = await NodeFSP.mkdtemp(
    NodePath.join(await NodeFSP.realpath(options.scratchParent), "jones-ios-pin-"),
  );
  const cleanup = new OwnedCleanup(),
    secrets: string[] = [];
  const receipt: Record<string, unknown> = {
    schema: "jones-ios-workflow-receipt/2",
    runner: "xcuitest",
    tier: "N-candidate",
    nativeClaim: false,
    sentinelQualification: "not-established-by-this-run",
    sourceSha: options.sourceSha,
    build: facts.build,
    admission: facts.admission,
    scratch: root,
    children: options.children,
    negativeControl: options.noAction,
  };
  cleanup.add(
    "scratch-removed",
    async () => {
      await NodeFSP.rm(root, { recursive: true });
      NodeAssert.equal((await pathFact(root)).kind, "absent");
    },
    true,
  );
  cleanup.add("owned-children-gone", async () => {
    NodeAssert.ok(
      options.children.every((child) => child.groupGone === true),
      "captured child/group cleanup unresolved",
    );
  });
  let primary: string | undefined;
  const env = nativeEnvironment(root, options.developerDir, process.env.PATH ?? "/usr/bin:/bin");
  const run = (exe: string, args: readonly string[], teardown = false) =>
    checked(exe, args, root, env, undefined, {
      ...(teardown || options.signal === undefined ? {} : { signal: options.signal }),
      children: options.children,
    });
  const devices = async (teardown = false) => {
    const response = record(
      JSON.parse(await run("/usr/bin/xcrun", ["simctl", "list", "devices", "--json"], teardown)),
    );
    return Object.values(record(response.devices)).flatMap((items) => {
      NodeAssert.ok(Array.isArray(items));
      return items.map((item) => {
        const device = record(item);
        return {
          udid: String(device.udid),
          name: String(device.name),
          state: String(device.state),
        };
      });
    });
  };
  try {
    for (const name of ["home", "tmp", "cache"]) await NodeFSP.mkdir(NodePath.join(root, name));
    NodeAssert.equal(
      await run(NodePath.join(options.developerDir, "usr/bin/xcodebuild"), ["-version"]),
      facts.build.xcodeVersion,
      "Xcode version changed",
    );
    await run(NodePath.join(options.developerDir, "usr/bin/xcodebuild"), [
      "-checkFirstLaunchStatus",
    ]);
    options.signal?.throwIfAborted();
    const previous = await devices(),
      name = `jones-pin-${id()}`;
    NodeAssert.ok(!previous.some((device) => device.name === name));
    let udid: string | undefined, creationError: unknown;
    try {
      udid = acceptCreatedDevice(
        await run("/usr/bin/xcrun", [
          "simctl",
          "create",
          name,
          options.deviceType,
          options.runtime,
        ]),
        previous,
        name,
      );
    } catch (error) {
      creationError = error;
      // Reconcile the exact unique create request before cleanup; never retry create.
      try {
        const created = (await devices(true)).filter(
          (device) => device.name === name && !previous.some((old) => old.udid === device.udid),
        );
        if (created.length === 1) udid = acceptCreatedDevice(created[0]!.udid, previous, name);
        else NodeAssert.equal(created.length, 0, "multiple devices for unique create request");
      } catch (reconciliationError) {
        cleanup.add("device-creation-unknown", async () => {
          throw reconciliationError;
        });
      }
    }
    if (udid) {
      const ownedUdid = udid;
      receipt.device = { udid: ownedUdid, createdByRun: true };
      cleanup.add(
        "owned-device-deleted",
        async () => {
          const current = (await devices(true)).find((device) => device.udid === ownedUdid);
          if (current) {
            if (current.state !== "Shutdown")
              await run("/usr/bin/xcrun", ["simctl", "shutdown", ownedUdid], true);
            await run("/usr/bin/xcrun", ["simctl", "delete", ownedUdid], true);
          }
          NodeAssert.ok(
            !(await devices(true)).some((device) => device.udid === ownedUdid),
            "owned device remains",
          );
        },
        true,
      );
    }
    if (creationError) throw creationError;
    NodeAssert.ok(udid, "created device unavailable");
    await run("/usr/bin/xcrun", ["simctl", "boot", udid]);
    await run("/usr/bin/xcrun", ["simctl", "bootstatus", udid, "-b"]);
    const fixture = await startFixture(options, NodePath.join(root, "fixture"), cleanup, secrets);
    receipt.server = { environmentId: fixture.environmentId, port: fixture.port };
    const phases = new PinPhases(fixture.read, options.signal);
    receipt.observations = phases.observations;
    const captures: { phase: string; path: string; sha256: string }[] = [];
    receipt.captures = captures;
    const bridgeToken = NodeCrypto.randomBytes(32).toString("hex");
    secrets.push(bridgeToken);
    let requestActive = false;
    const bridge = NodeHttp.createServer((request, response) => {
      void (async () => {
        response.setHeader("content-type", "application/json");
        if (request.headers.authorization !== `Bearer ${bridgeToken}`) {
          response.writeHead(403).end("{}");
          return;
        }
        if (request.method === "GET" && request.url === "/configuration") {
          response.end(
            JSON.stringify({
              origin: fixture.origin,
              pairingToken: fixture.pairingToken,
              title: fixture.title,
              bundleId: facts.bundleId,
              noAction: options.noAction,
            }),
          );
          return;
        }
        const capturePhase = request.url?.startsWith("/capture/")
          ? request.url.slice("/capture/".length)
          : undefined;
        if (request.method === "POST" && capturePhase) {
          NodeAssert.ok(
            ["pinnedRelaunch", "unpinnedRelaunch"].includes(capturePhase),
            "invalid capture phase",
          );
          NodeAssert.equal(
            phases.observations.at(-1)?.phase,
            capturePhase,
            "capture before verified relaunch",
          );
          NodeAssert.ok(!captures.some((item) => item.phase === capturePhase), "duplicate capture");
          const chunks: Buffer[] = [];
          let size = 0;
          for await (const chunk of request) {
            const bytes = Buffer.from(chunk);
            size += bytes.length;
            NodeAssert.ok(size <= 10 * 1024 * 1024, "capture too large");
            chunks.push(bytes);
          }
          const png = Buffer.concat(chunks);
          NodeAssert.equal(
            png.subarray(0, 8).toString("hex"),
            "89504e470d0a1a0a",
            "capture is not PNG",
          );
          const path = `${options.receipt}.${capturePhase}.png`;
          await NodeFSP.writeFile(path, png, { flag: "wx", mode: 0o600 });
          captures.push({ phase: capturePhase, path, sha256: sha256(png) });
          response.end("{}");
          return;
        }
        NodeAssert.ok(
          request.method === "POST" && request.url === "/phase",
          "unknown bridge request",
        );
        NodeAssert.ok(!requestActive, "concurrent phase rejected");
        requestActive = true;
        try {
          let body = "";
          for await (const chunk of request) {
            body += String(chunk);
            NodeAssert.ok(body.length <= 1024, "phase body too large");
          }
          const phase = record(JSON.parse(body));
          await phases.accept(String(phase.phase));
          response.end("{}");
        } finally {
          requestActive = false;
        }
      })().catch((error) => {
        response.writeHead(409).end(JSON.stringify({ error: redact(message(error), secrets) }));
      });
    });
    bridge.requestTimeout = 20_000;
    bridge.headersTimeout = 5_000;
    cleanup.add("phase-bridge-closed", async () => {
      bridge.closeAllConnections();
      if (bridge.listening)
        await new Promise<void>((resolve, reject) =>
          bridge.close((error) => (error ? reject(error) : resolve())),
        );
    });
    bridge.listen(0, "127.0.0.1");
    await NodeEvents.EventEmitter.once(bridge, "listening");
    const address = bridge.address();
    NodeAssert.ok(address && typeof address !== "string");
    const originalRun = String(record(facts.artifacts.xctestrun).path);
    const originalRoot = NodePath.dirname(originalRun);
    const decoded = JSON.parse(
      await run("/usr/bin/plutil", ["-convert", "json", "-o", "-", originalRun]),
    );
    const configured = configureTestRun(decoded, originalRoot, {
      JONES_E2E_URL: `http://127.0.0.1:${address.port}`,
      JONES_E2E_TOKEN: bridgeToken,
    });
    // Confirm that the run's referenced products are exactly the hashed build products.
    const configs = configured.TestConfigurations;
    const target = Array.isArray(configs)
      ? record((record(configs[0]).TestTargets as unknown[])[0])
      : record(configured.JonesE2eUITests);
    for (const [key, artifact] of [
      ["UITargetAppPath", "app"],
      ["TestHostPath", "testHost"],
      ["TestBundlePath", "testBundle"],
    ]) {
      NodeAssert.equal(
        await NodeFSP.realpath(String(target[key!])),
        await NodeFSP.realpath(String(record(facts.artifacts[artifact!]).path)),
        `${key} not bound to recorded build`,
      );
    }
    const configuredPath = NodePath.join(root, "owned.xctestrun");
    await NodeFSP.writeFile(configuredPath, JSON.stringify(configured), {
      mode: 0o600,
      flag: "wx",
    });
    await run("/usr/bin/plutil", ["-convert", "xml1", configuredPath]);
    receipt.configuredTestRunSha256 = await fileDigest(configuredPath);
    const argv = testArgv(configuredPath, udid, root);
    const test = ownedCommand(NodePath.join(options.developerDir, "usr/bin/xcodebuild"), argv, {
      cwd: root,
      env,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
      timeoutMs: 300_000,
      receipts: options.children,
    });
    cleanup.add("xcodebuild-group-gone", async () => {
      await test.stop();
    });
    const result = await test.result;
    receipt.testExitCode = result.code;
    receipt.outputSha256 = sha256(redact(result.stdout + result.stderr, secrets));
    NodeAssert.equal(result.code, 0, "XCUITest failed");
    phases.assertComplete();
    NodeAssert.deepEqual(
      captures.map((item) => item.phase),
      ["pinnedRelaunch", "unpinnedRelaunch"],
      "native captures missing",
    );
    NodeAssert.equal(options.noAction, false, "no-action negative control unexpectedly passed");
    receipt.nativeClaim = true;
  } catch (error) {
    primary = redact(message(error), secrets);
    if (error instanceof AggregateError)
      receipt.primaryErrors = error.errors.map((item) => redact(message(item), secrets));
  }
  const result = await cleanup.finish(primary);
  Object.assign(receipt, result);
  if (result.outcome !== "pass") receipt.nativeClaim = false;
  // Raw XCTest logs/bundles can contain pairing text. Only the redacted semantic
  // receipt survives scratch cleanup, including on a failed pairing attempt.
  await NodeFSP.writeFile(options.receipt, redact(JSON.stringify(receipt, null, 2), secrets), {
    mode: 0o600,
  });
  return receipt;
}

export async function main(argv: string[]) {
  if (argv.length === 1 && argv[0] === "--plan")
    return {
      schema: "jones-ios-workflow-plan/2",
      adapter: "owned-xcodebuild-xcuitest",
      nativeClaim: false,
      qualification: "pending-native-and-sentinel-evidence",
      phases: PHASES,
      required: [
        "--source",
        "--source-sha",
        "--build-record",
        "--developer-dir",
        "--runtime",
        "--device-type",
        "--scratch-parent",
        "--receipt",
        "--admission",
      ],
    };
  NodeAssert.ok(
    ["--preflight", "--execute"].includes(argv[0] ?? ""),
    "Use --plan, --preflight or --execute",
  );
  const allowed = new Set([
    "--source",
    "--source-sha",
    "--build-record",
    "--developer-dir",
    "--runtime",
    "--device-type",
    "--scratch-parent",
    "--receipt",
    "--admission",
    "--control-pinned",
    "--no-action",
  ]);
  const values = new Map<string, string>();
  for (let i = 1; i < argv.length; i += 2) {
    const key = argv[i],
      value = argv[i + 1];
    NodeAssert.ok(
      key && value && allowed.has(key) && !value.startsWith("--") && !values.has(key),
      "Invalid CLI options",
    );
    values.set(key, value);
  }
  const required = (key: string) => {
    const value = values.get(key);
    NodeAssert.ok(value, `Missing ${key}`);
    return value;
  };
  for (const key of ["--control-pinned", "--no-action"])
    NodeAssert.ok(
      !values.has(key) || ["true", "false"].includes(values.get(key)!),
      `${key} expects true or false`,
    );
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("Workflow cancelled"));
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    const options: Options = {
      source: NodePath.resolve(required("--source")),
      sourceSha: required("--source-sha"),
      buildRecord: NodePath.resolve(required("--build-record")),
      developerDir: NodePath.resolve(required("--developer-dir")),
      runtime: required("--runtime"),
      deviceType: required("--device-type"),
      scratchParent: NodePath.resolve(required("--scratch-parent")),
      receipt: NodePath.resolve(required("--receipt")),
      admission: NodePath.resolve(required("--admission")),
      controlPinned: values.get("--control-pinned") === "true",
      noAction: values.get("--no-action") === "true",
      signal: controller.signal,
      children: [],
    };
    const facts = await preflight(options);
    if (argv[0] === "--preflight") return { outcome: "pass", nativeClaim: false, facts };
    // Artifact hashing can take time; root admission must still be fresh at launch.
    assertAdmission(facts.admission, options.sourceSha);
    controller.signal.throwIfAborted();
    return await nativeRun(options, facts);
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  main(process.argv.slice(2))
    .then((result) => {
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
      if ("outcome" in result && result.outcome !== "pass") process.exitCode = 1;
    })
    .catch((error: unknown) => {
      process.stderr.write(`${message(error)}\n`);
      process.exitCode = 1;
    });
}
