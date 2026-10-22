// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off globalFetch:off - This standalone gated native harness owns Node children, local HTTP fixtures and bounded timing without loading the server Effect runtime.
/**
 * Maintainer pilot, not native qualification. Build a Release simulator .app with
 * T3CODE_MOBILE_UPDATES_ENABLED=0, CODE_SIGNING_ALLOWED=NO and an owned
 * -derivedDataPath; generated ios/ must be ignored. Supply --build-record JSON
 * containing sourceSha, appPath, bundleSha256, executableSha256, buildArgv and
 * xcodeVersion. The runner verifies those bytes and the built Expo.plist.
 *
 * agent-device 0.21.25 can kill by pattern during cleanup. --execute deliberately
 * refuses that unqualified driver; no CLI flag bypasses this repository boundary.
 * The native adapter below is authored source and remains unqualified. An injected run proves only B, never native success.
 */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";

export const DRIVER_VERSION = "0.21.25";
const LEGACY_REPAIR_SHA256 = "61750fbc630752c7fe3339adc9d3e4df4701ff0fcfce6939e81036cacda15302";
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

export function driverEnvironment(root: string, inherited: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  // An allowlist also excludes provider credentials and saved remote-driver settings.
  return {
    PATH: inherited.PATH ?? "/usr/bin:/bin",
    HOME: NodeOS.homedir(),
    TMPDIR: NodePath.join(root, "tmp"),
    AGENT_DEVICE_HOME: NodePath.join(root, "driver-home"),
    AGENT_DEVICE_STATE_DIR: NodePath.join(root, "driver-state"),
    AGENT_DEVICE_NO_UPDATE_NOTIFIER: "1",
    AGENT_DEVICE_IOS_RUNNER_DERIVED_PATH: NodePath.join(root, "driver", ".tmp", "runner"),
  };
}

export interface DriverTarget {
  config: string;
  session: string;
  udid: string;
}
export function driverArgv(target: DriverTarget, action: readonly string[]): string[] {
  return [
    ...action,
    "--config",
    target.config,
    "--session",
    target.session,
    "--platform",
    "ios",
    "--udid",
    target.udid,
    "--json",
    "--response-level",
    "full",
  ];
}

export class FreshRefs {
  private epoch = 0;
  private active:
    | { epoch: number; generation: number; nodes: Record<string, unknown>[] }
    | undefined;

  observe(envelope: unknown) {
    this.active = undefined;
    const response = record(envelope);
    NodeAssert.equal(response.success, true, "driver snapshot failed");
    const data = record(response.data);
    NodeAssert.ok(Array.isArray(data.nodes), "snapshot nodes unavailable");
    NodeAssert.ok(
      Number.isSafeInteger(data.refsGeneration) && Number(data.refsGeneration) >= 0,
      "snapshot generation unavailable",
    );
    NodeAssert.notEqual(data.truncated, true, "snapshot truncated");
    this.active = {
      epoch: ++this.epoch,
      generation: Number(data.refsGeneration),
      nodes: data.nodes.map(record),
    };
  }

  select(label: string) {
    NodeAssert.ok(this.active, "fresh snapshot required");
    const matches = this.active.nodes.filter((node) => node.label === label);
    NodeAssert.equal(
      matches.length,
      1,
      `selector ${JSON.stringify(label)} matched ${matches.length} nodes`,
    );
    const ref = matches[0]?.ref;
    NodeAssert.ok(typeof ref === "string" && /^@?e\d+$/.test(ref), "snapshot ref unavailable");
    return {
      epoch: this.active.epoch,
      ref: `${ref.startsWith("@") ? ref : `@${ref}`}~s${this.active.generation}`,
    };
  }

  consume(selection: { epoch: number; ref: string }) {
    NodeAssert.equal(selection.epoch, this.active?.epoch, "stale snapshot ref");
    this.active = undefined;
    return selection.ref;
  }

  invalidate() {
    this.active = undefined;
  }
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
  snapshot(): Promise<unknown>;
  action(argv: readonly string[]): Promise<void>;
  read(): Promise<PairState>;
  relaunch(): Promise<void>;
}

async function exercise(io: WorkflowIO, title: string) {
  const refs = new FreshRefs();
  const act = async (label: string, command: string) => {
    refs.observe(await io.snapshot());
    const ref = refs.consume(refs.select(label));
    await io.action([command, ref]);
  };
  const initial = await io.read();
  NodeAssert.equal(initial.target.pinnedAt, null, "target must start unpinned");
  await act(title, "longpress");
  await act("Pin", "click");
  const pinned = await io.read();
  assertTransition(initial, pinned, true);
  await io.relaunch();
  refs.invalidate();
  assertPersistence(pinned, await io.read());
  await act(title, "longpress");
  refs.observe(await io.snapshot());
  const unpin = refs.consume(refs.select("Unpin"));
  await io.action(["click", unpin]);
  const unpinned = await io.read();
  assertTransition(pinned, unpinned, false);
  await io.relaunch();
  refs.invalidate();
  assertPersistence(unpinned, await io.read());
  await act(title, "longpress");
  refs.observe(await io.snapshot());
  refs.select("Pin");
  return {
    initial,
    pinned,
    unpinned,
    uiAfterPinnedRelaunch: "Unpin",
    uiAfterUnpinnedRelaunch: "Pin",
  };
}

export async function runInjected(io: WorkflowIO, title: string, cleanup = new OwnedCleanup()) {
  const outcome = await exercise(io, title).then(
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
    schema: "jones-ios-workflow-receipt/1",
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
  app: string;
  buildRecord: string;
  driverPackage: string;
  runtime: string;
  deviceType: string;
  scratchParent: string;
  receipt: string;
  controlPinned: boolean;
}
interface CommandResult {
  stdout: string;
  code: number;
}
async function command(
  executable: string,
  args: readonly string[],
  cwd: string,
  env?: NodeJS.ProcessEnv,
  input?: string,
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(
      executable,
      [...args],
      {
        cwd,
        ...(env ? { env } : {}),
        timeout: 120_000,
        maxBuffer: 8 * 1024 * 1024,
      },
      (error, stdout) => {
        if (error && typeof error.code !== "number") {
          reject(new Error(`Command effect unknown: ${NodePath.basename(executable)}`));
          return;
        }
        resolve({ stdout, code: typeof error?.code === "number" ? error.code : 0 });
      },
    );
    child.stdin?.end(input);
  });
}
async function checked(
  executable: string,
  args: readonly string[],
  cwd: string,
  env?: NodeJS.ProcessEnv,
  input?: string,
) {
  const result = await command(executable, args, cwd, env, input);
  NodeAssert.equal(
    result.code,
    0,
    `${NodePath.basename(executable)} ${args[0]} failed (${result.code})`,
  );
  return result.stdout.trim();
}
async function jsonCommand(executable: string, args: readonly string[], cwd: string) {
  return record(JSON.parse(await checked(executable, args, cwd)));
}
async function pathFact(path: string) {
  try {
    const stat = await NodeFSP.lstat(path);
    return {
      kind: stat.isSymbolicLink() ? "symlink" : stat.isDirectory() ? "directory" : "other",
      dev: stat.dev,
      ino: stat.ino,
    };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { kind: "absent" };
    throw error;
  }
}
const xctestPath = () => NodePath.join(NodeOS.homedir(), "Library", "Developer", "XCTestDevices");

export interface GateFacts {
  resourceDiagnostics?: { cpuUsed: number; cores: number; freeBytes: number; measuredAt: number };
  source?: { head: string; requested: string; status: string; generatedIgnored: boolean };
  app?: {
    bundleId: string;
    bundleSha256: string;
    executableSha256: string;
    updatesEnabled: unknown;
    sourceSha: string;
    recordedBundleSha256: string;
    recordedExecutableSha256: string;
    buildArgv: string[];
  };
  driver?: { version: string; legacyRepairSha256: string };
  host?: {
    xctest: { kind: string };
    foreignProcesses: string[];
    firstLaunchStatus: number;
    xcodeVersion: string;
  };
  runtime?: { identifier: string; isAvailable: boolean };
  runnerOutput?: "owned-driver-copy/.tmp/runner";
}
export function blockers(facts: GateFacts): string[] {
  const result: string[] = [];
  // OS counters are diagnostics, not canonical memory-pressure/swap admission.
  // Native qualification must establish fresh canonical admission at its run boundary.
  result.push("G1-canonical-admission-unavailable");
  const s = facts.source,
    app = facts.app;
  if (
    !s ||
    !/^[0-9a-f]{40}$/.test(s.head) ||
    s.head !== s.requested ||
    s.status !== "" ||
    !s.generatedIgnored ||
    !app ||
    app.sourceSha !== s.head ||
    app.updatesEnabled !== false ||
    !app.bundleId ||
    !/^[0-9a-f]{64}$/.test(app.bundleSha256) ||
    !/^[0-9a-f]{64}$/.test(app.executableSha256) ||
    app.bundleSha256 !== app.recordedBundleSha256 ||
    app.executableSha256 !== app.recordedExecutableSha256 ||
    !app.buildArgv.includes("Release") ||
    !app.buildArgv.includes("CODE_SIGNING_ALLOWED=NO") ||
    !app.buildArgv.includes("-derivedDataPath")
  )
    result.push("G2-source-app-identity");
  if (
    !facts.driver ||
    facts.driver.version !== DRIVER_VERSION ||
    facts.driver.legacyRepairSha256 !== LEGACY_REPAIR_SHA256
  )
    result.push("G3-driver-source-identity");
  if (facts.host?.xctest.kind !== "directory") result.push("G4-xctestdevices-shared-path");
  result.push(
    facts.host?.foreignProcesses.length ? "G5-foreign-daemon" : "G5-process-scan-partial",
  );
  if (
    !facts.host ||
    facts.host.firstLaunchStatus !== 0 ||
    facts.host.xcodeVersion !== "Xcode 26.4\nBuild version 17E192" ||
    !facts.runtime?.isAvailable
  )
    result.push("G6-xcode-runtime");
  if (facts.runnerOutput !== "owned-driver-copy/.tmp/runner") result.push("G7-runner-output");
  // Pinned fresh startup and cleanup invoke runner-client's pkill -f. No approval boolean can qualify it.
  result.push("driver-pattern-kill-cleanup");
  return result;
}

async function preflight(options: Options) {
  const facts: GateFacts = {};
  const errors: string[] = [];
  const probe = async (name: string, run: () => Promise<void>) => {
    try {
      await run();
    } catch {
      errors.push(`${name}-unavailable`);
    }
  };
  await probe("source", async () => {
    const head = await checked("git", ["rev-parse", "HEAD"], options.source);
    const status = await checked("git", ["status", "--porcelain"], options.source);
    await NodeFSP.access(NodePath.join(options.source, "apps/server/dist/bin.mjs"));
    const ignored = await command("git", ["check-ignore", "apps/mobile/ios/"], options.source);
    facts.source = {
      head,
      requested: options.sourceSha,
      status,
      generatedIgnored: ignored.code === 0,
    };
  });
  await probe("app", async () => {
    const build = record(JSON.parse(await NodeFSP.readFile(options.buildRecord, "utf8")));
    NodeAssert.equal(
      await NodeFSP.realpath(String(build.appPath)),
      await NodeFSP.realpath(options.app),
    );
    const info = await jsonCommand(
      "/usr/bin/plutil",
      ["-convert", "json", "-o", "-", NodePath.join(options.app, "Info.plist")],
      options.source,
    );
    const expo = await jsonCommand(
      "/usr/bin/plutil",
      ["-convert", "json", "-o", "-", NodePath.join(options.app, "Expo.plist")],
      options.source,
    );
    NodeAssert.ok(
      typeof info.CFBundleExecutable === "string" && info.CFBundleExecutable.length > 0,
    );
    NodeAssert.ok(
      typeof info.CFBundleIdentifier === "string" && info.CFBundleIdentifier.length > 0,
    );
    const executable = info.CFBundleExecutable;
    NodeAssert.equal(NodePath.basename(executable), executable);
    NodeAssert.ok(
      Array.isArray(info.CFBundleSupportedPlatforms) &&
        info.CFBundleSupportedPlatforms.includes("iPhoneSimulator"),
    );
    NodeAssert.ok(
      Array.isArray(build.buildArgv) && build.buildArgv.every((arg) => typeof arg === "string"),
    );
    NodeAssert.equal(build.xcodeVersion, "Xcode 26.4\nBuild version 17E192");
    facts.app = {
      bundleId: String(info.CFBundleIdentifier),
      bundleSha256: sha256(await NodeFSP.readFile(NodePath.join(options.app, "main.jsbundle"))),
      executableSha256: sha256(await NodeFSP.readFile(NodePath.join(options.app, executable))),
      updatesEnabled: expo.EXUpdatesEnabled,
      sourceSha: String(build.sourceSha),
      recordedBundleSha256: String(build.bundleSha256),
      recordedExecutableSha256: String(build.executableSha256),
      buildArgv: build.buildArgv as string[],
    };
  });
  await probe("driver", async () => {
    const pkg = record(
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(options.driverPackage, "package.json"), "utf8"),
      ),
    );
    NodeAssert.equal(pkg.name, "agent-device");
    facts.driver = {
      version: String(pkg.version),
      legacyRepairSha256: sha256(
        await NodeFSP.readFile(
          NodePath.join(options.driverPackage, "dist/src/legacy-xctest-device-set.js"),
        ),
      ),
    };
  });
  await probe("host", async () => {
    const processes = await checked("/bin/ps", ["-axo", "pid=,comm="], options.source);
    facts.host = {
      xctest: await pathFact(xctestPath()),
      foreignProcesses: processes
        .split("\n")
        .filter((line) => /agent-device|AgentDevice|xctest|XCTest|xcodebuild/i.test(line)),
      firstLaunchStatus: (
        await command("/usr/bin/xcodebuild", ["-checkFirstLaunchStatus"], options.source)
      ).code,
      xcodeVersion: await checked("/usr/bin/xcodebuild", ["-version"], options.source),
    };
    const runtimes = await jsonCommand(
      "/usr/bin/xcrun",
      ["simctl", "list", "runtimes", "--json"],
      options.source,
    );
    NodeAssert.ok(Array.isArray(runtimes.runtimes));
    const runtime = runtimes.runtimes
      .map(record)
      .find((item) => item.identifier === options.runtime);
    NodeAssert.ok(runtime);
    facts.runtime = {
      identifier: String(runtime.identifier),
      isAvailable: runtime.isAvailable === true,
    };
    const types = await jsonCommand(
      "/usr/bin/xcrun",
      ["simctl", "list", "devicetypes", "--json"],
      options.source,
    );
    NodeAssert.ok(
      Array.isArray(types.devicetypes) &&
        types.devicetypes.map(record).some((type) => type.identifier === options.deviceType),
    );
  });
  facts.runnerOutput = "owned-driver-copy/.tmp/runner";
  const before = NodeOS.cpus();
  await NodeTimersPromises.setTimeout(250);
  const after = NodeOS.cpus();
  const total = (cpus: typeof before) =>
    cpus.reduce((sum, cpu) => sum + Object.values(cpu.times).reduce((a, b) => a + b, 0), 0);
  const elapsed = total(after) - total(before);
  const idle =
    after.reduce((sum, cpu) => sum + cpu.times.idle, 0) -
    before.reduce((sum, cpu) => sum + cpu.times.idle, 0);
  facts.resourceDiagnostics = {
    cpuUsed: elapsed > 0 ? 100 * (1 - idle / elapsed) : Number.NaN,
    cores: after.length,
    freeBytes: NodeOS.freemem(),
    measuredAt: Date.now(),
  };
  return { facts, blockers: [...errors, ...blockers(facts)], nativeEligible: false };
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
async function stopChild(child: NodeChildProcess.ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = NodeEvents.EventEmitter.once(child, "exit", {
    signal: AbortSignal.timeout(15_000),
  });
  child.kill("SIGTERM");
  await exited;
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
  const baseDir = NodePath.join(root, "server");
  const binary = NodePath.join(options.source, "apps/server/dist/bin.mjs");
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  cleanup.add("server-port-closed", () => portClosed(port));
  const child = NodeChildProcess.spawn(
    process.execPath,
    [binary, "serve", "--base-dir", baseDir, "--host", "127.0.0.1", "--port", String(port)],
    { cwd: root, env, stdio: "ignore" },
  );
  cleanup.add("server-child-exited", () => stopChild(child));
  await NodeEvents.EventEmitter.once(child, "spawn");
  const until = Date.now() + 30_000;
  let environmentId = "";
  while (Date.now() < until) {
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

async function nativeRun(options: Options, facts: GateFacts) {
  const gate = blockers(facts);
  NodeAssert.equal(gate.length, 0, `Native execution blocked: ${gate.join(", ")}`);
  await NodeFSP.writeFile(
    options.receipt,
    JSON.stringify({
      schema: "jones-ios-workflow-receipt/1",
      outcome: "unknown",
      nativeClaim: false,
      stage: "allocating",
    }),
    { flag: "wx", mode: 0o600 },
  );
  const root = await NodeFSP.mkdtemp(
    NodePath.join(await NodeFSP.realpath(options.scratchParent), "jones-ios-pin-"),
  );
  const cleanup = new OwnedCleanup();
  const secrets: string[] = [];
  const steps: { argv: string[]; code: number; digest: string }[] = [];
  const receipt: Record<string, unknown> = {
    schema: "jones-ios-workflow-receipt/1",
    runner: "native",
    tier: "N-candidate",
    nativeClaim: false,
    source: facts.source,
    app: facts.app,
    driver: facts.driver,
    runtime: facts.runtime,
    scratch: root,
    steps,
  };
  cleanup.add(
    "scratch-removed",
    async () => {
      await NodeFSP.writeFile(
        options.receipt,
        JSON.stringify({ ...receipt, outcome: "unknown", stage: "cleanup" }, null, 2),
      );
      await NodeFSP.rm(root, { recursive: true });
      NodeAssert.equal((await pathFact(root)).kind, "absent");
    },
    true,
  );
  let primary: string | undefined;
  try {
    await NodeFSP.mkdir(NodePath.join(root, "tmp"));
    await NodeFSP.cp(options.driverPackage, NodePath.join(root, "driver"), {
      recursive: true,
      errorOnExist: true,
      force: false,
    });
    const env = driverEnvironment(root, process.env);
    const fixture = await startFixture(options, root, cleanup, secrets);
    receipt.server = { environmentId: fixture.environmentId, port: fixture.port };
    const devices = async () => {
      const list = await jsonCommand(
        "/usr/bin/xcrun",
        ["simctl", "list", "devices", "--json"],
        root,
      );
      return Object.values(record(list.devices)).flatMap((items) => {
        NodeAssert.ok(Array.isArray(items));
        return items.map((item) => {
          const device = record(item);
          return { udid: String(device.udid), name: String(device.name), state: device.state };
        });
      });
    };
    const previous = await devices();
    const name = `jones-pin-${id()}`;
    NodeAssert.ok(!previous.some((device) => device.name === name), "device name already existed");
    let creationKnown = false;
    cleanup.add("device-creation-known", async () => {
      NodeAssert.ok(creationKnown, "simulator creation effect unknown; retain scratch");
    });
    const rawUdid = await checked(
      "/usr/bin/xcrun",
      ["simctl", "create", name, options.deviceType, options.runtime],
      root,
    );
    let udid: string;
    try {
      udid = acceptCreatedDevice(rawUdid, previous, name);
      creationKnown = true;
    } catch (error) {
      cleanup.add("unresolved-device-creation", async () => {
        throw error;
      });
      throw error;
    }
    receipt.device = { udid, createdByRun: true, preexisting: false };
    cleanup.add(
      "owned-device-deleted",
      async () => {
        const current = (await devices()).find((device) => device.udid === udid);
        if (current?.state !== "Shutdown")
          await checked("/usr/bin/xcrun", ["simctl", "shutdown", udid], root);
        await checked("/usr/bin/xcrun", ["simctl", "delete", udid], root);
        NodeAssert.ok(
          !(await devices()).some((device) => device.udid === udid),
          "owned simulator still exists",
        );
      },
      true,
    );
    await checked("/usr/bin/xcrun", ["simctl", "boot", udid], root);
    await checked("/usr/bin/xcrun", ["simctl", "bootstatus", udid, "-b"], root);
    await checked("/usr/bin/xcrun", ["simctl", "install", udid, options.app], root);
    const bundleId = facts.app!.bundleId;
    const target = {
      udid,
      session: `pin-${id()}`,
      config: NodePath.join(root, "driver-config.json"),
    };
    await NodeFSP.writeFile(
      target.config,
      JSON.stringify({ stateDir: env.AGENT_DEVICE_STATE_DIR }),
      { mode: 0o600, flag: "wx" },
    );
    const entry = NodePath.join(root, "driver/bin/agent-device.mjs");
    const driver = async (action: readonly string[], input?: string) => {
      const args = driverArgv(target, action);
      const result = await command(process.execPath, [entry, ...args], root, env, input);
      steps.push({
        argv: args.map((arg) => redact(arg, secrets)),
        code: result.code,
        digest: sha256(redact(result.stdout, secrets)),
      });
      NodeAssert.equal(result.code, 0, `driver ${action[0]} failed`);
      const envelope = record(JSON.parse(result.stdout));
      NodeAssert.equal(envelope.success, true, `driver ${action[0]} rejected`);
      return envelope;
    };
    receipt.xctestBefore = await pathFact(xctestPath());
    cleanup.add("xctest-unchanged", async () => {
      receipt.xctestAfter = await pathFact(xctestPath());
      NodeAssert.deepEqual(
        receipt.xctestAfter,
        receipt.xctestBefore,
        "shared XCTestDevices changed",
      );
    });
    cleanup.add("driver-daemon-stopped", async () => {
      await checked(
        process.execPath,
        [entry, "daemon", "stop", "--state-dir", env.AGENT_DEVICE_STATE_DIR!, "--json"],
        root,
        env,
      );
    });
    cleanup.add("driver-session-closed", async () => {
      await driver(["close", "--shutdown"]);
    });
    await driver(["open", bundleId]);
    const refs = new FreshRefs();
    const select = async (label: string, action: string, input?: string) => {
      refs.observe(await driver(["snapshot", "-i", "--force-full"]));
      const ref = refs.consume(refs.select(label));
      await driver([action, ref, ...(input === undefined ? [] : ["--text-stdin"])], input);
    };
    await select("Add environment", "click");
    await select("Address", "fill", fixture.origin);
    await select("Code", "fill", fixture.pairingToken);
    await select("Add environment", "click");
    await driver(["wait", `label=${JSON.stringify(fixture.title)}`, "--timeout", "15000"]);
    receipt.oracle = await exercise(
      {
        snapshot: () => driver(["snapshot", "-i", "--force-full"]),
        action: async (args) => {
          await driver(args);
        },
        read: fixture.read,
        relaunch: async () => {
          await checked("/usr/bin/xcrun", ["simctl", "terminate", udid, bundleId], root);
          await driver(["open", bundleId]);
          await driver(["wait", `label=${JSON.stringify(fixture.title)}`, "--timeout", "15000"]);
        },
      },
      fixture.title,
    );
  } catch (error) {
    primary = redact(message(error), secrets);
  }
  const result = await cleanup.finish(primary);
  Object.assign(receipt, result);
  await NodeFSP.writeFile(options.receipt, JSON.stringify(receipt, null, 2), { mode: 0o600 });
  return receipt;
}

export async function main(argv: string[]) {
  const mode = argv[0];
  NodeAssert.ok(
    ["--plan", "--preflight", "--execute"].includes(mode ?? ""),
    "Use --plan, --preflight or --execute",
  );
  if (mode === "--plan")
    return {
      schema: "jones-ios-workflow-plan/1",
      driver: DRIVER_VERSION,
      nativeEligible: false,
      blocker: "driver-pattern-kill-cleanup",
      required: [
        "--source",
        "--source-sha",
        "--app",
        "--build-record",
        "--driver-package",
        "--runtime",
        "--device-type",
        "--scratch-parent",
        "--receipt",
      ],
      steps: [
        "ordinary server pairing + project/two threads",
        "fresh owned simctl create",
        "install Release app",
        "pair through Address/Code UI",
        "fresh snapshot → longpress target → fresh snapshot → click Pin",
        "read target/control → relaunch → fresh menu Unpin",
        "click Unpin → read target/control → relaunch → fresh menu Pin",
        "close owned session → stop owned daemon → delete created simulator → stop server → verify port → cleanup scratch",
      ],
      targetFlags: [
        "--config <owned>",
        "--session <owned>",
        "--platform ios",
        "--udid <created>",
        "--json",
        "--response-level full",
      ],
    };
  const allowed = new Set([
    "--source",
    "--source-sha",
    "--app",
    "--build-record",
    "--driver-package",
    "--runtime",
    "--device-type",
    "--scratch-parent",
    "--receipt",
    "--control-pinned",
  ]);
  const values = new Map<string, string>();
  for (let index = 1; index < argv.length; index += 2) {
    const key = argv[index],
      value = argv[index + 1];
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
  const options: Options = {
    source: NodePath.resolve(required("--source")),
    sourceSha: required("--source-sha"),
    app: NodePath.resolve(required("--app")),
    buildRecord: NodePath.resolve(required("--build-record")),
    driverPackage: NodePath.resolve(required("--driver-package")),
    runtime: required("--runtime"),
    deviceType: required("--device-type"),
    scratchParent: NodePath.resolve(required("--scratch-parent")),
    receipt: NodePath.resolve(required("--receipt")),
    controlPinned: values.get("--control-pinned") === "true",
  };
  NodeAssert.ok(
    !values.has("--control-pinned") || ["true", "false"].includes(values.get("--control-pinned")!),
    "--control-pinned expects true or false",
  );
  const pre = await preflight(options);
  if (mode === "--preflight" || pre.blockers.length > 0)
    return {
      schema: "jones-ios-workflow-preflight/1",
      mode,
      ...pre,
      runner: "native",
      nativeClaim: false,
      outcome: "blocked",
    };
  return nativeRun(options, pre.facts);
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
