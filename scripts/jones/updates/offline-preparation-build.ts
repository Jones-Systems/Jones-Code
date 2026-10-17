// @effect-diagnostics-next-line nodeBuiltinImport:off -- This build adapter observes and produces exact native artifact bytes before an application runtime exists.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Build commands use exact checkout or owned scratch paths with bounded output and duration.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native SHA-256 binds downloaded, emitted and signed bytes without re-encoding them.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The build owns its checkout/output paths, never application state.
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native deadlines bound captured build resources before their scratch can be removed.
import * as NodeTimers from "node:timers";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Only the closed lifecycle fixture opens an owned loopback HTTP server.
import * as NodeHttp from "node:http";
import * as NodeURL from "node:url";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The sole download origin is the pinned official Node release over verified TLS.
import * as NodeHttps from "node:https";
import * as Schema from "effect/Schema";
import type * as TypeScript from "typescript-legacy";
import type { Plugin } from "vite-plus";
import {
  createJonesLegacyBootstrapPreparationBundle,
  type JonesLegacyBootstrapBundleInput,
  type JonesLegacyBootstrapPreparationBundle,
  readJonesPreparationArtifactEvidence,
  verifyJonesLegacyIdentityMachO,
} from "./legacy-bootstrap-bundle.ts";
import { jonesLegacyMaintenanceSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts";
import {
  verifyFunctionalPreparationGraph,
  verifyPreparationSourceOverlay,
} from "./functional-policy.ts";
export { verifyPreparationSourceOverlay } from "./functional-policy.ts";

const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const nodeVersion = "26.8.2";
const archiveName = `node-v${nodeVersion}-darwin-arm64.tar.gz`;
const releaseOrigin = `https://nodejs.org/download/release/v${nodeVersion}/`;
const entry = "src/jones/updates/offlinePreparationEntry.ts";
const bundleName = "offlinePreparationEntry.mjs";
const executableName = "jones-offline-preparation";
const outRelative = "apps/server/dist-exe/preparation";
const releaseManifests = [
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
];
const sourceFiles = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  ...releaseManifests,
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  `apps/server/${entry}`,
  "apps/server/src/jones/updates/offlinePreparation.ts",
  "apps/server/src/jones/updates/offlinePreparationCore.ts",
  "apps/server/src/jones/updates/offlinePreparationPlan.ts",
  "apps/server/src/jones/updates/offlinePreparationWorker.ts",
];
const maxBundleBytes = 32 * 1024 * 1024;
const maxRuntimeBytes = 256 * 1024 * 1024;
const forbiddenNames = new Set([
  "require",
  "createRequire",
  "eval",
  "Function",
  "constructor",
  "prototype",
  "__proto__",
  "dlopen",
  "getBuiltinModule",
  "binding",
  "_linkedBinding",
  "withSyntheticPreparationFixture",
  "initializeFixture",
]);
const forbiddenBuiltins = new Set([
  "module",
  "vm",
  "ffi",
  "worker_threads",
  "child_process",
  "process",
]);

const digest = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");

let preparationParser: typeof TypeScript | undefined;
function preparationTypeScript(): typeof TypeScript {
  // Parsing is an explicit operation; capturing the producer must not initialize TypeScript.
  return (preparationParser ??= NodeModule.createRequire(import.meta.url)(
    "typescript-legacy",
  ) as typeof TypeScript);
}

/** An oracle for the fixed held entry, not a sandbox for arbitrary JavaScript. */
export function verifyPreparationClosure(source: string): readonly string[] {
  const ts = preparationTypeScript();
  if (Buffer.byteLength(source) > maxBundleBytes)
    throw new Error("Preparation bundle exceeds bound.");
  const file = ts.createSourceFile(
    bundleName,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.JS,
  );
  const diagnostics = (
    file as TypeScript.SourceFile & { parseDiagnostics: readonly TypeScript.Diagnostic[] }
  ).parseDiagnostics;
  if (diagnostics.length) throw new Error("Preparation bundle is not valid JavaScript.");
  const imports = new Set<string>();
  const inspectSpecifier = (node: TypeScript.Node | undefined) => {
    if (!node || !ts.isStringLiteralLike(node)) throw new Error("Computed preparation loader.");
    const id = node.text;
    if (!NodeModule.isBuiltin(id) || forbiddenBuiltins.has(id.replace(/^node:/, "")))
      throw new Error(`Forbidden preparation import: ${id}`);
    imports.add(id);
  };
  const heldProperties = new Set([
    "JSON.stringify",
    "process.stdout",
    "process.stdout.write",
    "process.argv",
    "process.argv.length",
    "process.exitCode",
    "import.meta.main",
  ]);
  const visit = (node: TypeScript.Node): void => {
    if (
      ts.isElementAccessExpression(node) ||
      ts.isComputedPropertyName(node) ||
      (ts.isPropertyAccessExpression(node) &&
        !heldProperties.has(node.getText(file).replace(/\s/g, "")))
    )
      throw new Error("Computed or reflective access is outside the fixed preparation entry.");
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      if (node.moduleSpecifier) inspectSpecifier(node.moduleSpecifier);
    }
    if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      if (node.arguments.length !== 1)
        throw new Error("Preparation import attributes are not admitted.");
      inspectSpecifier(node.arguments[0]);
    }
    if (
      (ts.isIdentifier(node) || ts.isStringLiteralLike(node)) &&
      (forbiddenNames.has(node.text) || /(?:^|_)require(?:_|$)/.test(node.text))
    )
      throw new Error(`Forbidden preparation loader or initializer: ${node.text}`);
    if (ts.isIdentifier(node) && ["process", "globalThis", "module"].includes(node.text)) {
      const parent = node.parent;
      if (
        !ts.isPropertyAccessExpression(parent) ||
        parent.expression !== node ||
        node.text !== "process" ||
        !["stdout", "argv", "exitCode"].includes(parent.name.text)
      )
        throw new Error("Unadmitted preparation runtime capability.");
    }
    if (ts.isStringLiteralLike(node) && /\bCREATE\s+TABLE\b/i.test(node.text))
      throw new Error("Synthetic or migration initializer survived preparation bundling.");
    ts.forEachChild(node, visit);
  };
  visit(file);
  return [...imports].sort();
}

export function preparationSeaConfig() {
  return {
    main: bundleName,
    mainFormat: "module",
    executable: "node",
    output: executableName,
    disableExperimentalSEAWarning: true,
    useSnapshot: false,
    useCodeCache: false,
    execArgv: [],
    execArgvExtension: "none",
  } as const;
}

export function verifyPreparationSeaConfig(value: unknown): void {
  if (encode(value) !== encode(preparationSeaConfig()))
    throw new Error("Preparation SEA configuration differs from the fixed build contract.");
}

export function verifyNodeArchive(checksums: string, bytes: Uint8Array): string {
  const matches = checksums.split(/\r?\n/).filter((line) => line.endsWith(`  ${archiveName}`));
  if (
    matches.length !== 1 ||
    matches[0]!.length !== 66 + archiveName.length ||
    !/^[a-f0-9]{64}  /.test(matches[0]!)
  )
    throw new Error("Missing or ambiguous official Node checksum.");
  const sha256 = digest(bytes);
  if (matches[0]!.slice(0, 64) !== sha256)
    throw new Error("Official Node archive checksum mismatch.");
  return sha256;
}

interface BuildCustody {
  readonly kind: "command" | "download" | "qualification";
  readonly id: number;
  readonly target: string;
  readonly pid?: number | undefined;
  readonly exitObserved?: boolean;
  readonly closeObserved: false;
  readonly failures: readonly string[];
}

interface LifecycleFixtureHooks {
  readonly finalizationFault?: "nonempty-cleanup" | "custody-receipt-directory";
  readonly suppressCommandClose?: boolean;
  readonly child?: (child: NodeChildProcess.ChildProcess) => void;
  readonly stdout?: (bytes: Buffer) => void;
}

// Retention preserves actual captured handles during this process lifetime. The receipt's PID is diagnostic, never signalling authority.
const retainedBuildHandles = new Set<object>();

class PreparationBuildScope {
  readonly controller = new AbortController();
  readonly pending = new Set<Promise<unknown>>();
  readonly unresolved: BuildCustody[] = [];
  root: string | undefined;
  private admitting = true;
  private resourceId = 0;
  private readonly onInterrupt = () => {
    process.exitCode = 130;
    this.abort(new Error("Preparation build interrupted by SIGINT."));
  };
  private readonly onTerminate = () => {
    process.exitCode = 143;
    this.abort(new Error("Preparation build interrupted by SIGTERM."));
  };
  private readonly onExternalAbort = () =>
    this.abort(this.externalSignal?.reason ?? new Error("Preparation build aborted."));

  private readonly externalSignal: AbortSignal | undefined;
  private readonly fixture: LifecycleFixtureHooks | undefined;

  constructor(externalSignal?: AbortSignal, fixture?: LifecycleFixtureHooks) {
    this.externalSignal = externalSignal;
    this.fixture = fixture;
    process.on("SIGINT", this.onInterrupt);
    process.on("SIGTERM", this.onTerminate);
    externalSignal?.addEventListener("abort", this.onExternalAbort, { once: true });
    if (externalSignal?.aborted) this.onExternalAbort();
  }

  get listenersInstalled() {
    return (
      process.listeners("SIGINT").includes(this.onInterrupt) &&
      process.listeners("SIGTERM").includes(this.onTerminate)
    );
  }

  get listenersDetached() {
    return (
      !process.listeners("SIGINT").includes(this.onInterrupt) &&
      !process.listeners("SIGTERM").includes(this.onTerminate)
    );
  }

  assertActive() {
    this.controller.signal.throwIfAborted();
    if (!this.admitting) throw new Error("Preparation build admission is closed.");
  }

  abort(cause: unknown) {
    if (!this.controller.signal.aborted) this.controller.abort(cause);
  }

  private track<A>(operation: Promise<A>): Promise<A> {
    const tracked: Promise<A> = operation.finally(() => this.pending.delete(tracked));
    this.pending.add(tracked);
    void tracked.catch(() => {});
    return tracked;
  }

  async drain(): Promise<readonly unknown[]> {
    this.admitting = false;
    const pending = [...this.pending];
    if (pending.length !== 0)
      this.abort(new Error("Preparation build left admitted resources unfinished."));
    const results = await Promise.allSettled(pending);
    return results.flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
  }

  detach() {
    this.externalSignal?.removeEventListener("abort", this.onExternalAbort);
    process.removeListener("SIGINT", this.onInterrupt);
    process.removeListener("SIGTERM", this.onTerminate);
  }

  command = (
    file: string,
    args: readonly string[],
    cwd: string,
    maxBuffer = 1024 * 1024,
  ): Promise<Buffer> => {
    this.assertActive();
    const id = ++this.resourceId;
    return this.track(
      new Promise<Buffer>((resolve, reject) => {
        let child: NodeChildProcess.ChildProcessWithoutNullStreams;
        try {
          child = NodeChildProcess.spawn(file, [...args], {
            cwd,
            env: {
              PATH: "/usr/bin:/bin",
              HOME: cwd,
              TMPDIR: cwd,
              LC_ALL: "C",
              GIT_CONFIG_NOSYSTEM: "1",
              GIT_CONFIG_GLOBAL: "/dev/null",
            },
            stdio: ["pipe", "pipe", "pipe"],
          });
        } catch (cause) {
          reject(cause);
          return;
        }
        // No command accepts input; closing this captured pipe cannot delegate a second control channel.
        child.stdin.on("error", (cause) => stop(cause));
        let settled = false;
        let exited = false;
        let stopping = false;
        let failure: unknown;
        const failures: string[] = [];
        const stdout: Buffer[] = [];
        const stderr: Buffer[] = [];
        let outputBytes = 0;
        let killTimer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
        let drainTimer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
        // @effect-diagnostics-next-line globalTimers:off -- This pre-runtime native boundary clears the captured child deadline on close.
        const deadline = NodeTimers.setTimeout(
          () => stop(new Error("Preparation native command timed out.")),
          120_000,
        );
        const onAbort = () => stop(this.controller.signal.reason);
        const finish = (cause?: unknown, output?: Buffer) => {
          if (settled) return;
          settled = true;
          NodeTimers.clearTimeout(deadline);
          if (killTimer) NodeTimers.clearTimeout(killTimer);
          if (drainTimer) NodeTimers.clearTimeout(drainTimer);
          this.controller.signal.removeEventListener("abort", onAbort);
          if (cause !== undefined) reject(cause);
          else resolve(output ?? Buffer.alloc(0));
        };
        const unknownClose = () => {
          if (settled) return;
          this.unresolved.push({
            kind: "command",
            id,
            target: file,
            pid: child.pid,
            exitObserved: exited,
            closeObserved: false,
            failures,
          });
          retainedBuildHandles.add(child);
          // The unresolved receipt retains scratch; unref only releases the build driver's event-loop dependency.
          child.unref();
          child.stdin.destroy();
          child.stdout.destroy();
          child.stderr.destroy();
          finish(
            new AggregateError(
              failure === undefined ? [] : [failure],
              "Native command close was not established; captured custody retained.",
            ),
          );
        };
        const signal = (name: "SIGTERM" | "SIGKILL") => {
          if (exited || child.pid === undefined) return;
          try {
            if (!child.kill(name)) failures.push(`${name}:not-delivered`);
          } catch (cause) {
            failures.push(`${name}:${cause instanceof Error ? cause.message : String(cause)}`);
            failure =
              failure === undefined
                ? cause
                : new AggregateError([failure, cause], "Native command cancellation failed.");
          }
        };
        const stop = (cause: unknown) => {
          if (settled || stopping) return;
          stopping = true;
          failure = cause;
          signal("SIGTERM");
          // @effect-diagnostics-next-line globalTimers:off -- This cleanup-owned timer escalates only the captured child and is cleared on close.
          killTimer = NodeTimers.setTimeout(() => signal("SIGKILL"), 1_000);
          // @effect-diagnostics-next-line globalTimers:off -- Native close observation has a bounded deadline that retains unresolved custody instead of deleting scratch.
          drainTimer = NodeTimers.setTimeout(unknownClose, 3_000);
        };
        const receive = (chunks: Buffer[], bytes: Buffer) => {
          outputBytes += bytes.length;
          if (outputBytes > maxBuffer) {
            stop(new Error("Preparation native command output exceeds bound."));
            return;
          }
          chunks.push(bytes);
        };
        child.stdout.on("data", (bytes: Buffer) => {
          receive(stdout, bytes);
          try {
            this.fixture?.stdout?.(bytes);
          } catch (cause) {
            stop(cause);
          }
        });
        child.stdout.on("error", stop);
        child.stderr.on("error", stop);
        child.stderr.on("data", (bytes: Buffer) => receive(stderr, bytes));
        child.on("error", (cause) => stop(cause));
        child.once("exit", () => {
          exited = true;
        });
        child.once("close", (status, signalName) => {
          if (this.fixture?.suppressCommandClose) {
            unknownClose();
            return;
          }
          retainedBuildHandles.delete(child);
          const out = Buffer.concat(stdout);
          const err = Buffer.concat(stderr);
          const commandFailure =
            status === 0 && signalName === null
              ? undefined
              : Object.assign(new Error("Preparation native command failed."), {
                  status,
                  signal: signalName,
                  stdout: out,
                  stderr: err,
                });
          finish(failure ?? commandFailure, out);
        });
        this.controller.signal.addEventListener("abort", onAbort, { once: true });
        try {
          this.fixture?.child?.(child);
        } catch (cause) {
          stop(cause);
        }
        child.stdin.end();
        if (this.controller.signal.aborted) onAbort();
      }),
    );
  };

  download(name: string, limit: number) {
    if (name !== archiveName && name !== "SHASUMS256.txt")
      throw new Error("Unapproved Node download member.");
    return this.request(new URL(name, releaseOrigin), limit, true);
  }

  // The non-TLS branch is reachable only from the closed loopback fixture below.
  request(url: URL, limit: number, secure: boolean): Promise<Buffer> {
    this.assertActive();
    const id = ++this.resourceId;
    return this.track(
      new Promise<Buffer>((resolve, reject) => {
        let failure: unknown;
        let settled = false;
        let ended = false;
        let requestClosed = false;
        let response: NodeHttp.IncomingMessage | undefined;
        let responseClosed = false;
        let socket: import("node:net").Socket | undefined;
        let socketClosed = false;
        let drainTimer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
        const chunks: Buffer[] = [];
        let size = 0;
        const onAbort = () => stop(this.controller.signal.reason);
        // @effect-diagnostics-next-line globalTimers:off -- This captured HTTP request deadline is cleared with its native socket finalizer.
        const deadline = NodeTimers.setTimeout(
          () => stop(new Error("Official Node download timed out.")),
          60_000,
        );
        const finish = (unknownClose = false) => {
          if (
            settled ||
            (!unknownClose &&
              (!requestClosed || (socket && !socketClosed) || (response && !responseClosed)))
          )
            return;
          settled = true;
          NodeTimers.clearTimeout(deadline);
          if (drainTimer) NodeTimers.clearTimeout(drainTimer);
          this.controller.signal.removeEventListener("abort", onAbort);
          if (unknownClose) {
            this.unresolved.push({
              kind: "download",
              id,
              target: url.origin + url.pathname,
              closeObserved: false,
              failures: ["request/socket close unobserved"],
            });
            retainedBuildHandles.add(request);
            socket?.unref();
            reject(
              new AggregateError(
                failure === undefined ? [] : [failure],
                "Download close was not established; captured custody retained.",
              ),
            );
          } else {
            retainedBuildHandles.delete(request);
            if (failure !== undefined || !ended)
              reject(failure ?? new Error("Download ended without a complete response."));
            else resolve(Buffer.concat(chunks));
          }
        };
        const stop = (cause: unknown) => {
          if (settled) return;
          failure ??= cause;
          response?.destroy();
          request.destroy();
          // @effect-diagnostics-next-line globalTimers:off -- This native socket-close deadline records unresolved custody and is cleared after observed closure.
          drainTimer ??= NodeTimers.setTimeout(() => finish(true), 2_000);
        };
        let request: NodeHttp.ClientRequest;
        try {
          request = (secure ? NodeHttps : NodeHttp).get(url, { agent: false }, (incoming) => {
            response = incoming;
            incoming.once("close", () => {
              responseClosed = true;
              finish();
            });
            incoming.on("error", stop);
            incoming.on("data", (bytes: Buffer) => {
              size += bytes.length;
              if (size > limit) {
                stop(new Error("Official Node download exceeds bound."));
                return;
              }
              chunks.push(bytes);
            });
            incoming.once("end", () => {
              ended = true;
              finish();
            });
            if (incoming.statusCode !== 200) stop(new Error("Official Node download refused."));
          });
        } catch (cause) {
          NodeTimers.clearTimeout(deadline);
          reject(cause);
          return;
        }
        request.once("socket", (captured) => {
          socket = captured;
          captured.once("close", () => {
            socketClosed = true;
            finish();
          });
        });
        request.on("error", stop);
        request.once("close", () => {
          requestClosed = true;
          finish();
        });
        this.controller.signal.addEventListener("abort", onAbort, { once: true });
        if (this.controller.signal.aborted) onAbort();
      }),
    );
  }
}

async function withPreparationBuildScope<A>(
  parent: string,
  prefix: string,
  signal: AbortSignal | undefined,
  operation: (scope: PreparationBuildScope & { root: string }) => Promise<A>,
  fixture?: LifecycleFixtureHooks,
): Promise<A> {
  const scope = new PreparationBuildScope(signal, fixture);
  let result: A | undefined;
  const causes: unknown[] = [];
  try {
    scope.assertActive();
    scope.root = NodeFS.mkdtempSync(NodePath.join(parent, prefix));
    result = await operation(scope as PreparationBuildScope & { root: string });
  } catch (cause) {
    causes.push(cause);
    scope.abort(cause);
  }
  try {
    for (const cause of await scope.drain()) if (!causes.includes(cause)) causes.push(cause);
    if (scope.controller.signal.aborted && !causes.includes(scope.controller.signal.reason))
      causes.push(scope.controller.signal.reason);
    if (
      scope.root &&
      (scope.unresolved.length !== 0 || causes.some((cause) => qualificationCustodyUnknown(cause)))
    ) {
      const evidence = {
        observation: "incomplete",
        classification: "unusable",
        retainedRoot: scope.root,
        signalAuthority: "captured-handle-only",
        resources: scope.unresolved,
      };
      if (fixture?.finalizationFault === "custody-receipt-directory")
        NodeFS.mkdirSync(NodePath.join(scope.root, "retained-custody.json"));
      const fd = NodeFS.openSync(NodePath.join(scope.root, "retained-custody.json"), "wx", 0o600);
      try {
        NodeFS.writeFileSync(fd, `${encode(evidence)}\n`);
        NodeFS.fsyncSync(fd);
      } finally {
        NodeFS.closeSync(fd);
      }
      const directory = NodeFS.openSync(scope.root, NodeFS.constants.O_RDONLY);
      try {
        NodeFS.fsyncSync(directory);
      } finally {
        NodeFS.closeSync(directory);
      }
      causes.push(
        Object.assign(new Error(`Preparation build custody unresolved: ${scope.root}`), {
          retainedRoot: scope.root,
        }),
      );
    } else if (scope.root) {
      if (fixture?.finalizationFault === "nonempty-cleanup") NodeFS.rmdirSync(scope.root);
      else NodeFS.rmSync(scope.root, { recursive: true, force: true });
    }
  } catch (cause) {
    causes.push(cause);
  } finally {
    scope.detach();
  }
  if (causes.length === 1) throw causes[0];
  if (causes.length > 1)
    throw new AggregateError(
      causes,
      `Preparation build failed${scope.root ? `: ${scope.root}` : ""}`,
    );
  return result as A;
}

/** Fixed synthetic scenarios exercise the producer's actual resource scope without Node downloads or signing. */
export async function runPreparationLifecycleFixture(
  scenario:
    | "success"
    | "failure"
    | "abort-command"
    | "abort-download"
    | "uncertain-close"
    | "drain-unawaited-command"
    | "signal-command"
    | "await-signal-command"
    | "cleanup-failure"
    | "receipt-failure",
) {
  if (
    ![
      "success",
      "failure",
      "abort-command",
      "abort-download",
      "uncertain-close",
      "drain-unawaited-command",
      "signal-command",
      "await-signal-command",
      "cleanup-failure",
      "receipt-failure",
    ].includes(scenario)
  )
    throw new Error("Unknown closed preparation lifecycle fixture.");
  const outer = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-preparation-lifecycle-"));
  const controller = new AbortController();
  const observed: {
    scope?: PreparationBuildScope;
    child?: NodeChildProcess.ChildProcess;
    server?: NodeHttp.Server;
    rootIdentity?: { dev: number; ino: number };
  } = {};
  let childClosed = false;
  let closeChild: (() => void) | undefined;
  const childClose = new Promise<void>((resolve) => {
    closeChild = resolve;
  });
  let childReady: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    childReady = resolve;
  });
  let serverClosed = true;
  let retainedBeforeFixtureCleanup = false;
  let custodyEvidence: unknown;
  let retentionKind: "none" | "cleanup-retained" | "custody-unresolved" | "inspection-unresolved" =
    "none";
  let evidenceReadFailure: unknown;
  let retainedBeforeRecovery = false;
  let fixtureRecoveryObserved = false;
  let outerRemoved = false;
  let listenersInstalled = false;
  let operationFailure: unknown;
  let outcome: "complete" | "rejected" = "complete";
  let cleanupFailure: unknown;
  const bounded = async (promise: Promise<void>, message: string) => {
    let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
    try {
      await Promise.race([
        promise,
        new Promise<never>((_resolve, reject) => {
          // @effect-diagnostics-next-line globalTimers:off -- The owned native fixture barrier must remain bounded and clears its timer in finally.
          timer = NodeTimers.setTimeout(() => reject(new Error(message)), 5_000);
        }),
      ]);
    } finally {
      if (timer) NodeTimers.clearTimeout(timer);
    }
  };
  try {
    try {
      await withPreparationBuildScope(
        outer,
        ".build-",
        controller.signal,
        async (scope) => {
          observed.scope = scope;
          const identity = NodeFS.lstatSync(scope.root);
          observed.rootIdentity = { dev: identity.dev, ino: identity.ino };
          listenersInstalled = scope.listenersInstalled;
          NodeFS.writeFileSync(NodePath.join(scope.root, "owned.bin"), Buffer.alloc(4096), {
            flag: "wx",
            mode: 0o600,
          });
          if (scenario === "abort-download") {
            observed.server = NodeHttp.createServer((_request, response) => {
              response.on("error", () => {});
              response.writeHead(200);
              response.write("owned partial bytes");
              controller.abort(new Error("owned download abort"));
            });
            serverClosed = false;
            const listening = new Promise<void>((resolve, reject) => {
              observed.server!.once("error", reject);
              observed.server!.listen(0, "127.0.0.1", resolve);
            });
            await bounded(listening, "Owned loopback fixture did not listen.");
            const address = observed.server.address();
            if (!address || typeof address === "string")
              throw new Error("Missing owned loopback address.");
            await scope.request(new URL(`http://127.0.0.1:${address.port}/owned`), 4096, false);
            return;
          }
          const waiting =
            scenario === "abort-command" ||
            scenario === "drain-unawaited-command" ||
            scenario === "await-signal-command" ||
            scenario === "receipt-failure";
          const signalDriver = `import(${encode(import.meta.url)}).then(async ({ runPreparationLifecycleFixture }) => {
          const report = await runPreparationLifecycleFixture("await-signal-command");
          process.stdout.write("signal-report:" + JSON.stringify(report) + "\\n");
        }).catch((cause) => { process.stderr.write(String(cause)); process.exitCode = 1; });`;
          const program =
            scenario === "signal-command"
              ? signalDriver
              : waiting
                ? 'process.stdout.write("ready"); setInterval(() => {}, 60000);'
                : scenario === "failure" || scenario === "cleanup-failure"
                  ? 'process.stderr.write("owned failure"); process.exitCode = 7;'
                  : 'process.stdout.write("owned success");';
          const operation = scope.command(process.execPath, ["-e", program], scope.root, 4096);
          if (scenario === "drain-unawaited-command") {
            await bounded(ready, "Owned fixture child did not become ready.");
            return;
          }
          await operation;
        },
        {
          suppressCommandClose: scenario === "uncertain-close" || scenario === "receipt-failure",
          ...(scenario === "cleanup-failure"
            ? { finalizationFault: "nonempty-cleanup" as const }
            : scenario === "receipt-failure"
              ? { finalizationFault: "custody-receipt-directory" as const }
              : {}),
          child: (child) => {
            observed.child = child;
            child.once("close", () => {
              childClosed = true;
              closeChild?.();
            });
          },
          stdout: (bytes) => {
            if (!bytes.toString("utf8").includes("ready")) return;
            childReady?.();
            if (scenario === "signal-command") observed.child?.kill("SIGTERM");
            if (scenario === "await-signal-command") process.stdout.write("signal-scope-ready\n");
            if (scenario === "abort-command") controller.abort(new Error("owned command abort"));
            if (scenario === "receipt-failure")
              controller.abort(new Error("owned receipt primary failure"));
          },
        },
      );
    } catch (cause) {
      outcome = "rejected";
      operationFailure = cause;
    }
    if (observed.scope?.root) {
      let rootObserved = false;
      try {
        NodeFS.lstatSync(observed.scope.root);
        rootObserved = true;
        retainedBeforeFixtureCleanup = true;
        retentionKind =
          observed.scope.unresolved.length === 0 ? "cleanup-retained" : "custody-unresolved";
        // A failed cleanup does not imply that a custody receipt was ever produced.
        if (retentionKind === "custody-unresolved")
          custodyEvidence = decode(
            readFile(
              NodePath.join(observed.scope.root, "retained-custody.json"),
              32 * 1024,
            ).toString("utf8"),
          );
      } catch (cause) {
        const absentRoot =
          !rootObserved &&
          typeof cause === "object" &&
          cause !== null &&
          "code" in cause &&
          cause.code === "ENOENT";
        if (!absentRoot) {
          evidenceReadFailure = cause;
          if (!rootObserved) retentionKind = "inspection-unresolved";
          outcome = "rejected";
          operationFailure = new AggregateError(
            operationFailure === undefined ? [cause] : [operationFailure, cause],
            "Preparation fixture evidence inspection failed; prior cause and root retained.",
          );
        }
      }
    }
  } finally {
    try {
      // This supervisor retains the exact child independently of the scope's deliberately withheld close proof.
      if (observed.child && !childClosed) {
        observed.child.kill("SIGKILL");
        await bounded(childClose, "Fixture supervisor did not observe the captured child close.");
      }
      if (observed.server && !serverClosed) {
        const closed = new Promise<void>((resolve, reject) =>
          observed.server!.close((error) => {
            if (error) reject(error);
            else {
              serverClosed = true;
              resolve();
            }
          }),
        );
        observed.server.closeAllConnections();
        await bounded(closed, "Fixture loopback server close is unresolved.");
      }
      if (observed.child && !childClosed) throw new Error("Fixture child custody is unresolved.");
      const unresolvedClosedIndependently =
        observed.scope?.unresolved.every(
          (resource) =>
            resource.kind === "command" && resource.pid === observed.child?.pid && childClosed,
        ) ?? true;
      if (
        evidenceReadFailure === undefined &&
        retentionKind !== "cleanup-retained" &&
        unresolvedClosedIndependently
      ) {
        NodeFS.rmSync(outer, { recursive: true, force: true });
        outerRemoved = true;
        if (observed.child) retainedBuildHandles.delete(observed.child);
      } else retainedBeforeRecovery = true;
    } catch (cause) {
      cleanupFailure = cause;
    }
  }
  // Only these closed fault cases can reconcile their retained synthetic root. Neither a missing receipt nor a scenario name alone proves custody.
  if (
    retainedBeforeRecovery &&
    cleanupFailure === undefined &&
    (scenario === "cleanup-failure" || scenario === "receipt-failure")
  ) {
    try {
      const root = observed.scope?.root;
      if (
        !root ||
        !observed.rootIdentity ||
        !childClosed ||
        !serverClosed ||
        !observed.child ||
        !observed.scope?.unresolved.every(
          (resource) => resource.kind === "command" && resource.pid === observed.child?.pid,
        )
      )
        throw new Error("Synthetic finalization recovery lacks independent resource closure.");
      const identity = NodeFS.lstatSync(root);
      const names = NodeFS.readdirSync(root).sort();
      const expected =
        scenario === "cleanup-failure" ? ["owned.bin"] : ["owned.bin", "retained-custody.json"];
      if (
        !identity.isDirectory() ||
        identity.dev !== observed.rootIdentity.dev ||
        identity.ino !== observed.rootIdentity.ino ||
        encode(names) !== encode(expected) ||
        !readFile(NodePath.join(root, "owned.bin"), 4096).equals(Buffer.alloc(4096))
      )
        throw new Error("Synthetic finalization recovery root differs from the captured fixture.");
      if (scenario === "receipt-failure") {
        const receipt = NodePath.join(root, "retained-custody.json");
        if (!NodeFS.lstatSync(receipt).isDirectory() || NodeFS.readdirSync(receipt).length !== 0)
          throw new Error(
            "Synthetic receipt failure differs from the owned empty-directory fault.",
          );
      }
      // Inspection causes are already retained in operationFailure before removing these independently closed resources.
      NodeFS.rmSync(outer, { recursive: true, force: true });
      outerRemoved = true;
      retainedBuildHandles.delete(observed.child);
      fixtureRecoveryObserved = true;
    } catch (cause) {
      cleanupFailure = cause;
    }
  }
  if (cleanupFailure !== undefined)
    throw new AggregateError(
      operationFailure === undefined ? [cleanupFailure] : [operationFailure, cleanupFailure],
      `Preparation lifecycle fixture custody retained: ${outer}`,
    );
  let signalReport: unknown;
  if (
    scenario === "signal-command" &&
    typeof operationFailure === "object" &&
    operationFailure !== null &&
    "stdout" in operationFailure &&
    Buffer.isBuffer(operationFailure.stdout)
  ) {
    const line = operationFailure.stdout
      .toString("utf8")
      .split("\n")
      .find((value) => value.startsWith("signal-report:"));
    if (line) signalReport = decode(line.slice("signal-report:".length));
  }
  return {
    scenario,
    outcome,
    signalReport,
    listenersInstalled,
    listenersRestored: observed.scope?.listenersDetached === true,
    childCloseObserved: childClosed,
    serverClosed,
    retainedBeforeFixtureCleanup,
    custodyEvidence,
    retentionKind,
    retainedBeforeRecovery,
    fixtureRecoveryObserved,
    evidenceReadFailed: evidenceReadFailure !== undefined,
    failureCauses: preparationFailureCauses(operationFailure),
    retainedRoot: outerRemoved ? undefined : outer,
    rootRemoved: outerRemoved && !NodeFS.existsSync(outer),
    failureMessage: operationFailure instanceof Error ? operationFailure.message : undefined,
    failureStatus:
      typeof operationFailure === "object" &&
      operationFailure !== null &&
      "status" in operationFailure
        ? operationFailure.status
        : undefined,
  };
}

function preparationFailureCauses(
  cause: unknown,
): readonly { message: string; code?: unknown; status?: unknown }[] {
  const result: { message: string; code?: unknown; status?: unknown }[] = [];
  const visit = (value: unknown, depth: number): void => {
    if (depth > 8 || result.length >= 32 || !(value instanceof Error)) return;
    result.push({
      message: value.message,
      ...("code" in value ? { code: value.code } : {}),
      ...("status" in value ? { status: value.status } : {}),
    });
    if (value instanceof AggregateError)
      for (const inner of value.errors.slice(0, 32)) visit(inner, depth + 1);
  };
  visit(cause, 0);
  return result;
}

function readFile(file: string, limit: number): Buffer {
  const fd = NodeFS.openSync(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  let primary: unknown, result: Buffer | undefined;
  try {
    const before = NodeFS.fstatSync(fd);
    if (!before.isFile() || before.size > limit || before.nlink !== 1)
      throw new Error(`Unbounded or non-regular build member: ${NodePath.basename(file)}`);
    const buffer = Buffer.alloc(before.size + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = NodeFS.readSync(fd, buffer, offset, buffer.length - offset, null);
      if (count === 0) break;
      offset += count;
    }
    const bytes = buffer.subarray(0, offset);
    const after = NodeFS.fstatSync(fd);
    if (
      bytes.length !== before.size ||
      after.size !== before.size ||
      after.mtimeMs !== before.mtimeMs
    )
      throw new Error("Build member changed while hashing.");
    result = bytes;
  } catch (cause) {
    primary = cause;
  }
  try {
    NodeFS.closeSync(fd);
  } catch (cause) {
    throw Object.assign(
      new AggregateError(
        primary === undefined ? [cause] : [primary, cause],
        "Build input descriptor closure is unproved.",
        { cause: primary ?? cause },
      ),
      { preserveScratch: true },
    );
  }
  if (primary !== undefined) throw primary;
  return result!;
}

async function observeSource(root: string, command: PreparationBuildScope["command"]) {
  const git = async (...args: string[]) =>
    (await command("/usr/bin/git", args, root)).toString("utf8").trim();
  const observedRoot = NodeFS.realpathSync(await git("rev-parse", "--show-toplevel"));
  if (observedRoot !== root)
    throw new Error(
      `Preparation checkout root differs from the bound root: ${encode({ expected: root, observed: observedRoot })}`,
    );
  const untracked = await git("ls-files", "--others", "--exclude-standard");
  if (untracked !== "") {
    const paths = untracked.split("\n");
    throw new Error(
      `Preparation build refuses untracked source (${paths.length} paths; bounded first paths): ${encode(paths.slice(0, 32)).slice(0, 8192)}`,
    );
  }
  const sha = await git("rev-parse", "HEAD");
  const tree = await git("rev-parse", "HEAD^{tree}");
  if (![sha, tree].every((value) => /^[a-f0-9]{40}$(?![\s\S])/.test(value)))
    throw new Error("Invalid source identity.");
  const changed = (await git("diff", "--name-only", "--no-renames", "HEAD", "--"))
    .split("\n")
    .filter(Boolean);
  if (changed.some((file) => !releaseManifests.includes(file)))
    throw new Error("Preparation build source changed outside release stamping.");
  const versions: string[] = [];
  for (const file of releaseManifests)
    versions.push(
      verifyPreparationSourceOverlay(
        file,
        decode(await git("show", `HEAD:${file}`)),
        decode(readFile(NodePath.join(root, file), 1024 * 1024).toString("utf8")),
        { sha, tree },
      ),
    );
  if (new Set(versions).size !== 1) throw new Error("Release manifest versions differ.");
  const hashes = Object.fromEntries(
    sourceFiles.map((name) => [name, digest(readFile(NodePath.join(root, name), 8 * 1024 * 1024))]),
  );
  return {
    repository: "Jones-Systems/Jones-Code",
    sha,
    tree,
    version: versions[0]!,
    hashes,
    buildOnlyOverlays: changed,
  };
}

export function verifyPreparationHeldOutput(output: string): void {
  const status = Schema.decodeUnknownSync(
    Schema.Struct({
      status: Schema.Literal("held"),
      reason: Schema.Literal("maintenance-owner-binding-unqualified"),
      databaseOpened: Schema.Literal(false),
      productionInvocation: Schema.Literal(false),
      activation: Schema.Literal(false),
    }),
  )(decode(output));
  if (output !== `${encode(status)}\n`)
    throw new Error("Preparation entry output was not one exact held record.");
}

async function verifyArgumentRefusal(
  command: PreparationBuildScope["command"],
  executable: string,
  cwd: string,
): Promise<void> {
  try {
    await command(executable, ["--state", "/not-admitted", "--fixture"], cwd, 4096);
  } catch (cause) {
    if (typeof cause !== "object" || cause === null) throw cause;
    const error = cause as {
      status?: unknown;
      signal?: unknown;
      stdout?: unknown;
      stderr?: unknown;
    };
    if (
      error.status !== 64 ||
      error.signal !== null ||
      !Buffer.isBuffer(error.stdout) ||
      !Buffer.isBuffer(error.stderr) ||
      error.stderr.length !== 0
    )
      throw cause;
    verifyPreparationHeldOutput(error.stdout.toString("utf8"));
    return;
  }
  throw new Error("Preparation executable accepted application arguments.");
}

const SQLiteMetadata = Schema.Struct({
  version: Schema.String.check(
    Schema.isPattern(
      /^(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})$(?![\s\S])/,
    ),
  ),
  sourceId: Schema.String.check(
    Schema.isPattern(
      /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [a-f0-9]{64}$(?![\s\S])/,
    ),
  ),
  observation: Schema.Literal("in-memory-closed"),
});
const SQLiteProbeObservation = Schema.Struct({
  nodeVersion: Schema.Literal(nodeVersion),
  processVersion: Schema.Literal(`v${nodeVersion}`),
  platform: Schema.Literal("darwin"),
  arch: Schema.Literal("arm64"),
  database: Schema.Literal(":memory:"),
  databaseClosed: Schema.Literal(true),
  runtimeSQLiteVersion: SQLiteMetadata.fields.version,
  sqlite: SQLiteMetadata,
});

// Fixed native observation: no caller path, arguments, application state or migration can enter this script.
const sqliteProbeScript = `import { DatabaseSync } from "node:sqlite";
const database = new DatabaseSync(":memory:");
let row;
try {
  row = database.prepare("SELECT sqlite_version() AS version, sqlite_source_id() AS sourceId").get();
} finally {
  database.close();
}
if (row?.version !== process.versions.sqlite || process.versions.node !== "26.8.2" ||
    process.version !== "v26.8.2" || process.platform !== "darwin" || process.arch !== "arm64")
  throw new Error("Pinned Node SQLite observation mismatch.");
process.stdout.write(JSON.stringify({
  nodeVersion: process.versions.node, processVersion: process.version,
  platform: process.platform, arch: process.arch, database: ":memory:", databaseClosed: true,
  runtimeSQLiteVersion: process.versions.sqlite,
  sqlite: { version: row.version, sourceId: row.sourceId, observation: "in-memory-closed" }
}) + "\\n");`;

function validateSQLiteMetadata(value: typeof SQLiteMetadata.Type): void {
  const [year, month, day, hour, minute, second] = value.sourceId
    .slice(0, 19)
    .split(/[- :]/)
    .map(Number);
  const leap = year! % 4 === 0 && (year! % 100 !== 0 || year! % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month! < 1 ||
    month! > 12 ||
    day! < 1 ||
    day! > days[month! - 1]! ||
    hour! > 23 ||
    minute! > 59 ||
    second! > 59
  )
    throw new Error("Invalid observed SQLite source timestamp.");
}

export function verifyPreparationSQLiteObservation(raw: string): typeof SQLiteMetadata.Type {
  if (Buffer.byteLength(raw) > 4096) throw new Error("SQLite observation exceeds bound.");
  const observation = Schema.decodeUnknownSync(SQLiteProbeObservation, {
    onExcessProperty: "error",
  })(decode(raw));
  if (observation.runtimeSQLiteVersion !== observation.sqlite.version)
    throw new Error("SQLite runtime and SQL versions differ.");
  validateSQLiteMetadata(observation.sqlite);
  return Object.freeze(observation.sqlite);
}

const HashRecord = Schema.Struct({
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$(?![\s\S])/)),
  size: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maxRuntimeBytes)),
});
const ArtifactManifest = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  observation: Schema.Literal("complete"),
  classification: Schema.Literal("held-entry-only"),
  preparationCapable: Schema.Literal(false),
  productionInvocation: Schema.Literal(false),
  source: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$(?![\s\S])/)),
    tree: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$(?![\s\S])/)),
    version: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
    hashes: Schema.Record(
      Schema.String,
      Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$(?![\s\S])/)),
    ),
    buildOnlyOverlays: Schema.Array(Schema.String).check(Schema.isMaxLength(4)),
  }),
  entry: Schema.Literal(entry),
  dependencyInventory: Schema.Array(Schema.String),
  builtins: Schema.Array(Schema.String),
  bundle: HashRecord,
  seaConfig: Schema.Struct({
    ...HashRecord.fields,
    value: Schema.Struct({
      main: Schema.Literal(bundleName),
      mainFormat: Schema.Literal("module"),
      executable: Schema.Literal("node"),
      output: Schema.Literal(executableName),
      disableExperimentalSEAWarning: Schema.Literal(true),
      useSnapshot: Schema.Literal(false),
      useCodeCache: Schema.Literal(false),
      execArgv: Schema.Array(Schema.String),
      execArgvExtension: Schema.Literal("none"),
    }),
  }),
  upstream: Schema.Struct({
    archiveUrl: Schema.Literal(releaseOrigin + archiveName),
    checksumsUrl: Schema.Literal(releaseOrigin + "SHASUMS256.txt"),
    verification: Schema.Literal("official-https-checksum"),
    signedChecksumVerification: Schema.Literal(false),
    archive: HashRecord,
    checksumsSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$(?![\s\S])/)),
    runtime: Schema.Struct({
      ...HashRecord.fields,
      version: Schema.Literal(nodeVersion),
      platform: Schema.Literal("darwin"),
      arch: Schema.Literal("arm64"),
      sqlite: SQLiteMetadata,
    }),
  }),
  executable: Schema.Struct({
    ...HashRecord.fields,
    name: Schema.Literal(executableName),
    signature: Schema.Literal("ad-hoc-verified"),
    heldOutputObserved: Schema.Literal(true),
    argumentRefusalObserved: Schema.Literal(true),
  }),
});

export interface VerifiedHeldPreparationArtifact {
  readonly classification: "held-entry-only";
  readonly preparationCapable: false;
  readonly productionInvocation: false;
  readonly provenanceSha256: string;
  readonly provenanceBytes: number;
  readonly source: (typeof ArtifactManifest.Type)["source"];
  readonly entry: (typeof ArtifactManifest.Type)["bundle"];
  readonly seaConfig: (typeof ArtifactManifest.Type)["seaConfig"];
  readonly executable: (typeof ArtifactManifest.Type)["executable"];
  readonly node: (typeof ArtifactManifest.Type)["upstream"];
  readonly sqlite: typeof SQLiteMetadata.Type;
}

/** Re-observe final files; the returned held proof describes these bytes and grants no preparation or execution authority. */
export function verifyPreparationArtifact(directory: string): VerifiedHeldPreparationArtifact {
  return verifyPreparationArtifactAt(directory, "provenance.json");
}

function verifyPreparationArtifactAt(
  directory: string,
  provenanceName: "provenance.json" | "provenance.complete.json",
): VerifiedHeldPreparationArtifact {
  const provenancePath = NodePath.join(directory, provenanceName);
  const provenanceBytes = readFile(provenancePath, 64 * 1024);
  const manifest = Schema.decodeUnknownSync(ArtifactManifest, { onExcessProperty: "error" })(
    decode(provenanceBytes.toString("utf8")),
  );
  validateSQLiteMetadata(manifest.upstream.runtime.sqlite);
  if (
    new Set(manifest.source.buildOnlyOverlays).size !== manifest.source.buildOnlyOverlays.length ||
    manifest.source.buildOnlyOverlays.some((file) => !releaseManifests.includes(file))
  )
    throw new Error("Preparation provenance has unadmitted or duplicate release overlays.");
  if (
    manifest.bundle.size > maxBundleBytes ||
    manifest.seaConfig.size > 16 * 1024 ||
    manifest.upstream.archive.size > 128 * 1024 * 1024
  )
    throw new Error("Preparation provenance member exceeds its fixed bound.");
  if (
    encode(Object.keys(manifest.source.hashes).sort()) !== encode([...sourceFiles].sort()) ||
    encode(manifest.dependencyInventory) !== encode([`apps/server/${entry}`]) ||
    manifest.builtins.length !== 0
  )
    throw new Error("Preparation provenance has incomplete source/dependency observations.");
  const inventory = [bundleName, executableName, "provenance.json", "sea-config.json"];
  if (provenanceName === "provenance.complete.json") inventory.push(provenanceName);
  if (encode(NodeFS.readdirSync(directory).sort()) !== encode(inventory.sort()))
    throw new Error("Unexpected final preparation inventory.");
  for (const [name, expected] of [
    [bundleName, manifest.bundle],
    ["sea-config.json", manifest.seaConfig],
    [executableName, manifest.executable],
  ] as const) {
    const bytes = readFile(NodePath.join(directory, name), maxRuntimeBytes);
    if (bytes.length !== expected.size || digest(bytes) !== expected.sha256)
      throw new Error(`Preparation artifact changed: ${name}`);
    if (
      name === bundleName &&
      encode(verifyPreparationClosure(bytes.toString("utf8"))) !== encode(manifest.builtins)
    )
      throw new Error("Recorded builtin inventory differs from the emitted bundle.");
    if (name === "sea-config.json") {
      if (bytes.toString("utf8") !== `${encode(preparationSeaConfig())}\n`)
        throw new Error("Preparation SEA config bytes differ from the fixed encoding.");
      verifyPreparationSeaConfig(decode(bytes.toString("utf8")));
      if (encode(decode(bytes.toString("utf8"))) !== encode(manifest.seaConfig.value))
        throw new Error("Recorded SEA config differs from bytes.");
    }
  }
  if (!readFile(provenancePath, 64 * 1024).equals(provenanceBytes))
    throw new Error("Preparation provenance changed during verification.");
  const proof: VerifiedHeldPreparationArtifact = {
    classification: "held-entry-only",
    preparationCapable: false,
    productionInvocation: false,
    provenanceSha256: digest(provenanceBytes),
    provenanceBytes: provenanceBytes.length,
    source: manifest.source,
    entry: manifest.bundle,
    seaConfig: manifest.seaConfig,
    executable: manifest.executable,
    node: manifest.upstream,
    sqlite: manifest.upstream.runtime.sqlite,
  };
  const freeze = (value: unknown): void => {
    if (typeof value !== "object" || value === null || Object.isFrozen(value)) return;
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  };
  freeze(proof);
  return proof;
}

/** Rebuild only an empty output or a fully observed completed artifact; uncertainty is never a cleanup grant. */
export function preparePreparationOutput(directory: string): void {
  if (!NodeFS.existsSync(directory)) return;
  const stat = NodeFS.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error("Unadmitted preparation output directory.");
  if (NodeFS.readdirSync(directory).length === 0) return;
  // Verification rejects retained .build-* roots, incomplete receipts and extra members before any deletion.
  verifyPreparationArtifact(directory);
  // Invalidate completion before changing any bytes. A crash here leaves an unqualified output for explicit recovery.
  NodeFS.unlinkSync(NodePath.join(directory, "provenance.json"));
  for (const name of [bundleName, "sea-config.json", executableName])
    NodeFS.unlinkSync(NodePath.join(directory, name));
}

/** Mechanical construction shared by the held and functional producers; it issues no capability. */
async function buildPreparationNativeBytes(
  scope: PreparationBuildScope & { root: string },
  root: string,
  bundle: Buffer,
) {
  const scratch = scope.root,
    command = scope.command;
  if ((await command("/usr/bin/uname", ["-sm"], root)).toString("utf8").trim() !== "Darwin arm64")
    throw new Error("Preparation SEA production requires a native Darwin arm64 build host.");
  const checksums = await scope.download("SHASUMS256.txt", 128 * 1024);
  const archive = await scope.download(archiveName, 128 * 1024 * 1024);
  const archiveHash = verifyNodeArchive(checksums.toString("utf8"), archive);
  const archivePath = NodePath.join(scratch, archiveName);
  NodeFS.writeFileSync(archivePath, archive, { flag: "wx", mode: 0o600 });
  const member = `node-v${nodeVersion}-darwin-arm64/bin/node`;
  if (
    (await command("/usr/bin/tar", ["-tzf", archivePath, member], scratch)).toString("utf8") !==
    `${member}\n`
  )
    throw new Error("Official Node archive runtime member is not unique.");
  const runtime = await command(
    "/usr/bin/tar",
    ["-xOzf", archivePath, member],
    scratch,
    maxRuntimeBytes,
  );
  const runtimePath = NodePath.join(scratch, "node");
  NodeFS.writeFileSync(runtimePath, runtime, { flag: "wx", mode: 0o500 });
  const runtimeHash = digest(readFile(runtimePath, maxRuntimeBytes));
  if (runtimeHash !== digest(runtime))
    throw new Error("Extracted Node runtime changed before observation.");
  if (
    (await command(runtimePath, ["--version"], scratch)).toString("utf8").trim() !==
      `v${nodeVersion}` ||
    (await command("/usr/bin/lipo", ["-archs", runtimePath], scratch)).toString("utf8").trim() !==
      "arm64"
  )
    throw new Error("Observed Node runtime does not match the pinned version and architecture.");
  const sqlite = verifyPreparationSQLiteObservation(
    (
      await command(
        runtimePath,
        ["--input-type=module", "--eval", sqliteProbeScript],
        scratch,
        4096,
      )
    ).toString("utf8"),
  );
  if (digest(readFile(runtimePath, maxRuntimeBytes)) !== runtimeHash)
    throw new Error("Node runtime changed during SQLite observation.");
  NodeFS.writeFileSync(NodePath.join(scratch, bundleName), bundle, { flag: "wx", mode: 0o400 });
  const config = preparationSeaConfig();
  verifyPreparationSeaConfig(config);
  const configBytes = `${encode(config)}\n`;
  NodeFS.writeFileSync(NodePath.join(scratch, "sea-config.json"), configBytes, {
    flag: "wx",
    mode: 0o400,
  });
  // Node 26.8.2 direct SEA generation: https://nodejs.org/download/release/v26.8.2/docs/api/single-executable-applications.html
  await command(runtimePath, ["--build-sea", "sea-config.json"], scratch);
  const executable = NodePath.join(scratch, executableName);
  await command(
    "/usr/bin/codesign",
    ["--force", "--sign", "-", "--timestamp=none", executable],
    scratch,
  );
  await command("/usr/bin/codesign", ["--verify", "--strict", executable], scratch);
  NodeFS.chmodSync(executable, 0o500);
  return {
    checksums,
    archive,
    archiveHash,
    runtime,
    runtimePath,
    runtimeHash,
    sqlite,
    config,
    configBytes,
    executable,
  };
}

/**
 * Closed held packaging only. The entry's dispatch folds to its held refusal, so the worker
 * branch and its imports must leave the bundle; the closure checks below prove that they did.
 * Candidate and producer packs never set it, so functional builds keep the worker branch.
 */
export const preparationHeldEntryDefine = Object.freeze({
  "import.meta.jonesPreparationHeldEntry": "true",
});

/** Constructed only by the pack hook from its observed emitted graph. No caller-supplied provenance is admitted. */
export function offlinePreparationPack(
  rootPath: string,
  options: { readonly signal?: AbortSignal } = {},
) {
  const root = NodeFS.realpathSync(rootPath);
  const outDir = NodePath.join(root, outRelative);
  let source: Awaited<ReturnType<typeof observeSource>> | undefined;
  let emitted:
    | { sha256: string; size: number; builtins: readonly string[]; modules: readonly string[] }
    | undefined;
  const plugin: Plugin = {
    name: "jones:closed-offline-preparation",
    async buildStart() {
      source = await withPreparationBuildScope(
        NodeOS.tmpdir(),
        "jones-preparation-source-",
        options.signal,
        (scope) => observeSource(root, scope.command),
      );
      options.signal?.throwIfAborted();
      preparePreparationOutput(outDir);
    },
    generateBundle(_options, bundle) {
      const outputs = Object.values(bundle);
      const chunk = outputs[0];
      if (
        outputs.length !== 1 ||
        !chunk ||
        chunk.type !== "chunk" ||
        chunk.fileName !== bundleName ||
        !chunk.isEntry ||
        chunk.dynamicImports.length ||
        chunk.map
      )
        throw new Error("Preparation requires exactly one entry and no chunks, maps or assets.");
      const builtins = verifyPreparationClosure(chunk.code);
      if (chunk.imports.some((id) => !NodeModule.isBuiltin(id)))
        throw new Error("Unbundled preparation dependency.");
      const modules = Object.entries(chunk.modules)
        .filter(([, info]) => info.renderedLength > 0)
        .map(([id]) => {
          const relative = NodePath.relative(root, id).replaceAll("\\", "/");
          if (
            relative.startsWith("../") ||
            NodePath.isAbsolute(relative) ||
            /(?:^|\/)(?:Migrations|providers?|fixtures?)(?:\/|$)|\.node$/.test(relative)
          )
            throw new Error(`Unadmitted preparation module: ${relative}`);
          return relative;
        })
        .sort();
      // This revision proves only the held entry. Broader preparation dependencies require a new reviewed inventory.
      if (encode(modules) !== encode([`apps/server/${entry}`]) || builtins.length !== 0)
        throw new Error("Unqualified dependency survived the held preparation entry build.");
      emitted = {
        sha256: digest(chunk.code),
        size: Buffer.byteLength(chunk.code),
        builtins,
        modules,
      };
    },
  };
  return {
    entry: { offlinePreparationEntry: entry },
    outDir,
    format: "esm" as const,
    platform: "node" as const,
    define: { ...preparationHeldEntryDefine },
    sourcemap: false,
    // Framework cleanup must never remove custody retained by a previous producer.
    clean: false,
    dts: false,
    minify: false,
    outExtensions: () => ({ js: ".mjs" }),
    outputOptions: { codeSplitting: false },
    deps: {
      alwaysBundle: (id: string) => !NodeModule.isBuiltin(id),
      neverBundle: (id: string) => NodeModule.isBuiltin(id),
      onlyBundle: false as const,
    },
    plugins: [plugin],
    async onSuccess() {
      const sourceSnapshot = source;
      const emittedSnapshot = emitted;
      if (!sourceSnapshot || !emittedSnapshot)
        throw new Error("Missing observed preparation source or emitted graph.");
      if (encode(NodeFS.readdirSync(outDir).sort()) !== encode([bundleName]))
        throw new Error("Unexpected preparation output inventory.");
      const bundle = readFile(NodePath.join(outDir, bundleName), maxBundleBytes);
      if (digest(bundle) !== emittedSnapshot.sha256)
        throw new Error("Preparation bundle changed after closure verification.");
      const provenancePath = NodePath.join(outDir, "provenance.json");
      NodeFS.writeFileSync(
        provenancePath,
        `${encode({ observation: "incomplete", classification: "unusable", productionInvocation: false })}\n`,
        { flag: "wx", mode: 0o600 },
      );
      const completed = await withPreparationBuildScope(
        outDir,
        ".build-",
        options.signal,
        async (scope) => {
          const scratch = scope.root;
          const command = scope.command;
          if (encode(await observeSource(root, command)) !== encode(sourceSnapshot))
            throw new Error("Preparation source changed during bundling.");
          const {
            checksums,
            archive,
            archiveHash,
            runtime,
            runtimePath,
            runtimeHash,
            sqlite,
            config,
            configBytes,
            executable,
          } = await buildPreparationNativeBytes(scope, root, bundle);
          verifyPreparationHeldOutput(
            (await command(executable, [], scratch, 4096)).toString("utf8"),
          );
          await verifyArgumentRefusal(command, executable, scratch);
          const finalBytes = readFile(executable, maxRuntimeBytes);
          if (digest(readFile(runtimePath, maxRuntimeBytes)) !== runtimeHash)
            throw new Error("Node build runtime changed during use.");
          if (encode(await observeSource(root, command)) !== encode(sourceSnapshot))
            throw new Error("Preparation source changed during native build.");
          const provenance = {
            schemaVersion: 1,
            classification: "held-entry-only",
            preparationCapable: false,
            productionInvocation: false,
            observation: "complete",
            source: sourceSnapshot,
            entry,
            dependencyInventory: emittedSnapshot.modules,
            builtins: emittedSnapshot.builtins,
            bundle: { sha256: emittedSnapshot.sha256, size: emittedSnapshot.size },
            seaConfig: {
              value: config,
              sha256: digest(configBytes),
              size: Buffer.byteLength(configBytes),
            },
            upstream: {
              archiveUrl: releaseOrigin + archiveName,
              checksumsUrl: releaseOrigin + "SHASUMS256.txt",
              verification: "official-https-checksum",
              signedChecksumVerification: false,
              archive: { sha256: archiveHash, size: archive.length },
              checksumsSha256: digest(checksums),
              runtime: {
                version: nodeVersion,
                platform: "darwin",
                arch: "arm64",
                sha256: runtimeHash,
                size: runtime.length,
                sqlite,
              },
            },
            executable: {
              name: executableName,
              sha256: digest(finalBytes),
              size: finalBytes.length,
              signature: "ad-hoc-verified",
              heldOutputObserved: true,
              argumentRefusalObserved: true,
            },
          };
          return { bytes: finalBytes, configBytes, provenance };
        },
      );
      options.signal?.throwIfAborted();
      NodeFS.writeFileSync(NodePath.join(outDir, executableName), completed.bytes, {
        flag: "wx",
        mode: 0o500,
      });
      NodeFS.writeFileSync(NodePath.join(outDir, "sea-config.json"), completed.configBytes, {
        flag: "wx",
        mode: 0o400,
      });
      // Cancellation, failed close or failed cleanup cannot publish a complete receipt.
      const completePath = NodePath.join(outDir, "provenance.complete.json");
      NodeFS.writeFileSync(completePath, `${encode(completed.provenance)}\n`, {
        flag: "wx",
        mode: 0o400,
      });
      verifyPreparationArtifactAt(outDir, "provenance.complete.json");
      NodeFS.renameSync(completePath, provenancePath);
    },
  };
}

const candidateRelative = "apps/server/dist-exe/preparation-candidate";
const candidateReportName = "candidate-graph.json";
const producerCandidateEntry = "scripts/jones/updates/offline-preparation-producer-entry.ts";
const producerCandidateRelative = "apps/server/dist-exe/preparation-producer-candidate";
const candidateModuleLimit = 4096;
const candidateReportLimit = 16 * 1024 * 1024;

/** Candidate inspection describes syntax; it never approves or executes a loader. */
export function inspectPreparationCandidateJavaScript(source: string) {
  return inspectPreparationJavaScript(source);
}

function inspectPreparationJavaScript(source: string, producerFileName?: string) {
  const ts = preparationTypeScript();
  const producer = producerFileName !== undefined;
  if (Buffer.byteLength(source) > maxBundleBytes) throw new Error("Candidate entry exceeds bound.");
  const file = ts.createSourceFile(
    producerFileName ?? bundleName,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.JS,
  );
  const diagnostics = (
    file as TypeScript.SourceFile & { parseDiagnostics: readonly TypeScript.Diagnostic[] }
  ).parseDiagnostics;
  if (diagnostics.length) throw new Error("Candidate entry is not valid JavaScript.");
  const location = (expression: TypeScript.Node) => ({
    file: file.fileName,
    unit: "utf16-code-units" as const,
    origin: 0 as const,
    start: expression.getStart(file),
    end: expression.end,
    endExclusive: true as const,
  });
  const imports: {
    kind: "static" | "dynamic" | "require" | "process-builtin";
    specifier: string | null;
    computed: boolean;
    builtin: boolean;
    expressionSha256: string;
    location?: ReturnType<typeof location>;
  }[] = [];
  const loaderHazards: { kind: string; expressionSha256: string }[] = [];
  let loaderHazardCount = 0;
  const record = (
    kind: "static" | "dynamic" | "require" | "process-builtin",
    node: TypeScript.Node | undefined,
    expression: TypeScript.Node,
  ) => {
    const specifier = node && ts.isStringLiteralLike(node) ? node.text : null;
    if (specifier !== null && Buffer.byteLength(specifier) > 4096)
      throw new Error("Candidate import exceeds bound.");
    if (producer && imports.length >= candidateModuleLimit)
      throw new Error("Producer candidate imports exceed bound.");
    imports.push({
      kind,
      specifier,
      computed: specifier === null,
      builtin: specifier !== null && NodeModule.isBuiltin(specifier),
      expressionSha256: digest(expression.getText(file)),
      ...(producer ? { location: location(expression) } : {}),
    });
  };
  const visit = (node: TypeScript.Node): void => {
    if (!producer && imports.length + loaderHazards.length > candidateModuleLimit)
      throw new Error("Candidate loader observations exceed bound.");
    if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier)
      record("static", node.moduleSpecifier, node);
    if (ts.isCallExpression(node)) {
      if (node.expression.kind === ts.SyntaxKind.ImportKeyword)
        record("dynamic", node.arguments[0], node);
      if (ts.isIdentifier(node.expression) && /(?:^|_)require(?:_|$)/.test(node.expression.text))
        record("require", node.arguments[0], node);
      const access = node.expression;
      if (
        (ts.isPropertyAccessExpression(access) || ts.isElementAccessExpression(access)) &&
        ((ts.isIdentifier(access.expression) && access.expression.text === "process") ||
          (ts.isPropertyAccessExpression(access.expression) &&
            ts.isIdentifier(access.expression.expression) &&
            access.expression.expression.text === "globalThis" &&
            access.expression.name.text === "process")) &&
        (ts.isPropertyAccessExpression(access)
          ? access.name.text === "getBuiltinModule"
          : ts.isStringLiteralLike(access.argumentExpression) &&
            access.argumentExpression.text === "getBuiltinModule")
      )
        record("process-builtin", node.arguments[0], node);
    }
    if (
      ts.isElementAccessExpression(node) ||
      (ts.isIdentifier(node) && forbiddenNames.has(node.text))
    ) {
      loaderHazardCount += 1;
      // Producer reflection samples may truncate; traversal must still reach every later import.
      if (!producer || loaderHazards.length < candidateModuleLimit)
        loaderHazards.push({
          kind: ts.isElementAccessExpression(node)
            ? "computed-access"
            : "loader-or-reflection-name",
          expressionSha256: digest(node.getText(file)),
        });
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return {
    imports,
    loaderHazards,
    scannerQualification: "syntax-observation-only" as const,
    ...(producer
      ? {
          importsComplete: true,
          importCount: imports.length,
          importLimit: candidateModuleLimit,
          loaderHazardCount,
          loaderHazardSampleLimit: candidateModuleLimit,
          loaderHazardsTruncated: loaderHazardCount > loaderHazards.length,
        }
      : {}),
  };
}

/** Final syntax and bundler metadata are separate observations, never interchangeable loader approval. */
export function inspectPreparationCandidateImports(
  source: string,
  metadata: { readonly imports: readonly string[]; readonly dynamicImports: readonly string[] },
) {
  return inspectPreparationImports(source, metadata);
}

function inspectPreparationImports(
  source: string,
  metadata: { readonly imports: readonly string[]; readonly dynamicImports: readonly string[] },
  producerFileName?: string,
) {
  const syntax = inspectPreparationJavaScript(source, producerFileName);
  const unique = (values: readonly string[]) => [...new Set(values)].sort();
  const reconcile = (kind: "static" | "dynamic", values: readonly string[]) => {
    const emitted = unique(
      syntax.imports.flatMap((item) =>
        item.kind === kind && item.specifier !== null ? [item.specifier] : [],
      ),
    );
    const reported = unique(values);
    return {
      emitted,
      reported,
      metadataOnly: reported.filter((value) => !emitted.includes(value)),
      syntaxOnly: emitted.filter((value) => !reported.includes(value)),
    };
  };
  return {
    syntax,
    static: reconcile("static", metadata.imports),
    dynamic: reconcile("dynamic", metadata.dynamicImports),
    // This records spelling, including shadowed `process`; it makes no runtime identity claim.
    processBuiltinCalls: syntax.imports.filter((item) => item.kind === "process-builtin"),
    qualification: "unqualified" as const,
  };
}

/** Generated region bytes remain review input, not an exemption for bundler helpers. */
export function inspectPreparationGeneratedRuntime(source: string) {
  const ts = preparationTypeScript();
  if (Buffer.byteLength(source) > maxBundleBytes) throw new Error("Candidate entry exceeds bound.");
  const file = ts.createSourceFile(
    bundleName,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.JS,
  );
  const comments = new Map<number, TypeScript.CommentRange>();
  for (const statement of [...file.statements, file.endOfFileToken])
    for (const comment of ts.getLeadingCommentRanges(source, statement.pos) ?? [])
      comments.set(comment.pos, comment);
  const ranges = [...comments.values()].sort((left, right) => left.pos - right.pos);
  const starts = ranges.filter(
    (range) => source.slice(range.pos, range.end) === "//#region \\0rolldown/runtime.js",
  );
  if (starts.length !== 1)
    return {
      availability: "unavailable" as const,
      reason: starts.length === 0 ? "generated-region-not-observed" : "generated-region-ambiguous",
    };
  const start = starts[0]!;
  const end = ranges.find(
    (range) => range.pos > start.end && source.slice(range.pos, range.end).startsWith("//#"),
  );
  if (!end || source.slice(end.pos, end.end) !== "//#endregion")
    return {
      availability: "unavailable" as const,
      reason: "generated-region-boundary-not-observed",
    };
  const bytes = source.slice(start.end, end.pos);
  return {
    availability: "observed" as const,
    evidence: "emitted-top-level-region" as const,
    sha256: digest(bytes),
    size: Buffer.byteLength(bytes),
    startByte: Buffer.byteLength(source.slice(0, start.end)),
    endByte: Buffer.byteLength(source.slice(0, end.pos)),
    qualification: "unqualified" as const,
  };
}

/** A virtual or tree-shaken module without exposed code has no invented content hash. */
export function inspectPreparationCandidateCode(code: unknown) {
  if (typeof code !== "string")
    return { availability: "unavailable" as const, reason: "bundler-code-not-exposed" };
  const size = Buffer.byteLength(code);
  if (size > maxBundleBytes) throw new Error("Candidate module code exceeds bound.");
  return { availability: "observed" as const, sha256: digest(code), size };
}

/** Rolldown ModuleInfo omits external status; use only its actual output graph evidence. */
export function inspectPreparationCandidateExternal(
  id: string,
  renderedIds: ReadonlySet<string>,
  emittedImportIds: ReadonlySet<string>,
) {
  const rendered = renderedIds.has(id),
    emitted = emittedImportIds.has(id);
  if (rendered && emitted) throw new Error("Candidate external observations conflict.");
  if (emitted) return { external: true as const, externalEvidence: "emitted-import" as const };
  if (rendered) return { external: false as const, externalEvidence: "rendered-module" as const };
  return { external: null, externalEvidence: "unavailable-module-info-api" as const };
}

/** Capture every existing ancestor before allowing the builder to create an output child. */
function candidateDirectoryChain(directory: string) {
  if (!NodePath.isAbsolute(directory) || NodePath.normalize(directory) !== directory)
    throw new Error("Candidate output requires a canonical absolute path; evidence is retained.");
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("Candidate directory ownership is unavailable.");
  const chain: { path: string; stat: NodeFS.Stats }[] = [];
  let current = NodePath.parse(directory).root;
  const paths = [current];
  for (const part of directory.slice(current.length).split(NodePath.sep).filter(Boolean)) {
    current = NodePath.join(current, part);
    paths.push(current);
  }
  let missing: string | undefined;
  for (const path of paths) {
    let stat: NodeFS.Stats;
    try {
      stat = NodeFS.lstatSync(path);
    } catch (cause) {
      if (
        typeof cause === "object" &&
        cause !== null &&
        "code" in cause &&
        cause.code === "ENOENT"
      ) {
        missing = path;
        break;
      }
      throw cause;
    }
    // System scratch ancestors may be sticky; the output itself must remain privately writable.
    const trustedStickyParent = path !== directory && stat.uid === 0 && (stat.mode & 0o1000) !== 0;
    if (
      !stat.isDirectory() ||
      stat.isSymbolicLink() ||
      (stat.uid !== uid && stat.uid !== 0) ||
      ((stat.mode & 0o022) !== 0 && !trustedStickyParent) ||
      (path === directory && stat.uid !== uid)
    )
      throw new Error("Candidate output ancestor is aliased or unowned; evidence is retained.");
    chain.push({ path, stat });
  }
  return { chain, missing };
}

/** Prior candidate data is never a cleanup grant, including a report that claims completeness. */
export function requireEmptyPreparationCandidateOutput(directory: string): () => void {
  const captured = candidateDirectoryChain(directory);
  if (captured.missing === undefined && NodeFS.readdirSync(directory).length !== 0)
    throw new Error("Candidate output must be fresh or empty; existing evidence is retained.");
  return () => {
    const current = candidateDirectoryChain(directory);
    if (
      captured.missing !== current.missing ||
      captured.chain.length !== current.chain.length ||
      captured.chain.some(({ path, stat }, index) => {
        const actual = current.chain[index]!;
        return (
          path !== actual.path ||
          stat.dev !== actual.stat.dev ||
          stat.ino !== actual.stat.ino ||
          stat.uid !== actual.stat.uid ||
          stat.mode !== actual.stat.mode
        );
      })
    )
      throw new Error("Candidate output directory identity changed; evidence is retained.");
  };
}

function candidateId(root: string, id: string): string {
  if (Buffer.byteLength(id) > 4096) throw new Error("Candidate module identifier exceeds bound.");
  const relative = NodePath.relative(root, id).replaceAll("\\", "/");
  return NodePath.isAbsolute(id) && !relative.startsWith("../") && !NodePath.isAbsolute(relative)
    ? relative
    : id;
}

// Installed package files may be hardlinked by the package manager. Observe
// their bounded bytes without relaxing the single-link artifact writer boundary.
function readCandidateEvidence(file: string, limit: number): Buffer {
  const fd = NodeFS.openSync(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const before = NodeFS.fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > BigInt(limit) || before.nlink < 1n)
      throw new Error("Candidate file is not a bounded regular file.");
    const buffer = Buffer.alloc(Number(before.size) + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = NodeFS.readSync(fd, buffer, offset, buffer.length - offset, offset);
      if (count === 0) break;
      offset += count;
    }
    const after = NodeFS.fstatSync(fd, { bigint: true });
    const current = NodeFS.lstatSync(file, { bigint: true });
    if (
      offset !== Number(before.size) ||
      (["dev", "ino", "size", "mtimeNs", "ctimeNs", "mode", "nlink"] as const).some(
        (key) => before[key] !== after[key] || before[key] !== current[key],
      )
    )
      throw new Error("Candidate file changed during observation.");
    return buffer.subarray(0, offset);
  } finally {
    NodeFS.closeSync(fd);
  }
}

function candidateFile(
  root: string,
  id: string,
  limit = 8 * 1024 * 1024,
  installedEvidence = false,
) {
  const unavailable = (reason: string) => ({ availability: "unavailable" as const, reason });
  if (!NodePath.isAbsolute(id) || id.includes("\0") || id.includes("?"))
    return unavailable("non-file-module-id");
  try {
    const actual = NodeFS.realpathSync(id);
    const relative = NodePath.relative(root, actual);
    if (relative.startsWith(".." + NodePath.sep) || NodePath.isAbsolute(relative))
      return unavailable("outside-bound-source-root");
    const bytes = installedEvidence
      ? readCandidateEvidence(actual, limit)
      : readFile(actual, limit);
    return {
      availability: "observed" as const,
      path: candidateId(root, actual),
      sha256: digest(bytes),
      size: bytes.length,
    };
  } catch {
    return unavailable("file-content-not-observed");
  }
}

function candidatePackage(root: string, id: string) {
  if (!NodePath.isAbsolute(id) || id.includes("\0") || id.includes("?"))
    return { availability: "unavailable" as const, reason: "non-file-module-id" };
  let parent = NodePath.dirname(id);
  for (let depth = 0; depth < 32; depth++) {
    const relative = NodePath.relative(root, parent);
    if (relative.startsWith(".." + NodePath.sep) || NodePath.isAbsolute(relative)) break;
    const manifestPath = NodePath.join(parent, "package.json");
    if (NodeFS.existsSync(manifestPath)) {
      const file = candidateFile(root, manifestPath, 1024 * 1024, true);
      if (file.availability === "unavailable") return file;
      try {
        const manifestBytes = readCandidateEvidence(NodePath.join(root, file.path), 1048576);
        if (digest(manifestBytes) !== file.sha256)
          return {
            availability: "unavailable" as const,
            reason: "package-manifest-changed",
            manifest: file,
          };
        const manifest = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
          decode(manifestBytes.toString("utf8")),
        );
        if (
          typeof manifest.name !== "string" ||
          manifest.name.length > 256 ||
          (manifest.version !== undefined &&
            (typeof manifest.version !== "string" || manifest.version.length > 128))
        )
          return {
            availability: "unavailable" as const,
            reason: "package-identity-not-observed",
            manifest: file,
          };
        return {
          availability: "observed" as const,
          name: manifest.name,
          version: manifest.version ?? null,
          versionObservation: manifest.version === undefined ? "not-declared" : "observed",
          manifest: file,
        };
      } catch {
        return {
          availability: "unavailable" as const,
          reason: "package-manifest-not-decoded",
          manifest: file,
        };
      }
    }
    if (parent === root) break;
    parent = NodePath.dirname(parent);
  }
  return { availability: "unavailable" as const, reason: "package-manifest-not-found" };
}

/** Resolver attempts are recorded from concrete installed entries; no package code is loaded. */
export function inspectPreparationCandidateTools(
  root: string,
  resolverBase: string | URL = import.meta.url,
) {
  const names = ["vite-plus", "typescript-legacy", "effect", "rolldown", "tsdown"];
  const bases: (string | URL)[] = [resolverBase];
  const visited = new Set<string>();
  const attempts: {
    name: string;
    resolverBase: string;
    resolvedId?: string;
    entry?: ReturnType<typeof candidateFile>;
    package?: ReturnType<typeof candidatePackage>;
    reason?: string;
  }[] = [];
  // The pack entry can expose dependencies hidden from the repository's resolver.
  const specifiers = [...names, "vite-plus/pack"];
  for (let index = 0; index < bases.length && index < 16; index += 1) {
    const base = bases[index]!;
    const baseKey = String(base);
    if (visited.has(baseKey)) continue;
    visited.add(baseKey);
    const baseLabel = index === 0 ? "build-helper-module" : candidateId(root, baseKey);
    const resolver = NodeModule.createRequire(base);
    for (const name of specifiers) {
      try {
        const resolved = resolver.resolve(name);
        const entryEvidence = candidateFile(root, resolved, maxBundleBytes, true);
        const packageEvidence = candidatePackage(root, resolved);
        attempts.push({
          name,
          resolverBase: baseLabel,
          resolvedId: candidateId(root, resolved),
          entry: entryEvidence,
          package: packageEvidence,
        });
        if (
          entryEvidence.availability === "observed" &&
          !bases.includes(resolved) &&
          bases.length < 16
        )
          bases.push(resolved);
      } catch {
        attempts.push({ name, resolverBase: baseLabel, reason: "tool-package-not-resolved" });
      }
    }
  }
  return {
    qualification: "unqualified" as const,
    resolverBasis: "build-helper-and-resolved-installed-entries" as const,
    attempts,
    unresolved: names.filter(
      (name) =>
        !attempts.some(
          (attempt) =>
            attempt.name === name &&
            attempt.entry?.availability === "observed" &&
            attempt.package?.availability === "observed" &&
            typeof attempt.package.version === "string",
        ),
    ),
  };
}

function candidatePatches(root: string) {
  const workspace = readFile(NodePath.join(root, "pnpm-workspace.yaml"), 8 * 1024 * 1024);
  const lines = workspace.toString("utf8").split(/\r?\n/);
  const start = lines.indexOf("patchedDependencies:");
  const entries: {
    selector: string;
    declaredPath: string;
    content: ReturnType<typeof candidateFile>;
  }[] = [];
  let complete = start >= 0;
  if (start >= 0)
    for (const line of lines.slice(start + 1)) {
      if (/^\S/.test(line)) break;
      if (/^\s*(?:#.*)?$/.test(line)) continue;
      const match = /^  (?:"([^"]+)"|([^:]+)): (patches\/[^\s]+)\s*$/.exec(line);
      if (!match || entries.length >= 128) {
        complete = false;
        continue;
      }
      entries.push({
        selector: match[1] ?? match[2]!,
        declaredPath: match[3]!,
        content: candidateFile(root, NodePath.resolve(root, match[3]!)),
      });
    }
  return {
    configurationSha256: digest(workspace),
    mappingObservation: complete ? "observed-simple-mapping" : "incomplete-or-unsupported-syntax",
    applicationVerified: false,
    entries,
  };
}

/** A separate non-executing build path; its report cannot satisfy the held-artifact verifier or BUNDLE3. */
export function offlinePreparationCandidatePack(
  rootPath: string,
  options: { readonly signal?: AbortSignal } = {},
) {
  return preparationCandidatePack(rootPath, "sea", options);
}

/** Collect the producer's emitted graph without importing it or issuing a preparation handle. */
export function offlinePreparationProducerCandidatePack(
  rootPath: string,
  options: { readonly signal?: AbortSignal } = {},
) {
  return preparationCandidatePack(rootPath, "producer", options);
}

function preparationCandidatePack(
  rootPath: string,
  target: "sea" | "producer",
  options: { readonly signal?: AbortSignal },
) {
  const producer = target === "producer";
  const root = NodeFS.realpathSync(rootPath);
  const outDir = NodePath.join(root, producer ? producerCandidateRelative : candidateRelative);
  const outputName = producer ? "offlinePreparationProducer.mjs" : bundleName;
  const reportName = producer ? "producer-candidate-graph.json" : candidateReportName;
  const inputEntry = producer ? NodePath.join(root, producerCandidateEntry) : entry;
  let source: Awaited<ReturnType<typeof observeSource>> | undefined;
  let producerSources: ReturnType<typeof inspectPreparationProducerCandidateSources> | undefined;
  let verifyProducerGraph: (() => void) | undefined;
  let report: unknown;
  let verifyOutput: (() => void) | undefined;
  let outputHashes: { fileName: string; sha256: string; size: number }[] = [];
  const verifyProducerSources = () => {
    if (
      producer &&
      (!producerSources ||
        encode(inspectPreparationProducerCandidateSources(root)) !== encode(producerSources))
    )
      throw new Error("Producer candidate source inputs changed during collection.");
  };
  const plugin: Plugin = {
    name: producer
      ? "jones:unqualified-preparation-producer-candidate"
      : "jones:unqualified-preparation-candidate",
    async buildStart() {
      if (producer) options.signal?.throwIfAborted();
      const verifyInitialOutput = requireEmptyPreparationCandidateOutput(outDir);
      if (producer) producerSources = inspectPreparationProducerCandidateSources(root);
      source = await withPreparationBuildScope(
        NodeOS.tmpdir(),
        "jones-preparation-candidate-source-",
        options.signal,
        (scope) => observeSource(root, scope.command),
      );
      options.signal?.throwIfAborted();
      verifyProducerSources();
      verifyInitialOutput();
      requireEmptyPreparationCandidateOutput(outDir);
      NodeFS.mkdirSync(outDir, { recursive: true, mode: 0o700 });
      verifyOutput = requireEmptyPreparationCandidateOutput(outDir);
    },
    generateBundle(_options, bundle) {
      if (!source || !verifyOutput) throw new Error("Missing candidate source observation.");
      verifyProducerSources();
      verifyOutput();
      const outputs = Object.values(bundle);
      const chunks = outputs.filter((item) => item.type === "chunk");
      const chunk = chunks[0];
      if (
        outputs.length > 128 ||
        chunks.length !== 1 ||
        !chunk ||
        !chunk.isEntry ||
        chunk.fileName !== outputName
      )
        throw new Error("Candidate requires exactly one fixed ESM entry chunk.");
      const ids = [...this.getModuleIds()].sort();
      if (ids.length > candidateModuleLimit)
        throw new Error("Candidate graph exceeds module bound.");
      let importEvidence: ReturnType<typeof inspectPreparationCandidateImports> | undefined;
      let unavailableSyntax: { availability: "unavailable"; reason: string } | undefined;
      try {
        importEvidence = inspectPreparationImports(
          chunk.code,
          chunk,
          producer ? outputName : undefined,
        );
      } catch (cause) {
        if (!producer) throw cause;
        unavailableSyntax = {
          availability: "unavailable",
          reason: cause instanceof Error ? cause.message : "syntax-observation-failed",
        };
      }
      const renderedIds = new Set(Object.keys(chunk.modules));
      const emittedImportIds = new Set([
        ...(importEvidence?.static.emitted ?? []),
        ...(importEvidence?.dynamic.emitted ?? []),
      ]);
      const externalObservation = (id: string) =>
        inspectPreparationCandidateExternal(id, renderedIds, emittedImportIds);
      const packages = new Map<string, ReturnType<typeof candidatePackage>>();
      const graphFiles: {
        id: string;
        source: ReturnType<typeof candidateFile>;
        package: ReturnType<typeof candidatePackage>;
      }[] = [];
      let observedCodeBytes = 0;
      const codeRecord = (code: unknown) => {
        const observation = inspectPreparationCandidateCode(code);
        if (observation.availability === "observed") observedCodeBytes += observation.size;
        if (observedCodeBytes > 128 * 1024 * 1024)
          throw new Error("Candidate code observations exceed bound.");
        return observation;
      };
      const modules = ids.map((id) => {
        const info = this.getModuleInfo(id);
        const rendered = chunk.modules[id];
        const packageObservation = candidatePackage(root, id);
        const sourceContent = producer
          ? candidateFile(root, id, maxBundleBytes, true)
          : candidateFile(root, id);
        if (producer) graphFiles.push({ id, source: sourceContent, package: packageObservation });
        const packageKey =
          "manifest" in packageObservation &&
          packageObservation.manifest?.availability === "observed"
            ? packageObservation.manifest.path
            : candidateId(root, id);
        packages.set(packageKey, packageObservation);
        const edge = (target: string) => ({
          id: candidateId(root, target),
          builtin: NodeModule.isBuiltin(target),
          ...externalObservation(target),
        });
        return {
          id: candidateId(root, id),
          virtualId: id.includes("\0"),
          builtin: NodeModule.isBuiltin(id),
          ...externalObservation(id),
          entry: info?.isEntry ?? null,
          informationAvailable: info !== null && info !== undefined,
          sourceContent,
          bundlerContent: codeRecord((info as { code?: unknown } | null)?.code),
          rendered: rendered !== undefined,
          renderedLength: rendered?.renderedLength ?? null,
          renderedContent: codeRecord((rendered as { code?: unknown } | undefined)?.code),
          staticImports: info?.importedIds.map(edge) ?? null,
          dynamicImports: info?.dynamicallyImportedIds.map(edge) ?? null,
          packageObservation: packageKey,
        };
      });
      if (producer)
        verifyProducerGraph = () => {
          for (const observed of graphFiles)
            if (
              encode(candidateFile(root, observed.id, maxBundleBytes, true)) !==
                encode(observed.source) ||
              encode(candidatePackage(root, observed.id)) !== encode(observed.package)
            )
              throw new Error("Producer candidate graph inputs changed during collection.");
        };
      outputHashes = outputs.map((item) => {
        if (
          item.fileName.includes("/") ||
          item.fileName.includes("\\") ||
          [".", "..", reportName].includes(item.fileName)
        )
          throw new Error("Unadmitted candidate output name.");
        const bytes =
          item.type === "chunk"
            ? Buffer.from(item.code)
            : typeof item.source === "string"
              ? Buffer.from(item.source)
              : Buffer.from(item.source);
        if (bytes.length > maxBundleBytes) throw new Error("Candidate output exceeds bound.");
        return { fileName: item.fileName, sha256: digest(bytes), size: bytes.length };
      });
      const tools = inspectPreparationCandidateTools(root);
      const meta = this.meta as {
        rollupVersion?: string;
        rolldownVersion?: string;
        watchMode?: boolean;
      };
      report = {
        schemaVersion: 1,
        classification: producer
          ? "preparation-producer-candidate-graph"
          : "preparation-candidate-graph",
        qualification: "unqualified",
        executed: false,
        preparationCapable: false,
        productionInvocation: false,
        source: producer
          ? {
              repository: source.repository,
              sha: source.sha,
              tree: source.tree,
              version: source.version,
              buildOnlyOverlays: source.buildOnlyOverlays,
              inputs: producerSources!.producerInputs,
            }
          : source,
        entry: producer ? producerCandidateEntry : entry,
        ...(producer
          ? {
              emittedProducerExecuted: false,
              observation: "partial",
              completeDependencyClosure: false,
              loadedCollectorBytesAttested: false,
              lateQualifierSources: producerSources!.lateQualifierInputs,
            }
          : {}),
        emitted: {
          format: "esm",
          fileName: chunk.fileName,
          ...(producer ? { sha256: digest(chunk.code), size: Buffer.byteLength(chunk.code) } : {}),
          imports: chunk.imports,
          dynamicImports: chunk.dynamicImports,
          mapPresent: chunk.map !== null && chunk.map !== undefined,
          mapContent: chunk.map
            ? inspectPreparationCandidateCode(encode(chunk.map))
            : { availability: "absent" },
          assets: outputs.filter((item) => item.type === "asset").map((item) => item.fileName),
          files: outputHashes,
          syntax: importEvidence?.syntax ?? unavailableSyntax,
          importEvidence: importEvidence ?? unavailableSyntax,
          generatedRuntime: inspectPreparationGeneratedRuntime(chunk.code),
        },
        modules,
        packages: [...packages.entries()].map(([id, observation]) => ({ id, observation })),
        toolContext: {
          rollupVersion: meta.rollupVersion ?? null,
          rolldownVersion: meta.rolldownVersion ?? null,
          watchMode: meta.watchMode ?? null,
        },
        tools,
        patches: candidatePatches(root),
        missingContentQualified: false,
      };
      if (Buffer.byteLength(encode(report)) > candidateReportLimit)
        throw new Error("Candidate report exceeds bound.");
      verifyProducerSources();
      verifyProducerGraph?.();
      verifyOutput();
    },
    writeBundle() {
      if (!verifyOutput) throw new Error("Missing candidate output custody.");
      verifyOutput();
    },
  };
  return {
    entry: { [producer ? "offlinePreparationProducer" : "offlinePreparationEntry"]: inputEntry },
    outDir,
    format: "esm" as const,
    platform: "node" as const,
    sourcemap: false,
    clean: false,
    dts: false,
    minify: false,
    outExtensions: () => ({ js: ".mjs" }),
    outputOptions: { codeSplitting: false },
    deps: {
      alwaysBundle: (id: string) => !NodeModule.isBuiltin(id),
      neverBundle: (id: string) => NodeModule.isBuiltin(id),
      onlyBundle: false as const,
    },
    plugins: [plugin],
    async onSuccess() {
      if (!source || !verifyOutput || report === undefined || outputHashes.length === 0)
        throw new Error("Candidate graph observation is incomplete.");
      verifyOutput();
      await withPreparationBuildScope(
        NodeOS.tmpdir(),
        "jones-preparation-candidate-source-",
        options.signal,
        async (scope) => {
          if (encode(await observeSource(root, scope.command)) !== encode(source))
            throw new Error("Candidate source changed during bundling.");
        },
      );
      options.signal?.throwIfAborted();
      verifyProducerSources();
      verifyProducerGraph?.();
      verifyOutput();
      if (
        encode(NodeFS.readdirSync(outDir).sort()) !==
        encode(outputHashes.map((item) => item.fileName).sort())
      )
        throw new Error("Candidate output inventory changed before reporting.");
      for (const item of outputHashes) {
        const bytes = readFile(NodePath.join(outDir, item.fileName), maxBundleBytes);
        if (bytes.length !== item.size || digest(bytes) !== item.sha256)
          throw new Error("Candidate emitted bytes changed before reporting.");
      }
      const bytes = `${encode(report)}\n`;
      if (Buffer.byteLength(bytes) > candidateReportLimit)
        throw new Error("Candidate report exceeds bound.");
      verifyOutput();
      NodeFS.writeFileSync(NodePath.join(outDir, reportName), bytes, {
        flag: "wx",
        mode: 0o400,
      });
      verifyOutput();
    },
  };
}

const qualifierFiles = Object.freeze([
  ...sourceFiles,
  "scripts/jones/updates/legacy-preparation-native-fixture.mjs",
  "scripts/jones/updates/legacy-preparation-native-fixture.d.mts",
  "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs",
  "scripts/jones/updates/legacy-identity-native-fixture.mjs",
  ...[
    "jonesLegacyBootstrapSource.ts",
    "jonesLegacyMaintenanceSource.ts",
    "jonesNativeHelperSource.ts",
    "jonesPairedStateSource.ts",
    "jonesColdPairSource.ts",
    "jonesLegacyIdentityNative.c",
    "jonesColdReceiverSource.ts",
    "jonesColdOwnerSource.ts",
    "jonesPreparationWorkerSource.ts",
  ].map((name) => `apps/desktop/src/jones/updates/${name}`),
  "apps/server/src/jones/updates/offlinePreparationWorker.test.ts",
]);
const producerSourceFiles = Object.freeze([
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  "scripts/package.json",
  ...releaseManifests,
  "apps/server/vite.config.ts",
  "vite.config.ts",
  producerCandidateEntry,
  "scripts/jones/updates/offline-preparation-build.ts",
  "scripts/jones/updates/functional-policy.ts",
  "scripts/jones/updates/legacy-bootstrap-bundle.ts",
]);
const producerRawSourceFiles = Object.freeze([
  ...new Set([...producerSourceFiles, ...qualifierFiles]),
]);

function inspectLateQualifierModule(
  root: string,
  observed: { path: string; content: ReturnType<typeof candidateFile> },
) {
  const unavailable = (reason: string) => ({
    ...observed,
    syntax: { availability: "unavailable" as const, reason },
    importEdges: { availability: "unavailable" as const, reason },
  });
  if (observed.content.availability !== "observed")
    return unavailable("late-module-content-not-observed");
  let syntax: ReturnType<typeof inspectPreparationJavaScript>;
  try {
    const bytes = readFile(NodePath.join(root, observed.path), 8 * 1024 * 1024);
    if (bytes.length !== observed.content.size || digest(bytes) !== observed.content.sha256)
      return unavailable("late-module-changed-before-syntax-observation");
    syntax = inspectPreparationJavaScript(bytes.toString("utf8"), observed.path);
  } catch (cause) {
    if (qualificationCustodyUnknown(cause)) throw cause;
    return unavailable(cause instanceof Error ? cause.message : "late-module-syntax-not-observed");
  }
  type ImportObservation = (typeof syntax.imports)[number];
  const literalSiblingFiles: {
    import: ImportObservation;
    target: string;
    content: ReturnType<typeof candidateFile>;
  }[] = [];
  const builtins: ImportObservation[] = [];
  const computedImports: ImportObservation[] = [];
  const unresolvedEdges: {
    import: ImportObservation;
    reason: string;
    target?: string;
    content?: ReturnType<typeof candidateFile>;
  }[] = [];
  for (const item of syntax.imports) {
    if (item.specifier === null) {
      computedImports.push(item);
    } else if (item.builtin) {
      builtins.push(item);
    } else if (item.specifier.startsWith("./") || item.specifier.startsWith("../")) {
      let target: string;
      try {
        const url = new URL(
          item.specifier,
          NodeURL.pathToFileURL(NodePath.join(root, observed.path)),
        );
        if (url.search || url.hash) {
          unresolvedEdges.push({ import: item, reason: "literal-sibling-query-or-fragment" });
          continue;
        }
        target = NodeURL.fileURLToPath(url);
      } catch {
        unresolvedEdges.push({ import: item, reason: "literal-sibling-url-not-resolved" });
        continue;
      }
      const relative = candidateId(root, target);
      const content = candidateFile(root, target);
      if (!qualifierFiles.includes(relative)) {
        unresolvedEdges.push({
          import: item,
          target: relative,
          content,
          reason: "literal-sibling-outside-fixed-inventory",
        });
      } else if (content.availability !== "observed") {
        unresolvedEdges.push({
          import: item,
          target: relative,
          content,
          reason: "literal-sibling-content-not-observed",
        });
      } else {
        literalSiblingFiles.push({ import: item, target: relative, content });
      }
    } else {
      unresolvedEdges.push({
        import: item,
        reason: "literal-import-not-a-fixed-sibling-or-builtin",
      });
    }
  }
  return {
    ...observed,
    syntax: { availability: "observed" as const, ...syntax },
    importEdges: {
      availability: "observed" as const,
      literalSiblingFiles,
      builtins,
      computedImports,
      unresolvedEdges,
      builtinIdentityObserved: false,
      completeDependencyClosure: false,
    },
  };
}

/** Disk observations of fixed late imports are separate from the graph and never prove executed bytes. */
function inspectPreparationProducerCandidateSources(root: string) {
  const observe = (files: readonly string[]) =>
    files.map((path) => ({ path, content: candidateFile(root, NodePath.join(root, path)) }));
  const qualifierInputs = observe(qualifierFiles);
  return {
    producerInputs: observe(producerSourceFiles),
    lateQualifierInputs: {
      basis: "fixed-late-qualifier-file-inputs",
      computedImportsResolved: false,
      completeDependencyClosure: false,
      files: qualifierInputs,
      modules: qualifierInputs
        .filter(({ path }) => path.endsWith(".mjs"))
        .map((observed) => inspectLateQualifierModule(root, observed)),
    },
  };
}
const codeRoot = NodeFS.realpathSync(NodeURL.fileURLToPath(new URL("../../../", import.meta.url)));

function observeProducerFiles(root: string, files: readonly string[]) {
  return Object.fromEntries(
    files.map((name) => [name, digest(readFile(NodePath.join(root, name), 8 * 1024 * 1024))]),
  );
}

// Raw source bytes beside this module are a consistency baseline, not loaded-code attestation.
// Hashing a newer checkout cannot refresh code that has already entered this process.
const boundCodeSourceHashes = Object.freeze(observeProducerFiles(codeRoot, producerRawSourceFiles));

function qualificationCustodyUnknown(cause: unknown, depth = 0): boolean {
  if (depth > 8 || typeof cause !== "object" || cause === null) return false;
  return (
    ("preserveScratch" in cause && cause.preserveScratch === true) ||
    ("retainedRoot" in cause && typeof cause.retainedRoot === "string") ||
    (cause instanceof AggregateError &&
      cause.errors
        .slice(0, 32)
        .some((inner: unknown) => qualificationCustodyUnknown(inner, depth + 1))) ||
    ("cause" in cause && qualificationCustodyUnknown(cause.cause, depth + 1))
  );
}

function assertProducerFiles(root: string, expected: Readonly<Record<string, string>>) {
  if (encode(observeProducerFiles(root, Object.keys(expected))) !== encode(expected))
    throw new Error("Preparation producer or qualifier source changed during this process.");
}

function capturedStage(directory: string) {
  const captured = candidateDirectoryChain(directory);
  if (captured.missing !== undefined)
    throw new Error("Preparation stage root must already be owned.");
  return () => {
    const current = candidateDirectoryChain(directory);
    if (
      current.missing !== undefined ||
      current.chain.length !== captured.chain.length ||
      captured.chain.some(({ path, stat }, index) => {
        const item = current.chain[index]!;
        return (
          path !== item.path ||
          stat.dev !== item.stat.dev ||
          stat.ino !== item.stat.ino ||
          stat.uid !== item.stat.uid ||
          stat.mode !== item.stat.mode
        );
      })
    )
      throw new Error("Preparation stage identity changed.");
  };
}

function isWithin(parent: string, child: string) {
  const relative = NodePath.relative(parent, child);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
}

declare const qualifiedPreparationBrand: unique symbol;
export interface QualifiedPreparationHandle {
  readonly [qualifiedPreparationBrand]: true;
}
export interface QualifiedPreparationStageResult {
  readonly bootstrapBundle: JonesLegacyBootstrapPreparationBundle["files"];
  readonly bootstrapRuntime: JonesLegacyBootstrapPreparationBundle["runtime"];
  readonly diagnosticEvidence: {
    readonly artifactManifest: string;
    readonly nativeQualification: string;
    readonly nativeColdOwnerQualified: false;
    readonly stateAdmissionAllowed: false;
    readonly activationAllowed: false;
  };
}
interface QualifiedPreparationRecord {
  readonly sourceRoot: string;
  readonly codeRoot: string;
  readonly stageRoot: string;
  readonly verifyStage: () => void;
  readonly source: Awaited<ReturnType<typeof observeSource>>;
  readonly sourceHashes: Readonly<Record<string, string>>;
  readonly codeSourceHashes: Readonly<Record<string, string>>;
  readonly reportBytes: Buffer;
  readonly entryBytes: Buffer;
  readonly graph: ReturnType<typeof verifyFunctionalPreparationGraph>;
  readonly bundle: JonesLegacyBootstrapPreparationBundle;
  readonly runtime: Buffer;
  readonly diagnosticEvidence: QualifiedPreparationStageResult["diagnosticEvidence"];
  readonly signal: AbortSignal | undefined;
  readonly revoke: () => void;
}
const qualifiedPreparationHandles = new WeakMap<object, QualifiedPreparationRecord>();

/** Build and exercise the exact candidate. Serialized observations never enter the handle registry. */
export async function buildQualifiedPreparation(input: {
  /** Original Git checkout; raw source is checked against this module's private code root. */
  readonly repoRoot: string;
  readonly stageRoot: string;
  readonly signal?: AbortSignal;
}): Promise<QualifiedPreparationHandle> {
  input.signal?.throwIfAborted();
  const sourceRoot = NodeFS.realpathSync(input.repoRoot);
  const stageRoot = input.stageRoot;
  const verifyStage = capturedStage(stageRoot);
  const scratchParent = NodeFS.realpathSync(NodeOS.tmpdir());
  if (isWithin(stageRoot, scratchParent))
    throw new Error("Producer scratch must remain outside the packaging stage.");
  assertProducerFiles(codeRoot, boundCodeSourceHashes);
  const sourceHashes = Object.freeze(observeProducerFiles(sourceRoot, producerRawSourceFiles));
  if (encode(sourceHashes) !== encode(boundCodeSourceHashes))
    throw new Error("Preparation checkout raw source differs from the bound code source.");
  const candidate = NodePath.join(sourceRoot, candidateRelative);
  const verifyCandidate = capturedStage(candidate);
  if (
    encode(NodeFS.readdirSync(candidate).sort()) !==
    encode([bundleName, candidateReportName].sort())
  )
    throw new Error("Functional preparation requires the exact candidate graph and single entry.");
  const reportBytes = readFile(NodePath.join(candidate, candidateReportName), candidateReportLimit);
  const entryBytes = readFile(NodePath.join(candidate, bundleName), maxBundleBytes);
  const graph = verifyFunctionalPreparationGraph({ repoRoot: sourceRoot, reportBytes, entryBytes });
  const qualifierHashes = Object.freeze(
    Object.fromEntries(qualifierFiles.map((name) => [name, boundCodeSourceHashes[name]!])),
  );
  const completed = await withPreparationBuildScope(
    scratchParent,
    "jones-preparation-code-",
    input.signal,
    async (codeScope) => {
      process.stderr.write(`${encode({ event: "preparation-code-root", root: codeScope.root })}\n`);
      for (const name of qualifierFiles) {
        const bytes = readFile(NodePath.join(codeRoot, name), 8 * 1024 * 1024);
        if (digest(bytes) !== qualifierHashes[name])
          throw new Error("Qualifier source changed before snapshotting.");
        const destination = NodePath.join(codeScope.root, name);
        NodeFS.mkdirSync(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
        NodeFS.writeFileSync(destination, bytes, { flag: "wx", mode: 0o400 });
      }
      assertProducerFiles(codeScope.root, qualifierHashes);
      assertProducerFiles(codeRoot, boundCodeSourceHashes);
      assertProducerFiles(sourceRoot, sourceHashes);
      // A unique captured tree prevents an older ESM cache entry from satisfying this execution.
      const qualifier: typeof import("./legacy-preparation-native-fixture.mjs") = await import(
        NodeURL.pathToFileURL(
          NodePath.join(
            codeScope.root,
            "scripts/jones/updates/legacy-preparation-native-fixture.mjs",
          ),
        ).href
      );
      if (encode(qualifier.preparationNativeQualifierSourceInputs) !== encode(qualifierFiles))
        throw new Error("Preparation qualifier dependency inventory differs.");
      const materializer: {
        readColdMaintenanceSources(): Promise<{
          bootstrap: string;
          executor: string;
          maintenance: string;
          nativeSourceSha256: string;
        }>;
      } = await import(
        NodeURL.pathToFileURL(
          NodePath.join(
            codeScope.root,
            "scripts/jones/updates/legacy-cold-maintenance-fixture.mjs",
          ),
        ).href
      );
      const capturedTools = await materializer.readColdMaintenanceSources();
      if (
        capturedTools.maintenance !== jonesLegacyMaintenanceSource ||
        capturedTools.nativeSourceSha256 !==
          qualifierHashes["apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c"]
      )
        throw new Error(
          "Loaded maintenance composition differs from the captured qualifier source.",
        );
      assertProducerFiles(codeScope.root, qualifierHashes);
      try {
        return await withPreparationBuildScope(
          scratchParent,
          "jones-functional-preparation-",
          codeScope.controller.signal,
          async (scope) => {
            process.stderr.write(
              `${encode({ event: "preparation-build-root", root: scope.root })}\n`,
            );
            // Every native child uses this root or the qualifier's independently owned roots, never stageRoot.
            const source = await observeSource(sourceRoot, scope.command);
            if (
              source.version.length > 128 ||
              !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(source.version)
            )
              throw new Error("Functional preparation requires an exact preview version.");
            const observedGraphSource = Schema.decodeUnknownSync(
              Schema.Struct({
                source: Schema.Unknown,
              }),
            )(decode(reportBytes.toString("utf8"))).source;
            if (encode(observedGraphSource) !== encode(source))
              throw new Error("Candidate graph source is stale.");
            const native = await buildPreparationNativeBytes(scope, sourceRoot, entryBytes);
            verifyPreparationHeldOutput(
              (await scope.command(native.executable, [], scope.root, 4096)).toString("utf8"),
            );
            await verifyArgumentRefusal(scope.command, native.executable, scope.root);
            const cSourceName = "apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c";
            const cSource = readFile(NodePath.join(codeRoot, cSourceName), 4 * 1024 * 1024);
            if (digest(cSource) !== qualifierHashes[cSourceName])
              throw new Error("Observer source changed before compilation.");
            const cPath = NodePath.join(scope.root, "identity.c"),
              observerPath = NodePath.join(scope.root, "identity-native");
            NodeFS.writeFileSync(cPath, cSource, { flag: "wx", mode: 0o400 });
            await scope.command(
              "/usr/bin/clang",
              [
                "-target",
                "arm64-apple-macos13.0",
                "-std=c11",
                "-Wall",
                "-Wextra",
                "-Werror",
                cPath,
                "-o",
                observerPath,
              ],
              scope.root,
            );
            await scope.command(
              "/usr/bin/codesign",
              ["--force", "--sign", "-", "--timestamp=none", observerPath],
              scope.root,
            );
            await scope.command(
              "/usr/bin/codesign",
              ["--verify", "--strict", observerPath],
              scope.root,
            );
            NodeFS.chmodSync(observerPath, 0o500);
            const observerBytes = readFile(observerPath, 4 * 1024 * 1024);
            verifyJonesLegacyIdentityMachO(observerBytes);
            const runtime = readFile(native.executable, maxRuntimeBytes);
            const record = (bytes: string | Uint8Array) => ({
              sha256: digest(bytes),
              size: typeof bytes === "string" ? Buffer.byteLength(bytes) : bytes.length,
            });
            // This immutable input manifest precedes native qualification. Its digest has no cycle through the later report.
            const artifactManifest = `${encode({
              schema: 1,
              classification: "preparation-mechanical-artifact",
              source,
              graph,
              qualifierSourceHashes: qualifierHashes,
              entry: record(entryBytes),
              seaConfig: record(native.configBytes),
              executable: record(runtime),
              identityNative: record(observerBytes),
              identityNativeSourceSha256: digest(cSource),
              node: {
                version: nodeVersion,
                runtime: record(native.runtime),
                archive: record(native.archive),
                checksumsSha256: digest(native.checksums),
              },
              sqlite: native.sqlite,
              signature: "ad-hoc-verified",
              nativeColdOwnerQualified: false,
              stateAdmissionAllowed: false,
              activationAllowed: false,
            })}\n`;
            if (Buffer.byteLength(artifactManifest) > 65536)
              throw new Error("Preparation artifact manifest exceeds bound.");
            const manifestPath = NodePath.join(scope.root, "artifact-manifest.json");
            NodeFS.writeFileSync(manifestPath, artifactManifest, { flag: "wx", mode: 0o400 });
            const identity: JonesLegacyBootstrapBundleInput = {
              repository: "Jones-Systems/Jones-Code",
              sourceSha: source.sha,
              sourceTree: source.tree,
              version: source.version,
            };
            const preparation = {
              schema: 1,
              protocol: 1,
              provenanceSha256: digest(artifactManifest),
              sourceInputHashes: source.hashes,
              entry: record(entryBytes),
              seaConfig: record(native.configBytes),
              executable: { name: executableName, ...record(runtime) },
              node: {
                version: "26.8.2",
                platform: "darwin",
                architecture: "arm64",
                archiveSha256: native.archiveHash,
                archiveBytes: native.archive.length,
                checksumsSha256: digest(native.checksums),
                runtimeSha256: native.runtimeHash,
                runtimeBytes: native.runtime.length,
              },
              sqlite: { version: native.sqlite.version, sourceId: native.sqlite.sourceId },
            };
            const artifactEvidence = readJonesPreparationArtifactEvidence(
              `${encode({ schema: 1, observation: "complete", classification: "cold-preparation-worker", source: identity, preparation })}\n`,
              identity,
            );
            const file = (path: string, bytes: string | Uint8Array) => ({ path, ...record(bytes) });
            const fixtureInput = {
              schema: 1,
              classification: "synthetic-preparation-native-input",
              sourceSha: source.sha,
              sourceTree: source.tree,
              nativeSourceSha256: digest(cSource),
              preparation,
              qualifierSourceHashes: qualifierHashes,
              files: {
                executable: file(native.executable, runtime),
                node: file(native.runtimePath, native.runtime),
                native: file(observerPath, observerBytes),
                entry: file(NodePath.join(scope.root, bundleName), entryBytes),
                seaConfig: file(NodePath.join(scope.root, "sea-config.json"), native.configBytes),
                provenance: file(manifestPath, artifactManifest),
              },
            };
            // Ten individually captured cases have a separate total allowance; Git/codegen keep their short command deadlines.
            // @effect-diagnostics-next-line globalTimers:off -- This qualifier-owned deadline aborts through the captured fixture signal and is cleared after its finalizers.
            const qualifierDeadline = NodeTimers.setTimeout(
              () => scope.abort(new Error("Preparation native qualification timed out.")),
              15 * 60_000,
            );
            let result: unknown;
            try {
              result = await qualifier.runLegacyPreparationNativeFixture({
                input: fixtureInput,
                signal: scope.controller.signal,
                temporaryDirectory: scratchParent,
              });
            } catch (cause) {
              if (qualificationCustodyUnknown(cause))
                scope.unresolved.push({
                  kind: "qualification",
                  id: 0,
                  target: codeScope.root,
                  closeObserved: false,
                  failures: ["native qualifier custody remains unresolved"],
                });
              throw cause;
            } finally {
              NodeTimers.clearTimeout(qualifierDeadline);
            }
            qualifier.validatePreparationNativeResult(result, fixtureInput);
            const nativeQualification = encode(result);
            if (Buffer.byteLength(nativeQualification) > 4 * 1024 * 1024)
              throw new Error("Preparation native evidence exceeds bound.");
            for (const member of Object.values(fixtureInput.files)) {
              const bytes = readFile(member.path, maxRuntimeBytes);
              if (bytes.length !== member.size || digest(bytes) !== member.sha256)
                throw new Error("Qualified preparation input changed during use.");
            }
            await scope.command(
              "/usr/bin/codesign",
              ["--verify", "--strict", native.executable],
              scope.root,
            );
            await scope.command(
              "/usr/bin/codesign",
              ["--verify", "--strict", observerPath],
              scope.root,
            );
            assertProducerFiles(codeRoot, boundCodeSourceHashes);
            assertProducerFiles(sourceRoot, sourceHashes);
            assertProducerFiles(codeScope.root, qualifierHashes);
            verifyCandidate();
            verifyStage();
            if (
              encode(NodeFS.readdirSync(candidate).sort()) !==
                encode([bundleName, candidateReportName].sort()) ||
              !readFile(NodePath.join(candidate, candidateReportName), candidateReportLimit).equals(
                reportBytes,
              ) ||
              !readFile(NodePath.join(candidate, bundleName), maxBundleBytes).equals(entryBytes) ||
              encode(
                verifyFunctionalPreparationGraph({ repoRoot: sourceRoot, reportBytes, entryBytes }),
              ) !== encode(graph) ||
              encode(await observeSource(sourceRoot, scope.command)) !== encode(source)
            )
              throw new Error("Preparation graph or source changed during qualification.");
            const bundle = createJonesLegacyBootstrapPreparationBundle(
              identity,
              {
                identityNative: observerBytes,
                identityNativeSourceSha256: digest(cSource),
                maintenanceSource: jonesLegacyMaintenanceSource,
              },
              artifactEvidence,
            );
            const tools = new Map(
              bundle.files.map((member) => [member.relativePath, member.content]),
            );
            if (
              encode([...tools.keys()].sort()) !==
                encode(
                  [
                    "bootstrap.py",
                    "executor.py",
                    "BUNDLE.json",
                    "maintenance.py",
                    "identity-native",
                  ]
                    .map((name) => `jones-bootstrap/${name}`)
                    .sort(),
                ) ||
              tools.get("jones-bootstrap/bootstrap.py") !== capturedTools.bootstrap ||
              tools.get("jones-bootstrap/executor.py") !== capturedTools.executor ||
              tools.get("jones-bootstrap/maintenance.py") !== capturedTools.maintenance
            )
              throw new Error(
                "Packaged maintenance tools differ from the actually qualified source snapshot.",
              );
            return {
              source,
              bundle,
              runtime,
              diagnosticEvidence: Object.freeze({
                artifactManifest,
                nativeQualification,
                nativeColdOwnerQualified: false as const,
                stateAdmissionAllowed: false as const,
                activationAllowed: false as const,
              }),
            };
          },
        );
      } catch (cause) {
        if (qualificationCustodyUnknown(cause))
          codeScope.unresolved.push({
            kind: "qualification",
            id: 0,
            target: codeScope.root,
            closeObserved: false,
            failures: ["native build or qualifier custody remains unresolved"],
          });
        throw cause;
      }
    },
  );
  // withPreparationBuildScope returns only after every captured child and its scratch have finalized.
  input.signal?.throwIfAborted();
  verifyStage();
  assertProducerFiles(codeRoot, boundCodeSourceHashes);
  assertProducerFiles(sourceRoot, sourceHashes);
  const handle = Object.freeze(Object.create(null)) as QualifiedPreparationHandle;
  const revoke = () => {
    qualifiedPreparationHandles.delete(handle);
    input.signal?.removeEventListener("abort", revoke);
  };
  qualifiedPreparationHandles.set(handle, {
    ...completed,
    sourceRoot,
    codeRoot,
    stageRoot,
    verifyStage,
    sourceHashes,
    codeSourceHashes: boundCodeSourceHashes,
    reportBytes,
    entryBytes,
    graph,
    signal: input.signal,
    revoke,
  });
  input.signal?.addEventListener("abort", revoke, { once: true });
  if (input.signal?.aborted) {
    revoke();
    input.signal.throwIfAborted();
  }
  return handle;
}

function writeQualifiedMember(path: string, bytes: Uint8Array, mode: number) {
  let fd: number | undefined, primary: unknown;
  try {
    fd = NodeFS.openSync(
      path,
      NodeFS.constants.O_WRONLY |
        NodeFS.constants.O_CREAT |
        NodeFS.constants.O_EXCL |
        NodeFS.constants.O_NOFOLLOW,
      0o600,
    );
    let offset = 0;
    while (offset < bytes.length) {
      const count = NodeFS.writeSync(
        fd,
        bytes,
        offset,
        Math.min(1024 * 1024, bytes.length - offset),
        offset,
      );
      if (count <= 0) throw new Error("Preparation resource write did not advance.");
      offset += count;
    }
    NodeFS.fchmodSync(fd, mode);
    NodeFS.fsyncSync(fd);
  } catch (cause) {
    primary = cause;
  }
  if (fd !== undefined) {
    try {
      NodeFS.closeSync(fd);
    } catch (cause) {
      throw new AggregateError(
        primary === undefined ? [cause] : [primary, cause],
        "Preparation resource descriptor closure is unproved.",
      );
    }
  }
  if (primary !== undefined) throw primary;
}

/** Consume an actual same-process handle once; paths and matching JSON cannot substitute for it. */
export async function stageQualifiedPreparation(input: {
  readonly handle: QualifiedPreparationHandle;
  readonly expectedSource: JonesLegacyBootstrapBundleInput;
  readonly stageResourcesDir: string;
  readonly signal?: AbortSignal;
}): Promise<QualifiedPreparationStageResult> {
  const stored = qualifiedPreparationHandles.get(input.handle);
  if (stored === undefined) throw new Error("Preparation handle is absent, expired, or consumed.");
  stored.revoke();
  input.signal?.throwIfAborted();
  stored.signal?.throwIfAborted();
  stored.verifyStage();
  assertProducerFiles(stored.codeRoot, stored.codeSourceHashes);
  assertProducerFiles(stored.sourceRoot, stored.sourceHashes);
  if (
    input.expectedSource.repository !== stored.source.repository ||
    input.expectedSource.sourceSha !== stored.source.sha ||
    input.expectedSource.sourceTree !== stored.source.tree ||
    input.expectedSource.version !== stored.source.version
  )
    throw new Error("Preparation capability source identity differs from packaging.");
  const destination = input.stageResourcesDir;
  if (!isWithin(stored.stageRoot, destination) || destination === stored.stageRoot)
    throw new Error("Preparation resources must belong to the captured packaging stage.");
  const verifyDestination = capturedStage(destination);
  const candidate = NodePath.join(stored.sourceRoot, candidateRelative);
  if (
    encode(NodeFS.readdirSync(candidate).sort()) !==
      encode([bundleName, candidateReportName].sort()) ||
    !readFile(NodePath.join(candidate, candidateReportName), candidateReportLimit).equals(
      stored.reportBytes,
    ) ||
    !readFile(NodePath.join(candidate, bundleName), maxBundleBytes).equals(stored.entryBytes) ||
    encode(
      verifyFunctionalPreparationGraph({
        repoRoot: stored.sourceRoot,
        reportBytes: stored.reportBytes,
        entryBytes: stored.entryBytes,
      }),
    ) !== encode(stored.graph)
  )
    throw new Error("Preparation candidate changed before staging.");
  await withPreparationBuildScope(
    NodeFS.realpathSync(NodeOS.tmpdir()),
    "jones-preparation-stage-check-",
    input.signal,
    async (scope) => {
      if (encode(await observeSource(stored.sourceRoot, scope.command)) !== encode(stored.source))
        throw new Error("Preparation checkout identity changed before staging.");
    },
  );
  input.signal?.throwIfAborted();
  stored.signal?.throwIfAborted();
  verifyDestination();
  stored.verifyStage();
  assertProducerFiles(stored.codeRoot, stored.codeSourceHashes);
  assertProducerFiles(stored.sourceRoot, stored.sourceHashes);
  const directory = NodePath.join(destination, "jones-bootstrap");
  try {
    NodeFS.mkdirSync(directory, { mode: 0o700 });
    const verifyDirectory = capturedStage(directory);
    for (const member of stored.bundle.files) {
      input.signal?.throwIfAborted();
      stored.signal?.throwIfAborted();
      verifyDirectory();
      if (NodePath.dirname(member.relativePath) !== "jones-bootstrap")
        throw new Error("Preparation bundle resource escaped its fixed namespace.");
      writeQualifiedMember(
        NodePath.join(destination, member.relativePath),
        typeof member.content === "string" ? Buffer.from(member.content) : member.content,
        member.mode,
      );
    }
    const runtime = stored.bundle.runtime;
    if (stored.runtime.length !== runtime.size || digest(stored.runtime) !== runtime.sha256)
      throw new Error("Qualified preparation runtime changed before staging.");
    writeQualifiedMember(
      NodePath.join(destination, runtime.relativePath),
      stored.runtime,
      runtime.mode,
    );
    for (const member of stored.bundle.files) {
      const expected =
        typeof member.content === "string"
          ? Buffer.from(member.content)
          : Buffer.from(member.content);
      if (
        !readFile(NodePath.join(destination, member.relativePath), 4 * 1024 * 1024).equals(
          expected,
        ) ||
        (NodeFS.lstatSync(NodePath.join(destination, member.relativePath)).mode & 0o777) !==
          member.mode
      )
        throw new Error("Staged preparation bundle differs from qualified bytes.");
    }
    const staged = readFile(NodePath.join(destination, runtime.relativePath), runtime.maxBytes);
    if (
      staged.length !== runtime.size ||
      digest(staged) !== runtime.sha256 ||
      (NodeFS.lstatSync(NodePath.join(destination, runtime.relativePath)).mode & 0o777) !==
        runtime.mode
    )
      throw new Error("Staged preparation executable differs from qualified bytes.");
    verifyDirectory();
    verifyDestination();
    stored.verifyStage();
    assertProducerFiles(stored.codeRoot, stored.codeSourceHashes);
    assertProducerFiles(stored.sourceRoot, stored.sourceHashes);
    input.signal?.throwIfAborted();
    stored.signal?.throwIfAborted();
    for (const parent of [directory, destination]) {
      const fd = NodeFS.openSync(parent, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
      let primary: unknown;
      try {
        NodeFS.fsyncSync(fd);
      } catch (cause) {
        primary = cause;
      }
      try {
        NodeFS.closeSync(fd);
      } catch (cause) {
        throw new AggregateError(
          primary === undefined ? [cause] : [primary, cause],
          "Preparation directory closure is unproved.",
        );
      }
      if (primary !== undefined) throw primary;
    }
  } catch (cause) {
    // The desktop stage allocator must preserve this exact tree; its coarse finalizer cannot establish native FD closure.
    throw Object.assign(
      new AggregateError([cause], "Preparation staging failed; owning stage must be retained.", {
        cause,
      }),
      {
        preserveStageRoot: true,
        stageRoot: stored.stageRoot,
      },
    );
  }
  return Object.freeze({
    bootstrapBundle: stored.bundle.files,
    bootstrapRuntime: stored.bundle.runtime,
    diagnosticEvidence: stored.diagnosticEvidence,
  });
}
