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
import * as NodeHTTP from "node:http";
import * as NodeUtil from "node:util";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The sole download origin is the pinned official Node release over verified TLS.
import * as NodeHTTPS from "node:https";
import * as Schema from "effect/Schema";
import ts from "typescript-legacy";
import type { Plugin } from "vite-plus";

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

/** An oracle for the fixed held entry, not a sandbox for arbitrary JavaScript. */
export function verifyPreparationClosure(source: string): readonly string[] {
  if (Buffer.byteLength(source) > maxBundleBytes)
    throw new Error("Preparation bundle exceeds bound.");
  const file = ts.createSourceFile(
    bundleName,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.JS,
  );
  const diagnostics = (file as ts.SourceFile & { parseDiagnostics: readonly ts.Diagnostic[] })
    .parseDiagnostics;
  if (diagnostics.length) throw new Error("Preparation bundle is not valid JavaScript.");
  const imports = new Set<string>();
  const inspectSpecifier = (node: ts.Node | undefined) => {
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
  const visit = (node: ts.Node): void => {
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
  readonly kind: "command" | "download";
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
        let response: NodeHTTP.IncomingMessage | undefined;
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
        let request: NodeHTTP.ClientRequest;
        try {
          request = (secure ? NodeHTTPS : NodeHTTP).get(url, { agent: false }, (incoming) => {
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
    if (scope.root && scope.unresolved.length !== 0) {
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
    server?: NodeHTTP.Server;
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
            observed.server = NodeHTTP.createServer((_request, response) => {
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
  const fd = NodeFS.openSync(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
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
    return bytes;
  } finally {
    NodeFS.closeSync(fd);
  }
}

/** Only the existing release-stamp fields may differ from the committed source. */
export function verifyPreparationSourceOverlay(
  file: string,
  original: unknown,
  actual: unknown,
  identity: { sha: string; tree: string },
): string {
  if (!releaseManifests.includes(file)) throw new Error("Unadmitted source overlay.");
  const record = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));
  const before = { ...record(original) };
  const after = { ...record(actual) };
  const version = Schema.decodeUnknownSync(Schema.NonEmptyString)(after.version);
  if (["apps/server/package.json", "apps/desktop/package.json"].includes(file)) {
    if (
      after.jonesSource !== undefined &&
      !NodeUtil.isDeepStrictEqual(after.jonesSource, {
        repository: "Jones-Systems/Jones-Code",
        ...identity,
      })
    )
      throw new Error("Stamped source identity differs from observed Git source.");
    if (after.startupGateProtocol !== undefined && after.startupGateProtocol !== 1)
      throw new Error("Unadmitted startup metadata.");
    delete before.jonesSource;
    delete after.jonesSource;
    delete before.startupGateProtocol;
    delete after.startupGateProtocol;
  }
  delete before.version;
  delete after.version;
  if (!NodeUtil.isDeepStrictEqual(before, after))
    throw new Error("Preparation source has changes beyond release stamping.");
  return version;
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
  if (![sha, tree].every((value) => /^[a-f0-9]{40}$/.test(value)))
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

const HashRecord = Schema.Struct({
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  size: Schema.Int.check(Schema.isGreaterThan(0)),
});
const ArtifactManifest = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  observation: Schema.Literal("complete"),
  classification: Schema.Literal("held-entry-only"),
  preparationCapable: Schema.Literal(false),
  productionInvocation: Schema.Literal(false),
  source: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    tree: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    version: Schema.NonEmptyString,
    hashes: Schema.Record(Schema.String, Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))),
  }),
  entry: Schema.Literal(entry),
  dependencyInventory: Schema.Array(Schema.String),
  builtins: Schema.Array(Schema.String),
  bundle: HashRecord,
  seaConfig: Schema.Struct({ ...HashRecord.fields, value: Schema.Unknown }),
  upstream: Schema.Struct({
    archiveUrl: Schema.Literal(releaseOrigin + archiveName),
    checksumsUrl: Schema.Literal(releaseOrigin + "SHASUMS256.txt"),
    verification: Schema.Literal("official-https-checksum"),
    signedChecksumVerification: Schema.Literal(false),
    archive: HashRecord,
    checksumsSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    runtime: Schema.Struct({
      ...HashRecord.fields,
      version: Schema.Literal(nodeVersion),
      platform: Schema.Literal("darwin"),
      arch: Schema.Literal("arm64"),
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

/** Re-observe final files; a JSON receipt alone is not artifact or execution authority. */
export function verifyPreparationArtifact(directory: string): void {
  verifyPreparationArtifactAt(directory, "provenance.json");
}

function verifyPreparationArtifactAt(
  directory: string,
  provenanceName: "provenance.json" | "provenance.complete.json",
): void {
  const manifest = Schema.decodeUnknownSync(ArtifactManifest)(
    decode(readFile(NodePath.join(directory, provenanceName), 4 * 1024 * 1024).toString("utf8")),
  );
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
      verifyPreparationSeaConfig(decode(bytes.toString("utf8")));
      if (encode(decode(bytes.toString("utf8"))) !== encode(manifest.seaConfig.value))
        throw new Error("Recorded SEA config differs from bytes.");
    }
  }
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
      onlyBundle: false,
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
          // The native producer has no host-package installation or cross-runtime fallback.
          if (
            (await command("/usr/bin/uname", ["-sm"], root)).toString("utf8").trim() !==
            "Darwin arm64"
          )
            throw new Error(
              "Preparation SEA production requires a native Darwin arm64 build host.",
            );
          const checksums = await scope.download("SHASUMS256.txt", 128 * 1024);
          const archive = await scope.download(archiveName, 128 * 1024 * 1024);
          const archiveHash = verifyNodeArchive(checksums.toString("utf8"), archive);
          const archivePath = NodePath.join(scratch, archiveName);
          NodeFS.writeFileSync(archivePath, archive, { flag: "wx", mode: 0o600 });
          const member = `node-v${nodeVersion}-darwin-arm64/bin/node`;
          if (
            (await command("/usr/bin/tar", ["-tzf", archivePath, member], scratch)).toString(
              "utf8",
            ) !== `${member}\n`
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
            (await command("/usr/bin/lipo", ["-archs", runtimePath], scratch))
              .toString("utf8")
              .trim() !== "arm64"
          )
            throw new Error(
              "Observed Node runtime does not match the pinned version and architecture.",
            );
          NodeFS.writeFileSync(NodePath.join(scratch, bundleName), bundle, {
            flag: "wx",
            mode: 0o400,
          });
          const config = preparationSeaConfig();
          verifyPreparationSeaConfig(config);
          const configBytes = `${encode(config)}\n`;
          NodeFS.writeFileSync(NodePath.join(scratch, "sea-config.json"), configBytes, {
            flag: "wx",
            mode: 0o400,
          });
          // Node 26.8.2 documents direct SEA generation and module argv at:
          // https://nodejs.org/download/release/v26.8.2/docs/api/single-executable-applications.html
          await command(runtimePath, ["--build-sea", "sea-config.json"], scratch);
          const executable = NodePath.join(scratch, executableName);
          await command(
            "/usr/bin/codesign",
            ["--force", "--sign", "-", "--timestamp=none", executable],
            scratch,
          );
          await command("/usr/bin/codesign", ["--verify", "--strict", executable], scratch);
          NodeFS.chmodSync(executable, 0o500);
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
