/* oxlint-disable t3code/no-global-process-runtime -- CI-native adapter verifies its own runtime before package execution. */
// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off globalConsole:off - Bounded hosted artifact consumer, never application startup.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeModule from "node:module";
import * as NodeURL from "node:url";
import { parseSync } from "vite-plus";
import {
  processAbsence,
  qualificationCancellation,
  withSmokeScratch,
} from "./desktop-worker-smoke.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const REPO = "Jones-Systems/Jones-Code";
const ENTRY = "apps/desktop/dist-electron/jones/updates/pickerVerification";
const hash = (bytes: NodeCrypto.BinaryLike) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
function proof(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

type Ast = { type: string; [key: string]: unknown };
const ast = (value: unknown): Ast | undefined =>
  value !== null && typeof value === "object" && "type" in value && typeof value.type === "string"
    ? (value as Ast)
    : undefined;
function walkAst(
  node: Ast,
  visit: (node: Ast, parent?: Ast, key?: string) => void,
  parent?: Ast,
  key?: string,
) {
  visit(node, parent, key);
  for (const [name, value] of Object.entries(node)) {
    if (name === "comments") continue;
    if (Array.isArray(value))
      for (const item of value) {
        const child = ast(item);
        if (child) walkAst(child, visit, node, name);
      }
    else {
      const child = ast(value);
      if (child) walkAst(child, visit, node, name);
    }
  }
}
const member = (node: Ast | undefined) =>
  node?.type === "MemberExpression"
    ? node.computed
      ? ast(node.property)?.value
      : ast(node.property)?.name
    : undefined;
function parse(file: string, source: string) {
  const result = parseSync(file, source);
  proof(result.errors.length === 0, `Picker dependency parse failed: ${file}`);
  return result.program as unknown as Ast;
}
export function scanPickerBundle(source: string, kind: "main" | "preload") {
  const dependencies = new Set<string>(),
    failures: string[] = [];
  const accept = (value: unknown) => {
    if (typeof value !== "string") {
      failures.push("dynamic-module-load");
      return;
    }
    dependencies.add(value);
    if (
      value === "module" ||
      value === "node:module" ||
      (value !== "electron" && (kind === "preload" || !NodeModule.isBuiltin(value)))
    )
      failures.push(`external:${value}`);
  };
  walkAst(parse("picker-emitted.cjs", source), (node, parent, key) => {
    if (
      node.type === "ImportDeclaration" ||
      node.type === "ExportAllDeclaration" ||
      node.type === "ExportNamedDeclaration"
    ) {
      const source = ast(node.source);
      if (source) accept(source.value);
    }
    if (node.type === "CallExpression" || node.type === "NewExpression") {
      const callee = ast(node.callee),
        name = member(callee);
      if (callee?.type === "Identifier" && callee.name === "require") {
        const args = node.arguments as unknown[];
        if (args.length !== 1) failures.push("dynamic-module-load");
        else accept(ast(args[0])?.value);
      }
      if (
        [
          "createRequire",
          "dlopen",
          "_load",
          "_resolveFilename",
          "binding",
          "_linkedBinding",
        ].includes(String(name))
      )
        failures.push(`module-loader:${name}`);
      if (name === "getBuiltinModule") {
        const args = node.arguments as unknown[];
        if (args.length !== 1) failures.push("dynamic-builtin-load");
        else accept(ast(args[0])?.value);
      }
      if (name === "require" && ast(callee?.object)?.name === "module")
        failures.push("indirect-require");
      if (callee?.type === "Identifier" && ["eval", "Function"].includes(String(callee.name)))
        failures.push("dynamic-code");
    }
    if (node.type === "ImportExpression") accept(ast(node.source)?.value);
    if (
      member(node) === "getBuiltinModule" &&
      !(parent?.type === "CallExpression" && key === "callee")
    )
      failures.push("aliased-builtin-loader");
    if (
      node.type === "Identifier" &&
      node.name === "require" &&
      !(parent?.type === "CallExpression" && key === "callee") &&
      !(key === "key" || key === "property")
    )
      failures.push("aliased-require");
    if (
      node.type === "Literal" &&
      typeof node.value === "string" &&
      (/\.node$|(?:^|\/)(?:boot|main|preload)\.cjs$/.test(node.value) ||
        node.value === "original-fs")
    )
      failures.push(`forbidden-reference:${node.value}`);
  });
  proof(
    failures.length === 0,
    `Picker emitted dependency scan refused: ${[...new Set(failures)].join(", ")}`,
  );
  return {
    sha256: hash(source),
    bytes: Buffer.byteLength(source),
    dependencies: [...dependencies].sort(),
  };
}
export function scanPickerSourceGraph(entry: string) {
  const visited = new Set<string>(),
    imports: { from: string; to: string }[] = [];
  const visit = (file: string) => {
    file = NodeFS.realpathSync(file);
    if (visited.has(file)) return;
    visited.add(file);
    proof(
      !/(?:\/src\/(?:app|backend|electron)\/|\/(?:boot|main|preload|DesktopUpdates)\.ts$)/.test(
        file,
      ),
      `Forbidden picker source module: ${file}`,
    );
    const require = NodeModule.createRequire(file);
    walkAst(parse(file, NodeFS.readFileSync(file, "utf8")), (node) => {
      if (node.type === "CallExpression" && member(ast(node.callee)) === "createRequire")
        throw new Error(`Indirect source module loader: ${file}`);
      if (node.type === "ImportExpression") throw new Error(`Dynamic source module load: ${file}`);
      if (
        !["ImportDeclaration", "ExportNamedDeclaration", "ExportAllDeclaration"].includes(
          node.type,
        ) ||
        node.importKind === "type" ||
        node.exportKind === "type"
      )
        return;
      const specifiers = node.specifiers as Ast[] | undefined;
      if (
        node.type === "ImportDeclaration" &&
        specifiers?.length &&
        specifiers.every((specifier) => specifier.importKind === "type")
      )
        return;
      const name = ast(node.source)?.value;
      if (typeof name !== "string") return;
      imports.push({ from: NodePath.relative(process.cwd(), file), to: name });
      proof(name !== "module" && name !== "node:module", `Indirect source module loader: ${file}`);
      if (NodeModule.isBuiltin(name) || name === "effect" || name.startsWith("effect/")) return;
      proof(
        !/^(?:electron(?:-updater)?|@clerk\/)/.test(name),
        `Forbidden picker dependency: ${name}`,
      );
      proof(
        name.startsWith(".") || name.startsWith("@t3tools/"),
        `Unexpected picker package: ${name}`,
      );
      if (name.startsWith("@t3tools/")) {
        const parts = name.split("/"),
          workspace = NodePath.resolve("packages", parts[1]!),
          subpath = parts.length === 2 ? "." : "./" + parts.slice(2).join("/");
        const exports = JSON.parse(
          NodeFS.readFileSync(NodePath.join(workspace, "package.json"), "utf8"),
        ).exports;
        const target = exports[subpath]?.import ?? exports[subpath]?.default;
        proof(
          typeof target === "string" && target.startsWith("./src/"),
          `Unknown workspace source export: ${name}`,
        );
        visit(NodePath.join(workspace, target));
      } else visit(require.resolve(name));
    });
  };
  visit(entry);
  return {
    files: [...visited].map((file) => NodePath.relative(process.cwd(), file)).sort(),
    imports,
  };
}

export function readPickerFuses(binary: Buffer) {
  const sentinel = Buffer.from("dL7pKGdnNz796PbbjQWNKmHXBZaB9tsX");
  const at = binary.indexOf(sentinel);
  proof(
    at >= 0 && at === binary.lastIndexOf(sentinel),
    "Missing or ambiguous arm64 Electron fuse wire.",
  );
  const offset = at + sentinel.length,
    version = binary[offset],
    length = binary[offset + 1];
  proof(
    version === 1 && length !== undefined && length >= 6 && offset + 2 + length <= binary.length,
    "Unsupported Electron fuse wire.",
  );
  const values = [...binary.subarray(offset + 2, offset + 2 + length)];
  proof(
    values.every((value) => [0x30, 0x31, 0x72].includes(value)),
    "Unknown Electron fuse state.",
  );
  proof(values[5] === 0x30, "OnlyLoadAppFromAsar is not off; narrow COPY loader unavailable.");
  return {
    version,
    length,
    values,
    runAsNode: values[0],
    embeddedAsarIntegrity: values[4],
    onlyLoadAppFromAsar: values[5],
  };
}

export function requirePickerCopyPath(value: string) {
  proof(
    NodePath.isAbsolute(value) &&
      NodePath.resolve(value) === value &&
      value.endsWith(".app") &&
      !/(?:^|\/)(?:tmp|Volumes|AppTranslocation|jones-updates)(?:\/|$)/.test(value),
    "Canonical copied app path cannot pass the real stable-path check.",
  );
}

export const expectedPickerChecks = {
  catalog: [
    "CAT-A1-two-builds",
    "CAT-A2-catalog-unavailable",
    "CAT-A3-no-dispatch",
    "CAT-A4-no-plan",
    "SEL-A1-unselected-start",
    "SEL-A2-stale-selection",
    "SEL-A3-alternate-selected",
    "SEL-A4-no-dispatch-before-confirm",
    "SEL-A5-mismatch-refused",
    "IPC-B1-malformed-no-state",
    "IPC-B2-selected-provenance",
    "IPC-B3-provenance-listener",
    "IPC-B4-plan-listener",
    "PLAN-A1-start-admitted",
    "PLAN-A2-duplicate-plan",
    "PLAN-A3-duplicate-zero-dispatch",
    "PREP-A1-exact-artifact",
    "PREP-A2-operation-token",
  ],
  persisted: [
    "RESTART-A1-retained-failure",
    "RESTART-A2-zero-auto-replay",
    "RESTART-A3-requires-reselection",
    "RESTART-A4-explicit-reselection",
    "RESTART-A5-single-explicit-retry",
  ],
  inflight: [
    "RESTART-B1-reselection",
    "CONC-A1-one-dispatch",
    "CONC-A2-one-plan",
    "CONC-A3-operation-token",
  ],
  held: [
    "RESTART-B2-held-status",
    "RESTART-B3-held-code",
    "RESTART-B4-no-replay",
    "RESTART-B5-held-refused",
    "RESTART-B6-no-extra-dispatch",
  ],
};
const commonPickerChecks = [
  "IPC-A1-capability-members",
  "IPC-A2-renderer-require",
  "IPC-A3-renderer-process",
  "IPC-A4-inert-list",
  "IPC-A5-inert-transport",
  "SAFE-A1-zero-native",
  "SAFE-A2-zero-install",
  "SAFE-A3-zero-transport-download",
  "SAFE-A4-no-install-operation",
];

export function evaluatePickerReports(reports: Array<Record<string, unknown>>) {
  proof(
    JSON.stringify(reports.map((report) => report.phase)) ===
      JSON.stringify(["catalog", "persisted", "inflight", "held"]),
    "Missing or unordered native picker phases.",
  );
  for (const report of reports) {
    proof(
      report.status === "pass" &&
        report.nativeHop === true &&
        Array.isArray(report.checks) &&
        report.checks.length > 0 &&
        report.checks.every((check) => check.status === "pass"),
      "Picker phase did not pass the native assertions.",
    );
    const expected = [
      ...commonPickerChecks,
      ...expectedPickerChecks[report.phase as keyof typeof expectedPickerChecks],
    ];
    const observed = (report.checks as Array<{ id: string }>).map((check) => check.id);
    proof(
      expected.length === observed.length &&
        new Set(observed).size === expected.length &&
        expected.every((id) => observed.includes(id)),
      "Missing, duplicate or unexpected picker acceptance case.",
    );
    const preferences = report.preferences as Record<string, unknown> | undefined;
    proof(
      preferences?.contextIsolation === true &&
        preferences.sandbox === true &&
        preferences.nodeIntegration === false,
      "Native window security readback unavailable.",
    );
    proof(
      Array.isArray(report.violations) &&
        report.violations.length === 0 &&
        Array.isArray(report.nativeCalls) &&
        report.nativeCalls.length === 0 &&
        report.installCalls === 0,
      "Forbidden native/Install effect observed.",
    );
  }
  return "PASS";
}

async function command(file: string, args: string[], signal?: AbortSignal) {
  return (
    await exec(file, args, {
      encoding: "utf8",
      timeout: 60_000,
      maxBuffer: 2 * 1024 * 1024,
      ...(signal === undefined ? {} : { signal }),
    })
  ).stdout;
}

export async function locatePickerProducer(
  input: {
    source: string;
    tree: string;
    event: "pull_request" | "push" | "schedule";
    pr?: number;
    head?: string;
    runId?: number;
  },
  api: (endpoint: string) => Promise<unknown>,
  wait: () => Promise<void>,
  budget = 90,
) {
  proof(
    /^[a-f0-9]{40}$/.test(input.source) && /^[a-f0-9]{40}$/.test(input.tree),
    "Invalid exact consumer source/tree.",
  );
  for (let count = 0; count < budget; count++) {
    const list =
      input.runId === undefined
        ? ((await api(
            `repos/${REPO}/actions/workflows/artifact-desktop-mac.yml/runs?event=${input.event}&per_page=100`,
          )) as { workflow_runs: Array<Record<string, unknown>> })
        : {
            workflow_runs: [
              (await api(`repos/${REPO}/actions/runs/${input.runId}`)) as Record<string, unknown>,
            ],
          };
    const matches = list.workflow_runs.filter(
      (run) =>
        (run.head_sha === input.source ||
          (input.event === "pull_request" && run.head_sha === input.head)) &&
        run.event === input.event &&
        run.path === ".github/workflows/artifact-desktop-mac.yml" &&
        (run.repository as { full_name?: string })?.full_name === REPO &&
        (run.head_repository as { full_name?: string })?.full_name === REPO &&
        (input.event !== "pull_request" ||
          (run.pull_requests as Array<{ number: number }> | undefined)?.some(
            (pr) => pr.number === input.pr,
          )),
    );
    for (const run of matches) {
      if (run.status !== "completed") continue;
      if (run.conclusion !== "success") continue;
      const artifacts = (await api(
        `repos/${REPO}/actions/runs/${run.id}/artifacts?per_page=100`,
      )) as { artifacts: Array<Record<string, unknown>> };
      const eligible = artifacts.artifacts.filter(
        (artifact) =>
          artifact.expired === false &&
          typeof artifact.name === "string" &&
          artifact.name.startsWith(`desktop-mac-arm64-${run.id}-${run.run_attempt}--`),
      );
      if (eligible.length !== 1) continue;
      // PR API head_sha may be the branch head. The downloaded manifest binds the synthetic merge.
      return { run, artifact: eligible[0]!, expected: input };
    }
    if (count + 1 < budget) await wait();
  }
  throw new Error("UNAVAILABLE: no successful exact-source producer within finite lookup budget.");
}

export function verifyPickerManifest(
  value: unknown,
  binding: {
    source: string;
    tree: string;
    runId: string;
    attempt: string;
    event: string;
    ref: string;
  },
) {
  proof(value !== null && typeof value === "object", "Missing picker artifact manifest.");
  const receipt = value as Record<string, unknown>;
  proof(
    receipt.schema === 1 &&
      receipt.repository === REPO &&
      receipt.source === binding.source &&
      receipt.tree === binding.tree &&
      receipt.runId === binding.runId &&
      receipt.runAttempt === binding.attempt &&
      receipt.event === binding.event &&
      receipt.ref === binding.ref &&
      receipt.workflow === ".github/workflows/artifact-desktop-mac.yml" &&
      receipt.platform === "darwin" &&
      receipt.architecture === "arm64" &&
      receipt.signed === false &&
      receipt.notarized === false,
    "Producer manifest/source/tree/run binding mismatch.",
  );
  proof(
    typeof receipt.version === "string" &&
      typeof receipt.artifact === "string" &&
      receipt.artifact === `T3-Code-${receipt.version}-arm64.dmg` &&
      typeof receipt.sha256 === "string" &&
      /^[a-f0-9]{64}$/.test(receipt.sha256),
    "Invalid picker artifact payload binding.",
  );
  return receipt as typeof receipt & { version: string; artifact: string; sha256: string };
}

async function downloadZip(id: number, destination: string, signal: AbortSignal) {
  const result = await exec("gh", ["api", `repos/${REPO}/actions/artifacts/${id}/zip`], {
    encoding: "buffer",
    timeout: 180_000,
    maxBuffer: 2 * 1024 * 1024 * 1024,
    signal,
  });
  await NodeFSP.writeFile(destination, result.stdout, { flag: "wx", mode: 0o600 });
}

type PickerErrorDetail = {
  name: string;
  message: string;
  stack?: string;
  code?: string;
  cause?: PickerErrorDetail;
  errors?: PickerErrorDetail[];
  truncated?: boolean;
};

export function pickerErrorDetail(error: unknown): PickerErrorDetail {
  const seen = new Set<unknown>();
  let remaining = 64;
  const detail = (value: unknown, depth: number): PickerErrorDetail => {
    if (remaining-- <= 0 || depth > 8 || seen.has(value))
      return {
        name: "Truncated",
        message: "Error depth, count or cycle bound reached.",
        truncated: true,
      };
    if (!(value instanceof Error))
      return { name: "ThrownValue", message: String(value).slice(0, 8192) };
    seen.add(value);
    const result: PickerErrorDetail = { name: value.name, message: value.message.slice(0, 8192) };
    if (value.stack !== undefined) result.stack = value.stack.slice(0, 16384);
    if ("code" in value && typeof value.code === "string") result.code = value.code;
    if (value.cause !== undefined) result.cause = detail(value.cause, depth + 1);
    if (value instanceof AggregateError) {
      result.errors = value.errors.slice(0, 32).map((nested) => detail(nested, depth + 1));
      if (value.errors.length > 32) result.truncated = true;
    }
    if (value.message.length > 8192 || (value.stack?.length ?? 0) > 16384) result.truncated = true;
    return result;
  };
  return detail(error, 0);
}

type Absence = ReturnType<typeof processAbsence>;
export type PickerPhaseObservation = {
  phase: string;
  pid?: number;
  exitCode?: number | null;
  exitSignal?: NodeJS.Signals | null;
  timedOut?: boolean;
  processAbsence: Absence;
  groupAbsence: Absence;
  evidence: { file: string; status: "copied" | "absent" | "error"; error?: PickerErrorDetail }[];
  error?: PickerErrorDetail;
};

export function requirePickerPhaseCleanup(observation: PickerPhaseObservation, root: string) {
  proof(
    observation.processAbsence === "pass" && observation.groupAbsence === "pass",
    `UNAVAILABLE: picker process/group absence not proved; retained exact root: ${root}`,
  );
  proof(
    !observation.evidence.some((file) => file.status === "error"),
    `Picker diagnostic preservation failed; retained exact root: ${root}`,
  );
}

export async function runPickerPhase(
  executable: string,
  config: Record<string, unknown>,
  environment: NodeJS.ProcessEnv,
  signal: AbortSignal,
  output: string,
  observation: PickerPhaseObservation,
  ports: {
    copyFile?: typeof NodeFSP.copyFile;
    processAbsence?: typeof processAbsence;
  } = {},
) {
  const root = String(config.root),
    phase = String(config.phase),
    file = NodePath.join(root, `config-${phase}.json`),
    failures: unknown[] = [];
  let log: NodeFSP.FileHandle | undefined, timer: ReturnType<typeof setTimeout> | undefined;
  observation.timedOut = false;
  try {
    signal.throwIfAborted();
    log = await NodeFSP.open(NodePath.join(root, `electron-${phase}.log`), "wx", 0o600);
    await NodeFSP.writeFile(file, JSON.stringify(config), { flag: "wx", mode: 0o600 });
    const child = NodeChildProcess.spawn(executable, [`--jones-picker-config=${file}`], {
      env: environment,
      detached: true,
      stdio: ["ignore", log.fd, log.fd],
      signal,
    });
    if (child.pid !== undefined) observation.pid = child.pid;
    let childError: unknown;
    // Wait for close even after cancellation/error so the log and absence probes follow child exit.
    await new Promise<void>((resolve, reject) => {
      child.once("error", (error) => {
        childError = error;
      });
      child.once("close", (code, exitSignal) => {
        observation.exitCode = code;
        observation.exitSignal = exitSignal;
        if (childError !== undefined) reject(childError);
        else if (code === 0 && !observation.timedOut) resolve();
        else reject(new Error(`Picker phase ${phase} exited ${code}.`));
      });
      timer = setTimeout(() => {
        observation.timedOut = true;
        if (observation.pid === undefined) return;
        try {
          process.kill(-observation.pid, "SIGTERM");
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") failures.push(error);
        }
      }, 90_000);
    });
    proof(observation.pid !== undefined, "Picker spawn identity unavailable.");
  } catch (error) {
    failures.unshift(error);
  } finally {
    clearTimeout(timer);
    try {
      await log?.close();
    } catch (error) {
      failures.push(error);
    }
    if (observation.pid !== undefined) {
      const probe = ports.processAbsence ?? processAbsence;
      observation.processAbsence = probe(observation.pid);
      observation.groupAbsence = probe(-observation.pid);
    }
    if (observation.processAbsence !== "pass" || observation.groupAbsence !== "pass")
      failures.push(
        new Error("UNAVAILABLE: picker process/group absence not proved; preserve scratch."),
      );
    // Preserve every available allowlisted diagnostic before the caller can remove its owned root.
    for (const name of [`phase-${phase}.json`, `electron-${phase}.log`, `loader-${phase}.json`]) {
      try {
        await (ports.copyFile ?? NodeFSP.copyFile)(
          NodePath.join(root, name),
          NodePath.join(output, name),
        );
        observation.evidence.push({ file: name, status: "copied" });
      } catch (error) {
        const absent =
          (error as NodeJS.ErrnoException).code === "ENOENT" &&
          (await NodeFSP.lstat(NodePath.join(root, name)).then(
            () => false,
            (cause: NodeJS.ErrnoException) => cause.code === "ENOENT",
          ));
        observation.evidence.push({
          file: name,
          status: absent ? "absent" : "error",
          ...(absent ? {} : { error: pickerErrorDetail(error) }),
        });
        if (!absent) failures.push(error);
      }
    }
  }
  if (failures.length === 0) {
    try {
      // A missing or malformed phase report never qualifies, even when the child exited zero.
      return JSON.parse(await NodeFSP.readFile(NodePath.join(root, `phase-${phase}.json`), "utf8"));
    } catch (error) {
      failures.push(error);
    }
  }
  const error =
    failures.length === 1
      ? failures[0]
      : new AggregateError(failures, "Picker phase failed; additional diagnostics retained.");
  observation.error = pickerErrorDetail(error);
  throw error;
}

async function consume(producerFile: string, output: string) {
  proof(
    process.platform === "darwin" && process.arch === "arm64",
    "Native picker consumer requires hosted macOS arm64.",
  );
  proof(
    process.env.GITHUB_ACTIONS === "true" && process.env.GITHUB_REPOSITORY === REPO,
    "Picker execution is CI-only.",
  );
  const cancellation = qualificationCancellation(),
    signal = cancellation.signal;
  const producer = JSON.parse(await NodeFSP.readFile(producerFile, "utf8"));
  const source = (await command("git", ["rev-parse", "HEAD"])).trim(),
    tree = (await command("git", ["rev-parse", "HEAD^{tree}"])).trim();
  proof(
    source === producer.expected.source && tree === producer.expected.tree,
    "Consumer checkout moved from producer selection.",
  );
  const report: Record<string, unknown> = {
    schema: "jones-packaged-picker/v1",
    verdict: "UNAVAILABLE",
    source,
    tree,
    producer,
    oldArtifact13959290460: "unverified; never consumed",
    limits: [
      "modified disposable COPY layout",
      "unchanged packaged narrow entry bytes",
      "production main/preload bytes not executed",
      "no OS confinement or continuous custody claim",
    ],
  };
  await NodeFSP.mkdir(output, { recursive: true });
  let mountAbsent = true;
  const phaseObservations: PickerPhaseObservation[] = [];
  report.phaseObservations = phaseObservations;
  let primaryFailure: unknown;
  let invocationRoot: string | undefined;
  try {
    report.sourceGraph = scanPickerSourceGraph(
      "apps/desktop/src/jones/updates/pickerVerificationCore.ts",
    );
    await withSmokeScratch(
      await NodeFSP.realpath(process.env.RUNNER_TEMP!),
      async (root) => {
        invocationRoot = root;
        const nonce = NodeCrypto.randomBytes(16).toString("hex"),
          zip = NodePath.join(root, "artifact.zip"),
          archive = NodePath.join(root, "artifact");
        await NodeFSP.writeFile(NodePath.join(root, ".picker-owner"), nonce, {
          flag: "wx",
          mode: 0o600,
        });
        await downloadZip(producer.artifact.id, zip, signal);
        proof(
          producer.artifact.digest === `sha256:${hash(await NodeFSP.readFile(zip))}`,
          "API archive digest mismatch.",
        );
        const names = (await command("/usr/bin/unzip", ["-Z1", zip], signal)).trim().split("\n");
        proof(
          names.length === 4 &&
            names.every((name) => NodePath.basename(name) === name) &&
            names.includes("ARTIFACT.json") &&
            names.includes("SOURCE_COMMIT"),
          "Unexpected producer archive inventory.",
        );
        await command("/usr/bin/ditto", ["-x", "-k", zip, archive], signal);
        const receipt = verifyPickerManifest(
          JSON.parse(await NodeFSP.readFile(NodePath.join(archive, "ARTIFACT.json"), "utf8")),
          {
            source,
            tree,
            runId: String(producer.run.id),
            attempt: String(producer.run.run_attempt),
            event: producer.expected.event,
            ref:
              producer.expected.event === "pull_request"
                ? `refs/pull/${producer.expected.pr}/merge`
                : "refs/heads/main",
          },
        );
        proof(
          (await NodeFSP.readFile(NodePath.join(archive, "SOURCE_COMMIT"), "utf8")).trim() ===
            source,
          "Producer SOURCE_COMMIT mismatch.",
        );
        const dmg = NodePath.join(archive, receipt.artifact),
          dmgHash = hash(await NodeFSP.readFile(dmg));
        proof(
          dmgHash === receipt.sha256 &&
            (await NodeFSP.readFile(`${dmg}.sha256`, "utf8")).trim() ===
              `${dmgHash}  ${receipt.artifact}`,
          "DMG checksum binding failed.",
        );
        report.binding = {
          receipt,
          artifact: producer.artifact,
          dmgBytes: (await NodeFSP.stat(dmg)).size,
        };
        const mount = NodePath.join(root, "mount"),
          applications = NodePath.join(root, "Applications");
        await NodeFSP.mkdir(mount);
        await NodeFSP.mkdir(applications);
        mountAbsent = false;
        let appPath = "",
          executable = "",
          bundleBefore: Record<string, string> = {};
        await command(
          "/usr/bin/hdiutil",
          ["attach", "-readonly", "-nobrowse", "-noautoopen", "-mountpoint", mount, dmg],
          signal,
        );
        try {
          const apps = (await NodeFSP.readdir(mount)).filter((name) => name.endsWith(".app"));
          proof(apps.length === 1, "Producer must contain one app.");
          const mounted = NodePath.join(mount, apps[0]!);
          appPath = NodePath.join(applications, apps[0]!);
          requirePickerCopyPath(appPath);
          const plist = JSON.parse(
            await command(
              "/usr/bin/plutil",
              ["-convert", "json", "-o", "-", NodePath.join(mounted, "Contents/Info.plist")],
              signal,
            ),
          );
          proof(
            typeof plist.CFBundleExecutable === "string" &&
              NodePath.basename(plist.CFBundleExecutable) === plist.CFBundleExecutable &&
              plist.CFBundleShortVersionString === receipt.version,
            "Bundle identity mismatch.",
          );
          for (const relative of [
            "Contents/Resources/app.asar",
            "Contents/Info.plist",
            `Contents/MacOS/${plist.CFBundleExecutable}`,
          ])
            bundleBefore[relative] = hash(await NodeFSP.readFile(NodePath.join(mounted, relative)));
          await command("/usr/bin/ditto", [mounted, appPath], signal);
          executable = NodePath.join(appPath, "Contents/MacOS", plist.CFBundleExecutable);
        } finally {
          await command("/usr/bin/hdiutil", ["detach", mount], signal);
        }
        // hdiutil info is checked directly; an uncertain detach keeps the invocation root.
        const info = await command("/usr/bin/hdiutil", ["info", "-plist"], signal);
        proof(!info.includes(`<string>${mount}</string>`), "Mount remains after detach.");
        mountAbsent = true;
        for (const [relative, digest] of Object.entries(bundleBefore))
          proof(
            hash(await NodeFSP.readFile(NodePath.join(appPath, relative))) === digest,
            "Copied bundle differs from mounted payload.",
          );
        report.fuses = readPickerFuses(
          await NodeFSP.readFile(
            NodePath.join(
              appPath,
              "Contents/Frameworks/Electron Framework.framework/Electron Framework",
            ),
          ),
        );
        const resources = NodePath.join(appPath, "Contents/Resources"),
          asar = NodePath.join(resources, "app.asar");
        // Read ASAR bytes with the checkout parser, never by running its normal entry.
        const asarApi = await import("@electron/asar");
        const pkg = JSON.parse(asarApi.extractFile(asar, "package.json").toString());
        proof(
          pkg.version === receipt.version &&
            pkg.jonesSource?.sha === source &&
            pkg.jonesSource?.tree === tree &&
            pkg.jonesSource?.repository === REPO,
          "Embedded app identity mismatch.",
        );
        const electronVersion = JSON.parse(
          await command(
            "/usr/bin/plutil",
            [
              "-convert",
              "json",
              "-o",
              "-",
              NodePath.join(
                appPath,
                "Contents/Frameworks/Electron Framework.framework/Resources/Info.plist",
              ),
            ],
            signal,
          ),
        ).CFBundleVersion;
        proof(
          electronVersion === "44.10.1",
          "UNAVAILABLE: Electron source/search-order version mismatch.",
        );
        const scans = {
          main: scanPickerBundle(asarApi.extractFile(asar, `${ENTRY}Main.cjs`).toString(), "main"),
          preload: scanPickerBundle(
            asarApi.extractFile(asar, `${ENTRY}Preload.cjs`).toString(),
            "preload",
          ),
        };
        report.entries = scans;
        report.inventory = asarApi
          .listPackage(asar, { isPack: false })
          .filter((file) => file.includes("dist-electron/"))
          .sort();
        proof(
          !NodeFS.existsSync(NodePath.join(resources, "app")),
          "Unexpected existing app loader.",
        );
        const retained = NodePath.join(resources, "picker-retained.asar");
        await NodeFSP.rename(asar, retained);
        proof(
          hash(await NodeFSP.readFile(retained)) === bundleBefore["Contents/Resources/app.asar"],
          "Retained archive bytes changed.",
        );
        const loader = NodePath.join(resources, "app");
        await NodeFSP.mkdir(loader);
        await NodeFSP.writeFile(
          NodePath.join(loader, "package.json"),
          JSON.stringify({
            name: "jones-picker-verification",
            version: receipt.version,
            main: "picker-loader.cjs",
          }),
        );
        await NodeFSP.copyFile(
          NodeURL.fileURLToPath(new URL("./packaged-picker-electron-shim.cjs", import.meta.url)),
          NodePath.join(loader, "picker-loader.cjs"),
        );
        const home = NodePath.join(root, "state"),
          profile = NodePath.join(home, "profile"),
          osHome = NodePath.join(root, "os-home"),
          temp = NodePath.join(root, "electron-temp");
        for (const directory of [
          profile,
          NodePath.join(home, "userdata"),
          osHome,
          temp,
          NodePath.join(root, "electron-user"),
          NodePath.join(root, "electron-session"),
          NodePath.join(root, "electron-logs"),
          NodePath.join(root, "electron-crashes"),
        ])
          await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
        await NodeFSP.writeFile(NodePath.join(home, "userdata/statev2.sqlite"), "");
        await NodeFSP.writeFile(NodePath.join(home, "userdata/environment-id"), "picker-synthetic");
        // Preserve the core's fresh-home profile name while keeping HOME completely synthetic.
        const environment = {
          HOME: osHome,
          TMPDIR: temp,
          PATH: "/usr/bin:/bin",
          LANG: "en_US.UTF-8",
          ELECTRON_ENABLE_LOGGING: "1",
        };
        const reports = [];
        for (const phase of ["catalog", "persisted", "inflight", "held"]) {
          const content = NodePath.join(root, "index.html");
          await NodeFSP.writeFile(
            content,
            "<!doctype html><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; script-src 'none'; connect-src 'none'\"><title>Synthetic picker IPC</title>",
          );
          const reportPath = NodePath.join(root, `phase-${phase}.json`);
          const observation: PickerPhaseObservation = {
            phase,
            processAbsence: "unknown",
            groupAbsence: "unknown",
            evidence: [],
          };
          phaseObservations.push(observation);
          const phaseReport = await runPickerPhase(
            executable,
            {
              root,
              nonce,
              home,
              profile,
              appRoot: retained,
              appPath,
              executablePath: executable,
              version: receipt.version,
              source,
              phase,
              osHome,
              temp,
              driver: NodeURL.fileURLToPath(
                new URL("./packaged-picker-driver.mjs", import.meta.url),
              ),
              main: NodePath.join(retained, `${ENTRY}Main.cjs`),
              preload: NodePath.join(retained, `${ENTRY}Preload.cjs`),
              content,
              report: reportPath,
            },
            environment,
            signal,
            output,
            observation,
          );
          reports.push(phaseReport);
        }
        proof(
          hash(await NodeFSP.readFile(retained)) === bundleBefore["Contents/Resources/app.asar"] &&
            hash(await NodeFSP.readFile(NodePath.join(appPath, "Contents/Info.plist"))) ===
              bundleBefore["Contents/Info.plist"] &&
            hash(await NodeFSP.readFile(executable)) ===
              bundleBefore[NodePath.relative(appPath, executable)],
          "Packaged archive/identity/executable changed.",
        );
        report.phases = reports;
        report.verdict = evaluatePickerReports(reports);
      },
      async (root) => {
        proof(mountAbsent, `Unknown mount state; retained exact root: ${root}`);
        for (const observation of phaseObservations) requirePickerPhaseCleanup(observation, root);
      },
    );
    report.cleanup = "proved; exact invocation root removed";
  } catch (error) {
    primaryFailure = error;
    report.error = pickerErrorDetail(error);
    report.cleanup =
      mountAbsent &&
      phaseObservations.every(
        (phase) =>
          phase.processAbsence === "pass" &&
          phase.groupAbsence === "pass" &&
          !phase.evidence.some((file) => file.status === "error"),
      )
        ? "exact invocation cleanup attempted; see nested errors for cleanup failure"
        : "retained; unknown process/mount or diagnostic copy failure";
  } finally {
    cancellation.dispose();
    if (invocationRoot !== undefined) {
      report.scratch = await NodeFSP.lstat(invocationRoot).then(
        () => ({ root: invocationRoot, pathAbsence: "fail" }),
        (error: NodeJS.ErrnoException) => ({
          root: invocationRoot,
          pathAbsence: error.code === "ENOENT" ? "pass" : "unknown",
          ...(error.code === "ENOENT" ? {} : { error: pickerErrorDetail(error) }),
        }),
      );
    }
    try {
      await NodeFSP.writeFile(
        NodePath.join(output, "qualification.json"),
        JSON.stringify(report, null, 2),
      );
    } catch (error) {
      primaryFailure =
        primaryFailure === undefined
          ? error
          : new AggregateError(
              [primaryFailure, error],
              "Qualification and report publication failed.",
            );
    }
  }
  if (primaryFailure !== undefined) throw primaryFailure;
}

if (
  process.argv[1] !== undefined &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  const mode = process.argv[2];
  if (mode === "--scan") {
    const file = process.argv[3];
    proof(file !== undefined, "Missing bundle NodePath.");
    console.log(
      JSON.stringify(
        scanPickerBundle(
          await NodeFSP.readFile(file, "utf8"),
          process.argv[4] === "preload" ? "preload" : "main",
        ),
      ),
    );
  } else if (mode === "--source-graph")
    console.log(
      JSON.stringify(
        scanPickerSourceGraph("apps/desktop/src/jones/updates/pickerVerificationCore.ts"),
        null,
        2,
      ),
    );
  else if (mode === "--locate") {
    const source = (await command("git", ["rev-parse", "HEAD"])).trim(),
      tree = (await command("git", ["rev-parse", "HEAD^{tree}"])).trim();
    const event = JSON.parse(await NodeFSP.readFile(process.env.GITHUB_EVENT_PATH!, "utf8"));
    const input =
      process.env.GITHUB_EVENT_NAME === "pull_request"
        ? {
            source,
            tree,
            event: "pull_request" as const,
            pr: Number(event.number),
            head: String(event.pull_request.head.sha),
          }
        : {
            source,
            tree,
            event: event.workflow_run.event as "push" | "schedule",
            runId: Number(event.workflow_run.id),
          };
    const producer = await locatePickerProducer(
      input,
      async (endpoint) => JSON.parse(await command("gh", ["api", endpoint])),
      () => new Promise((resolve) => setTimeout(resolve, 20_000)),
    );
    await NodeFSP.writeFile("picker-producer.json", JSON.stringify(producer, null, 2));
  } else if (mode === "--consume")
    await consume(NodePath.resolve(process.argv[3]!), NodePath.resolve(process.argv[4]!));
  else throw new Error("Expected --scan, --source-graph, --locate or --consume.");
}
