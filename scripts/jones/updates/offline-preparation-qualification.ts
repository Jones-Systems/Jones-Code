// @effect-diagnostics nodeBuiltinImport:off -- This fixed hosted adapter owns synthetic qualification roots and bounded artifact evidence.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeURL from "node:url";
import { verifyFunctionalPreparationGraph } from "./functional-policy.ts";
import { buildAndStagePreparationProducer } from "./offline-preparation-producer-loader.mjs";

function readArtifact(path: string, limit: number): Buffer {
  const fd = NodeFS.openSync(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  let primary: unknown, bytes: Buffer | undefined;
  try {
    const before = NodeFS.fstatSync(fd);
    if (!before.isFile() || before.nlink !== 1 || before.size > limit)
      throw new Error("Hosted graph input is not a bounded regular file.");
    const buffer = Buffer.alloc(before.size + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = NodeFS.readSync(fd, buffer, offset, buffer.length - offset, offset);
      if (count === 0) break;
      offset += count;
    }
    const after = NodeFS.fstatSync(fd),
      current = NodeFS.lstatSync(path);
    if (
      offset !== before.size ||
      [after, current].some(
        (value) =>
          value.dev !== before.dev ||
          value.ino !== before.ino ||
          value.size !== before.size ||
          value.mtimeMs !== before.mtimeMs ||
          value.ctimeMs !== before.ctimeMs ||
          value.nlink !== 1,
      )
    )
      throw new Error("Hosted graph input changed while reading.");
    bytes = buffer.subarray(0, offset);
  } catch (cause) {
    primary = cause;
  }
  try {
    NodeFS.closeSync(fd);
  } catch (cause) {
    throw Object.assign(
      new AggregateError(
        primary === undefined ? [cause] : [primary, cause],
        "Hosted graph descriptor closure is unproved.",
        { cause: primary ?? cause },
      ),
      { preserveScratch: true },
    );
  }
  if (primary !== undefined) throw primary;
  return bytes!;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Hosted graph requires an object.");
  return value as Record<string, unknown>;
}

// Every mutation starts from the actual emitted artifact which just passed the policy.
// These cases exercise refusals without importing code, modifying source or minting handles.
function verifyMutationRejections(
  repoRoot: string,
  reportBytes: Buffer,
  entryBytes: Buffer,
): string[] {
  const rejected: string[] = [];
  const expectedFailures: Readonly<Record<string, string>> = {
    "entry-byte-drift": "Functional preparation entry is outside the reviewed worker policy.",
    "claimed-execution": "Preparation graph is outside the nonexecuted observation contract.",
    "claimed-qualification": "Preparation graph is outside the nonexecuted observation contract.",
    "claimed-preparation": "Preparation graph is outside the nonexecuted observation contract.",
    "wrong-entry": "Preparation graph is outside the nonexecuted observation contract.",
    "extra-output": "Preparation emitted inventory differs from the reviewed worker.",
    "extra-asset": "Preparation emitted inventory differs from the reviewed worker.",
    "dynamic-loader": "Preparation import inventory differs from the reviewed worker.",
    "producer-builtin": "Preparation import inventory differs from the reviewed worker.",
    "computed-builtin": "Preparation import inventory differs from the reviewed worker.",
    "runtime-region": "Preparation generated runtime differs from the reviewed worker.",
    "runtime-hash": "Preparation generated runtime differs from the reviewed worker.",
    "external-dynamic-evidence": "Preparation import evidence differs from the reviewed worker.",
    "missing-source": "Preparation source inventory differs from the fixed contract.",
    "stale-source-bytes": "Preparation graph source bytes are stale: pnpm-lock.yaml",
    "version-disagreement": "Preparation graph version differs from release manifests.",
    "unadmitted-overlay": "Preparation source inventory differs from the fixed contract.",
    "duplicate-overlay": "Preparation source inventory differs from the fixed contract.",
  };
  const mustReject = (name: string, candidateReport: Uint8Array, candidateEntry = entryBytes) => {
    try {
      verifyFunctionalPreparationGraph({
        repoRoot,
        reportBytes: candidateReport,
        entryBytes: candidateEntry,
      });
    } catch (cause) {
      if (
        !(cause instanceof Error) ||
        "preserveScratch" in cause ||
        cause.message !== expectedFailures[name]
      )
        throw cause;
      rejected.push(name);
      return;
    }
    throw new Error(`Worker policy accepted a changed artifact: ${name}`);
  };
  const changed = Buffer.from(entryBytes);
  changed[changed.length - 1] = changed[changed.length - 1]! ^ 1;
  mustReject("entry-byte-drift", reportBytes, changed);
  const mutations: readonly [string, (report: Record<string, unknown>) => void][] = [
    [
      "claimed-execution",
      (report) => {
        report.executed = true;
      },
    ],
    [
      "claimed-qualification",
      (report) => {
        report.qualification = "qualified";
      },
    ],
    [
      "claimed-preparation",
      (report) => {
        report.preparationCapable = true;
      },
    ],
    [
      "wrong-entry",
      (report) => {
        report.entry = "src/server.ts";
      },
    ],
    [
      "extra-output",
      (report) => {
        const files = record(report.emitted).files;
        if (!Array.isArray(files)) throw new Error("Expected actual emitted files.");
        files.push({ fileName: "external.mjs", sha256: "0".repeat(64), size: 0 });
      },
    ],
    [
      "extra-asset",
      (report) => {
        record(report.emitted).assets = ["external.js"];
      },
    ],
    [
      "dynamic-loader",
      (report) => {
        const syntax = record(record(report.emitted).syntax);
        const imports = syntax.imports;
        if (!Array.isArray(imports)) throw new Error("Expected actual import array.");
        record(imports[0]).kind = "dynamic";
      },
    ],
    [
      "producer-builtin",
      (report) => {
        const imports = record(record(report.emitted).syntax).imports;
        if (!Array.isArray(imports)) throw new Error("Expected actual import array.");
        record(imports[0]).specifier = "node:child_process";
      },
    ],
    [
      "computed-builtin",
      (report) => {
        const imports = record(record(report.emitted).syntax).imports;
        if (!Array.isArray(imports)) throw new Error("Expected actual import array.");
        record(imports[7]).computed = true;
      },
    ],
    [
      "runtime-region",
      (report) => {
        record(record(report.emitted).generatedRuntime).startByte = 0;
      },
    ],
    [
      "runtime-hash",
      (report) => {
        record(record(report.emitted).generatedRuntime).sha256 = "0".repeat(64);
      },
    ],
    [
      "external-dynamic-evidence",
      (report) => {
        record(record(record(report.emitted).importEvidence).dynamic).emitted = ["external.js"];
      },
    ],
    [
      "missing-source",
      (report) => {
        delete record(record(report.source).hashes)["pnpm-lock.yaml"];
      },
    ],
    [
      "stale-source-bytes",
      (report) => {
        record(record(report.source).hashes)["pnpm-lock.yaml"] = "0".repeat(64);
      },
    ],
    [
      "version-disagreement",
      (report) => {
        record(report.source).version = "0.0.0-preview.20000101.1";
      },
    ],
    [
      "unadmitted-overlay",
      (report) => {
        record(report.source).buildOnlyOverlays = ["package.json"];
      },
    ],
    [
      "duplicate-overlay",
      (report) => {
        record(report.source).buildOnlyOverlays = [
          "apps/server/package.json",
          "apps/server/package.json",
        ];
      },
    ],
  ];
  for (const [name, mutate] of mutations) {
    const report = record(JSON.parse(reportBytes.toString("utf8")) as unknown);
    mutate(report);
    mustReject(name, Buffer.from(JSON.stringify(report)));
  }
  return rejected;
}

const nativeCaseNames = [
  "apply",
  "reconcile",
  "repeat-plan",
  "repeat-apply",
  "durable-plan-drift",
  "wrong-scope",
  "paused-cancel",
  "closed-query-failure",
  "close-uncertainty",
  "custody-uncertainty",
];

// Diagnostic fields are allowlisted; never serialize stacks, environments, handles or payload objects.
function diagnosticText(value: unknown): string | undefined {
  if (typeof value !== "string" && !Buffer.isBuffer(value)) return undefined;
  const length = value.length;
  const prefix =
    typeof value === "string" ? value.slice(0, 16384) : value.subarray(0, 16384).toString("utf8");
  // Drop an incomplete input tail before redaction so truncation cannot hide a secret marker.
  const bounded = length > 16384 ? prefix.slice(0, Math.max(0, prefix.lastIndexOf("\n"))) : prefix;
  const text = bounded
    .replace(/-----BEGIN[\s\S]*/g, "[redacted key material]")
    .replace(/[[{][\s\S]*/g, "[redacted structured payload]")
    .split(/\r?\n/)
    .map((line) =>
      /bearer|authorization|cookie|token|secret|password|credential|private[ _-]?(?:key|payload)|payload|-----BEGIN|(?:gh[pousr]_|github_pat_|sk-)[A-Za-z0-9_-]+|[A-Za-z0-9_-]{48,}|https?:\/\/|[{}]/i.test(
        line,
      )
        ? "[redacted diagnostic line]"
        : line.replace(/[^\x20-\x7e\t\u0080-\uffff]/g, "?"),
    )
    .join("\n");
  return text.slice(0, 2048) + (length > 16384 || text.length > 2048 ? "[truncated]" : "");
}

/** Bounded failure evidence survives helper cleanup without copying arbitrary error properties. */
export function qualificationFailureDiagnostic(cause: unknown): Record<string, unknown> {
  const seen = new Set<object>();
  let count = 0;
  const field = (value: object, key: string): unknown => {
    const property = Object.getOwnPropertyDescriptor(value, key);
    return property && "value" in property ? property.value : undefined;
  };
  const visit = (value: unknown, depth: number): Record<string, unknown> => {
    if (depth >= 8 || count >= 16) return { omitted: "diagnostic limit" };
    count += 1;
    if (value === null || typeof value !== "object")
      return { message: diagnosticText(value) ?? "Non-error failure." };
    if (seen.has(value)) return { omitted: "repeated error reference" };
    seen.add(value);
    const result: Record<string, unknown> = {
      message: diagnosticText(field(value, "message")) ?? "Qualification failed.",
    };
    for (const key of ["status", "exitCode", "childPid"]) {
      const item = field(value, key);
      if (item === null || (typeof item === "number" && Number.isSafeInteger(item)))
        result[key] = item;
    }
    for (const key of ["signal", "code", "stdout", "stderr", "retainedRoot", "stageRoot"]) {
      const raw = field(value, key);
      const item = diagnosticText(raw);
      if (item !== undefined) result[key] = item;
      else if (key === "signal" && raw === null) result[key] = null;
    }
    for (const key of ["preserveScratch", "preserveStageRoot"]) {
      const item = field(value, key);
      if (typeof item === "boolean") result[key] = item;
    }
    const cleanup = field(value, "cleanup");
    if (cleanup !== null && typeof cleanup === "object") {
      const disposition = field(cleanup, "disposition");
      result.cleanup = {
        disposition:
          typeof disposition === "string" &&
          ["removed", "not-created", "retained-uncertain"].includes(disposition)
            ? disposition
            : "unknown",
        ...(typeof field(cleanup, "root") === "string"
          ? { root: diagnosticText(field(cleanup, "root")) }
          : {}),
      };
    }
    const nested = field(value, "cause");
    if (nested !== undefined) result.cause = visit(nested, depth + 1);
    const children = field(value, "errors");
    if (Array.isArray(children)) {
      result.errors = children.slice(0, 8).map((child) => visit(child, depth + 1));
      if (children.length > 8) result.omittedErrors = children.length - 8;
    }
    return result;
  };
  return visit(cause, 0);
}

function retainRoots(cause: unknown, seen = new Set<unknown>()): boolean {
  if (typeof cause !== "object" || cause === null || seen.has(cause)) return false;
  if (seen.size >= 64) return true;
  seen.add(cause);
  const value = cause as Record<string, unknown>;
  return (
    value.preserveScratch === true ||
    value.preserveStageRoot === true ||
    typeof value.retainedRoot === "string" ||
    retainRoots(value.cause, seen) ||
    (cause instanceof AggregateError && cause.errors.some((item) => retainRoots(item, seen)))
  );
}

/** The fixed hosted mode has no caller-supplied producer, callback, namespace or handle. */
export async function runHostedNativePreparation(
  repoRoot: string,
  qualificationRoot: string,
): Promise<void> {
  if (
    NodeProcess.platform !== "darwin" ||
    NodeProcess.arch !== "arm64" ||
    process.versions.node !== "24.13.1" ||
    process.execArgv.length !== 0 ||
    process.env.NODE_OPTIONS ||
    process.env.NODE_PATH
  )
    throw new Error(
      "Native qualification requires fresh Node 24.13.1 on Darwin arm64 without preload options.",
    );
  const source = NodeFS.realpathSync(repoRoot),
    root = NodeFS.realpathSync(qualificationRoot);
  const rootInfo = NodeFS.lstatSync(root, { bigint: true });
  if (
    root !== qualificationRoot ||
    !rootInfo.isDirectory() ||
    rootInfo.uid !== BigInt(process.getuid!()) ||
    (rootInfo.mode & 0o077n) !== 0n ||
    NodeFS.readdirSync(root).some(
      (name) => !["home", "tmp", "driver.stdout", "driver.stderr", "storage.txt"].includes(name),
    ) ||
    process.env.HOME !== NodePath.join(root, "home") ||
    process.env.TMPDIR !== NodePath.join(root, "tmp") ||
    source === root ||
    source.startsWith(root + NodePath.sep) ||
    root.startsWith(source + NodePath.sep)
  )
    throw new Error("Native qualification roots must be private, fresh and separate from source.");
  const disk = NodeFS.statfsSync(root, { bigint: true });
  if (disk.bavail * disk.bsize < 4n * 1024n * 1024n * 1024n)
    throw new Error("Native qualification requires at least 4 GiB of available runner storage.");
  const owned = new Map<string, { dev: bigint; ino: bigint; uid: bigint }>();
  for (const name of ["home", "tmp"]) {
    const path = NodePath.join(root, name),
      info = NodeFS.lstatSync(path, { bigint: true });
    if (
      !info.isDirectory() ||
      info.uid !== rootInfo.uid ||
      (info.mode & 0o077n) !== 0n ||
      NodeFS.readdirSync(path).length !== 0
    )
      throw new Error("Native qualification HOME and TMPDIR must be empty owned directories.");
    owned.set(path, info);
  }
  const evidence = NodePath.join(root, "evidence");
  NodeFS.mkdirSync(evidence, { mode: 0o700 });
  const evidenceInfo = NodeFS.lstatSync(evidence, { bigint: true });
  const verifyEvidenceCustody = () => {
    for (const [path, original] of [
      [root, rootInfo],
      [evidence, evidenceInfo],
    ] as const) {
      const current = NodeFS.lstatSync(path, { bigint: true });
      if (
        current.dev !== original.dev ||
        current.ino !== original.ino ||
        current.uid !== original.uid ||
        !current.isDirectory()
      )
        throw Object.assign(new Error("Hosted evidence directory custody changed."), {
          preserveScratch: true,
        });
    }
  };
  const stage = NodePath.join(root, "stage"),
    resources = NodePath.join(stage, "resources");
  const controller = new AbortController();
  const cancelled = () => controller.abort(new Error("Hosted native qualification cancelled."));
  process.once("SIGINT", cancelled);
  process.once("SIGTERM", cancelled);
  let primary: unknown;
  let completed: Awaited<ReturnType<typeof buildAndStagePreparationProducer>> | undefined;
  try {
    NodeFS.mkdirSync(stage, { mode: 0o700 });
    owned.set(stage, NodeFS.lstatSync(stage, { bigint: true }));
    NodeFS.mkdirSync(resources, { mode: 0o700 });
    const graph = record(
      JSON.parse(
        readArtifact(
          NodePath.join(source, "apps/server/dist-exe/preparation-candidate/candidate-graph.json"),
          16 * 1024 * 1024,
        ).toString("utf8"),
      ) as unknown,
    );
    const identity = record(graph.source);
    const sourceSha = process.env.JONES_EXPECTED_SOURCE_SHA,
      sourceTree = process.env.JONES_EXPECTED_SOURCE_TREE;
    if (
      sourceSha === undefined ||
      sourceTree === undefined ||
      !/^[a-f0-9]{40}$/.test(sourceSha) ||
      !/^[a-f0-9]{40}$/.test(sourceTree) ||
      identity.sha !== sourceSha ||
      identity.tree !== sourceTree ||
      typeof identity.version !== "string"
    )
      throw new Error("Native qualification graph differs from its checkout identity.");
    completed = await buildAndStagePreparationProducer({
      sourceRoot: source,
      stageRoot: stage,
      stageResourcesDir: resources,
      expectedSource: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha,
        sourceTree,
        version: identity.version,
      },
      signal: controller.signal,
    });
    controller.signal.throwIfAborted();
    if (
      completed.cleanup.disposition !== "removed" ||
      completed.handleConsumption.recreatedRejected !== 4 ||
      completed.handleConsumption.consumedRejected !== true ||
      completed.diagnosticEvidence.nativeColdOwnerQualified !== false ||
      completed.diagnosticEvidence.stateAdmissionAllowed !== false ||
      completed.diagnosticEvidence.activationAllowed !== false
    )
      throw Object.assign(
        new Error("Native qualification returned an incomplete fixed lifecycle."),
        { preserveScratch: true },
      );
    const native = record(JSON.parse(completed.diagnosticEvidence.nativeQualification) as unknown);
    if (
      native.qualification !== "synthetic-native-preparation" ||
      !Array.isArray(native.cases) ||
      native.cases.length !== nativeCaseNames.length ||
      native.cases.some((value, index) => {
        const cases = record(value).cases;
        return (
          !Array.isArray(cases) ||
          cases.length !== 1 ||
          record(cases[0]).case !== nativeCaseNames[index]
        );
      })
    )
      throw Object.assign(new Error("Native qualification did not return all ten cases."), {
        preserveScratch: true,
      });
    // Move the exact checked stage into deliverable custody; no duplicate runtime copy is made.
    verifyEvidenceCustody();
    NodeFS.renameSync(stage, NodePath.join(evidence, "stage"));
    owned.delete(stage);
  } catch (cause) {
    primary = cause;
  } finally {
    process.removeListener("SIGINT", cancelled);
    process.removeListener("SIGTERM", cancelled);
    controller.abort(new Error("Hosted qualification lifetime ended."));
  }
  if (!retainRoots(primary)) {
    for (const [path, original] of [...owned].toReversed()) {
      try {
        const current = NodeFS.lstatSync(path, { bigint: true });
        if (
          current.dev !== original.dev ||
          current.ino !== original.ino ||
          current.uid !== original.uid ||
          !current.isDirectory()
        )
          throw new Error("Hosted qualification root identity changed before cleanup.");
        NodeFS.rmSync(path, { recursive: true });
        if (NodeFS.existsSync(path))
          throw new Error("Hosted qualification cleanup was not observed.");
      } catch (cause) {
        primary = Object.assign(
          new AggregateError(
            primary === undefined ? [cause] : [primary, cause],
            "Hosted qualification cleanup remains uncertain.",
            { cause: primary ?? cause },
          ),
          { preserveScratch: true },
        );
        break;
      }
    }
  }
  const receipt = {
    schema: 1,
    classification: "hosted-synthetic-preparation-qualification",
    status: primary === undefined ? "passed" : "failed",
    retainedRoots: retainRoots(primary) ? root : null,
    nativeColdOwnerQualified: false,
    stateAdmissionAllowed: false,
    activationAllowed: false,
    ...(completed === undefined
      ? {}
      : {
          runtime: completed.bootstrapRuntime,
          diagnosticEvidence: completed.diagnosticEvidence,
          handleConsumption: completed.handleConsumption,
          captureCleanup: completed.cleanup,
        }),
    ...(primary === undefined
      ? {}
      : {
          error: qualificationFailureDiagnostic(primary).message,
          failureDiagnostic: qualificationFailureDiagnostic(primary),
        }),
  };
  const bytes = Buffer.from(JSON.stringify(receipt) + "\n");
  if (bytes.length > 8 * 1024 * 1024)
    throw Object.assign(new Error("Hosted qualification evidence exceeds its bound."), {
      preserveScratch: true,
    });
  verifyEvidenceCustody();
  NodeFS.writeFileSync(NodePath.join(evidence, "result.json"), bytes, { flag: "wx", mode: 0o600 });
  if (primary !== undefined) throw primary;
  process.stdout.write(
    JSON.stringify({
      status: "passed",
      qualification: "synthetic-native-preparation",
      cases: nativeCaseNames,
      evidence,
      nativeColdOwnerQualified: false,
      stateAdmissionAllowed: false,
      activationAllowed: false,
    }) + "\n",
  );
}

if (import.meta.main) {
  if (
    process.argv.length !== 3 ||
    !["--verify-worker-graph", "--native-preparation"].includes(process.argv[2]!)
  )
    throw new Error("Expected one fixed hosted qualification mode.");
  const repoRoot = NodeFS.realpathSync(
    NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
  );
  if (process.argv[2] === "--native-preparation") {
    const root = process.env.JONES_QUALIFICATION_ROOT;
    if (root === undefined || !NodePath.isAbsolute(root))
      throw new Error("Hosted qualification root is missing.");
    try {
      await runHostedNativePreparation(repoRoot, root);
    } catch (cause) {
      process.stderr.write(
        JSON.stringify({
          status: "failed",
          retainedRoot: retainRoots(cause) ? root : null,
          error: qualificationFailureDiagnostic(cause).message,
          failureDiagnostic: qualificationFailureDiagnostic(cause),
        }) + "\n",
      );
      process.exitCode = 1;
    }
  } else {
    const candidate = NodePath.join(repoRoot, "apps/server/dist-exe/preparation-candidate");
    if (process.execArgv.length !== 0 || process.env.NODE_OPTIONS)
      throw new Error(
        "Hosted worker verification requires a fresh Node process without preload options.",
      );
    const reportBytes = readArtifact(
      NodePath.join(candidate, "candidate-graph.json"),
      16 * 1024 * 1024,
    );
    const entryBytes = readArtifact(
      NodePath.join(candidate, "offlinePreparationEntry.mjs"),
      32 * 1024 * 1024,
    );
    const source = record(record(JSON.parse(reportBytes.toString("utf8")) as unknown).source);
    const expectedSha = process.env.JONES_EXPECTED_SOURCE_SHA;
    const expectedTree = process.env.JONES_EXPECTED_SOURCE_TREE;
    if (
      expectedSha === undefined ||
      !/^[a-f0-9]{40}$/.test(expectedSha) ||
      expectedTree === undefined ||
      !/^[a-f0-9]{40}$/.test(expectedTree) ||
      source.sha !== expectedSha ||
      source.tree !== expectedTree
    )
      throw new Error("Hosted graph differs from the independently observed checkout identity.");
    const binding = verifyFunctionalPreparationGraph({ repoRoot, reportBytes, entryBytes });
    const mutationRejections = verifyMutationRejections(repoRoot, reportBytes, entryBytes);
    verifyFunctionalPreparationGraph({ repoRoot, reportBytes, entryBytes });
    process.stdout.write(
      `${JSON.stringify({
        classification: "worker-byte-policy-only",
        binding,
        mutationRejections,
        emittedCodeExecuted: false,
        nativeQualification: false,
        nativeColdOwnerQualified: false,
        stateAdmissionAllowed: false,
        activationAllowed: false,
      })}\n`,
    );
  }
}
