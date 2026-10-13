// @effect-diagnostics nodeBuiltinImport:off -- Hosted graph verification reads fixed artifacts without importing emitted code.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { verifyFunctionalPreparationGraph } from "./functional-policy.ts";

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

// No producer import or native-build mode is admitted until the changed emitted producer is reviewed.
if (import.meta.main) {
  if (process.argv.length !== 3 || process.argv[2] !== "--verify-worker-graph")
    throw new Error("Only --verify-worker-graph is available; producer execution remains held.");
  const repoRoot = NodeFS.realpathSync(
    NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
  );
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
