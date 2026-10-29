import * as HostProcess from "../../../packages/shared/src/HostProcess.ts";
// Serial per-OS admission and evidence fan-in. Every required command is attempted;
// missing inputs remain unknown. No release, deployment, live state or provider call.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import { qualificationDecision, nativeBlockMatrixDecision } from "./qualificationResult.mjs";
const [output, installedRoot, scope = "full", priorDirectory] = process.argv.slice(2);
const diagnosticScope = ["observer-diagnostic", "observer-control"].includes(scope);
if (diagnosticScope && (HostProcess.Platform.defaultValue() !== "linux" || process.env.GITHUB_EVENT_NAME !== "workflow_dispatch" || (scope === "observer-control" && !NodePath.isAbsolute(priorDirectory ?? "")))) throw new Error("Manual Linux diagnostic route requires its bound inputs");
if (!["full", "source-evidence", "observer-diagnostic", "observer-control"].includes(scope)) throw new Error("Unknown qualification scope");
if (!NodePath.isAbsolute(output ?? "") || !NodePath.isAbsolute(installedRoot ?? ""))
  throw new Error("ci-run.mjs <absolute reports directory> <absolute extracted archive directory>");
await NodeFSP.mkdir(output, { recursive: true, mode: 0o700 });
const root = NodePath.resolve(import.meta.dirname, "../../.."),
  source = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim();
const sourceTree = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD^{tree}"], {
  cwd: root,
  encoding: "utf8",
}).trim();
const env = {
  ...process.env,
  JONES_SP_SOURCE_SHA: source,
  JONES_SP_SOURCE_TREE: sourceTree,
  T3CODE_DEV_SEED: "0",
};
const blockReceipts = [],
  blockExpected = [];
const results = [],
  reports = [],
  predicates = {};
let diagnosticComplete = false;
let active,
  cancelled = false,
  admissionClosed = false;
const stop = () => {
  cancelled = true;
  const child = active;
  child?.kill("SIGTERM");
  if (child) {
    const timer = setTimeout(() => {
      if (active === child) child.kill("SIGKILL");
    }, 15_000);
    timer.unref();
  }
};
const deadline = setTimeout(stop, 55 * 60_000);
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
async function run(name, file, args, extra = {}, timeoutMs = 12 * 60_000) {
  if (cancelled || admissionClosed) {
    results.push({ name, outcome: "unknown", reason: "cancelled-before-start" });
    return false;
  }
  const log = NodePath.join(output, `${name}.log`),
    stream = NodeFS.createWriteStream(log, { flags: "wx", mode: 0o600 });
  let child, timer, hardStop;
  const started = new Date().toISOString();
  const result = await new Promise((done) => {
    try {
      child = NodeChildProcess.spawn(file, args, {
        cwd: root,
        env: { ...env, ...extra },
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch (cause) {
      done({ code: null, error: String(cause) });
      return;
    }
    active = child;
    child.stdout.pipe(stream, { end: false });
    child.stderr.pipe(stream, { end: false });
    timer = setTimeout(() => {
      child.kill("SIGTERM");
      hardStop = setTimeout(() => child.kill("SIGKILL"), 15_000);
    }, timeoutMs);
    child.once("error", (cause) => done({ code: null, error: String(cause) }));
    child.once("close", (code, signal) =>
      done({ code, signal, capturedExit: Number.isSafeInteger(child.pid) }),
    );
  });
  clearTimeout(timer);
  clearTimeout(hardStop);
  active = undefined;
  await new Promise((done) => stream.end(done));
  results.push({
    name,
    started,
    finished: new Date().toISOString(),
    command: { file, args },
    ...result,
    outcome: result.code === 0 ? "passed" : "failed",
    log,
  });
  await NodeFSP.writeFile(
    NodePath.join(output, "commands.json"),
    JSON.stringify(results, null, 2) + "\n",
  );
  return result.code === 0;
}
const node = (name, args, extra) => run(name, process.execPath, args, extra);
function requireRetirement(proved, name) {
  if (proved === true) return;
  admissionClosed = true;
  predicates.nativeRetirement = "unknown";
  results.push({
    name: "remaining-native-cohorts",
    outcome: "unknown",
    reason: `${name}: retirement-unproved; zero further native admissions`,
  });
  throw new Error(`${name}: retirement unproved; retaining scratch and closing admission`);
}
async function report(name, path) {
  try {
    const value = JSON.parse(await NodeFSP.readFile(path, "utf8"));
    reports.push({
      name,
      path,
      outcome: value.outcome ?? "unknown",
      cleanupProved: value.cleanupProved ?? null,
    });
    predicates[name] =
      value.outcome === "passed" ? true : value.outcome === "failed" ? false : "unknown";
  } catch (cause) {
    reports.push({ name, path, outcome: "unknown", error: String(cause) });
    predicates[name] = "unknown";
  }
  if (/^(baseline|native|packaging)-/.test(name))
    requireRetirement(
      reports.at(-1)?.cleanupProved === true && results.at(-1)?.capturedExit === true,
      name,
    );
}
async function runBaseline(profile) {
    const baseline = NodePath.join(output, `baseline-${profile}.json`);
    await node(`baseline-${profile}`, [
      "scripts/jones/serverParallelism/baseline.mjs",
      baseline,
      profile,
    ]);
    try {
      const original = await NodeFSP.readFile(baseline, "utf8"),
        value = JSON.parse(original),
        result = results.at(-1);
      await NodeFSP.writeFile(NodePath.join(output, `baseline-${profile}.initial.json`), original, {
        flag: "wx",
        mode: 0o600,
      });
      let cleanupProved = false,
        cleanupError;
      try {
        const parent = await NodeFSP.realpath(process.env.TMPDIR),
          scratch = value.scratch;
        if (
          result.capturedExit !== true ||
          !NodePath.isAbsolute(scratch) ||
          NodePath.dirname(scratch) !== parent ||
          !NodePath.basename(scratch).startsWith("jones-sp-baseline-") ||
          (await NodeFSP.lstat(scratch)).isSymbolicLink()
        )
          throw new Error("Unproved baseline process exit or scratch ownership");
        await NodeFSP.rm(scratch, { recursive: true });
        try {
          await NodeFSP.lstat(scratch);
        } catch (cause) {
          if (cause.code === "ENOENT") cleanupProved = true;
          else throw cause;
        }
      } catch (cause) {
        cleanupError = String(cause);
      }
      await NodeFSP.writeFile(
        baseline,
        JSON.stringify(
          {
            ...value,
            capturedExit: result.capturedExit === true,
            cleanupProved,
            ...(cleanupError ? { cleanupError } : {}),
            outcome:
              result.code === 0 && value.outcome === "passed" && cleanupProved
                ? "passed"
                : "failed",
          },
          null,
          2,
        ) + "\n",
      );
    } catch (cause) {
      results.push({
        name: `baseline-cleanup-${profile}`,
        outcome: "failed",
        error: String(cause),
      });
    }
    await report(`baseline-${profile}`, baseline);
    return baseline;
}

try {
  await NodeFSP.writeFile(
    NodePath.join(output, "source-build.patch"),
    NodeChildProcess.execFileSync("git", ["diff", "--binary"], { cwd: root }),
  );
  await node("deterministic", [
    "--test",
    "--test-concurrency=1",
    "scripts/jones/serverParallelism/buildIdentity.node-test.mjs",
    "scripts/jones/serverParallelism/commandIdentity.node-test.mjs",
    "scripts/jones/serverParallelism/qualificationResult.node-test.mjs",
    "scripts/jones/serverParallelism/snapshotDecision.node-test.mjs",
    "scripts/jones/serverParallelism/native-block-proof.node-test.mjs",
    "scripts/jones/serverParallelism/observer-diagnostic.node-test.mjs",
  ]);
  const sourceManifest = NodePath.join(output, "source-command.json");
  await node("manifest-source", [
    "scripts/jones/serverParallelism/source-command.mjs",
    NodePath.join(root, "apps/server/src/bin.ts"),
    sourceManifest,
  ]);
  const manifests = [{ form: "source", path: sourceManifest }];
  const requireDesktop = NodeModule.createRequire(NodePath.join(root, "apps/desktop/package.json"));
  let electron;
  try {
    electron = requireDesktop("electron");
  } catch {
    /* The attempted manifest below records missing runtime as a failure. */
  }
  const roots = await NodeFSP.readdir(installedRoot).catch(() => []);
  const sea =
    roots.length === 1
      ? NodePath.join(installedRoot, roots[0], "t3")
      : NodePath.join(installedRoot, "MISSING-INSTALLED-LAYOUT");
  for (const [form, executable, entry] of scope === "full"
    ? [
        ["bundle", process.execPath, NodePath.join(root, "apps/server/dist/bin.mjs")],
        [
          "electron",
          electron ?? NodePath.join(output, "MISSING-ELECTRON"),
          NodePath.join(root, "apps/server/dist/bin.mjs"),
        ],
        ["sea", sea, "-"],
      ]
    : []) {
    const path = NodePath.join(output, `${form}-command.json`);
    await node(`manifest-${form}`, [
      "scripts/jones/serverParallelism/artifact-command.mjs",
      form,
      executable,
      entry,
      path,
    ]);
    manifests.push({ form, path });
  }
  if (diagnosticScope) {
    const mode = scope === "observer-control" ? "cpu-stat-only" : "full";
    let baseline;
    let prior;
    if (mode === "full") baseline = await runBaseline("laptop");
    else {
      baseline = NodePath.join(output, "baseline-laptop.json");
      prior = NodePath.join(priorDirectory, "observer-full.json");
      const { verifyDiagnosticArtifacts } = await import("./observer-diagnostic.mjs");
      const value = JSON.parse(await NodeFSP.readFile(prior, "utf8"));
      if (value.mode !== "full" || value.source !== source || value.sourceTree !== sourceTree || !(await verifyDiagnosticArtifacts(value, prior)))
        throw new Error("Control requires an intact same-source full diagnostic");
      await NodeFSP.writeFile(baseline, await NodeFSP.readFile(NodePath.join(priorDirectory, "baseline-laptop.json")), { flag: "wx", mode: 0o600 });
    }
    const name = mode === "full" ? "observer-full" : "observer-cpu-stat-only";
    const diagnosticOutput = NodePath.join(output, `${name}.json`);
    const succeeded = await node(name, [
      "scripts/jones/serverParallelism/observer-diagnostic.mjs", sourceManifest, diagnosticOutput, baseline, mode,
      ...(prior ? [prior] : []),
    ]);
    let value;
    try { value = JSON.parse(await NodeFSP.readFile(diagnosticOutput, "utf8")); }
    catch { /* Missing evidence remains incomplete and closes native admission. */ }
    const { verifyDiagnosticArtifacts } = await import("./observer-diagnostic.mjs");
    diagnosticComplete = succeeded && value?.source === source && value?.sourceTree === sourceTree && value?.mode === mode && await verifyDiagnosticArtifacts(value, diagnosticOutput);
    reports.push({ name, path: diagnosticOutput, eligible: false, outcome: value?.outcome ?? "unknown", cleanupProved: value?.cleanupProved ?? null });
    requireRetirement(value?.cleanupProved === true && results.at(-1)?.capturedExit === true, name);
  } else {
  for (const profile of ["laptop", "vps"]) {
    const baseline = await runBaseline(profile);
    for (const { form, path } of manifests) {
      const smoke = NodePath.join(output, `packaging-${form}-${profile}.json`);
      await node(`packaging-${form}-${profile}`, [
        "scripts/jones/serverParallelism/packaging-smoke.mjs",
        path,
        smoke,
        profile,
      ]);
      await report(`packaging-${form}-${profile}`, smoke);
      if (scope === "source-evidence") {
        const dispatch = await run(
          `dispatch-${form}-${profile}`,
          "vp",
          [
            "test",
            "run",
            "apps/server/src/jones/serverParallelism/nativeProcess.integration.test.ts",
            "--maxWorkers=1",
            "--testNamePattern=private dispatch",
          ],
          { JONES_SP_COMMAND_MANIFEST: path, JONES_SP_PROFILE: profile },
        );
        predicates[`dispatch-${form}-${profile}`] = dispatch;
        requireRetirement(
          dispatch === true && results.at(-1)?.capturedExit === true,
          `dispatch-${form}-${profile}`,
        );
      }
      const native = NodePath.join(output, `native-${form}-${profile}.json`);
      await node(`native-${form}-${profile}`, [
        "scripts/jones/serverParallelism/qualify.mjs",
        path,
        native,
        profile,
        baseline,
      ]);
      await report(`native-${form}-${profile}`, native);
      // This bounded diagnosis stops before unrelated packaging/fault cohorts.
      // Full qualification remains explicitly unknown until the complete matrix runs.
      if (scope === "source-evidence") continue;
      let manifestExists = true;
      try {
        await NodeFSP.readFile(path);
      } catch {
        manifestExists = false;
      }
      if (!manifestExists) {
        blockExpected.push({
          os: HostProcess.Platform.defaultValue() === "linux" ? "linux-x64" : "macos-arm64",
          form,
          profile,
          build: "unknown",
        });
        predicates[`fault-${form}-${profile}`] = "unknown";
        results.push({
          name: `fault-${form}-${profile}`,
          outcome: "unknown",
          reason: "missing-command-manifest; vitest skip cannot qualify",
        });
        continue;
      }
      const blockOutput = NodePath.join(output, `native-block-${form}-${profile}`);
      const binding = {
        os: HostProcess.Platform.defaultValue() === "linux" ? "linux-x64" : "macos-arm64",
        form,
        profile,
        build: JSON.parse(await NodeFSP.readFile(path, "utf8")).build,
      };
      blockExpected.push(binding);
      await run(
        `native-block-${form}-${profile}`,
        process.execPath,
        ["scripts/jones/serverParallelism/native-block-run.mjs", path, blockOutput, profile],
        {},
        240_000,
      );
      const blockCommand = results.at(-1);
      let retirementProved = false;
      try {
        const receipt = JSON.parse(
          await NodeFSP.readFile(NodePath.join(blockOutput, "receipt.json"), "utf8"),
        );
        const { verifyNativeBlockArtifacts, verifyNativeBlockRetirement } =
          await import("./native-block-verify.mjs");
        receipt.artifactsVerified = await verifyNativeBlockArtifacts(receipt, binding, blockOutput);
        blockReceipts.push(receipt);
        retirementProved =
          blockCommand?.capturedExit === true &&
          (await verifyNativeBlockRetirement(receipt, blockOutput, profile === "vps" ? 3 : 2));
      } catch (cause) {
        reports.push({
          name: `native-block-${form}-${profile}`,
          outcome: "unknown",
          error: String(cause),
        });
      }
      requireRetirement(retirementProved, `native-block-${form}-${profile}`);
      const fault = await run(
        `fault-${form}-${profile}`,
        "vp",
        [
          "test",
          "run",
          "apps/server/src/jones/serverParallelism/nativeProcess.integration.test.ts",
          "--maxWorkers=1",
          "--testNamePattern=guardian|private dispatch",
        ],
        {
          JONES_SP_COMMAND_MANIFEST: path,
          JONES_SP_PROFILE: profile,
          JONES_SP_NATIVE_BLOCK_RECEIPT: NodePath.join(blockOutput, "receipt.json"),
        },
      );
      predicates[`fault-${form}-${profile}`] = fault;
      requireRetirement(
        fault === true && results.at(-1)?.capturedExit === true,
        `fault-${form}-${profile}`,
      );
    }
  }
  const snapshot = NodePath.join(output, "snapshot.json");
  await node("snapshot", ["scripts/jones/serverParallelism/snapshot.mjs", snapshot, "2000"]);
  await report("snapshot", snapshot);
  const nativeBlock = nativeBlockMatrixDecision(blockReceipts, blockExpected);
  predicates.nativeExecutionAtParentLoss =
    blockExpected.length === 8 ? nativeBlock.predicate : "unknown";
  await NodeFSP.writeFile(
    NodePath.join(output, "native-block-matrix.json"),
    JSON.stringify(nativeBlock, null, 2) + "\n",
  );
  }
} finally {
  clearTimeout(deadline);
  process.removeListener("SIGINT", stop);
  process.removeListener("SIGTERM", stop);
  if (diagnosticScope) {
    const cleanupProved = !admissionClosed && reports.length > 0 && reports.every((value) => value.cleanupProved === true) && results.filter((value) => value.name.startsWith("observer-")).length === 1 && results.filter((value) => value.name.startsWith("observer-")).every((value) => value.capturedExit === true);
    const complete = diagnosticComplete && cleanupProved && !cancelled && results.every((value) => value.outcome === "passed");
    await NodeFSP.writeFile(NodePath.join(output, "qualification.json"), JSON.stringify({
      schema: "jones.server-parallelism.observer-diagnostic-ci/v1", eligible: false, passed: false,
      outcome: complete ? "recorded" : "incomplete", complete, source, sourceTree, scope,
      predicates: { cleanupProved }, results, reports,
      interpretation: "Bounded manual diagnostic only. Official predicates are separate in the diagnostic receipt and cannot qualify normal fan-in.",
    }, null, 2) + "\n");
    if (!complete) process.exitCode = 1;
  } else {
  predicates.commandsSucceeded =
    results.length > 0 && results.every((result) => result.outcome === "passed");
  predicates.completed = !cancelled && !admissionClosed;
  predicates.nativeExecutionAtParentLoss ??= "unknown";
  predicates.fullMatrix = scope === "full" ? blockExpected.length === 8 : "unknown";
  const lifecycleReports = reports.filter((value) =>
    /^(baseline|native|packaging)-/.test(value.name),
  );
  predicates.cleanupProved =
    !admissionClosed &&
    lifecycleReports.length === 18 &&
    lifecycleReports.every((value) => value.cleanupProved === true) &&
    blockReceipts.length === 8 &&
    blockReceipts.every((value) => value.cleanupProved === true) &&
    results.filter((value) => value.name.startsWith("fault-")).length === 8 &&
    results
      .filter((value) => value.name.startsWith("fault-"))
      .every((value) => value.outcome === "passed" && value.capturedExit === true);
  const decision = qualificationDecision(predicates);
  const inputs = {};
  for (const name of [
    "package.json",
    "pnpm-lock.yaml",
    "patches/@ff-labs__fff-node@0.11.0.patch",
  ]) {
    inputs[name] = NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(NodePath.join(root, name)))
      .digest("hex");
  }
  await NodeFSP.writeFile(
    NodePath.join(output, "qualification.json"),
    JSON.stringify(
      {
        schema: "jones.server-parallelism.ci/v1",
        ...decision,
        source,
        sourceTree,
        scope,
        scopedCleanupProved:
          !admissionClosed &&
          lifecycleReports.length === (scope === "source-evidence" ? 6 : 18) &&
          lifecycleReports.every((value) => value.cleanupProved === true),
        inputs,
        node: process.versions,
        platform: HostProcess.Platform.defaultValue(),
        architecture: HostProcess.Architecture.defaultValue(),
        availableParallelism: NodeOS.availableParallelism(),
        totalMemoryBytes: NodeOS.totalmem(),
        predicates,
        results,
        reports,
        interpretation:
          "Advisory source qualification; unknown is not passing evidence. No runtime adoption or deployment.",
      },
      null,
      2,
    ) + "\n",
  );
  if (!decision.passed) process.exitCode = 1;
  }
}
