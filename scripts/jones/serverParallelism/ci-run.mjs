// Serial per-OS admission and evidence fan-in. Every required command is attempted;
// missing inputs remain unknown. No release, deployment, live state or provider call.
import { spawn, execFileSync } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, writeFile, readFile, readdir, realpath, lstat, rm } from "node:fs/promises";
import { join, resolve, isAbsolute, dirname, basename } from "node:path";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { availableParallelism, totalmem } from "node:os";
import { qualificationDecision, nativeBlockMatrixDecision } from "./qualificationResult.mjs";
const [output, installedRoot] = process.argv.slice(2);
if (!isAbsolute(output ?? "") || !isAbsolute(installedRoot ?? ""))
  throw new Error("ci-run.mjs <absolute reports directory> <absolute extracted archive directory>");
await mkdir(output, { recursive: true, mode: 0o700 });
const root = resolve(import.meta.dirname, "../../.."),
  source = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
const sourceTree = execFileSync("git", ["rev-parse", "HEAD^{tree}"], {
  cwd: root,
  encoding: "utf8",
}).trim();
const env = {
  ...process.env,
  JONES_SP_SOURCE_SHA: source,
  JONES_SP_SOURCE_TREE: sourceTree,
  T3CODE_DEV_SEED: "0",
};
const blockReceipts = [], blockExpected = [];
const results = [],
  reports = [],
  predicates = {};
let active,
  cancelled = false;
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
  if (cancelled) {
    results.push({ name, outcome: "unknown", reason: "cancelled-before-start" });
    return false;
  }
  const log = join(output, `${name}.log`),
    stream = createWriteStream(log, { flags: "wx", mode: 0o600 });
  let child, timer, hardStop;
  const started = new Date().toISOString();
  const result = await new Promise((done) => {
    try {
      child = spawn(file, args, {
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
  await writeFile(join(output, "commands.json"), JSON.stringify(results, null, 2) + "\n");
  return result.code === 0;
}
const node = (name, args, extra) => run(name, process.execPath, args, extra);
async function report(name, path) {
  try {
    const value = JSON.parse(await readFile(path, "utf8"));
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
}
try {
  await writeFile(
    join(output, "source-build.patch"),
    execFileSync("git", ["diff", "--binary"], { cwd: root }),
  );
  await node("deterministic", [
    "--test",
    "--test-concurrency=1",
    "scripts/jones/serverParallelism/buildIdentity.node-test.mjs",
    "scripts/jones/serverParallelism/commandIdentity.node-test.mjs",
    "scripts/jones/serverParallelism/qualificationResult.node-test.mjs",
    "scripts/jones/serverParallelism/native-block-proof.node-test.mjs",
  ]);
  const sourceManifest = join(output, "source-command.json");
  await node("manifest-source", [
    "scripts/jones/serverParallelism/source-command.mjs",
    join(root, "apps/server/src/bin.ts"),
    sourceManifest,
  ]);
  const manifests = [{ form: "source", path: sourceManifest }];
  const requireDesktop = createRequire(join(root, "apps/desktop/package.json"));
  let electron;
  try {
    electron = requireDesktop("electron");
  } catch {
    /* The attempted manifest below records missing runtime as a failure. */
  }
  const roots = await readdir(installedRoot).catch(() => []);
  const sea =
    roots.length === 1
      ? join(installedRoot, roots[0], "t3")
      : join(installedRoot, "MISSING-INSTALLED-LAYOUT");
  for (const [form, executable, entry] of [
    ["bundle", process.execPath, join(root, "apps/server/dist/bin.mjs")],
    [
      "electron",
      electron ?? join(output, "MISSING-ELECTRON"),
      join(root, "apps/server/dist/bin.mjs"),
    ],
    ["sea", sea, "-"],
  ]) {
    const path = join(output, `${form}-command.json`);
    await node(`manifest-${form}`, [
      "scripts/jones/serverParallelism/artifact-command.mjs",
      form,
      executable,
      entry,
      path,
    ]);
    manifests.push({ form, path });
  }
  for (const profile of ["laptop", "vps"]) {
    const baseline = join(output, `baseline-${profile}.json`);
    await node(`baseline-${profile}`, [
      "scripts/jones/serverParallelism/baseline.mjs",
      baseline,
      profile,
    ]);
    try {
      const original = await readFile(baseline, "utf8"),
        value = JSON.parse(original),
        result = results.at(-1);
      await writeFile(join(output, `baseline-${profile}.initial.json`), original, {
        flag: "wx",
        mode: 0o600,
      });
      let cleanupProved = false,
        cleanupError;
      try {
        const parent = await realpath(process.env.TMPDIR),
          scratch = value.scratch;
        if (
          result.capturedExit !== true ||
          !isAbsolute(scratch) ||
          dirname(scratch) !== parent ||
          !basename(scratch).startsWith("jones-sp-baseline-") ||
          (await lstat(scratch)).isSymbolicLink()
        )
          throw new Error("Unproved baseline process exit or scratch ownership");
        await rm(scratch, { recursive: true });
        try {
          await lstat(scratch);
        } catch (cause) {
          if (cause.code === "ENOENT") cleanupProved = true;
          else throw cause;
        }
      } catch (cause) {
        cleanupError = String(cause);
      }
      await writeFile(
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
    for (const { form, path } of manifests) {
      const smoke = join(output, `packaging-${form}-${profile}.json`);
      await node(`packaging-${form}-${profile}`, [
        "scripts/jones/serverParallelism/packaging-smoke.mjs",
        path,
        smoke,
        profile,
      ]);
      await report(`packaging-${form}-${profile}`, smoke);
      const native = join(output, `native-${form}-${profile}.json`);
      await node(`native-${form}-${profile}`, [
        "scripts/jones/serverParallelism/qualify.mjs",
        path,
        native,
        profile,
        baseline,
      ]);
      await report(`native-${form}-${profile}`, native);
      let manifestExists = true;
      try {
        await readFile(path);
      } catch {
        manifestExists = false;
      }
      if (!manifestExists) {
        blockExpected.push({ os: process.platform === "linux" ? "linux-x64" : "macos-arm64", form, profile, build: "unknown" });
        predicates[`fault-${form}-${profile}`] = "unknown";
        results.push({
          name: `fault-${form}-${profile}`,
          outcome: "unknown",
          reason: "missing-command-manifest; vitest skip cannot qualify",
        });
        continue;
      }
      const blockOutput = join(output, `native-block-${form}-${profile}`);
      const binding = { os: process.platform === "linux" ? "linux-x64" : "macos-arm64", form, profile,
        build: JSON.parse(await readFile(path, "utf8")).build };
      blockExpected.push(binding);
      await run(`native-block-${form}-${profile}`, process.execPath, [
        "scripts/jones/serverParallelism/native-block-run.mjs", path, blockOutput, profile,
      ], {}, 240_000);
      try {
        const receipt = JSON.parse(await readFile(join(blockOutput, "receipt.json"), "utf8"));
        const { verifyNativeBlockArtifacts } = await import("./native-block-verify.mjs");
        receipt.artifactsVerified = await verifyNativeBlockArtifacts(receipt, binding, blockOutput);
        blockReceipts.push(receipt);
      } catch (cause) {
        reports.push({ name: `native-block-${form}-${profile}`, outcome: "unknown", error: String(cause) });
      }
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
        { JONES_SP_COMMAND_MANIFEST: path, JONES_SP_PROFILE: profile,
          JONES_SP_NATIVE_BLOCK_RECEIPT: join(blockOutput, "receipt.json") },
      );
      predicates[`fault-${form}-${profile}`] = fault;
    }
  }
  const snapshot = join(output, "snapshot.json");
  await node("snapshot", ["scripts/jones/serverParallelism/snapshot.mjs", snapshot, "2000"]);
  await report("snapshot", snapshot);
  const nativeBlock = nativeBlockMatrixDecision(blockReceipts, blockExpected);
  predicates.nativeExecutionAtParentLoss = blockExpected.length === 8 ? nativeBlock.predicate : "unknown";
  await writeFile(join(output, "native-block-matrix.json"), JSON.stringify(nativeBlock, null, 2) + "\n");
} finally {
  clearTimeout(deadline);
  process.removeListener("SIGINT", stop);
  process.removeListener("SIGTERM", stop);
  predicates.commandsSucceeded =
    results.length > 0 && results.every((result) => result.outcome === "passed");
  predicates.completed = !cancelled;
  predicates.nativeExecutionAtParentLoss ??= "unknown";
  const lifecycleReports = reports.filter((value) =>
    /^(baseline|native|packaging)-/.test(value.name),
  );
  predicates.cleanupProved =
    lifecycleReports.length === 18 &&
    lifecycleReports.every((value) => value.cleanupProved === true) &&
    blockReceipts.length === 8 && blockReceipts.every((value) => value.cleanupProved === true) &&
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
    inputs[name] = createHash("sha256")
      .update(await readFile(join(root, name)))
      .digest("hex");
  }
  await writeFile(
    join(output, "qualification.json"),
    JSON.stringify(
      {
        schema: "jones.server-parallelism.ci/v1",
        ...decision,
        source,
        sourceTree,
        inputs,
        node: process.versions,
        platform: process.platform,
        architecture: process.arch,
        availableParallelism: availableParallelism(),
        totalMemoryBytes: totalmem(),
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
