import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { bindArtifactInputs, qualifyPackageSourceDiff } from "./support.mjs";

// The caller supplies the host's bounded collector and project lock; this runner installs neither.
const script = NodeURL.fileURLToPath(import.meta.url);
const repositoryRoot = NodePath.resolve(NodePath.dirname(script), "../..");
const args = process.argv.slice(2);
function option(name, fallback) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  const value = args[index + 1];
  if (!value || value.startsWith("--")) throw new Error(`Missing ${name} value`);
  return value;
}
function outsideRepository(value, name) {
  if (!value || !NodePath.isAbsolute(value)) throw new Error(`${name} requires an absolute path`);
  const path = NodePath.resolve(value);
  const subpath = NodePath.relative(repositoryRoot, path);
  if (subpath === "" || (!subpath.startsWith(`..${NodePath.sep}`) && subpath !== "..")) {
    throw new Error(`${name} must be outside the source worktree`);
  }
  if (path === "/tmp" || path.startsWith("/tmp/"))
    throw new Error(`${name} cannot use shared /tmp`);
  return path;
}

async function main() {
  const evidenceDir = outsideRepository(option("--evidence-dir"), "--evidence-dir");
  const lockFile = option("--lock-file");
  const admissionTool = option("--admission-tool");
  if (
    !lockFile ||
    !NodePath.isAbsolute(lockFile) ||
    !admissionTool ||
    !NodePath.isAbsolute(admissionTool)
  ) {
    throw new Error("Explicit absolute project lock and bounded admission collector are required");
  }
  const mode = option("--mode", "dev");
  if (mode !== "dev" && mode !== "final") throw new Error("mode must be dev or final");
  if (!args.includes("--locked")) {
    const fd = NodeFS.openSync(lockFile, "r");
    const child = NodeChildProcess.spawn(
      "bash",
      [
        "-c",
        'flock -n -E 75 9 || exit 75; exec "$@"',
        "t4-lock",
        process.execPath,
        script,
        ...args,
        "--locked",
      ],
      {
        stdio: [
          "inherit",
          "inherit",
          "inherit",
          "ignore",
          "ignore",
          "ignore",
          "ignore",
          "ignore",
          "ignore",
          fd,
        ],
      },
    );
    NodeFS.closeSync(fd);
    const forward = (signal) => child.kill(signal);
    const sigint = () => forward("SIGINT");
    const sigterm = () => forward("SIGTERM");
    process.on("SIGINT", sigint);
    process.on("SIGTERM", sigterm);
    const result = await new Promise((done, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => done(code ?? (signal ? 130 : 1)));
    });
    process.off("SIGINT", sigint);
    process.off("SIGTERM", sigterm);
    process.exitCode = result;
    return;
  }

  await NodeFSP.mkdir(evidenceDir, { recursive: true });
  const collect = NodeChildProcess.spawnSync("python3", [admissionTool, "--format", "admission"], {
    encoding: "utf8",
  });
  if (collect.status !== 0) throw new Error("CPU/RAM admission unavailable; zero tests started");
  const admission = JSON.parse(collect.stdout);
  await NodeFSP.writeFile(
    NodePath.join(evidenceDir, "admission.json"),
    JSON.stringify(admission, null, 2),
  );
  const capacity = admission.admission;
  const measurements = capacity?.measurements;
  console.log(capacity?.summary ?? "Admission missing; zero starts");
  if (
    !Number.isFinite(measurements?.cpu_used_percent) ||
    !Number.isFinite(measurements?.ram_available_gib) ||
    measurements.cpu_used_percent >= 85 ||
    capacity.serial_test_process_ceiling !== 1
  ) {
    throw new Error("Admission denied; zero tests started");
  }
  const git = (...values) => {
    const result = NodeChildProcess.spawnSync("git", values, {
      cwd: repositoryRoot,
      encoding: "utf8",
    });
    if (result.status !== 0) throw new Error("Source identity read failed");
    return result.stdout.trim();
  };
  const observed = {
    repository: "Jones-Systems/Jones-Code",
    commit: git("rev-parse", "HEAD"),
    tree: git("rev-parse", "HEAD^{tree}"),
    clean: git("status", "--porcelain") === "",
  };
  const origin = git("remote", "get-url", "origin");
  if (origin !== "https://github.com/Jones-Systems/Jones-Code.git") {
    throw new Error("Source origin is not the canonical Jones repository");
  }
  const descriptorPath = option("--descriptor");
  const inputs = descriptorPath
    ? JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8"))
    : undefined;
  if (mode === "final") {
    if (!/^[a-f0-9]{40}$/.test(inputs?.acceptedT2Source ?? "")) {
      throw new Error("Final replay requires the accepted exact T2 source commit");
    }
    const ancestry = NodeChildProcess.spawnSync(
      "git",
      ["merge-base", "--is-ancestor", inputs.acceptedT2Source, inputs.acceptedCumulativeSource],
      { cwd: repositoryRoot },
    );
    if (ancestry.status !== 0)
      throw new Error("Package source does not retain the accepted T2 source");
  }
  const sourceObservation = { ...observed };
  const incompleteDevelopment =
    mode === "dev" && (!observed.clean || !inputs?.candidate?.path || !inputs?.candidate?.sha256);
  if (
    !incompleteDevelopment &&
    inputs?.acceptedCumulativeSource &&
    /^[a-f0-9]{40}$/.test(inputs.acceptedCumulativeSource)
  ) {
    const packageTree = git("rev-parse", `${inputs.acceptedCumulativeSource}^{tree}`);
    if (inputs.acceptedCumulativeTree && packageTree !== inputs.acceptedCumulativeTree) {
      throw new Error("Descriptor package source tree does not match its exact commit");
    }
    if (
      mode === "final" &&
      (inputs.candidate?.sourceCommit !== inputs.acceptedCumulativeSource ||
        inputs.candidate?.sourceTree !== packageTree)
    ) {
      throw new Error("Final candidate attribution must retain its actual source commit and tree");
    }
    const changed = git("diff", "--name-only", inputs.acceptedCumulativeSource, observed.commit);
    const changedPaths = changed.split("\n").filter(Boolean);
    let packageKnip;
    let qualificationKnip;
    if (changedPaths.includes("knip.jsonc")) {
      for (const revision of [inputs.acceptedCumulativeSource, observed.commit]) {
        const entry = git("ls-tree", revision, "--", "knip.jsonc");
        if (!/^100644 blob [a-f0-9]{40}\tknip\.jsonc$/.test(entry)) {
          throw new Error("Knip metadata must retain its regular-file mode");
        }
      }
      const bytes = NodeChildProcess.spawnSync(
        "git",
        ["show", `${inputs.acceptedCumulativeSource}:knip.jsonc`],
        { cwd: repositoryRoot },
      );
      if (bytes.status !== 0) throw new Error("Exact package Knip bytes unavailable");
      packageKnip = bytes.stdout;
      qualificationKnip = await NodeFSP.readFile(NodePath.join(repositoryRoot, "knip.jsonc"));
    }
    const sourceDiff = qualifyPackageSourceDiff(changedPaths, packageKnip, qualificationKnip);
    // Harness and proved one-entry Knip metadata changes retain the actual package source.
    sourceObservation.harnessCommit = observed.commit;
    sourceObservation.commit = inputs.acceptedCumulativeSource;
    sourceObservation.packageSourceTree = packageTree;
    sourceObservation.qualificationTree = observed.tree;
    sourceObservation.productionDiffPaths = sourceDiff.productionDiffPaths;
    sourceObservation.nonBuildMetadataDiff = sourceDiff.nonBuildMetadataDiff;
  }
  const binding = incompleteDevelopment
    ? { status: "unbound", reasons: ["Development source or artifact inputs are incomplete"] }
    : inputs
      ? await bindArtifactInputs(inputs, sourceObservation)
      : { status: "unbound", reasons: ["No package descriptor supplied"] };
  await NodeFSP.writeFile(
    NodePath.join(evidenceDir, "binding.json"),
    JSON.stringify({ mode, observed, binding }, null, 2),
  );
  if (mode === "final" && binding.status !== "bound")
    throw new Error("Final replay requires exact accepted source and package bindings");

  const scratch = await NodeFSP.mkdtemp(NodePath.join(evidenceDir, "scratch-"));
  const log = NodeFS.openSync(NodePath.join(evidenceDir, "tests.log"), "wx");
  const started = Date.now();
  let outcome = { code: 1, signal: null };
  let cancelled = false;
  let child;
  const cancel = (signal) => {
    cancelled = true;
    if (!child?.pid) return;
    // The session group is captured at NodeChildProcess.spawn and contains only this invocation's synthetic tests.
    try {
      process.kill(-child.pid, signal);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  };
  const sigint = () => cancel("SIGINT");
  const sigterm = () => cancel("SIGTERM");
  process.on("SIGINT", sigint);
  process.on("SIGTERM", sigterm);
  try {
    child = NodeChildProcess.spawn(
      NodePath.join(repositoryRoot, "node_modules/.bin/vp"),
      [
        "test",
        "run",
        "scripts/runtime-adoption-qualification",
        "--fileParallelism=false",
        "--maxWorkers=1",
      ],
      {
        cwd: repositoryRoot,
        detached: true,
        env: {
          ...process.env,
          T4_QUAL_SCRATCH_ROOT: scratch,
          T4_QUAL_EVIDENCE_DIR: evidenceDir,
          T4_QUAL_DESCRIPTOR_PATH: binding.status === "bound" ? descriptorPath : "",
          T4_QUAL_MODE: mode,
          NODE_COMPILE_CACHE: NodePath.join(scratch, "node-cache"),
          TMPDIR: scratch,
          TMP: scratch,
          TEMP: scratch,
        },
        stdio: ["ignore", log, log],
      },
    );
    outcome = await new Promise((done, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => done({ code, signal }));
    });
  } finally {
    process.off("SIGINT", sigint);
    process.off("SIGTERM", sigterm);
    NodeFS.closeSync(log);
    if (child?.pid) {
      let alive = true;
      const deadline = Date.now() + 10_000;
      while (alive && Date.now() < deadline) {
        try {
          process.kill(-child.pid, 0);
        } catch (error) {
          if (error.code === "ESRCH") alive = false;
          else throw error;
        }
        if (alive) await new Promise((done) => setTimeout(done, 100));
      }
      if (alive) {
        await NodeFSP.writeFile(
          NodePath.join(evidenceDir, "summary.json"),
          JSON.stringify(
            {
              schema: "jones-runtime-qualification/v1",
              mode,
              observed,
              binding,
              outcome,
              cancelled,
              scratch,
              scratchAbsent: false,
              cleanup: "unknown-owned-survivors",
            },
            null,
            2,
          ),
        );
        throw new Error("Owned test processes remain alive; scratch preserved for reconciliation");
      }
    }
    await NodeFSP.rm(scratch, { recursive: true, force: true });
    const scratchAbsent = await NodeFSP.stat(scratch).then(
      () => false,
      (error) => {
        if (error.code === "ENOENT") return true;
        throw error;
      },
    );
    await NodeFSP.writeFile(
      NodePath.join(evidenceDir, "summary.json"),
      JSON.stringify(
        {
          schema: "jones-runtime-qualification/v1",
          mode,
          observed,
          binding,
          outcome,
          cancelled,
          durationMs: Date.now() - started,
          scratch,
          scratchAbsent,
          proofKind: "production-seams/synthetic-fixtures",
          nativeResume: "unproved",
          installedAdoption: "unproved",
          electronDecryption: "unproved",
          browserPersistence: "unproved",
        },
        null,
        2,
      ),
    );
    console.log(
      JSON.stringify({ outcome, cancelled, scratchAbsent, durationMs: Date.now() - started }),
    );
  }
  process.exitCode = cancelled ? 130 : (outcome.code ?? 1);
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
