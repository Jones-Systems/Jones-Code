import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import { doctor } from "./ui-evidence/doctor.mjs";
import { setupRuntime, dependencyClosure, sha256, exec } from "./ui-evidence/runtime.mjs";
import {
  allocateRun,
  removeRun,
  superviseRun,
  cleanupRegistered,
} from "./ui-evidence/lifecycle.mjs";
import { stageRun, bwrapArguments } from "./ui-evidence/sandbox.mjs";
import { collectProvenance } from "./ui-evidence/provenance.mjs";
import { createManifest, writeManifest } from "./ui-evidence/manifest.mjs";

export const harness = NodePath.resolve(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../..",
);
export function parseOptions(argv) {
  const [command, ...rest] = argv;
  if (!["doctor", "setup", "run", "cleanup"].includes(command))
    throw new Error("Expected doctor, setup, run, or cleanup");
  const allowed = {
    doctor: ["source"],
    setup: ["source"],
    run: ["source", "scenario", "size", "scale", "theme", "comparison", "out", "build-receipt"],
    cleanup: ["run"],
  }[command];
  const options = {};
  for (let index = 0; index < rest.length; index += 2) {
    const key = rest[index].slice(2);
    if (
      !rest[index].startsWith("--") ||
      !allowed.includes(key) ||
      options[key] !== undefined ||
      !rest[index + 1] ||
      rest[index + 1].startsWith("--")
    )
      throw new Error(`Invalid option: ${rest[index]}`);
    options[key] = rest[index + 1];
  }
  const size = (options.size || "1280x800").match(/^(\d+)x(\d+)$/);
  if (!size || size.slice(1).some((value) => Number(value) < 320 || Number(value) > 8192))
    throw new Error("Size must be 320..8192 by 320..8192");
  const scale = Number(options.scale || "1");
  if (!Number.isFinite(scale) || scale < 0.5 || scale > 4)
    throw new Error("Scale must be between 0.5 and 4");
  const theme = options.theme || "system";
  if (!["system", "light", "dark"].includes(theme)) throw new Error("Invalid theme");
  return {
    command,
    ...options,
    source: NodePath.resolve(options.source || harness),
    size: { width: Number(size[1]), height: Number(size[2]) },
    scale,
    theme,
  };
}
export async function assertHarnessPin() {
  const pin = process.env.JCUE_HARNESS_OID;
  if (!pin) return;
  const head = (await exec("git", ["-C", harness, "rev-parse", "HEAD"])).stdout.trim();
  const status = (
    await exec("git", ["-C", harness, "status", "--porcelain=v1", "--untracked-files=all"])
  ).stdout;
  if (!/^[a-f0-9]{40}$/.test(pin) || pin !== head || status.length)
    throw new Error("Pinned harness revision mismatch or dirty harness");
}
async function runEvidence(options) {
  const outerStart = performance.now();
  const timing = {};
  const checked = await doctor(options.source);
  timing.preflightSeconds = (performance.now() - outerStart) / 1000;
  if (checked.outcome !== "complete") return { code: 2, ...checked };
  const source = await NodeFSP.realpath(options.source);
  const scenarioId = options.scenario || "sidebar-rename";
  const scenario =
    scenarioId === "sidebar-rename"
      ? NodePath.join(harness, "apps/desktop/scripts/ui-evidence/scenarios/sidebar-rename.mjs")
      : scenarioId;
  if (
    !NodePath.isAbsolute(scenario) ||
    !scenario.endsWith(".mjs") ||
    !(await NodeFSP.lstat(scenario)).isFile()
  )
    throw new Error("Custom scenario must be an absolute regular .mjs file");
  let cancelled = null;
  const interrupt = () => {
    cancelled = "SIGINT";
  };
  const terminate = () => {
    cancelled = "SIGTERM";
  };
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", terminate);
  let run,
    artifacts,
    provenance,
    result,
    inner = null,
    cleanup;
  const identity = {
    id: scenarioId === "sidebar-rename" ? scenarioId : "custom",
    path: scenario,
    sha256: await sha256(scenario),
  };
  try {
    run = await allocateRun();
    artifacts = NodePath.resolve(
      options.out || NodePath.join(source, ".t3/ui-evidence/runs", run.id),
    );
    if (artifacts === run.root || artifacts.startsWith(`${run.root}/`))
      throw new Error("Artifacts must be separate from scratch");
    await NodeFSP.mkdir(NodePath.dirname(artifacts), { recursive: true, mode: 0o700 });
    await NodeFSP.mkdir(artifacts, { mode: 0o700 });
    const provenanceStart = performance.now();
    provenance = await collectProvenance({
      source,
      harness,
      comparison: options.comparison,
      artifacts,
      buildPaths: {
        desktop: NodePath.join(source, "apps/desktop/dist-electron"),
        server: NodePath.join(source, "apps/server/dist"),
        web: NodePath.join(source, "apps/server/dist/client"),
      },
      buildReceipt: options["build-receipt"],
    });
    timing.provenanceSeconds = (performance.now() - provenanceStart) / 1000;
    const stagingStart = performance.now();
    const closure = await dependencyClosure(source);
    const clockTicks = Number((await exec("/usr/bin/getconf", ["CLK_TCK"])).stdout.trim());
    if (!Number.isFinite(clockTicks) || clockTicks <= 0)
      throw new Error("Cannot determine CPU clock tick units");
    const configuration = {
      size: options.size,
      scale: options.scale,
      theme: options.theme,
      realHome: NodeOS.homedir(),
      clockTicks,
      hostEffectiveCores: NodeOS.availableParallelism(),
    };
    const stage = await stageRun({ run, source, harness, scenario, closure, configuration });
    if ((await sha256(NodePath.join(stage, "harness/scenario.mjs"))) !== identity.sha256)
      throw new Error("Scenario changed during staging");
    timing.stagingSeconds = (performance.now() - stagingStart) / 1000;
    if (cancelled) throw new Error("Cancelled during preparation");
    const args = await bwrapArguments({ stage, runtime: checked.runtime, artifacts });
    let diagnostic = "";
    result = await superviseRun({
      run,
      start: () => {
        const child = NodeChildProcess.spawn("/usr/bin/bwrap", args, {
          stdio: ["ignore", "ignore", "pipe"],
          env: {},
        });
        child.stderr.on("data", (data) => {
          diagnostic = (diagnostic + data.toString()).slice(-65536);
        });
        return child;
      },
    });
    cleanup = result.cleanup;
    diagnostic = diagnostic
      .replace(/Bearer\s+\S+/gi, "Bearer [redacted]")
      .replace(/((?:token|ticket|secret|password)=)[^\s&]+/gi, "$1[redacted]");
    await NodeFSP.writeFile(NodePath.join(artifacts, "sandbox.log"), diagnostic, {
      mode: 0o600,
      flag: "wx",
    });
    try {
      const innerPath = NodePath.join(artifacts, "inner.json");
      if (!(await NodeFSP.lstat(innerPath)).isFile())
        throw new Error("Sandbox result must be a regular file");
      inner = JSON.parse(await NodeFSP.readFile(innerPath, "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  } catch (error) {
    result = {
      code: cancelled ? (cancelled === "SIGINT" ? 130 : 143) : 2,
      error: error.message,
      signal: cancelled,
    };
  } finally {
    if (run && !cleanup)
      try {
        cleanup = await removeRun(run);
      } catch (error) {
        cleanup = { outcome: "unknown", error: error.message };
        if (result?.code === 0) result.code = 1;
      }
    process.off("SIGINT", interrupt);
    process.off("SIGTERM", terminate);
  }
  const status =
    result.code === 0 ? "passed" : [130, 143].includes(result.code) ? "cancelled" : "failed";
  if (provenance) {
    const manifest = createManifest({
      runId: run.id,
      provenance,
      scenario: identity,
      inner: {
        ...inner,
        error: result.error || inner?.error || null,
        namespaceProcess: result.namespaceProcess || null,
        resources: {
          ...inner?.resources,
          outer: { ...timing, totalSeconds: (performance.now() - outerStart) / 1000 },
        },
      },
      cleanup,
      status,
    });
    await writeManifest(artifacts, manifest);
  }
  return {
    code: result.code,
    outcome: result.code === 0 ? "complete" : result.code === 2 ? "rejected" : "partial",
    status,
    runId: run?.id,
    artifacts,
    cleanup,
    error: result.error || inner?.error || null,
  };
}
export async function main(argv = process.argv.slice(2)) {
  try {
    const options = parseOptions(argv);
    await assertHarnessPin();
    if (options.command === "doctor") {
      const result = await doctor(options.source);
      return { code: result.outcome === "complete" ? 0 : 2, ...result };
    }
    if (options.command === "setup") return { code: 0, ...(await setupRuntime(options.source)) };
    if (options.command === "cleanup")
      return { code: 0, ...(await cleanupRegistered(options.run)) };
    return await runEvidence(options);
  } catch (error) {
    return { code: 2, outcome: "rejected", error: error.message };
  }
}
if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href) {
  const result = await main();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  process.exitCode = result.code;
}
