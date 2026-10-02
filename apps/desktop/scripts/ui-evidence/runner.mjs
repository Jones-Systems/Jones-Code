import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import { isolationProbe } from "./probe.mjs";
import { createScenarioContext, loadScenario } from "./scenario-api.mjs";

async function startXvfb(size) {
  const child = NodeChildProcess.spawn(
    "/usr/bin/Xvfb",
    ["-displayfd", "3", "-nolisten", "tcp", "-screen", "0", `${size.width}x${size.height}x24`],
    { stdio: ["ignore", "ignore", "pipe", "pipe"], env: process.env },
  );
  try {
    const display = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Xvfb startup timed out")), 15000);
      let value = "";
      const failed = (error) => {
        clearTimeout(timer);
        reject(error);
      };
      child.once("error", failed);
      child.once("exit", () => failed(new Error("Xvfb exited before readiness")));
      child.stdio[3].on("data", (chunk) => {
        value += chunk;
        if (value.includes("\n")) {
          clearTimeout(timer);
          resolve(`:${value.trim()}`);
        }
      });
    });
    return { child, display };
  } catch (error) {
    child.kill("SIGTERM");
    throw error;
  }
}
async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise((resolve) => child.once("close", resolve));
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 3000);
  await closed;
  clearTimeout(timer);
}
async function sampleResources() {
  let ticks = 0,
    pssKiB = 0;
  // /proc belongs to the private PID namespace, so samples cannot inspect sibling work.
  for (const name of await NodeFSP.readdir("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = await NodeFSP.readFile(`/proc/${name}/stat`, "utf8");
      const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      ticks += Number(fields[11]) + Number(fields[12]);
      const memory = await NodeFSP.readFile(`/proc/${name}/smaps_rollup`, "utf8");
      pssKiB += Number(memory.match(/^Pss:\s+(\d+)/m)?.[1] || 0);
    } catch (error) {
      if (!["ENOENT", "ESRCH", "EACCES"].includes(error.code)) throw error;
    }
  }
  return { ticks, pssKiB, at: performance.now() };
}
export async function runInner() {
  const configuration = JSON.parse(await NodeFSP.readFile("/harness/config.json", "utf8"));
  let app, xvfb, ctx, timer;
  const start = performance.now();
  const result = {
    status: "failed",
    isolation: null,
    runtime: null,
    window: null,
    resources: {
      cpuUnit: "100% = one fully used logical core",
      clockTicksPerSecond: configuration.clockTicks,
      measuredPeakScope: "after app readiness through scenario",
      settledIntervalSeconds: 20,
      peakCpuPercent: 0,
      peakPssKiB: 0,
    },
  };
  let code = 2;
  try {
    result.isolation = await isolationProbe(configuration.realHome);
    const scenario = await loadScenario("/harness/scenario.mjs");
    xvfb = await startXvfb(configuration.size);
    const { _electron } = await import("playwright-core");
    app = await _electron.launch({
      executablePath: "/electron/electron",
      args: [
        "/app/apps/desktop/dist-electron/boot.cjs",
        `--force-device-scale-factor=${configuration.scale}`,
      ],
      env: { ...process.env, DISPLAY: xvfb.display },
      chromiumSandbox: true,
      timeout: 60000,
    });
    const page = await app.firstWindow({ timeout: 60000 });
    await page.waitForFunction(() => Boolean(window.desktopBridge), undefined, { timeout: 60000 });
    const networkState = await app.context().newCDPSession(page);
    await networkState.send("Network.enable");
    result.isolation.browserConnectivity = {
      kind: "CDP navigator state only",
      latencyMarkerMs: 1,
      externalNetwork: "unavailable in private namespace",
      readbacks: [],
    };
    const restoreConnectivity = async () => {
      const onlineBefore = await page.evaluate(() => navigator.onLine);
      // A nonneutral CDP state enables local-only connectivity without adding a network interface.
      await networkState.send("Network.overrideNetworkState", {
        offline: false,
        latency: 1,
        downloadThroughput: -1,
        uploadThroughput: -1,
        connectionType: "ethernet",
      });
      await page.waitForFunction(() => navigator.onLine === true);
      result.isolation.browserConnectivity.readbacks.push({ onlineBefore, onlineAfter: true });
    };
    await restoreConnectivity();
    const paths = await app.evaluate(({ app }) => ({
      userData: app.getPath("userData"),
      home: app.getPath("home"),
      t3home: process.env.T3CODE_HOME,
    }));
    if (
      !paths.userData.startsWith("/scratch/xdg-config/") ||
      paths.home !== "/scratch/home" ||
      paths.t3home !== "/scratch/t3home"
    )
      throw new Error("Electron wrote outside its private data roots");
    result.isolation.assertedPaths = paths;
    ctx = createScenarioContext({
      electronApp: app,
      page,
      workspace: "/scratch/workspace",
      artifacts: "/artifacts",
      size: configuration.size,
      theme: configuration.theme,
      configuration,
      restoreConnectivity,
    });
    await ctx.topology();
    await ctx.setWindowSize(configuration.size.width, configuration.size.height);
    await ctx.setTheme(configuration.theme);
    result.resources.startupSeconds = (performance.now() - start) / 1000;
    result.runtime = await app.evaluate(({ app }) => ({
      electron: process.versions.electron,
      chromium: process.versions.chrome,
      node: process.versions.node,
      appVersion: app.getVersion(),
    }));
    result.runtime.previewAutomationIdentity = await page.evaluate(async () =>
      window.desktopBridge.getPreviewAutomationRuntimeIdentity
        ? await window.desktopBridge.getPreviewAutomationRuntimeIdentity()
        : null,
    );
    result.runtime.os = {
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone evidence script has no Effect runtime.
      platform: NodeOS.platform(),
      release: NodeOS.release(),
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone evidence script has no Effect runtime.
      arch: NodeOS.arch(),
    };
    result.runtime.xvfb = { ...configuration.size, depth: 24 };
    let previous = await sampleResources();
    const idleStart = previous;
    let sampling = false;
    timer = setInterval(async () => {
      if (sampling) return;
      sampling = true;
      try {
        const current = await sampleResources();
        const cpu = Math.max(
          0,
          ((current.ticks - previous.ticks) /
            configuration.clockTicks /
            ((current.at - previous.at) / 1000)) *
            100,
        );
        result.resources.peakCpuPercent = Math.max(result.resources.peakCpuPercent, cpu);
        result.resources.peakPssKiB = Math.max(result.resources.peakPssKiB, current.pssKiB);
        previous = current;
      } catch {
        result.resources.samplingIncomplete = true;
      } finally {
        sampling = false;
      }
    }, 1000);
    await new Promise((resolve) => setTimeout(resolve, 20000));
    const settled = await sampleResources();
    result.resources.settledCpuPercent = Math.max(
      0,
      ((settled.ticks - idleStart.ticks) /
        configuration.clockTicks /
        ((settled.at - idleStart.at) / 1000)) *
        100,
    );
    result.resources.settledPssKiB = settled.pssKiB;
    result.resources.settledHostSharePercent =
      result.resources.settledCpuPercent / configuration.hostEffectiveCores;
    code = 1;
    const captureStart = performance.now();
    await scenario.run(ctx);
    result.resources.scenarioSeconds = (performance.now() - captureStart) / 1000;
    result.window = await app.evaluate(({ BrowserWindow, nativeTheme }) => ({
      bounds: BrowserWindow.getAllWindows()[0].getBounds(),
      theme: nativeTheme.shouldUseDarkColors ? "dark" : "light",
      themeSource: nativeTheme.themeSource,
    }));
    result.window.viewport = await page.evaluate(() => ({
      width: innerWidth,
      height: innerHeight,
      scale: devicePixelRatio,
    }));
    if (Math.abs(result.window.viewport.scale - configuration.scale) > 0.01)
      throw new Error("Requested device scale readback mismatch");
    result.status = "passed";
    code = 0;
  } catch (error) {
    result.error = error.message;
    if (ctx)
      try {
        await ctx.capture("failure");
      } catch {
        result.failureCapture = "unavailable";
      }
  } finally {
    clearInterval(timer);
    if (ctx) Object.assign(result, ctx.results());
    const teardown = performance.now();
    if (app)
      try {
        await app.close();
      } catch (error) {
        result.closeError = error.message;
        if (code === 0) {
          code = 1;
          result.status = "failed";
        }
      }
    await stop(xvfb?.child);
    result.resources.teardownSeconds = (performance.now() - teardown) / 1000;
    await NodeFSP.writeFile("/artifacts/inner.json", JSON.stringify(result, null, 2), {
      mode: 0o600,
    });
  }
  return code;
}
if (
  process.argv.includes("--inner") &&
  import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href
)
  process.exitCode = await runInner();
