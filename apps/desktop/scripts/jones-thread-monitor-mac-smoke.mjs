/* oxlint-disable t3code/no-global-process-runtime -- Standalone Node CLI composition root reads real host prerequisites and evidence; no Effect runtime is being substituted. */
import * as NodeUtil from "node:util";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeTimersPromises from "node:timers/promises";
import { createScenarioContext, assertLoopbackEndpoint } from "./ui-evidence/scenario-api.mjs";
import { waitForMainPage } from "./ui-evidence/main-window.mjs";
const repo = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const require = NodeModule.createRequire(import.meta.url);
const monitorEntry = "/thread-monitor.html";
const timeout = 60_000;
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
class SmokeFailure extends Error {}
function check(condition, reason) {
  if (!condition) throw new SmokeFailure(reason);
}

// Never inherit runner tokens, provider configuration, shell init, or Node injection.
export function childEnvironment(root) {
  check(NodePath.isAbsolute(root), "Private scratch root must be absolute");
  return {
    PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
    HOME: NodePath.join(root, "home"),
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CONFIG_HOME: NodePath.join(root, "config"),
    XDG_DATA_HOME: NodePath.join(root, "data"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    T3CODE_HOME: NodePath.join(root, "server"),
    T3CODE_DESKTOP_USER_DATA_DIR: NodePath.join(root, "desktop"),
    T3CODE_DEV_SEED: "0",
    T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
    T3CODE_DISABLE_AUTO_UPDATE: "true",
    T3CODE_TELEMETRY_ENABLED: "false",
    AGENT_CLI_CREDENTIAL_STORE: "memory",
    OTEL_SDK_DISABLED: "true",
    LANG: "en_US.UTF-8",
    TZ: "UTC",
  };
}
async function until(read, accepts, reason, limit = timeout) {
  const deadline = performance.now() + limit;
  while (performance.now() < deadline) {
    const value = await read();
    if (accepts(value)) return value;
    await NodeTimersPromises.setTimeout(100);
  }
  throw new SmokeFailure(reason);
}
async function discoverPeer(app) {
  return until(
    async () => {
      const matches = [];
      for (const page of app.windows()) {
        try {
          if (new URL(page.url()).pathname !== monitorEntry) continue;
          const valid = await page.evaluate(
            () =>
              !window.desktopBridge &&
              ["dispatch", "onFrame", "ready"].every(
                (key) => typeof window.threadMonitor?.[key] === "function",
              ),
          );
          if (valid) matches.push(page);
        } catch {
          /* A window still navigating is not a role match. */
        }
      }
      check(matches.length <= 1, "Ambiguous thread monitor renderer identity");
      return matches[0] ?? null;
    },
    Boolean,
    "Missing real thread-monitor.html renderer and its dedicated preload bridge",
  );
}
async function nativeState(handle) {
  return handle.evaluate((win) => ({
    id: win.id,
    visible: win.isVisible(),
    minimized: win.isMinimized(),
    focused: win.isFocused(),
    alwaysOnTop: win.isAlwaysOnTop(),
    bounds: win.getBounds(),
  }));
}
export async function runSmoke() {
  const evidenceRoot = NodePath.join(repo, "artifacts/thread-monitor-mac");
  await NodeFSP.mkdir(evidenceRoot, {
    recursive: true,
    mode: 0o700,
  });
  const output = await NodeFSP.mkdtemp(NodePath.join(evidenceRoot, "run-"));
  const result = {
    status: "running",
    startedAt: new Date().toISOString(),
    steps: [],
    captures: [],
    host: {
      platform: NodeOS.platform(),
      arch: NodeOS.arch(),
      release: NodeOS.release(),
      node: process.version,
    },
    limitations: [
      "Unpackaged compiled feature boot; no installed updater, signing, packaging or release proof.",
      "Single hosted macOS GUI session; Spaces, fullscreen, multi-display, Windows and Linux are separate evidence.",
      "Metadata-only server commands; provider replies and approval/input/plan execution are covered elsewhere.",
      "Private HOME/profile and allowlisted child environment, not an OS filesystem or network sandbox.",
      "Electron native window readbacks prove minimization/visibility/focus; cross-application OS z-order is unmeasured.",
    ],
  };
  let root,
    app,
    child,
    main,
    peer,
    mainWindow,
    peerWindow,
    runningWork,
    phase = "prerequisites";
  let stopping = false;
  const abort = new AbortController();
  const signals = new Map();
  const bounded = async (operation, milliseconds, reason) => {
    let timer;
    try {
      return await Promise.race([
        operation,
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new SmokeFailure(reason)), milliseconds);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  const stop = async () => {
    if (!app || stopping) return;
    stopping = true;
    let graceful = true;
    try {
      await bounded(app.close(), 20_000, "Owned Electron app did not close within 20 seconds");
    } catch {
      graceful = false;
      // Only the captured spawn object can be terminated; never search for a PID.
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      try {
        await until(
          () => child?.exitCode !== null || child?.signalCode !== null,
          Boolean,
          "Owned Electron process remained alive after close",
          10_000,
        );
      } catch {
        if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }
    }
    await until(
      () => child.exitCode !== null || child.signalCode !== null,
      Boolean,
      "Owned Electron process exit is unproved",
      5_000,
    );
    result.cleanup = {
      appClosed: true,
      graceful,
      pid: child.pid,
      exitCode: child.exitCode,
      signal: child.signalCode,
    };
    if (!graceful) {
      result.status = "failed";
      result.cleanup.reason =
        "Forced owned process exit; backend shutdown unproved, private state retained for runner disposal";
    }
  };
  const step = async (name, expected, action) => {
    abort.signal.throwIfAborted();
    phase = name;
    const entry = {
      name,
      expected,
      status: "running",
      startedAt: new Date().toISOString(),
    };
    result.steps.push(entry);
    try {
      entry.observed = await bounded(
        action(),
        90_000,
        `${name}: prerequisite or behavior timed out`,
      );
      abort.signal.throwIfAborted();
      entry.status = "passed";
    } catch (error) {
      entry.status = "failed";
      throw error;
    }
  };
  const state = () => main.evaluate(() => window.desktopBridge.threadMonitor.getState());
  const capture = async (page, name) => {
    const bytes = await page.screenshot({
      type: "png",
      animations: "disabled",
      timeout: 15_000,
    });
    const file = `${name}.png`;
    await NodeFSP.writeFile(NodePath.join(output, file), bytes, {
      flag: "wx",
      mode: 0o600,
    });
    result.captures.push({
      file,
      sha256: sha256(bytes),
      bytes: bytes.length,
    });
  };
  const minimized = async () => {
    const [mainNative, peerNative, bridge] = await Promise.all([
      nativeState(mainWindow),
      nativeState(peerWindow),
      state(),
    ]);
    result.lastWindowReadback = {
      main: mainNative,
      monitor: peerNative,
      bridge,
    };
    check(
      mainNative.minimized &&
        !mainNative.focused &&
        peerNative.visible &&
        peerNative.alwaysOnTop &&
        !bridge.mainVisible &&
        !bridge.mainFocused &&
        bridge.visible,
      "Main must stay minimized/unfocused while monitor stays visible and always-on-top",
    );
    return {
      main: mainNative,
      monitor: peerNative,
      bridge,
    };
  };
  try {
    check(NodeOS.platform() === "darwin", "Prerequisite: real macOS GUI runner required");
    check(
      process.env.RUNNER_TEMP && NodePath.isAbsolute(process.env.RUNNER_TEMP),
      "Prerequisite: absolute RUNNER_TEMP required",
    );
    const git = (...args) =>
      NodeChildProcess.execFileSync("git", args, {
        cwd: repo,
        encoding: "utf8",
        timeout: 10_000,
        stdio: ["ignore", "pipe", "ignore"],
      }).trim();
    const source = git("rev-parse", "HEAD"),
      tree = git("rev-parse", "HEAD^{tree}");
    check(
      /^[a-f0-9]{40}$/.test(process.env.JONES_MONITOR_SOURCE_SHA ?? "") &&
        source === process.env.JONES_MONITOR_SOURCE_SHA,
      "Prerequisite: checkout must equal the exact requested feature source SHA",
    );
    check(
      tree === process.env.JONES_MONITOR_SOURCE_TREE,
      "Prerequisite: source tree must match checkout receipt",
    );
    result.source = {
      repository: "Jones-Systems/Jones-Code",
      commit: source,
      tree,
      compiled: {},
    };
    for (const file of [
      "apps/desktop/dist-electron/boot.cjs",
      "apps/desktop/dist-electron/main.cjs",
      "apps/desktop/dist-electron/preload.cjs",
      "apps/desktop/dist-electron/jones/threadMonitor/preload.cjs",
      "apps/server/dist/bin.mjs",
      "apps/server/dist/client/thread-monitor.html",
    ]) {
      const bytes = await NodeFSP.readFile(NodePath.join(repo, file)).catch(() => null);
      check(
        bytes?.length,
        `Prerequisite: missing compiled feature artifact ${file}; run vp run build:desktop`,
      );
      result.source.compiled[file] = sha256(bytes);
    }
    root = await NodeFSP.mkdtemp(
      NodePath.join(await NodeFSP.realpath(process.env.RUNNER_TEMP), "jones-monitor-mac-"),
    );
    await NodeFSP.chmod(root, 0o700);
    for (const signal of ["SIGINT", "SIGTERM"]) {
      const handler = () => abort.abort(new SmokeFailure(`Cancelled by ${signal}`));
      signals.set(signal, handler);
      process.once(signal, handler);
    }
    const deadline = setTimeout(
      () => abort.abort(new SmokeFailure("Smoke exceeded its eight-minute runtime bound")),
      480_000,
    );
    signals.set("deadline", deadline);
    const env = childEnvironment(root);
    for (const directory of [
      "home",
      "tmp",
      "config",
      "data",
      "cache",
      "server",
      "desktop",
      "workspace",
    ]) {
      await NodeFSP.mkdir(NodePath.join(root, directory), {
        mode: 0o700,
      });
    }
    const workspace = NodePath.join(root, "workspace");
    const preference = NodePath.join(root, "server/userdata/jones-thread-monitor.json");
    check(
      !(await NodeFSP.stat(preference).catch(() => null)),
      "Private first launch unexpectedly has a saved monitor preference",
    );
    result.isolation = {
      root,
      home: env.HOME,
      serverHome: env.T3CODE_HOME,
      desktopProfile: env.T3CODE_DESKTOP_USER_DATA_DIR,
      workspace,
      environmentKeys: Object.keys(env).sort(),
      seed: false,
      autoUpdate: false,
      preferenceInitiallyAbsent: true,
    };
    phase = "launch compiled Electron in native WindowServer session";
    const { _electron } = await import("playwright-core").catch(() => {
      throw new SmokeFailure("Prerequisite: installed playwright-core package is unavailable");
    });
    let executablePath;
    try {
      executablePath = require("electron");
      await NodeFSP.access(executablePath);
    } catch {
      throw new SmokeFailure(
        "Prerequisite: Electron executable is unavailable; run ensure:electron",
      );
    }
    app = await _electron.launch({
      executablePath,
      cwd: repo,
      // Electron's own macOS process tests use this to avoid the system cookie-encryption Keychain.
      // https://releases.electronjs.org/pr/53790
      args: [NodePath.join(repo, "apps/desktop/dist-electron/boot.cjs"), "--use-mock-keychain"],
      env,
      timeout,
      chromiumSandbox: true,
    });
    child = app.process();
    check(Number.isInteger(child?.pid), "Electron launch did not return its owned spawn PID");
    result.process = {
      pid: child.pid,
      logs: {
        stdoutBytes: 0,
        stderrBytes: 0,
        retained: false,
      },
    };
    // Drain pipes without retaining arbitrary application or third-party output.
    child.stdout?.on("data", (bytes) => {
      result.process.logs.stdoutBytes += bytes.length;
    });
    child.stderr?.on("data", (bytes) => {
      result.process.logs.stderrBytes += bytes.length;
    });
    const work = async () => {
      await step(
        "Native runtime and private storage",
        "Real displays, exact PID and isolated Electron paths",
        async () => {
          const runtime = await app.evaluate(({ app, screen }) => ({
            pid: process.pid,
            versions: process.versions,
            home: app.getPath("home"),
            userData: app.getPath("userData"),
            sessionData: app.getPath("sessionData"),
            serverHome: process.env.T3CODE_HOME,
            displays: screen.getAllDisplays().map(({ id, bounds, workArea, scaleFactor }) => ({
              id,
              bounds,
              workArea,
              scaleFactor,
            })),
            seed: process.env.T3CODE_DEV_SEED,
            updates: process.env.T3CODE_DISABLE_AUTO_UPDATE,
            mockKeychain: app.commandLine.hasSwitch("use-mock-keychain"),
            credentialStore: process.env.AGENT_CLI_CREDENTIAL_STORE,
            secretKeysPresent: Object.keys(process.env).filter(
              (key) =>
                key !== "AGENT_CLI_CREDENTIAL_STORE" &&
                /TOKEN|SECRET|API_KEY|CREDENTIAL|NODE_OPTIONS/.test(key),
            ),
          }));
          check(
            runtime.pid === child.pid && runtime.displays.length > 0,
            "Prerequisite: actual spawned Electron and native display required",
          );
          check(
            runtime.home === env.HOME &&
              runtime.userData === env.T3CODE_DESKTOP_USER_DATA_DIR &&
              runtime.sessionData === env.T3CODE_DESKTOP_USER_DATA_DIR &&
              runtime.serverHome === env.T3CODE_HOME &&
              runtime.seed === "0" &&
              runtime.updates === "true" &&
              runtime.mockKeychain &&
              runtime.credentialStore === "memory" &&
              runtime.secretKeysPresent.length === 0,
            "Electron private HOME/profile/server-home or child environment isolation readback failed",
          );
          return runtime;
        },
      );
      main = await waitForMainPage(app).catch(() => {
        throw new SmokeFailure(
          "Prerequisite: real main desktop renderer bridge did not become ready",
        );
      });
      main.setDefaultTimeout(30_000);
      mainWindow = await app.browserWindow(main);
      check(
        await main.evaluate(() =>
          ["getState", "setEnabled", "onState"].every(
            (key) => typeof window.desktopBridge.threadMonitor?.[key] === "function",
          ),
        ),
        "Prerequisite: native optional threadMonitor bridge is unavailable",
      );
      const ctx = createScenarioContext({
        electronApp: app,
        page: main,
        workspace,
        artifacts: output,
        size: {
          width: 1280,
          height: 800,
        },
        theme: "dark",
      });
      let localEnvironmentId;
      await step(
        "Provider-free onboarding",
        "Existing first-run UI helper; one private loopback backend",
        async () => {
          await main.waitForFunction(
            () =>
              document.body.innerText.includes("Connect your computers") ||
              !!document.querySelector('[data-testid="composer-editor"]') ||
              document.querySelector("aside"),
          );
          await ctx.completeOnboarding();
          const endpoint = await ctx.topology();
          assertLoopbackEndpoint(endpoint.httpBaseUrl, ["http:"]);
          localEnvironmentId = await main.evaluate(async (base) => {
            const response = await fetch(new URL("/.well-known/t3/environment", base), {
              signal: AbortSignal.timeout(15_000),
              credentials: "omit",
            });
            if (!response.ok) throw new Error("Private environment descriptor unavailable");
            return (await response.json()).environmentId;
          }, endpoint.httpBaseUrl);
          check(
            typeof localEnvironmentId === "string" && localEnvironmentId.length > 0,
            "Private backend did not return an environment identity",
          );
          const database = NodePath.join(root, "server/userdata/statev2.sqlite");
          check(
            (await NodeFSP.stat(database)).isFile(),
            "Private synthetic database was not created at the documented server path",
          );
          return {
            endpoint,
            environmentId: localEnvironmentId,
            database,
            inode: (await NodeFSP.stat(database)).ino,
          };
        },
      );
      peer = await discoverPeer(app);
      peer.setDefaultTimeout(30_000);
      peerWindow = await app.browserWindow(peer);
      await step(
        "Dedicated native monitor peer",
        "Isolated sandbox session and only dispatch/onFrame/ready bridge",
        async () => {
          const peerKeys = await peer.evaluate(() => Object.keys(window.threadMonitor).sort());
          check(
            NodeUtil.isDeepStrictEqual(peerKeys, ["dispatch", "onFrame", "ready"]),
            "Dedicated peer exposes unexpected bridge methods",
          );
          const isolation = await peerWindow.evaluate((win) => {
            const preferences = win.webContents.getLastWebPreferences();
            return {
              storagePath: win.webContents.session.getStoragePath(),
              sandbox: preferences.sandbox,
              contextIsolation: preferences.contextIsolation,
              nodeIntegration: preferences.nodeIntegration,
            };
          });
          check(
            !isolation.storagePath &&
              isolation.sandbox &&
              isolation.contextIsolation &&
              !isolation.nodeIntegration,
            "Native monitor peer is not an ephemeral isolated sandbox",
          );
          return {
            peerKeys,
            ...isolation,
          };
        },
      );
      await main.evaluate(() => {
        window.__monitorSmoke = {
          states: [],
          intents: [],
        };
        window.__monitorSmokeStops = [
          window.desktopBridge.threadMonitor.onState((value) => {
            if (window.__monitorSmoke.states.length < 128) window.__monitorSmoke.states.push(value);
          }),
          window.desktopBridge.threadMonitor.onIntent((intent) => {
            if (window.__monitorSmoke.intents.length < 128)
              window.__monitorSmoke.intents.push({
                type: intent.type,
                environmentId: intent.environmentId,
                threadId: intent.threadId,
                actionId: intent.actionId,
              });
          }),
        ];
      });
      await step(
        "Absent preference defaults visible",
        "Enabled native monitor visible without setEnabled call",
        async () => {
          const value = await until(
            async () => {
              const current = await state();
              check(
                current.supported,
                `Native monitor prerequisite failed: ${current.reason ?? "backend unsupported"}`,
              );
              return current;
            },
            (s) => s.enabled && s.visible,
            "Default-visible monitor failed to become ready",
          );
          const native = await nativeState(peerWindow);
          check(
            native.visible && native.alwaysOnTop,
            "Default native monitor is not visible and always-on-top",
          );
          await capture(main, "main-synthetic-empty");
          await capture(peer, "monitor-default-notch");
          return {
            bridge: value,
            native,
            url: peer.url(),
          };
        },
      );
      await step(
        "Hide and show synchronization",
        "Real setEnabled, native visibility, persisted choice and onState agree",
        async () => {
          const before = await ctx.readSnapshot();
          await main.evaluate(() => window.desktopBridge.threadMonitor.setEnabled(false));
          await until(
            state,
            (s) => !s.enabled && !s.visible,
            "Hide did not synchronize native state",
          );
          check(!(await nativeState(peerWindow)).visible, "Hidden monitor window remains visible");
          check(
            JSON.parse(await NodeFSP.readFile(preference, "utf8")).enabled === false,
            "Hidden preference was not persisted",
          );
          await main.evaluate(() => window.desktopBridge.threadMonitor.setEnabled(true));
          const shown = await until(
            state,
            (s) => s.enabled && s.visible,
            "Show did not synchronize native state",
          );
          check(
            JSON.parse(await NodeFSP.readFile(preference, "utf8")).enabled === true,
            "Shown preference was not persisted",
          );
          check(
            NodeUtil.isDeepStrictEqual(await ctx.readSnapshot(), before),
            "Visibility changes must not change domain state",
          );
          return shown;
        },
      );
      let minimizedObservationIndex;
      await step(
        "Actual main minimization",
        "BrowserWindow.minimize and native minimized/visibility/focus readbacks",
        async () => {
          await mainWindow.evaluate((win) => win.minimize());
          await until(
            () => nativeState(mainWindow),
            (s) => s.minimized && !s.focused,
            "Prerequisite: macOS WindowServer did not minimize the actual main window",
          );
          await until(
            state,
            (s) => !s.mainVisible && !s.mainFocused,
            "Main minimized state did not reach bridge",
          );
          minimizedObservationIndex = await main.evaluate(
            () => window.__monitorSmoke.states.length,
          );
          return minimized();
        },
      );
      await step(
        "Passive hover expansion",
        "Real hover expands monitor and never restores/focuses main",
        async () => {
          await peer
            .getByRole("button", {
              name: /^Open thread monitor:/,
            })
            .hover();
          await until(state, (s) => s.expanded, "Native monitor did not expand from real hover");
          return minimized();
        },
      );
      const projectId = NodeCrypto.randomUUID(),
        threadId = NodeCrypto.randomUUID();
      const title = "Synthetic macOS monitor thread",
        renamed = "Synthetic live update while minimized";
      let environmentId;
      await step(
        "Live metadata while main minimized",
        "Real metadata commands update monitor title and Waiting count without restoring main",
        async () => {
          await ctx.dispatch({
            type: "project.create",
            commandId: NodeCrypto.randomUUID(),
            projectId,
            title: "Synthetic macOS monitor project",
            workspaceRoot: workspace,
          });
          await ctx.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: NodeCrypto.randomUUID(),
            projectId,
            threadId,
            title,
            modelSelection: {
              instanceId: "codex",
              model: "gpt-5",
            },
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
          });
          // The source's stable row identity is the exact environment/thread tuple.
          const liveRow = peer.locator("[data-row-open]").filter({
            hasText: title,
          });
          await liveRow.waitFor({
            state: "visible",
          });
          const identity = JSON.parse(await liveRow.getAttribute("data-row-open"));
          check(
            Array.isArray(identity) &&
              identity.length === 2 &&
              identity[1] === threadId &&
              identity[0] === localEnvironmentId,
            "Monitor row lost exact private backend/thread identity",
          );
          environmentId = identity[0];
          await ctx.dispatch({
            type: "thread.metadata.update",
            commandId: NodeCrypto.randomUUID(),
            threadId,
            title: renamed,
          });
          await peer
            .locator("[data-row-open]")
            .filter({
              hasText: renamed,
            })
            .waitFor({
              state: "visible",
            });
          await peer.waitForFunction(
            () =>
              document.querySelector('.count-strip [data-status="waiting"] strong')?.textContent ===
              "1",
          );
          const snapshot = await ctx.readSnapshot();
          check(
            snapshot.threads.some((thread) => thread.id === threadId && thread.title === renamed),
            "Backend did not retain synthetic rename",
          );
          await capture(peer, "monitor-live-main-minimized");
          return {
            projectId,
            threadId,
            environmentId,
            title: renamed,
            waitingCount: 1,
            windows: await minimized(),
          };
        },
      );
      await step(
        "Monitor hide has no run effect",
        "Peer hide preserves exact synthetic domain snapshot and minimized main",
        async () => {
          const before = await ctx.readSnapshot();
          await peer
            .getByRole("button", {
              name: "Hide thread monitor",
              exact: true,
            })
            .click();
          await until(
            state,
            (s) => !s.enabled && !s.visible && !s.mainVisible && !s.mainFocused,
            "Peer hide did not preserve main minimization",
          );
          check(
            NodeUtil.isDeepStrictEqual(await ctx.readSnapshot(), before),
            "Peer hide must not issue domain run commands",
          );
          await main.evaluate(() => window.desktopBridge.threadMonitor.setEnabled(true));
          await until(state, (s) => s.enabled && s.visible, "Monitor did not show after peer hide");
          await peer.locator(".monitor-root").hover();
          await until(state, (s) => s.expanded, "Monitor did not expand after show");
          return minimized();
        },
      );
      await step(
        "Open exact thread in Jones Code",
        "Real peer UI action reveals main and routes exact environment/thread",
        async () => {
          const passiveStates = await main.evaluate(
            (index) => window.__monitorSmoke.states.slice(index),
            minimizedObservationIndex,
          );
          check(
            passiveStates.every((s) => !s.mainVisible && !s.mainFocused),
            "Passive monitor interaction restored or focused main before Open",
          );
          await peer
            .locator("[data-row-open]")
            .filter({
              hasText: renamed,
            })
            .click();
          await peer
            .getByRole("button", {
              name: "Open in Jones Code",
            })
            .click();
          await main.waitForFunction(
            ({ environmentId, threadId }) => {
              const route = new URL(location.href).pathname
                .split("/")
                .filter(Boolean)
                .map(decodeURIComponent);
              return route.at(-2) === environmentId && route.at(-1) === threadId;
            },
            {
              environmentId,
              threadId,
            },
          );
          await until(
            () => nativeState(mainWindow),
            (s) => !s.minimized && s.visible && s.focused,
            "Open did not restore and focus the actual main window",
          );
          await until(
            state,
            (s) => s.mainVisible && s.mainFocused,
            "Open main visibility/focus did not synchronize",
          );
          const observed = await main.evaluate(() => window.__monitorSmoke);
          check(
            observed.intents.some(
              (intent) =>
                intent.type === "open" &&
                intent.environmentId === environmentId &&
                intent.threadId === threadId,
            ),
            "Exact typed open action was not observed",
          );
          check(
            observed.states.some((s) => !s.enabled && !s.visible) &&
              observed.states.some((s) => s.visible && !s.mainVisible && !s.mainFocused),
            "Native onState did not report hidden and minimized-main transitions",
          );
          await capture(main, "main-open-exact-synthetic-thread");
          await capture(peer, "monitor-synthetic-detail");
          return {
            route: new URL(main.url()).pathname,
            main: await nativeState(mainWindow),
            bridge: await state(),
            observations: observed,
          };
        },
      );
      abort.signal.throwIfAborted();
      result.status = "passed";
    };
    abort.signal.throwIfAborted();
    runningWork = work();
    await Promise.race([
      runningWork,
      new Promise((_, reject) => {
        abort.signal.addEventListener("abort", () => reject(abort.signal.reason), {
          once: true,
        });
      }),
    ]);
  } catch (error) {
    result.status = "failed";
    // Third-party errors can contain launch environments and URLs; retain only our bounded reasons.
    result.failure = {
      phase,
      reason:
        error instanceof SmokeFailure
          ? error.message
          : `Failed during ${phase}; prerequisite/behavior not proved (raw error suppressed)`,
    };
  } finally {
    try {
      await stop();
    } catch {
      result.status = "failed";
      result.cleanup = {
        appClosed: false,
        reason: "Owned app exit unproved; private state retained for runner disposal",
      };
    }
    if (runningWork) {
      try {
        await bounded(
          runningWork.catch(() => undefined),
          10_000,
          "Smoke work did not drain after owned app close",
        );
      } catch {
        result.status = "failed";
        result.cleanup = {
          ...result.cleanup,
          workDrained: false,
        };
      }
    }
    for (const [signal, handler] of signals) {
      if (signal === "deadline") clearTimeout(handler);
      else process.removeListener(signal, handler);
    }
    if (root && (!app || result.cleanup?.graceful) && result.cleanup?.workDrained !== false) {
      try {
        await NodeFSP.rm(root, {
          recursive: true,
        });
        check(
          !(await NodeFSP.stat(root).catch(() => null)),
          "Owned scratch cleanup readback failed",
        );
        result.scratchRemoved = true;
      } catch {
        result.status = "failed";
        result.scratchRemoved = false;
      }
    }
    result.finishedAt = new Date().toISOString();
    await NodeFSP.writeFile(
      NodePath.join(output, "result.json"),
      JSON.stringify(result, null, 2) + "\n",
      {
        mode: 0o600,
      },
    );
  }
  console.log(
    `Native macOS thread monitor smoke: ${result.status}; ${NodePath.relative(repo, output)}/result.json`,
  );
  if (result.failure) console.log(result.failure.reason);
  if (result.status !== "passed") process.exitCode = 1;
  return result;
}
if (process.argv[1] && NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url))
  await runSmoke();
