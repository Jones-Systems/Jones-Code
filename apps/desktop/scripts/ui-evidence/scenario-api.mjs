import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { sha256 } from "./provenance.mjs";

export function validateCaptureName(name) {
  if (typeof name !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(name))
    throw new Error("Capture name must be a short plain filename stem");
  return name;
}
export function assertLoopbackEndpoint(value, protocols = ["http:", "ws:"]) {
  const url = new URL(value);
  if (
    !protocols.includes(url.protocol) ||
    !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error("Backend endpoint must be private loopback without credentials");
  return url;
}
export async function loadScenario(file) {
  const bytes = await NodeFSP.readFile(file);
  const module = await import(`${NodeURL.pathToFileURL(file).href}?sha256=${sha256(bytes)}`);
  if (sha256(await NodeFSP.readFile(file)) !== sha256(bytes))
    throw new Error("Scenario changed during loading");
  if (typeof module.default !== "function")
    throw new Error("Scenario must export a default async function");
  return { run: module.default, identity: { path: file, sha256: sha256(bytes) } };
}
export function pngDimensions(bytes) {
  if (
    bytes.length < 24 ||
    !bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])) ||
    bytes.toString("ascii", 12, 16) !== "IHDR"
  )
    throw new Error("Screenshot is not a PNG");
  const width = bytes.readUInt32BE(16),
    height = bytes.readUInt32BE(20);
  if (!width || !height) throw new Error("Screenshot has invalid dimensions");
  return { width, height };
}
// This function is serialized into the isolated renderer; keep it self-contained.
export async function requestLocalBackend(
  { endpoint, method, payload },
  { desktopBridge, fetch, WebSocket, setTimeout, clearTimeout } = globalThis,
) {
  if (method !== "snapshot" &&
      (method !== "dispatch" || !["project.create", "thread.create", "thread.metadata.update"].includes(payload?.type)))
    throw new Error("Scenario fixture dispatch is limited to project/thread metadata");
  for (const [key, protocol] of [["httpBaseUrl", "http:"], ["wsBaseUrl", "ws:"]]) {
    const url = new URL(endpoint[key]);
    if (url.protocol !== protocol || !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname) ||
        url.username || url.password || url.search || url.hash)
      throw new Error("Backend endpoint must be private loopback without credentials");
  }
  if (method === "dispatch") {
    const fields = {
      "project.create": ["type", "commandId", "projectId", "title", "workspaceRoot"],
      "thread.create": ["type", "commandId", "createdBy", "creationSource", "threadId", "projectId", "title", "modelSelection", "runtimeMode", "interactionMode", "branch", "worktreePath"],
      "thread.metadata.update": ["type", "commandId", "threadId", "title"],
    }[payload.type];
    if (Object.keys(payload).some((key) => !fields.includes(key)))
      throw new Error("Fixture command contains fields outside metadata scope");
    if (payload.type === "thread.create" &&
        (payload.createdBy !== "user" || payload.creationSource !== "web" ||
         payload.branch !== null || payload.worktreePath !== null))
      throw new Error("Fixture thread must use the private project without a worktree");
  }
  const token = await desktopBridge.getLocalEnvironmentBearerToken();
  const headers = { authorization: `Bearer ${token}` };
  if (method === "snapshot") {
    const response = await fetch(new URL("/api/orchestration/shell", endpoint.httpBaseUrl), {
      headers: { ...headers, "x-t3-orchestration-protocol": "2" },
      credentials: "omit",
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Snapshot HTTP status ${response.status}`);
    return response.json();
  }
  if (payload.type === "project.create") {
    const response = await fetch(new URL("/api/projects/mutate", endpoint.httpBaseUrl), {
      method: "POST",
      headers: { ...headers, "content-type": "application/json" },
      credentials: "omit",
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Project mutation HTTP status ${response.status}`);
    return response.json();
  }
  const response = await fetch(new URL("/api/auth/websocket-ticket", endpoint.httpBaseUrl), {
    method: "POST",
    headers,
    credentials: "omit",
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error(`WebSocket authorization HTTP status ${response.status}`);
  const { ticket } = await response.json();
  if (typeof ticket !== "string" || !ticket) throw new Error("WebSocket authorization missing ticket");
  const url = new URL(endpoint.wsBaseUrl);
  url.searchParams.set("wsTicket", ticket);
  url.searchParams.set("orchestrationProtocol", "2");
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url.href);
    let settled = false;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.close();
      if (error) reject(error);
      else resolve(value);
    };
    const timer = setTimeout(() => finish(new Error("Fixture RPC timed out")), 15000);
    socket.addEventListener("open", () => {
      if (settled) return;
      try {
        socket.send(JSON.stringify({
          _tag: "Request", id: "0", tag: "orchestration.dispatchCommand", payload, headers: [],
        }));
      } catch { finish(new Error("Fixture RPC send failed")); }
    });
    socket.addEventListener("message", ({ data }) => {
      if (settled) return;
      try {
        const message = JSON.parse(data);
        if (message._tag === "Ping") { socket.send(JSON.stringify({ _tag: "Pong" })); return; }
        if (message._tag !== "Exit" || String(message.requestId) !== "0") return;
        if (message.exit?._tag === "Success") finish(null, message.exit.value);
        else finish(new Error("Fixture RPC rejected metadata command"));
      } catch { finish(new Error("Invalid fixture RPC response")); }
    });
    socket.addEventListener("error", () => finish(new Error("Fixture RPC connection failed")));
    socket.addEventListener("close", () => finish(new Error("Fixture RPC closed before result")));
  });
}

export function createScenarioContext({
  electronApp,
  page,
  workspace,
  artifacts,
  size,
  theme,
  configuration = {},
  restoreConnectivity,
}) {
  const state = { steps: [], captures: [], assertions: [], fixture: {}, logs: [], topology: null };
  async function topology() {
    const result = await page.evaluate(() => {
      const values = window.desktopBridge.getLocalEnvironmentBootstraps();
      return values.map(({ id, httpBaseUrl, wsBaseUrl }) => ({ id, httpBaseUrl, wsBaseUrl }));
    });
    if (result.length !== 1 || result[0].id !== "primary")
      throw new Error("Expected exactly one isolated primary backend");
    assertLoopbackEndpoint(result[0].httpBaseUrl, ["http:"]);
    assertLoopbackEndpoint(result[0].wsBaseUrl, ["ws:"]);
    state.topology = result[0];
    return result[0];
  }
  async function request(method, payload) {
    const endpoint = await topology();
    return page.evaluate(requestLocalBackend, { endpoint, method, payload });
  }

  const ctx = {
    electronApp,
    page,
    workspace,
    artifacts,
    size,
    theme,
    configuration,
    fixture: state.fixture,
    topology,
    async step(name, fn) {
      if (typeof name !== "string" || !name.trim() || state.steps.length >= 1000)
        throw new Error("Invalid scenario step");
      const entry = { name, status: "running", startedAt: new Date().toISOString() };
      state.steps.push(entry);
      const start = performance.now();
      try {
        const result = await fn();
        entry.status = "passed";
        return result;
      } catch (error) {
        entry.status = "failed";
        throw error;
      } finally {
        entry.durationMs = Math.round(performance.now() - start);
      }
    },
    assert(condition, name) {
      state.assertions.push({ name, status: condition ? "passed" : "failed" });
      if (!condition) throw new Error(`Scenario assertion failed: ${name}`);
    },
    async capture(name) {
      validateCaptureName(name);
      if (state.captures.some((capture) => capture.name === name))
        throw new Error("Duplicate capture name");
      const window = await electronApp.evaluate(({ BrowserWindow, nativeTheme }) => {
        const win = BrowserWindow.getAllWindows()[0];
        return {
          bounds: win.getBounds(),
          theme: nativeTheme.shouldUseDarkColors ? "dark" : "light",
          themeSource: nativeTheme.themeSource,
        };
      });
      const viewport = await page.evaluate(() => ({
        width: innerWidth,
        height: innerHeight,
        scale: devicePixelRatio,
        renderedTheme: document.documentElement.classList.contains("dark") ? "dark" : "light",
      }));
      const bytes = await page.screenshot({ type: "png", animations: "disabled" });
      const dimensions = pngDimensions(bytes);
      const file = `${name}.png`;
      await NodeFSP.writeFile(NodePath.join(artifacts, file), bytes, { flag: "wx", mode: 0o600 });
      const capture = {
        name,
        file,
        sha256: sha256(bytes),
        ...dimensions,
        scale: viewport.scale,
        theme: viewport.renderedTheme,
        window,
        viewport,
      };
      state.captures.push(capture);
      return capture;
    },
    async completeOnboarding() {
      const heading = page.getByRole("heading", { name: "Connect your computers", exact: true });
      if (!(await heading.isVisible())) return false;
      await page.getByRole("button", { name: "Continue", exact: true }).click();
      await page.getByRole("heading", { name: "Connect your agents", exact: true }).waitFor();
      await page.getByRole("button", { name: "Continue", exact: true }).click();
      await page.getByRole("button", { name: "Do not import projects", exact: true }).click();
      await heading.waitFor({ state: "hidden" });
      await page.waitForFunction(
        () => location.pathname !== "/welcome" && !location.hash.includes("/welcome"),
      );
      ctx.assert(true, "Provider-free first-run UI completed in isolated profile");
      return true;
    },
    async dispatch(command) {
      if (!["project.create", "thread.create", "thread.metadata.update"].includes(command?.type))
        throw new Error("Scenario fixture dispatch is limited to project/thread metadata");
      if (command.type === "project.create" && command.workspaceRoot !== workspace)
        throw new Error("Fixture project must use the private workspace");
      return request("dispatch", command);
    },
    readSnapshot: () => request("snapshot"),
    async reload() {
      await page.reload({ waitUntil: "domcontentloaded" });
      await page.waitForFunction(() => Boolean(window.desktopBridge));
      if (restoreConnectivity) await restoreConnectivity();
    },
    async setTheme(value) {
      if (!["dark", "light", "system"].includes(value)) throw new Error("Invalid theme");
      const actual = await electronApp.evaluate(({ nativeTheme }, desired) => {
        nativeTheme.themeSource = desired;
        return { themeSource: nativeTheme.themeSource, dark: nativeTheme.shouldUseDarkColors };
      }, value);
      if (
        actual.themeSource !== value ||
        (value !== "system" && actual.dark !== (value === "dark"))
      )
        throw new Error("Native theme readback mismatch");
      await page.emulateMedia({ colorScheme: actual.dark ? "dark" : "light" });
      await page.waitForFunction(
        (dark) => matchMedia("(prefers-color-scheme: dark)").matches === dark,
        actual.dark,
      );
      await page.waitForFunction(
        (dark) => document.documentElement.classList.contains("dark") === dark,
        actual.dark,
      );
      const finalNative = await electronApp.evaluate(({ nativeTheme }, desired) => {
        nativeTheme.themeSource = desired;
        return { themeSource: nativeTheme.themeSource, dark: nativeTheme.shouldUseDarkColors };
      }, value);
      if (finalNative.themeSource !== value || finalNative.dark !== actual.dark)
        throw new Error("Final native theme fixture readback mismatch");
      state.themeFixture = {
        native: finalNative,
        cssMedia: actual.dark ? "dark" : "light",
        kind: "Native theme and CSS media fixture; no user preference persistence claim",
      };
      state.assertions.push({
        name: `Native and rendered theme fixture match ${value}`,
        status: "passed",
      });
      ctx.theme = actual.dark ? "dark" : "light";
      return actual;
    },
    async setWindowSize(width, height) {
      if (
        ![width, height].every((value) => Number.isInteger(value) && value >= 320 && value <= 8192)
      )
        throw new Error("Invalid window dimensions");
      return electronApp.evaluate(
        ({ BrowserWindow }, dimensions) => {
          const win = BrowserWindow.getAllWindows()[0];
          win.setSize(...dimensions);
          return win.getBounds();
        },
        [width, height],
      );
    },
    log(message) {
      if (typeof message !== "string" || /token|bearer|secret|credential|wsTicket/i.test(message))
        throw new Error("Logs must contain nonsecret plain text");
      state.logs.push(message.slice(0, 2000));
    },
    results: () => structuredClone(state),
  };
  return ctx;
}
