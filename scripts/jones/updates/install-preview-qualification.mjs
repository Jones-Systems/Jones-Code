import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import {
  mkdir,
  mkdtemp,
  readFile,
  writeFile,
  readdir,
  lstat,
  realpath,
  rm,
  cp,
} from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { atomicJson, waitFile, redact, processIdentity } from "./install-preview-agent.mjs";

const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const id = () => randomUUID();
const deadline = (signal) =>
  signal ? AbortSignal.any([signal, AbortSignal.timeout(240_000)]) : AbortSignal.timeout(240_000);

export async function withOwnedScratch(parent, use, cleanup = rm) {
  const root = await mkdtemp(path.join(await realpath(parent), "jones-install-preview-"));
  let primary;
  try {
    return await use(root);
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    if (primary?.retainScratch) {
      primary.cleanupError = `Owned scratch retained for unresolved evidence or process cleanup: ${root}`;
    } else
      try {
        await cleanup(root, { recursive: true, force: false });
        await assert.rejects(lstat(root), { code: "ENOENT" });
      } catch (error) {
        if (primary) primary.cleanupError = error.message;
        else {
          error.cleanupError = error.message;
          throw error;
        }
      }
  }
}

export function isolatedEnvironment(root) {
  // An allowlist prevents inherited provider credentials, app homes and diagnostic sinks from entering the fixture.
  return {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    HOME: path.join(root, "home"),
    TMPDIR: path.join(root, "tmp"),
    XDG_CONFIG_HOME: path.join(root, "home/config"),
    XDG_CACHE_HOME: path.join(root, "home/cache"),
    XDG_DATA_HOME: path.join(root, "home/data"),
    LANG: "C.UTF-8",
    CI: "true",
    NO_COLOR: "1",
    T3CODE_SERVER_BROWSER_SANDBOX: "1",
    T3CODE_DISABLE_TELEMETRY: "1",
  };
}

export async function hashTree(root) {
  const output = {};
  async function visit(directory, prefix) {
    for (const entry of (await readdir(directory, { withFileTypes: true })).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      const file = path.join(directory, entry.name);
      assert.ok(!entry.isSymbolicLink(), `Build output contains a symlink: ${relative}`);
      if (entry.isDirectory()) await visit(file, relative);
      else if (entry.isFile()) output[relative] = digest(await readFile(file));
      else throw new Error(`Unexpected build object: ${relative}`);
    }
  }
  await visit(root, "");
  return output;
}

export async function recordBuild(source, out, rendererDir) {
  const git = (...args) =>
    execFileSync("git", ["-C", source, ...args], { encoding: "utf8" }).trim();
  const sourceSha = git("rev-parse", "HEAD");
  const sourceTree = git("rev-parse", "HEAD^{tree}");
  const patch = git("diff", "HEAD", "--", ".");
  const rendererManifest = rendererDir
    ? await readRendererManifest(rendererDir, sourceSha, sourceTree)
    : undefined;
  const receipt = {
    schema: "jones-install-preview-build/v1",
    sourceSha,
    sourceTree,
    trackedPatchSha256: digest(patch),
    cleanTrackedSource: patch.length === 0,
    server: await hashTree(path.join(source, "apps/server/dist")),
    web: await hashTree(path.join(source, "apps/web/dist")),
    ...(rendererDir ? { renderer: await hashTree(rendererDir), rendererManifest } : {}),
  };
  await atomicJson(out, receipt);
  return receipt;
}

export async function readRendererManifest(directory, sourceSha, sourceTree) {
  const manifest = JSON.parse(
    await readFile(path.join(directory, "jones-install-fixture-manifest.json"), "utf8"),
  );
  assert.equal(manifest.schema, "jones-install-renderer-fixture/v1");
  assert.equal(manifest.sourceSha, sourceSha, "Fixture must bind the candidate source revision");
  assert.equal(manifest.sourceTree, sourceTree, "Fixture must bind the candidate source tree");
  assert.equal(manifest.entry, "jones-install-fixture.html");
  assert.equal(
    manifest.boundary,
    "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
  );
  return manifest;
}

export async function openRpc(url, signal) {
  const socket = new WebSocket(url);
  try {
    await once(socket, "open", { signal });
  } catch (error) {
    socket.close();
    throw error;
  }
  let nextId = 0;
  const pending = new Map();
  const send = (message) => socket.send(JSON.stringify(message));
  const closePending = (error) => {
    for (const entry of pending.values()) entry.reject(error);
    pending.clear();
  };
  const cancel = () => {
    closePending(signal.reason);
    socket.close();
  };
  signal?.addEventListener("abort", cancel, { once: true });
  socket.addEventListener("close", () => {
    signal?.removeEventListener("abort", cancel);
    closePending(new Error("Native RPC socket closed"));
  });
  socket.addEventListener("error", () => closePending(new Error("Native RPC socket error")));
  socket.addEventListener("message", ({ data }) => {
    try {
      const parsed = JSON.parse(String(data));
      for (const frame of Array.isArray(parsed) ? parsed : [parsed]) {
        const entry = pending.get(String(frame.requestId));
        if (!entry) continue;
        if (frame._tag === "Chunk") {
          for (const item of frame.values) entry.onItem?.(item);
          send({ _tag: "Ack", requestId: frame.requestId });
        } else if (frame._tag === "Exit") {
          pending.delete(String(frame.requestId));
          if (frame.exit._tag === "Success") entry.resolve(frame.exit.value);
          else
            entry.reject(
              new Error(`Native RPC ${entry.tag} failed: ${JSON.stringify(frame.exit.cause)}`),
            );
        }
      }
    } catch (error) {
      closePending(error);
    }
  });
  return {
    call(tag, payload, onItem) {
      signal?.throwIfAborted();
      const requestId = String(++nextId);
      let timer;
      const result = new Promise((resolve, reject) => {
        timer = setTimeout(() => {
          pending.delete(requestId);
          send({ _tag: "Interrupt", requestId });
          reject(new Error(`Native RPC timed out: ${tag}`));
        }, 90_000);
        pending.set(requestId, { tag, resolve, reject, onItem });
        send({ _tag: "Request", id: requestId, tag, payload, headers: [] });
      }).finally(() => clearTimeout(timer));
      return { result, interrupt: () => send({ _tag: "Interrupt", requestId }) };
    },
    async close() {
      closePending(new Error("Qualification closed RPC"));
      socket.close();
    },
  };
}

async function unusedPort() {
  const server = createNetServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = server.address().port;
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

async function startServer(source, root, label, processes, signal) {
  const owned = path.join(root, label);
  await mkdir(owned);
  const baseDir = path.join(owned, "t3");
  const env = isolatedEnvironment(owned);
  await Promise.all([mkdir(env.HOME), mkdir(env.TMPDIR)]);
  const port = await unusedPort();
  const child = spawn(
    process.execPath,
    [
      path.join(source, "apps/server/dist/bin.mjs"),
      "serve",
      "--base-dir",
      baseDir,
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
    ],
    { cwd: root, env, stdio: ["ignore", "pipe", "pipe"] },
  );
  processes.push(child);
  let output = "";
  let rejectStartup;
  const startup = new Promise((resolve, reject) => {
    rejectStartup = reject;
    const collect = (chunk) => {
      output = (output + chunk.toString()).slice(-256_000);
      const match = output.match(
        /Pairing URL:\s*(http:\/\/127\.0\.0\.1:\d+\/pair#token=[^\s\x1b]+)/,
      );
      if (match) resolve(match[1]);
    };
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.once("error", reject);
    child.once("exit", (code) =>
      reject(new Error(`Owned ${label} server exited (${code}): ${redact(output)}`)),
    );
  });
  startup.catch(() => {});
  child.ownedIdentity = await processIdentity(child.pid);
  const abortStartup = () => rejectStartup(signal.reason);
  signal?.addEventListener("abort", abortStartup, { once: true });
  const timer = setTimeout(
    () => rejectStartup(new Error(`Owned ${label} server startup timed out: ${redact(output)}`)),
    90_000,
  );
  let pairingUrl;
  try {
    pairingUrl = await startup;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abortStartup);
  }
  const origin = `http://127.0.0.1:${port}`;
  const credential = new URL(pairingUrl).hash.slice("#token=".length);
  const response = await fetch(`${origin}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential }),
    signal: deadline(signal),
  });
  assert.equal(response.status, 200, "Ordinary pairing must establish the fixture driver session");
  const cookies = response.headers
    .getSetCookie()
    .map((cookie) => cookie.split(";")[0])
    .join("; ");
  assert.ok(cookies);
  const api = async (pathname, payload) => {
    const result = await fetch(`${origin}${pathname}`, {
      method: payload === undefined ? "GET" : "POST",
      headers: { cookie: cookies, "content-type": "application/json" },
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
      signal: deadline(signal),
    });
    assert.ok(result.ok, `Fixture API ${pathname} returned ${result.status}`);
    return result.json();
  };
  const descriptor = await (
    await fetch(`${origin}/.well-known/t3/environment`, { signal: deadline(signal) })
  ).json();
  assert.equal(typeof descriptor.orchestrationProtocolVersion, "number");
  const ticket = await api("/api/auth/websocket-ticket", {});
  const rpc = await openRpc(
    `${origin.replace("http:", "ws:")}/ws?orchestrationProtocol=${descriptor.orchestrationProtocolVersion}&wsTicket=${encodeURIComponent(ticket.ticket)}`,
    signal,
  );
  return {
    baseDir,
    origin,
    api,
    rpc,
    child,
    output: () => redact(output, [credential, cookies]),
    async pairing() {
      const issued = await api("/api/auth/pairing-token", { label: "Owned synthetic Preview tab" });
      return `${origin}/pair#token=${encodeURIComponent(issued.credential)}`;
    },
  };
}

export async function stopChild(child) {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  const timer = setTimeout(() => {
    if (child.exitCode === null && child.signalCode === null) {
      child.forcedTermination = true;
      child.kill("SIGKILL");
    }
  }, 15_000);
  try {
    await exited;
  } finally {
    clearTimeout(timer);
  }
}

async function stopOwnedDescendant(identity) {
  const same = async () => (await processIdentity(identity.pid))?.startTime === identity.startTime;
  for (const signal of ["SIGTERM", "SIGKILL"]) {
    if (!(await same())) return;
    try {
      process.kill(identity.pid, signal);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (!(await same())) return;
      await delay(100);
    }
  }
  assert.ok(!(await same()), `Owned process ${identity.pid} was not reaped`);
}

async function preserveCaptures(result, baseDir, out, mode) {
  for (const capture of result.captures ?? []) {
    const resolved = await realpath(capture.snapshot.screenshotPath);
    const artifactRoot = await realpath(path.join(baseDir, "userdata/browser-artifacts"));
    assert.ok(resolved.startsWith(`${artifactRoot}${path.sep}`));
    assert.match(capture.name, /^[a-z0-9-]+$/);
    const target = `${mode}-${capture.name}.png`;
    await cp(resolved, path.join(out, target), { errorOnExist: true, force: false });
    capture.snapshot.screenshotPath = target;
  }
}

export async function fixtureProxy(upstream, sourceIdentity) {
  let checks = 0;
  let websocketAttempts = 0;
  const requests = [];
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://fixture.invalid");
      if (request.method === "OPTIONS") {
        response.writeHead(204, {
          "access-control-allow-origin": request.headers.origin ?? "*",
          "access-control-allow-credentials": "true",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-allow-headers": "authorization,content-type,dpop",
        });
        response.end();
        return;
      }

      const headers = { ...request.headers };
      delete headers.host;
      const body = [];
      for await (const part of request) body.push(part);
      const init = {
        method: request.method,
        headers,
        ...(body.length ? { body: Buffer.concat(body) } : {}),
        signal: deadline(),
      };
      if (url.pathname.startsWith("/api/jones-updates")) {
        const session = await fetch(`${upstream.origin}/api/auth/session`, {
          headers,
          signal: deadline(),
        });
        const auth = await session.json();
        if (!auth.authenticated) {
          response.writeHead(401);
          response.end();
          return;
        }
        const requiredScope =
          request.method === "GET" ? "orchestration:read" : "orchestration:operate";
        if (!auth.scopes?.includes(requiredScope)) {
          response.writeHead(403);
          response.end();
          return;
        }
        requests.push({ method: request.method, path: url.pathname, authenticated: true });
        if (url.pathname === "/api/jones-updates/check" && request.method === "POST") checks += 1;
        else if (url.pathname !== "/api/jones-updates" || request.method !== "GET") {
          response.writeHead(409, { "content-type": "application/json" });
          response.end(
            JSON.stringify({ message: "This synthetic host never installs or downloads" }),
          );
          return;
        }
        response.writeHead(200, {
          "content-type": "application/json",
          "access-control-allow-origin": request.headers.origin ?? "*",
          "access-control-allow-credentials": "true",
        });
        response.end(
          JSON.stringify({
            source: "jones-actions",
            channel: "jones-main",
            phase: "blocked",
            revision: checks,
            capability: {
              check: true,
              download: false,
              install: false,
              reason: "bootstrap-required",
            },
            message: checks
              ? "Synthetic authenticated check completed."
              : "Synthetic host needs qualified launcher setup.",
          }),
        );
        return;
      }
      assert.ok(
        url.pathname === "/.well-known/t3/environment" ||
          url.pathname.startsWith("/api/auth/") ||
          url.pathname === "/oauth/token",
        "Unexpected fixture endpoint",
      );
      const result = await fetch(`${upstream.origin}${url.pathname}${url.search}`, init);
      let bytes = Buffer.from(await result.arrayBuffer());
      if (url.pathname === "/.well-known/t3/environment") {
        const descriptor = JSON.parse(bytes);
        descriptor.label = "Synthetic incompatible Jones host";
        descriptor.serverVersion = "0.0.45-preview.20261011.1.1";
        descriptor.orchestrationProtocolVersion = 0;
        descriptor.jonesSource = { ...sourceIdentity, ...(checks ? { sha: "0".repeat(40) } : {}) };
        bytes = Buffer.from(JSON.stringify(descriptor));
      }
      const forwarded = Object.fromEntries(result.headers);
      delete forwarded["content-length"];
      delete forwarded["content-encoding"];
      forwarded["access-control-allow-origin"] = request.headers.origin ?? "*";
      forwarded["access-control-allow-credentials"] = "true";
      response.writeHead(result.status, forwarded);
      response.end(bytes);
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: redact(error.message) }));
    }
  });
  server.on("upgrade", (_request, socket) => {
    websocketAttempts += 1;
    socket.destroy();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    evidence: () => ({
      requests,
      websocketAttempts,
      checks,
      fixtureBoundary:
        "Real T3 pairing/auth; synthetic descriptor and update responses; no installation",
    }),
    close: () =>
      new Promise((resolve, reject) => {
        server.closeAllConnections();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export async function serveRendererFixture(directory) {
  const root = await realpath(directory);
  const requests = [];
  const types = {
    ".html": "text/html",
    ".js": "text/javascript",
    ".css": "text/css",
    ".json": "application/json",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".woff2": "font/woff2",
    ".wasm": "application/wasm",
  };
  const server = createServer(async (request, response) => {
    try {
      assert.ok(request.method === "GET" || request.method === "HEAD");
      const pathname = decodeURIComponent(new URL(request.url, "http://fixture.invalid").pathname);
      const file = await realpath(path.resolve(root, `.${pathname}`));
      assert.ok(file.startsWith(`${root}${path.sep}`));
      const bytes = await readFile(file);
      requests.push({ path: path.relative(root, file), sha256: digest(bytes) });
      response.writeHead(200, {
        "content-type": types[path.extname(file)] ?? "application/octet-stream",
        "cache-control": "no-store",
      });
      response.end(request.method === "HEAD" ? undefined : bytes);
    } catch {
      response.writeHead(404);
      response.end("Owned fixture file unavailable");
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    evidence: () => ({ requests }),
    close: () =>
      new Promise((resolve, reject) => {
        server.closeAllConnections();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

function scenarioSteps(remotePairing) {
  const disabledPreference = {
    action: "assert-evaluate",
    label: "Update access must preserve the saved disabled host",
    expression: `(() => { const toggle = document.querySelector('[role="switch"][aria-label="Switch on Synthetic incompatible Jones host"]'); return { present: toggle !== null, checked: toggle?.getAttribute("aria-checked"), disabled: toggle?.disabled }; })()`,
    expected: { present: true, checked: "false", disabled: true },
  };
  return [
    { action: "navigate", path: "/settings/connections" },
    { action: "wait", text: "Add environment" },
    { action: "capture", name: "connections-before", contains: ["Add environment"] },
    { action: "click", locator: 'role=button[name="Add environment"]' },
    { action: "type", locator: 'role=textbox[name="Host"]', text: remotePairing },
    { action: "click", locator: 'role=dialog >> role=button[name="Add environment"]' },
    { action: "wait", text: "Source matches this app." },
    {
      action: "capture",
      name: "same-source-independent-preview-build",
      contains: ["Source matches this app.", "Synthetic host needs qualified launcher setup."],
      excludes: ["A newer Jones source is available."],
    },
    disabledPreference,
    { action: "click", locator: 'button:enabled:text-is("Check for builds")' },
    { action: "wait", text: "Synthetic authenticated check completed." },
    {
      action: "capture",
      name: "authenticated-update-only-check",
      contains: ["Synthetic authenticated check completed."],
    },
    disabledPreference,
    { action: "click", locator: 'role=button[name="Refresh status"]' },
    { action: "wait", text: "Source relationship has not been verified." },
    {
      action: "capture",
      name: "unknown-source-does-not-invent-order",
      contains: ["Source relationship has not been verified."],
      excludes: ["A newer Jones source is available."],
    },
    { action: "navigate", path: "/settings/connections" },
    { action: "wait", text: "Source relationship has not been verified." },
    {
      action: "capture",
      name: "saved-connection-reload",
      contains: ["Synthetic incompatible Jones host", "Source relationship has not been verified."],
    },
    disabledPreference,
  ];
}

function rendererSteps(origin, sourceSha, runId) {
  const steps = [];
  const click = (locator) => steps.push({ action: "click", locator });
  const control = (name) => click(`[data-testid="${name}"]`);
  const settings = (name) =>
    click(`[data-testid="settings-updates"] >> role=button[name="${name}"]`);
  const sidebar = () =>
    click(
      '[data-testid="sidebar-updates"] button[aria-label]:not([aria-label="Nightly update release notes"])',
    );
  const wait = (phase) => steps.push({ action: "wait", text: `Production atom: ${phase}` });
  const check = (label, expression, expected = true) =>
    steps.push({
      action: "assert-evaluate",
      label,
      expression: `(() => { const s = window.__jonesInstallPreview.snapshot(); return (${expression}); })()`,
      expected,
    });
  const capture = (name, contains, excludes = []) =>
    steps.push({ action: "capture", name, contains, excludes });
  const begin = (scenario, suffix = scenario) => {
    steps.push({
      action: "navigate",
      path: `${origin}/jones-install-fixture.html?run=${runId}-${suffix}&scenario=${scenario}`,
    });
    steps.push({ action: "wait", text: "Jones install renderer fixture" });
  };
  const confirm = () => click('role=alertdialog >> role=button[name="Confirm"]');
  const outcomeTooltip = (text) => {
    steps.push({
      action: "hover",
      locator: '[data-testid="settings-updates"] >> role=button[name="Up to Date"]',
    });
    steps.push({ action: "wait", text });
  };
  const identity = () =>
    check(
      "Pre-bootstrap fixture and production atom bind the exact source",
      `s.mounted && s.sourceSha === ${JSON.stringify(sourceSha)} && s.listenerCount > 0`,
    );
  const pendingDisabled = () =>
    check(
      "Both production controls refuse duplicate pending installation",
      `document.querySelector('[data-testid="settings-updates"] button').disabled === true && document.querySelector('[data-testid="sidebar-updates"] button[aria-label]').getAttribute('aria-disabled') === 'true'`,
    );
  begin("available");
  wait("available");
  identity();
  capture("desktop-available-controls", ["Jones main", "Download"]);
  control("next-download-reject");
  settings("Download");
  wait("error");
  capture("desktop-download-rejected", [
    "Could not download update",
    "Synthetic download transport failure.",
  ]);
  control("next-download-refuse");
  sidebar();
  wait("error");
  steps.push({ action: "wait", text: "Synthetic download refusal." });
  capture("desktop-download-refused", ["Synthetic download refusal."]);
  settings("Download");
  wait("downloading");
  check(
    "Download requests bind the displayed source and artifact",
    `s.calls.filter(c => c.action === 'downloadUpdate').length === 3 && s.calls.filter(c => c.action === 'downloadUpdate').every(c => c.input.artifactId === 9001 && c.input.sourceSha === 'a'.repeat(40))`,
  );
  check(
    "Accepted asynchronous download remains incomplete",
    `s.calls.filter(c => c.action === 'download-result').at(-1).input.completed === false && s.observed.jones.phase === 'downloading'`,
  );
  capture("desktop-download-pending", ["Production atom: downloading"], ["Update downloaded"]);
  control("complete-download");
  wait("staged");
  capture("desktop-staged-selection", ["Install", "Discard download"]);
  settings("Install");
  steps.push({ action: "wait", text: "Confirm" });
  capture("desktop-install-confirmation", ["Cancel", "Confirm"]);
  click('role=alertdialog >> role=button[name="Cancel"]');
  check(
    "Cancelled confirmation never dispatches installation",
    `s.calls.filter(c => c.action === 'installUpdate').length === 0`,
  );
  control("next-install-refuse");
  settings("Install");
  confirm();
  wait("error");
  capture("desktop-install-refused", ["Synthetic install refusal."]);
  control("next-install-reject");
  sidebar();
  confirm();
  steps.push({ action: "wait", text: "Synthetic install transport failure." });
  capture("desktop-install-rejected", ["Synthetic install transport failure."]);
  settings("Install");
  confirm();
  wait("preparing");
  pendingDisabled();
  check(
    "Installation binds one staged handle without a fleet campaign",
    `s.calls.filter(c => c.action === 'installUpdate').length === 3 && s.calls.filter(c => c.action === 'installUpdate').every(c => c.input.stagedHandle === 'c'.repeat(64) && c.input.campaignId === null) && s.calls.filter(c => c.action === 'install-result').at(-1).input.completed === false`,
  );
  capture(
    "desktop-install-pending",
    ["Synthetic installation accepted; waiting for restart outcome."],
    ["committed:"],
  );
  control("commit-install");
  wait("committed");
  outcomeTooltip("committed:");
  capture("desktop-committed", [
    "committed:",
    "0.0.45-preview.20261011.1.1",
    "0.0.45-preview.20261011.2.1",
  ]);
  control("reload-fixture");
  wait("committed");
  identity();
  outcomeTooltip("committed:");
  check(
    "Committed outcome survives renderer reload",
    `s.boots === 2 && s.observed.jones.updateId === 'd'.repeat(64) && s.observed.jones.installedSource.sha === 'a'.repeat(40)`,
  );
  capture("desktop-committed-reload", ["committed:", "boot 2"]);
  begin("staged", "rollback");
  wait("staged");
  identity();
  sidebar();
  confirm();
  wait("preparing");
  control("rollback-install");
  wait("rolled-back");
  outcomeTooltip("rolled back:");
  capture("desktop-rolled-back", ["rolled back:", "remains active"]);
  control("reload-fixture");
  wait("rolled-back");
  outcomeTooltip("rolled back:");
  check(
    "Rollback keeps the old version after reload",
    `s.boots === 2 && s.observed.currentVersion === '0.0.45-preview.20261011.1.1' && s.observed.jones.installedSource.sha === 'b'.repeat(40)`,
  );
  capture("desktop-rollback-reload", ["rolled back:", "boot 2"]);
  begin("staged", "discard");
  wait("staged");
  settings("Discard download");
  wait("available");
  check(
    "Discard binds only the displayed staged selection",
    `s.calls.filter(c => c.action === 'discardUpdate').length === 1 && s.calls.find(c => c.action === 'discardUpdate').input.stagedHandle === 'c'.repeat(64) && !s.observed.jones.stagedHandle`,
  );
  capture("desktop-discarded", ["Production atom: available", "Download"], ["Discard download"]);
  begin("bootstrap-required");
  wait("blocked");
  identity();
  pendingDisabled();
  check(
    "Unqualified installation never dispatches",
    `s.calls.every(c => c.action !== 'installUpdate')`,
  );
  capture("desktop-bootstrap-required", ["Synthetic host needs qualified launcher setup."]);
  begin("initial-race");
  check("Initial state read is held before the newer event", `s.initialReadPending`);
  control("publish-race-event");
  wait("staged");
  control("release-initial-read");
  check(
    "Late initial read does not overwrite newer production state",
    `!s.initialReadPending && s.observed.jones.phase === 'staged' && s.observed.jones.revision === 2`,
  );
  capture("desktop-initial-read-race", ["Production atom: staged; revision 2", "Install"]);
  return steps;
}

async function waitInactive(rpc, threadId) {
  let resolveInactive, rejectInactive;
  const inactive = new Promise((resolve, reject) => {
    resolveInactive = resolve;
    rejectInactive = reject;
  });
  let examining = false;
  let changed = false;
  const inspect = async () => {
    if (examining) {
      changed = true;
      return;
    }
    examining = true;
    try {
      const projection = await rpc.call("orchestration.getThreadProjection", { threadId }).result;
      const actual = projection.projection ?? projection;
      if (actual.thread.activeRunId === null) resolveInactive(actual);
    } catch (error) {
      rejectInactive(error);
    } finally {
      examining = false;
      if (changed) {
        changed = false;
        void inspect();
      }
    }
  };
  const subscription = rpc.call(
    "orchestration.subscribeThread",
    { threadId },
    () => void inspect(),
  );
  subscription.result.catch(rejectInactive);
  try {
    await inspect();
    return await inactive;
  } finally {
    subscription.interrupt();
  }
}

export async function qualify({ source, buildReceipt, out, scratchParent, rendererDir, signal }) {
  assert.equal(process.platform, "linux", "This qualification runs on an isolated Linux CI host");
  await mkdir(out, { recursive: false });
  const expected = JSON.parse(await readFile(buildReceipt, "utf8"));
  const nativeGit = (...args) =>
    execFileSync("git", ["-C", source, ...args], { encoding: "utf8" }).trim();
  assert.equal(expected.sourceSha, nativeGit("rev-parse", "HEAD"));
  assert.equal(expected.sourceTree, nativeGit("rev-parse", "HEAD^{tree}"));
  assert.equal(expected.trackedPatchSha256, digest(nativeGit("diff", "HEAD", "--", ".")));
  assert.deepEqual(await hashTree(path.join(source, "apps/server/dist")), expected.server);
  assert.deepEqual(await hashTree(path.join(source, "apps/web/dist")), expected.web);
  assert.equal(
    Boolean(rendererDir),
    Boolean(expected.renderer),
    "Renderer fixture must be bound into the build receipt",
  );
  if (rendererDir) {
    assert.deepEqual(await hashTree(rendererDir), expected.renderer);
    assert.deepEqual(
      await readRendererManifest(rendererDir, expected.sourceSha, expected.sourceTree),
      expected.rendererManifest,
    );
  }
  const evidence = {
    schema: "jones-install-preview-qualification/v1",
    build: expected,
    outcome: "running",
    scenarios: [],
    boundaries: [
      "Shared web renderer through native T3 Preview",
      "Synthetic remote descriptor/update responses; real authentication",
      ...(rendererDir ? [expected.rendererManifest.boundary] : []),
      "No desktop preload/IPC, packaged Mac activation, live deployment or fleet rollout proof",
    ],
  };
  let scratchCreated = false;
  try {
    await withOwnedScratch(scratchParent, async (root) => {
      scratchCreated = true;
      const processes = [];
      const servers = [];
      const jobs = [];
      let proxy;
      let renderer;
      let primaryError;
      try {
        const primary = await startServer(source, root, "primary", processes, signal);
        servers.push(primary);
        const remote = await startServer(source, root, "remote", processes, signal);
        servers.push(remote);
        const descriptor = await (
          await fetch(`${primary.origin}/.well-known/t3/environment`, { signal: deadline(signal) })
        ).json();
        assert.ok(
          descriptor.serverVersion.includes("-preview."),
          "Build a matching Jones preview version before source stamping",
        );
        assert.deepEqual(
          descriptor.jonesSource,
          {
            repository: "Jones-Systems/Jones-Code",
            sha: expected.sourceSha,
            tree: expected.sourceTree,
          },
          "Build must carry the exact Jones source stamp",
        );
        const servedIndex = Buffer.from(
          await (await fetch(primary.origin, { signal: deadline(signal) })).arrayBuffer(),
        );
        assert.equal(
          digest(servedIndex),
          expected.web["index.html"],
          "Served index must match the qualified web build",
        );
        evidence.servedIndexSha256 = digest(servedIndex);
        proxy = await fixtureProxy(remote, descriptor.jonesSource);
        if (rendererDir) renderer = await serveRendererFixture(rendererDir);
        const remotePairing = (await remote.pairing()).replace(remote.origin, proxy.origin);
        const projectId = id();
        const workspace = path.join(root, "synthetic-project");
        await mkdir(workspace);
        await primary.api("/api/projects/mutate", {
          type: "project.create",
          commandId: id(),
          projectId,
          title: "Install Preview Qualification",
          workspaceRoot: workspace,
        });
        for (const mode of ["active", "browser-disabled"]) {
          const reportFile = path.join(root, `${mode}.json`);
          const releaseFile = path.join(root, `${mode}.release.json`);
          const instanceId = `preview-${mode}`;
          await primary.rpc.call("server.updateSettings", {
            patch: { enableAgentBrowserAccess: mode === "active" },
            providerInstanceMutation: {
              operation: "create",
              instanceId,
              instance: {
                driver: "acpRegistry",
                displayName: `Preview ${mode}`,
                enabled: true,
                environment: Object.entries({
                  JONES_PREVIEW_SOURCE: source,
                  JONES_PREVIEW_REPORT: reportFile,
                  JONES_PREVIEW_RELEASE: releaseFile,
                }).map(([name, value]) => ({ name, value, sensitive: false })),
                config: {
                  source: "local",
                  commandPath: process.execPath,
                  commandArgs: [
                    path.join(source, "scripts/jones/updates/install-preview-agent.mjs"),
                  ],
                  customModels: ["preview-verifier"],
                },
              },
            },
          }).result;
          const threadId = id();
          jobs.push({ mode, reportFile, threadId, baseDir: primary.baseDir });
          const scenario = {
            mode,
            origin: primary.origin,
            pairingUrl: await primary.pairing(),
            initialText: "Install Preview Qualification",
            steps: [
              ...scenarioSteps(remotePairing),
              ...(renderer ? rendererSteps(renderer.origin, expected.sourceSha, id()) : []),
            ],
          };
          await primary.rpc.call("orchestration.launchThread", {
            commandId: id(),
            projectId,
            threadId,
            title: `Preview ${mode}`,
            generateTitle: false,
            modelSelection: { instanceId, model: "preview-verifier" },
            runtimeMode: "full-access",
            interactionMode: "default",
            workspaceStrategy: { type: "root" },
            initialMessage: {
              messageId: id(),
              text: `JONES_PREVIEW_SCENARIO:${Buffer.from(JSON.stringify(scenario)).toString("base64")}`,
              attachments: [],
            },
          }).result;
          const observation = new AbortController();
          let preliminary;
          try {
            const observationSignal = AbortSignal.any([deadline(signal), observation.signal]);
            preliminary = await Promise.race([
              waitFile(reportFile, observationSignal),
              waitFile(`${reportFile}.final`, observationSignal),
            ]);
          } finally {
            observation.abort();
          }
          if (preliminary.outcome === "failed") throw new Error(preliminary.error);
          assert.equal(preliminary.outcome, "actions-complete");
          const inactive = await waitInactive(primary.rpc, threadId);
          assert.equal(inactive.thread.activeRunId, null);
          await atomicJson(releaseFile, { inactive: true });
          const result = await waitFile(`${reportFile}.final`, deadline(signal));
          jobs.at(-1).nativeInactiveReadback = true;
          assert.equal(result.outcome, "passed", result.error);
        }
        evidence.remoteFixture = proxy.evidence();
        assert.equal(
          evidence.remoteFixture.websocketAttempts,
          0,
          "An incompatible host must never receive orchestration traffic",
        );
        assert.ok(
          evidence.remoteFixture.requests.some(
            (request) => request.path === "/api/jones-updates/check" && request.authenticated,
          ),
        );
        if (renderer) {
          const served = renderer.evidence();
          assert.ok(
            served.requests.some((request) => request.path === expected.rendererManifest.entry),
            "The native browser must request the candidate renderer entry",
          );
          assert.ok(
            served.requests.some((request) => request.path.endsWith(".js")),
            "The native browser must request the candidate renderer bundle",
          );
          for (const request of served.requests)
            assert.equal(
              request.sha256,
              expected.renderer[request.path],
              `Served fixture bytes changed: ${request.path}`,
            );
        }
        evidence.outcome = "passed";
      } catch (error) {
        primaryError = error;
        evidence.outcome = "failed";
        evidence.error = redact(error.stack ?? error.message);
      } finally {
        const errors = [];
        const identities = [];
        for (const job of jobs) {
          try {
            const started = JSON.parse(await readFile(`${job.reportFile}.started`, "utf8"));
            identities.push(...[started.provider, started.bridge].filter(Boolean));
          } catch (error) {
            if (error.code !== "ENOENT") errors.push(redact(error.message));
          }
          try {
            let result;
            try {
              result = JSON.parse(await readFile(`${job.reportFile}.final`, "utf8"));
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
              result = JSON.parse(await readFile(job.reportFile, "utf8"));
            }
            evidence.scenarios.push({
              threadId: job.threadId,
              nativeInactiveReadback: job.nativeInactiveReadback === true,
              ...result,
            });
            await preserveCaptures(result, job.baseDir, out, job.mode);
          } catch (error) {
            errors.push(redact(error.message));
          }
        }
        if (proxy) evidence.remoteFixture = proxy.evidence();
        if (renderer)
          evidence.rendererFixture = {
            manifest: expected.rendererManifest,
            ...renderer.evidence(),
          };
        const preserved = await Promise.allSettled([
          ...servers.map((server, index) =>
            writeFile(
              path.join(out, `${index === 0 ? "primary" : "remote"}-server.log`),
              server.output(),
            ),
          ),
          atomicJson(path.join(out, "qualification.json"), evidence),
        ]);
        errors.push(
          ...preserved
            .filter((entry) => entry.status === "rejected")
            .map((entry) => redact(String(entry.reason))),
        );
        const cleanup = await Promise.allSettled([
          ...servers.map((server) => server.rpc.close()),
          ...(proxy ? [proxy.close()] : []),
          ...(renderer ? [renderer.close()] : []),
        ]);
        for (const child of processes.reverse()) {
          try {
            await stopChild(child);
          } catch (error) {
            cleanup.push({ status: "rejected", reason: error });
          }
          if (child.forcedTermination)
            errors.push(
              `Owned server ${child.pid} required forced termination; native browser cleanup is unproven`,
            );
        }
        for (const identity of identities) {
          try {
            await stopOwnedDescendant(identity);
          } catch (error) {
            cleanup.push({ status: "rejected", reason: error });
          }
        }
        errors.push(
          ...cleanup
            .filter((entry) => entry.status === "rejected")
            .map((entry) => redact(String(entry.reason))),
        );
        const remaining = await Promise.all(identities.map(({ pid }) => processIdentity(pid)));
        evidence.cleanup = {
          servers: processes.map((child) => ({
            identity: child.ownedIdentity,
            exitCode: child.exitCode,
            signalCode: child.signalCode,
          })),
          providerAndBridgeIdentities: identities,
          capturedServersReaped: processes.every(
            (child) =>
              child.pid === undefined || child.exitCode !== null || child.signalCode !== null,
          ),
          capturedDescendantsReaped: remaining.every(
            (entry, index) => entry?.startTime !== identities[index].startTime,
          ),
          errors,
        };
        if (
          errors.length ||
          !evidence.cleanup.capturedServersReaped ||
          !evidence.cleanup.capturedDescendantsReaped
        ) {
          evidence.outcome = "failed";
          primaryError ??= new Error("Owned evidence preservation or process cleanup failed");
          primaryError.retainScratch = true;
          evidence.retainedScratch = root;
        }
        try {
          await atomicJson(path.join(out, "qualification.json"), evidence);
        } catch (error) {
          primaryError ??= error;
          primaryError.retainScratch = true;
        }
      }
      if (primaryError) throw primaryError;
    });
  } catch (error) {
    evidence.outcome = "failed";
    evidence.error ??= redact(error.stack ?? error.message);
    if (error.cleanupError) evidence.scratchCleanupError = error.cleanupError;
    throw error;
  } finally {
    evidence.scratchRemoved = scratchCreated && !evidence.scratchCleanupError;
    await atomicJson(path.join(out, "qualification.json"), evidence);
  }
  return evidence;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [command, ...args] = process.argv.slice(2);
  const options = Object.fromEntries(
    Array.from({ length: args.length / 2 }, (_, index) => [
      args[index * 2].replace(/^--/, ""),
      args[index * 2 + 1],
    ]),
  );
  const source = path.resolve(options.source ?? ".");
  const rendererDir = options["fixture-build"] ? path.resolve(options["fixture-build"]) : undefined;
  const cancellation = new AbortController();
  const onSignal = (name) => {
    process.exitCode = name === "SIGINT" ? 130 : 143;
    cancellation.abort(new Error(`Qualification cancelled by ${name}`));
  };
  const onInterrupt = () => onSignal("SIGINT");
  const onTerminate = () => onSignal("SIGTERM");
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onTerminate);
  const run =
    command === "record-build"
      ? recordBuild(source, path.resolve(options.out), rendererDir)
      : command === "run"
        ? qualify({
            source,
            buildReceipt: path.resolve(options.receipt),
            out: path.resolve(options.out),
            scratchParent: path.resolve(options["scratch-parent"]),
            rendererDir,
            signal: cancellation.signal,
          })
        : Promise.reject(
            new Error(
              "Usage: record-build --source DIR --out FILE [--fixture-build DIR] | run --source DIR --receipt FILE --out NEW_DIR --scratch-parent DIR [--fixture-build DIR]",
            ),
          );
  run
    .then((result) =>
      process.stdout.write(`${JSON.stringify({ outcome: result.outcome ?? "recorded" })}\n`),
    )
    .catch((error) => {
      process.stderr.write(`${redact(error.stack ?? error)}\n`);
      process.exitCode ||= 1;
    })
    .finally(() => {
      process.removeListener("SIGINT", onInterrupt);
      process.removeListener("SIGTERM", onTerminate);
    });
}
