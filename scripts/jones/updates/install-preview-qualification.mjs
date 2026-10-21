import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeEvents from "node:events";
import * as NodeTimersPromises from "node:timers/promises";
import {
  atomicJson,
  createMilestoneChannel,
  readAcknowledgedMilestone,
  redact,
  processIdentity,
} from "./install-preview-agent.mjs";

const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const id = () => NodeCrypto.randomUUID();
const deadline = (signal) =>
  signal ? AbortSignal.any([signal, AbortSignal.timeout(240_000)]) : AbortSignal.timeout(240_000);

// oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone native fixture must identify the actual host for Linux process custody and qualification without loading the application runtime.
const nativePlatform = process.platform;

export async function withOwnedScratch(parent, use, cleanup = NodeFSP.rm) {
  const root = await NodeFSP.mkdtemp(
    NodePath.join(await NodeFSP.realpath(parent), "jones-install-preview-"),
  );
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
        await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
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
    HOME: NodePath.join(root, "home"),
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CONFIG_HOME: NodePath.join(root, "home/config"),
    XDG_CACHE_HOME: NodePath.join(root, "home/cache"),
    XDG_DATA_HOME: NodePath.join(root, "home/data"),
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
    for (const entry of (await NodeFSP.readdir(directory, { withFileTypes: true })).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      const file = NodePath.join(directory, entry.name);
      NodeAssert.ok(!entry.isSymbolicLink(), `Build output contains a symlink: ${relative}`);
      if (entry.isDirectory()) await visit(file, relative);
      else if (entry.isFile()) output[relative] = digest(await NodeFSP.readFile(file));
      else throw new Error(`Unexpected build object: ${relative}`);
    }
  }
  await visit(root, "");
  return output;
}

const trackedPatchFile = "install-preview-build.patch";
const trackedPatchFormat = "git-diff-binary-full-index/v1";
const trackedPatchMaxBytes = 16 * 1024 * 1024;
const trackedPatchPath = (receiptPath) =>
  NodePath.join(NodePath.dirname(receiptPath), trackedPatchFile);
const readBinaryTrackedPatch = (source) =>
  NodeChildProcess.execFileSync(
    "git",
    [
      "-C",
      source,
      "diff",
      "--no-ext-diff",
      "--no-textconv",
      "--no-color",
      "--binary",
      "--full-index",
      "HEAD",
      "--",
      ".",
    ],
    { maxBuffer: trackedPatchMaxBytes },
  );

async function readPatchArtifact(file, bytes) {
  const handle = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const stat = await handle.stat();
    NodeAssert.ok(stat.isFile(), "Tracked patch artifact must be a regular file");
    NodeAssert.equal(stat.size, bytes, "Tracked patch artifact size differs");
    const buffer = Buffer.alloc(bytes + 1);
    let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await handle.read(buffer, length, buffer.length - length, null);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    NodeAssert.equal(length, bytes, "Tracked patch artifact changed while reading");
    return buffer.subarray(0, length);
  } finally {
    await handle.close();
  }
}

export async function recordBuild(source, out, rendererDir) {
  const git = (...args) =>
    NodeChildProcess.execFileSync("git", ["-C", source, ...args], { encoding: "utf8" }).trim();
  const sourceSha = git("rev-parse", "HEAD");
  const sourceTree = git("rev-parse", "HEAD^{tree}");
  // Keep the v1 ordinary UTF-8/trim digest distinct from the replayable binary patch.
  const patch = git("diff", "HEAD", "--", ".");
  const binaryPatch = readBinaryTrackedPatch(source);
  const rendererManifest = rendererDir
    ? await readRendererManifest(rendererDir, sourceSha, sourceTree)
    : undefined;
  const receipt = {
    schema: "jones-install-preview-build/v1",
    sourceSha,
    sourceTree,
    trackedPatchSha256: digest(patch),
    trackedPatchArtifact: {
      format: trackedPatchFormat,
      file: trackedPatchFile,
      sha256: digest(binaryPatch),
      bytes: binaryPatch.length,
    },
    cleanTrackedSource: patch.length === 0,
    server: await hashTree(NodePath.join(source, "apps/server/dist")),
    web: await hashTree(NodePath.join(source, "apps/web/dist")),
    ...(rendererDir ? { renderer: await hashTree(rendererDir), rendererManifest } : {}),
  };
  await NodeFSP.writeFile(trackedPatchPath(out), binaryPatch, { flag: "wx", mode: 0o600 });
  await atomicJson(out, receipt);
  return receipt;
}

export async function readRendererManifest(directory, sourceSha, sourceTree) {
  const manifest = JSON.parse(
    await NodeFSP.readFile(NodePath.join(directory, "jones-install-fixture-manifest.json"), "utf8"),
  );
  NodeAssert.equal(manifest.schema, "jones-install-renderer-fixture/v1");
  NodeAssert.equal(
    manifest.sourceSha,
    sourceSha,
    "Fixture must bind the candidate source revision",
  );
  NodeAssert.equal(manifest.sourceTree, sourceTree, "Fixture must bind the candidate source tree");
  NodeAssert.equal(manifest.entry, "jones-install-fixture.html");
  NodeAssert.equal(
    manifest.boundary,
    "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
  );
  return manifest;
}

export async function openRpc(url, signal) {
  const socket = new WebSocket(url);
  try {
    await NodeEvents.once(socket, "open", { signal });
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
          else {
            const error = new Error(
              `Native RPC ${entry.tag} failed: ${JSON.stringify(frame.exit.cause)}`,
            );
            // Effect RPC encodes interruption as a nonempty array of Interrupt reasons.
            error.nativeRpcInterrupted =
              Array.isArray(frame.exit.cause) &&
              frame.exit.cause.length > 0 &&
              frame.exit.cause.every((reason) => reason?._tag === "Interrupt");
            entry.reject(error);
          }
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
      return {
        result,
        interrupt: () => {
          if (pending.has(requestId)) send({ _tag: "Interrupt", requestId });
        },
      };
    },
    async close() {
      closePending(new Error("Qualification closed RPC"));
      socket.close();
    },
  };
}

async function unusedPort() {
  const server = NodeNet.createServer();
  server.listen(0, "127.0.0.1");
  await NodeEvents.once(server, "listening");
  const port = server.address().port;
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

export function waitForFixtureStartup(child, label, signal, timeoutMs = 90_000) {
  let output = "";
  let ready = false;
  let timer;
  let collect, onError, onExit, onAbort;
  const detach = () => {
    child.stdout.off("data", collect);
    child.stderr.off("data", collect);
    child.off("error", onError);
    child.off("exit", onExit);
  };
  return new Promise((resolve, reject) => {
    const fail = (message) => reject(new Error(`Owned ${label} server ${message}`));
    collect = (chunk) => {
      output = (output + chunk.toString()).slice(-256_000);
      if (output.includes("Startup pairing credential issued; credential output withheld.")) {
        ready = true;
        resolve(() => output);
      }
    };
    onError = () => fail("could not start");
    onExit = (code) => {
      detach();
      fail(`exited (${code})`);
    };
    onAbort = () => fail("startup cancelled");
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.once("error", onError);
    child.once("exit", onExit);
    signal?.addEventListener("abort", onAbort, { once: true });
    timer = setTimeout(() => fail("startup timed out"), timeoutMs);
    if (signal?.aborted) onAbort();
  }).finally(() => {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
    if (!ready) detach();
  });
}

export async function issueFixturePairing({
  source,
  owned,
  baseDir,
  processes,
  signal,
  timeoutMs = 30_000,
}) {
  NodeAssert.equal(baseDir, NodePath.join(owned, "t3"), "Pair only the owned fixture base");
  if (signal?.aborted) throw new Error("Owned fixture pairing cancelled");
  // Match startup's administrative grant: this driver must mint the ordinary
  // client links used by Preview tabs. Explicit --scope replaces CLI defaults.
  const scopes = [
    "orchestration:read",
    "orchestration:operate",
    "settings:write",
    "providers:manage",
    "environment:maintain",
    "preview:operate",
    "diagnostics:read",
    "terminal:read",
    "terminal:operate",
    "source-control:write",
    "filesystem:read",
    "filesystem:write",
    "relay:read",
    "access:read",
    "access:write",
    "relay:write",
  ];
  let child;
  let primary;
  try {
    return await new Promise((resolve, reject) => {
      child = NodeChildProcess.execFile(
        process.execPath,
        [
          NodePath.join(source, "apps/server/dist/bin.mjs"),
          "pair",
          "--base-dir",
          baseDir,
          ...scopes.flatMap((scope) => ["--scope", scope]),
        ],
        {
          cwd: owned,
          env: isolatedEnvironment(owned),
          encoding: "utf8",
          timeout: timeoutMs,
          killSignal: "SIGKILL",
          maxBuffer: 256_000,
          signal,
        },
        (error, stdout) => {
          // execFile errors contain captured stdout/stderr. Never propagate
          // those objects or copy credential output into fixture evidence.
          if (error) {
            reject(new Error("Owned fixture pairing command failed"));
            return;
          }
          const tokens = [...stdout.matchAll(/^Token: ([^\s]+)\r?$/gm)];
          if (tokens.length !== 1) {
            reject(new Error("Owned fixture pairing command returned invalid output"));
            return;
          }
          resolve(tokens[0][1]);
        },
      );
      processes.push(child);
    });
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    if (child) {
      try {
        await stopChild(child);
      } catch {
        const failure = primary ?? new Error("Owned fixture pairing cleanup failed");
        failure.retainScratch = true;
        failure.cleanupError = "Owned fixture pairing child cleanup is unresolved";
        if (!primary) throw failure;
      }
    }
  }
}

async function startServer(source, root, label, processes, signal) {
  const owned = NodePath.join(root, label);
  await NodeFSP.mkdir(owned);
  const baseDir = NodePath.join(owned, "t3");
  const env = isolatedEnvironment(owned);
  await Promise.all([NodeFSP.mkdir(env.HOME), NodeFSP.mkdir(env.TMPDIR)]);
  const port = await unusedPort();
  const child = NodeChildProcess.spawn(
    process.execPath,
    [
      NodePath.join(source, "apps/server/dist/bin.mjs"),
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
  const startup = waitForFixtureStartup(child, label, signal);
  startup.catch(() => {});
  child.ownedIdentity = await processIdentity(child.pid);
  const output = await startup;
  const credential = await issueFixturePairing({ source, owned, baseDir, processes, signal });
  const origin = `http://127.0.0.1:${port}`;
  const response = await fetch(`${origin}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential }),
    signal: deadline(signal),
  });
  NodeAssert.equal(
    response.status,
    200,
    "Ordinary pairing must establish the fixture driver session",
  );
  const cookies = response.headers
    .getSetCookie()
    .map((cookie) => cookie.split(";")[0])
    .join("; ");
  NodeAssert.ok(cookies);
  const api = async (pathname, payload) => {
    const result = await fetch(`${origin}${pathname}`, {
      method: payload === undefined ? "GET" : "POST",
      headers: { cookie: cookies, "content-type": "application/json" },
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
      signal: deadline(signal),
    });
    NodeAssert.ok(result.ok, `Fixture API ${pathname} returned ${result.status}`);
    return result.json();
  };
  const descriptor = await (
    await fetch(`${origin}/.well-known/t3/environment`, { signal: deadline(signal) })
  ).json();
  NodeAssert.equal(typeof descriptor.orchestrationProtocolVersion, "number");
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
    output: () => redact(output(), [credential, cookies]),
    async pairing() {
      const issued = await api("/api/auth/pairing-token", { label: "Owned synthetic Preview tab" });
      return `${origin}/pair#token=${encodeURIComponent(issued.credential)}`;
    },
  };
}

export async function stopChild(child) {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const exited = NodeEvents.once(child, "exit");
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
      await NodeTimersPromises.setTimeout(100);
    }
  }
  NodeAssert.ok(!(await same()), `Owned process ${identity.pid} was not reaped`);
}

async function preserveCaptures(result, baseDir, out, mode) {
  for (const capture of result.captures ?? []) {
    const resolved = await NodeFSP.realpath(capture.snapshot.screenshotPath);
    const artifactRoot = await NodeFSP.realpath(
      NodePath.join(baseDir, "userdata/browser-artifacts"),
    );
    NodeAssert.ok(resolved.startsWith(`${artifactRoot}${NodePath.sep}`));
    NodeAssert.match(capture.name, /^[a-z0-9-]+$/);
    const target = `${mode}-${capture.name}.png`;
    await NodeFSP.cp(resolved, NodePath.join(out, target), { errorOnExist: true, force: false });
    capture.snapshot.screenshotPath = target;
  }
}

export async function fixtureProxy(upstream, sourceIdentity) {
  let checks = 0;
  let websocketAttempts = 0;
  const requests = [];
  // Match apps/server/src/httpCors.ts, including Effect HTTP trace propagation headers.
  const allowedHeaders = [
    "authorization",
    "b3",
    "traceparent",
    "content-type",
    "dpop",
    "x-t3-orchestration-protocol",
  ];
  let omittedRequests = 0;
  const server = NodeHttp.createServer(async (request, response) => {
    let receipt;
    try {
      const url = new URL(request.url, "http://fixture.invalid");
      const knownPath = [
        "/.well-known/t3/environment",
        "/api/auth/session",
        "/api/auth/browser-session",
        "/api/auth/pairing-token",
        "/oauth/token",
        "/api/jones-updates",
        "/api/jones-updates/check",
      ].includes(url.pathname);
      receipt = {
        method: ["GET", "POST", "OPTIONS"].includes(request.method) ? request.method : "other",
        path: knownPath ? url.pathname : "other",
        status: null,
      };
      if (requests.length < 128) requests.push(receipt);
      else omittedRequests += 1;
      if (request.method === "OPTIONS") {
        const requested = String(request.headers["access-control-request-headers"] ?? "")
          .toLowerCase()
          .split(",")
          .map((name) => name.trim())
          .filter(Boolean);
        receipt.requestedHeaders = Object.fromEntries(
          allowedHeaders.map((name) => [name, requested.includes(name)]),
        );
        receipt.unsupportedHeaders = requested.some((name) => !allowedHeaders.includes(name));
        receipt.status = 204;
        response.writeHead(204, {
          "access-control-allow-origin": request.headers.origin ?? "*",
          "access-control-allow-credentials": "true",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-allow-headers": allowedHeaders.join(","),
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
        receipt.forwardingStarted = true;
        const session = await fetch(`${upstream.origin}/api/auth/session`, {
          headers,
          signal: deadline(),
        });
        const auth = await session.json();
        if (!auth.authenticated) {
          receipt.status = 401;
          response.writeHead(401);
          response.end();
          return;
        }
        const requiredScope =
          request.method === "GET" ? "orchestration:read" : "orchestration:operate";
        if (!auth.scopes?.includes(requiredScope)) {
          receipt.status = 403;
          response.writeHead(403);
          response.end();
          return;
        }
        receipt.authenticated = true;
        if (url.pathname === "/api/jones-updates/check" && request.method === "POST") checks += 1;
        else if (url.pathname !== "/api/jones-updates" || request.method !== "GET") {
          receipt.status = 409;
          response.writeHead(409, { "content-type": "application/json" });
          response.end(
            JSON.stringify({ message: "This synthetic host never installs or downloads" }),
          );
          return;
        }
        receipt.status = 200;
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
      NodeAssert.ok(
        url.pathname === "/.well-known/t3/environment" ||
          url.pathname.startsWith("/api/auth/") ||
          url.pathname === "/oauth/token",
        "Unexpected fixture endpoint",
      );
      receipt.forwardingStarted = true;
      const result = await fetch(`${upstream.origin}${url.pathname}${url.search}`, init);
      let bytes = Buffer.from(await result.arrayBuffer());
      if (url.pathname === "/.well-known/t3/environment") {
        const descriptor = JSON.parse(bytes);
        descriptor.label = "Synthetic incompatible Jones host";
        descriptor.serverVersion = "0.0.45-preview.20261011.1.1";
        descriptor.orchestrationProtocolVersion = 0;
        // Model an older supervised host; the update response still requires qualified bootstrap.
        descriptor.capabilities = { ...descriptor.capabilities, serverSelfUpdate: "boot-service" };
        descriptor.jonesSource = { ...sourceIdentity, ...(checks ? { sha: "0".repeat(40) } : {}) };
        bytes = Buffer.from(JSON.stringify(descriptor));
      }
      receipt.status = result.status;
      const forwarded = Object.fromEntries(result.headers);
      delete forwarded["content-length"];
      delete forwarded["content-encoding"];
      forwarded["access-control-allow-origin"] = request.headers.origin ?? "*";
      forwarded["access-control-allow-credentials"] = "true";
      response.writeHead(result.status, forwarded);
      response.end(bytes);
    } catch (error) {
      if (receipt) receipt.status = 500;
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: redact(error.message) }));
    }
  });
  server.on("upgrade", (_request, socket) => {
    websocketAttempts += 1;
    socket.destroy();
  });
  server.listen(0, "127.0.0.1");
  await NodeEvents.once(server, "listening");
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    evidence: () => ({
      requests,
      omittedRequests,
      websocketAttempts,
      checks,
      fixtureBoundary:
        "Real T3 pairing/auth; synthetic older boot-service descriptor and bootstrap-blocked update responses; no installation",
    }),
    close: () =>
      new Promise((resolve, reject) => {
        server.closeAllConnections();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export async function serveRendererFixture(directory) {
  const root = await NodeFSP.realpath(directory);
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
  const server = NodeHttp.createServer(async (request, response) => {
    try {
      NodeAssert.ok(request.method === "GET" || request.method === "HEAD");
      const pathname = decodeURIComponent(new URL(request.url, "http://fixture.invalid").pathname);
      const file = await NodeFSP.realpath(NodePath.resolve(root, `.${pathname}`));
      NodeAssert.ok(file.startsWith(`${root}${NodePath.sep}`));
      const bytes = await NodeFSP.readFile(file);
      requests.push({ path: NodePath.relative(root, file), sha256: digest(bytes) });
      response.writeHead(200, {
        "content-type": types[NodePath.extname(file)] ?? "application/octet-stream",
        "cache-control": "no-store",
      });
      response.end(request.method === "HEAD" ? undefined : bytes);
    } catch {
      response.writeHead(404);
      response.end("Owned fixture file unavailable");
    }
  });
  server.listen(0, "127.0.0.1");
  await NodeEvents.once(server, "listening");
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

// Serialized into native preview_evaluate; no application cache hooks or browser substitutes.
export async function indexedDbBrowserProof(phase) {
  const slot = "__jonesLegacyIndexedDbProof";
  const runtimeName = "t3code:connection-runtime";
  const hintName = "t3code:connection-compatibility-hints";
  const require = (condition, message) => {
    if (!condition) throw new Error(message);
  };
  const makeSession = (temporaryName) => {
    const handles = new Set();
    let deletion;
    const state = {
      disposed: false,
      temporaryOwned: false,
      versionChanged: false,
      unexpectedlyClosed: false,
      database: null,
      baseline: null,
      environmentId: null,
      dispose() {
        state.disposed = true;
        for (const database of handles) database.close();
        handles.clear();
        state.database = null;
        state.baseline = null;
        state.environmentId = null;
        window.removeEventListener("pagehide", onPageHide);
        if (window[slot] === state) delete window[slot];
      },
      removeTemporary() {
        if (!state.temporaryOwned) return Promise.resolve();
        if (!deletion)
          deletion = new Promise((resolve, reject) => {
            const request = indexedDB.deleteDatabase(temporaryName);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(new Error("Owned IndexedDB control cleanup failed"));
            request.onblocked = () => reject(new Error("Owned IndexedDB control cleanup blocked"));
          });
        return deletion;
      },
      open(name, version, expectedOldVersion = null) {
        return new Promise((resolve, reject) => {
          const request = indexedDB.open(name, version);
          request.onupgradeneeded = (event) => {
            if (state.disposed || expectedOldVersion !== event.oldVersion) {
              request.transaction.abort();
              return;
            }
            if (event.oldVersion === 0) {
              if (name === temporaryName) state.temporaryOwned = true;
              request.result.createObjectStore("probe");
            }
          };
          request.onerror = () => {
            const error = new Error("Owned IndexedDB open failed");
            error.name = request.error?.name ?? "Error";
            reject(error);
          };
          request.onblocked = () => reject(new Error("Owned IndexedDB open blocked"));
          request.onsuccess = () => {
            const database = request.result;
            // A native tool cancellation can outlive this promise. Never retain a late handle.
            if (state.disposed) {
              database.close();
              reject(new Error("Owned IndexedDB proof ended"));
              return;
            }
            handles.add(database);
            resolve(database);
          };
        });
      },
      read(database, store, all = false) {
        return new Promise((resolve, reject) => {
          const transaction = database.transaction(store, "readonly");
          const objectStore = transaction.objectStore(store);
          const request = all ? objectStore.getAll(undefined, 129) : objectStore.get("document");
          transaction.oncomplete = () => resolve(request.result);
          transaction.onabort = () => reject(new Error("Owned IndexedDB read aborted"));
          transaction.onerror = () => reject(new Error("Owned IndexedDB read failed"));
        });
      },
      hold(database) {
        state.database = database;
        database.onversionchange = () => {
          state.versionChanged = true;
          database.close();
        };
        database.onclose = () => {
          state.unexpectedlyClosed = true;
        };
      },
    };
    const onPageHide = () => {
      state.dispose();
      // The same deletion promise is awaited below on ordinary success/failure.
      if (temporaryName) void state.removeTemporary().catch(() => {});
    };
    window.addEventListener("pagehide", onPageHide);
    return state;
  };
  if (phase === "upgrade-control") {
    const temporaryName = `jones-install-preview-idb-${crypto.randomUUID()}`;
    const state = makeSession(temporaryName);
    let result;
    let failure;
    try {
      require(!(await indexedDB.databases()).some(
        (entry) => entry.name === temporaryName,
      ), "Owned IndexedDB control name already exists");
      state.hold(await state.open(temporaryName, 4, 0));
      require(state.temporaryOwned, "IndexedDB control creation was not owned");
      await state.open(temporaryName, 5, 4);
      let closed = false;
      try {
        state.database.transaction("probe", "readonly");
      } catch (error) {
        closed = error.name === "InvalidStateError";
      }
      let oldVersionRejected = false;
      try {
        await state.open(temporaryName, 4);
      } catch (error) {
        oldVersionRejected = error.name === "VersionError";
      }
      result = {
        versionChangeObserved: state.versionChanged,
        legacyConnectionClosed: closed,
        oldVersionRejected,
      };
    } catch (error) {
      failure = error;
    } finally {
      state.dispose();
      try {
        await state.removeTemporary();
      } catch (cleanupError) {
        if (failure)
          throw new AggregateError([failure, cleanupError], "IndexedDB control and cleanup failed");
        throw cleanupError;
      }
    }
    if (failure) throw failure;
    return {
      ...result,
      temporaryDatabaseRemoved: !(await indexedDB.databases()).some(
        (entry) => entry.name === temporaryName,
      ),
    };
  }
  if (phase === "hold") {
    require(window[slot] === undefined, "Legacy IndexedDB proof already active");
    const state = makeSession();
    window[slot] = state;
    try {
      require((await indexedDB.databases()).some(
        (entry) => entry.name === runtimeName && entry.version === 4,
      ), "Existing connection catalog must remain version 4");
      state.hold(await state.open(runtimeName, 4));
      return { legacyV4Held: state.database.version === 4 };
    } catch (error) {
      state.dispose();
      throw error;
    }
  }
  const state = window[slot];
  require(state && !state.disposed, "Legacy IndexedDB proof is not active");
  try {
    if (phase === "checkpoint") {
      const raw = await state.read(state.database, "catalog");
      let catalog;
      try {
        catalog = JSON.parse(raw);
      } catch {
        throw new Error("Synthetic catalog is not valid JSON");
      }
      require(typeof raw === "string" &&
        catalog.disabledEnvironmentIds?.length ===
          1, "Synthetic catalog must contain exactly one saved Off environment");
      state.baseline = raw;
      state.environmentId = catalog.disabledEnvironmentIds[0];
      return {
        savedOffCatalogCaptured: true,
        legacyConnectionUnchanged: !state.versionChanged && !state.unexpectedlyClosed,
      };
    }
    if (phase === "verify") {
      require(typeof state.baseline === "string", "Synthetic catalog checkpoint is absent");
      const catalogUnchanged = (await state.read(state.database, "catalog")) === state.baseline;
      const reopened = await state.open(runtimeName, 4);
      let reopenedCatalogUnchanged;
      try {
        reopenedCatalogUnchanged = (await state.read(reopened, "catalog")) === state.baseline;
      } finally {
        reopened.close();
      }
      const separateHintDatabase = (await indexedDB.databases()).some(
        (entry) => entry.name === hintName && entry.version === 1,
      );
      let hintRecordPresent = false;
      if (separateHintDatabase) {
        const hints = await state.open(hintName, 1);
        try {
          if (hints.objectStoreNames.contains("compatibility-hints")) {
            const records = await state.read(hints, "compatibility-hints", true);
            hintRecordPresent = records.some(
              (record) =>
                record.environmentId === state.environmentId &&
                record.kind === "outdated-update" &&
                typeof record.routeBinding === "string" &&
                record.routeBinding.length > 0,
            );
          }
        } finally {
          hints.close();
        }
      }
      return {
        legacyConnectionUnchanged: !state.versionChanged && !state.unexpectedlyClosed,
        catalogUnchanged,
        reopenedV4: reopened.version === 4,
        reopenedCatalogUnchanged,
        separateHintDatabase,
        hintRecordPresent,
      };
    }
    require(phase === "close", "Unknown IndexedDB proof phase");
    state.dispose();
    return { legacyProofReleased: window[slot] === undefined };
  } catch (error) {
    state.dispose();
    throw error;
  }
}

function indexedDbStep(phase, label, expected) {
  return {
    action: "assert-evaluate",
    label,
    expression: `(${indexedDbBrowserProof.toString()})(${JSON.stringify(phase)})`,
    expected,
  };
}

export function scenarioSteps(remotePairing) {
  const disabledPreference = {
    action: "assert-evaluate",
    label: "Update access must preserve the saved disabled host",
    // Base UI Switch renders a span; its disabled state is exposed through ARIA.
    expression: `(() => { const toggle = document.querySelector('[role="switch"][aria-label="Switch on Synthetic incompatible Jones host"]'); return { present: toggle !== null, checked: toggle?.getAttribute("aria-checked"), disabled: toggle?.getAttribute("aria-disabled") === "true" }; })()`,
    expected: { present: true, checked: "false", disabled: true },
  };
  return [
    { action: "navigate", path: "/settings/connections" },
    { action: "wait", text: "Add environment" },
    { action: "capture", name: "connections-before", contains: ["Add environment"] },
    indexedDbStep("upgrade-control", "Native IndexedDB detects the legacy upgrade hazard", {
      versionChangeObserved: true,
      legacyConnectionClosed: true,
      oldVersionRejected: true,
      temporaryDatabaseRemoved: true,
    }),
    indexedDbStep("hold", "Hold the actual legacy version-4 catalog connection", {
      legacyV4Held: true,
    }),
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
    indexedDbStep("checkpoint", "Capture the saved Off catalog before the authenticated check", {
      savedOffCatalogCaptured: true,
      legacyConnectionUnchanged: true,
    }),
    { action: "click", locator: 'button:enabled:text-is("Check for builds")' },
    { action: "wait", text: "Synthetic authenticated check completed." },
    {
      action: "capture",
      name: "authenticated-update-only-check",
      contains: ["Synthetic authenticated check completed."],
    },
    disabledPreference,
    indexedDbStep("verify", "New hint persistence preserves the held legacy catalog", {
      legacyConnectionUnchanged: true,
      catalogUnchanged: true,
      reopenedV4: true,
      reopenedCatalogUnchanged: true,
      separateHintDatabase: true,
      hintRecordPresent: true,
    }),
    { action: "click", locator: 'role=button[name="Refresh status"]' },
    { action: "wait", text: "Source relationship has not been verified." },
    {
      action: "capture",
      name: "unknown-source-does-not-invent-order",
      contains: ["Source relationship has not been verified."],
      excludes: ["A newer Jones source is available."],
    },
    indexedDbStep("close", "Release the legacy catalog handle before hard reload", {
      legacyProofReleased: true,
    }),
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

export function rendererSteps(origin, sourceSha, runId) {
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
      `(() => {
        const install = [...document.querySelectorAll('[data-testid="settings-updates"] button')]
          .filter(button => button.textContent?.trim() === 'Install');
        const sidebar = document.querySelectorAll('[data-testid="sidebar-updates"] button[aria-label]:not([aria-label="Nightly update release notes"])');
        return install.length === 1 && install[0].disabled === true &&
          sidebar.length === 1 && sidebar[0].getAttribute('aria-disabled') === 'true';
      })()`,
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

  begin("available", "client-notices");
  wait("available");
  identity();
  control("show-client-notices");
  const noticeCount = (count) => steps.push({ action: "wait", text: `Client notices: ${count}` });
  const noticeText = `document.querySelector('[data-testid="notice-banners"]').textContent`;
  const noticeBuild = (sha, version = "0.0.45-preview.20261011.1.1") => `(() => {
    const labels = [...document.querySelectorAll('[data-testid="notice-banners"] [aria-label]')]
      .filter(node => {
        const detail = node.getAttribute('aria-label') ?? '';
        return detail.split('\\n').includes(${JSON.stringify(version)}) &&
          detail.endsWith(${JSON.stringify(`source ${sha}`)});
      });
    return labels.length === 1 && labels[0].textContent === ${JSON.stringify(sha.slice(0, 7))};
  })()`;
  const noticeTitles = (label, titles) =>
    check(
      label,
      `Array.from(document.querySelectorAll('[data-testid="notice-banners"] [role="alert"]')).map(e => e.querySelector('.font-medium').textContent).sort()`,
      [...titles].sort(),
    );
  const details = () =>
    steps.push({
      action: "hover",
      locator: '[data-testid="notice-banners"] >> role=button[name="Show notice details"]',
    });
  const syntheticSource = (sourceSha.startsWith("a") ? "b" : "a").repeat(39) + "b";
  noticeCount(1);
  check(
    "Warning inputs use explicit synthetic older-client branding and the exact source",
    `document.querySelector('[data-testid="notice-branding"]').textContent === ${JSON.stringify(`Synthetic client 0.0.45-preview.20261011.1.1; exact source ${sourceSha}`)}`,
  );
  noticeTitles("Manual selection shows only Laptop", ["Laptop: Different preview sources"]);
  details();
  steps.push({ action: "wait", text: "Which source is newer is unknown." });
  capture(
    "client-notice-active-laptop",
    ["Laptop: Different preview sources", "Which source is newer is unknown."],
    ["Remote: Different preview sources", "Client update available"],
  );
  control("notice-select-remote");
  steps.push({ action: "wait", text: "Remote: Different preview sources" });
  noticeTitles("Changing active environment selects Remote only", [
    "Remote: Different preview sources",
  ]);
  capture(
    "client-notice-active-remote",
    ["Remote: Different preview sources"],
    ["Laptop: Different preview sources"],
  );
  control("notice-select-excluded");
  steps.push({ action: "wait", text: "Excluded: Different preview sources" });
  control("notice-automatic");
  noticeCount(2);
  noticeTitles("Automatic selection uses eligible environments, not the active excluded one", [
    "Laptop: Different preview sources",
    "Remote: Different preview sources",
  ]);
  steps.push({
    action: "hover",
    locator: '[data-testid="notice-banners"] >> role=button[name="Show other notices"]',
  });
  steps.push({ action: "wait", text: "Remote: Different preview sources" });
  check(
    "Both automatic notices are visibly expanded",
    `Array.from(document.querySelectorAll('[data-testid="notice-banners"] [role="alert"]')).every(e => getComputedStyle(e).visibility === 'visible' && e.getBoundingClientRect().height > 0)`,
  );
  capture(
    "client-notice-automatic-eligible",
    ["Laptop: Different preview sources", "Remote: Different preview sources"],
    ["Excluded: Different preview sources"],
  );
  control("notice-select-laptop");
  noticeCount(1);
  check(
    "Initial preview pair renders the expected full source and abbreviation",
    `document.querySelector('[data-testid="notice-server"]').textContent.endsWith(${JSON.stringify(syntheticSource)}) && ${noticeBuild(syntheticSource)}`,
  );
  click('[data-testid="notice-banners"] >> role=button[name="Dismiss client version notice"]');
  noticeCount(0);
  noticeTitles("Production dismissal removes the actual notice", []);
  control("notice-remount");
  steps.push({ action: "wait", text: "mount: 2" });
  noticeCount(0);
  noticeTitles("Dismissal survives remount of the production hook and stack", []);
  capture(
    "client-notice-dismissed-remount",
    ["Client notices: 0", "mount: 2"],
    ["Laptop: Different preview sources"],
  );
  control("notice-pair-changed");
  noticeCount(1);
  noticeTitles("A changed full SHA restores the dismissed notice", [
    "Laptop: Different preview sources",
  ]);
  check(
    "Changed full source retains the same displayed abbreviation",
    `document.querySelector('[data-testid="notice-server"]').textContent.endsWith(${JSON.stringify(syntheticSource.slice(0, -1) + "c")}) && ${noticeBuild(syntheticSource.slice(0, -1) + "c")}`,
  );
  capture("client-notice-changed-full-source", [
    "Laptop: Different preview sources",
    syntheticSource.slice(0, 7),
  ]);
  control("notice-pair-equal");
  noticeCount(0);
  noticeTitles("Equal preview sources stay quiet", []);
  capture(
    "client-notice-equal-source",
    ["pair: equal", "Client notices: 0"],
    ["Laptop: Different preview sources"],
  );
  control("notice-pair-missing");
  noticeCount(0);
  steps.push({ action: "wait", text: "source missing" });
  noticeTitles("Missing preview source stays quiet", []);
  capture(
    "client-notice-missing-source",
    ["source missing", "Client notices: 0"],
    ["Laptop: Different preview sources"],
  );
  control("notice-pair-stable");
  noticeCount(1);
  noticeTitles("Stable 0.1.0 directs the older client to update", [
    "Laptop: Client update available",
  ]);
  check(
    "Stable guidance retains full version details and update-or-reload instructions",
    `${noticeBuild(syntheticSource, "0.1.0")} && ${noticeText}.includes('Update this app or reload an updated web client.')`,
  );
  details();
  steps.push({ action: "wait", text: "Update this app or reload an updated web client." });
  capture(
    "client-notice-stable-server",
    [
      "Laptop: Client update available",
      "Update this app or reload an updated web client.",
      syntheticSource.slice(0, 7),
    ],
    ["Which source is newer is unknown."],
  );
  check(
    "Client notices never request a server installation or download",
    `s.calls.every(c => c.action !== 'downloadUpdate' && c.action !== 'installUpdate')`,
  );

  // Exact-source action: the production notice and controller against a scripted synthetic host.
  // Targets are always the running app's own source; the desktop bridge is never involved.
  const shortSource = sourceSha.slice(0, 7);
  const banners = '[data-testid="exact-banners"]';
  const exactButton = (name) => `${banners} >> role=button[name="${name}"]`;
  const exactCalls = (select) =>
    `JSON.stringify(JSON.parse(document.querySelector('[data-testid="exact-receipts"]').textContent).map(c => ${select}))`;
  const exactCallsAre = (label, select, expected) =>
    check(label, exactCalls(select), JSON.stringify(expected));
  const noExactButton = (label) =>
    check(
      label,
      `!Array.from(document.querySelectorAll('${banners} button')).some(b => /^(Download|Install) [0-9a-f]{7}$/.test(b.textContent))`,
    );
  const exactState = (text) => steps.push({ action: "wait", text });
  const exactReset = (scenario) => control(`exact-scenario-${scenario}`);
  const noBridge = () =>
    check(
      "The exact-source notice never used the desktop update bridge",
      `s.calls.every(c => c.action !== 'downloadUpdate' && c.action !== 'installUpdate')`,
    );
  // The production controller keeps its action report (and any lost-response hold) while it stays
  // mounted, so each independent case loads a fresh fixture page with its own run id and mounts
  // the exact-source fixture itself; only the scripted host scenario is chosen inside the case.
  const exactCase = (suffix, scenario = "exact") => {
    begin("available", `exact-${suffix}`);
    wait("available");
    control("show-exact-source");
    if (scenario !== "exact") exactReset(scenario);
  };
  exactCase("source");
  exactState("Exact-source notices: 1");
  exactState(`Download ${shortSource}`);
  capture("exact-source-download-offer", [
    "Different preview sources",
    `Download ${shortSource}`,
    "Host: exact",
  ]);
  click(exactButton(`Download ${shortSource}`));
  exactState("Synthetic download is in progress.");
  exactCallsAre(
    "Download selects only the app's own full source, never the host's offered build",
    "[c.action, c.input.targetSource]",
    [["downloadExact", sourceSha]],
  );
  noExactButton("No second request is offered while the host downloads");
  control("exact-complete-download");
  exactState(`Install ${shortSource}`);
  capture("exact-source-staged", [`Install ${shortSource}`, "handle staged-target-handle"]);
  click(exactButton(`Install ${shortSource}`));
  steps.push({ action: "wait", text: "Confirm" });
  capture("exact-source-install-confirmation", [
    "Cancel",
    "Confirm",
    `Install source ${shortSource} on Mac Mini?`,
  ]);
  click('role=alertdialog >> role=button[name="Cancel"]');
  exactCallsAre("Cancelled confirmation never dispatches installation", "c.action", [
    "downloadExact",
  ]);
  click(exactButton(`Install ${shortSource}`));
  confirm();
  // The production presentation replaces the host message while the phase is "installing"
  // (jonesUpdatePresentation), so the fixture's own accepted-install message is never rendered.
  exactState("Installing — server restarting. Waiting for the launcher outcome.");
  noExactButton("No second install is offered while the host installs");
  exactCallsAre(
    "Installation binds the staged handle of the matching source",
    "[c.action, c.input.stagedHandle ?? c.input.targetSource]",
    [
      ["downloadExact", sourceSha],
      ["install", "staged-target-handle"],
    ],
  );
  control("exact-commit-install");
  exactState("Exact-source notices: 0");
  capture("exact-source-committed", ["Exact-source notices: 0"], [`Install ${shortSource}`]);
  noBridge();

  exactCase("lost-install");
  exactState("Exact-source notices: 1");
  click(exactButton(`Download ${shortSource}`));
  control("exact-complete-download");
  exactState(`Install ${shortSource}`);
  control("exact-next-install-lost-not-started");
  click(exactButton(`Install ${shortSource}`));
  confirm();
  exactState("will not be sent again");
  noExactButton("A lost install response is never offered for resubmission");
  capture("exact-source-lost-install", ["will not be sent again"], [`Install ${shortSource}`]);
  control("exact-report-state");
  exactState(`Install ${shortSource}`);
  exactCallsAre("Reconciling never resent the install", "c.action", ["downloadExact", "install"]);
  noBridge();

  exactCase("wrong-source");
  exactState("Exact-source notices: 1");
  control("exact-next-download-wrong-source");
  click(exactButton(`Download ${shortSource}`));
  exactState("will not be installed from this notice");
  noExactButton("A host that stages another source is refused");
  capture("exact-source-wrong-source", ["will not be installed from this notice"]);
  noBridge();

  exactCase("refusal");
  exactState("Exact-source notices: 1");
  control("exact-next-download-refuse");
  click(exactButton(`Download ${shortSource}`));
  exactState("Synthetic refusal: this app's build is not newer than the host's.");
  capture("exact-source-refusal", ["is not newer than the host's"]);
  noBridge();

  exactCase("staged-other", "staged-other");
  exactState("It will not be installed from this notice.");
  noExactButton("Another staged source offers no action");
  capture("exact-source-staged-other", ["already has source", "downloaded"]);
  noBridge();

  exactCase("legacy-mismatch", "legacy-mismatch");
  exactState("cannot download a specific source");
  noExactButton("An older host offering another build offers no action");
  noBridge();

  exactCase("legacy-match", "legacy-match");
  exactState(`Download ${shortSource}`);
  click(exactButton(`Download ${shortSource}`));
  exactState("Synthetic download is in progress.");
  exactCallsAre("An older host downloads only by its own artifact binding", "c.action", [
    "download",
  ]);
  noBridge();

  exactCase("stale", "stale");
  exactState("temporarily unavailable");
  noExactButton("A stale observation offers no action");
  noBridge();

  exactCase("unsupported", "unsupported");
  exactState("does not support Jones updates");
  noBridge();

  exactCase("permission");
  exactState("Exact-source notices: 1");
  control("exact-toggle-permission");
  exactState("cannot change updates");
  noExactButton("A session without operate permission offers no action");
  control("exact-toggle-permission");
  exactState(`Download ${shortSource}`);
  click(`${banners} >> role=button[name="Dismiss client version notice"]`);
  exactState("Exact-source notices: 0");
  noBridge();
  return steps;
}

export async function waitProviderReady(rpc, instanceId) {
  let resolveReady, rejectReady;
  const ready = new Promise((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  let finished = false;
  const subscription = rpc.call("subscribeServerConfig", {}, (item) => {
    if (finished) return;
    try {
      let providers;
      if (item.type === "snapshot") providers = item.config?.providers;
      else if (item.type === "providerStatuses") providers = item.payload?.providers;
      else return;
      NodeAssert.ok(Array.isArray(providers), "Native provider snapshot is missing providers");
      const matches = providers.filter((provider) => provider?.instanceId === instanceId);
      if (matches.length === 0) return;
      NodeAssert.equal(matches.length, 1, "Native provider snapshot contains duplicate instances");
      const provider = matches[0];
      NodeAssert.equal(provider.driver, "acpRegistry", "Native provider has the wrong driver");
      NodeAssert.equal(provider.enabled, true, "Native provider is disabled");
      NodeAssert.ok(
        provider.availability === undefined || provider.availability === "available",
        "Native provider is unavailable",
      );
      resolveReady({ instanceId, driver: provider.driver, enabled: true });
    } catch (error) {
      rejectReady(error);
    }
  });
  const terminal = subscription.result.then(
    () => {
      if (!finished)
        rejectReady(new Error("Native config subscription ended before provider readiness"));
      return { error: null };
    },
    (error) => {
      if (!finished) rejectReady(error);
      return { error };
    },
  );
  let failure;
  try {
    return await ready;
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    finished = true;
    try {
      subscription.interrupt();
      const exit = await terminal;
      if (exit.error && exit.error !== failure && exit.error.nativeRpcInterrupted !== true)
        throw exit.error;
    } catch (cleanupError) {
      if (failure && cleanupError !== failure)
        throw new AggregateError(
          [failure, cleanupError],
          "Native provider readiness and subscription cleanup failed",
        );
      throw cleanupError;
    }
  }
}

export async function waitInactive(rpc, threadId) {
  let resolveInactive, rejectInactive;
  const inactive = new Promise((resolve, reject) => {
    resolveInactive = resolve;
    rejectInactive = reject;
  });
  let finished = false;
  const inspect = (thread) => {
    if (finished || thread?.id !== threadId) return;
    if (
      !Object.hasOwn(thread, "activeRunId") ||
      !(
        thread.activeRunId === null ||
        (typeof thread.activeRunId === "string" && thread.activeRunId.length > 0)
      )
    ) {
      rejectInactive(new Error("Native thread shell is missing a valid activeRunId"));
      return;
    }
    if (thread.activeRunId === null) resolveInactive({ thread });
  };
  const subscription = rpc.call("orchestration.subscribeShell", {}, (item) => {
    if (finished) return;
    if (item.kind === "snapshot") {
      for (const thread of [...item.snapshot.threads, ...item.snapshot.archivedThreads])
        inspect(thread);
    } else if (item.kind === "thread.updated") inspect(item.thread);
  });
  // Attach both handlers immediately; a native failure also settles the inactivity wait.
  const terminal = subscription.result.then(
    () => {
      if (!finished) rejectInactive(new Error("Native shell subscription ended before inactivity"));
      return { error: null };
    },
    (error) => {
      if (!finished) rejectInactive(error);
      return { error };
    },
  );
  let failure;
  try {
    return await inactive;
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    finished = true;
    try {
      subscription.interrupt();
      // Do not leave a pending RPC/timer behind or accept a failed native finalizer.
      const exit = await terminal;
      if (exit.error && exit.error !== failure && exit.error.nativeRpcInterrupted !== true)
        throw exit.error;
    } catch (cleanupError) {
      if (failure && cleanupError !== failure)
        throw new AggregateError(
          [failure, cleanupError],
          "Native inactivity observation and subscription cleanup failed",
        );
      throw cleanupError;
    }
  }
}

export async function readBuildReceipt(source, buildReceipt, rendererDir) {
  const expected = JSON.parse(await NodeFSP.readFile(buildReceipt, "utf8"));
  const nativeGit = (...args) =>
    NodeChildProcess.execFileSync("git", ["-C", source, ...args], { encoding: "utf8" }).trim();
  NodeAssert.equal(expected.sourceSha, nativeGit("rev-parse", "HEAD"));
  NodeAssert.equal(expected.sourceTree, nativeGit("rev-parse", "HEAD^{tree}"));
  NodeAssert.equal(expected.trackedPatchSha256, digest(nativeGit("diff", "HEAD", "--", ".")));
  const artifact = expected.trackedPatchArtifact;
  NodeAssert.equal(
    artifact?.format,
    trackedPatchFormat,
    "Tracked patch format is missing or invalid",
  );
  NodeAssert.equal(artifact.file, trackedPatchFile, "Tracked patch filename differs");
  NodeAssert.match(artifact.sha256, /^[a-f0-9]{64}$/);
  NodeAssert.ok(
    Number.isSafeInteger(artifact.bytes) &&
      artifact.bytes >= 0 &&
      artifact.bytes <= trackedPatchMaxBytes,
    "Tracked patch artifact exceeds its byte bound",
  );
  const binaryPatch = await readPatchArtifact(trackedPatchPath(buildReceipt), artifact.bytes);
  NodeAssert.equal(digest(binaryPatch), artifact.sha256, "Tracked patch artifact digest differs");
  NodeAssert.deepEqual(
    readBinaryTrackedPatch(source),
    binaryPatch,
    "Tracked patch artifact differs from current source",
  );
  NodeAssert.deepEqual(await hashTree(NodePath.join(source, "apps/server/dist")), expected.server);
  NodeAssert.deepEqual(await hashTree(NodePath.join(source, "apps/web/dist")), expected.web);
  NodeAssert.equal(
    Boolean(rendererDir),
    Boolean(expected.renderer),
    "Renderer fixture must be bound into the build receipt",
  );
  if (rendererDir) {
    NodeAssert.deepEqual(await hashTree(rendererDir), expected.renderer);
    NodeAssert.deepEqual(
      await readRendererManifest(rendererDir, expected.sourceSha, expected.sourceTree),
      expected.rendererManifest,
    );
  }
  return expected;
}

export async function qualify({ source, buildReceipt, out, scratchParent, rendererDir, signal }) {
  NodeAssert.equal(nativePlatform, "linux", "This qualification runs on an isolated Linux CI host");
  await NodeFSP.mkdir(out, { recursive: false });
  const expected = await readBuildReceipt(source, buildReceipt, rendererDir);
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
        NodeAssert.ok(
          descriptor.serverVersion.includes("-preview."),
          "Build a matching Jones preview version before source stamping",
        );
        NodeAssert.deepEqual(
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
        NodeAssert.equal(
          digest(servedIndex),
          expected.web["index.html"],
          "Served index must match the qualified web build",
        );
        evidence.servedIndexSha256 = digest(servedIndex);
        proxy = await fixtureProxy(remote, descriptor.jonesSource);
        if (rendererDir) renderer = await serveRendererFixture(rendererDir);
        const remotePairing = (await remote.pairing()).replace(remote.origin, proxy.origin);
        const projectId = id();
        const workspace = NodePath.join(root, "synthetic-project");
        await NodeFSP.mkdir(workspace);
        await primary.api("/api/projects/mutate", {
          type: "project.create",
          commandId: id(),
          projectId,
          title: "Install Preview Qualification",
          workspaceRoot: workspace,
        });
        for (const mode of ["active", "browser-disabled"]) {
          const reportFile = NodePath.join(root, `${mode}.json`);
          const releaseFile = NodePath.join(root, `${mode}.release.json`);
          const instanceId = `preview-${mode}`;
          const threadId = id();
          const files = {
            started: `${reportFile}.started`,
            custody: `${reportFile}.custody`,
            report: reportFile,
            final: `${reportFile}.final`,
            release: releaseFile,
          };
          const channel = await createMilestoneChannel({
            runId: threadId,
            caseId: mode,
            files,
            signal: deadline(signal),
          });
          jobs.push({ mode, reportFile, threadId, baseDir: primary.baseDir, channel });
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
                  JONES_PREVIEW_MILESTONE: JSON.stringify(channel.binding),
                }).map(([name, value]) => ({
                  name,
                  value,
                  sensitive: name === "JONES_PREVIEW_MILESTONE",
                })),
                config: {
                  source: "local",
                  commandPath: process.execPath,
                  commandArgs: [
                    NodePath.join(source, "scripts/jones/updates/install-preview-agent.mjs"),
                  ],
                  customModels: ["preview-verifier"],
                },
              },
            },
          }).result;
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
          // Settings persistence acknowledges before the provider registry finishes hydration.
          await waitProviderReady(primary.rpc, instanceId);
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
          const acknowledged = await channel.wait(["report", "final"], deadline(signal));
          const preliminary = await readAcknowledgedMilestone(
            files[acknowledged.name],
            acknowledged,
          );
          if (preliminary.outcome === "failed") throw new Error(preliminary.error);
          NodeAssert.equal(preliminary.outcome, "actions-complete");
          const inactive = await waitInactive(primary.rpc, threadId);
          NodeAssert.equal(inactive.thread.activeRunId, null);
          await channel.release({ inactive: true });
          const result = await readAcknowledgedMilestone(
            files.final,
            await channel.wait(["final"], deadline(signal)),
          );
          jobs.at(-1).nativeInactiveReadback = true;
          NodeAssert.equal(result.outcome, "passed", result.error);
          await channel.close();
        }
        evidence.remoteFixture = proxy.evidence();
        NodeAssert.equal(
          evidence.remoteFixture.websocketAttempts,
          0,
          "An incompatible host must never receive orchestration traffic",
        );
        NodeAssert.ok(
          evidence.remoteFixture.requests.some(
            (request) => request.path === "/api/jones-updates/check" && request.authenticated,
          ),
        );
        if (renderer) {
          const served = renderer.evidence();
          NodeAssert.ok(
            served.requests.some((request) => request.path === expected.rendererManifest.entry),
            "The native browser must request the candidate renderer entry",
          );
          NodeAssert.ok(
            served.requests.some((request) => request.path.endsWith(".js")),
            "The native browser must request the candidate renderer bundle",
          );
          for (const request of served.requests)
            NodeAssert.equal(
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
            let started;
            try {
              started = JSON.parse(await NodeFSP.readFile(`${job.reportFile}.custody`, "utf8"));
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
              started = JSON.parse(await NodeFSP.readFile(`${job.reportFile}.started`, "utf8"));
            }
            identities.push(...[started.provider, started.bridge].filter(Boolean));
          } catch (error) {
            if (error.code !== "ENOENT") errors.push(redact(error.message));
          }
          try {
            let result;
            try {
              result = JSON.parse(await NodeFSP.readFile(`${job.reportFile}.final`, "utf8"));
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
              result = JSON.parse(await NodeFSP.readFile(job.reportFile, "utf8"));
            }
            evidence.scenarios.push({
              threadId: job.threadId,
              nativeInactiveReadback: job.nativeInactiveReadback === true,
              ...result,
              milestones: job.channel.evidence(),
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
            NodeFSP.writeFile(
              NodePath.join(out, `${index === 0 ? "primary" : "remote"}-server.log`),
              redact(
                server.output(),
                jobs.map((job) => job.channel.binding.nonce),
              ),
            ),
          ),
          atomicJson(NodePath.join(out, "qualification.json"), evidence),
        ]);
        errors.push(
          ...preserved
            .filter((entry) => entry.status === "rejected")
            .map((entry) => redact(String(entry.reason))),
        );
        const cleanup = await Promise.allSettled([
          ...jobs.map((job) => job.channel.close()),
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
          milestoneChannels: jobs.map((job) => job.channel.evidence()),
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
          await atomicJson(NodePath.join(out, "qualification.json"), evidence);
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
    await atomicJson(NodePath.join(out, "qualification.json"), evidence);
  }
  return evidence;
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  const [command, ...args] = process.argv.slice(2);
  const options = Object.fromEntries(
    Array.from({ length: args.length / 2 }, (_, index) => [
      args[index * 2].replace(/^--/, ""),
      args[index * 2 + 1],
    ]),
  );
  const source = NodePath.resolve(options.source ?? ".");
  const rendererDir = options["fixture-build"]
    ? NodePath.resolve(options["fixture-build"])
    : undefined;
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
      ? recordBuild(source, NodePath.resolve(options.out), rendererDir)
      : command === "run"
        ? qualify({
            source,
            buildReceipt: NodePath.resolve(options.receipt),
            out: NodePath.resolve(options.out),
            scratchParent: NodePath.resolve(options["scratch-parent"]),
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
