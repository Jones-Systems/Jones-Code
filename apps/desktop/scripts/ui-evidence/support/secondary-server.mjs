import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as NodeTimersPromises from "node:timers/promises";
import { assertStandardScopes } from "./node-client.mjs";

export const SECONDARY = Object.freeze({
  origin: "http://127.0.0.1:43774",
  port: 43774,
  root: "/scratch/tmp/je03-b",
  home: "/scratch/tmp/je03-b/home",
  user: "/scratch/tmp/je03-b/user",
  workspace: "/scratch/tmp/je03-b/workspace",
  executable: "/electron/electron",
  entry: "/app/apps/server/dist/bin.mjs",
});
const PRIMARY = "http://127.0.0.1:43773";
const fail = (code) => new Error(code);

export function secondaryLaunch() {
  return {
    executable: SECONDARY.executable,
    args: [
      SECONDARY.entry,
      "serve",
      "--mode",
      "web",
      "--host",
      "127.0.0.1",
      "--port",
      String(SECONDARY.port),
      "--base-dir",
      SECONDARY.home,
      "--no-browser",
    ],
    options: {
      cwd: SECONDARY.workspace,
      stdio: ["ignore", "ignore", "pipe"],
      env: {
        ELECTRON_RUN_AS_NODE: "1",
        PATH: "/usr/bin:/bin",
        HOME: SECONDARY.user,
        XDG_CONFIG_HOME: `${SECONDARY.user}/config`,
        XDG_CACHE_HOME: `${SECONDARY.user}/cache`,
        XDG_DATA_HOME: `${SECONDARY.user}/data`,
        XDG_STATE_HOME: `${SECONDARY.user}/state`,
        XDG_RUNTIME_DIR: `${SECONDARY.user}/run`,
        TMPDIR: `${SECONDARY.user}/tmp`,
        LANG: "C.UTF-8",
        TZ: "UTC",
        T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
        T3CODE_DISABLE_AUTO_UPDATE: "true",
        OTEL_SDK_DISABLED: "true",
      },
    },
  };
}

export async function assertPortClosed(connectSocket = NodeNet.connect) {
  await new Promise((resolve, reject) => {
    const socket = connectSocket({ host: "127.0.0.1", port: SECONDARY.port });
    const finish = (error) => {
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    socket.setTimeout(1000, () => finish(fail("b-port-occupied")));
    socket.once("connect", () => finish(fail("b-port-occupied")));
    socket.once("error", (error) =>
      finish(error.code === "ECONNREFUSED" ? null : fail("b-port-occupied")),
    );
  });
}

// runtime.mjs's reader is private. Preserve its Linux start-ticks contract without changing it.
export async function readOwnedIdentity(pid, readFile = NodeFSP.readFile) {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw fail("secondary-identity-unbound");
  const stat = await readFile(`/proc/${pid}/stat`, "utf8");
  const fields = stat
    .slice(stat.lastIndexOf(")") + 2)
    .trim()
    .split(/\s+/);
  if (!/^\d+$/.test(fields[19] ?? "")) throw fail("secondary-identity-unbound");
  return { pid, startTicks: fields[19] };
}

export function validateReady({ descriptor, primary, persistedId, runtime, identity }) {
  if (
    runtime?.version !== 1 ||
    runtime.pid !== identity.pid ||
    runtime.port !== SECONDARY.port ||
    runtime.origin !== SECONDARY.origin
  )
    throw fail("runtime-state-unbound");
  if (
    typeof descriptor?.environmentId !== "string" ||
    !descriptor.environmentId ||
    descriptor.environmentId !== persistedId.trim() ||
    typeof primary?.environmentId !== "string" ||
    !primary.environmentId ||
    descriptor.environmentId === primary.environmentId ||
    typeof descriptor.label !== "string"
  )
    throw fail("secondary-start-failed");
  return {
    environmentId: descriptor.environmentId,
    primaryEnvironmentId: primary.environmentId,
    label: descriptor.label,
    origin: SECONDARY.origin,
    ...identity,
  };
}

export function validateSeed(command) {
  const allowed = {
    "project.create": ["type", "commandId", "projectId", "title", "workspaceRoot"],
    "thread.create": [
      "type",
      "commandId",
      "createdBy",
      "creationSource",
      "threadId",
      "projectId",
      "title",
      "modelSelection",
      "runtimeMode",
      "interactionMode",
      "branch",
      "worktreePath",
    ],
    "thread.metadata.update": ["type", "commandId", "threadId", "title"],
  }[command?.type];
  if (!allowed || Object.keys(command).some((key) => !allowed.includes(key)))
    throw fail("secondary-seed-outside-metadata");
  if (command.type === "project.create" && command.workspaceRoot !== SECONDARY.workspace)
    throw fail("secondary-seed-outside-workspace");
  if (
    command.type === "thread.create" &&
    (command.createdBy !== "user" ||
      command.creationSource !== "web" ||
      command.branch !== null ||
      command.worktreePath !== null)
  )
    throw fail("secondary-seed-outside-metadata");
}

export async function stopOwnedProcess(
  identity,
  {
    readIdentity = readOwnedIdentity,
    kill = (pid, signal) => process.kill(pid, signal),
    now = () => performance.now(),
    sleep = NodeTimersPromises.setTimeout,
  } = {},
) {
  const absent = async () => {
    try {
      return (await readIdentity(identity.pid)).startTicks !== identity.startTicks;
    } catch (error) {
      if (error.code === "ENOENT" || error.code === "ESRCH") return true;
      throw error;
    }
  };
  const signal = async (name) => {
    if (await absent()) return;
    try {
      kill(identity.pid, name);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  };
  try {
    if (await absent()) return { outcome: "stopped", escalated: false, ...identity };
    const end = now() + 15000;
    await signal("SIGTERM");
    while (now() < end - 1000) {
      if (await absent()) return { outcome: "stopped", escalated: false, ...identity };
      await sleep(50);
    }
    await signal("SIGKILL");
    while (now() < end) {
      if (await absent()) return { outcome: "stopped", escalated: true, ...identity };
      await sleep(25);
    }
    if (await absent()) return { outcome: "stopped", escalated: true, ...identity };
  } catch {
    /* An unreadable identity never licenses a signal. */
  }
  throw fail("secondary-stop-unknown");
}

export class SecondaryServer {
  #deps;
  #controller = new AbortController();
  #child = null;
  #identity = null;
  #cli = null;
  #sockets = new Set();
  #pairing = null;
  #seederPairing = null;
  #bearer = null;
  #stopPromise = null;
  #stderr = Buffer.alloc(0);
  #spawnFailed = false;
  #readyDeadline = null;
  #onAbort;
  #parentSignal;
  #evidence = { identity: null, stop: { outcome: "not-started" } };

  constructor({ signal, ...dependencies } = {}) {
    this.#deps = {
      fs: NodeFSP,
      spawn: NodeChildProcess.spawn,
      fetch: globalThis.fetch,
      WebSocket: globalThis.WebSocket,
      portClosed: assertPortClosed,
      readIdentity: readOwnedIdentity,
      kill: (pid, name) => process.kill(pid, name),
      now: () => performance.now(),
      sleep: NodeTimersPromises.setTimeout,
      ...dependencies,
    };
    this.#parentSignal = signal;
    this.#onAbort = () => this.#controller.abort(fail("secondary-cancelled"));
    signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (signal?.aborted) this.#onAbort();
  }

  get evidence() {
    return {
      ...structuredClone(this.#evidence),
      stderr: {
        sha256: NodeCrypto.createHash("sha256").update(this.#stderr).digest("hex"),
        lineCount: this.#stderr.toString().split("\n").length - 1,
        bytes: this.#stderr.length,
      },
    };
  }
  get pairingCode() {
    if (!this.#pairing || this.#controller.signal.aborted)
      throw fail("secondary-pairing-unavailable");
    return this.#pairing;
  }
  assertSafeEvidence(value) {
    const text = typeof value === "string" ? value : JSON.stringify(value);
    if (
      [this.#pairing, this.#seederPairing, this.#bearer].some(
        (secret) => secret && text.includes(secret),
      )
    )
      throw fail("secret-in-evidence");
  }

  async #request(origin, path, options = {}) {
    this.#controller.signal.throwIfAborted();
    const timeout =
      this.#readyDeadline === null
        ? 15000
        : Math.max(1, Math.min(15000, Math.ceil(this.#readyDeadline - this.#deps.now())));
    return this.#deps.fetch(new URL(path, origin), {
      ...options,
      credentials: "omit",
      redirect: "error",
      signal: AbortSignal.any([this.#controller.signal, AbortSignal.timeout(timeout)]),
    });
  }
  async #json(path, options = {}) {
    const response = await this.#request(SECONDARY.origin, path, options);
    if (!response.ok) throw fail("secondary-http-failed");
    return response.json();
  }
  #headers() {
    if (!this.#bearer) throw fail("secondary-auth-unavailable");
    return { authorization: `Bearer ${this.#bearer}` };
  }

  async start() {
    if (this.#child || this.#stopPromise) throw fail("secondary-start-repeated");
    try {
      this.#controller.signal.throwIfAborted();
      await this.#deps.portClosed();
      await this.#deps.fs.mkdir(SECONDARY.root, { mode: 0o700 });
      for (const path of [
        SECONDARY.home,
        SECONDARY.user,
        SECONDARY.workspace,
        ...["config", "cache", "data", "state", "run", "tmp"].map(
          (part) => `${SECONDARY.user}/${part}`,
        ),
      ])
        await this.#deps.fs.mkdir(path, { mode: 0o700 });
      this.#controller.signal.throwIfAborted();
      const launch = secondaryLaunch();
      this.#child = this.#deps.spawn(launch.executable, launch.args, launch.options);
      this.#child.on("error", () => {
        this.#spawnFailed = true;
      });
      this.#child.stderr.on("data", (chunk) => {
        this.#stderr = Buffer.concat([
          this.#stderr,
          Buffer.from(chunk).subarray(0, 65536 - this.#stderr.length),
        ]);
      });
      this.#identity = await this.#deps.readIdentity(this.#child.pid);
      this.#evidence.stop = { outcome: "pending" };
      const end = this.#deps.now() + 30000;
      this.#readyDeadline = end;
      let ready;
      while (this.#deps.now() < end) {
        this.#controller.signal.throwIfAborted();
        if (this.#spawnFailed || this.#child.exitCode !== null || this.#child.signalCode !== null)
          throw fail("secondary-start-failed");
        try {
          const descriptor = await this.#json("/.well-known/t3/environment");
          const response = await this.#request(PRIMARY, "/.well-known/t3/environment");
          if (!response.ok) throw fail("secondary-start-failed");
          const primary = await response.json();
          const persistedId = await this.#deps.fs.readFile(
            `${SECONDARY.home}/userdata/environment-id`,
            "utf8",
          );
          const runtime = JSON.parse(
            await this.#deps.fs.readFile(`${SECONDARY.home}/userdata/server-runtime.json`, "utf8"),
          );
          const identity = await this.#deps.readIdentity(this.#identity.pid);
          if (identity.startTicks !== this.#identity.startTicks)
            throw fail("secondary-start-failed");
          ready = validateReady({ descriptor, primary, persistedId, runtime, identity });
          if (this.#deps.now() >= end) throw fail("secondary-start-failed");
          break;
        } catch (error) {
          if (["runtime-state-unbound", "secondary-start-failed"].includes(error.message))
            throw error;
        }
        await this.#deps.sleep(100, undefined, { signal: this.#controller.signal });
      }
      if (!ready) throw fail("secondary-start-failed");
      this.#readyDeadline = null;
      this.#evidence.identity = ready;
      this.#pairing = await this.#issuePairing();
      this.#seederPairing = await this.#issuePairing();
      if (this.#pairing === this.#seederPairing) throw fail("secondary-pairing-not-independent");
      const grant = await this.#json("/oauth/token", {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
          subject_token: this.#seederPairing,
          subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
          requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
          client_label: "UI evidence JE03 B seeder",
          client_device_type: "bot",
          client_os: "Linux",
        }),
      });
      assertStandardScopes(grant.scope);
      if (
        grant.token_type !== "Bearer" ||
        typeof grant.access_token !== "string" ||
        !grant.access_token
      )
        throw fail("secondary-auth-unavailable");
      this.#bearer = grant.access_token;
      return ready;
    } catch (error) {
      await this.stop();
      if (["b-port-occupied", "runtime-state-unbound"].includes(error.message)) throw error;
      throw fail("secondary-start-failed");
    }
  }

  async #issuePairing() {
    this.#controller.signal.throwIfAborted();
    const launch = secondaryLaunch();
    const child = this.#deps.spawn(
      launch.executable,
      [
        SECONDARY.entry,
        "auth",
        "pairing",
        "create",
        "--base-dir",
        SECONDARY.home,
        "--ttl",
        "5m",
        "--base-url",
        SECONDARY.origin,
        "--json",
      ],
      { ...launch.options, stdio: ["ignore", "pipe", "ignore"] },
    );
    let output = Buffer.alloc(0);
    const completion = new Promise((resolve, reject) => {
      child.on("error", () => reject(fail("secondary-pairing-failed")));
      child.stdout.on("data", (chunk) => {
        if (output.length + chunk.length > 65536) reject(fail("secondary-pairing-failed"));
        else output = Buffer.concat([output, chunk]);
      });
      child.once("close", (code) =>
        code === 0 ? resolve() : reject(fail("secondary-pairing-failed")),
      );
    });
    void completion.catch(() => {});
    this.#cli = { child, identity: null };
    let timer, abort;
    try {
      this.#cli.identity = await this.#deps.readIdentity(child.pid);
      await Promise.race([
        completion,
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(fail("secondary-pairing-failed")), 15000);
          abort = () => reject(fail("secondary-cancelled"));
          this.#controller.signal.addEventListener("abort", abort, { once: true });
          if (this.#controller.signal.aborted) abort();
        }),
      ]);
      const value = JSON.parse(output.toString());
      if (typeof value.credential !== "string" || !value.credential || !Array.isArray(value.scopes))
        throw fail("secondary-pairing-failed");
      assertStandardScopes(value.scopes.join(" "));
      this.#cli = null;
      return value.credential;
    } finally {
      clearTimeout(timer);
      if (abort) this.#controller.signal.removeEventListener("abort", abort);
      output.fill(0);
    }
  }

  async seed(command) {
    validateSeed(command);
    if (command.type === "project.create")
      return this.#json("/api/projects/mutate", {
        method: "POST",
        headers: { ...this.#headers(), "content-type": "application/json" },
        body: JSON.stringify(command),
      });
    const { ticket } = await this.#json("/api/auth/websocket-ticket", {
      method: "POST",
      headers: this.#headers(),
    });
    if (typeof ticket !== "string" || !ticket) throw fail("secondary-rpc-auth-failed");
    const url = new URL("ws://127.0.0.1:43774/ws");
    url.searchParams.set("wsTicket", ticket);
    url.searchParams.set("orchestrationProtocol", "2");
    return new Promise((resolve, reject) => {
      const socket = new this.#deps.WebSocket(url.href);
      this.#sockets.add(socket);
      let settled = false;
      const finish = (error, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.#controller.signal.removeEventListener("abort", abort);
        socket.close();
        this.#sockets.delete(socket);
        if (error) reject(error);
        else resolve(value);
      };
      const abort = () => finish(fail("secondary-cancelled"));
      const timer = setTimeout(() => finish(fail("secondary-rpc-timeout")), 15000);
      this.#controller.signal.addEventListener("abort", abort, { once: true });
      if (this.#controller.signal.aborted) {
        abort();
        return;
      }
      socket.addEventListener("open", () => {
        if (settled) return;
        try {
          socket.send(
            JSON.stringify({
              _tag: "Request",
              id: "0",
              tag: "orchestration.dispatchCommand",
              payload: command,
              headers: [],
            }),
          );
        } catch {
          finish(fail("secondary-rpc-failed"));
        }
      });
      socket.addEventListener("message", ({ data }) => {
        if (settled) return;
        try {
          const value = JSON.parse(data);
          if (value._tag === "Ping") {
            socket.send(JSON.stringify({ _tag: "Pong" }));
            return;
          }
          if (value._tag !== "Exit" || String(value.requestId) !== "0") return;
          finish(
            value.exit?._tag === "Success" ? null : fail("secondary-rpc-rejected"),
            value.exit?.value,
          );
        } catch {
          finish(fail("secondary-rpc-failed"));
        }
      });
      socket.addEventListener("error", () => finish(fail("secondary-rpc-failed")));
      socket.addEventListener("close", () => finish(fail("secondary-rpc-closed")));
    });
  }

  readSnapshot() {
    return this.#json("/api/orchestration/shell", {
      headers: {
        ...this.#headers(),
        "x-t3-orchestration-protocol": "2",
      },
    });
  }
  async credentialFence() {
    const statuses = [];
    for (const origin of [PRIMARY, SECONDARY.origin]) {
      const response = await this.#request(origin, "/api/orchestration/shell", {
        headers: { ...this.#headers(), "x-t3-orchestration-protocol": "2" },
      });
      statuses.push(response.status);
      await response.body?.cancel();
    }
    if (![401, 403].includes(statuses[0]) || statuses[1] !== 200)
      throw fail("credential-fence-failed");
    return { ok: true, aStatus: statuses[0], bStatus: statuses[1] };
  }

  stop() {
    if (!this.#stopPromise) this.#stopPromise = this.#stop();
    return this.#stopPromise;
  }
  async #stop() {
    this.#controller.abort(fail("secondary-cancelled"));
    this.#parentSignal?.removeEventListener("abort", this.#onAbort);
    for (const socket of this.#sockets) socket.close();
    this.#sockets.clear();
    try {
      let unknown = false;
      await Promise.all(
        [this.#cli, this.#child ? { child: this.#child, identity: this.#identity } : null].map(
          async (owned) => {
            if (!owned) return;
            try {
              if (!owned.identity) {
                if (
                  owned.child.exitCode !== null ||
                  owned.child.signalCode !== null ||
                  (!owned.child.pid && this.#spawnFailed)
                )
                  return;
                throw fail("secondary-stop-unknown");
              }
              const stopped = await stopOwnedProcess(owned.identity, this.#deps);
              if (owned.child === this.#child) this.#evidence.stop = stopped;
            } catch {
              unknown = true;
            }
          },
        ),
      );
      if (unknown) {
        this.#evidence.stop = { outcome: "unknown" };
        throw fail("secondary-stop-unknown");
      }
      return this.evidence.stop;
    } finally {
      this.#pairing = null;
      this.#seederPairing = null;
      this.#bearer = null;
    }
  }
}
