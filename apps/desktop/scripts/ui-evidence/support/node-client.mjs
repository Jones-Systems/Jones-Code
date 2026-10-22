import * as NodeAssert from "node:assert/strict";

const standardScopes = [
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
];

function loopback(value, protocol) {
  const url = new URL(value);
  if (
    url.protocol !== protocol ||
    !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error("Node client requires a private loopback endpoint");
  return url;
}

export function assertStandardScopes(scope) {
  NodeAssert.equal(typeof scope, "string", "Pairing must return explicit scopes");
  NodeAssert.deepEqual(
    scope.split(/\s+/).sort(),
    [...standardScopes].sort(),
    "Node client must have exactly the ordinary standard grant",
  );
}

function validateSnapshot(snapshot) {
  NodeAssert.ok(Number.isSafeInteger(snapshot.snapshotSequence) && snapshot.snapshotSequence >= 0);
  for (const key of ["projects", "threads", "archivedThreads"]) {
    NodeAssert.ok(Array.isArray(snapshot[key]), `Subscription snapshot has ${key}`);
    NodeAssert.equal(
      new Set(snapshot[key].map((row) => row.id)).size,
      snapshot[key].length,
      `Subscription snapshot has no duplicate ${key}`,
    );
  }
}

// This is a protocol fixture cache, not the production client-runtime state service.
export class NodeSubscriptionClient {
  #endpoint;
  #token;
  #fetch;
  #WebSocket;
  #socket = null;
  #generation = 0;
  #snapshot = null;
  #removed = false;
  #error = null;
  #waiters = new Set();
  #requests = [];
  #connections = [];

  constructor({
    endpoint,
    accessToken,
    fetch = globalThis.fetch,
    WebSocket = globalThis.WebSocket,
  }) {
    loopback(endpoint.httpBaseUrl, "http:");
    loopback(endpoint.wsBaseUrl, "ws:");
    NodeAssert.ok(typeof accessToken === "string" && accessToken.length > 0);
    this.#endpoint = endpoint;
    this.#token = accessToken;
    this.#fetch = fetch;
    this.#WebSocket = WebSocket;
  }

  get snapshot() {
    return structuredClone(this.#snapshot);
  }
  get evidence() {
    return {
      kind: "paired Node protocol fixture; independent of production client-runtime",
      requests: [...this.#requests],
      commandCount: this.#requests.filter((tag) => tag !== "orchestration.subscribeShell").length,
      connections: structuredClone(this.#connections),
      removed: this.#removed,
      credentialsCleared: this.#token === null,
      projectionCleared: this.#snapshot === null,
      socketClosed: this.#socket === null,
    };
  }

  #notify() {
    for (const waiter of this.#waiters) waiter();
  }
  #fail(message) {
    this.#error ??= new Error(message);
    this.#notify();
  }

  async waitFor(predicate) {
    return new Promise((resolve, reject) => {
      const finish = (error, value) => {
        clearTimeout(timer);
        this.#waiters.delete(check);
        if (error) reject(error);
        else resolve(value);
      };
      const check = () => {
        if (this.#error) return finish(this.#error);
        try {
          const snapshot = this.snapshot;
          if (predicate(snapshot, this.evidence)) finish(null, snapshot);
        } catch (error) {
          finish(error);
        }
      };
      const timer = setTimeout(
        () => finish(new Error("Node subscription observation timed out")),
        15000,
      );
      this.#waiters.add(check);
      check();
    });
  }

  restoreCachedSnapshot(snapshot) {
    validateSnapshot(snapshot);
    if (
      this.#removed ||
      (this.#snapshot && snapshot.snapshotSequence <= this.#snapshot.snapshotSequence)
    )
      return false;
    this.#snapshot = structuredClone(snapshot);
    this.#notify();
    return true;
  }

  #receive(item, connection) {
    if (item.kind === "synchronized") {
      NodeAssert.equal(
        connection.snapshots,
        1,
        "Each connection receives exactly one authoritative snapshot",
      );
      connection.synchronized = true;
    } else if (item.kind === "snapshot") {
      // Marked snapshots refresh project enrichment only; they are not a new thread projection.
      if (item.resolvedRepositoryIdentityRoots !== undefined) return;
      validateSnapshot(item.snapshot);
      connection.snapshots += 1;
      NodeAssert.equal(connection.snapshots, 1, "Duplicate authoritative subscription snapshot");
      NodeAssert.ok(
        !this.#snapshot || item.snapshot.snapshotSequence >= this.#snapshot.snapshotSequence,
        "Reconnect cannot regress the cached sequence",
      );
      this.#snapshot = structuredClone(item.snapshot);
    } else {
      NodeAssert.ok(this.#snapshot, "Subscription update follows an initial snapshot");
      NodeAssert.ok(Number.isSafeInteger(item.sequence), "Subscription update has a sequence");
      if (item.sequence < this.#snapshot.snapshotSequence) return;
      if (["thread.updated", "thread.removed"].includes(item.kind)) {
        const key = item.location === "active" ? "threads" : "archivedThreads";
        const id = item.kind === "thread.updated" ? item.thread.id : item.threadId;
        this.#snapshot[key] = this.#snapshot[key].filter((row) => row.id !== id);
        if (item.kind === "thread.updated") this.#snapshot[key].push(structuredClone(item.thread));
      } else if (["project.updated", "project.removed"].includes(item.kind)) {
        const id = item.kind === "project.updated" ? item.project.id : item.projectId;
        this.#snapshot.projects = this.#snapshot.projects.filter((row) => row.id !== id);
        if (item.kind === "project.updated")
          this.#snapshot.projects.push(structuredClone(item.project));
      } else throw new Error("Unexpected shell subscription item");
      this.#snapshot.snapshotSequence = item.sequence;
      connection.updates += 1;
    }
  }

  async connect() {
    NodeAssert.ok(
      !this.#removed && !this.#socket,
      "Node client must be disconnected and registered",
    );
    if (this.#error) throw this.#error;
    const response = await this.#fetch(
      new URL("/api/auth/websocket-ticket", this.#endpoint.httpBaseUrl),
      {
        method: "POST",
        headers: { authorization: `Bearer ${this.#token}` },
        credentials: "omit",
        redirect: "error",
        signal: AbortSignal.timeout(15000),
      },
    );
    if (!response.ok)
      throw new Error(`Node websocket authorization HTTP status ${response.status}`);
    const { ticket } = await response.json();
    NodeAssert.ok(
      typeof ticket === "string" && ticket.length > 0,
      "Pairing returns a websocket ticket",
    );
    const url = loopback(this.#endpoint.wsBaseUrl, "ws:");
    if (url.pathname === "/") url.pathname = "/ws";
    url.searchParams.set("wsTicket", ticket);
    url.searchParams.set("orchestrationProtocol", "2");
    const generation = ++this.#generation;
    const connection = { generation, snapshots: 0, updates: 0, synchronized: false };
    this.#connections.push(connection);
    const socket = new this.#WebSocket(url.href);
    this.#socket = socket;
    const current = () => generation === this.#generation && !this.#removed;
    socket.addEventListener("open", () => {
      if (!current()) return;
      const tag = "orchestration.subscribeShell";
      this.#requests.push(tag);
      socket.send(
        JSON.stringify({
          _tag: "Request",
          id: "0",
          tag,
          payload: { requestCompletionMarker: true },
          headers: [],
        }),
      );
    });
    socket.addEventListener("message", ({ data }) => {
      if (!current()) return;
      try {
        const message = JSON.parse(data);
        if (message._tag === "Ping") {
          socket.send(JSON.stringify({ _tag: "Pong" }));
          return;
        }
        if (message._tag === "Defect") throw new Error("RPC defect");
        if (String(message.requestId) !== "0") return;
        if (message._tag === "Exit") throw new Error("Subscription ended unexpectedly");
        if (message._tag !== "Chunk") throw new Error("Unexpected subscription response");
        for (const item of message.values) this.#receive(item, connection);
        socket.send(JSON.stringify({ _tag: "Ack", requestId: "0" }));
        this.#notify();
      } catch {
        this.#fail("Node subscription received an invalid or rejected projection");
      }
    });
    socket.addEventListener("error", () => {
      if (current()) this.#fail("Node subscription socket failed");
    });
    socket.addEventListener("close", () => {
      if (current()) this.#fail("Node subscription closed unexpectedly");
    });
    await this.waitFor(() => connection.synchronized);
  }

  async disconnect() {
    const socket = this.#socket;
    if (!socket) return;
    ++this.#generation;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Node subscription close timed out")), 15000);
      const closed = () => {
        clearTimeout(timer);
        resolve();
      };
      if (socket.readyState === 3) closed();
      else {
        socket.addEventListener("close", closed, { once: true });
        socket.close();
      }
    });
    this.#socket = null;
  }

  async remove() {
    await this.disconnect();
    this.#removed = true;
    this.#token = null;
    this.#snapshot = null;
    this.#notify();
  }
}

export async function pairNodeClient(ctx) {
  const endpoint = await ctx.topology();
  loopback(endpoint.httpBaseUrl, "http:");
  // A creates a one-time standard grant. Its administrative bearer never leaves the renderer.
  const credential = await ctx.page.evaluate(async ({ httpBaseUrl }) => {
    const bearer = await window.desktopBridge.getLocalEnvironmentBearerToken();
    const response = await fetch(new URL("/api/auth/pairing-token", httpBaseUrl), {
      method: "POST",
      headers: { authorization: `Bearer ${bearer}`, "content-type": "application/json" },
      body: JSON.stringify({ label: "UI evidence Node client B" }),
      credentials: "omit",
      redirect: "error",
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Synthetic pairing HTTP status ${response.status}`);
    const value = await response.json();
    if (typeof value.credential !== "string" || !value.credential)
      throw new Error("Missing synthetic pairing grant");
    return value.credential;
  }, endpoint);
  const response = await fetch(new URL("/oauth/token", endpoint.httpBaseUrl), {
    method: "POST",
    credentials: "omit",
    redirect: "error",
    signal: AbortSignal.timeout(15000),
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: credential,
      subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      client_label: "UI evidence Node client B",
      client_device_type: "bot",
      client_os: "Linux",
    }),
  });
  if (!response.ok) throw new Error(`Node pairing exchange HTTP status ${response.status}`);
  const grant = await response.json();
  assertStandardScopes(grant.scope);
  NodeAssert.equal(grant.token_type, "Bearer", "Node fixture uses ordinary bearer pairing");
  return new NodeSubscriptionClient({ endpoint, accessToken: grant.access_token });
}
