import { createHmac, randomBytes } from "node:crypto";

const A = "http://127.0.0.1:43773";
const B = "http://127.0.0.1:43774";

function endpoint(value) {
  const url = new URL(value);
  if (url.protocol === "ws:") url.protocol = "http:";
  if (url.protocol === "wss:") url.protocol = "https:";
  return url;
}

export function assertCrossover(entries, expectedByGeneration) {
  const tickets = new Map();
  for (const entry of entries) {
    if (![A, B].includes(entry.origin)) throw new Error("renderer-unexpected-origin");
    const expected = expectedByGeneration[entry.generation];
    if (!expected?.a || !expected?.b || expected.a === expected.b)
      throw new Error("renderer-fingerprints-unbound");
    if (entry.authFp && entry.authFp !== expected[entry.origin === A ? "a" : "b"])
      throw new Error("renderer-credential-crossover");
    if (entry.ticketFp) {
      const previous = tickets.get(entry.ticketFp);
      if (previous && previous !== entry.origin) throw new Error("renderer-ticket-crossover");
      tickets.set(entry.ticketFp, entry.origin);
    }
  }
  return { ok: true };
}

export function assertPinRouting(entries, from, through, generation) {
  const pins = entries.filter(
    (entry) => entry.sequence >= from && entry.sequence < through &&
      entry.generation === generation && entry.kind === "frame" &&
      entry.direction === "sent" && entry.tag === "orchestration.dispatchCommand" &&
      entry.commandType === "thread.pin",
  );
  if (pins.length !== 1 || pins[0].origin !== B || !pins[0].threadMatch)
    throw new Error("pin-endpoint-mismatch");
  return { ok: true, count: pins.length, origin: B };
}

export class EndpointTrace {
  #key;
  #threadId;
  #entries = [];
  #sequence = 0;
  #generation = -1;
  #detach = [];
  #pending = new Set();
  #failure = null;
  #expected = {};
  #limit;
  #localAssetRequests = 0;

  constructor(threadId, { key = randomBytes(32), limit = 5000 } = {}) {
    if (key.length !== 32 || !Number.isInteger(limit) || limit < 1 || limit > 5000)
      throw new Error("invalid-trace-bounds");
    this.#key = Buffer.from(key);
    this.#threadId = threadId;
    this.#limit = limit;
  }

  fingerprint(value) {
    return createHmac("sha256", this.#key).update(value).digest("hex").slice(0, 16);
  }
  get generation() { return this.#generation; }
  get entries() { return structuredClone(this.#entries).sort((a, b) => a.sequence - b.sequence); }
  mark() { return this.#sequence; }

  #reserve() {
    if (this.#sequence >= this.#limit) {
      this.#failure = "renderer-trace-overflow";
      return null;
    }
    return { sequence: this.#sequence++, generation: this.#generation, t: performance.now() };
  }

  attach(page) {
    this.detach();
    this.#generation++;
    const listen = (emitter, event, fn) => {
      emitter.on(event, fn);
      this.#detach.push(() => emitter.off(event, fn));
    };
    listen(page, "request", (request) => {
      const slot = this.#reserve();
      if (!slot) return;
      const pending = (async () => {
        const url = endpoint(request.url());
        const headers = await request.allHeaders();
        const auth = headers.authorization;
        // ElectronProtocol.ts serves packaged assets at t3code://app, outside HTTP endpoints.
        const packagedAsset = url.protocol === "t3code:" && url.hostname === "app";
        if ((packagedAsset || ["file:", "data:", "blob:"].includes(url.protocol)) && !auth) {
          this.#localAssetRequests++;
          return;
        }
        this.#entries.push({ ...slot, kind: "request", method: request.method(),
          origin: url.origin, pathname: url.pathname,
          authFp: auth ? this.fingerprint(auth) : null });
      })().catch(() => { this.#failure = "renderer-network-unobservable"; });
      this.#pending.add(pending);
      void pending.finally(() => this.#pending.delete(pending));
    });
    listen(page, "websocket", (socket) => {
      try {
        const url = endpoint(socket.url());
        const origin = url.origin;
        const ticket = url.searchParams.get("wsTicket");
        const slot = this.#reserve();
        if (!slot) return;
        if (!ticket) this.#failure = "renderer-network-unobservable";
        this.#entries.push({ ...slot, kind: "websocket", origin, pathname: url.pathname,
          ticketFp: ticket ? this.fingerprint(ticket) : null });
        for (const [event, direction] of [["framesent", "sent"], ["framereceived", "received"]]) {
          listen(socket, event, ({ payload }) => {
            const frameSlot = this.#reserve();
            if (!frameSlot) return;
            const frame = { ...frameSlot, kind: "frame", origin, direction };
            if (direction === "sent") {
              try {
                const value = JSON.parse(typeof payload === "string" ? payload : payload.toString());
                if (value._tag === "Request") {
                  if (typeof value.tag !== "string" || !/^[a-zA-Z0-9_.-]{1,128}$/.test(value.tag) ||
                      (value.payload?.type !== undefined && (typeof value.payload.type !== "string" ||
                        !/^[a-zA-Z0-9_.-]{1,128}$/.test(value.payload.type)))) {
                    this.#failure = "renderer-frame-unreadable";
                  } else {
                    frame.tag = value.tag;
                    frame.commandType = value.payload?.type ?? null;
                  }
                  frame.threadMatch = value.payload?.threadId === this.#threadId;
                }
              } catch { this.#failure = "renderer-frame-unreadable"; }
            }
            this.#entries.push(frame);
          });
        }
        listen(socket, "close", () => {
          const closeSlot = this.#reserve();
          if (closeSlot) this.#entries.push({ ...closeSlot, kind: "close", origin });
        });
      } catch { this.#failure = "renderer-network-unobservable"; }
    });
  }

  async flush() {
    await Promise.all([...this.#pending]);
    if (this.#failure) throw new Error(this.#failure);
  }

  async catalog(page, environmentId) {
    const value = await page.evaluate(async ({ keyBytes, environmentId, origin }) => {
      const key = await crypto.subtle.importKey("raw", new Uint8Array(keyBytes),
        { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const fp = async (text) => Array.from(new Uint8Array(await crypto.subtle.sign(
        "HMAC", key, new TextEncoder().encode(text),
      ))).map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 16);
      const a = await window.desktopBridge.getLocalEnvironmentBearerToken();
      const raw = await window.desktopBridge.getConnectionCatalogSnapshot();
      if (typeof a !== "string" || !a || typeof raw !== "string")
        throw new Error("catalog-credential-unbound");
      const document = JSON.parse(raw);
      const id = `bearer:${environmentId}:${origin}`;
      const targets = document.targets.filter((target) => target.connectionId === id &&
        target.environmentId === environmentId && target._tag === "BearerConnectionTarget");
      const profiles = document.profiles.filter((profile) => profile.connectionId === id &&
        profile.environmentId === environmentId && profile._tag === "BearerConnectionProfile" &&
        new URL(profile.httpBaseUrl).origin === origin &&
        new URL(profile.wsBaseUrl).host === new URL(origin).host &&
        new URL(profile.wsBaseUrl).protocol === "ws:");
      const credentials = document.credentials.filter((entry) => entry.connectionId === id &&
        entry.credential?._tag === "BearerConnectionCredential");
      if (targets.length !== 1 || profiles.length !== 1 || credentials.length !== 1 ||
          typeof credentials[0].credential.token !== "string" || !credentials[0].credential.token)
        throw new Error("catalog-credential-unbound");
      return { a: await fp(`Bearer ${a}`), b: await fp(`Bearer ${credentials[0].credential.token}`),
        target: true, profile: true, credential: true,
        disabled: document.disabledEnvironmentIds.includes(environmentId) };
    }, { keyBytes: [...this.#key], environmentId, origin: B });
    const previous = this.#expected[this.#generation];
    if (previous && (previous.a !== value.a || previous.b !== value.b))
      throw new Error("catalog-credential-changed");
    const first = this.#expected[0];
    if (first && first.b !== value.b) throw new Error("catalog-credential-changed");
    this.#expected[this.#generation] = { a: value.a, b: value.b };
    return value;
  }

  async checkpoint() {
    await this.flush();
    return assertCrossover(this.entries, this.#expected);
  }
  summary() {
    const counts = {};
    for (const entry of this.#entries) {
      const key = `${entry.generation} ${entry.origin} ${entry.kind} ${entry.tag ?? ""}`;
      counts[key] = (counts[key] ?? 0) + 1;
    }
    return { count: this.#entries.length, localAssetRequests: this.#localAssetRequests,
      counts, fingerprints: structuredClone(this.#expected),
      failure: this.#failure };
  }
  detach() {
    for (const off of this.#detach.splice(0)) off();
  }
  dispose() { this.detach(); this.#key.fill(0); }
}
