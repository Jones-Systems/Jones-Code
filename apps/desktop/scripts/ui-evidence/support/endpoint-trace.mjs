import * as NodeCrypto from "node:crypto";

const A = "http://127.0.0.1:43773";
const B = "http://127.0.0.1:43774";
const REQUEST_CLASSES = new Set([
  "document",
  "stylesheet",
  "image",
  "media",
  "font",
  "script",
  "texttrack",
  "xhr",
  "fetch",
  "eventsource",
  "websocket",
  "manifest",
  "other",
]);
// Only constant templates escape into diagnostics; IDs, queries and unknown paths never do.
const PATH_TEMPLATES = [
  [/^\/$/, "/"],
  [/^\/ws$/, "/ws"],
  [/^\/oauth\/token$/, "/oauth/token"],
  [/^\/api\/auth\/websocket-ticket$/, "/api/auth/websocket-ticket"],
  [/^\/api\/orchestration\/shell$/, "/api/orchestration/shell"],
  [/^\/api\/orchestration\/dispatch$/, "/api/orchestration/dispatch"],
  [/^\/api\/projects\/mutate$/, "/api/projects/mutate"],
  [/^\/api\/orchestration\/threads\/[^/]+$/, "/api/orchestration/threads/:threadId"],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/commands\/[^/]+$/,
    "/api/orchestration/threads/:threadId/commands/:commandId",
  ],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/provider-goal-state$/,
    "/api/orchestration/threads/:threadId/provider-goal-state",
  ],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/attention$/,
    "/api/orchestration/threads/:threadId/attention",
  ],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/transcript$/,
    "/api/orchestration/threads/:threadId/transcript",
  ],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/bounded$/,
    "/api/orchestration/threads/:threadId/bounded",
  ],
  [
    /^\/api\/orchestration\/threads\/[^/]+\/history$/,
    "/api/orchestration/threads/:threadId/history",
  ],
];
// A serialization bound, never a wait or a qualification deadline.
const RELOAD_DIAGNOSTIC_BOUND_MS = 3_600_000;

function endpoint(value) {
  const url = new URL(value);
  if (url.protocol === "ws:") url.protocol = "http:";
  if (url.protocol === "wss:") url.protocol = "https:";
  return url;
}

function originClass(url) {
  if (["file:", "data:", "blob:"].includes(url.protocol)) return url.protocol.slice(0, -1);
  if (url.origin === A) return "A";
  if (url.origin === B) return "B";
  if (url.protocol === "t3code:" && url.hostname === "app") return "packaged";
  return "other";
}

function requestClass(request) {
  try {
    const value = request.resourceType();
    return REQUEST_CLASSES.has(value) ? value : "unavailable";
  } catch {
    return "unavailable";
  }
}

function endpointPathTemplate(url) {
  if (!["A", "B"].includes(originClass(url))) return "not-endpoint";
  for (const [pattern, template] of PATH_TEMPLATES) if (pattern.test(url.pathname)) return template;
  return "other";
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
    (entry) =>
      entry.sequence >= from &&
      entry.sequence < through &&
      entry.generation === generation &&
      entry.kind === "frame" &&
      entry.direction === "sent" &&
      entry.tag === "orchestration.dispatchCommand" &&
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
  #pending = new Map();
  #failure = null;
  #expected = {};
  #limit;
  #localAssetRequests = 0;
  #drainTimeoutMs;
  #incomplete = false;
  #pendingAtTimeout = null;
  #pendingDiagnosticsAtTimeout = null;
  #sealed = false;
  #disposed = false;
  #diagnosticNow;
  #diagnosticLast = null;
  #diagnosticClockFailed = false;
  #lifecycle = null;

  constructor(
    threadId,
    {
      key = NodeCrypto.randomBytes(32),
      limit = 5000,
      drainTimeoutMs = 1000,
      diagnosticNow = () => performance.now(),
    } = {},
  ) {
    if (
      key.length !== 32 ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 5000 ||
      !Number.isInteger(drainTimeoutMs) ||
      drainTimeoutMs < 1 ||
      drainTimeoutMs > 1000
    )
      throw new Error("invalid-trace-bounds");
    this.#key = Buffer.from(key);
    this.#threadId = threadId;
    this.#limit = limit;
    this.#drainTimeoutMs = drainTimeoutMs;
    this.#diagnosticNow = diagnosticNow;
  }

  fingerprint(value) {
    if (this.#sealed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    return NodeCrypto.createHmac("sha256", this.#key).update(value).digest("hex").slice(0, 16);
  }
  get generation() {
    return this.#generation;
  }
  get entries() {
    return structuredClone(this.#entries).sort((a, b) => a.sequence - b.sequence);
  }
  mark() {
    return this.#sequence;
  }

  #diagnosticTime() {
    if (this.#diagnosticClockFailed) return null;
    try {
      const value = this.#diagnosticNow();
      if (
        typeof value === "number" &&
        Number.isFinite(value) &&
        value >= 0 &&
        value <= Number.MAX_SAFE_INTEGER &&
        (this.#diagnosticLast === null || value >= this.#diagnosticLast)
      ) {
        this.#diagnosticLast = value;
        return value;
      }
    } catch {
      // Unavailable diagnostics must not change header observation or the qualification result.
    }
    this.#diagnosticClockFailed = true;
    return null;
  }

  observeReload(work) {
    // Both endpoints of the delta use this trace's Node clock, not browser or wall-clock time.
    // The marker is the invocation boundary; it is not a browser navigation timestamp.
    if (!this.#sealed && this.#lifecycle && !this.#lifecycle.detached) {
      if (this.#lifecycle.reloadStart !== null) this.#lifecycle.multipleReloads = true;
      else this.#lifecycle.reloadStart = { t: this.#diagnosticTime() };
    }
    return work();
  }

  #reloadTiming(metadata) {
    let state = "reload-unmarked";
    let requestObservedRelativeToReloadStartMs = null;
    const reload = metadata.lifecycle.reloadStart;
    if (metadata.lifecycle.multipleReloads) state = "multiple-reloads";
    else if (reload !== null) {
      if (this.#diagnosticClockFailed || reload.t === null || metadata.observedAt === null)
        state = "clock-unavailable";
      else {
        const delta = metadata.observedAt - reload.t;
        if (Number.isFinite(delta) && Math.abs(delta) <= RELOAD_DIAGNOSTIC_BOUND_MS) {
          state = "observed";
          requestObservedRelativeToReloadStartMs = delta;
        } else state = "out-of-range";
      }
    }
    return Object.freeze({ state, requestObservedRelativeToReloadStartMs });
  }

  #reserve() {
    if (this.#sealed) return null;
    if (this.#sequence >= this.#limit) {
      this.#failure ??= "renderer-trace-overflow";
      return null;
    }
    return { sequence: this.#sequence++, generation: this.#generation, t: performance.now() };
  }

  attach(page) {
    if (this.#sealed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    this.detach();
    this.#generation++;
    const lifecycle = {
      pageClosed: false,
      detached: false,
      reloadStart: null,
      multipleReloads: false,
    };
    this.#lifecycle = lifecycle;
    const requests = new WeakMap();
    this.#detach.push(() => {
      lifecycle.detached = true;
    });
    const listen = (emitter, event, fn) => {
      emitter.on(event, fn);
      this.#detach.push(() => emitter.off(event, fn));
    };
    listen(page, "close", () => {
      if (!this.#sealed) lifecycle.pageClosed = true;
    });
    for (const [event, field] of [
      ["requestfinished", "requestFinished"],
      ["requestfailed", "requestFailed"],
    ]) {
      listen(page, event, (request) => {
        const metadata = requests.get(request);
        if (!this.#sealed && metadata) metadata[field] = true;
      });
    }
    listen(page, "request", (request) => {
      const slot = this.#reserve();
      if (!slot) return;
      const metadata = {
        ...slot,
        originClass: "other",
        observedAt: this.#diagnosticTime(),
        requestClass: requestClass(request),
        pathTemplate: "unavailable",
        headerState: "pending",
        requestFinished: false,
        requestFailed: false,
        lifecycle,
      };
      requests.set(request, metadata);
      const pending = (async () => {
        const url = endpoint(request.url());
        metadata.originClass = originClass(url);
        metadata.pathTemplate = endpointPathTemplate(url);
        const headers = await request.allHeaders();
        if (this.#sealed) return;
        metadata.headerState = "resolved";
        const auth = headers.authorization;
        // ElectronProtocol.ts serves packaged assets at t3code://app, outside HTTP endpoints.
        const packagedAsset = url.protocol === "t3code:" && url.hostname === "app";
        if ((packagedAsset || ["file:", "data:", "blob:"].includes(url.protocol)) && !auth) {
          this.#localAssetRequests++;
          return;
        }
        this.#entries.push({
          ...slot,
          kind: "request",
          method: request.method(),
          origin: url.origin,
          pathname: url.pathname,
          authFp: auth ? this.fingerprint(auth) : null,
        });
      })().catch(() => {
        if (!this.#sealed) {
          if (metadata.headerState === "pending") metadata.headerState = "rejected";
          this.#failure ??= "renderer-network-unobservable";
        }
      });
      this.#pending.set(pending, metadata);
      // EventEmitter ignores this receipt; injected observers can await terminal bookkeeping.
      return pending.finally(() => {
        this.#pending.delete(pending);
        requests.delete(request);
      });
    });
    listen(page, "websocket", (socket) => {
      try {
        const url = endpoint(socket.url());
        const origin = url.origin;
        const ticket = url.searchParams.get("wsTicket");
        const slot = this.#reserve();
        if (!slot) return;
        if (!ticket) this.#failure ??= "renderer-network-unobservable";
        this.#entries.push({
          ...slot,
          kind: "websocket",
          origin,
          pathname: url.pathname,
          ticketFp: ticket ? this.fingerprint(ticket) : null,
        });
        for (const [event, direction] of [
          ["framesent", "sent"],
          ["framereceived", "received"],
        ]) {
          listen(socket, event, ({ payload }) => {
            const frameSlot = this.#reserve();
            if (!frameSlot) return;
            const frame = { ...frameSlot, kind: "frame", origin, direction };
            if (direction === "sent") {
              try {
                const value = JSON.parse(
                  typeof payload === "string" ? payload : payload.toString(),
                );
                if (value._tag === "Request") {
                  if (
                    typeof value.tag !== "string" ||
                    !/^[a-zA-Z0-9_.-]{1,128}$/.test(value.tag) ||
                    (value.payload?.type !== undefined &&
                      (typeof value.payload.type !== "string" ||
                        !/^[a-zA-Z0-9_.-]{1,128}$/.test(value.payload.type)))
                  ) {
                    this.#failure ??= "renderer-frame-unreadable";
                  } else {
                    frame.tag = value.tag;
                    frame.commandType = value.payload?.type ?? null;
                  }
                  frame.threadMatch = value.payload?.threadId === this.#threadId;
                }
              } catch {
                this.#failure ??= "renderer-frame-unreadable";
              }
            }
            this.#entries.push(frame);
          });
        }
        listen(socket, "close", () => {
          const closeSlot = this.#reserve();
          if (closeSlot) this.#entries.push({ ...closeSlot, kind: "close", origin });
        });
      } catch {
        if (!this.#sealed) this.#failure ??= "renderer-network-unobservable";
      }
    });
  }

  async flush() {
    if (this.#sealed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    let timer;
    try {
      const deadline = new Promise((_, reject) => {
        timer = setTimeout(() => {
          this.#incomplete = true;
          this.#pendingAtTimeout = this.#pending.size;
          if (this.#pendingDiagnosticsAtTimeout === null) {
            const now = performance.now();
            const samples = [];
            // Capture before timeout cleanup detaches the current page; retain no live references.
            for (const metadata of this.#pending.values()) {
              if (samples.length === 16) break;
              samples.push(
                Object.freeze({
                  sequence: metadata.sequence,
                  generation: metadata.generation,
                  originClass: metadata.originClass,
                  requestClass: metadata.requestClass,
                  pathTemplate: metadata.pathTemplate,
                  reloadTiming: this.#reloadTiming(metadata),
                  ageMs: Math.max(0, now - metadata.t),
                  headerState: metadata.headerState,
                  requestFinished: metadata.requestFinished,
                  requestFailed: metadata.requestFailed,
                  pageClosed: metadata.lifecycle.pageClosed,
                  detached: metadata.lifecycle.detached,
                }),
              );
            }
            this.#pendingDiagnosticsAtTimeout = Object.freeze({
              currentGeneration: this.#generation,
              samples: Object.freeze(samples),
              omittedCount: this.#pending.size - samples.length,
            });
          }
          this.#sealed = true;
          this.#failure ??= "renderer-trace-drain-timeout";
          this.detach();
          reject(new Error(this.#failure));
        }, this.#drainTimeoutMs);
      });
      // Include observations arriving during the drain without extending its deadline.
      while (this.#pending.size) await Promise.race([Promise.all(this.#pending.keys()), deadline]);
    } finally {
      clearTimeout(timer);
    }
    if (this.#disposed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    if (this.#failure) throw new Error(this.#failure);
  }

  async catalog(page, environmentId) {
    if (this.#sealed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    const generation = this.#generation;
    const value = await page.evaluate(
      async ({ keyBytes, environmentId, origin }) => {
        const key = await crypto.subtle.importKey(
          "raw",
          new Uint8Array(keyBytes),
          { name: "HMAC", hash: "SHA-256" },
          false,
          ["sign"],
        );
        const fp = async (text) =>
          Array.from(
            new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(text))),
          )
            .map((byte) => byte.toString(16).padStart(2, "0"))
            .join("")
            .slice(0, 16);
        const a = await window.desktopBridge.getLocalEnvironmentBearerToken();
        const raw = await window.desktopBridge.getConnectionCatalogSnapshot();
        if (typeof a !== "string" || !a || typeof raw !== "string")
          throw new Error("catalog-credential-unbound");
        const document = JSON.parse(raw);
        const id = `bearer:${environmentId}:${origin}`;
        const targets = document.targets.filter(
          (target) =>
            target.connectionId === id &&
            target.environmentId === environmentId &&
            target._tag === "BearerConnectionTarget",
        );
        const profiles = document.profiles.filter(
          (profile) =>
            profile.connectionId === id &&
            profile.environmentId === environmentId &&
            profile._tag === "BearerConnectionProfile" &&
            new URL(profile.httpBaseUrl).origin === origin &&
            new URL(profile.wsBaseUrl).host === new URL(origin).host &&
            new URL(profile.wsBaseUrl).protocol === "ws:",
        );
        const credentials = document.credentials.filter(
          (entry) =>
            entry.connectionId === id && entry.credential?._tag === "BearerConnectionCredential",
        );
        if (
          targets.length !== 1 ||
          profiles.length !== 1 ||
          credentials.length !== 1 ||
          typeof credentials[0].credential.token !== "string" ||
          !credentials[0].credential.token
        )
          throw new Error("catalog-credential-unbound");
        return {
          a: await fp(`Bearer ${a}`),
          b: await fp(`Bearer ${credentials[0].credential.token}`),
          target: true,
          profile: true,
          credential: true,
          disabled: document.disabledEnvironmentIds.includes(environmentId),
        };
      },
      { keyBytes: [...this.#key], environmentId, origin: B },
    );
    if (this.#sealed) throw new Error(this.#failure ?? "renderer-trace-disposed");
    if (generation !== this.#generation) throw new Error("catalog-generation-changed");
    const previous = this.#expected[generation];
    if (previous && (previous.a !== value.a || previous.b !== value.b))
      throw new Error("catalog-credential-changed");
    const first = this.#expected[0];
    if (first && first.b !== value.b) throw new Error("catalog-credential-changed");
    this.#expected[generation] = { a: value.a, b: value.b };
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
    return {
      count: this.#entries.length,
      localAssetRequests: this.#localAssetRequests,
      counts,
      fingerprints: structuredClone(this.#expected),
      failure: this.#failure,
      incomplete: this.#incomplete,
      pending: this.#pending.size,
      pendingAtTimeout: this.#pendingAtTimeout,
      drainTimeoutMs: this.#drainTimeoutMs,
      pendingDiagnosticsAtTimeout: structuredClone(this.#pendingDiagnosticsAtTimeout),
    };
  }
  detach() {
    for (const off of this.#detach.splice(0)) off();
  }
  dispose() {
    this.#sealed = true;
    this.#disposed = true;
    if (this.#pending.size) this.#incomplete = true;
    this.detach();
    this.#key.fill(0);
  }
}
