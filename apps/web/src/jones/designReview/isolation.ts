/**
 * Design reviews mount real product components in an opaque-origin attachment. Product modules
 * read storage and the desktop bridge while they evaluate, and their hooks can reach the network,
 * so an entry installs this before it imports any of them.
 */

export interface BlockedEffect {
  readonly kind: string;
  readonly target: string;
}

export interface IsolationReport {
  readonly ok: boolean;
  readonly failures: ReadonlyArray<string>;
}

const blockedListeners = new Set<() => void>();
let blockedEffects: ReadonlyArray<BlockedEffect> = [];
let report: IsolationReport = { ok: false, failures: ["Isolation was not installed."] };

function recordBlocked(kind: string, target: unknown): void {
  blockedEffects = [...blockedEffects, { kind, target: describeTarget(target) }];
  for (const listener of blockedListeners) listener();
}

function describeTarget(target: unknown): string {
  if (typeof target === "string") return target;
  if (target instanceof URL) return target.href;
  if (typeof Request !== "undefined" && target instanceof Request) return target.url;
  return String(target);
}

export function subscribeBlockedEffects(listener: () => void): () => void {
  blockedListeners.add(listener);
  return () => blockedListeners.delete(listener);
}

export function blockedEffectsSnapshot(): ReadonlyArray<BlockedEffect> {
  return blockedEffects;
}

export function isolationReport(): IsolationReport {
  return report;
}

function createMemoryStorage(): Storage {
  const entries = new Map<string, string>();
  const storage = {
    get length() {
      return entries.size;
    },
    clear: () => entries.clear(),
    getItem: (key: string) => entries.get(String(key)) ?? null,
    key: (index: number) => [...entries.keys()][index] ?? null,
    removeItem: (key: string) => {
      entries.delete(String(key));
    },
    setItem: (key: string, value: string) => {
      entries.set(String(key), String(value));
    },
  };
  return storage as Storage;
}

function blockedError(kind: string): DOMException {
  return new DOMException(`${kind} is disabled in this design review.`, "SecurityError");
}

function blockedConstructor(kind: string) {
  return function BlockedConnection(target: unknown) {
    recordBlocked(kind, target);
    throw blockedError(kind);
  };
}

/**
 * Replaces page storage with memory, hides any desktop bridge, and turns script-initiated network
 * and window effects into recorded failures. Static assets the page markup loads are the host
 * page's concern; its content security policy covers them.
 */
export function installDesignReviewIsolation(target: Window = window): IsolationReport {
  const failures: string[] = [];
  const define = (owner: object, name: string, descriptor: PropertyDescriptor, label: string) => {
    try {
      Object.defineProperty(owner, name, { configurable: true, ...descriptor });
    } catch {
      failures.push(`Could not replace ${label}.`);
    }
  };

  define(target, "localStorage", { value: createMemoryStorage() }, "localStorage");
  define(target, "sessionStorage", { value: createMemoryStorage() }, "sessionStorage");
  define(target, "indexedDB", { value: undefined }, "indexedDB");
  define(target.document, "cookie", { get: () => "", set: () => undefined }, "document.cookie");
  define(target, "desktopBridge", { value: undefined, writable: false }, "the desktop bridge");
  define(
    target,
    "threadMonitor",
    { value: undefined, writable: false },
    "the thread monitor bridge",
  );

  define(
    target,
    "fetch",
    {
      value: (input: unknown) => {
        recordBlocked("fetch", input);
        return Promise.reject(blockedError("Network access"));
      },
      writable: true,
    },
    "fetch",
  );
  for (const name of ["WebSocket", "EventSource", "XMLHttpRequest"] as const) {
    define(target, name, { value: blockedConstructor(name), writable: true }, name);
  }
  define(
    target.navigator,
    "sendBeacon",
    {
      value: (url: unknown) => {
        recordBlocked("sendBeacon", url);
        return false;
      },
    },
    "navigator.sendBeacon",
  );
  define(
    target,
    "open",
    {
      value: (url: unknown) => {
        recordBlocked("window.open", url ?? "");
        return null;
      },
      writable: true,
    },
    "window.open",
  );

  if (target.desktopBridge !== undefined) failures.push("A desktop bridge is still visible.");
  try {
    target.localStorage.setItem("jones:design-review:probe", "1");
    target.localStorage.removeItem("jones:design-review:probe");
  } catch {
    failures.push("Memory storage is not reachable.");
  }

  report = { ok: failures.length === 0, failures };
  return report;
}

/** Plain DOM, so it works before React or any product module loads. */
export function renderReviewTextFallback(container: HTMLElement, message: string): void {
  const paragraph = container.ownerDocument.createElement("p");
  paragraph.setAttribute("role", "alert");
  paragraph.style.cssText = "margin:16px;font:14px/1.5 system-ui,sans-serif;white-space:pre-wrap";
  paragraph.textContent = message;
  container.replaceChildren(paragraph);
}

export const DESIGN_REVIEW_MOUNTED_ATTRIBUTE = "data-design-review-mounted";

/** Shows a startup failure as text when the page script fails before the review mounts. */
export function reportStartupFailures(target: Window = window): void {
  target.addEventListener("error", (event) => {
    const container = target.document.getElementById("root");
    if (container === null || container.hasAttribute(DESIGN_REVIEW_MOUNTED_ATTRIBUTE)) return;
    const reason = event.error instanceof Error ? event.error.message : event.message;
    renderReviewTextFallback(container, `This design review could not start.\n${reason}`);
  });
}
