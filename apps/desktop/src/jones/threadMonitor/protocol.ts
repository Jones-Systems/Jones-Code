// @effect-diagnostics nodeBuiltinImport:off - Native session adapter serves only local, confined static assets.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { Session } from "electron";
import { net } from "electron";
import { MONITOR_ENTRY } from "./channels.ts";

export interface MonitorAssetSource {
  readonly scheme: string;
  readonly assetDirectory: string;
  readonly developmentOrigin: URL | null;
}
export const monitorCsp = (development: boolean): string =>
  [
    "default-src 'none'",
    `script-src 'self'${development ? " 'unsafe-inline'" : ""}`,
    "style-src 'self' 'unsafe-inline'",
    "font-src 'self' data:",
    "img-src 'self' data:",
    "connect-src 'none'",
    "frame-src 'none'",
    "object-src 'none'",
    "worker-src 'none'",
    "base-uri 'none'",
    "form-action 'none'",
    "frame-ancestors 'none'",
  ].join("; ");

const contentType = (pathname: string): string => {
  switch (NodePath.extname(pathname)) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
    case ".mjs":
    case ".ts":
    case ".tsx":
      return "text/javascript; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".woff":
      return "font/woff";
    case ".woff2":
      return "font/woff2";
    case ".png":
      return "image/png";
    default:
      return "application/octet-stream";
  }
};

/** Protocol handlers are session-local: never borrow the authenticated default session. */
export function registerMonitorProtocol(session: Session, source: MonitorAssetSource): () => void {
  if (
    source.developmentOrigin &&
    (!["http:", "https:"].includes(source.developmentOrigin.protocol) ||
      !["localhost", "127.0.0.1", "[::1]"].includes(source.developmentOrigin.hostname) ||
      source.developmentOrigin.username ||
      source.developmentOrigin.password ||
      source.developmentOrigin.pathname !== "/" ||
      source.developmentOrigin.search ||
      source.developmentOrigin.hash)
  ) {
    throw new Error("Monitor development assets require a fixed loopback Vite origin");
  }
  const origin = `${source.scheme}://app`;
  const csp = monitorCsp(source.developmentOrigin !== null);
  session.setPermissionRequestHandler((_contents, _permission, callback) => callback(false));
  session.setPermissionCheckHandler(() => false);
  session.setDevicePermissionHandler(() => false);
  session.on("will-download", preventDownload);
  session.webRequest.onBeforeRequest((details, callback) => {
    let allowed = false;
    try {
      const url = new URL(details.url);
      allowed =
        `${url.protocol}//${url.host}` === origin &&
        !url.username &&
        !url.password &&
        (details.resourceType !== "mainFrame" || url.pathname === MONITOR_ENTRY) &&
        details.resourceType !== "subFrame" &&
        details.resourceType !== "object";
    } catch {
      /* Invalid URLs are denied. */
    }
    callback({ cancel: !allowed });
  });
  session.protocol.handle(source.scheme, async (request) => {
    const url = new URL(request.url);
    if (
      `${url.protocol}//${url.host}` !== origin ||
      (request.method !== "GET" && request.method !== "HEAD")
    )
      return new Response(null, { status: 403 });
    let pathname: string;
    try {
      pathname = decodeURIComponent(url.pathname);
    } catch {
      return new Response(null, { status: 400 });
    }
    if (pathname.includes("\0") || pathname.includes("\\") || pathname.split("/").includes(".."))
      return new Response(null, { status: 400 });
    if (pathname.endsWith(".html") && pathname !== MONITOR_ENTRY)
      return new Response(null, { status: 403 });
    let response: Response;
    if (source.developmentOrigin) {
      const target = new URL(source.developmentOrigin.href);
      target.pathname = pathname;
      target.search = url.search;
      target.hash = "";
      // A fixed Vite origin is the only native proxy destination; peer headers/cookies are never relayed.
      response = await net.fetch(target.href, {
        method: request.method,
        redirect: "error",
        credentials: "omit",
        useSessionCookies: false,
      } as RequestInit);
    } else {
      try {
        const root = await NodeFSP.realpath(source.assetDirectory);
        const file = await NodeFSP.realpath(NodePath.resolve(root, `.${pathname}`));
        if (!file.startsWith(root + NodePath.sep)) return new Response(null, { status: 403 });
        const data = await NodeFSP.readFile(file);
        response = new Response(request.method === "HEAD" ? null : new Uint8Array(data), {
          headers: { "Content-Type": contentType(file) },
        });
      } catch {
        return new Response(null, { status: 404 });
      }
    }
    const headers = new Headers(response.headers);
    headers.set("Content-Security-Policy", csp);
    headers.set("X-Content-Type-Options", "nosniff");
    headers.set("Cache-Control", "no-store");
    headers.delete("set-cookie");
    return new Response(response.body, { status: response.status, headers });
  });
  return () => {
    session.protocol.unhandle(source.scheme);
    session.webRequest.onBeforeRequest(null);
    session.setPermissionRequestHandler(null);
    session.setPermissionCheckHandler(null);
    session.setDevicePermissionHandler(null);
    session.removeListener("will-download", preventDownload);
  };
}
function preventDownload(event: Electron.Event): void {
  event.preventDefault();
}
