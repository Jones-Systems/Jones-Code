// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - native HTTP sockets use cleanup-owned wall-clock deadlines; validation captures Effect context at acquisition.
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { DeviceHostError } from "./DeviceHost.ts";
import {
  DeviceDirectGrants,
  DeviceDirectAdmissionInput,
  type DirectAdmissionBinding,
} from "./DeviceDirectGrants.ts";

const decodeAdmission = Schema.decodeUnknownEffect(
  Schema.fromJsonString(DeviceDirectAdmissionInput),
);

/** Owns only validation sockets and fibers for one SSH generation. */
export const openDirectAdmission = Effect.fn("DeviceDirectAdmission.open")(function* (
  binding: DirectAdmissionBinding,
) {
  const grants = yield* DeviceDirectGrants;
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);
  const fibers = new Set<Fiber.Fiber<unknown, unknown>>();
  const sockets = new Set<NodeNet.Socket>();
  const lifecycle = new AbortController();
  let active = true;
  const server = NodeHttp.createServer({ maxHeaderSize: 8192 }, (req, res) => {
    let finished = false;
    let fiber: Fiber.Fiber<unknown, unknown> | undefined;
    const reply = (status: number, value?: unknown) => {
      if (finished || !active || res.destroyed) return;
      finished = true;
      clearTimeout(deadline);
      const body = value === undefined ? "" : JSON.stringify(value);
      res.writeHead(Buffer.byteLength(body) <= 2048 ? status : 503, {
        "cache-control": "no-store",
        connection: "close",
        "content-type": "application/json",
      });
      res.end(Buffer.byteLength(body) <= 2048 ? body : "");
      req.resume();
    };
    const deadline = setTimeout(() => {
      reply(400);
      fiber?.interruptUnsafe();
    }, 5000);
    res.once("close", () => {
      finished = true;
      clearTimeout(deadline);
      fiber?.interruptUnsafe();
    });
    req.once("error", () => {
      clearTimeout(deadline);
      fiber?.interruptUnsafe();
      res.destroy();
    });
    if (req.url !== "/api/device-hub/direct-admission") return reply(404);
    if (req.method !== "POST" || req.headers.upgrade !== undefined) return reply(405);
    if (
      req.headers["content-encoding"] !== undefined ||
      !/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(req.headers["content-type"] ?? "")
    )
      return reply(400);
    const headerBytes = req.rawHeaders.reduce(
      (size, header) => size + Buffer.byteLength(header) + 2,
      0,
    );
    if (headerBytes > 8192) return reply(413);
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      if (finished) return;
      size += chunk.length;
      if (size > 8192) {
        reply(413);
        return;
      }
      chunks.push(chunk);
    });
    req.once("end", () => {
      if (finished || !active) return;
      const validation = Effect.gen(function* () {
        const input = yield* decodeAdmission(Buffer.concat(chunks).toString("utf8")).pipe(
          Effect.result,
        );
        if (input._tag === "Failure") {
          reply(400);
          return;
        }
        const verdict = yield* grants.admit(input.success, binding).pipe(Effect.result);
        if (verdict._tag === "Failure") {
          reply(503);
          return;
        }
        if (verdict.success._tag === "Denied") {
          reply(403);
          return;
        }
        const { allowed, expiresAt, owner, generation, origin } = verdict.success;
        reply(200, { allowed, expiresAt, owner, generation, origin });
      });
      fiber = runFork(validation);
      fibers.add(fiber);
      const owned = fiber;
      owned.addObserver(() => {
        fibers.delete(owned);
        if (!finished && active) reply(503);
      });
    });
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    const deadline = setTimeout(() => socket.destroy(), 5000);
    socket.once("close", () => {
      clearTimeout(deadline);
      sockets.delete(socket);
    });
  });
  server.on("upgrade", (_req, socket) =>
    socket.end(
      "HTTP/1.1 405 Method Not Allowed\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
    ),
  );
  server.on("connect", (_req, socket) =>
    socket.end(
      "HTTP/1.1 405 Method Not Allowed\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
    ),
  );
  server.on("clientError", (_error, socket) =>
    socket.end(
      "HTTP/1.1 413 Payload Too Large\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
    ),
  );
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      active = false;
      const ownedFibers = [...fibers];
      lifecycle.abort();
      for (const socket of sockets) socket.destroy();
      yield* Fiber.interruptAll(ownedFibers);
      yield* Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve())));
    }),
  );
  yield* Effect.callback<void, DeviceHostError>((resume) => {
    const failed = (cause: Error) =>
      resume(
        Effect.fail(
          new DeviceHostError({ hostId: binding.hostId, step: "binding direct admission", cause }),
        ),
      );
    server.once("error", failed);
    server.listen({ port: 0, host: "127.0.0.1", signal: lifecycle.signal }, () => {
      server.removeListener("error", failed);
      resume(Effect.void);
    });
    return Effect.sync(() => {
      server.removeListener("error", failed);
    });
  });
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* new DeviceHostError({
      hostId: binding.hostId,
      step: "binding direct admission",
      cause: new Error("Missing private listener port."),
    });
  return { port: address.port };
});
