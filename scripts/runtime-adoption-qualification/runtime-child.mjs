import * as NodeAssert from "node:assert/strict";
import * as NodeNet from "node:net";

import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError } from "effect/unstable/sql/SqlError";

import {
  decodeServiceLauncherContext,
  decodeServiceLauncherParentMessage,
  SERVICE_LAUNCHER_CONTEXT_ENV,
} from "../../apps/server/src/cloud/serviceProtocol.ts";
import { readFixture, withDatabase } from "./fixture.mjs";

export async function runRuntimeChild({ fixture, mode, receiptPort, receiptToken }) {
  const context = decodeServiceLauncherContext(process.env[SERVICE_LAUNCHER_CONTEXT_ENV]);
  NodeAssert.ok(context, "synthetic runtime requires real launcher context");
  NodeAssert.equal(process.argv[2], "serve");
  NodeAssert.equal(process.env.T3CODE_HOME, fixture.baseDir);
  const socket = NodeNet.createConnection({ host: "127.0.0.1", port: receiptPort });
  await new Promise((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  const receipt = (type, fields = {}) =>
    new Promise((resolve, reject) => {
      socket.write(
        `${JSON.stringify({ token: receiptToken, type, version: context.childVersion, pid: process.pid, ...fields })}\n`,
        (error) => (error ? reject(error) : resolve()),
      );
    });
  let stopping;
  const stop = (code) => {
    stopping ??= (async () => {
      await receipt("child-closing", { code });
      socket.end();
      if (process.connected) process.disconnect();
      process.exitCode = code;
    })();
    return stopping;
  };
  process.once("SIGTERM", () => void stop(0));
  process.once("SIGINT", () => void stop(0));
  socket.on("error", () => {
    process.exitCode = 1;
    if (process.connected) process.disconnect();
  });
  const committed = Promise.withResolvers();
  process.on("message", (value) => {
    const message = decodeServiceLauncherParentMessage(value);
    if (message?.type === "committed") committed.resolve(message);
  });
  try {
    if (context.childVersion === "1.0.0") {
      const snapshot = await readFixture(fixture);
      const schema = await withDatabase(
        fixture,
        {},
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql`PRAGMA table_info(projection_threads)`;
        }),
      );
      await receipt("prior-readback", { snapshot, schema });
      return;
    }
    NodeAssert.equal(context.childVersion, "1.1.0");
    NodeAssert.equal(context.update?.status, "pending");
    NodeAssert.equal(context.update.phase, "trial-ready");
    try {
      await withDatabase(
        fixture,
        { startup: true },
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT auto_settle_disabled_at FROM projection_threads`;
          yield* sql`SELECT resource_path FROM worktree_ownership_leases`;
        }),
      );
    } catch (cause) {
      const sqlFailure = isSqlError(cause)
        ? {
            tag: cause._tag,
            reason: {
              tag: cause.reason._tag,
              operation: cause.reason.operation,
              message: cause.reason.message,
              cause: {
                name: cause.reason.cause?.name,
                message: cause.reason.cause?.message,
                code: cause.reason.cause?.code,
                errcode: cause.reason.cause?.errcode,
                errstr: cause.reason.cause?.errstr,
              },
            },
          }
        : null;
      await receipt("migration-failed", { error: String(cause), sqlFailure });
      await stop(23);
      return;
    }
    await receipt("migrated", { snapshot: await readFixture(fixture) });
    if (mode === "exit-after-migration") {
      await stop(23);
      return;
    }
    if (mode === "hold-before-prepared") return;
    const updateId = mode === "wrong-prepared" ? `${context.update.id}-wrong` : context.update.id;
    await new Promise((resolve, reject) =>
      process.send({ type: "prepared", updateId }, (error) => (error ? reject(error) : resolve())),
    );
    await receipt("prepared", { updateId });
    if (mode === "wrong-prepared") return;
    const message = await committed.promise;
    NodeAssert.equal(message.updateId, context.update.id);
    await receipt("committed", {
      updateId: message.updateId,
      snapshot: await readFixture(fixture),
    });
  } catch (cause) {
    await receipt("child-error", { error: String(cause) });
    await stop(1);
  }
}
