// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { publishAcpReplayStatus } from "../../../scripts/acpReplayStatusPublication.ts";
import {
  makeAcpReplayCompletenessAssertion,
  type AcpReplayTranscript,
} from "./AcpAdapterV2.testkit.ts";

const encodeJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeCheckpoint = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      cursor: Schema.Number,
      total: Schema.Number,
      failure: Schema.optional(Schema.Unknown),
    }),
  ),
);

const transcript: AcpReplayTranscript = {
  provider: ProviderDriverKind.make("acpRegistry"),
  protocol: "acp.ndjson-jsonrpc",
  version: "1",
  scenario: "atomic-status-publication",
  entries: [{ type: "runtime_exit", status: "success" }],
};
const scoped = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer), Effect.scoped);

it.effect("publishes a complete checkpoint and cleans only its temporary file", () =>
  scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-acp-status-publication-" });
      const statusPath = path.join(directory, "status.json");
      publishAcpReplayStatus(statusPath, { cursor: 0, total: 1 });
      publishAcpReplayStatus(statusPath, { cursor: 1, total: 1 });
      yield* makeAcpReplayCompletenessAssertion(fs, statusPath, transcript);
      assert.deepEqual(yield* fs.readDirectory(directory), ["status.json"]);
    }),
  ),
);

it.effect("cleans temporary publication state when the destination cannot be replaced", () =>
  scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-acp-status-failure-" });
      const statusPath = path.join(directory, "status-directory");
      yield* fs.makeDirectory(statusPath);
      assert.throws(() => publishAcpReplayStatus(statusPath, { cursor: 1, total: 1 }));
      assert.deepEqual(yield* fs.readDirectory(directory), ["status-directory"]);
    }),
  ),
);

it.effect.each([
  { label: "empty publication", raw: "", detail: "Failed to decode" },
  { label: "partial JSON", raw: '{"cursor":', detail: "Failed to decode" },
  { label: "malformed shape", raw: "{}", detail: "Failed to decode" },
  {
    label: "incomplete cursor",
    raw: '{"cursor":0,"total":1}',
    detail: "did not consume all frames",
  },
  { label: "wrong total", raw: '{"cursor":1,"total":2}', detail: "did not consume all frames" },
  {
    label: "recorded failure",
    raw: '{"cursor":1,"total":1,"failure":{"detail":"mismatch"}}',
    detail: "did not consume all frames",
  },
  {
    label: "null failure fact",
    raw: '{"cursor":1,"total":1,"failure":null}',
    detail: "did not consume all frames",
  },
])("rejects $label without retrying or fabricating completion", ({ raw, detail }) =>
  scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-acp-status-reader-" });
      const statusPath = path.join(directory, "status.json");
      yield* fs.writeFileString(statusPath, raw);
      const error = yield* makeAcpReplayCompletenessAssertion(fs, statusPath, transcript).pipe(
        Effect.flip,
      );
      assert.equal(error._tag, "AcpTransportError");
      if (error._tag !== "AcpTransportError")
        return assert.fail("Expected a replay transport error.");
      assert.include(error.detail, detail);
      assert.equal(yield* fs.readFileString(statusPath), raw);
    }),
  ),
);

// Pause the real writer after opening its final checkpoint but before writing
// any bytes. This deterministically exposes the old truncate-before-read race.
const publicationGate = String.raw`
const fs = require("node:fs");
const { syncBuiltinESMExports } = require("node:module");
const paths = new Map();
const open = fs.openSync;
const write = fs.writeFileSync;
const stdoutWrite = process.stdout.write.bind(process.stdout);
let finalAnswerSent = false;
fs.openSync = function(path, ...args) {
  const descriptor = open.call(this, path, ...args);
  paths.set(descriptor, String(path));
  return descriptor;
};
process.stdout.write = function(chunk, ...args) {
  try {
    const frame = JSON.parse(String(chunk));
    if (frame.id === 2 && Object.hasOwn(frame, "result")) finalAnswerSent = true;
  } catch {}
  return stdoutWrite(chunk, ...args);
};
fs.writeFileSync = function(target, data, ...args) {
  const path = typeof target === "number" ? paths.get(target) : String(target);
  let status;
  try { status = JSON.parse(String(data)); } catch {}
  if (path?.startsWith(process.env.T3_ACP_REPLAY_STATUS_PATH) && status?.cursor === 4) {
    write.call(this, target, "", ...args);
    process.send({ type: "publication-paused", finalAnswerSent });
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }
  return write.call(this, target, data, ...args);
};
syncBuiltinESMExports();
`;

it.effect("keeps a valid prior checkpoint and withholds the final answer during publication", () =>
  scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-acp-status-interrupt-" });
      const statusPath = path.join(directory, "status.json");
      const gatePath = path.join(directory, "publication-gate.cjs");
      const transcriptPath = path.join(directory, "transcript.json");
      yield* fs.writeFileString(gatePath, publicationGate);
      yield* fs.writeFileString(
        transcriptPath,
        encodeJsonString({
          scenario: "interrupted-final-publication",
          entries: [
            {
              type: "expect_outbound",
              frame: { kind: "request", method: "initialize", params: {} },
            },
            { type: "emit_inbound", frame: { kind: "response", method: "initialize", result: {} } },
            {
              type: "expect_outbound",
              frame: { kind: "request", method: "session/new", params: {} },
            },
            {
              type: "emit_inbound",
              frame: { kind: "response", method: "session/new", result: {} },
            },
          ],
        }),
      );
      const child = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeChildProcess.fork(
            NodeURL.fileURLToPath(new URL("../../../scripts/acp-replay-agent.ts", import.meta.url)),
            [],
            {
              cwd: directory,
              execArgv: ["--experimental-strip-types", "--require", gatePath],
              stdio: ["pipe", "pipe", "pipe", "ipc"],
              env: {
                T3_ACP_REPLAY_TRANSCRIPT_PATH: transcriptPath,
                T3_ACP_REPLAY_STATUS_PATH: statusPath,
              },
            },
          ),
        ),
        (child) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                if (child.exitCode !== null || child.signalCode !== null) return resolve();
                child.once("exit", () => resolve());
                child.kill("SIGTERM");
              }),
          ),
      );
      // Keep pipes drained; this fixture owns only this captured child and root.
      child.stdout!.resume();
      child.stderr!.resume();
      const paused = yield* Effect.promise(
        (signal) =>
          new Promise<{ finalAnswerSent: boolean }>((resolve, reject) => {
            const cleanup = () => {
              child.off("message", onMessage);
              child.off("error", onError);
              child.off("exit", onExit);
              signal.removeEventListener("abort", onAbort);
            };
            const onMessage = (message: unknown) => {
              if (
                typeof message !== "object" ||
                message === null ||
                Reflect.get(message, "type") !== "publication-paused"
              )
                return;
              cleanup();
              resolve({ finalAnswerSent: Reflect.get(message, "finalAnswerSent") === true });
            };
            const onError = (cause: Error) => {
              cleanup();
              reject(cause);
            };
            const onExit = () => {
              cleanup();
              reject(new Error("Replay exited before the publication gate."));
            };
            const onAbort = () => {
              cleanup();
              reject(new Error("Replay publication fixture cancelled."));
            };
            child.on("message", onMessage);
            child.once("error", onError);
            child.once("exit", onExit);
            signal.addEventListener("abort", onAbort, { once: true });
            child.stdin!.end(
              JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }) +
                "\n" +
                JSON.stringify({ jsonrpc: "2.0", id: 2, method: "session/new", params: {} }) +
                "\n",
            );
          }),
      );
      const prior = decodeCheckpoint(NodeFS.readFileSync(statusPath, "utf8"));
      assert.equal(prior.cursor, 3);
      assert.equal(prior.total, 4);
      assert.isUndefined(prior.failure);
      assert.isFalse(paused.finalAnswerSent);
    }),
  ),
);
