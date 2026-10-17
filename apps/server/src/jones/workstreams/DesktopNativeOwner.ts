// @effect-diagnostics nodeBuiltinImport:off -- Only inherited private pipes carry desktop owner proof.
// @effect-diagnostics globalTimers:off -- These timers are owned and cleared by the native inherited-pipe lifecycle.
import * as NodeTimers from "node:timers";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import packageJson from "../../../package.json" with { type: "json" };
import {
  holdJonesNativeWriterFence,
  type JonesNativeWriterAdmission,
} from "@t3tools/shared/jones/nativeWriterFence";
import {
  decodeDesktopNativeOwnerMessage,
  sameDesktopNativeOwnerBinding,
  DESKTOP_NATIVE_OWNER_MAX_BYTES,
} from "@t3tools/shared/jones/desktopNativeOwner";
import * as ServerConfig from "../../config.ts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";

const unavailable = () =>
  new NativeStoreAuthorityPersistenceError(
    "launcher_upgrade_required",
    "A live admitted desktop owner is required.",
  );
export const DesktopNativeOwner = Context.Reference<{
  readonly available: boolean;
  readonly binding: JonesNativeWriterAdmission["binding"] | undefined;
  readonly assertCurrent: () => void;
  readonly refresh: Effect.Effect<void, NativeStoreAuthorityPersistenceError>;
}>("jones/workstreams/DesktopNativeOwner", {
  defaultValue: () => ({
    available: false,
    binding: undefined,
    assertCurrent: () => {
      throw unavailable();
    },
    refresh: Effect.fail(unavailable()),
  }),
});

/** Owns only the descriptors inherited for this actual child; disconnect never reconnects or adopts. */
export function openDesktopNativeOwnerChannel(
  admission: JonesNativeWriterAdmission,
  inputFd: number,
  outputFd: number,
) {
  if (inputFd !== 8 || outputFd !== 9) throw unavailable();
  for (const fd of [inputFd, outputFd]) {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFIFO() && !stat.isSocket()) throw unavailable();
  }
  admission.assertCurrent();
  const input = new NodeNet.Socket({ fd: inputFd, readable: true, writable: false });
  const output = new NodeNet.Socket({ fd: outputFd, readable: false, writable: true });
  const parentPid = process.ppid;
  let live = true;
  let runId: string | undefined;
  let sequence = 0;
  let lastReply = 0;
  let buffer = "";
  let pending:
    | {
        sequence: number;
        nonce: string;
        resolve: () => void;
        reject: (error: Error) => void;
        promise: Promise<void>;
      }
    | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  const close = () => {
    if (!live) return;
    live = false;
    lastReply = 0;
    if (deadline !== undefined) clearTimeout(deadline);
    if (heartbeat !== undefined) clearInterval(heartbeat);
    const failed = pending;
    pending = undefined;
    failed?.reject(unavailable());
    input.destroy();
    output.destroy();
  };
  input.on("error", close);
  input.on("end", close);
  input.on("close", close);
  output.on("error", close);
  output.on("close", close);
  input.setEncoding("utf8");
  input.on("data", (chunk: string | Buffer) => {
    try {
      buffer += String(chunk);
      if (Buffer.byteLength(buffer) > DESKTOP_NATIVE_OWNER_MAX_BYTES) throw unavailable();
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const response = decodeDesktopNativeOwnerMessage(buffer.slice(0, newline), true);
        buffer = buffer.slice(newline + 1);
        if (
          !live ||
          pending === undefined ||
          response.sequence !== pending.sequence ||
          response.nonce !== pending.nonce ||
          response.childPid !== process.pid ||
          response.parentPid !== parentPid ||
          process.ppid !== parentPid ||
          (runId !== undefined && response.runId !== runId) ||
          !sameDesktopNativeOwnerBinding(response.binding, admission.binding)
        )
          throw unavailable();
        admission.assertCurrent();
        runId = response.runId;
        lastReply = performance.now();
        if (deadline !== undefined) clearTimeout(deadline);
        const complete = pending;
        pending = undefined;
        complete.resolve();
      }
    } catch {
      close();
    }
  });
  const refresh = (): Promise<void> => {
    if (!live) return Promise.reject(unavailable());
    if (pending !== undefined) return pending.promise;
    try {
      admission.assertCurrent();
    } catch {
      close();
      return Promise.reject(unavailable());
    }
    let resolve!: () => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<void>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    const request = {
      protocol: 1,
      sequence: ++sequence,
      nonce: NodeCrypto.randomBytes(32).toString("hex"),
      childPid: process.pid,
      binding: admission.binding,
    };
    pending = { sequence, nonce: request.nonce, resolve, reject, promise };
    deadline = NodeTimers.setTimeout(close, 3000);
    output.write(`${JSON.stringify(request)}\n`, (error) => {
      if (error) close();
    });
    return promise;
  };
  heartbeat = NodeTimers.setInterval(() => {
    void refresh().catch(close);
  }, 1000);
  heartbeat.unref();
  return {
    close,
    refresh,
    assertCurrent: () => {
      if (
        !live ||
        process.ppid !== parentPid ||
        runId === undefined ||
        lastReply === 0 ||
        performance.now() - lastReply > 3000
      )
        throw unavailable();
      admission.assertCurrent();
    },
  };
}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  if (
    config.mode !== "desktop" ||
    config.desktopNativeOwnerInputFd === undefined ||
    config.desktopNativeOwnerOutputFd === undefined
  )
    return DesktopNativeOwner.of({
      available: false,
      binding: undefined,
      assertCurrent: () => {
        throw unavailable();
      },
      refresh: Effect.fail(unavailable()),
    });
  const channel = yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        if (
          config.authorityStateDir !== NodePath.join(config.baseDir, "native-store-authority") ||
          process.env.T3CODE_JONES_TRIAL_DESCRIPTOR !== undefined
        )
          throw unavailable();
        const admission = holdJonesNativeWriterFence({
          home: config.baseDir,
          databasePath: config.dbPath,
          version: packageJson.version,
          buildMetadata: packageJson,
        });
        if (admission === undefined) throw unavailable();
        return {
          admission,
          channel: openDesktopNativeOwnerChannel(
            admission,
            config.desktopNativeOwnerInputFd!,
            config.desktopNativeOwnerOutputFd!,
          ),
        };
      },
      catch: () => unavailable(),
    }),
    (value) => Effect.sync(() => value.channel.close()),
  );
  const refresh = Effect.tryPromise({ try: channel.channel.refresh, catch: () => unavailable() });
  yield* refresh;
  return DesktopNativeOwner.of({
    available: true,
    binding: channel.admission.binding,
    assertCurrent: channel.channel.assertCurrent,
    refresh,
  });
});

export const layer = Layer.effect(DesktopNativeOwner, make);
