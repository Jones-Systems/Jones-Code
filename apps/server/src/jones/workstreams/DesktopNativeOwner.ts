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
  expectedParentPid = process.ppid,
) {
  if (inputFd !== 8 || outputFd !== 9 || process.ppid !== expectedParentPid) throw unavailable();
  for (const fd of [inputFd, outputFd]) {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFIFO() && !stat.isSocket()) throw unavailable();
  }
  admission.assertCurrent();
  const input = new NodeNet.Socket({ fd: inputFd, readable: true, writable: false });
  const output = new NodeNet.Socket({ fd: outputFd, readable: false, writable: true });
  const parentPid = expectedParentPid;
  let live = true;
  let runId: string | undefined;
  let sequence = 0;
  let lastReply = 0;
  let suspended = false;
  let buffer = "";
  let pending:
    | {
        sequence: number;
        nonce: string;
        resolve: () => void;
        reject: (error: Error) => void;
        promise: Promise<void>;
        expired: boolean;
        expiresAt: number;
      }
    | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  const transition = (state: "suspended" | "resumed" | "closed") => {
    process.stderr.write(`[jones/desktop-native-owner] ${state}\n`);
  };
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
    transition("closed");
  };
  const expirePending = () => {
    if (pending === undefined || pending.expired) return;
    if (deadline !== undefined) clearTimeout(deadline);
    deadline = undefined;
    if (runId === undefined) {
      close();
      return;
    }
    lastReply = 0;
    if (!suspended) {
      suspended = true;
      transition("suspended");
    }
    pending.expired = true;
    pending.reject(unavailable());
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
        if (performance.now() >= pending.expiresAt) expirePending();
        if (!live || pending === undefined) return;
        // The late response consumes only the expired challenge. It cannot qualify this child.
        if (pending.expired) {
          pending = undefined;
          continue;
        }
        runId = response.runId;
        lastReply = performance.now();
        if (suspended) {
          suspended = false;
          transition("resumed");
        }
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
    try {
      if (process.ppid !== parentPid) throw unavailable();
      admission.assertCurrent();
    } catch {
      close();
      return Promise.reject(unavailable());
    }
    // Keep one expired challenge until its exact reply drains; never accumulate retries.
    if (pending !== undefined) return pending.promise;
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
    pending = {
      sequence,
      nonce: request.nonce,
      resolve,
      reject,
      promise,
      expired: false,
      expiresAt: performance.now() + 3000,
    };
    deadline = NodeTimers.setTimeout(expirePending, 3000);
    output.write(`${JSON.stringify(request)}\n`, (error) => {
      if (error) close();
    });
    return promise;
  };
  heartbeat = NodeTimers.setInterval(() => {
    void refresh().catch(() => {
      if (!suspended) close();
    });
  }, 1000);
  heartbeat.unref();
  return {
    close,
    refresh,
    get suspended() {
      return live && suspended;
    },
    assertCurrent: () => {
      if (process.ppid !== parentPid) close();
      if (
        !live ||
        suspended ||
        runId === undefined ||
        lastReply === 0 ||
        performance.now() - lastReply > 3000
      )
        throw unavailable();
      try {
        admission.assertCurrent();
      } catch (cause) {
        close();
        throw cause;
      }
    },
  };
}

/** A valid trial may wait for admission; established proof may suspend, but a lost channel is never replaced. */
export function openDesktopNativeOwnerSession(input: {
  readonly home: string;
  readonly databasePath: string;
  readonly authorityDir: string;
  readonly version: string;
  readonly buildMetadata: unknown;
  readonly descriptorPath: string | undefined;
  readonly inputFd: number;
  readonly outputFd: number;
}) {
  if (input.authorityDir !== NodePath.join(input.home, "native-store-authority"))
    throw unavailable();
  const parentPid = process.ppid;
  if (input.inputFd !== 8 || input.outputFd !== 9) throw unavailable();
  for (const fd of [input.inputFd, input.outputFd]) {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFIFO() && !stat.isSocket()) throw unavailable();
  }
  // Validate the original startup admission before retaining a deliberately pending trial.
  holdJonesNativeWriterFence(input);
  let live = true;
  let admission: JonesNativeWriterAdmission | undefined;
  let channel: ReturnType<typeof openDesktopNativeOwnerChannel> | undefined;
  let pending: Promise<void> | undefined;
  const close = () => {
    if (!live) return;
    live = false;
    if (channel !== undefined) channel.close();
    else
      for (const fd of [input.inputFd, input.outputFd]) {
        try {
          NodeFS.closeSync(fd);
        } catch {
          /* The captured channel is already closed. */
        }
      }
  };
  const refresh = (): Promise<void> => {
    if (!live || process.ppid !== parentPid) {
      close();
      return Promise.reject(unavailable());
    }
    if (pending !== undefined) return pending;
    const attempt = async () => {
      if (channel === undefined) {
        let ordinary: JonesNativeWriterAdmission | undefined;
        try {
          ordinary = holdJonesNativeWriterFence({ ...input, descriptorPath: undefined });
        } catch (cause) {
          if (input.descriptorPath !== undefined) {
            try {
              // Only a still-valid trial is pending. Missing, malformed or stale admission is terminal.
              holdJonesNativeWriterFence(input);
            } catch {
              close();
            }
          } else close();
          throw cause;
        }
        if (ordinary === undefined) {
          close();
          throw unavailable();
        }
        try {
          admission = ordinary;
          channel = openDesktopNativeOwnerChannel(
            ordinary,
            input.inputFd,
            input.outputFd,
            parentPid,
          );
        } catch (cause) {
          close();
          throw cause;
        }
      }
      try {
        await channel.refresh();
      } catch (cause) {
        if (!channel.suspended) close();
        throw cause;
      }
    };
    pending = attempt().finally(() => {
      pending = undefined;
    });
    return pending;
  };
  return {
    get available() {
      return live && channel !== undefined && !channel.suspended;
    },
    get binding() {
      return live && channel !== undefined ? admission?.binding : undefined;
    },
    assertCurrent: () => {
      if (!live || channel === undefined || process.ppid !== parentPid) throw unavailable();
      channel.assertCurrent();
    },
    refresh,
    close,
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
  const session = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        openDesktopNativeOwnerSession({
          home: config.baseDir,
          databasePath: config.dbPath,
          authorityDir: config.authorityStateDir,
          version: packageJson.version,
          buildMetadata: packageJson,
          descriptorPath: process.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
          inputFd: config.desktopNativeOwnerInputFd!,
          outputFd: config.desktopNativeOwnerOutputFd!,
        }),
      catch: () => unavailable(),
    }),
    (value) => Effect.sync(value.close),
  );
  const refresh = Effect.tryPromise({ try: session.refresh, catch: () => unavailable() });
  if (process.env.T3CODE_JONES_TRIAL_DESCRIPTOR === undefined) yield* refresh;
  return DesktopNativeOwner.of({
    get available() {
      return session.available;
    },
    get binding() {
      return session.binding;
    },
    assertCurrent: session.assertCurrent,
    refresh,
  });
});

export const layer = Layer.effect(DesktopNativeOwner, make);
