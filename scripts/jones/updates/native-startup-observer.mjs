import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import path from "node:path";
import { FixtureOwnership } from "./native-startup-fixture.mjs";

const historyLost = 0x01 | 0x02 | 0x04 | 0x08 | 0x20 | 0x40 | 0x80;
const mutation = 0x100 | 0x200 | 0x400 | 0x800 | 0x1000 | 0x2000 | 0x4000 | 0x8000;

export function protectedMutations(events, protectedPaths) {
  assert.ok(
    events.every((event) => (event.flags & historyLost) === 0),
    "FSEvents lost history or a watched root changed; qualification is unknown.",
  );
  return events.filter(
    (event) =>
      (event.flags & mutation) !== 0 &&
      protectedPaths.some((root) => event.path === root || event.path.startsWith(root + path.sep)),
  );
}

export async function startObserver(executable, roots, signal) {
  const child = spawn(executable, roots, { stdio: ["pipe", "pipe", "pipe"] });
  const events = [];
  const messages = [];
  const waiters = new Set();
  let buffer = "";
  let stderr = "";
  let failure;
  let stopping = false;
  let closed = false;
  const stop = (error) => {
    failure ??= error;
    for (const waiter of waiters) waiter.reject(failure);
    waiters.clear();
    if (!closed) child.kill("SIGKILL");
  };
  const cancel = () => stop(new Error("Native startup observer cancelled."));
  const completion = new Promise((resolve) =>
    child.once("close", (code) => {
      closed = true;
      if (!stopping || code !== 0)
        stop(
          new Error(
            `Native startup observer exited unexpectedly (${code}): ${stderr.slice(0, 1024)}`,
          ),
        );
      resolve();
    }),
  );
  signal?.addEventListener("abort", cancel, { once: true });
  if (signal?.aborted) cancel();
  child.once("error", stop);
  child.stdin.on("error", stop);
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
    if (stderr.length > 65536) stop(new Error("Observer stderr exceeded its bound."));
  });
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    if (buffer.length > 1024 * 1024) return stop(new Error("Observer output exceeded its bound."));
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      try {
        const message = JSON.parse(line);
        if (message.type === "event") {
          assert.equal(typeof message.path, "string");
          assert.ok(Number.isInteger(message.flags));
          events.push(message);
          if (events.length > 10000) throw new Error("Observer event history exceeded its bound.");
        } else {
          messages.push(message);
          if (messages.length > 50)
            throw new Error("Observer handshake history exceeded its bound.");
          for (const waiter of waiters)
            if (waiter.matches(message)) {
              waiter.resolve(message);
              waiters.delete(waiter);
            }
        }
      } catch (error) {
        stop(error);
      }
    }
  });
  const wait = (matches) =>
    new Promise((resolve, reject) => {
      if (failure) return reject(failure);
      const prior = messages.find(matches);
      if (prior) return resolve(prior);
      const timer = setTimeout(() => stop(new Error("Observer handshake timed out.")), 5000);
      const waiter = {
        matches,
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      };
      waiters.add(waiter);
    });
  const close = async () => {
    stopping = true;
    signal?.removeEventListener("abort", cancel);
    if (!closed) child.stdin.end("quit\n");
    const timer = setTimeout(() => stop(new Error("Observer cleanup timed out.")), 5000);
    try {
      await completion;
    } finally {
      clearTimeout(timer);
    }
    if (failure) throw failure;
  };
  const flush = async () => {
    const token = randomUUID();
    const acknowledgement = wait(
      (message) => message.type === "flushed" && message.token === token,
    );
    child.stdin.write(`flush:${token}\n`);
    await acknowledgement;
    protectedMutations(events, []);
    return events.length;
  };
  try {
    await wait((message) => message.type === "ready");
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }
  return { events, flush, close };
}

export async function observerControl(observer, roots) {
  const offset = await observer.flush();
  const names = [];
  const ownership = new FixtureOwnership();
  try {
    for (const root of roots) {
      const directory = await ownership.temporary(path.join(root, ".jones-observer-control-"));
      const name = path.join(directory, "sentinel");
      names.push(name, `${name}.renamed`);
      await fs.writeFile(name, "created", { flag: "wx", mode: 0o600 });
      await fs.appendFile(name, "-changed");
      await fs.rename(name, `${name}.renamed`);
      await fs.unlink(`${name}.renamed`);
    }
    await observer.flush();
    const observed = observer.events.slice(offset);
    for (let index = 0; index < names.length; index += 2) {
      assert.ok(
        observed.some(
          (event) =>
            (event.flags & mutation) !== 0 &&
            (event.path === names[index] || event.path === names[index + 1]),
        ),
        "Observer missed a transient positive control.",
      );
    }
  } finally {
    await ownership.cleanup();
  }
}
