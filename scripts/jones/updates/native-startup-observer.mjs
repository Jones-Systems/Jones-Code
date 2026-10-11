import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
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
          let matched = false;
          for (const waiter of waiters)
            if (waiter.matches(message)) {
              matched = true;
              waiter.resolve(message);
              waiters.delete(waiter);
            }
          if (!matched) {
            messages.push(message);
            if (messages.length > 50)
              throw new Error("Observer unmatched handshakes exceeded their bound.");
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
      const prior = messages.findIndex(matches);
      if (prior !== -1) return resolve(messages.splice(prior, 1)[0]);
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

export class ObserverControlError extends Error {
  constructor(phase, root, names, observed, cause) {
    super(`Observer positive control failed during ${phase}: ${String(cause).slice(0, 512)}`, {
      cause,
    });
    this.name = "ObserverControlError";
    this.control = {
      phase,
      root: root.slice(0, 1024),
      paths: names.map((name) => path.relative(root, name).slice(0, 512)),
      observedCount: observed.length,
      events: observed.slice(-12).map((event) => ({
        path: path.relative(root, event.path).slice(0, 512),
        flags: event.flags,
      })),
    };
  }
}

export async function observerControl(observer, roots, { timeoutMs = 5000 } = {}) {
  const ownership = new FixtureOwnership();
  const observe = async (phase, root, names, flags, offset) => {
    const deadline = performance.now() + timeoutMs;
    try {
      for (;;) {
        // Flush acknowledges queued callbacks. The kernel may not yet have
        // delivered this phase, so wait for the exact leaf event within a bound.
        await observer.flush();
        protectedMutations(observer.events, []);
        if (
          observer.events
            .slice(offset)
            .some((event) => names.includes(event.path) && (event.flags & flags) !== 0)
        )
          return;
        assert.ok(performance.now() < deadline, "Observer missed the required leaf mutation.");
        await delay(Math.min(25, Math.max(1, deadline - performance.now())));
      }
    } catch (cause) {
      throw new ObserverControlError(phase, root, names, observer.events.slice(offset), cause);
    }
  };
  try {
    await observer.flush();
    for (const root of roots) {
      const directory = await ownership.temporary(path.join(root, ".jones-observer-control-"));
      const name = path.join(directory, "sentinel");
      const renamed = `${name}.renamed`;
      const names = [name, renamed];
      let offset = observer.events.length;
      await fs.writeFile(name, "created", { flag: "wx", mode: 0o600 });
      await fs.appendFile(name, "-changed");
      await observe("created", root, [name], 0x100 | 0x1000, offset);
      offset = observer.events.length;
      await fs.rename(name, renamed);
      await observe("renamed", root, names, 0x800, offset);
      offset = observer.events.length;
      await fs.unlink(renamed);
      await observe("removed", root, names, 0x200, offset);
      for (const filename of names) await assert.rejects(fs.lstat(filename), { code: "ENOENT" });

      const burst = path.join(directory, "burst");
      const burstNames = [burst, `${burst}.renamed`];
      offset = observer.events.length;
      // A separate rapid burst must disappear before the first flush. Paced
      // controls alone cannot qualify observation of short-lived mutations.
      await fs.writeFile(burst, "created", { flag: "wx", mode: 0o600 });
      await fs.appendFile(burst, "-changed");
      await fs.rename(burst, burstNames[1]);
      await fs.unlink(burstNames[1]);
      for (const filename of burstNames)
        await assert.rejects(fs.lstat(filename), { code: "ENOENT" });
      await observe("rapid-burst", root, burstNames, mutation, offset);
    }
  } finally {
    await ownership.cleanup();
  }
}
