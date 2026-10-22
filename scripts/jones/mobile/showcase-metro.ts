// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off - The showcase runner owns these local sockets, child listeners and bounded startup timers.
import type * as NodeChildProcess from "node:child_process";
import * as NodeHTTP from "node:http";
import * as NodeNet from "node:net";
import * as NodeTimers from "node:timers/promises";

export async function selectShowcaseMetroPort(options: {
  readonly configuredPort: number;
  readonly override: string | undefined;
  readonly reuse: boolean;
  readonly reserveAvailablePort: () => Promise<number>;
}): Promise<number> {
  const { configuredPort, override, reuse, reserveAvailablePort } = options;
  if (override !== undefined && !/^\d+$/u.test(override)) {
    throw new Error("T3_SHOWCASE_METRO_PORT must be an integer from 1 to 65535.");
  }
  const port = override === undefined ? configuredPort : Number(override);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("The showcase Metro port must be an integer from 1 to 65535.");
  }
  if (reuse) return port;
  // 8199 is the legacy checked-in convenience default, not an explicit reservation.
  if (override === undefined && configuredPort === 8199) return reserveAvailablePort();
  await new Promise<void>((resolve, reject) => {
    const probe = NodeNet.createServer();
    probe.once("error", (cause) => {
      reject(new Error(`Showcase Metro port ${port} is unavailable.`, { cause }));
    });
    probe.listen(port, "127.0.0.1", () => {
      probe.close((error) => (error ? reject(error) : resolve()));
    });
  });
  return port;
}

export function warmShowcaseMetroBundle(url: string, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = NodeHTTP.get(url, { signal }, (response) => {
      response.once("error", reject);
      response.once("end", () => {
        if (response.statusCode === 200) resolve();
        else reject(new Error(`Metro bundle warm failed: HTTP ${response.statusCode} at ${url}.`));
      });
      response.resume();
    });
    request.once("error", reject);
  });
}

export async function waitForShowcaseMetro(options: {
  readonly port: number;
  readonly child: NodeChildProcess.ChildProcess | null;
  readonly warmBundle: (signal: AbortSignal) => Promise<void>;
  readonly timeoutMs?: number;
}): Promise<void> {
  const { port, child, warmBundle, timeoutMs = 180_000 } = options;
  const controller = new AbortController();
  const { signal } = controller;
  const onExit = () =>
    controller.abort(new Error(`Owned Metro exited before readiness on port ${port}.`));
  const onError = (error: Error) => controller.abort(error);
  const assertAlive = () => {
    signal.throwIfAborted();
    if (child && (child.exitCode !== null || child.signalCode !== null || child.killed)) {
      onExit();
      signal.throwIfAborted();
    }
  };
  child?.once("exit", onExit);
  child?.once("error", onError);
  const timer = setTimeout(
    () => controller.abort(
      new Error(`Metro did not become ready on port ${port} within ${timeoutMs}ms.`),
    ),
    timeoutMs,
  );
  try {
    assertAlive();
    while (true) {
      assertAlive();
      let running = false;
      try {
        const response = await fetch(`http://127.0.0.1:${port}/status`, { signal });
        running = response.ok && (await response.text()).trim() === "packager-status:running";
        if (!response.ok) await response.body?.cancel();
      } catch {
        signal.throwIfAborted();
      }
      assertAlive();
      if (running) break;
      await NodeTimers.setTimeout(100, undefined, { signal });
    }
    await warmBundle(signal);
    assertAlive();
  } catch (error) {
    if (signal.aborted) throw signal.reason;
    throw error;
  } finally {
    clearTimeout(timer);
    child?.removeListener("exit", onExit);
    child?.removeListener("error", onError);
    controller.abort();
  }
}

export async function stopShowcaseMetro(
  child: NodeChildProcess.ChildProcess,
  graceMs = 5_000,
  killMs = 1_000,
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null || child.pid === undefined) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onExit: () => void = () => undefined;
  const exited = new Promise<void>((resolve) => {
    onExit = resolve;
    child.once("exit", onExit);
  });
  const wait = async (milliseconds: number) => {
    try {
      await Promise.race([
        exited,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, milliseconds);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  try {
    child.kill("SIGTERM");
    await wait(graceMs);
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGKILL");
    await wait(killMs);
    if (child.exitCode === null && child.signalCode === null) {
      throw new Error(`Owned Metro process ${child.pid} did not exit after SIGKILL.`);
    }
  } finally {
    child.removeListener("exit", onExit);
  }
}
