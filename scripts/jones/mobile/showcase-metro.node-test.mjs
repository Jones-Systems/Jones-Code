import * as assert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeHTTP from "node:http";
import * as NodeNet from "node:net";
import * as NodeProcess from "node:process";
import { test } from "node:test";

import {
  selectShowcaseMetroPort,
  stopShowcaseMetro,
  waitForShowcaseMetro,
  warmShowcaseMetroBundle,
} from "./showcase-metro.ts";

async function listener(t, handler) {
  const server = NodeHTTP.createServer(handler);
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  });
  server.listen(0, "127.0.0.1");
  await NodeEvents.once(server, "listening");
  const port = server.address().port;
  return { port, url: `http://127.0.0.1:${port}` };
}

async function ownedChild(t) {
  const child = NodeChildProcess.spawn(NodeProcess.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    stdio: "ignore",
  });
  t.after(() => stopShowcaseMetro(child));
  await NodeEvents.once(child, "spawn");
  return child;
}

const portOptions = {
  configuredPort: 8199,
  override: undefined,
  reuse: false,
  reserveAvailablePort: async () => 49152,
};

test("default runs allocate a port rather than inheriting the legacy 8199", async () => {
  let allocations = 0;
  const select = () => selectShowcaseMetroPort({
    ...portOptions,
    reserveAvailablePort: async () => 49152 + allocations++,
  });
  assert.deepEqual(await Promise.all([select(), select()]), [49152, 49153]);
});

test("reuse preserves explicit ports and the legacy manual-server default", async () => {
  assert.equal(await selectShowcaseMetroPort({ ...portOptions, reuse: true }), 8199);
  assert.equal(await selectShowcaseMetroPort({ ...portOptions, configuredPort: 8234, reuse: true }), 8234);
  assert.equal(await selectShowcaseMetroPort({ ...portOptions, override: "8199", reuse: true }), 8199);
  assert.equal(await selectShowcaseMetroPort({ ...portOptions, override: "49153", reuse: true }), 49153);
});

test("rejects invalid explicit ports", async () => {
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, override: "" }), /integer/);
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, override: "12oops" }), /integer/);
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, override: "65536" }), /integer/);
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, configuredPort: 0 }), /integer/);
});

test("rejects an occupied explicit config or environment port before spawning", async (t) => {
  const { port } = await listener(t, (_request, response) => response.end("other run"));
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, configuredPort: port }), /unavailable/);
  await assert.rejects(selectShowcaseMetroPort({ ...portOptions, override: String(port) }), /unavailable/);
});

test("an available explicit port stays selected and the probe releases its socket", async () => {
  const probe = NodeNet.createServer();
  probe.listen(0, "127.0.0.1");
  await NodeEvents.once(probe, "listening");
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  assert.equal(await selectShowcaseMetroPort({ ...portOptions, override: String(port) }), port);
  probe.listen(port, "127.0.0.1");
  try {
    await NodeEvents.once(probe, "listening");
  } finally {
    await new Promise((resolve) => probe.close(resolve));
  }
});

test("rejects an unrelated listener despite an alive owned child", async (t) => {
  const child = await ownedChild(t);
  const { port } = await listener(t, (_request, response) => response.end("unrelated service"));
  let warmed = false;
  await assert.rejects(waitForShowcaseMetro({
    port, child, timeoutMs: 100,
    warmBundle: async () => { warmed = true; },
  }), /Metro did not become ready/);
  assert.equal(warmed, false);
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
});

test("rejects a dead owned child even when another listener reports Metro running", async (t) => {
  const child = await ownedChild(t);
  await stopShowcaseMetro(child);
  const { port } = await listener(t, (_request, response) => response.end("packager-status:running"));
  await assert.rejects(waitForShowcaseMetro({
    port, child, warmBundle: async () => assert.fail("must not warm a dead child's bundle"),
  }), /Owned Metro exited/);
});

test("readiness warms both platform bundles on the selected port", async (t) => {
  const child = await ownedChild(t);
  const requests = [];
  const { port, url } = await listener(t, (request, response) => {
    requests.push(request.url);
    response.end(request.url === "/status" ? "packager-status:running" : "synthetic bundle");
  });
  await waitForShowcaseMetro({
    port, child,
    warmBundle: (signal) => Promise.all([
      warmShowcaseMetroBundle(`${url}/index.bundle?platform=ios`, signal),
      warmShowcaseMetroBundle(`${url}/index.bundle?platform=android`, signal),
    ]),
  });
  assert.deepEqual(requests.toSorted(), ["/index.bundle?platform=android", "/index.bundle?platform=ios", "/status"]);
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
});

test("preserves a bundle warm failure and cleans up child listeners", async (t) => {
  const child = await ownedChild(t);
  const { port, url } = await listener(t, (request, response) => {
    response.statusCode = request.url === "/status" ? 200 : 500;
    response.end(request.url === "/status" ? "packager-status:running" : "compile failed");
  });
  await assert.rejects(waitForShowcaseMetro({
    port, child, warmBundle: (signal) => warmShowcaseMetroBundle(`${url}/bundle`, signal),
  }), /bundle warm failed: HTTP 500/);
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
});

test("child exit interrupts a hanging bundle and releases listeners", async (t) => {
  const child = await ownedChild(t);
  const { port, url } = await listener(t, (request, response) => {
    if (request.url === "/status") response.end("packager-status:running");
    else child.kill("SIGTERM");
  });
  await assert.rejects(waitForShowcaseMetro({
    port, child, warmBundle: (signal) => warmShowcaseMetroBundle(`${url}/bundle`, signal),
  }), /Owned Metro exited/);
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
});

test("a readiness deadline aborts a hanging status request", async (t) => {
  const child = await ownedChild(t);
  const { port } = await listener(t, () => {});
  await assert.rejects(waitForShowcaseMetro({
    port, child, timeoutMs: 100, warmBundle: async () => assert.fail("status never responded"),
  }), /Metro did not become ready/);
  assert.equal(child.listenerCount("exit"), 0);
});

test("cleanup stops only its captured child and releases its exit listener", async (t) => {
  const first = await ownedChild(t);
  const second = await ownedChild(t);
  await stopShowcaseMetro(first);
  assert.notEqual(first.exitCode ?? first.signalCode, null);
  assert.equal(first.listenerCount("exit"), 0);
  assert.equal(second.exitCode, null);
  assert.equal(second.signalCode, null);
  assert.equal(second.killed, false);
});

test("spawn failure preserves its error and removes readiness listeners", async (t) => {
  const child = NodeChildProcess.spawn(NodeProcess.execPath, ["-e", ""], {
    cwd: new URL("./missing-showcase-spawn-fixture/", import.meta.url),
    stdio: "ignore",
  });
  t.after(() => stopShowcaseMetro(child));
  await assert.rejects(waitForShowcaseMetro({
    port: 1, child, warmBundle: async () => assert.fail("spawn failed"),
  }), { code: "ENOENT" });
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
});

test("unconfirmed cleanup fails within its bounds and removes listeners", async () => {
  // This child deliberately acknowledges signals without reporting exit; no process is killed.
  const child = Object.assign(new NodeEvents.EventEmitter(), {
    pid: 123,
    exitCode: null,
    signalCode: null,
    signals: [],
    kill(signal) { this.signals.push(signal); return true; },
  });
  await assert.rejects(stopShowcaseMetro(child, 0, 0), /did not exit after SIGKILL/);
  assert.deepEqual(child.signals, ["SIGTERM", "SIGKILL"]);
  assert.equal(child.listenerCount("exit"), 0);
});
