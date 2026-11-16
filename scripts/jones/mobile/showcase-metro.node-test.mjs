import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeProcess from "node:process";
import * as NodeTest from "node:test";

import {
  selectShowcaseMetroPort,
  stopShowcaseMetro,
  waitForShowcaseMetro,
  warmShowcaseMetroBundle,
} from "./showcase-metro.ts";

async function listener(t, handler) {
  const server = NodeHttp.createServer(handler);
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  });
  server.listen(0, "127.0.0.1");
  await NodeEvents.once(server, "listening");
  const port = server.address().port;
  return { port, url: `http://127.0.0.1:${port}` };
}

async function ownedChild(t) {
  const child = NodeChildProcess.spawn(
    NodeProcess.execPath,
    ["-e", "setInterval(() => {}, 1000)"],
    {
      stdio: "ignore",
    },
  );
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

NodeTest.test("default runs allocate a port rather than inheriting the legacy 8199", async () => {
  let allocations = 0;
  const select = () =>
    selectShowcaseMetroPort({
      ...portOptions,
      reserveAvailablePort: async () => 49152 + allocations++,
    });
  NodeAssert.deepEqual(await Promise.all([select(), select()]), [49152, 49153]);
});

NodeTest.test("reuse preserves explicit ports and the legacy manual-server default", async () => {
  NodeAssert.equal(await selectShowcaseMetroPort({ ...portOptions, reuse: true }), 8199);
  NodeAssert.equal(
    await selectShowcaseMetroPort({ ...portOptions, configuredPort: 8234, reuse: true }),
    8234,
  );
  NodeAssert.equal(
    await selectShowcaseMetroPort({ ...portOptions, override: "8199", reuse: true }),
    8199,
  );
  NodeAssert.equal(
    await selectShowcaseMetroPort({ ...portOptions, override: "49153", reuse: true }),
    49153,
  );
});

NodeTest.test("rejects invalid explicit ports", async () => {
  await NodeAssert.rejects(selectShowcaseMetroPort({ ...portOptions, override: "" }), /integer/);
  await NodeAssert.rejects(
    selectShowcaseMetroPort({ ...portOptions, override: "12oops" }),
    /integer/,
  );
  await NodeAssert.rejects(
    selectShowcaseMetroPort({ ...portOptions, override: "65536" }),
    /integer/,
  );
  await NodeAssert.rejects(
    selectShowcaseMetroPort({ ...portOptions, configuredPort: 0 }),
    /integer/,
  );
});

NodeTest.test(
  "rejects an occupied explicit config or environment port before spawning",
  async (t) => {
    const { port } = await listener(t, (_request, response) => response.end("other run"));
    await NodeAssert.rejects(
      selectShowcaseMetroPort({ ...portOptions, configuredPort: port }),
      /unavailable/,
    );
    await NodeAssert.rejects(
      selectShowcaseMetroPort({ ...portOptions, override: String(port) }),
      /unavailable/,
    );
  },
);

NodeTest.test(
  "an available explicit port stays selected and the probe releases its socket",
  async () => {
    const probe = NodeNet.createServer();
    probe.listen(0, "127.0.0.1");
    await NodeEvents.once(probe, "listening");
    const port = probe.address().port;
    await new Promise((resolve) => probe.close(resolve));
    NodeAssert.equal(
      await selectShowcaseMetroPort({ ...portOptions, override: String(port) }),
      port,
    );
    probe.listen(port, "127.0.0.1");
    try {
      await NodeEvents.once(probe, "listening");
    } finally {
      await new Promise((resolve) => probe.close(resolve));
    }
  },
);

NodeTest.test("rejects an unrelated listener despite an alive owned child", async (t) => {
  const child = await ownedChild(t);
  const { port } = await listener(t, (_request, response) => response.end("unrelated service"));
  let warmed = false;
  await NodeAssert.rejects(
    waitForShowcaseMetro({
      port,
      child,
      timeoutMs: 100,
      warmBundle: async () => {
        warmed = true;
      },
    }),
    /Metro did not become ready/,
  );
  NodeAssert.equal(warmed, false);
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.listenerCount("error"), 0);
});

NodeTest.test(
  "rejects a dead owned child even when another listener reports Metro running",
  async (t) => {
    const child = await ownedChild(t);
    await stopShowcaseMetro(child);
    const { port } = await listener(t, (_request, response) =>
      response.end("packager-status:running"),
    );
    await NodeAssert.rejects(
      waitForShowcaseMetro({
        port,
        child,
        warmBundle: async () => NodeAssert.fail("must not warm a dead child's bundle"),
      }),
      /Owned Metro exited/,
    );
  },
);

NodeTest.test("readiness warms both platform bundles on the selected port", async (t) => {
  const child = await ownedChild(t);
  const requests = [];
  const { port, url } = await listener(t, (request, response) => {
    requests.push(request.url);
    response.end(request.url === "/status" ? "packager-status:running" : "synthetic bundle");
  });
  await waitForShowcaseMetro({
    port,
    child,
    warmBundle: (signal) =>
      Promise.all([
        warmShowcaseMetroBundle(`${url}/index.bundle?platform=ios`, signal),
        warmShowcaseMetroBundle(`${url}/index.bundle?platform=android`, signal),
      ]),
  });
  NodeAssert.deepEqual(requests.toSorted(), [
    "/index.bundle?platform=android",
    "/index.bundle?platform=ios",
    "/status",
  ]);
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.listenerCount("error"), 0);
});

NodeTest.test("preserves a bundle warm failure and cleans up child listeners", async (t) => {
  const child = await ownedChild(t);
  const { port, url } = await listener(t, (request, response) => {
    response.statusCode = request.url === "/status" ? 200 : 500;
    response.end(request.url === "/status" ? "packager-status:running" : "compile failed");
  });
  await NodeAssert.rejects(
    waitForShowcaseMetro({
      port,
      child,
      warmBundle: (signal) => warmShowcaseMetroBundle(`${url}/bundle`, signal),
    }),
    /bundle warm failed: HTTP 500/,
  );
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.listenerCount("error"), 0);
});

NodeTest.test("child exit interrupts a hanging bundle and releases listeners", async (t) => {
  const child = await ownedChild(t);
  const { port, url } = await listener(t, (request, response) => {
    if (request.url === "/status") response.end("packager-status:running");
    else child.kill("SIGTERM");
  });
  await NodeAssert.rejects(
    waitForShowcaseMetro({
      port,
      child,
      warmBundle: (signal) => warmShowcaseMetroBundle(`${url}/bundle`, signal),
    }),
    /Owned Metro exited/,
  );
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.listenerCount("error"), 0);
});

NodeTest.test("a readiness deadline aborts a hanging status request", async (t) => {
  const child = await ownedChild(t);
  const { port } = await listener(t, () => {});
  await NodeAssert.rejects(
    waitForShowcaseMetro({
      port,
      child,
      timeoutMs: 100,
      warmBundle: async () => NodeAssert.fail("status never responded"),
    }),
    /Metro did not become ready/,
  );
  NodeAssert.equal(child.listenerCount("exit"), 0);
});

NodeTest.test("cleanup stops only its captured child and releases its exit listener", async (t) => {
  const first = await ownedChild(t);
  const second = await ownedChild(t);
  await stopShowcaseMetro(first);
  NodeAssert.notEqual(first.exitCode ?? first.signalCode, null);
  NodeAssert.equal(first.listenerCount("exit"), 0);
  NodeAssert.equal(second.exitCode, null);
  NodeAssert.equal(second.signalCode, null);
  NodeAssert.equal(second.killed, false);
});

NodeTest.test("spawn failure preserves its error and removes readiness listeners", async (t) => {
  const child = NodeChildProcess.spawn(NodeProcess.execPath, ["-e", ""], {
    cwd: new URL("./missing-showcase-spawn-fixture/", import.meta.url),
    stdio: "ignore",
  });
  t.after(() => stopShowcaseMetro(child));
  await NodeAssert.rejects(
    waitForShowcaseMetro({
      port: 1,
      child,
      warmBundle: async () => NodeAssert.fail("spawn failed"),
    }),
    { code: "ENOENT" },
  );
  NodeAssert.equal(child.listenerCount("exit"), 0);
  NodeAssert.equal(child.listenerCount("error"), 0);
});

NodeTest.test("unconfirmed cleanup fails within its bounds and removes listeners", async () => {
  // This child deliberately acknowledges signals without reporting exit; no process is killed.
  const child = Object.assign(new NodeEvents.EventEmitter(), {
    pid: 123,
    exitCode: null,
    signalCode: null,
    signals: [],
    kill(signal) {
      this.signals.push(signal);
      return true;
    },
  });
  await NodeAssert.rejects(stopShowcaseMetro(child, 0, 0), /did not exit after SIGKILL/);
  NodeAssert.deepEqual(child.signals, ["SIGTERM", "SIGKILL"]);
  NodeAssert.equal(child.listenerCount("exit"), 0);
});
