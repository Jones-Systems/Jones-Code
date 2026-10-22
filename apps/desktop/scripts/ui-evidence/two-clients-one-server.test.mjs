import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  NodeSubscriptionClient,
  assertStandardScopes,
  pairNodeClient,
} from "./support/node-client.mjs";
import twoClientsOneServer, {
  assertProjection,
  metadataBytes,
} from "./scenarios/two-clients-one-server.mjs";
import { loadScenario } from "./scenario-api.mjs";
import { stageRun } from "./sandbox.mjs";

const fixture = {
  projectId: "project",
  threadId: "target",
  controlThreadId: "control",
  controlTitle: "control",
};
const endpoint = { httpBaseUrl: "http://127.0.0.1:43773", wsBaseUrl: "ws://127.0.0.1:43773" };
function snapshot(sequence = 1, title = "initial", pinned = false) {
  return {
    schemaVersion: 1,
    snapshotSequence: sequence,
    projects: [{ id: fixture.projectId }],
    archivedThreads: [],
    threads: [
      {
        id: "target",
        projectId: "project",
        title,
        pinnedAt: pinned ? "2026-10-22T12:00:00.000Z" : null,
        pinOrderKey: pinned ? "n" : null,
      },
      { id: "control", projectId: "project", title: "control", pinnedAt: null, pinOrderKey: null },
    ],
  };
}

function clientHarness(initialItems) {
  const sockets = [],
    httpCalls = [];
  class Socket extends EventTarget {
    readyState = 0;
    sent = [];
    constructor(url) {
      super();
      this.url = url;
      sockets.push(this);
      queueMicrotask(() => {
        this.readyState = 1;
        this.dispatchEvent(new Event("open"));
        this.items(...initialItems(sockets.length));
      });
    }
    send(value) {
      this.sent.push(JSON.parse(value));
    }
    message(value) {
      this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(value) }));
    }
    items(...values) {
      this.message({ _tag: "Chunk", requestId: "0", values });
    }
    close() {
      this.readyState = 3;
      this.dispatchEvent(new Event("close"));
    }
  }
  const client = new NodeSubscriptionClient({
    endpoint,
    accessToken: "synthetic-access",
    WebSocket: Socket,
    fetch: async (url, options) => {
      httpCalls.push({ url: String(url), options });
      return { ok: true, json: async () => ({ ticket: "synthetic-ticket" }) };
    },
  });
  return { client, sockets, httpCalls };
}
const initial = (state) => [{ kind: "snapshot", snapshot: state }, { kind: "synchronized" }];

NodeTest.test("two-client scenario loads under the native scenario contract", async () => {
  const loaded = await loadScenario(
    NodeURL.fileURLToPath(new URL("./scenarios/two-clients-one-server.mjs", import.meta.url)),
  );
  NodeAssert.equal(loaded.run.constructor.name, "AsyncFunction");
  NodeAssert.match(loaded.identity.sha256, /^[a-f0-9]{64}$/);
});

NodeTest.test(
  "scenario selects a rendered title whose accessible name includes its project",
  async () => {
    const threads = [];
    const observations = [];
    let projectTitle;
    const locator = (selector) => ({
      count: async () => (selector.includes(":has(") ? 0 : 1),
      locator: (selector) => ({ selector }),
      getByText: (title, options) => ({
        and: (visibleTitle) => ({
          waitFor: async () => {
            NodeAssert.equal(visibleTitle.selector, "span[aria-hidden]");
            NodeAssert.equal(options.exact, true);
            NodeAssert.equal(title, threads[0].title);
            observations.push("exact rendered title");
          },
        }),
      }),
      getByRole: (role, options) => {
        NodeAssert.equal(role, "button");
        const accessibleName = `${threads[0].title}, ${projectTitle}`;
        const matches = options.exact
          ? accessibleName === options.name
          : accessibleName.includes(options.name);
        const assertMatch = () => {
          NodeAssert.ok(matches, "Rendered sidebar row accessible name includes its project");
        };
        return {
          count: async () => (options.name === "Unpin thread" ? 0 : Number(matches)),
          waitFor: async () => assertMatch(),
          click: async () => {
            assertMatch();
            observations.push("selected target row");
          },
        };
      },
    });
    const ctx = {
      fixture: {},
      workspace: "/scratch/workspace",
      assert: NodeAssert.ok,
      completeOnboarding: async () => {},
      topology: async () => endpoint,
      step: async (name, run) => {
        if (name === "Pair Node B with the ordinary standard grant and subscribe")
          throw new Error("Reached pairing after selecting the rendered target");
        return run();
      },
      dispatch: async (command) => {
        if (command.type === "project.create") projectTitle = command.title;
        if (command.type === "thread.create")
          threads.push({
            id: command.threadId,
            projectId: command.projectId,
            title: command.title,
            pinnedAt: null,
            pinOrderKey: null,
          });
      },
      readSnapshot: async () => ({ threads }),
      page: {
        locator,
        waitForFunction: async () => {},
        getByTestId: () => ({ waitFor: async () => {}, focus: async () => {} }),
      },
    };
    await NodeAssert.rejects(twoClientsOneServer(ctx), {
      message: "Reached pairing after selecting the rendered target",
    });
    NodeAssert.deepEqual(observations, ["exact rendered title", "selected target row"]);
  },
);

NodeTest.test(
  "projection oracle rejects no-op, wrong-thread, stale-title and duplicate observations",
  () => {
    assertProjection(snapshot(2, "renamed", true), fixture, "renamed", true);
    NodeAssert.throws(
      () => assertProjection(snapshot(1), fixture, "renamed", true),
      /UI-renamed title/,
    );
    NodeAssert.throws(
      () => assertProjection(snapshot(2, "renamed"), fixture, "renamed", true),
      /UI pin state/,
    );
    const duplicate = snapshot(2, "renamed", true);
    duplicate.threads.push({ ...duplicate.threads[0] });
    NodeAssert.throws(() => assertProjection(duplicate, fixture, "renamed", true), /exactly once/);
    const wrongThread = snapshot(2, "renamed", true);
    wrongThread.threads[1].pinnedAt = wrongThread.threads[0].pinnedAt;
    NodeAssert.throws(
      () => assertProjection(wrongThread, fixture, "renamed", true),
      /Control thread/,
    );
    NodeAssert.notEqual(
      metadataBytes(snapshot(), fixture),
      metadataBytes(snapshot(2, "renamed", true), fixture),
    );
  },
);

NodeTest.test(
  "wire adapter accepts updates, reconnects once, rejects stale cache, and disposes B",
  async (t) => {
    const latest = snapshot(4, "offline rename", true);
    const { client, sockets, httpCalls } = clientHarness((connection) =>
      initial(connection === 1 ? snapshot() : latest),
    );
    t.after(() => client.remove());
    await client.connect();
    const stale = client.snapshot;
    sockets[0].items({
      kind: "thread.updated",
      sequence: 2,
      location: "active",
      thread: snapshot(2, "live rename", true).threads[0],
    });
    assertProjection(
      await client.waitFor((state) => state?.snapshotSequence === 2),
      fixture,
      "live rename",
      true,
    );
    sockets[0].items({
      kind: "snapshot",
      resolvedRepositoryIdentityRoots: ["/scratch/workspace"],
      snapshot: { ...snapshot(3), threads: [] },
    });
    assertProjection(client.snapshot, fixture, "live rename", true);
    await client.disconnect();
    assertProjection(client.snapshot, fixture, "live rename", true);
    sockets[0].items({ kind: "snapshot", snapshot: snapshot(99, "late old generation", true) });
    await client.connect();
    NodeAssert.deepEqual(client.snapshot, latest);
    NodeAssert.equal(client.restoreCachedSnapshot(stale), false);
    NodeAssert.deepEqual(client.snapshot, latest);
    NodeAssert.deepEqual(
      client.evidence.connections.map((item) => [item.snapshots, item.synchronized]),
      [
        [1, true],
        [1, true],
      ],
    );
    NodeAssert.equal(client.evidence.commandCount, 0);
    NodeAssert.deepEqual(
      sockets.flatMap((socket) => socket.sent.filter((frame) => frame._tag === "Request")),
      [1, 2].map(() => ({
        _tag: "Request",
        id: "0",
        tag: "orchestration.subscribeShell",
        payload: { requestCompletionMarker: true },
        headers: [],
      })),
    );
    NodeAssert.equal(httpCalls.length, 2);
    for (const { url, options } of httpCalls) {
      NodeAssert.equal(url, `${endpoint.httpBaseUrl}/api/auth/websocket-ticket`);
      NodeAssert.equal(options.headers.authorization, "Bearer synthetic-access");
      NodeAssert.equal(options.redirect, "error");
    }
    for (const socket of sockets) {
      const url = new URL(socket.url);
      NodeAssert.equal(url.pathname, "/ws");
      NodeAssert.equal(url.searchParams.get("orchestrationProtocol"), "2");
    }
    await client.remove();
    NodeAssert.equal(client.snapshot, null);
    NodeAssert.equal(client.restoreCachedSnapshot(latest), false);
    await NodeAssert.rejects(client.connect(), /disconnected and registered/);
    NodeAssert.ok(
      client.evidence.removed &&
        client.evidence.credentialsCleared &&
        client.evidence.projectionCleared &&
        client.evidence.socketClosed,
    );
    NodeAssert.ok(!JSON.stringify(client.evidence).includes("synthetic-access"));
  },
);

NodeTest.test(
  "duplicate snapshots and completed/error streams cannot claim synchronized success",
  async (t) => {
    for (const items of [
      [{ kind: "snapshot", snapshot: snapshot() }, ...initial(snapshot())],
      initial({ ...snapshot(), threads: [snapshot().threads[0], snapshot().threads[0]] }),
      [{ kind: "synchronized" }],
    ]) {
      const { client } = clientHarness(() => items);
      t.after(() => client.remove());
      await NodeAssert.rejects(client.connect(), /invalid or rejected projection/);
    }
    const { client, sockets } = clientHarness(() => initial(snapshot()));
    t.after(() => client.remove());
    await client.connect();
    sockets[0].message({ _tag: "Exit", requestId: "0", exit: { _tag: "Success" } });
    await NodeAssert.rejects(
      client.waitFor(() => true),
      /invalid or rejected projection/,
    );
  },
);

NodeTest.test("a reconnect snapshot older than B's cached sequence fails closed", async (t) => {
  const { client } = clientHarness((connection) => initial(snapshot(connection === 1 ? 5 : 2)));
  t.after(() => client.remove());
  await client.connect();
  await client.disconnect();
  await NodeAssert.rejects(client.connect(), /invalid or rejected projection/);
  NodeAssert.equal(client.snapshot.snapshotSequence, 5);
});

NodeTest.test(
  "pairing scope and endpoint guards reject elevated grants and non-private targets",
  () => {
    for (const scope of [undefined, "orchestration:read", "orchestration:read access:write"])
      NodeAssert.throws(() => assertStandardScopes(scope));
    for (const url of [
      "http://example.com",
      "http://127.0.0.1?token=x",
      "http://user:pass@localhost",
    ])
      NodeAssert.throws(
        () =>
          new NodeSubscriptionClient({
            endpoint: { ...endpoint, httpBaseUrl: url },
            accessToken: "synthetic",
          }),
        /private loopback/,
      );
  },
);

NodeTest.test(
  "ordinary pairing requests default scopes and keeps A's bearer inside its renderer",
  async (t) => {
    const calls = [];
    const scopes = [
      "orchestration:read",
      "orchestration:operate",
      "settings:write",
      "providers:manage",
      "environment:maintain",
      "preview:operate",
      "diagnostics:read",
      "terminal:read",
      "terminal:operate",
      "source-control:write",
      "filesystem:read",
      "filesystem:write",
      "relay:read",
    ].join(" ");
    t.mock.method(globalThis, "fetch", async (url, options) => {
      calls.push({ url: String(url), options });
      return {
        ok: true,
        json: async () =>
          String(url).endsWith("/pairing-token")
            ? { credential: "synthetic-once" }
            : { access_token: "synthetic-B", scope: scopes, token_type: "Bearer" },
      };
    });
    const oldWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: { desktopBridge: { getLocalEnvironmentBearerToken: async () => "synthetic-A" } },
    });
    t.after(() => {
      if (oldWindow) Object.defineProperty(globalThis, "window", oldWindow);
      else delete globalThis.window;
    });
    const client = await pairNodeClient({
      topology: async () => endpoint,
      page: { evaluate: (fn, input) => fn(input) },
    });
    t.after(() => client.remove());
    NodeAssert.equal(calls[0].url, `${endpoint.httpBaseUrl}/api/auth/pairing-token`);
    NodeAssert.deepEqual(JSON.parse(calls[0].options.body), { label: "UI evidence Node client B" });
    NodeAssert.equal(calls[0].options.headers.authorization, "Bearer synthetic-A");
    NodeAssert.equal(calls[1].url, `${endpoint.httpBaseUrl}/oauth/token`);
    NodeAssert.equal(calls[1].options.body.get("subject_token"), "synthetic-once");
    NodeAssert.equal(calls[1].options.body.has("scope"), false);
    NodeAssert.equal(calls[1].options.headers.authorization, undefined);
  },
);

async function stagingFixture(t) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "two-clients-stage-"));
  t.after(() => NodeFSP.rm(root, { recursive: true, force: true }));
  const source = NodePath.join(root, "source"),
    harness = NodePath.join(root, "harness");
  for (const path of ["apps/desktop/dist-electron", "apps/server/dist"])
    await NodeFSP.mkdir(NodePath.join(source, path), { recursive: true });
  for (const path of ["package.json", "apps/desktop/package.json", "apps/server/package.json"])
    await NodeFSP.writeFile(NodePath.join(source, path), "{}");
  const support = NodePath.join(harness, "apps/desktop/scripts/ui-evidence/support");
  await NodeFSP.mkdir(support, { recursive: true });
  const scenario = NodePath.join(root, "scenario.mjs");
  await NodeFSP.writeFile(scenario, "export default async function() {}\n");
  return {
    root,
    support,
    source,
    harness,
    scenario,
    run: { root: NodePath.join(root, "run") },
    closure: { packages: [], roots: [] },
    configuration: {},
  };
}

NodeTest.test("support modules stage inside the existing read-only harness closure", async (t) => {
  const options = await stagingFixture(t);
  await NodeFSP.writeFile(
    NodePath.join(options.support, "node-client.mjs"),
    "export const fixture = true;\n",
  );
  const stage = await stageRun(options);
  NodeAssert.equal(
    await NodeFSP.readFile(NodePath.join(stage, "harness/support/node-client.mjs"), "utf8"),
    "export const fixture = true;\n",
  );
});

NodeTest.test("staged scenario resolves support when the loader adds its hash query", async (t) => {
  const options = await stagingFixture(t);
  options.scenario = NodeURL.fileURLToPath(
    new URL("./scenarios/two-clients-one-server.mjs", import.meta.url),
  );
  await NodeFSP.copyFile(
    new URL("./support/node-client.mjs", import.meta.url),
    NodePath.join(options.support, "node-client.mjs"),
  );
  try {
    const stage = await stageRun(options);
    const loaded = await loadScenario(NodePath.join(stage, "harness/scenario.mjs"));
    NodeAssert.equal(loaded.run.constructor.name, "AsyncFunction");
    NodeAssert.match(loaded.identity.sha256, /^[a-f0-9]{64}$/);
  } finally {
    await NodeFSP.rm(options.root, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.access(options.root), { code: "ENOENT" });
  }
});

NodeTest.test(
  "support staging rejects protected files and escaping or broken symlinks",
  async (t) => {
    for (const kind of ["protected", "escaping", "broken"]) {
      const options = await stagingFixture(t);
      if (kind === "protected")
        await NodeFSP.writeFile(NodePath.join(options.support, ".env"), "synthetic");
      else {
        const target = NodePath.join(options.root, "outside.mjs");
        if (kind === "escaping") await NodeFSP.writeFile(target, "synthetic");
        await NodeFSP.symlink(target, NodePath.join(options.support, "client.mjs"));
      }
      await NodeAssert.rejects(
        stageRun(options),
        kind === "protected" ? /Protected file/ : kind === "escaping" ? /escaped/ : /ENOENT/,
      );
    }
  },
);

NodeTest.test(
  "support staging rejects a symlink root and supports older harnesses without support",
  async (t) => {
    const linked = await stagingFixture(t);
    await NodeFSP.rm(linked.support, { recursive: true });
    await NodeFSP.symlink(linked.source, linked.support);
    await NodeAssert.rejects(stageRun(linked), /regular directory/);
    const older = await stagingFixture(t);
    await NodeFSP.rm(older.support, { recursive: true });
    const stage = await stageRun(older);
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(stage, "harness/scenario.mjs"), "utf8"),
      "export default async function() {}\n",
    );
  },
);
