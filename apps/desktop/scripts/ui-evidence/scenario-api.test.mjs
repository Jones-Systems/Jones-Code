import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  assertLoopbackEndpoint,
  loadScenario,
  pngDimensions,
  requestLocalBackend,
  createScenarioContext,
  validateCaptureName,
} from "./scenario-api.mjs";
import { sha256 } from "./provenance.mjs";

NodeTest.test(
  "scenario loader records exact content and rejects missing default function",
  async () => {
    const parent = NodePath.resolve(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../../../.t3/ui-evidence-test-scratch",
    );
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "scenario-"));
    try {
      const file = NodePath.join(root, "scenario.mjs");
      const bytes = "export default async () => 42;\n";
      await NodeFSP.writeFile(file, bytes);
      const loaded = await loadScenario(file);
      NodeAssert.equal(loaded.identity.sha256, sha256(bytes));
      NodeAssert.equal(await loaded.run(), 42);
      const invalid = NodePath.join(root, "invalid.mjs");
      await NodeFSP.writeFile(invalid, "export const other = 1;\n");
      await NodeAssert.rejects(loadScenario(invalid), /default async function/);
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);
NodeTest.test("endpoints and capture paths stay bounded", () => {
  NodeAssert.equal(assertLoopbackEndpoint("http://127.0.0.1:1234").port, "1234");
  for (const endpoint of [
    "https://example.com",
    "http://127.0.0.1:1?token=secret",
    "http://user:pass@localhost",
  ])
    NodeAssert.throws(() => assertLoopbackEndpoint(endpoint));
  NodeAssert.equal(validateCaptureName("after-reload"), "after-reload");
  for (const name of ["../secret", "/absolute", "", "a.png"])
    NodeAssert.throws(() => validateCaptureName(name));
});
NodeTest.test("PNG metadata uses encoded physical dimensions", () => {
  const bytes = Buffer.alloc(24);
  Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]).copy(bytes);
  bytes.write("IHDR", 12);
  bytes.writeUInt32BE(1280, 16);
  bytes.writeUInt32BE(800, 20);
  NodeAssert.deepEqual(pngDimensions(bytes), { width: 1280, height: 800 });
  NodeAssert.throws(() => pngDimensions(Buffer.from("not a PNG")));
});

const endpoint = { id: "primary", httpBaseUrl: "http://127.0.0.1:43773", wsBaseUrl: "ws://127.0.0.1:43773/ws" };
const threadCommand = {
  type: "thread.create", commandId: "fixture-command", threadId: "fixture-thread", projectId: "fixture-project",
  title: "Synthetic", createdBy: "user", creationSource: "web", modelSelection: { instanceId: "codex", model: "gpt-5" },
  runtimeMode: "approval-required", interactionMode: "default", branch: null, worktreePath: null,
};
function backendFixture({ httpStatus = 200, rpcExit = { _tag: "Success", value: { sequence: 1 } }, malformed = false, socketError = false } = {}) {
  const calls = [], sockets = [];
  const dependencies = {
    setTimeout,
    clearTimeout,
    desktopBridge: { getLocalEnvironmentBearerToken: async () => "synthetic-bearer" },
    fetch: async (url, options) => {
      calls.push({ url: String(url), options });
      return { ok: httpStatus === 200, status: httpStatus, json: async () =>
        String(url).endsWith("websocket-ticket") ? { ticket: "synthetic-ticket" } : { threads: [{ id: "fixture-thread", title: "Renamed" }] } };
    },
    WebSocket: class {
      listeners = new Map();
      messages = [];
      closed = false;
      constructor(url) { this.url = url; sockets.push(this); queueMicrotask(() => this.emit(socketError ? "error" : "open")); }
      addEventListener(type, listener) { this.listeners.set(type, listener); }
      emit(type, event) { this.listeners.get(type)?.(event); }
      send(data) {
        this.messages.push(JSON.parse(data));
        if (this.messages.at(-1)._tag !== "Request") return;
        queueMicrotask(() => {
          this.emit("message", { data: JSON.stringify({ _tag: "Exit", requestId: "unrelated", exit: rpcExit }) });
          this.emit("message", { data: JSON.stringify({ _tag: "Ping" }) });
          this.emit("message", { data: malformed ? "bad JSON" : JSON.stringify({ _tag: "Exit", requestId: "0", exit: rpcExit }) });
        });
      }
      close() { this.closed = true; this.emit("close"); }
    },
  };
  return { dependencies, calls, sockets };
}
NodeTest.test("V2 fixture snapshot authenticates with bearer, omits cookies, and negotiates protocol", async () => {
  const fixture = backendFixture();
  const value = await requestLocalBackend({ endpoint, method: "snapshot" }, fixture.dependencies);
  NodeAssert.equal(value.threads[0].title, "Renamed");
  NodeAssert.equal(fixture.calls[0].url, "http://127.0.0.1:43773/api/orchestration/shell");
  NodeAssert.deepEqual(fixture.calls[0].options.headers, { authorization: "Bearer synthetic-bearer", "x-t3-orchestration-protocol": "2" });
  NodeAssert.equal(fixture.calls[0].options.credentials, "omit");
  NodeAssert.equal(fixture.sockets.length, 0);
});
NodeTest.test("V2 project fixtures use project mutation HTTP rather than legacy orchestration dispatch", async () => {
  const fixture = backendFixture();
  const command = { type: "project.create", commandId: "c", projectId: "p", title: "Synthetic", workspaceRoot: "/scratch/workspace" };
  await requestLocalBackend({ endpoint, method: "dispatch", payload: command }, fixture.dependencies);
  NodeAssert.equal(fixture.calls[0].url, "http://127.0.0.1:43773/api/projects/mutate");
  NodeAssert.equal(fixture.calls[0].options.method, "POST");
  NodeAssert.deepEqual(JSON.parse(fixture.calls[0].options.body), command);
  NodeAssert.equal(fixture.calls[0].options.credentials, "omit");
  NodeAssert.equal(fixture.sockets.length, 0);
});
NodeTest.test("V2 thread fixture RPC negotiates a private ticket, handles ping, and closes on success", async () => {
  const fixture = backendFixture();
  NodeAssert.deepEqual(await requestLocalBackend({ endpoint, method: "dispatch", payload: threadCommand }, fixture.dependencies), { sequence: 1 });
  NodeAssert.equal(fixture.calls[0].url, "http://127.0.0.1:43773/api/auth/websocket-ticket");
  NodeAssert.equal(fixture.calls[0].options.credentials, "omit");
  const socket = fixture.sockets[0];
  const url = new URL(socket.url);
  NodeAssert.equal(url.pathname, "/ws");
  NodeAssert.equal(url.searchParams.get("orchestrationProtocol"), "2");
  NodeAssert.equal(url.searchParams.get("wsTicket"), "synthetic-ticket");
  NodeAssert.deepEqual(socket.messages[0], { _tag: "Request", id: "0", tag: "orchestration.dispatchCommand", payload: threadCommand, headers: [] });
  NodeAssert.deepEqual(socket.messages[1], { _tag: "Pong" });
  NodeAssert.equal(socket.closed, true);
});
NodeTest.test("V2 fixture HTTP failures reject before opening a socket", async () => {
  for (const method of ["snapshot", "dispatch"]) {
    const fixture = backendFixture({ httpStatus: 401 });
    await NodeAssert.rejects(requestLocalBackend({ endpoint, method, payload: threadCommand }, fixture.dependencies), /HTTP status 401/);
    NodeAssert.equal(fixture.sockets.length, 0);
  }
});
NodeTest.test("V2 fixture RPC failures preserve rejection and close only the owned socket", async () => {
  for (const { options, expected } of [
    { options: { rpcExit: { _tag: "Failure", cause: "synthetic-ticket" } }, expected: /rejected metadata command/ },
    { options: { malformed: true }, expected: /Invalid fixture RPC response/ },
    { options: { socketError: true }, expected: /connection failed/ },
  ]) {
    const fixture = backendFixture(options);
    await NodeAssert.rejects(requestLocalBackend({ endpoint, method: "dispatch", payload: threadCommand }, fixture.dependencies), (error) => expected.test(error.message) && !error.message.includes("synthetic-ticket"));
    NodeAssert.equal(fixture.sockets[0].closed, true);
  }
});
NodeTest.test("V2 fixture scope rejects provider effects and nonloopback endpoints before credentials", async () => {
  const fixture = backendFixture();
  fixture.dependencies.desktopBridge.getLocalEnvironmentBearerToken = () => { throw new Error("credentials reached"); };
  for (const payload of [
    { type: "thread.turn.start" }, { type: "thread.meta.update" },
    { type: "thread.metadata.update", commandId: "c", threadId: "t", regenerateTitle: true },
    { ...threadCommand, worktreePath: "/live/worktree" },
  ]) await NodeAssert.rejects(requestLocalBackend({ endpoint, method: "dispatch", payload }, fixture.dependencies), /metadata|private project/);
  await NodeAssert.rejects(requestLocalBackend({ endpoint: { ...endpoint, wsBaseUrl: "ws://example.test/ws" }, method: "snapshot" }, fixture.dependencies), /private loopback/);
  NodeAssert.equal(fixture.calls.length, 0);
});
NodeTest.test("scenario context restricts project fixtures to its owned workspace", async () => {
  const fixture = backendFixture();
  const page = { evaluate: (fn, input) => input ? fn(input, fixture.dependencies) : [endpoint] };
  const ctx = createScenarioContext({ page, workspace: "/scratch/workspace" });
  await NodeAssert.rejects(ctx.dispatch({ type: "project.create", workspaceRoot: "/app" }), /private workspace/);
  await ctx.dispatch({ type: "thread.metadata.update", commandId: "c", threadId: "t", title: "Renamed" });
  NodeAssert.equal(fixture.sockets[0].messages[0].payload.title, "Renamed");
});

NodeTest.test("V2 fixture RPC timeout rejects and closes its owned socket without waiting on wall time", async () => {
  const fixture = backendFixture();
  fixture.dependencies.setTimeout = (fn, milliseconds) => {
    NodeAssert.equal(milliseconds, 15000);
    queueMicrotask(fn);
    return "synthetic-timer";
  };
  let cleared;
  fixture.dependencies.clearTimeout = (timer) => { cleared = timer; };
  await NodeAssert.rejects(requestLocalBackend({ endpoint, method: "dispatch", payload: threadCommand }, fixture.dependencies), /timed out/);
  NodeAssert.equal(fixture.sockets[0].closed, true);
  NodeAssert.equal(cleared, "synthetic-timer");
});
