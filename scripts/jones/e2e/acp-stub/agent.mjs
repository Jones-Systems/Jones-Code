import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const MAX_FRAME = 1024 * 1024;
export function parseOptions(argv) {
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i];
    if (
      !["--state-dir", "--run-id", "--timeout-ms"].includes(name) ||
      options[name] ||
      !argv[i + 1]
    )
      throw new Error("Invalid stub arguments");
    options[name] = argv[i + 1];
  }
  const stateDir = options["--state-dir"];
  const runId = options["--run-id"];
  const timeoutMs = Number(options["--timeout-ms"] ?? 120000);
  if (
    !stateDir ||
    !NodePath.isAbsolute(stateDir) ||
    !/^[a-zA-Z0-9-]{1,64}$/.test(runId ?? "") ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 100 ||
    timeoutMs > 120000
  )
    throw new Error("Invalid stub scope");
  const stat = NodeFS.lstatSync(stateDir);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    NodeFS.realpathSync(stateDir) !== stateDir ||
    (stat.mode & 0o077) !== 0
  )
    throw new Error("Stub state must be a private real directory");
  return { stateDir, runId, timeoutMs };
}

export function startAgent({ stateDir, runId, timeoutMs }) {
  const processId = `${process.pid}-${NodeCrypto.randomUUID()}`;
  const ledgerPath = NodePath.join(stateDir, `ledger-${processId}.ndjson`);
  const ledger = NodeFS.openSync(ledgerPath, "wx", 0o600);
  const active = new Map();
  let buffer = "",
    stopped = false,
    deliveries = 0,
    sessionsCreated = 0;
  const timers = new Set();
  const record = (entry) =>
    NodeFS.writeSync(ledger, `${JSON.stringify({ runId, processId, at: Date.now(), ...entry })}\n`);
  let startTicks = null;
  try {
    const stat = NodeFS.readFileSync(`/proc/${process.pid}/stat`, "utf8");
    startTicks = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  record({ event: "start", pid: process.pid, ppid: process.ppid, startTicks });
  function send(message) {
    if (!stopped) process.stdout.write(`${JSON.stringify(message)}\n`);
  }
  function sessionPath(sessionId) {
    if (typeof sessionId !== "string" || !/^je06-[a-f0-9-]{36}$/.test(sessionId))
      throw new Error("Unknown session");
    return NodePath.join(stateDir, `${sessionId}.json`);
  }
  function readSession(sessionId) {
    const path = sessionPath(sessionId);
    if (!NodeFS.lstatSync(path).isFile() || NodeFS.lstatSync(path).isSymbolicLink())
      throw new Error("Invalid session file");
    const bytes = NodeFS.readFileSync(path, "utf8");
    if (bytes.length > MAX_FRAME) throw new Error("Session too large");
    const session = JSON.parse(bytes);
    if (session.runId !== runId || session.sessionId !== sessionId)
      throw new Error("Session scope mismatch");
    return session;
  }
  function save(session, create = false) {
    NodeFS.writeFileSync(sessionPath(session.sessionId), JSON.stringify(session), {
      mode: 0o600,
      flag: create ? "wx" : "w",
    });
  }
  function update(sessionId, value) {
    const session = readSession(sessionId);
    if (session.updates.length >= 256) throw new Error("History bound exceeded");
    session.updates.push(value);
    save(session);
    send({ jsonrpc: "2.0", method: "session/update", params: { sessionId, update: value } });
  }
  function finish(sessionId, reason) {
    const pending = active.get(sessionId);
    if (!pending) return;
    clearInterval(pending.timer);
    timers.delete(pending.timer);
    active.delete(sessionId);
    if (reason === "end_turn")
      update(sessionId, {
        sessionUpdate: "agent_message_chunk",
        messageId: pending.messageId,
        content: { type: "text", text: `ACK ${pending.marker}` },
      });
    update(sessionId, { sessionUpdate: "state_update", state: "idle", stopReason: reason });
    record({
      event: "complete",
      sessionId,
      ordinal: pending.ordinal,
      marker: pending.marker,
      reason,
    });
  }
  function shutdown(reason, code = 0) {
    if (stopped) return;
    for (const sessionId of active.keys()) finish(sessionId, "cancelled");
    record({ event: "close", reason });
    stopped = true;
    for (const timer of timers) clearInterval(timer);
    clearTimeout(deadline);
    NodeFS.closeSync(ledger);
    process.stdin.removeAllListeners("data");
    process.stdin.destroy();
    process.exitCode = code;
  }
  const deadline = setTimeout(() => shutdown("timeout", 1), timeoutMs);
  function handle(message) {
    const hasId = Object.hasOwn(message, "id");
    if (
      message.jsonrpc !== "2.0" ||
      typeof message.method !== "string" ||
      (hasId && typeof message.id !== "string" && !Number.isSafeInteger(message.id))
    )
      throw new Error("Invalid frame");
    const reply = (result) => {
      if (hasId) send({ jsonrpc: "2.0", id: message.id, result });
    };
    const params = message.params ?? {};
    const sessionId = params.sessionId;
    switch (message.method) {
      case "initialize":
        if (params.protocolVersion !== 2) throw new Error("ACP v2 required");
        record({ event: "initialize" });
        reply({
          protocolVersion: 2,
          info: { name: "jones-je06-stub", version: "1" },
          capabilities: { session: {} },
          authMethods: [],
        });
        break;
      case "session/new": {
        if (++sessionsCreated > 32 || typeof params.cwd !== "string")
          throw new Error("Invalid session creation");
        const id = `je06-${NodeCrypto.randomUUID()}`;
        save({ runId, sessionId: id, cwd: params.cwd, updates: [] }, true);
        record({ event: "session/new", sessionId: id });
        reply({ sessionId: id });
        update(id, { sessionUpdate: "available_commands_update", availableCommands: [] });
        break;
      }
      case "session/list": {
        const names = NodeFS.readdirSync(stateDir).filter((name) =>
          /^je06-[a-f0-9-]{36}\.json$/.test(name),
        );
        if (names.length > 64) throw new Error("Session limit exceeded");
        reply({
          sessions: names
            .map((name) => readSession(name.slice(0, -5)))
            .map((session) => ({ sessionId: session.sessionId, cwd: session.cwd })),
        });
        break;
      }
      case "session/resume": {
        const session = readSession(sessionId);
        if (params.replayFrom && params.replayFrom.type !== "start")
          throw new Error("Only start replay is supported");
        for (const value of session.updates)
          send({ jsonrpc: "2.0", method: "session/update", params: { sessionId, update: value } });
        record({ event: "resume", sessionId, replayed: session.updates.length });
        reply({});
        break;
      }
      case "session/close":
        readSession(sessionId);
        finish(sessionId, "cancelled");
        reply({});
        break;
      case "session/cancel":
        readSession(sessionId);
        finish(sessionId, "cancelled");
        reply({});
        break;
      case "session/prompt": {
        readSession(sessionId);
        if (active.has(sessionId) || !Array.isArray(params.prompt) || ++deliveries > 32)
          throw new Error("Invalid prompt state");
        const text = params.prompt
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("\n");
        const markers = ["M1", "M2-original", "M2-edited", "M3", "D"].filter((kind) =>
          text.includes(`JE06-${runId}-${kind}`),
        );
        if (markers.length !== 1) throw new Error("Expected one scoped synthetic prompt marker");
        const marker = `JE06-${runId}-${markers[0]}`;
        const ordinal = deliveries;
        // Every received prompt is recorded. Deduplicating here would hide a product regression.
        record({
          event: "delivery",
          sessionId,
          ordinal,
          marker,
          sha256: NodeCrypto.createHash("sha256").update(text).digest("hex"),
        });
        reply({});
        update(sessionId, { sessionUpdate: "state_update", state: "running" });
        const pending = {
          ordinal,
          marker,
          messageId: `ack-${NodeCrypto.randomUUID()}`,
          timer: null,
        };
        active.set(sessionId, pending);
        if (markers[0] === "M1") {
          pending.timer = setInterval(() => {
            try {
              const file = NodePath.join(stateDir, "release");
              if (!NodeFS.existsSync(file)) return;
              const stat = NodeFS.lstatSync(file);
              if (
                !stat.isFile() ||
                stat.isSymbolicLink() ||
                NodeFS.readFileSync(file, "utf8") !== runId
              )
                throw new Error("Invalid release gate");
              finish(sessionId, "end_turn");
            } catch {
              shutdown("gate-error", 1);
            }
          }, 25);
          timers.add(pending.timer);
        } else finish(sessionId, "end_turn");
        break;
      }
      default:
        if (hasId)
          send({
            jsonrpc: "2.0",
            id: message.id,
            error: { code: -32601, message: "Unsupported method" },
          });
    }
  }
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    if (stopped) return;
    buffer += chunk;
    if (Buffer.byteLength(buffer) > MAX_FRAME) {
      shutdown("frame-limit", 1);
      return;
    }
    let newline;
    while (!stopped && (newline = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      try {
        handle(JSON.parse(line));
      } catch {
        shutdown("protocol-error", 1);
      }
    }
  });
  process.stdin.on("end", () =>
    shutdown(buffer.trim() ? "truncated-frame" : "eof", buffer.trim() ? 1 : 0),
  );
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.stdout.on("error", () => shutdown("output-closed", 1));
}

if (
  process.argv[1] &&
  NodeURL.fileURLToPath(import.meta.url) === NodePath.resolve(process.argv[1])
) {
  try {
    startAgent(parseOptions(process.argv.slice(2)));
  } catch {
    process.stderr.write("JE06 stub refused invalid startup scope\n");
    process.exitCode = 1;
  }
}
