#!/usr/bin/env node
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeReadline from "node:readline";

const args = process.argv.slice(2);
const options = {};
for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === "app-server") continue;
  if (["--hold", "--version", "--defer-close", "--shared-producer"].includes(arg))
    options[arg] = true;
  else if (["--state-dir", "--account", "--mode", "--outcome"].includes(arg) && args[i + 1])
    options[arg] = args[++i];
  else throw new Error("Unsupported synthetic CLI argument");
}
if (options["--version"]) {
  process.stdout.write("codex-cli 0.162.0\n");
  process.exit(0);
}
const instance = options["--account"];
const mode = options["--mode"] ?? "chain";
if (options["--outcome"] !== undefined && options["--outcome"] !== "success")
  throw new Error("Unsupported synthetic outcome");
const requestedRoot = options["--state-dir"];
if (
  !/[ABCDE]/.test(instance ?? "") ||
  instance.length !== 1 ||
  !["chain", "all-unavailable"].includes(mode) ||
  !requestedRoot ||
  !NodePath.isAbsolute(requestedRoot)
)
  throw new Error("Require explicit absolute --state-dir and --account A/B/C/D/E");
if (!NodeFS.lstatSync(requestedRoot).isDirectory())
  throw new Error("State root must be a directory");
const root = NodeFS.realpathSync(requestedRoot);
if (root === NodePath.parse(root).root) throw new Error("State root must be isolated");
const processId = NodeCrypto.randomUUID();
const model = "synthetic-codex-model";
const safeToken = (value) => typeof value === "string" && /^[a-zA-Z0-9_-]{1,96}$/.test(value);
const currentInputMarker = (text) => {
  if (typeof text !== "string" || Buffer.byteLength(text) > 65536) return null;
  const directMarker = (value) =>
    /^SYNTHETIC_[A-Z0-9_]{1,80}$/.test(value)
      ? value
      : value === "Continue where you left off."
        ? "SYNTHETIC_CONTINUATION"
        : null;
  const direct = directMarker(text);
  if (direct !== null) return direct;
  // Unsupported history injection falls back to one text containing context and the current input.
  const separator = "\n\nUser message:\n";
  const boundary = text.lastIndexOf(separator);
  return text.startsWith("Context handoff (") && boundary > 0
    ? directMarker(text.slice(boundary + separator.length))
    : null;
};
const output = (name) => {
  if (!safeToken(name.replaceAll(".", "_"))) throw new Error("Invalid synthetic filename");
  const target = NodePath.resolve(root, name);
  if (NodePath.dirname(target) !== root) throw new Error("Output escaped state root");
  return target;
};
const read = (name) => {
  const fd = NodeFS.openSync(output(name), NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    if (!NodeFS.fstatSync(fd).isFile() || NodeFS.fstatSync(fd).size > 262144)
      throw new Error("Invalid synthetic state file");
    return NodeFS.readFileSync(fd, "utf8");
  } finally {
    NodeFS.closeSync(fd);
  }
};
const save = (name, value) => {
  const temporary = output(`tmp-${NodeCrypto.randomUUID()}.json`);
  try {
    NodeFS.writeFileSync(temporary, JSON.stringify(value), { flag: "wx", mode: 0o600 });
    NodeFS.renameSync(temporary, output(name));
  } finally {
    NodeFS.rmSync(temporary, { force: true });
  }
};
const traceName = `trace-${processId}.ndjson`;
NodeFS.writeFileSync(output(traceName), "", { flag: "wx", mode: 0o600 });
let sequence = 0;
let thread;
let active;
let timer;
let closing;
let closeTimer;
const ownedLocks = new Set();
const lockName = (id) => {
  if (!safeToken(id)) throw new Error("Invalid synthetic lock identity");
  return `lock-${id}`;
};
const takeWriter = (id) => {
  const name = lockName(id);
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      NodeFS.writeFileSync(output(name), JSON.stringify({ pid: process.pid, processId }), {
        flag: "wx",
        mode: 0o600,
      });
      ownedLocks.add(id);
      return true;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      const holder = JSON.parse(read(name));
      if (
        !Number.isSafeInteger(holder.pid) ||
        holder.pid <= 0 ||
        typeof holder.processId !== "string"
      )
        throw new Error("Invalid synthetic writer holder");
      if (holder.pid === process.pid && holder.processId === processId) return true;
      try {
        process.kill(holder.pid, 0);
        return false;
      } catch (probe) {
        if (probe.code !== "ESRCH") return false;
      }
      // Only a verified dead holder in this isolated fixture can be reclaimed.
      if (read(name) === JSON.stringify(holder)) NodeFS.unlinkSync(output(name));
    }
  }
  return false;
};
const releaseWriter = (id) => {
  if (!ownedLocks.has(id)) return;
  const holder = JSON.parse(read(lockName(id)));
  if (holder.pid !== process.pid || holder.processId !== processId)
    throw new Error("Synthetic writer ownership changed");
  NodeFS.unlinkSync(output(lockName(id)));
  ownedLocks.delete(id);
};
const trace = (method, extra = {}) => {
  if (++sequence > 512) throw new Error("Synthetic trace limit reached");
  const fd = NodeFS.openSync(
    output(traceName),
    NodeFS.constants.O_APPEND | NodeFS.constants.O_WRONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    NodeFS.writeSync(
      fd,
      JSON.stringify({
        synthetic: true,
        processId,
        instance,
        sequence,
        method,
        nativeThreadId: thread?.id ?? null,
        nativeTurnId: active?.id ?? null,
        ...extra,
      }) + "\n",
    );
  } finally {
    NodeFS.closeSync(fd);
  }
};
const write = (message) => process.stdout.write(JSON.stringify(message) + "\n");
const notify = (method, params) => write({ method, params });
const persist = () => save(`thread-${thread.id}.json`, thread);
const load = (id) => {
  if (!safeToken(id)) throw new Error("Invalid synthetic thread ID");
  const value = JSON.parse(read(`thread-${id}.json`));
  if (value.synthetic !== true || value.id !== id || !Array.isArray(value.turns))
    throw new Error("Invalid synthetic thread state");
  return value;
};
const threadResult = () => ({
  thread,
  model,
  modelProvider: "openai",
  serviceTier: null,
  cwd: thread.cwd,
  runtimeWorkspaceRoots: [thread.cwd],
  instructionSources: [],
  disabledPluginIds: [],
  approvalPolicy: "never",
  approvalsReviewer: "user",
  sandbox: { type: "dangerFullAccess" },
  reasoningEffort: "medium",
  activePermissionProfile: null,
  multiAgentMode: "explicitRequestOnly",
});
const finish = (status) => {
  if (!active) return;
  clearTimeout(timer);
  let error = null;
  if (status !== "interrupted") {
    if (options["--outcome"] !== "success" && instance === "A")
      error = {
        message: "SYNTHETIC_A_USAGE_LIMIT",
        codexErrorInfo: "usageLimitExceeded",
        additionalDetails: null,
      };
    else if (options["--outcome"] !== "success" && (instance === "B" || mode === "all-unavailable"))
      error = {
        message: `SYNTHETIC_${instance}_UNAVAILABLE`,
        codexErrorInfo: { responseStreamDisconnected: { httpStatusCode: 503 } },
        additionalDetails: null,
      };
    if (error) {
      status = "failed";
      notify("error", { threadId: thread.id, turnId: active.id, error, willRetry: false });
    } else {
      const item = {
        type: "agentMessage",
        id: NodeCrypto.randomUUID(),
        text: `SYNTHETIC_${instance}_COMPLETED`,
        phase: "final_answer",
        memoryCitation: null,
        delivery: null,
        questions: null,
      };
      notify("item/started", {
        threadId: thread.id,
        turnId: active.id,
        item: { ...item, text: "" },
      });
      notify("item/agentMessage/delta", {
        threadId: thread.id,
        turnId: active.id,
        itemId: item.id,
        delta: item.text,
      });
      active.items.push(item);
      notify("item/completed", { threadId: thread.id, turnId: active.id, item });
    }
  }
  Object.assign(active, {
    status,
    error,
    completedAt: Math.floor(Date.now() / 1000),
    durationMs: 1,
  });
  thread.status = { type: "idle" };
  persist();
  trace("synthetic/terminal", {
    status,
    marker:
      error?.message ??
      (status === "interrupted" ? "SYNTHETIC_INTERRUPTED" : `SYNTHETIC_${instance}_COMPLETED`),
  });
  notify("turn/completed", { threadId: thread.id, turn: active });
  active = undefined;
};
const release = () => {
  if (!active || !options["--hold"]) return;
  try {
    if (read(`release-${active.id}`) === "SYNTHETIC_RELEASE\n") finish("completed");
  } catch (error) {
    if (error.code !== "ENOENT") fail();
  }
};
const closeThread = () => {
  if (!closing) return;
  if (options["--defer-close"]) {
    try {
      if (read(`close-${closing}`) !== "SYNTHETIC_CLOSE\n") return;
    } catch (error) {
      if (error.code === "ENOENT") return;
      return fail();
    }
  }
  clearTimeout(closeTimer);
  const id = closing;
  closing = undefined;
  releaseWriter(id);
  trace("synthetic/closed", { nativeThreadId: id });
  notify("thread/closed", { threadId: id });
};
const watcher = NodeFS.watch(root, () => {
  try {
    release();
    closeThread();
  } catch {
    fail();
  }
});
const fail = () => {
  clearTimeout(timer);
  clearTimeout(closeTimer);
  process.stderr.write("Synthetic peer state or protocol failure\n");
  process.exitCode = 1;
  lines.close();
  process.stdin.destroy();
};
const handle = ({ id, method, params = {} }) => {
  const respond = (result) => write({ id, result });
  if (method === "initialized") return;
  if (id === undefined || typeof method !== "string") throw new Error("Invalid request");
  if (method === "initialize")
    return respond({
      userAgent: "synthetic-codex/0.162.0",
      codexHome: root,
      platformFamily: "unix",
      platformOs: "linux",
    });
  if (method === "account/read")
    return respond({
      account: {
        type: "chatgpt",
        email: `synthetic-${instance.toLowerCase()}@example.invalid`,
        planType: "plus",
      },
      requiresOpenaiAuth: false,
    });
  if (method === "account/rateLimits/read")
    return write({ id, error: { code: -32000, message: "Synthetic usage unavailable" } });
  if (method === "skills/list") return respond({ data: [] });
  if (method === "thread/goal/get") return respond({ goal: null });
  if (method === "model/list")
    return respond({
      data: [
        {
          id: model,
          model,
          displayName: "Synthetic Codex",
          description: "No inference",
          hidden: false,
          isDefault: true,
          defaultReasoningEffort: "medium",
          supportedReasoningEfforts: ["low", "medium", "high"].map((reasoningEffort) => ({
            reasoningEffort,
            description: "Synthetic",
          })),
          serviceTiers: [
            { id: "priority", name: "Synthetic priority", description: "No inference" },
          ],
        },
      ],
      nextCursor: null,
    });
  if (method === "thread/start") {
    if (active) throw new Error("Turn already active");
    thread = {
      synthetic: true,
      id: NodeCrypto.randomUUID(),
      sessionId: null,
      createdAt: 1,
      updatedAt: 1,
      cwd: typeof params.cwd === "string" && NodePath.isAbsolute(params.cwd) ? params.cwd : root,
      cliVersion: "0.162.0",
      source: "cli",
      modelProvider: "openai",
      preview: "",
      ephemeral: false,
      projectId: null,
      status: { type: "idle" },
      turns: [],
    };
    if (!takeWriter(thread.id)) throw new Error("New synthetic conversation already owned");
    thread.sessionId = thread.id;
    persist();
    trace(method);
    return respond(threadResult());
  }
  if (method === "thread/read") {
    const snapshot = load(params.threadId);
    trace(method, { nativeThreadId: snapshot.id });
    return respond({ thread: snapshot });
  }
  if (method === "thread/resume") {
    if (active) throw new Error("Turn already active");
    const resumed = load(params.threadId);
    if (!takeWriter(resumed.id)) {
      trace("synthetic/writer-refused", { nativeThreadId: resumed.id });
      return write({
        id,
        error: {
          code: -32000,
          message: "Thread already has an active writer in another Codex process",
        },
      });
    }
    thread = resumed;
    trace(method);
    return respond(threadResult());
  }
  if (method === "turn/start") {
    if (!thread || params.threadId !== thread.id || active || thread.turns.length >= 64)
      throw new Error("Invalid synthetic turn state");
    const input = params.input;
    if (
      !Array.isArray(input) ||
      input.length !== 1 ||
      input[0]?.type !== "text" ||
      currentInputMarker(input[0].text) === null
    )
      throw new Error("Require synthetic input marker");
    const selection = (value) => (safeToken(value) ? value : null);
    active = {
      id: NodeCrypto.randomUUID(),
      items: [
        {
          type: "userMessage",
          id: NodeCrypto.randomUUID(),
          content: [{ type: "text", text: input[0].text, text_elements: [] }],
        },
      ],
      status: "inProgress",
      error: null,
      startedAt: Math.floor(Date.now() / 1000),
      completedAt: null,
      durationMs: null,
    };
    thread.turns.push(active);
    thread.status = { type: "active", activeFlags: [] };
    persist();
    trace(method, {
      model: selection(params.model),
      effort: selection(params.effort),
      serviceTier: selection(params.serviceTier),
      inputMarker: currentInputMarker(input[0].text),
      recoveredContext: input[0].text.startsWith("Context handoff ("),
    });
    respond({ turn: active });
    notify("turn/started", { threadId: thread.id, turn: active });
    if (options["--hold"]) {
      trace("synthetic/held");
      timer = setTimeout(fail, 30000);
      release();
    } else
      timer = setTimeout(() => {
        try {
          finish("completed");
        } catch {
          fail();
        }
      }, 0);
    return;
  }
  if (method === "thread/loaded/list")
    return respond({
      data: [...ownedLocks, ...(options["--shared-producer"] ? ["synthetic-sibling"] : [])],
      nextCursor: null,
    });
  if (method === "thread/backgroundTerminals/list") return respond({ data: [], nextCursor: null });
  if (method === "thread/unsubscribe") {
    if (!thread || params.threadId !== thread.id) throw new Error("Unknown synthetic thread");
    trace("synthetic/unsubscribe");
    respond({ status: "unsubscribed" });
    closing = thread.id;
    if (options["--defer-close"]) closeTimer = setTimeout(fail, 60000);
    closeThread();
    return;
  }
  if (method === "turn/interrupt") {
    if (!active || params.threadId !== thread.id || params.turnId !== active.id)
      throw new Error("No matching synthetic active turn");
    trace(method);
    respond({});
    finish("interrupted");
    return;
  }
  write({ id, error: { code: -32601, message: "Unsupported synthetic method" } });
};
const lines = NodeReadline.createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on("line", (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    write({ id: null, error: { code: -32700, message: "Invalid JSON" } });
    return;
  }
  try {
    handle(message);
  } catch (error) {
    trace("synthetic/request-rejected", {
      requestMethod: typeof message?.method === "string" ? message.method.slice(0, 80) : null,
      rejectionReason: error instanceof Error ? error.message.slice(0, 200) : "Unknown rejection",
      inputCount: Array.isArray(message?.params?.input) ? message.params.input.length : null,
      textLength:
        typeof message?.params?.input?.[0]?.text === "string"
          ? message.params.input[0].text.length
          : null,
    });
    write({
      id: message?.id ?? null,
      error: { code: -32602, message: "Invalid synthetic request or state" },
    });
  }
});
let cleaned = false;
const cleanup = () => {
  if (cleaned) return;
  cleaned = true;
  clearTimeout(timer);
  clearTimeout(closeTimer);
  watcher.close();
  for (const id of ownedLocks) releaseWriter(id);
  trace("synthetic/exit");
};
lines.on("close", () => {
  cleanup();
  process.stdin.destroy();
});
process.on("SIGTERM", () => {
  cleanup();
  process.exit(0);
});
process.on("SIGINT", () => {
  cleanup();
  process.exit(0);
});
