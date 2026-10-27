import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import { installCheckpointWorktree } from "./thread-self-configure-support.mjs";
export const fixtureRoot = "/scratch/home/native-self-config";
export const selection = {
  instanceId: "native-fixture-a",
  model: "native-model-a",
  options: [
    {
      id: "reasoningEffort",
      value: "high",
    },
    {
      id: "serviceTier",
      value: "default",
    },
  ],
};
export const fixture = {
  once: "MONITOR_NATIVE_APPROVE_ONCE",
  deny: "MONITOR_NATIVE_DENY",
  questions: "MONITOR_NATIVE_QUESTIONS",
  overlap: "MONITOR_NATIVE_OVERLAPPING_REQUESTS",
  plan: "MONITOR_NATIVE_PLAN",
  markdown:
    "# Synthetic monitor plan\n\nKeep the synthetic fixture isolated and report its completion.",
  custom: "MONITOR_NATIVE_CUSTOM_ANSWER",
  questionsPayload: [
    {
      id: "route",
      header: "Route",
      question: "Which synthetic route should continue?",
      isOther: true,
      options: [
        {
          label: "Isolated route",
          description: "Stay inside this fixture",
        },
        {
          label: "Alternate route",
          description: "Another synthetic choice",
        },
      ],
    },
    {
      id: "note",
      header: "Note",
      question: "Provide the synthetic verification note.",
      isOther: true,
      options: [
        {
          label: "Offered note",
          description: "An optional synthetic note",
        },
      ],
    },
  ],
};

// Self-contained so the installed peer and the protocol tests exercise identical code.
export function createMonitorProtocol({ write, trace, id, now }, spec) {
  const cwd = "/scratch/workspace/native-self-config-execution";
  const root = "/scratch/home/native-self-config";
  const model = "native-model-a";
  const implementation = "PLEASE IMPLEMENT THIS PLAN:\n" + spec.markdown.trim();
  let nativeThreadId,
    turn,
    pending,
    suspendedInput,
    held = false,
    requestSequence = 10000;
  const admitted = new Set();
  const notify = (method, params) =>
    write({
      method,
      params,
    });
  const thread = () => ({
    id: nativeThreadId,
    sessionId: nativeThreadId,
    environments: [],
    extra: null,
    forkedFromId: null,
    parentThreadId: null,
    preview: "",
    ephemeral: false,
    section: null,
    sectionEnteredAt: null,
    projectId: null,
    historyMode: "paginated",
    modelProvider: "openai",
    model,
    reasoningEffort: "high",
    createdAt: 1,
    updatedAt: 1,
    recencyAt: 1,
    status: turn
      ? {
          type: "active",
          activeFlags: [],
        }
      : {
          type: "idle",
        },
    path: root + "/account/fixture.jsonl",
    cwd,
    cliVersion: "0.162.0",
    originator: "synthetic-monitor",
    source: "cli",
    canAcceptDirectInput: true,
    threadSource: null,
    agentNickname: null,
    agentRole: null,
    gitInfo: null,
    name: null,
    daybreakEnabled: null,
    turns: [],
  });
  const itemEvent = (method, item) =>
    notify(method, {
      threadId: nativeThreadId,
      turnId: turn.id,
      item,
      ...(method === "item/started"
        ? {
            startedAtMs: now(),
          }
        : {
            completedAtMs: now(),
          }),
    });
  const complete = () => {
    if (!turn || pending || suspendedInput)
      throw new Error("Synthetic completion without a resolved turn");
    const text = "ACK " + turn.marker;
    const item = {
      type: "agentMessage",
      id: id(),
      text,
      phase: "final_answer",
      memoryCitation: null,
      delivery: null,
      questions: null,
    };
    itemEvent("item/started", {
      ...item,
      text: "",
    });
    notify("item/agentMessage/delta", {
      threadId: nativeThreadId,
      turnId: turn.id,
      itemId: item.id,
      delta: text,
    });
    itemEvent("item/completed", item);
    const { marker, ...wireTurn } = turn;
    notify("turn/completed", {
      threadId: nativeThreadId,
      turn: {
        ...wireTurn,
        items: [item],
        status: "completed",
        completedAt: Math.floor(now() / 1000),
        durationMs: 1,
      },
    });
    trace({
      event: "completed",
      nativeThreadId,
      turnId: turn.id,
      marker,
    });
    turn = null;
    held = false;
  };
  const request = (method, itemId, extra, expected) => {
    const rpcId = ++requestSequence;
    pending = {
      rpcId,
      itemId,
      method,
      expected,
    };
    const params = {
      threadId: nativeThreadId,
      turnId: turn.id,
      itemId,
      ...extra,
    };
    trace({
      event: "request",
      nativeThreadId,
      turnId: turn.id,
      rpcId,
      itemId,
      method,
    });
    write({
      id: rpcId,
      method,
      params,
    });
  };
  const equal = (a, b) => {
    if (a === b) return true;
    if (
      a === null ||
      b === null ||
      typeof a !== "object" ||
      typeof b !== "object" ||
      Array.isArray(a) !== Array.isArray(b)
    )
      return false;
    const keys = Object.keys(a);
    return (
      keys.length === Object.keys(b).length &&
      keys.every((key) => Object.hasOwn(b, key) && equal(a[key], b[key]))
    );
  };
  function handle(message) {
    if (!message || typeof message !== "object") throw new Error("Invalid synthetic frame");
    if (message.method === undefined) {
      if (
        !pending ||
        message.id !== pending.rpcId ||
        message.error ||
        !equal(message.result, pending.expected)
      )
        throw new Error("Unexpected synthetic request response");
      trace({
        event: "response",
        nativeThreadId,
        turnId: turn.id,
        rpcId: pending.rpcId,
        itemId: pending.itemId,
        method: pending.method,
        result: pending.expected,
      });
      pending = suspendedInput;
      suspendedInput = null;
      if (!pending) complete();
      return;
    }
    const { method, params = {}, id: rpcId } = message;
    if (method === "initialized") return;
    if (method === "turn/steer" || method === "turn/interrupt") {
      trace({
        event: "forbidden",
        method,
      });
      throw new Error("Synthetic monitor must never steer or interrupt");
    }
    const reply = (result) =>
      write({
        id: rpcId,
        result,
      });
    if (method === "initialize")
      return reply({
        userAgent: "synthetic-monitor/0.162.0",
        codexHome: root + "/account",
        platformFamily: "unix",
        platformOs: "linux",
      });
    if (method === "account/read")
      return reply({
        account: {
          type: "apiKey",
        },
        requiresOpenaiAuth: false,
      });
    if (method === "account/rateLimits/read")
      return write({
        id: rpcId,
        error: {
          code: -32000,
          message: "Synthetic usage unavailable",
        },
      });
    if (method === "skills/list")
      return reply({
        data: [],
      });
    if (method === "model/list")
      return reply({
        data: [
          {
            id: model,
            model,
            displayName: "Synthetic monitor model",
            description: "No live inference",
            hidden: false,
            isDefault: true,
            defaultReasoningEffort: "high",
            supportedReasoningEfforts: [
              {
                reasoningEffort: "high",
                description: "Synthetic high",
              },
            ],
            serviceTiers: [
              {
                id: "flex",
                name: "Synthetic unused tier",
                description: "",
              },
            ],
          },
        ],
        nextCursor: null,
      });
    if (method === "config/read" || method === "server/diagnostics") {
      trace({ event: "unsupported-read", method });
      return write({
        id: rpcId,
        error: { code: -32601, message: "Synthetic optional read unavailable" },
      });
    }
    if (method === "thread/start" || method === "thread/resume") {
      if (nativeThreadId || params.cwd !== cwd || (params.model != null && params.model !== model))
        throw new Error("Unexpected synthetic session context");
      nativeThreadId = params.threadId ?? id();
      trace({
        event: "session",
        method,
        nativeThreadId,
        cwd,
      });
      return reply({
        thread: thread(),
        model,
        modelProvider: "openai",
        serviceTier: null,
        disabledPluginIds: [],
        cwd,
        runtimeWorkspaceRoots: [cwd],
        instructionSources: [],
        approvalPolicy: "on-request",
        approvalsReviewer: "user",
        sandbox: {
          type: "dangerFullAccess",
        },
        activePermissionProfile: null,
        reasoningEffort: "high",
        multiAgentMode: "explicitRequestOnly",
      });
    }
    if (method === "thread/read") {
      if (params.threadId !== nativeThreadId) throw new Error("Foreign synthetic thread read");
      return reply({
        thread: thread(),
      });
    }
    if (method === "thread/goal/get")
      return reply({
        goal: null,
      });
    if (method !== "turn/start")
      throw new Error(
        "Unsupported synthetic method: " + (/^[a-zA-Z/]+$/.test(method) ? method : "invalid"),
      );
    if (
      !nativeThreadId ||
      params.threadId !== nativeThreadId ||
      turn ||
      pending ||
      !Array.isArray(params.input) ||
      params.input.some((p) => p.type !== "text")
    )
      throw new Error("Unexpected synthetic turn context");
    const text = params.input.map((p) => p.text).join("\n");
    const marker = text === implementation ? "MONITOR_NATIVE_IMPLEMENTATION" : text;
    const expectedOrder = [
      spec.once,
      spec.deny,
      spec.questions,
      spec.plan,
      "MONITOR_NATIVE_IMPLEMENTATION",
    ];
    if (
      (marker !== expectedOrder[admitted.size] &&
        !(marker === spec.overlap && admitted.size === 0)) ||
      admitted.has(marker)
    )
      throw new Error("Unexpected or duplicate synthetic input");
    if (
      (params.model != null && params.model !== model) ||
      params.collaborationMode?.mode === "plan"
    )
      throw new Error("Unexpected synthetic model or interaction mode");
    admitted.add(marker);
    turn = {
      id: id(),
      marker,
      items: [],
      itemsView: "notLoaded",
      status: "inProgress",
      error: null,
      startedAt: Math.floor(now() / 1000),
      completedAt: null,
      durationMs: null,
    };
    trace({
      event: "start",
      nativeThreadId,
      turnId: turn.id,
      marker,
      model: params.model ?? null,
      collaborationMode: params.collaborationMode?.mode ?? null,
    });
    const { marker: _, ...wireTurn } = turn;
    reply({
      turn: wireTurn,
    });
    notify("turn/started", {
      threadId: nativeThreadId,
      turn: wireTurn,
    });
    if (marker === spec.once || marker === spec.deny) {
      request(
        "item/commandExecution/requestApproval",
        id(),
        {
          command: "synthetic-monitor-noop",
          reason: "Synthetic monitor approval; no command will execute",
          cwd,
          startedAtMs: now(),
        },
        {
          decision: marker === spec.once ? "accept" : "decline",
        },
      );
    } else if (marker === spec.questions || marker === spec.overlap) {
      request(
        "item/tool/requestUserInput",
        id(),
        {
          isBlocking: marker !== spec.overlap,
          questions: spec.questionsPayload,
        },
        {
          answers: {
            route: {
              answers: ["Isolated route"],
            },
            note: {
              answers: [spec.custom],
            },
          },
        },
      );
    } else if (marker === spec.plan) {
      const item = {
        type: "plan",
        id: id(),
        text: spec.markdown,
      };
      itemEvent("item/started", {
        ...item,
        text: "",
      });
      notify("item/plan/delta", {
        threadId: nativeThreadId,
        turnId: turn.id,
        itemId: item.id,
        delta: item.text,
      });
      itemEvent("item/completed", item);
      trace({
        event: "plan",
        nativeThreadId,
        turnId: turn.id,
        itemId: item.id,
      });
      complete();
    } else held = true;
  }
  return {
    handle,
    startApproval() {
      if (turn?.marker !== spec.overlap || pending?.method !== "item/tool/requestUserInput")
        throw new Error("Synthetic overlap requires its unanswered input");
      suspendedInput = pending;
      request(
        "item/commandExecution/requestApproval",
        id(),
        { command: "synthetic-monitor-noop", reason: "Approval alongside unanswered input", cwd },
        { decision: "accept" },
      );
    },
    release() {
      if (!held) throw new Error("Synthetic release without held implementation");
      complete();
    },
  };
}
export async function runMonitorPeer(makeProtocol, spec) {
  const fs = await import("node:fs");
  const readline = await import("node:readline");
  const { randomUUID } = await import("node:crypto");
  if (process.env.HOME !== "/scratch/home") throw new Error("Synthetic namespace required");
  if (process.argv.includes("--version")) {
    process.stdout.write("codex-cli 0.162.0\n");
    return;
  }
  if (process.argv[2] !== "app-server") throw new Error("Synthetic app-server only");
  const root = "/scratch/home/native-self-config";
  const processId = randomUUID();
  const startTicks = fs.readFileSync("/proc/self/stat", "utf8").split(") ").at(-1).split(" ")[19];
  const path = root + "/monitor-trace-" + processId + ".ndjson";
  let sequence = 0,
    bytes = 0,
    closed = false;
  const trace = (event) => {
    const line =
      JSON.stringify({
        synthetic: true,
        processId,
        pid: process.pid,
        startTicks,
        sequence: ++sequence,
        ...event,
      }) + "\n";
    bytes += Buffer.byteLength(line);
    if (bytes > 65536 || sequence > 128) throw new Error("Synthetic ledger bound exceeded");
    fs.appendFileSync(path, line, {
      mode: 0o600,
    });
  };
  trace({
    event: "spawn",
  });
  const protocol = makeProtocol(
    {
      write: (frame) => process.stdout.write(JSON.stringify(frame) + "\n"),
      trace,
      id: randomUUID,
      now: Date.now,
    },
    spec,
  );
  const input = readline.createInterface({
    input: process.stdin,
  });
  const finish = (event, code = 0) => {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    trace({
      event,
    });
    input.close();
    process.exit(code);
  };
  const failed = (error) => {
    trace({
      event: "failure-reason",
      reason: String(error?.message ?? "Synthetic fixture failed").slice(0, 256),
    });
    finish("failure", 1);
  };
  let released = false,
    approvalStarted = false;
  const timer = setInterval(() => {
    try {
      if (fs.existsSync(root + "/monitor-shutdown")) return finish("shutdown");
      if (!approvalStarted && fs.existsSync(root + "/monitor-overlap-approval")) {
        approvalStarted = true;
        protocol.startApproval();
      }
      if (!released && fs.existsSync(root + "/monitor-release")) {
        released = true;
        protocol.release();
      }
    } catch (error) {
      failed(error);
    }
  }, 50);
  input.on("line", (line) => {
    try {
      if (Buffer.byteLength(line) > 1048576) throw new Error("Frame bound");
      protocol.handle(JSON.parse(line));
    } catch (error) {
      failed(error);
    }
  });
  input.on("close", () => finish("stdin-closed"));
  process.once("SIGTERM", () => finish("sigterm"));
  process.once("SIGINT", () => finish("sigint"));
}
export async function assertPrivateNamespace(workspace) {
  if (workspace !== "/scratch/workspace" || process.env.HOME !== "/scratch/home")
    throw new Error("Private native namespace required");
  for (const path of ["/scratch/home", workspace]) {
    if (!(await NodeFSP.lstat(path)).isDirectory() || (await NodeFSP.realpath(path)) !== path)
      throw new Error("Private namespace must not be redirected");
  }
}
export async function installMonitorFixture(workspace) {
  await assertPrivateNamespace(workspace);
  await NodeFSP.mkdir(fixtureRoot, {
    mode: 0o700,
  });
  try {
    await NodeFSP.mkdir(fixtureRoot + "/account", {
      mode: 0o700,
    });
    const source =
      "(" +
      runMonitorPeer.toString() +
      ")(" +
      createMonitorProtocol.toString() +
      "," +
      JSON.stringify(fixture) +
      ").catch(() => { process.exitCode = 1; });\n";
    await NodeFSP.writeFile(fixtureRoot + "/monitor-peer.mjs", source, {
      flag: "wx",
      mode: 0o600,
    });
    await NodeFSP.writeFile(
      fixtureRoot + "/codex-A",
      '#!/bin/sh\nexec /usr/bin/env -i HOME=/scratch/home PATH=/usr/bin:/bin /usr/bin/node /scratch/home/native-self-config/monitor-peer.mjs "$@"\n',
      {
        flag: "wx",
        mode: 0o700,
      },
    );
    const git = await installCheckpointWorktree(workspace);
    return {
      git,
      peerSha256: NodeCrypto.createHash("sha256").update(source).digest("hex"),
      engine: "synthetic",
      network: "none",
    };
  } catch (error) {
    await NodeFSP.rm(fixtureRoot, {
      recursive: true,
    });
    throw error;
  }
}
export async function readMonitorLedger() {
  await assertPrivateNamespace("/scratch/workspace");
  const names = (await NodeFSP.readdir(fixtureRoot))
    .filter((n) => /^monitor-trace-[a-f0-9-]+\.ndjson$/.test(n))
    .sort();
  if (names.length > 32) throw new Error("Synthetic process count bound exceeded");
  const rows = [];
  let bytes = 0;
  for (const name of names) {
    const path = fixtureRoot + "/" + name,
      stat = await NodeFSP.lstat(path);
    bytes += stat.size;
    if (!stat.isFile() || stat.size > 65536 || bytes > 262144)
      throw new Error("Synthetic trace bound exceeded");
    for (const line of (await NodeFSP.readFile(path, "utf8")).trim().split("\n").filter(Boolean)) {
      const row = JSON.parse(line);
      if (
        row.synthetic !== true ||
        !Number.isInteger(row.pid) ||
        row.pid <= 1 ||
        !/^\d+$/.test(row.startTicks) ||
        !/^[a-f0-9-]+$/.test(row.processId)
      )
        throw new Error("Invalid synthetic process receipt");
      rows.push(row);
    }
  }
  return rows;
}
export async function waitForMonitor(read, label, timeout = 30000) {
  const end = performance.now() + timeout;
  while (performance.now() < end) {
    const value = await read();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Synthetic monitor deadline: " + label);
}
export async function releaseMonitorWork() {
  await assertPrivateNamespace("/scratch/workspace");
  await NodeFSP.writeFile(fixtureRoot + "/monitor-release", "synthetic\n", {
    flag: "wx",
    mode: 0o600,
  });
}
export async function closeMonitorFixture(preserve) {
  await assertPrivateNamespace("/scratch/workspace");
  const before = await readMonitorLedger();
  await preserve({
    phase: "before-cleanup",
    ledger: before,
  });
  await NodeFSP.writeFile(fixtureRoot + "/monitor-shutdown", "synthetic\n", {
    flag: "wx",
    mode: 0o600,
  });
  const identities = [
    ...new Map(
      before.map((row) => [
        row.processId,
        {
          pid: row.pid,
          startTicks: row.startTicks,
        },
      ]),
    ).values(),
  ];
  await waitForMonitor(
    async () => {
      for (const identity of identities) {
        try {
          const stat = await NodeFSP.readFile("/proc/" + identity.pid + "/stat", "utf8");
          const fields = stat.split(") ").at(-1).split(" ");
          if (fields[19] === identity.startTicks && fields[0] !== "Z") return false;
        } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
      }
      return true;
    },
    "captured peer exit",
    10000,
  );
  const after = await readMonitorLedger();
  if (after.some((row) => !before.some((prior) => prior.processId === row.processId)))
    throw new Error("Synthetic peer creation raced cleanup; outer namespace owner must reconcile");
  const result = {
    phase: "after-cleanup",
    identities,
    stopped: true,
    ledger: after,
    workspaceCleanupOwner: "outer harness namespace owner",
  };
  await preserve(result);
  await NodeFSP.rm(fixtureRoot, {
    recursive: true,
  });
  try {
    await NodeFSP.access(fixtureRoot);
    throw new Error("Synthetic fixture cleanup incomplete");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  return result;
}
