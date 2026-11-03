import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";

export const fixtureRoot = "/scratch/home/native-self-config";
export const selectionA = {
  instanceId: "native-fixture-a",
  model: "native-model-a",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "default" },
  ],
};
export const selectionB = {
  instanceId: "native-fixture-b",
  model: "native-model-b",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "default" },
  ],
};

export function issuedConnection(params) {
  const config = params?.config?.["mcp_servers.t3-code"];
  if (!config) throw new Error("App-issued MCP connection missing from thread/start");
  const url = new URL(config.url);
  if (
    url.protocol !== "http:" ||
    url.hostname !== "127.0.0.1" ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    typeof config.http_headers?.Authorization !== "string" ||
    !config.http_headers.Authorization.startsWith("Bearer ")
  )
    throw new Error("Invalid app-issued private MCP connection");
  return { url: url.href, authorization: config.http_headers.Authorization };
}

export function decodeMcpResponse(text, id) {
  const frames = text.trim().startsWith("{")
    ? [JSON.parse(text)]
    : text.split(/\r?\n\r?\n/).flatMap((block) => {
        const data = block
          .split(/\r?\n/)
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trim())
          .join("\n");
        return data ? [JSON.parse(data)] : [];
      });
  const frame = frames.find((value) => value.id === id);
  if (!frame || frame.error) throw new Error("MCP protocol response rejected");
  return frame.result;
}

export function assertContinuity({ before, after, projection, trace, nativeThreadId, processId }) {
  const prefix = "SC-COR-001 ordinary UI follow-up continuity";
  NodeAssert.ok(before.executionPrecondition, prefix + ": initial execution");
  NodeAssert.deepEqual(
    after.executionPrecondition,
    before.executionPrecondition,
    prefix + ": same run/attempt/session/runtime generation",
  );
  NodeAssert.deepEqual(
    after.currentExecution.requestedSelection,
    selectionA,
    prefix + ": executing A",
  );
  NodeAssert.deepEqual(after.savedSelection, selectionB, prefix + ": saved B");
  NodeAssert.equal(projection.runs.length, 1, prefix + ": no replacement run");
  NodeAssert.equal(projection.attempts.length, 1, prefix + ": no replacement attempt");
  const events = trace.filter((entry) => entry.nativeThreadId === nativeThreadId);
  NodeAssert.equal(
    events.filter((entry) => entry.method === "turn/start").length,
    1,
    prefix + ": exactly one start",
  );
  NodeAssert.equal(
    events.filter((entry) => entry.method === "thread/start" || entry.method === "thread/resume")
      .length,
    1,
    prefix + ": no duplicate session start/resume",
  );
  const steers = events.filter((entry) => entry.method === "turn/steer");
  NodeAssert.equal(steers.length, 1, prefix + ": exactly one steering delivery");
  NodeAssert.equal(steers[0].processId, processId, prefix + ": original subprocess");
  NodeAssert.equal(steers[0].input, "NATIVE_ORDINARY_FOLLOWUP", prefix + ": actual composer text");
  NodeAssert.equal(
    events.filter((entry) => entry.method === "turn/interrupt").length,
    0,
    prefix + ": no interrupt",
  );
  NodeAssert.equal(
    new Set(
      events
        .filter((entry) =>
          ["thread/start", "thread/resume", "turn/start", "turn/steer"].includes(entry.method),
        )
        .map((entry) => entry.processId),
    ).size,
    1,
    prefix + ": no replacement process",
  );
}

export function assertFreshAdmission({ projection, trace, nativeThreadId, priorRunId }) {
  const run = projection.runs.find((entry) => entry.id !== priorRunId);
  NodeAssert.equal(projection.runs.length, 2);
  NodeAssert.ok(run, "fresh run admitted");
  NodeAssert.deepEqual(run.modelSelection, selectionB);
  const starts = trace.filter(
    (entry) =>
      entry.nativeThreadId === nativeThreadId &&
      entry.method === "turn/start" &&
      entry.input === "NATIVE_FRESH_B",
  );
  NodeAssert.equal(starts.length, 1, "exactly one fresh outbound start");
  NodeAssert.equal(starts[0].instance, "B");
  NodeAssert.equal(starts[0].model, selectionB.model);
  NodeAssert.equal(starts[0].effort, "high");
  NodeAssert.equal(starts[0].serviceTier, "default", "explicit Standard tier on the wire");
  NodeAssert.equal(starts[0].collaborationModel, selectionB.model);
  NodeAssert.equal(starts[0].collaborationEffort, "high");
  return run;
}

// Serialized into a disposable executable. Nothing reads ambient account state.
export async function syntheticCodexPeer(connectionFromStart, decodeResponse) {
  const fs = await import("node:fs");
  const readline = await import("node:readline");
  const { randomUUID } = await import("node:crypto");
  const root = "/scratch/home/native-self-config";
  const instance = process.argv[2];
  if (!["A", "B"].includes(instance)) throw new Error("Unknown synthetic instance");
  if (process.argv.includes("--version")) {
    process.stdout.write("codex-cli 0.162.0\n");
    return;
  }
  const processId = randomUUID();
  const model = instance === "A" ? "native-model-a" : "native-model-b";
  const selection = (letter) => ({
    instanceId: "native-fixture-" + letter.toLowerCase(),
    model: "native-model-" + letter.toLowerCase(),
    options: [
      { id: "reasoningEffort", value: "high" },
      { id: "serviceTier", value: "default" },
    ],
  });
  const tracePath = root + "/trace-" + processId + ".ndjson";
  let sequence = 0,
    nativeThreadId,
    issuedCwd,
    connection,
    sessionId,
    activeTurn,
    mcpId = 0;
  const trace = (event) =>
    fs.appendFileSync(
      tracePath,
      JSON.stringify({
        synthetic: true,
        processId,
        instance,
        sequence: ++sequence,
        nativeThreadId: nativeThreadId ?? null,
        ...event,
      }) + "\n",
      { mode: 0o600 },
    );
  const save = (name, value) => {
    const file = root + "/" + name + "-" + nativeThreadId + ".json";
    fs.writeFileSync(file + ".tmp", JSON.stringify(value, null, 2), { mode: 0o600 });
    fs.renameSync(file + ".tmp", file);
  };
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const bounded = async (read, label) => {
    const end = Date.now() + 45000;
    while (Date.now() < end) {
      const value = await read();
      if (value) return value;
      await delay(50);
    }
    throw new Error("Synthetic barrier deadline: " + label);
  };
  const write = (value) => process.stdout.write(JSON.stringify(value) + "\n");
  const notify = (method, params) => write({ method, params });
  const rpc = async (method, params, notification = false) => {
    const id = notification ? undefined : ++mcpId;
    const response = await fetch(connection.url, {
      method: "POST",
      headers: {
        Authorization: connection.authorization,
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        ...(sessionId ? { "mcp-session-id": sessionId, "mcp-protocol-version": "2025-06-18" } : {}),
      },
      body: JSON.stringify({ jsonrpc: "2.0", ...(id ? { id } : {}), method, params }),
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error("Synthetic MCP HTTP status " + response.status);
    if (method === "initialize") sessionId = response.headers.get("mcp-session-id");
    if (notification) {
      await response.body?.cancel();
      return;
    }
    return decodeResponse(await response.text(), id);
  };
  const tool = (name, args = {}) => rpc("tools/call", { name, arguments: args });
  const content = (result) => {
    if (result.isError) throw new Error("Synthetic MCP tool rejected");
    return (
      result.structuredContent ??
      JSON.parse(
        result.content
          .filter((p) => p.type === "text")
          .map((p) => p.text)
          .join(""),
      )
    );
  };
  const refusal = (result, expected) =>
    result.isError === true &&
    JSON.stringify(result.structuredContent ?? result.content).includes(expected);
  const connect = async () => {
    await rpc("initialize", {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "synthetic-native-self-config", version: "1" },
    });
    await rpc("notifications/initialized", {}, true);
  };
  const read = async () => content(await tool("t3_thread_configuration"));
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
    status: { type: "idle" },
    path: root + "/account/fixture.jsonl",
    cwd: issuedCwd,
    cliVersion: "0.162.0",
    originator: "synthetic",
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
  const threadResult = () => ({
    thread: thread(),
    model,
    modelProvider: "openai",
    serviceTier: null,
    disabledPluginIds: [],
    cwd: issuedCwd,
    runtimeWorkspaceRoots: [issuedCwd],
    instructionSources: [],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "dangerFullAccess" },
    activePermissionProfile: null,
    reasoningEffort: "high",
    multiAgentMode: "explicitRequestOnly",
  });
  const complete = (text, status = "completed") => {
    const item = {
      type: "agentMessage",
      id: randomUUID(),
      text,
      phase: "final_answer",
      memoryCitation: null,
      delivery: null,
      questions: null,
    };
    notify("item/started", {
      threadId: nativeThreadId,
      turnId: activeTurn.id,
      startedAtMs: 1000,
      item: { ...item, text: "" },
    });
    notify("item/agentMessage/delta", {
      threadId: nativeThreadId,
      turnId: activeTurn.id,
      itemId: item.id,
      delta: text,
    });
    notify("item/completed", {
      threadId: nativeThreadId,
      turnId: activeTurn.id,
      completedAtMs: 2000,
      item,
    });
    notify("turn/completed", {
      threadId: nativeThreadId,
      turn: { ...activeTurn, items: [item], status, completedAt: 2, durationMs: 1 },
    });
    trace({ method: "synthetic/completed", status, marker: text });
  };
  const journey = async (input) => {
    if (input === "NATIVE_SELF_CONFIG_START") {
      await connect();
      const before = await bounded(async () => {
        const value = await read();
        return value.executionPrecondition ? value : null;
      }, "app-attached execution");
      const legacy = await tool("t3_thread_configure", { modelSelection: selection("B") });
      if (!refusal(legacy, "t3_thread_self_configure")) throw new Error("Legacy control accepted");
      const afterLegacy = await read();
      if (afterLegacy.configurationRevision !== before.configurationRevision)
        throw new Error("Legacy control changed selection revision");
      const request = {
        clientRequestId: "native-save-" + nativeThreadId,
        modelSelection: selection("B"),
        expectedConfigurationRevision: before.configurationRevision,
        executionPrecondition: before.executionPrecondition,
      };
      const receipt = content(await tool("t3_thread_self_configure", request));
      const replay = content(await tool("t3_thread_self_configure", request));
      const conflict = await tool("t3_thread_self_configure", {
        ...request,
        modelSelection: selection("A"),
      });
      if (!refusal(conflict, "request_conflict")) throw new Error("Conflict control accepted");
      const subsequent = await read();
      save("held", {
        synthetic: true,
        nativeThreadId,
        processId,
        before,
        afterLegacy,
        receipt,
        replay,
        legacyRejected: true,
        conflictRejected: true,
        subsequent,
      });
      trace({ method: "synthetic/held" });
      await bounded(() => fs.existsSync(root + "/release-" + nativeThreadId), "release A");
      complete("NATIVE_A_CONTINUED_AND_COMPLETED");
    } else if (input === "NATIVE_PICKER_START") {
      save("picker-held", { synthetic: true, nativeThreadId, processId });
    } else if (input === "NATIVE_FRESH_B" || input === "NATIVE_PICKER_CHANGED") {
      complete(input + "_COMPLETED");
    } else {
      throw new Error("Unexpected synthetic composer input");
    }
  };
  const handle = async ({ id, method, params = {} }) => {
    if (method === "initialized") return;
    if (method === "initialize") {
      write({
        id,
        result: {
          userAgent: "synthetic-codex/0.162.0",
          codexHome: root + "/account",
          platformFamily: "unix",
          platformOs: "linux",
        },
      });
    } else if (method === "account/read") {
      write({ id, result: { account: { type: "apiKey" }, requiresOpenaiAuth: false } });
    } else if (method === "account/rateLimits/read") {
      write({ id, error: { code: -32000, message: "Synthetic usage unavailable" } });
    } else if (method === "skills/list") {
      write({ id, result: { data: [] } });
    } else if (method === "model/list") {
      write({
        id,
        result: {
          data: [
            {
              id: model,
              model,
              displayName: "Synthetic model " + instance,
              description: "Synthetic engine; no live inference",
              hidden: false,
              isDefault: true,
              defaultReasoningEffort: "high",
              supportedReasoningEfforts: [
                { reasoningEffort: "high", description: "Synthetic high" },
              ],
              // The catalog mapper adds Standard when any optional tier is advertised.
              // No turn selects this unused synthetic alternative.
              serviceTiers: [{ id: "flex", name: "Synthetic unused tier", description: "" }],
            },
          ],
          nextCursor: null,
        },
      });
    } else if (method === "thread/start" || method === "thread/resume") {
      nativeThreadId = params.threadId ?? randomUUID();
      if (params.cwd !== "/scratch/workspace/native-self-config-execution")
        throw new Error("Unexpected app-issued fixture cwd");
      issuedCwd = params.cwd;
      connection = connectionFromStart(params);
      const { execFileSync } = await import("node:child_process");
      const gitIdentity = (arg) =>
        execFileSync(
          "/usr/bin/git",
          ["-C", issuedCwd, "rev-parse", "--path-format=absolute", arg],
          { encoding: "utf8", timeout: 15000 },
        ).trim();
      const gitDir = gitIdentity("--git-dir");
      const commonDir = gitIdentity("--git-common-dir");
      if (
        gitDir === commonDir ||
        !gitDir.startsWith("/scratch/workspace/.git/worktrees/") ||
        commonDir !== "/scratch/workspace/.git"
      )
        throw new Error("Invalid linked fixture identity");
      trace({
        method,
        model: params.model ?? null,
        appIssuedMcp: true,
        issuedCwd,
        gitDir,
        commonDir,
        head: gitIdentity("HEAD"),
      });
      write({ id, result: threadResult() });
    } else if (method === "thread/read") {
      nativeThreadId = params.threadId;
      write({ id, result: { thread: thread() } });
    } else if (method === "thread/goal/get") {
      write({ id, result: { goal: null } });
    } else if (method === "turn/start") {
      const input = params.input
        ?.filter((p) => p.type === "text")
        .map((p) => p.text)
        .join("\n");
      if (
        ![
          "NATIVE_SELF_CONFIG_START",
          "NATIVE_FRESH_B",
          "NATIVE_PICKER_START",
          "NATIVE_PICKER_CHANGED",
        ].includes(input)
      )
        throw new Error("Nonfixture turn");
      trace({
        method,
        input,
        model: params.model ?? null,
        effort: params.effort ?? null,
        serviceTier: params.serviceTier ?? null,
        collaborationModel: params.collaborationMode?.settings?.model ?? null,
        collaborationEffort: params.collaborationMode?.settings?.reasoning_effort ?? null,
      });
      activeTurn = {
        id: randomUUID(),
        items: [],
        itemsView: "notLoaded",
        status: "inProgress",
        error: null,
        startedAt: 1,
        completedAt: null,
        durationMs: null,
      };
      write({ id, result: { turn: activeTurn } });
      notify("turn/started", { threadId: nativeThreadId, turn: activeTurn });
      await journey(input);
    } else if (method === "turn/steer") {
      const input = params.input
        ?.filter((p) => p.type === "text")
        .map((p) => p.text)
        .join("\n");
      trace({
        method,
        input: input === "NATIVE_ORDINARY_FOLLOWUP" ? input : "unexpected",
        expectedTurnId: params.expectedTurnId ?? null,
      });
      write({ id, result: { turnId: activeTurn.id } });
      save("after-steer", { observation: await read(), processId, nativeThreadId });
    } else if (method === "turn/interrupt") {
      trace({
        method,
        requestId: id,
        threadId: params.threadId ?? null,
        turnId: params.turnId ?? null,
      });
      write({ id, result: {} });
      trace({ method: "turn/interrupt/ack", requestId: id, acknowledged: true });
      complete("NATIVE_INTERRUPTED", "interrupted");
      trace({ method: "turn/interrupt/terminal", requestId: id, turnId: activeTurn.id });
    } else if (id !== undefined) {
      trace({ method: "unsupported", requestedMethod: method });
      write({ id, error: { code: -32601, message: "Unsupported synthetic method" } });
    }
  };
  let firstFailure = false;
  const failed = (error) => {
    if (firstFailure) return;
    firstFailure = true;
    const reason =
      /^(Synthetic |Legacy |Conflict |App-issued |Invalid app-issued |Unexpected |Nonfixture )/.test(
        error?.message ?? "",
      )
        ? error.message
        : "Synthetic protocol decoding or transport failed";
    trace({ method: "synthetic/failure", reason });
    if (nativeThreadId) save("failure", { reason });
    process.exitCode = 1;
  };
  readline.createInterface({ input: process.stdin }).on("line", (line) => {
    try {
      void handle(JSON.parse(line)).catch(failed);
    } catch (error) {
      failed(error);
    }
  });
}

export async function installFixture(workspace) {
  if (workspace !== "/scratch/workspace" || process.env.HOME !== "/scratch/home")
    throw new Error("Synthetic peer requires the private native namespace");
  await NodeFSP.mkdir(fixtureRoot + "/account", { recursive: true, mode: 0o700 });
  const source =
    "(" +
    syntheticCodexPeer.toString() +
    ")(" +
    issuedConnection.toString() +
    "," +
    decodeMcpResponse.toString() +
    ").catch(() => { process.exitCode = 1; });\n";
  await NodeFSP.writeFile(fixtureRoot + "/peer.mjs", source, { flag: "wx", mode: 0o600 });
  for (const letter of ["A", "B"])
    await NodeFSP.writeFile(
      fixtureRoot + "/codex-" + letter,
      "#!/bin/sh\nexec /usr/bin/node /scratch/home/native-self-config/peer.mjs " +
        letter +
        ' "$@"\n',
      { flag: "wx", mode: 0o700 },
    );
  return installCheckpointWorktree(workspace);
}

export async function installCheckpointWorktree(workspace) {
  // The primary repository and linked execution checkout are both disposable.
  const git = (args) =>
    NodeChildProcess.execFileSync("/usr/bin/git", ["-C", workspace, ...args], {
      timeout: 15000,
      encoding: "utf8",
      env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" },
    }).trim();
  git(["init", "--initial-branch=fixture"]);
  await NodeFSP.writeFile(workspace + "/fixture.txt", "Synthetic native checkpoint fixture\n", {
    flag: "wx",
    mode: 0o600,
  });
  git(["add", "--", "fixture.txt"]);
  git([
    "-c",
    "user.name=Synthetic Fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "-m",
    "Synthetic checkpoint baseline",
  ]);
  const initialCommit = git(["rev-parse", "HEAD"]);
  const branch = "native-self-config-execution";
  const worktreePath = workspace + "/" + branch;
  git(["worktree", "add", "-b", branch, worktreePath, "HEAD"]);
  const identity = (arg) => git(["-C", worktreePath, "rev-parse", "--path-format=absolute", arg]);
  const gitDir = identity("--git-dir");
  const commonDir = identity("--git-common-dir");
  if (gitDir === commonDir || identity("HEAD") !== initialCommit)
    throw new Error("Invalid synthetic linked worktree");
  return { initialCommit, synthetic: true, branch, worktreePath, gitDir, commonDir };
}

export async function readTrace() {
  const entries = await NodeFSP.readdir(fixtureRoot).catch((error) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  const names = entries.filter((name) => /^trace-[a-f0-9-]+\.ndjson$/.test(name)).sort();
  return (
    await Promise.all(
      names.map(async (name) =>
        (await NodeFSP.readFile(fixtureRoot + "/" + name, "utf8"))
          .trim()
          .split("\n")
          .filter(Boolean)
          .map((line) => JSON.parse(line)),
      ),
    )
  ).flat();
}

export async function waitForFixture(read, label, timeout = 15000) {
  const end = performance.now() + timeout;
  while (performance.now() < end) {
    const value = await read();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Native fixture deadline: " + label);
}

export async function fixtureRecord(prefix, nativeThreadId) {
  const names = (await NodeFSP.readdir(fixtureRoot)).filter(
    (name) =>
      name.startsWith(prefix + "-") &&
      name.endsWith(".json") &&
      (!nativeThreadId || name === prefix + "-" + nativeThreadId + ".json"),
  );
  if (names.length > 1) throw new Error("Ambiguous fixture receipt");
  return names.length
    ? JSON.parse(await NodeFSP.readFile(fixtureRoot + "/" + names[0], "utf8"))
    : null;
}
