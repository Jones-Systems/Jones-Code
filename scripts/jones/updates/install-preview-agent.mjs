import * as NodeAssert from "node:assert/strict";
import * as NodeModule from "node:module";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeEvents from "node:events";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";

// oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone native fixture must identify the actual host for Linux process custody and qualification without loading the application runtime.
const nativePlatform = process.platform;

export function toolValue(result) {
  if (result?.isError) throw new Error(`Preview tool refused: ${JSON.stringify(result.content)}`);
  if (result?.structuredContent !== undefined) return result.structuredContent;
  const text = result?.content?.find((item) => item.type === "text")?.text;
  if (text === undefined) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { text };
  }
}

export function assertVisible(snapshot, expected, absent = false) {
  NodeAssert.equal(typeof expected, "string");
  NodeAssert.ok(expected.length > 0);
  const visible = snapshot.visibleText ?? snapshot.text ?? "";
  NodeAssert.equal(
    visible.includes(expected),
    !absent,
    `${absent ? "Unexpected" : "Missing"} visible text: ${expected}`,
  );
}

export async function atomicJson(file, value) {
  const temporary = `${file}.${NodeCrypto.randomUUID()}.partial`;
  try {
    await NodeFSP.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, {
      flag: "wx",
      mode: 0o600,
    });
    await NodeFSP.rename(temporary, file);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}

const milestoneSchema = "jones-install-fixture-milestone/v1";
const milestoneNames = ["started", "custody", "report", "final", "release"];
const milestoneDigest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

function validateMilestoneBinding(binding) {
  NodeAssert.equal(binding.schema, milestoneSchema);
  for (const key of ["runId", "caseId"]) NodeAssert.match(binding[key], /^[A-Za-z0-9_-]{1,80}$/);
  NodeAssert.match(binding.nonce, /^[a-f0-9]{64}$/);
  const url = new URL(binding.endpoint);
  NodeAssert.equal(url.protocol, "http:");
  NodeAssert.equal(url.hostname, "127.0.0.1");
  NodeAssert.ok(
    url.port &&
      url.pathname === "/milestone" &&
      !url.search &&
      !url.hash &&
      !url.username &&
      !url.password,
  );
}

async function milestoneRequest(binding, payload, signal) {
  validateMilestoneBinding(binding);
  const response = await fetch(binding.endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      schema: binding.schema,
      runId: binding.runId,
      caseId: binding.caseId,
      nonce: binding.nonce,
      ...payload,
    }),
    signal: signal
      ? AbortSignal.any([signal, AbortSignal.timeout(90_000)])
      : AbortSignal.timeout(90_000),
  });
  NodeAssert.equal(response.status, 200, "Owned milestone channel refused the message");
  const receipt = await response.json();
  NodeAssert.equal(receipt.schema, milestoneSchema);
  NodeAssert.equal(receipt.runId, binding.runId);
  NodeAssert.equal(receipt.caseId, binding.caseId);
  NodeAssert.equal(receipt.name, payload.name);
  NodeAssert.match(receipt.sha256, /^[a-f0-9]{64}$/);
  return receipt;
}

export async function readAcknowledgedMilestone(file, receipt) {
  const bytes = await NodeFSP.readFile(file);
  NodeAssert.equal(
    milestoneDigest(bytes),
    receipt.sha256,
    "Acknowledged milestone changed before readback",
  );
  return JSON.parse(bytes);
}

export async function publishMilestone(binding, name, file, value, signal) {
  NodeAssert.ok(milestoneNames.includes(name) && name !== "release");
  await atomicJson(file, value);
  const sha256 = milestoneDigest(await NodeFSP.readFile(file));
  const receipt = await milestoneRequest(binding, { kind: "publish", name, sha256 }, signal);
  NodeAssert.equal(receipt.sha256, sha256);
  return receipt;
}

export async function awaitMilestoneRelease(binding, file, signal) {
  const receipt = await milestoneRequest(binding, { kind: "wait", name: "release" }, signal);
  return readAcknowledgedMilestone(file, receipt);
}

export async function createMilestoneChannel({ runId, caseId, files, signal }) {
  signal?.throwIfAborted();
  NodeAssert.deepEqual(Object.keys(files).sort(), [...milestoneNames].sort());
  const nonce = NodeCrypto.randomBytes(32).toString("hex");
  const receipts = new Map();
  const waiters = new Set();
  const held = new Set();
  let markReleaseWaiting;
  const waitingForRelease = new Promise((resolve) => {
    markReleaseWaiting = resolve;
  });
  let closed = false;
  let closing;
  let requestCount = 0;
  const binding = { schema: milestoneSchema, runId, caseId, nonce, endpoint: "" };
  const reply = (response, status, value) => {
    response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(value));
  };
  const record = (name, sha256) => {
    NodeAssert.ok(!closed, "Milestone channel is closed");
    NodeAssert.ok(!receipts.has(name), "Duplicate milestone");
    const receipt = { schema: milestoneSchema, runId, caseId, name, sha256 };
    receipts.set(name, receipt);
    for (const waiter of [...waiters])
      if (waiter.names.includes(name)) waiter.finish(undefined, receipt);
    if (name === "release") for (const response of [...held]) reply(response, 200, receipt);
    return receipt;
  };
  const server = NodeHttp.createServer(async (request, response) => {
    try {
      NodeAssert.ok(!closed && ++requestCount <= 32);
      NodeAssert.equal(request.method, "POST");
      NodeAssert.equal(request.url, "/milestone");
      let text = "";
      for await (const chunk of request) {
        text += chunk;
        NodeAssert.ok(Buffer.byteLength(text) <= 2048, "Milestone message exceeds bound");
      }
      const message = JSON.parse(text);
      NodeAssert.equal(message.schema, milestoneSchema);
      NodeAssert.equal(message.runId, runId);
      NodeAssert.equal(message.caseId, caseId);
      NodeAssert.equal(message.nonce, nonce);
      NodeAssert.ok(milestoneNames.includes(message.name));
      if (message.kind === "publish") {
        NodeAssert.notEqual(message.name, "release", "Only the driver can release the provider");
        NodeAssert.match(message.sha256, /^[a-f0-9]{64}$/);
        const bytes = await NodeFSP.readFile(files[message.name]);
        NodeAssert.equal(milestoneDigest(bytes), message.sha256);
        JSON.parse(bytes);
        reply(response, 200, record(message.name, message.sha256));
      } else {
        NodeAssert.equal(message.kind, "wait");
        NodeAssert.equal(message.name, "release");
        NodeAssert.ok(held.size < 1, "Only one provider may await release");
        const receipt = receipts.get("release");
        if (receipt) reply(response, 200, receipt);
        else {
          held.add(response);
          markReleaseWaiting(true);
          response.once("close", () => held.delete(response));
        }
      }
    } catch {
      if (!response.destroyed && !response.headersSent)
        reply(response, 409, { error: "invalid_fixture_milestone" });
    }
  });
  server.requestTimeout = 15_000;
  server.headersTimeout = 15_000;
  const close = () => {
    if (closing) return closing;
    closed = true;
    signal?.removeEventListener("abort", cancel);
    for (const waiter of [...waiters]) waiter.finish(new Error("Milestone channel closed"));
    for (const response of [...held]) reply(response, 503, { error: "fixture_channel_closed" });
    held.clear();
    markReleaseWaiting(false);
    server.closeAllConnections();
    closing = new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    return closing;
  };
  const cancel = () => {
    void close().catch(() => {});
  };
  server.listen(0, "127.0.0.1");
  try {
    await NodeEvents.once(server, "listening", { signal });
    binding.endpoint = `http://127.0.0.1:${server.address().port}/milestone`;
    validateMilestoneBinding(binding);
    signal?.addEventListener("abort", cancel, { once: true });
    signal?.throwIfAborted();
  } catch (error) {
    await close();
    throw error;
  }
  return {
    binding,
    waitingForRelease,
    wait(names, waitSignal) {
      return new Promise((resolve, reject) => {
        NodeAssert.ok(!closed, "Milestone channel closed");
        NodeAssert.ok(names.length > 0 && names.every((name) => milestoneNames.includes(name)));
        NodeAssert.ok(waiters.size < 4, "Too many milestone waiters");
        const cancelWait = () => finish(waitSignal.reason ?? new Error("Milestone wait cancelled"));
        const waiter = { names, finish: (error, receipt) => finish(error, receipt) };
        const finish = (error, receipt) => {
          waiters.delete(waiter);
          waitSignal?.removeEventListener("abort", cancelWait);
          if (error) reject(error);
          else resolve(receipt);
        };
        if (waitSignal?.aborted) {
          cancelWait();
          return;
        }
        const receipt = names.map((name) => receipts.get(name)).find(Boolean);
        if (receipt) {
          resolve(receipt);
          return;
        }
        waiters.add(waiter);
        waitSignal?.addEventListener("abort", cancelWait, { once: true });
      });
    },
    async release(value) {
      NodeAssert.ok(!closed, "Milestone channel closed");
      await atomicJson(files.release, value);
      return record("release", milestoneDigest(await NodeFSP.readFile(files.release)));
    },
    evidence: () => ({
      runId,
      caseId,
      receipts: [...receipts.values()],
      closed,
      pendingWaiters: waiters.size,
      heldRequests: held.size,
    }),
    close,
  };
}

export function redact(value, secrets = []) {
  let text = typeof value === "string" ? value : JSON.stringify(value);
  for (const secret of secrets.filter(Boolean).sort((a, b) => b.length - a.length))
    text = text.replaceAll(secret, "[redacted]");
  return text
    .replace(/(Bearer\s+)[^\s"\\]+/gi, "$1[redacted]")
    .replace(/((?:token|wsTicket|credential|authorization)=)[^\s&"\\]+/gi, "$1[redacted]")
    .replace(/JONES_PREVIEW_SCENARIO:[A-Za-z0-9+/=]+/g, "JONES_PREVIEW_SCENARIO:[redacted]");
}

export function scenarioSecrets(scenario) {
  const inputs = [
    scenario.pairingUrl,
    ...(scenario.steps?.filter((step) => step.action === "type").map((step) => step.text) ?? []),
  ].filter(Boolean);
  const secrets = new Set(inputs);
  for (const input of inputs) {
    try {
      const url = new URL(input);
      // Match shared/remote.getPairingTokenFromUrl: the UI separates this code from the pasted URL.
      const hashToken = new URLSearchParams(url.hash.slice(1)).get("token")?.trim() ?? "";
      const token = hashToken || url.searchParams.get("token")?.trim();
      if (token) secrets.add(token);
    } catch {
      // Non-URL typed input is already included verbatim.
    }
  }
  return [...secrets];
}

async function connectInjectedMcp(descriptor, source, onTransportCreated) {
  NodeAssert.equal(descriptor?.name, "t3-code", "Normal T3 MCP injection is required");
  NodeAssert.equal(typeof descriptor.command, "string");
  NodeAssert.ok(Array.isArray(descriptor.args));
  const injected = Object.fromEntries(
    (descriptor.env ?? []).map(({ name, value }) => [name, value]),
  );
  NodeAssert.ok(
    injected.T3_ACP_MCP_ENDPOINT && injected.T3_ACP_MCP_AUTHORIZATION,
    "Injected session credential is absent",
  );
  const endpoint = new URL(injected.T3_ACP_MCP_ENDPOINT);
  NodeAssert.ok(["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname));
  const require = NodeModule.createRequire(NodePath.join(source, "apps/server/package.json"));
  const { Client } = await import(
    NodeURL.pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/index.js")).href
  );
  const { StdioClientTransport } = await import(
    NodeURL.pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/stdio.js")).href
  );
  const transport = new StdioClientTransport({
    command: descriptor.command,
    args: descriptor.args,
    env: { ...process.env, ...injected },
    stderr: "pipe",
  });
  onTransportCreated();
  transport.stderr?.on("data", () => {});
  const client = new Client(
    { name: "jones-install-preview-verifier", version: "1" },
    { capabilities: {} },
  );
  try {
    await client.connect(transport);
  } catch (error) {
    await transport.close();
    throw error;
  }
  return { client, transport, injected };
}

export async function runScenario(client, scenario, report) {
  const call = async (name, args = {}) => {
    try {
      const result = await client.callTool({ name, arguments: args }, undefined, {
        timeout: 180_000,
      });
      const value = toolValue(result);
      // Keep only results, never the pairing URL carried by a navigation input.
      report.calls.push({ name, ok: true });
      return value;
    } catch (error) {
      report.calls.push({ name, ok: false, error: String(error.message) });
      throw error;
    }
  };
  report.initialStatus = await call("preview_status");
  const opened = await call("preview_open", { open: false, reuseExistingTab: false });
  const tabId = opened.tabId;
  NodeAssert.equal(typeof tabId, "string");
  report.tabId = tabId;
  report.opened = opened;
  let scenarioError;
  try {
    await call("preview_resize", { tabId, mode: "freeform", width: 1280, height: 900 });
    await call("preview_set_appearance", { tabId, colorScheme: "light" });
    await call("preview_navigate", { tabId, url: scenario.pairingUrl });
    await call("preview_wait_for", {
      tabId,
      urlIncludes: "/",
      text: scenario.initialText,
      timeoutMs: 30_000,
    });
    for (const [index, step] of scenario.steps.entries()) {
      report.currentAction = {
        index,
        action: step.action,
        ...(step.action === "wait" ? { text: step.text } : {}),
        ...(step.locator ? { locator: step.locator } : {}),
        ...(step.name ? { name: step.name } : {}),
      };
      if (step.action === "navigate")
        await call("preview_navigate", { tabId, url: new URL(step.path, scenario.origin).href });
      else if (step.action === "click")
        await call("preview_click", { tabId, locator: step.locator, timeoutMs: 15_000 });
      else if (step.action === "hover")
        await call("preview_hover", { tabId, locator: step.locator });
      else if (step.action === "type")
        await call("preview_type", { tabId, locator: step.locator, text: step.text, clear: true });
      else if (step.action === "wait")
        await call("preview_wait_for", { tabId, text: step.text, timeoutMs: 30_000 });
      else if (step.action === "capture") {
        const snapshot = await call("preview_snapshot", { tabId, save: true, includeImage: true });
        report.captures.push({ name: step.name, snapshot });
        for (const expected of step.contains ?? []) assertVisible(snapshot, expected);
        for (const absent of step.excludes ?? []) assertVisible(snapshot, absent, true);
      } else if (step.action === "assert-evaluate") {
        const result = await call("preview_evaluate", { tabId, expression: step.expression });
        NodeAssert.deepEqual(result.value ?? result.result ?? result, step.expected, step.label);
      } else throw new Error(`Unknown bounded scenario action: ${step.action}`);
    }
    const snapshot = await call("preview_snapshot", { tabId, save: true, includeImage: true });
    const sentinel = `jones-deliberate-wrong-UI-${NodeCrypto.randomUUID()}`;
    NodeAssert.throws(() => assertVisible(snapshot, sentinel), /Missing visible text/);
    report.negativeControls.wrongUiAssertion = "rejected";
    report.viewport = await call("preview_status", { tabId });
  } catch (error) {
    scenarioError = error;
    report.failedAction = report.currentAction ?? { action: "initial-navigation" };
    try {
      // A failed pairing dialog can still display a plain-text code. Never persist its pixels or text.
      const snapshot = await call("preview_snapshot", { tabId, save: false, includeImage: false });
      report.failureReconciliation = {
        kind: "text-only",
        visibleTextLength: (snapshot.visibleText ?? "").length,
        contentOmitted: true,
      };
      report.controlledPageReconciled = true;
    } catch (snapshotError) {
      report.controlledPageReconciled = false;
      report.reconciliationError = String(snapshotError.message);
    }
    throw error;
  } finally {
    try {
      await call("t3_preview_close", { tabId });
      report.nativeTabClosed = true;
    } catch (error) {
      report.nativeTabClosed = false;
      report.tabCleanupError = String(error.message);
      if (!scenarioError) throw error;
    }
  }
}

export function scenarioFromPrompt(prompt) {
  const text = prompt
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("\n");
  const match = text.match(/(?:^|\n)JONES_PREVIEW_SCENARIO:([A-Za-z0-9+/=]+)(?:\n|$)/);
  NodeAssert.ok(match, "Owned scenario marker is absent from the normal ACP prompt");
  return JSON.parse(Buffer.from(match[1], "base64").toString("utf8"));
}

export async function processIdentity(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1) return null;
  if (nativePlatform !== "linux") return { pid, startTime: null };
  try {
    const stat = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
    return { pid, startTime: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] };
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

export async function assertInactiveDenied(client) {
  let stopped;
  try {
    stopped = await client.callTool({ name: "preview_open", arguments: { open: false } });
  } catch (error) {
    NodeAssert.match(
      error.message,
      /\b401\b|invalid_mcp_credential|\bparent_not_active\b/i,
      "A transport failure alone does not prove revoked authority",
    );
    return "transport authorization refused";
  }
  NodeAssert.equal(stopped?.isError, true, "An inactive provider must not act as a live caller");
  NodeAssert.match(JSON.stringify(stopped), /\bparent_not_active\b|invalid_mcp_credential/i);
  return "tool authorization refused";
}

export function assertBrowserDisabledDenied(result) {
  NodeAssert.equal(result?.isError, true, "Disabled browser must reject Preview");
  NodeAssert.match(
    JSON.stringify(result),
    /MCP credential does not grant the preview capability/,
    "A browser startup failure does not prove disabled capability enforcement",
  );
}

export async function runAgent() {
  const source = process.env.JONES_PREVIEW_SOURCE;
  const reportFile = process.env.JONES_PREVIEW_REPORT;
  const releaseFile = process.env.JONES_PREVIEW_RELEASE;
  const milestone = JSON.parse(process.env.JONES_PREVIEW_MILESTONE ?? "null");
  validateMilestoneBinding(milestone);
  NodeAssert.ok(
    source && reportFile && releaseFile,
    "Verifier must be launched with owned output bindings",
  );
  const sessions = new Map();
  const input = NodeReadline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  let active;
  let activePrompt;
  const lifetime = new AbortController();
  const send = (message) =>
    process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
  const finish = (id, result) => send({ id, result });
  const fail = (id, error) => send({ id, error: { code: -32603, message: redact(error.message) } });
  const runPrompt = async (message) => {
    const session = sessions.get(message.params.sessionId);
    NodeAssert.ok(session, "Unknown ACP session");
    const scenario = scenarioFromPrompt(message.params.prompt);
    const report = {
      schema: "jones-install-preview/v1",
      providerPid: process.pid,
      mode: scenario.mode,
      calls: [],
      captures: [],
      negativeControls: {},
      transportCreated: false,
      outcome: "running",
    };
    let mcp;
    let replied = false;
    const secrets = [milestone.nonce, ...scenarioSecrets(scenario)];
    const providerIdentity = await processIdentity(process.pid);
    try {
      await publishMilestone(
        milestone,
        "started",
        `${reportFile}.started`,
        { provider: providerIdentity },
        lifetime.signal,
      );
      mcp = await connectInjectedMcp(session.mcp, source, () => {
        report.transportCreated = true;
      });
      active = mcp;
      report.mcpBridgePid = mcp.transport.pid;
      await publishMilestone(
        milestone,
        "custody",
        `${reportFile}.custody`,
        {
          provider: providerIdentity,
          bridge: await processIdentity(report.mcpBridgePid),
        },
        lifetime.signal,
      );
      secrets.push(mcp.injected.T3_ACP_MCP_AUTHORIZATION);
      for (const variant of ["missing", "invalid"]) {
        const headers = {
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
        };
        if (variant === "invalid") {
          // Corrupt this fixture's own injected grant; no credential is fabricated or copied from another session.
          const original = mcp.injected.T3_ACP_MCP_AUTHORIZATION;
          headers.authorization = `${original.slice(0, -1)}${original.endsWith("x") ? "y" : "x"}`;
        }
        const response = await fetch(mcp.injected.T3_ACP_MCP_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            method: "initialize",
            params: {
              protocolVersion: "2025-06-18",
              capabilities: {},
              clientInfo: { name: "negative-control", version: "1" },
            },
          }),
          signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(15_000)]),
        });
        NodeAssert.equal(
          response.status,
          401,
          `${variant} credential must be rejected by the actual MCP endpoint`,
        );
        const rejection = await response.json();
        NodeAssert.equal(
          rejection.error,
          "invalid_mcp_credential",
          "Native MCP authentication must supply the refusal",
        );
        report.negativeControls[variant] = { status: response.status, error: rejection.error };
      }
      if (scenario.mode === "browser-disabled") {
        const denied = await mcp.client.callTool({
          name: "preview_open",
          arguments: { open: false },
        });
        assertBrowserDisabledDenied(denied);
        report.negativeControls.browserDisabled = "rejected";
      } else await runScenario(mcp.client, scenario, report);
      report.outcome = "actions-complete";
      await publishMilestone(
        milestone,
        "report",
        reportFile,
        JSON.parse(redact(report, secrets)),
        lifetime.signal,
      );
      send({
        method: "session/update",
        params: {
          sessionId: message.params.sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "Owned Preview scenario actions completed." },
          },
        },
      });
      finish(message.id, { stopReason: "end_turn" });
      replied = true;
      // The driver releases this only after native projection readback says the run is no longer active.
      const release = await awaitMilestoneRelease(milestone, releaseFile, lifetime.signal);
      NodeAssert.deepEqual(release, { inactive: true });
      report.negativeControls.stoppedProviderMechanism = await assertInactiveDenied(mcp.client);
      report.negativeControls.stoppedProvider = "rejected";
      report.outcome = "passed";
    } catch (error) {
      report.outcome = "failed";
      report.error = redact(error.stack ?? error.message, secrets);
      if (!replied) fail(message.id, new Error(report.error));
    } finally {
      try {
        await mcp?.client.close();
        report.transportClosed = mcp !== undefined;
      } catch (error) {
        report.outcome = "failed";
        report.error = redact(error.message, secrets);
      }
      active = undefined;
      // Preserve the final report even when cancellation has already closed coordination.
      await publishMilestone(
        milestone,
        "final",
        `${reportFile}.final`,
        JSON.parse(redact(report, secrets)),
        lifetime.signal,
      );
    }
  };
  const shutdown = async () => {
    lifetime.abort(new Error("Native provider cancellation"));
    await active?.client.close();
    await activePrompt;
    input.close();
  };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);
  input.on("line", (line) => {
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      return;
    }
    if (message.method === "initialize")
      finish(message.id, {
        protocolVersion: 1,
        agentInfo: { name: "jones-install-preview-verifier", version: "1" },
        agentCapabilities: {
          loadSession: false,
          promptCapabilities: {},
          mcpCapabilities: { http: false, sse: false },
        },
        authMethods: [],
      });
    else if (message.method === "session/new") {
      const sessionId = NodeCrypto.randomUUID();
      sessions.set(sessionId, {
        mcp: message.params.mcpServers?.find((server) => server.name === "t3-code"),
      });
      finish(message.id, {
        sessionId,
        models: {
          currentModelId: "preview-verifier",
          availableModels: [
            { modelId: "preview-verifier", name: "Deterministic Preview verifier" },
          ],
        },
        modes: { currentModeId: "default", availableModes: [{ id: "default", name: "Default" }] },
      });
    } else if (message.method === "session/prompt") {
      if (activePrompt) {
        fail(message.id, new Error("Verifier accepts one owned prompt"));
        return;
      }
      activePrompt = runPrompt(message).catch((error) => fail(message.id, error));
    } else if (["session/set_mode", "session/set_model", "authenticate"].includes(message.method))
      finish(message.id, {});
    else if (message.method === "session/cancel") void shutdown();
    else if (message.id !== undefined)
      send({
        id: message.id,
        error: { code: -32601, message: "Unsupported deterministic provider operation" },
      });
  });
  input.once("close", () => {
    lifetime.abort(new Error("Provider stdin closed"));
    void active?.client.close();
  });
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  runAgent().catch((error) => {
    process.stderr.write(`${redact(error.stack ?? error)}\n`);
    process.exitCode = 1;
  });
}
