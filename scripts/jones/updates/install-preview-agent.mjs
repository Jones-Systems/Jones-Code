import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile, writeFile, rename, rm } from "node:fs/promises";
import { watch } from "node:fs";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { randomUUID } from "node:crypto";

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
  assert.equal(typeof expected, "string");
  assert.ok(expected.length > 0);
  const visible = snapshot.visibleText ?? snapshot.text ?? "";
  assert.equal(
    visible.includes(expected),
    !absent,
    `${absent ? "Unexpected" : "Missing"} visible text: ${expected}`,
  );
}

export async function atomicJson(file, value) {
  const temporary = `${file}.${randomUUID()}.partial`;
  try {
    await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { flag: "wx", mode: 0o600 });
    await rename(temporary, file);
  } finally {
    await rm(temporary, { force: true });
  }
}

export async function waitFile(file, signal) {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    let watcher;
    let closed = false;
    let reading = false;
    let changed = false;
    const finish = (error, value) => {
      if (closed) return;
      closed = true;
      watcher?.close();
      signal?.removeEventListener("abort", cancel);
      if (error) reject(error);
      else resolve(value);
    };
    const cancel = () => finish(signal.reason ?? new Error("Milestone observation cancelled"));
    const inspect = async () => {
      if (closed) return;
      changed = true;
      if (reading) return;
      reading = true;
      try {
        do {
          changed = false;
          try {
            finish(undefined, JSON.parse(await readFile(file, "utf8")));
          } catch (error) {
            if (error.code !== "ENOENT") throw error;
          }
          // A notification received during the read must trigger another read before idling.
        } while (changed && !closed);
      } catch (error) {
        finish(error);
      } finally {
        reading = false;
      }
    };
    try {
      // Register synchronously before the first read; callbacks remain attached between reads.
      watcher = watch(path.dirname(file), () => void inspect());
      watcher.on("error", (error) => finish(error));
      signal?.addEventListener("abort", cancel, { once: true });
      if (signal?.aborted) cancel();
      else void inspect();
    } catch (error) {
      finish(error);
    }
  });
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

async function connectInjectedMcp(descriptor, source, onTransportCreated) {
  assert.equal(descriptor?.name, "t3-code", "Normal T3 MCP injection is required");
  assert.equal(typeof descriptor.command, "string");
  assert.ok(Array.isArray(descriptor.args));
  const injected = Object.fromEntries(
    (descriptor.env ?? []).map(({ name, value }) => [name, value]),
  );
  assert.ok(
    injected.T3_ACP_MCP_ENDPOINT && injected.T3_ACP_MCP_AUTHORIZATION,
    "Injected session credential is absent",
  );
  const endpoint = new URL(injected.T3_ACP_MCP_ENDPOINT);
  assert.ok(["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname));
  const require = createRequire(path.join(source, "apps/server/package.json"));
  const { Client } = await import(
    pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/index.js")).href
  );
  const { StdioClientTransport } = await import(
    pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/stdio.js")).href
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
    const result = await client.callTool({ name, arguments: args }, undefined, {
      timeout: 180_000,
    });
    const value = toolValue(result);
    // Keep only results, never the pairing URL carried by a navigation input.
    report.calls.push({ name, ok: true });
    return value;
  };
  report.initialStatus = await call("preview_status");
  const opened = await call("preview_open", { open: false, reuseExistingTab: false });
  const tabId = opened.tabId;
  assert.equal(typeof tabId, "string");
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
    for (const step of scenario.steps) {
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
        assert.deepEqual(result.value ?? result.result ?? result, step.expected, step.label);
      } else throw new Error(`Unknown bounded scenario action: ${step.action}`);
    }
    const snapshot = await call("preview_snapshot", { tabId, save: true, includeImage: true });
    const sentinel = `jones-deliberate-wrong-UI-${randomUUID()}`;
    assert.throws(() => assertVisible(snapshot, sentinel), /Missing visible text/);
    report.negativeControls.wrongUiAssertion = "rejected";
    report.viewport = await call("preview_status", { tabId });
  } catch (error) {
    scenarioError = error;
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
  assert.ok(match, "Owned scenario marker is absent from the normal ACP prompt");
  return JSON.parse(Buffer.from(match[1], "base64").toString("utf8"));
}

export async function processIdentity(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1) return null;
  if (process.platform !== "linux") return { pid, startTime: null };
  try {
    const stat = await readFile(`/proc/${pid}/stat`, "utf8");
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
    assert.match(
      error.message,
      /\b401\b|invalid_mcp_credential|\bparent_not_active\b/i,
      "A transport failure alone does not prove revoked authority",
    );
    return "transport authorization refused";
  }
  assert.equal(stopped?.isError, true, "An inactive provider must not act as a live caller");
  assert.match(JSON.stringify(stopped), /\bparent_not_active\b|invalid_mcp_credential/i);
  return "tool authorization refused";
}

export function assertBrowserDisabledDenied(result) {
  assert.equal(result?.isError, true, "Disabled browser must reject Preview");
  assert.match(
    JSON.stringify(result),
    /MCP credential does not grant the preview capability/,
    "A browser startup failure does not prove disabled capability enforcement",
  );
}

export async function runAgent() {
  const source = process.env.JONES_PREVIEW_SOURCE;
  const reportFile = process.env.JONES_PREVIEW_REPORT;
  const releaseFile = process.env.JONES_PREVIEW_RELEASE;
  assert.ok(
    source && reportFile && releaseFile,
    "Verifier must be launched with owned output bindings",
  );
  const sessions = new Map();
  const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
  let active;
  let activePrompt;
  const lifetime = new AbortController();
  const send = (message) =>
    process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
  const finish = (id, result) => send({ id, result });
  const fail = (id, error) => send({ id, error: { code: -32603, message: redact(error.message) } });
  const runPrompt = async (message) => {
    const session = sessions.get(message.params.sessionId);
    assert.ok(session, "Unknown ACP session");
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
    const secrets = [
      scenario.pairingUrl,
      ...(scenario.steps?.filter((step) => step.action === "type").map((step) => step.text) ?? []),
    ];
    const providerIdentity = await processIdentity(process.pid);
    await atomicJson(`${reportFile}.started`, { provider: providerIdentity });
    try {
      mcp = await connectInjectedMcp(session.mcp, source, () => {
        report.transportCreated = true;
      });
      active = mcp;
      report.mcpBridgePid = mcp.transport.pid;
      await atomicJson(`${reportFile}.started`, {
        provider: providerIdentity,
        bridge: await processIdentity(report.mcpBridgePid),
      });
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
        assert.equal(
          response.status,
          401,
          `${variant} credential must be rejected by the actual MCP endpoint`,
        );
        const rejection = await response.json();
        assert.equal(
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
      await atomicJson(reportFile, JSON.parse(redact(report, secrets)));
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
      await waitFile(releaseFile, AbortSignal.any([lifetime.signal, AbortSignal.timeout(90_000)]));
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
      await atomicJson(`${reportFile}.final`, JSON.parse(redact(report, secrets)));
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
      const sessionId = randomUUID();
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

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  runAgent().catch((error) => {
    process.stderr.write(`${redact(error.stack ?? error)}\n`);
    process.exitCode = 1;
  });
}
