import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

const digest = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const TABLES = {
  threads: "orchestration_v2_projection_threads",
  messages: "orchestration_v2_projection_messages",
  runs: "orchestration_v2_projection_runs",
  attempts: "orchestration_v2_projection_run_attempts",
  nodes: "orchestration_v2_projection_nodes",
  providerTurns: "orchestration_v2_projection_provider_turns",
  turnItems: "orchestration_v2_projection_turn_items",
};

export function assertReplayScope(command, captured, fixture) {
  NodeAssert.equal(command?.type, "message.dispatch", "Replay only accepts message.dispatch");
  NodeAssert.equal(command.threadId, fixture.threadId, "Replay is target-thread scoped");
  NodeAssert.equal(command.text, fixture.m2Original, "Replay is the original queued M2 command");
  NodeAssert.ok(
    typeof command.commandId === "string" && command.commandId.length > 0,
    "Replay needs the captured command ID",
  );
  NodeAssert.ok(
    typeof command.messageId === "string" && command.messageId.length > 0,
    "Replay needs the captured message ID",
  );
  NodeAssert.deepEqual(command, captured, "Replay must preserve every captured command field");
  return structuredClone(command);
}

export function assertQueueState(projection, fixture, phase, assert = NodeAssert.ok) {
  assert(projection?.thread?.id === fixture.threadId, "Projection is the target thread");
  const users = projection.messages.filter((message) => message.role === "user");
  assert(users.length === 3, "Exactly three durable user messages exist");
  assert(projection.runs.length === 3, "Exactly three runs exist");
  const mapped = [
    fixture.m1,
    phase === "queued" ? fixture.m2Original : fixture.m2Edited,
    fixture.m3,
  ].map((text, index) => {
    const messages = users.filter((message) => message.text === text);
    assert(
      messages.length === 1,
      index === 1 ? "M2 retains exactly the edited text" : "Each sentinel has one durable message",
    );
    const message = messages[0];
    const runs = projection.runs.filter((run) => run.userMessageId === message.id);
    assert(runs.length === 1, "Each message owns exactly one run");
    if (fixture.ids) {
      assert(
        message.id === fixture.ids[index].messageId && runs[0].id === fixture.ids[index].runId,
        "Edit/cancel/replay preserve message and run IDs",
      );
    }
    return { messageId: message.id, runId: runs[0].id, run: runs[0] };
  });
  assert(
    mapped[1].run.status === (phase === "complete" ? "completed" : "queued"),
    "M2 has the expected queue lifecycle",
  );
  assert(
    mapped[2].run.status === (phase === "queued" ? "queued" : "cancelled"),
    "M3 has the expected cancellation lifecycle",
  );
  if (phase === "complete") {
    assert(mapped[0].run.status === "completed", "M1 completed before M2 settled");
    assert(
      projection.messages.filter((message) => message.role === "assistant").length === 2,
      "Exactly two assistant messages exist",
    );
    assert(projection.providerTurns.length === 2, "Exactly two provider turns exist");
    assert(
      projection.messages.filter((m) => m.role === "assistant" && m.text === `ACK ${fixture.m1}`)
        .length === 1,
      "M1 has exactly one assistant acknowledgement",
    );
    assert(
      projection.messages.filter(
        (m) => m.role === "assistant" && m.text === `ACK ${fixture.m2Edited}`,
      ).length === 1,
      "Edited M2 has exactly one assistant acknowledgement",
    );
  } else {
    assert(
      ["running", "starting"].includes(mapped[0].run.status),
      "M1 remains active behind its file gate",
    );
  }
  if (phase !== "queued") {
    const attempts = projection.attempts.filter((attempt) => attempt.runId === mapped[2].runId);
    const nodes = projection.nodes.filter((node) => node.runId === mapped[2].runId);
    assert(
      attempts.length === 1 && attempts[0].status === "cancelled",
      "M3's attempt is cancelled",
    );
    assert(
      !projection.providerTurns.some((turn) => turn.runAttemptId === attempts[0].id),
      "Cancelled M3 has no provider turn",
    );
    assert(
      nodes.length > 0 && nodes.every((node) => node.status === "cancelled"),
      "M3's nodes are cancelled",
    );
    if (projection.visibleTurnItems)
      assert(
        !projection.visibleTurnItems.some(
          (row) => row.item.type === "user_message" && row.item.runId === mapped[2].runId,
        ),
        "Cancelled M3 is absent from the visible transcript",
      );
  }
  return mapped.map(({ messageId, runId }) => ({ messageId, runId }));
}
function assertCancelledQueueView(projection, fixture, assert = NodeAssert.ok) {
  const runId = fixture.ids[2].runId;
  assert(projection?.thread?.id === fixture.threadId, "Queue view is the target thread");
  assert(
    !projection.runs.some((run) => run.id === runId && run.status !== "cancelled"),
    "M3 is absent from the active queue view",
  );
  assert(
    !projection.visibleTurnItems.some(
      (row) => row.item.type === "user_message" && row.item.runId === runId,
    ),
    "Cancelled M3 is absent from the bounded transcript",
  );
}
export function assertDeliveries(ledger, fixture, complete, assert = NodeAssert.ok) {
  const actual = ledger.filter((entry) => entry.event === "delivery");
  const expected = complete ? [fixture.m1, fixture.m2Edited] : [fixture.m1];
  assert(
    actual.length === expected.length &&
      actual.every((entry, index) => entry.marker === expected[index]),
    "Provider ledger has exactly the expected deliveries in order",
  );
  if (complete) {
    const completions = ledger.filter((entry) => entry.event === "complete");
    assert(
      completions.length === 2 &&
        completions.every(
          (entry, index) => entry.reason === "end_turn" && entry.marker === expected[index],
        ),
      "Both delivered prompts have terminal provider evidence",
    );
  }
}
export function assertControlUnchanged(before, after, assert = NodeAssert.ok) {
  assert(
    JSON.stringify(before) === JSON.stringify(after),
    "Control thread domain rows remain byte-stable",
  );
}
export function assertDraft(actual, expected, assert = NodeAssert.ok) {
  assert(actual === expected, "Normal unsent draft is restored unchanged");
}

// Serialized into the isolated renderer: this adapter exposes two reads and one exact replay.
export async function queueRpc({ endpoint, action, threadId, replay, captured }) {
  for (const [key, protocol] of [
    ["httpBaseUrl", "http:"],
    ["wsBaseUrl", "ws:"],
  ]) {
    const url = new URL(endpoint[key]);
    if (
      url.protocol !== protocol ||
      !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new Error("Queue RPC requires private loopback");
  }
  if (!["providers", "projection", "replay"].includes(action))
    throw new Error("Unsupported queue RPC");
  if (
    action === "replay" &&
    (replay?.type !== "message.dispatch" ||
      replay.threadId !== threadId ||
      JSON.stringify(replay) !== JSON.stringify(captured) ||
      !replay.commandId ||
      !replay.messageId)
  )
    throw new Error("Replay scope mismatch");
  const token = await window.desktopBridge.getLocalEnvironmentBearerToken();
  const response = await fetch(new URL("/api/auth/websocket-ticket", endpoint.httpBaseUrl), {
    method: "POST",
    credentials: "omit",
    headers: { authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error("Queue RPC authorization failed");
  const { ticket } = await response.json();
  if (typeof ticket !== "string" || !ticket) throw new Error("Queue RPC ticket missing");
  const url = new URL(endpoint.wsBaseUrl);
  if (url.pathname === "/" || !url.pathname) url.pathname = "/ws";
  url.searchParams.set("wsTicket", ticket);
  url.searchParams.set("orchestrationProtocol", "2");
  const id = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url.href);
    let settled = false;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.close();
      if (error) reject(error);
      else resolve(value);
    };
    const timer = setTimeout(() => finish(new Error("Queue RPC timeout")), 15000);
    socket.addEventListener("open", () =>
      socket.send(
        JSON.stringify({
          _tag: "Request",
          id,
          tag:
            action === "providers"
              ? "server.getConfig"
              : action === "projection"
                ? "orchestration.getThreadProjection"
                : "orchestration.dispatchCommand",
          payload: action === "providers" ? {} : action === "projection" ? { threadId } : replay,
          headers: [],
        }),
      ),
    );
    socket.addEventListener("message", ({ data }) => {
      try {
        const message = JSON.parse(data);
        if (message._tag === "Ping") {
          socket.send(JSON.stringify({ _tag: "Pong" }));
          return;
        }
        if (message._tag !== "Exit" || String(message.requestId) !== id) return;
        if (message.exit?._tag !== "Success") {
          finish(new Error("Queue RPC refused"));
          return;
        }
        const value = message.exit.value;
        finish(
          null,
          action === "providers"
            ? value.providers.map(
                ({
                  instanceId,
                  driver,
                  enabled,
                  installed,
                  status,
                  checkedAt,
                  models,
                  message,
                }) => ({
                  instanceId,
                  driver,
                  enabled,
                  installed,
                  status,
                  checkedAt,
                  models,
                  checking: message === "Checking ACP Registry agent readiness...",
                }),
              )
            : value,
        );
      } catch {
        finish(new Error("Invalid queue RPC frame"));
      }
    });
    socket.addEventListener("error", () => finish(new Error("Queue RPC transport error")));
    socket.addEventListener("close", () => finish(new Error("Queue RPC closed before receipt")));
  });
}

async function milestone(read, predicate, label, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  do {
    const value = await read();
    if (predicate(value)) return value;
    await NodeTimersPromises.setTimeout(50);
  } while (Date.now() < deadline);
  throw new Error(`Timed out waiting for ${label}`);
}
async function readLedger(directory) {
  const names = (await NodeFSP.readdir(directory))
    .filter((name) => /^ledger-\d+-[a-f0-9-]+\.ndjson$/.test(name))
    .sort();
  if (names.length > 64) throw new Error("Stub process bound exceeded");
  const result = [];
  for (const name of names) {
    const path = NodePath.join(directory, name);
    const stat = await NodeFSP.lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 * 1024)
      throw new Error("Invalid stub ledger");
    const text = await NodeFSP.readFile(path, "utf8");
    const complete = text.slice(0, text.lastIndexOf("\n") + 1);
    result.push(...complete.split("\n").filter(Boolean).map(JSON.parse));
  }
  return result.sort((left, right) => left.at - right.at);
}
async function persisted(fixture) {
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync("/scratch/t3home/userdata/statev2.sqlite", { readOnly: true });
  try {
    db.exec("BEGIN");
    const read = (threadId) =>
      Object.fromEntries(
        Object.entries(TABLES).map(([key, table]) => [
          key,
          db
            .prepare(`SELECT payload_json FROM ${table} WHERE thread_id = ? ORDER BY payload_json`)
            .all(threadId)
            .map((row) => JSON.parse(row.payload_json)),
        ]),
      );
    const target = read(fixture.threadId),
      control = read(fixture.controlThreadId);
    const receipts = db
      .prepare(
        "SELECT command_id, command_type, status, result_sequence FROM orchestration_command_receipts WHERE aggregate_id = ? ORDER BY command_id",
      )
      .all(fixture.threadId);
    const events = db
      .prepare(
        "SELECT sequence, event_id, event_type, command_id FROM orchestration_events WHERE stream_id = ? ORDER BY sequence",
      )
      .all(fixture.threadId);
    db.exec("COMMIT");
    NodeAssert.equal(target.threads.length, 1, "One durable target thread");
    NodeAssert.equal(control.threads.length, 1, "One durable control thread");
    return { target: { ...target, thread: target.threads[0] }, control, receipts, events };
  } finally {
    db.close();
  }
}
async function startTicks(pid) {
  try {
    const stat = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}
async function closeOwnedPeers(directory) {
  const ledger = await readLedger(directory);
  const starts = ledger.filter((entry) => entry.event === "start");
  for (const entry of starts) {
    const ticks = await startTicks(entry.pid);
    if (ticks === null) continue;
    if (!entry.startTicks || ticks !== entry.startTicks)
      throw new Error("Stub process ownership changed; cleanup held");
    process.kill(entry.pid, "SIGTERM");
    await milestone(
      () => startTicks(entry.pid),
      (value) => value === null || value !== entry.startTicks,
      "owned stub exit",
      3000,
    );
  }
  return {
    starts: starts.map(({ pid, ppid, startTicks: ticks, processId }) => ({
      pid,
      ppid,
      startTicks: ticks,
      processId,
    })),
    closed: true,
  };
}

export async function editAndCancel(
  ctx,
  fixture,
  projection,
  { skipEditGesture = false, readDurable = async () => (await persisted(fixture)).target } = {},
) {
  const editor = ctx.page.getByTestId("composer-editor");
  const enter = (text) => editor.fill(text);
  await enter(fixture.draft);
  if (!skipEditGesture) {
    const row = ctx.page
      .locator("li")
      .filter({ has: ctx.page.getByRole("button", { name: "Edit queued message", exact: true }) })
      .filter({ hasText: fixture.m2Original });
    await row.getByRole("button", { name: "Edit queued message", exact: true }).click();
    await enter(fixture.m2Edited);
    await ctx.page.getByRole("button", { name: "Update queued message", exact: true }).click();
    await milestone(
      () => editor.innerText(),
      (value) => value === fixture.draft,
      "normal draft restoration",
    );
  }
  const row = ctx.page
    .locator("li")
    .filter({ has: ctx.page.getByRole("button", { name: "Remove queued message", exact: true }) })
    .filter({ hasText: fixture.m3 });
  await row.getByRole("button", { name: "Remove queued message", exact: true }).click();
  const edited = await milestone(
    readDurable,
    (value) =>
      value.runs.some((run) => run.id === fixture.ids[2].runId && run.status === "cancelled"),
    "M3 cancellation",
  );
  assertQueueState(edited, fixture, "edited", ctx.assert);
  await row.waitFor({ state: "hidden", timeout: 15000 });
  assertCancelledQueueView(await projection(), fixture, ctx.assert);
  assertDraft(await editor.innerText(), fixture.draft, ctx.assert);
}

export function resolveQueueCase(configuration = {}, options = {}) {
  const configured = configuration.queueCase;
  if (
    configured !== undefined &&
    !["workflow", "gate-only", "no-edit-negative"].includes(configured)
  )
    throw new Error("Unknown staged queue case");
  for (const [name, value] of Object.entries(options)) {
    if (!["gateOnly", "skipEditGesture"].includes(name) || typeof value !== "boolean")
      throw new Error("Invalid queue scenario option");
  }
  if (options.gateOnly && options.skipEditGesture)
    throw new Error("Queue cases cannot be combined");
  const requested = options.gateOnly
    ? "gate-only"
    : options.skipEditGesture
      ? "no-edit-negative"
      : "workflow";
  if (configured !== undefined && Object.keys(options).length > 0 && configured !== requested)
    throw new Error("Staged queue case conflicts with function options");
  return configured ?? requested;
}

export function assertFullQueueEvidence(fixture) {
  NodeAssert.equal(fixture.queueCase, "workflow", "Only the workflow case can qualify full JE06");
  NodeAssert.equal(
    fixture.variant,
    "queue-edit-cancel-replay",
    "Gate-only or negative evidence cannot qualify full JE06",
  );
  NodeAssert.equal(fixture.providerGate?.status, "ready", "Full JE06 requires provider readiness");
  NodeAssert.equal(fixture.durable?.users, 3, "Full JE06 requires three durable user messages");
  NodeAssert.equal(fixture.durable?.runs, 3, "Full JE06 requires three durable runs");
  NodeAssert.equal(fixture.durable?.providerTurns, 2, "Full JE06 requires two provider turns");
  NodeAssert.equal(
    fixture.durable?.ids?.length,
    3,
    "Full JE06 requires all three durable identities",
  );
  NodeAssert.equal(
    fixture.replay?.messageId,
    fixture.durable.ids[1].messageId,
    "Full JE06 requires native M2 replay evidence",
  );
  NodeAssert.ok(
    typeof fixture.replay?.commandId === "string" && fixture.replay.commandId.length > 0,
    "Full JE06 requires the replayed native command ID",
  );
}

export async function openProviderSettings(ctx) {
  const destination = new URL(ctx.page.url());
  destination.hash = "/settings/providers";
  await ctx.page.goto(destination.href);
  await ctx.reload();
  await ctx.page.getByRole("heading", { name: "Providers", exact: true }).waitFor({
    state: "visible",
    timeout: 15000,
  });
}

export async function selectQueueTarget(ctx, fixture) {
  const target = ctx.page
    .getByRole("navigation", { name: "Threads", exact: true })
    .getByRole("button", { name: /^JE06 queue target(?:,|$)/ });
  await target.click();
  await ctx.page.waitForURL(
    (url) => {
      const path = url.hash.slice(1).split("?")[0].split("/");
      return (
        path.length === 3 &&
        path[0] === "" &&
        path[1].length > 0 &&
        decodeURIComponent(path[2]) === fixture.threadId
      );
    },
    { timeout: 15000 },
  );
  await target
    .and(ctx.page.locator('[aria-current="page"], [data-active="true"]'))
    .waitFor({ state: "visible", timeout: 15000 });
  await ctx.page.getByTestId("composer-editor").waitFor({ state: "visible", timeout: 15000 });
}

export async function withQueueCleanup(run, cleanup) {
  let failed;
  try {
    await run();
  } catch (error) {
    failed = { error };
  }
  try {
    await cleanup();
  } catch (cleanupError) {
    if (!failed) throw cleanupError;
    throw new AggregateError(
      [failed.error, cleanupError],
      "Queue scenario failed and cleanup is incomplete",
      { cause: cleanupError },
    );
  }
  if (failed) throw failed.error;
}

export default async function queueEditCancel(ctx, options = {}) {
  const queueCase = resolveQueueCase(ctx.configuration, options);
  const gateOnly = queueCase === "gate-only";
  const skipEditGesture = queueCase === "no-edit-negative";
  const runId = NodeCrypto.randomUUID();
  const fixture = {
    runId,
    queueCase,
    projectId: NodeCrypto.randomUUID(),
    threadId: NodeCrypto.randomUUID(),
    controlThreadId: NodeCrypto.randomUUID(),
    m1: `JE06-${runId}-M1`,
    m2Original: `JE06-${runId}-M2-original`,
    m2Edited: `JE06-${runId}-M2-edited`,
    m3: `JE06-${runId}-M3`,
    draft: `JE06-${runId}-D`,
    variant: gateOnly
      ? "gate-only"
      : skipEditGesture
        ? "no-edit-negative"
        : "queue-edit-cancel-replay",
    idempotencyContract: "Native message.dispatch commandId and messageId; not MCP clientRequestId",
  };
  Object.assign(ctx.fixture, fixture);
  const directory = `/scratch/tmp/je06-${runId}`;
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  let observeSocket;
  const watched = [];
  const captured = [];
  await withQueueCleanup(
    async () => {
      const agentFile = "/harness/acp-stub-agent.mjs";
      const stat = await NodeFSP.lstat(agentFile);
      if (!stat.isFile() || stat.isSymbolicLink())
        throw new Error("Staged ACP stub is missing or not a regular file");
      ctx.fixture.agentSha256 = digest(await NodeFSP.readFile(agentFile));
      await ctx.step(
        "Configure the synthetic Local ACP provider through ordinary Settings",
        async () => {
          await ctx.completeOnboarding();
          await openProviderSettings(ctx);
          await ctx.page.getByRole("button", { name: "Add provider", exact: true }).first().click();
          const dialog = ctx.page.getByRole("dialog");
          await dialog.getByRole("button", { name: "Local ACP command", exact: true }).click();
          await dialog.getByLabel("Executable", { exact: true }).fill("/usr/bin/node");
          await dialog.getByLabel("Executable", { exact: true }).press("Tab");
          const args = [agentFile, "--state-dir", directory, "--run-id", runId];
          for (let index = 0; index < args.length; index++) {
            await dialog.getByRole("button", { name: "Add argument", exact: true }).click();
            await dialog.getByLabel(`Argument ${index + 1}`, { exact: true }).fill(args[index]);
            await dialog.getByLabel(`Argument ${index + 1}`, { exact: true }).press("Tab");
          }
          await dialog.getByRole("button", { name: "Next", exact: true }).click();
          await dialog.getByLabel("Label", { exact: true }).fill(`JE06 ${runId}`);
          fixture.instanceId = await dialog.getByLabel("Instance ID", { exact: true }).inputValue();
          ctx.assert(Boolean(fixture.instanceId), "Ordinary UI produced a provider instance ID");
          await dialog.getByRole("button", { name: "Add instance", exact: true }).click();
          await dialog.waitFor({ state: "hidden" });
        },
      );
      const rpc = async (action, extra = {}) =>
        ctx.page.evaluate(queueRpc, {
          endpoint: await ctx.topology(),
          action,
          threadId: fixture.threadId,
          ...extra,
        });
      const projection = () => rpc("projection");
      await ctx.step(
        "Prove ACP initialize/new and fresh production provider readiness",
        async () => {
          const provider = await milestone(
            () => rpc("providers"),
            (providers) => {
              const own = providers.find((p) => p.instanceId === fixture.instanceId);
              if (!own || own.checking) return false;
              if (own.status !== "ready")
                throw new Error(`First Local ACP probe did not qualify: ${own.status}`);
              return true;
            },
            "local ACP readiness",
            65000,
          );
          const own = provider.find((p) => p.instanceId === fixture.instanceId);
          ctx.assert(
            own.enabled &&
              own.installed &&
              own.driver === "acpRegistry" &&
              own.models.some((model) => model.slug === "default"),
            "Local ACP is enabled, ready and exposes the fallback model",
          );
          const ledger = await readLedger(directory);
          ctx.assert(
            ledger.some((entry) => entry.event === "initialize") &&
              ledger.some((entry) => entry.event === "session/new"),
            "The owned stub completed initialize and session/new",
          );
          ctx.fixture.providerGate = {
            instanceId: fixture.instanceId,
            status: own.status,
            checkedAt: own.checkedAt,
            protocolVersion: 2,
          };
        },
      );
      await ctx.capture("queue-provider-ready");
      if (gateOnly) return;
      await ctx.step("Create selected target and unchanged control metadata fixtures", async () => {
        await ctx.dispatch({
          type: "project.create",
          commandId: NodeCrypto.randomUUID(),
          projectId: fixture.projectId,
          title: "JE06 queue project",
          workspaceRoot: ctx.workspace,
        });
        for (const [threadId, title] of [
          [fixture.threadId, "JE06 queue target"],
          [fixture.controlThreadId, "JE06 control"],
        ]) {
          await ctx.dispatch({
            type: "thread.create",
            commandId: NodeCrypto.randomUUID(),
            createdBy: "user",
            creationSource: "web",
            projectId: fixture.projectId,
            threadId,
            title,
            modelSelection: { instanceId: fixture.instanceId, model: "default" },
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
          });
        }
        observeSocket = (socket) => {
          const observe = ({ payload }) => {
            try {
              const frame = JSON.parse(String(payload));
              if (
                frame._tag === "Request" &&
                frame.tag === "orchestration.dispatchCommand" &&
                frame.payload?.type === "message.dispatch" &&
                frame.payload.threadId === fixture.threadId &&
                [fixture.m1, fixture.m2Original, fixture.m3].includes(frame.payload.text)
              ) {
                if (captured.length >= 16) throw new Error("Command capture bound exceeded");
                captured.push(structuredClone(frame.payload));
              }
            } catch {
              /* Unrelated frames are outside this exact synthetic capture. */
            }
          };
          socket.on("framesent", observe);
          watched.push({ socket, observe });
        };
        ctx.page.on("websocket", observeSocket);
        await ctx.page.goto(new URL("/", ctx.page.url()).href);
        await ctx.reload();
        await selectQueueTarget(ctx, fixture);
      });
      const before = await persisted(fixture);
      const editor = ctx.page.getByTestId("composer-editor");
      async function enter(text) {
        await editor.fill(text);
      }
      await ctx.step("Hold M1 and queue M2 and M3 through the composer", async () => {
        await enter(fixture.m1);
        await ctx.page.getByRole("button", { name: "Submit message", exact: true }).click();
        await milestone(
          () => readLedger(directory),
          (rows) => rows.some((row) => row.event === "delivery" && row.marker === fixture.m1),
          "M1 held delivery",
        );
        for (const text of [fixture.m2Original, fixture.m3]) {
          await enter(text);
          await ctx.page.getByRole("button", { name: "Queue message", exact: true }).click();
        }
        const queued = await milestone(
          projection,
          (value) => value.runs.filter((run) => run.status === "queued").length === 2,
          "two queued runs",
        );
        fixture.ids = assertQueueState(queued, fixture, "queued", ctx.assert);
        assertDeliveries(await readLedger(directory), fixture, false, ctx.assert);
      });
      await ctx.capture("queue-held");
      let replayCommand;
      await ctx.step(
        "Edit M2, restore the normal draft, and cancel M3 through queue UI",
        async () => {
          await editAndCancel(ctx, fixture, projection, { skipEditGesture });
          assertDeliveries(await readLedger(directory), fixture, false, ctx.assert);
          const commands = captured.filter((command) => command.text === fixture.m2Original);
          ctx.assert(
            commands.length === 1,
            "Exactly one original M2 command was captured from the UI socket",
          );
          replayCommand = commands[0];
          ctx.assert(
            replayCommand.messageId === fixture.ids[1].messageId,
            "Captured native M2 command owns the persisted queued message",
          );
        },
      );
      await ctx.capture("queue-edited-cancelled");
      await ctx.step(
        "Replay the exact native M2 command without a new message or run",
        async () => {
          const original = await persisted(fixture);
          const receipt = original.receipts.filter(
            (row) => row.command_id === replayCommand.commandId,
          );
          ctx.assert(
            receipt.length === 1 && receipt[0].status === "accepted",
            "Original M2 has one durable accepted receipt",
          );
          const command = assertReplayScope(replayCommand, replayCommand, fixture);
          await rpc("replay", { replay: command, captured: replayCommand });
          const replayed = await persisted(fixture);
          NodeAssert.deepEqual(
            replayed.receipts.filter((row) => row.command_id === replayCommand.commandId),
            receipt,
            "Replay retains the same durable receipt and result sequence",
          );
          NodeAssert.deepEqual(
            replayed.receipts.filter((row) => row.command_type === "message.dispatch"),
            original.receipts.filter((row) => row.command_type === "message.dispatch"),
            "Replay creates no additional message receipt",
          );
          const originalEvents = original.events.filter(
            (event) => event.command_id === replayCommand.commandId,
          );
          ctx.assert(
            originalEvents.some((event) => event.sequence === receipt[0].result_sequence),
            "M2 receipt is bound to persisted command events",
          );
          NodeAssert.deepEqual(
            replayed.events.filter((event) => event.command_id === replayCommand.commandId),
            originalEvents,
            "Replay appends no duplicate M2 events",
          );
          assertCancelledQueueView(await projection(), fixture, ctx.assert);
          assertQueueState(replayed.target, fixture, "edited", ctx.assert);
          assertControlUnchanged(before.control, replayed.control, ctx.assert);
          assertDeliveries(await readLedger(directory), fixture, false, ctx.assert);
          ctx.fixture.replay = {
            commandId: command.commandId,
            messageId: command.messageId,
            payloadSha256: digest(JSON.stringify(command)),
            resultSequence: receipt[0].result_sequence,
          };
        },
      );
      await ctx.step(
        "Release M1 and prove edited M2 alone reaches the provider exactly once",
        async () => {
          await NodeFSP.writeFile(NodePath.join(directory, "release"), runId, {
            flag: "wx",
            mode: 0o600,
          });
          const stored = await milestone(
            () => persisted(fixture),
            (value) => value.target.runs.filter((run) => run.status === "completed").length === 2,
            "two settled completed runs",
            30000,
          );
          assertQueueState(stored.target, fixture, "complete", ctx.assert);
          assertCancelledQueueView(await projection(), fixture, ctx.assert);
          assertControlUnchanged(before.control, stored.control, ctx.assert);
          assertDeliveries(await readLedger(directory), fixture, true, ctx.assert);
          assertDraft(await editor.innerText(), fixture.draft, ctx.assert);
          ctx.assert(
            (await ctx.page
              .getByRole("button", { name: "Edit queued message", exact: true })
              .count()) === 0,
            "Visible queue is empty after settlement",
          );
          const shell = await ctx.readSnapshot();
          const target = shell.threads.filter((thread) => thread.id === fixture.threadId);
          ctx.assert(
            target.length === 1 && target[0].activeRunId === null,
            "Selected thread has no remaining active run",
          );
          ctx.fixture.durable = {
            ids: fixture.ids,
            users: 3,
            runs: 3,
            providerTurns: 2,
            controlSha256: digest(JSON.stringify(stored.control)),
          };
        },
      );
      assertFullQueueEvidence(ctx.fixture);
      await ctx.capture("queue-complete");
    },
    async () => {
      if (observeSocket) ctx.page.off("websocket", observeSocket);
      for (const { socket, observe } of watched) socket.off("framesent", observe);
      try {
        await NodeFSP.writeFile(
          NodePath.join(ctx.artifacts, `je06-${runId}-ledger-before-cleanup.json`),
          JSON.stringify({ runId, ledger: await readLedger(directory) }, null, 2),
          { flag: "wx", mode: 0o600 },
        );
        const cleanup = await closeOwnedPeers(directory);
        const ledger = await readLedger(directory);
        await NodeFSP.writeFile(
          NodePath.join(ctx.artifacts, `je06-${runId}-ledger.json`),
          JSON.stringify({ runId, cleanup, ledger }, null, 2),
          { flag: "wx", mode: 0o600 },
        );
        ctx.fixture.stubCleanup = cleanup;
        await NodeFSP.rm(directory, { recursive: true });
        await NodeAssert.rejects(NodeFSP.access(directory), { code: "ENOENT" });
      } catch (cleanupError) {
        ctx.fixture.stubCleanup = {
          closed: false,
          reason: "Owned peer cleanup incomplete; namespace owner must reconcile",
        };
        throw cleanupError;
      }
    },
  );
}
