// @effect-diagnostics nodeBuiltinImport:off -- Opt-in physical test driver owns only loopback sockets and disposable synthetic homes.
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimers from "node:timers";
import * as NodeURL from "node:url";
import {
  absolute,
  canonical,
  Channel,
  closedListener,
  deadline,
  digest,
  isolate,
  keys,
  loadRuntime,
  object,
  offlineSnapshot,
  ownedRoot,
  parse,
  processIdentity,
  protocol,
  removeOwned,
  snapshot,
  text,
  validateSource,
  verifySource,
  writeEvidence,
  type DomainSnapshot,
  type Live,
  type SourceBinding,
} from "./physicalHost.worker.ts";

type Request = {
  runId: string;
  stage: "import-smoke" | "lifecycle-smoke" | "qualify";
  runs: number;
  local: SourceBinding;
  remote: { source: SourceBinding; scratchParent: string; evidenceParent: string };
  scratchParent: string;
  evidenceParent: string;
  transport: { kind: "relay"; controlPort: number; peerPort: number; relayBindingId: string };
};
const port = (value: unknown): number => {
  NodeAssert.ok(typeof value === "number" && Number.isInteger(value) && value > 0 && value < 65536);
  return value;
};
const requestOf = (input: unknown): Request => {
  const value = object(input);
  keys(value, [
    "schema",
    "runId",
    "stage",
    "runs",
    "local",
    "remote",
    "scratchParent",
    "evidenceParent",
    "transport",
  ]);
  NodeAssert.equal(value.schema, "jones-je08h-request/v1");
  const stage = text(value.stage);
  NodeAssert.ok(stage === "import-smoke" || stage === "lifecycle-smoke" || stage === "qualify");
  NodeAssert.equal(value.runs, stage === "qualify" ? 4 : 1);
  const runId = text(value.runId);
  NodeAssert.match(runId, /^[a-zA-Z0-9-]{1,64}$/u);
  const remote = object(value.remote);
  keys(remote, ["source", "scratchParent", "evidenceParent"]);
  const transport = object(value.transport);
  keys(transport, ["kind", "controlPort", "peerPort", "relayBindingId"]);
  NodeAssert.equal(transport.kind, "relay");
  NodeAssert.notEqual(transport.controlPort, transport.peerPort);
  return {
    runId,
    stage,
    runs: stage === "qualify" ? 4 : 1,
    local: validateSource(value.local),
    remote: {
      source: validateSource(remote.source),
      scratchParent: absolute(remote.scratchParent),
      evidenceParent: absolute(remote.evidenceParent),
    },
    scratchParent: absolute(value.scratchParent),
    evidenceParent: absolute(value.evidenceParent),
    transport: {
      kind: "relay",
      controlPort: port(transport.controlPort),
      peerPort: port(transport.peerPort),
      relayBindingId: text(transport.relayBindingId),
    },
  };
};
const domain = (value: unknown): DomainSnapshot => {
  const entry = object(value);
  keys(entry, ["tables", "sha256"]);
  const tables = object(entry.tables);
  for (const rows of Object.values(tables)) {
    NodeAssert.ok(Array.isArray(rows));
    for (const row of rows) object(row);
  }
  NodeAssert.equal(entry.sha256, digest(canonical(tables)));
  return { tables: tables as DomainSnapshot["tables"], sha256: text(entry.sha256) };
};
const rows = (state: DomainSnapshot, table: string) => {
  const value = state.tables[table];
  NodeAssert.ok(value);
  return value;
};
const dispatches = (state: DomainSnapshot) =>
  rows(state, "orchestration_command_receipts").filter(
    (row) => row.command_type === "message.dispatch",
  );
const threadIdText = "thread:cross-server";
const outsideIdText = "thread:outside-link";
const projectionFor = (state: DomainSnapshot, id: string) => {
  const result: Record<string, readonly Record<string, unknown>[]> = {};
  for (const [table, values] of Object.entries(state.tables)) {
    if (table.startsWith("orchestration_v2_projection_"))
      result[table] = values.filter((row) => row.thread_id === id);
  }
  return result;
};

class Remote {
  private id = 0;
  private closed = false;
  private inFlight = false;
  readonly channel: Channel;
  readonly socket: NodeNet.Socket;
  readonly runId: string;
  readonly evidence: string;
  readonly ended: Promise<void>;
  constructor(socket: NodeNet.Socket, runId: string, evidence: string) {
    this.socket = socket;
    this.runId = runId;
    this.evidence = evidence;
    this.channel = new Channel(socket, socket);
    this.ended = new Promise<void>((resolve, reject) => {
      socket.once("end", resolve);
      socket.once("error", reject);
    });
    void this.ended.catch(() => {});
  }
  async reply(id: number, ms = 15000): Promise<unknown> {
    const value = object(await deadline(this.channel.read(), ms, "remote reply"));
    NodeAssert.equal(value.schema, protocol);
    NodeAssert.equal(value.runId, this.runId);
    NodeAssert.equal(value.id, id);
    NodeAssert.equal(value.ok, true);
    return value.value;
  }
  async call(op: string): Promise<unknown> {
    NodeAssert.ok(!this.closed && !this.inFlight);
    this.inFlight = true;
    const id = ++this.id;
    try {
      await this.channel.send({ schema: protocol, runId: this.runId, id, op });
      return await this.reply(id);
    } finally {
      this.inFlight = false;
    }
  }
  async finish(op: "close" | "cancel" = "close") {
    NodeAssert.ok(!this.closed && !this.inFlight);
    this.closed = true;
    await this.channel.send({ schema: protocol, runId: this.runId, id: ++this.id, op });
    const receipt = await this.readClosed();
    NodeAssert.equal(receipt.cancelled, op === "cancel");
    NodeAssert.equal(receipt.failed, false);
    this.socket.end();
    await deadline(this.ended, 10000, "relay EOF after cleanup");
    return receipt;
  }
  async readClosed(ms = 25000) {
    const receipt = object(await deadline(this.channel.read(), ms, "remote cleanup"));
    NodeAssert.equal(receipt.schema, protocol);
    NodeAssert.equal(receipt.runId, this.runId);
    NodeAssert.equal(receipt.kind, "closed");
    NodeAssert.equal(receipt.clean, true);
    writeEvidence(this.evidence, "b-cleanup.json", receipt);
    return receipt;
  }
  disconnect() {
    this.socket.destroy();
  }
}

const connect = async (
  request: Request,
  runId: string,
  evidence: string,
  fault: "none" | "after-listening" = "none",
) => {
  const socket = NodeNet.connect({ host: "127.0.0.1", port: request.transport.controlPort });
  const remote = new Remote(socket, runId, evidence);
  try {
    await deadline(
      new Promise<void>((resolve, reject) => {
        socket.once("connect", resolve);
        socket.once("error", reject);
      }),
      15000,
      "relay connect",
    );
    const boot = object(await deadline(remote.channel.read(), 15000, "remote boot"));
    NodeAssert.equal(boot.schema, protocol);
    NodeAssert.equal(boot.kind, "boot");
    const identity = object(boot.process);
    NodeAssert.notEqual(
      identity.bootIdSha256,
      processIdentity().bootIdSha256,
      "B must be on a different host",
    );
    NodeAssert.equal(identity.nodeVersion, request.remote.source.nodeVersion);
    NodeAssert.equal(identity.cwd, request.remote.source.repository);
    writeEvidence(evidence, "b-boot.json", boot);
    await remote.channel.send({
      schema: protocol,
      runId,
      id: 0,
      op: "init",
      source: request.remote.source,
      scratchParent: request.remote.scratchParent,
      evidenceParent: request.remote.evidenceParent,
      stage: request.stage,
      fault,
    });
    if (fault === "after-listening") return { remote, ready: undefined };
    const ready = object(await remote.reply(0, 60000));
    if (request.stage !== "import-smoke") {
      NodeAssert.equal(canonical(ready.source), canonical(request.remote.source));
      NodeAssert.equal(canonical(ready.process), canonical(boot.process));
      const url = new globalThis.URL(text(ready.url));
      NodeAssert.equal(url.hostname, "127.0.0.1");
      NodeAssert.equal(url.protocol, "http:");
      port(Number(url.port));
      NodeAssert.ok(text(ready.home).startsWith(`${text(ready.owned)}/`));
      NodeAssert.ok(text(ready.dbPath).startsWith(`${text(ready.home)}/`));
    } else NodeAssert.equal(ready.imported, true);
    writeEvidence(evidence, "b-ready.json", ready);
    return { remote, ready };
  } catch (error) {
    // Loss before a final receipt is unknown; the operator reconciles before another connection.
    socket.end();
    remote.disconnect();
    throw error;
  }
};

const descriptor = async (
  runtime: Awaited<ReturnType<typeof loadRuntime>>,
  live: Live,
  url: string,
) => {
  const [HttpClient, FetchHttpClient] = await Promise.all([
    import("effect/http/HttpClient"),
    import("effect/http/FetchHttpClient"),
  ]);
  return live.run(
    HttpClient.HttpClient.pipe(
      runtime.Effect.flatMap((client) =>
        HttpClient.filterStatusOk(client).get(`${url}/.well-known/t3/environment`),
      ),
      runtime.Effect.flatMap((response) => response.json),
      runtime.Effect.timeout("5 seconds"),
      runtime.Effect.provide(FetchHttpClient.layer),
    ),
  );
};

async function qualifyingRun(
  request: Request,
  runId: string,
  evidence: string,
  signal: AbortSignal,
) {
  verifySource(request.local);
  const root = ownedRoot(request.scratchParent, `je08h-${runId}-`);
  const localCancellation = new AbortController();
  const cancelLocal = () => localCancellation.abort();
  signal.addEventListener("abort", cancelLocal, { once: true });
  if (signal.aborted) cancelLocal();
  // @effect-diagnostics-next-line globalTimers:off -- This native process boundary must enforce cancellation before the Effect runtime loads and after it closes.
  const runDeadline = NodeTimers.setTimeout(cancelLocal, 180000);
  const started = NodePerfHooks.performance.now();
  const phases: { name: string; elapsedMs: number }[] = [];
  const phase = (name: string) => {
    phases.push({ name, elapsedMs: NodePerfHooks.performance.now() - started });
  };
  let runtime: Awaited<ReturnType<typeof loadRuntime>> | undefined;
  let a: Live | undefined;
  let c: Live | undefined;
  let remote: Remote | undefined;
  let remoteDone = false;
  let clean = false;
  let failure: unknown;
  const cleanupFailures: string[] = [];
  try {
    isolate(root);
    const native = processIdentity();
    writeEvidence(evidence, "binding.json", {
      runId,
      source: request.local,
      process: native,
      root,
      transport: request.transport,
    });
    runtime = await loadRuntime();
    localCancellation.signal.throwIfAborted();
    if (request.stage === "import-smoke") {
      const result = await connect(request, runId, evidence);
      remote = result.remote;
      const receipt = await remote.finish();
      remoteDone = true;
      NodeAssert.equal(receipt.failed, false);
      phase("imports-passed");
    } else {
      const aHome = NodePath.join(root, "a");
      const cHome = NodePath.join(root, "c");
      NodeFS.mkdirSync(aHome);
      NodeFS.mkdirSync(cHome);
      const open = async (home: string, name: string) => {
        const pending = runtime!.open(home, localCancellation.signal);
        try {
          return await deadline(pending, 60000, `${name} startup`);
        } catch (error) {
          localCancellation.abort();
          try {
            await deadline(
              pending.then((live) => live.close()),
              20000,
              `${name} startup reconciliation`,
            );
          } catch {
            cleanupFailures.push(`${name} startup cleanup needs reconciliation`);
          }
          throw error;
        }
      };
      a = await open(aHome, "A");
      c = await open(cHome, "C");
      const { remote: connected, ready } = await connect(request, runId, evidence);
      remote = connected;
      NodeAssert.ok(ready);
      const peerUrl = `http://127.0.0.1:${request.transport.peerPort}`;
      NodeAssert.equal(
        object(await descriptor(runtime, a, peerUrl)).environmentId,
        ready.environmentId,
      );
      NodeAssert.equal(
        new Set([a.server.environmentId, c.server.environmentId, ready.environmentId]).size,
        3,
      );
      NodeAssert.notEqual(a.server.config.dbPath, c.server.config.dbPath);
      NodeAssert.notEqual(a.server.config.authorityStateDir, c.server.config.authorityStateDir);
      writeEvidence(evidence, "placement.json", {
        a: {
          environmentId: a.server.environmentId,
          dbPath: a.server.config.dbPath,
          authorityStateDir: a.server.config.authorityStateDir,
          url: a.server.url,
        },
        c: {
          environmentId: c.server.environmentId,
          dbPath: c.server.config.dbPath,
          authorityStateDir: c.server.config.authorityStateDir,
          url: c.server.url,
        },
        b: ready,
        peerUrl,
      });
      phase("ready");
      if (request.stage === "lifecycle-smoke") {
        const before = await descriptor(runtime, a, a.server.url);
        const receipt = await remote.finish("cancel");
        remoteDone = true;
        NodeAssert.equal(receipt.cancelled, true);
        NodeAssert.equal(receipt.failed, false);
        NodeAssert.equal(canonical(await descriptor(runtime, a, a.server.url)), canonical(before));
        phase("remote-cancellation-passed");
      } else {
        const pairing = object(await remote.call("pairing"));
        const link = await a.run(
          a.server.links.link({
            url: peerUrl,
            pairingCode: text(pairing.credential),
            access: "auto",
          }),
        );
        NodeAssert.equal(link.environmentId, ready.environmentId);
        // Pairing data is consumed only here and never included in an evidence frame.
        delete pairing.credential;
        const seeded = object(await remote.call("seed"));
        NodeAssert.equal(seeded.linkedSessions, 1);
        const { CommandId, MessageId, ProjectId, ThreadId } = runtime.contracts;
        const projectId = ProjectId.make("project:cross-server");
        const threadId = ThreadId.make(threadIdText);
        for (const live of [a, c]) {
          const workspaceRoot = NodePath.join(live.server.config.baseDir, "workspace");
          NodeFS.mkdirSync(workspaceRoot);
          await live.run(
            live.server.projects.create({
              commandId: CommandId.make("create:cross-server-project"),
              projectId,
              title: "Same project",
              workspaceRoot,
            }),
          );
          await live.run(runtime.fixture.createThread(live.server, { projectId, threadId }));
        }
        await a.run(
          a.server.threads.sendToThread({
            commandId: CommandId.make("je08h-caller-run"),
            projectId,
            threadId,
            messageId: MessageId.make("je08h-caller-prompt"),
            text: "Synthetic caller; provider execution disabled.",
            mode: "auto",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          }),
        );
        const caller = await a.run(a.server.threads.getThreadShell(threadId));
        NodeAssert.ok(caller);
        NodeAssert.equal(typeof caller.activeRunId, "string");
        const beforeA = await snapshot(a);
        const beforeC = await snapshot(c);
        const beforeB = domain(await remote.call("snapshot"));
        writeEvidence(evidence, "baseline.json", { a: beforeA, b: beforeB, c: beforeC });
        const input = {
          environmentId: link.environmentId,
          threadId,
          message: `JE-08H ${runId}: destination B only.`,
          mode: "queue",
          clientRequestId: `je08h-${runId}-delivery`,
        };
        const send = () =>
          a!.run(
            a!.server
              .call("t3_thread_send", input, threadId)
              .pipe(runtime!.Effect.timeout("15 seconds")),
          );
        const sent = await send();
        NodeAssert.equal(sent.isError, false);
        const Schema = await import("effect/Schema");

        const decode = Schema.decodeUnknownEffect(
          runtime.contracts.OrchestratorMcpThreadSendResult,
        );
        const decoded = await a.run(decode(sent.structuredContent));
        const committed = domain(await remote.call("snapshot"));
        const messages = rows(committed, "orchestration_v2_projection_messages").filter(
          (row) => row.thread_id === threadId,
        );
        NodeAssert.equal(messages.length, 1);
        NodeAssert.equal(messages[0]!.message_id, decoded.messageId);
        NodeAssert.ok(
          text(object(parse(text(messages[0]!.payload_json))).text).includes(input.message),
        );
        NodeAssert.equal(
          rows(committed, "orchestration_v2_projection_runs").filter(
            (row) => row.thread_id === threadId,
          ).length,
          1,
        );
        NodeAssert.equal(rows(committed, "orchestration_v2_projection_provider_turns").length, 0);
        NodeAssert.equal(dispatches(committed).length, 1);
        const receipt = dispatches(committed)[0]!;
        NodeAssert.equal(receipt.aggregate_id, threadId);
        NodeAssert.equal(receipt.status, "accepted");
        NodeAssert.ok(
          rows(committed, "orchestration_events").some(
            (row) => row.sequence === receipt.result_sequence,
          ),
        );
        writeEvidence(evidence, "committed.json", {
          state: committed,
          result: sent.structuredContent,
        });
        const retried = await send();
        NodeAssert.equal(retried.isError, false);
        NodeAssert.equal(canonical(retried.structuredContent), canonical(sent.structuredContent));
        NodeAssert.equal(domain(await remote.call("snapshot")).sha256, committed.sha256);
        const refused = await a.run(
          a.server.call(
            "t3_thread_send",
            { ...input, threadId: outsideIdText, clientRequestId: `${runId}-outside` },
            threadId,
          ),
        );
        NodeAssert.equal(object(runtime.fixture.failureOf(refused)).code, "capability_denied");
        const unlinked = await a.run(
          a.server.call(
            "t3_thread_send",
            {
              ...input,
              environmentId: c.server.environmentId,
              clientRequestId: `${runId}-unlinked`,
            },
            threadId,
          ),
        );
        NodeAssert.equal(object(runtime.fixture.failureOf(unlinked)).code, "invalid_request");
        const after = domain(await remote.call("snapshot"));
        NodeAssert.equal(after.sha256, committed.sha256);
        NodeAssert.equal(
          canonical(projectionFor(after, outsideIdText)),
          canonical(projectionFor(beforeB, outsideIdText)),
        );
        NodeAssert.equal((await snapshot(a)).sha256, beforeA.sha256);
        NodeAssert.equal((await snapshot(c)).sha256, beforeC.sha256);
        phase("delivery-replay-fences-passed");
        const stopped = object(await remote.call("stop-server"));
        NodeAssert.equal(stopped.listenerClosed, true);
        NodeAssert.equal(stopped.runtimeStateRemoved, true);
        NodeAssert.equal(domain(stopped.state).sha256, committed.sha256);
        writeEvidence(evidence, "b-offline.json", stopped);
        let disconnected: { outcome: string; code?: string };
        try {
          const response = await send();
          NodeAssert.equal(response.isError, true, "Offline destination cannot report success");
          disconnected = {
            outcome: "refused",
            code: text(object(runtime.fixture.failureOf(response)).code),
          };
        } catch (error) {
          if (error instanceof NodeAssert.AssertionError) throw error;
          disconnected = { outcome: "unknown-until-ledger-reconciliation" };
        }
        const offline = domain(await remote.call("closed-snapshot"));
        NodeAssert.equal(offline.sha256, committed.sha256);
        NodeAssert.equal(dispatches(offline).length, 1);
        NodeAssert.equal((await snapshot(a)).sha256, beforeA.sha256);
        NodeAssert.equal((await snapshot(c)).sha256, beforeC.sha256);
        writeEvidence(evidence, "disconnect.json", {
          call: disconnected,
          reconciled: true,
          state: offline,
        });
        phase("offline-refusal-no-duplicate-passed");
        const closed = await remote.finish();
        remoteDone = true;
        NodeAssert.equal(closed.failed, false);
        const aUrl = a.server.url;
        const cUrl = c.server.url;
        const aDb = a.server.config.dbPath;
        const cDb = c.server.config.dbPath;
        await a.close();
        await c.close();
        await closedListener(aUrl);
        await closedListener(cUrl);
        const finalA = await offlineSnapshot(aDb);
        const finalC = await offlineSnapshot(cDb);
        NodeAssert.equal(finalA.sha256, beforeA.sha256);
        NodeAssert.equal(finalC.sha256, beforeC.sha256);
        writeEvidence(evidence, "ac-offline.json", { a: finalA, c: finalC });
        phase("durable-readback-passed");
      }
    }
  } catch (error) {
    failure = error;
  } finally {
    if (remote && !remoteDone) {
      try {
        const result = await remote.finish();
        remoteDone = result.clean === true;
      } catch {
        cleanupFailures.push("B cleanup unknown; reconcile captured remote identity before retry");
      }
    }
    remote?.disconnect();
    for (const live of [c, a]) {
      if (!live) continue;
      try {
        await deadline(live.close(), 20000, "local server close");
        await closedListener(live.server.url);
        NodeAssert.equal(NodeFS.existsSync(live.server.config.serverRuntimeStatePath), false);
      } catch {
        cleanupFailures.push("local server cleanup unknown");
      }
    }
    if (runtime) {
      try {
        await deadline(runtime.dispose(), 20000, "local runtime close");
      } catch {
        cleanupFailures.push("local runtime cleanup unknown");
      }
    }
    if (cleanupFailures.length === 0) {
      try {
        removeOwned(root);
        clean = true;
      } catch {
        cleanupFailures.push("local root cleanup failed");
      }
    }
    NodeTimers.clearTimeout(runDeadline);
    signal.removeEventListener("abort", cancelLocal);
    phase("cleanup-ended");
    writeEvidence(evidence, "result.json", {
      runId,
      stage: request.stage,
      outcome: failure === undefined && clean && remoteDone ? "passed" : "failed",
      error:
        failure === undefined ? null : failure instanceof Error ? failure.name : "UnknownFailure",
      localCleanup: clean,
      remoteCleanup: remoteDone,
      cleanupFailures,
      phases,
      relayBindingId: request.transport.relayBindingId,
      relayAndTunnelCleanup: "operator-owned-pending-readback",
      physicalQualification: "pending-operator-route-and-process-readback",
    });
  }
  if (failure !== undefined) throw failure;
  NodeAssert.ok(clean && remoteDone, "Cleanup must be known before another run");
}

async function injectedFailureRun(request: Request, runId: string, evidence: string) {
  const { remote } = await connect(request, runId, evidence, "after-listening");
  try {
    const receipt = await remote.readClosed(60000);
    NodeAssert.equal(receipt.failed, true);
    NodeAssert.equal(receipt.clean, true);
    remote.socket.end();
    await deadline(remote.ended, 10000, "failed-setup relay EOF");
    writeEvidence(evidence, "result.json", {
      runId,
      outcome: "expected-setup-failure-cleaned",
      remoteCleanup: true,
      relayAndTunnelCleanup: "operator-owned-pending-readback",
    });
  } finally {
    remote.disconnect();
  }
}

async function main() {
  NodeAssert.equal(process.argv.length, 4);
  NodeAssert.equal(process.argv[2], "--request");
  const requestPath = absolute(process.argv[3]);
  const stat = NodeFS.lstatSync(requestPath);
  NodeAssert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 1024 * 1024);
  const request = requestOf(parse(NodeFS.readFileSync(requestPath, "utf8")));
  verifySource(request.local);
  const evidence = ownedRoot(request.evidenceParent, `je08h-${request.runId}-`);
  writeEvidence(evidence, "request.json", request);
  const cancellation = new AbortController();
  const cancel = () => cancellation.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  // @effect-diagnostics-next-line globalTimers:off -- This native process boundary must enforce cancellation before the Effect runtime loads and after it closes.
  const lifetime = NodeTimers.setTimeout(cancel, request.stage === "qualify" ? 4 * 180000 : 240000);
  let passed = 0;
  try {
    for (let index = 0; index < request.runs; index++) {
      cancellation.signal.throwIfAborted();
      const runId = `${request.runId}-${index + 1}`;
      const runEvidence = NodePath.join(evidence, `run-${index + 1}`);
      NodeFS.mkdirSync(runEvidence);
      await qualifyingRun(request, runId, runEvidence, cancellation.signal);
      passed++;
    }
    if (request.stage === "lifecycle-smoke") {
      cancellation.signal.throwIfAborted();
      const failedEvidence = NodePath.join(evidence, "setup-failure");
      NodeFS.mkdirSync(failedEvidence);
      await injectedFailureRun(request, `${request.runId}-failure`, failedEvidence);
    }
    writeEvidence(evidence, "summary.json", {
      stage: request.stage,
      passed,
      expected: request.runs,
      oracleOutcome: "passed",
      physicalQualification: "pending-operator-route-and-process-readback",
      relayBindingId: request.transport.relayBindingId,
    });
    process.stdout.write(
      `${canonical({ evidence, stage: request.stage, passed, physicalQualification: "pending-operator-route-and-process-readback" })}\n`,
    );
  } catch {
    writeEvidence(evidence, "failure.json", {
      stage: request.stage,
      passed,
      expected: request.runs,
      outcome: "failed-or-unknown",
      retry: "reconcile-owned-processes-first",
    });
    process.stderr.write(
      `${canonical({ evidence, stage: request.stage, passed, outcome: "failed-or-unknown" })}\n`,
    );
    process.exitCode = 1;
  } finally {
    NodeTimers.clearTimeout(lifetime);
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}
if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  await main();
}
