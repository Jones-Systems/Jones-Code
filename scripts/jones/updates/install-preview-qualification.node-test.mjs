import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeReadline from "node:readline";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  assertVisible,
  assertInactiveDenied,
  assertBrowserDisabledDenied,
  atomicJson,
  redact,
  scenarioFromPrompt,
  createMilestoneChannel,
  publishMilestone,
  awaitMilestoneRelease,
  readAcknowledgedMilestone,
  runScenario,
  scenarioSecrets,
} from "./install-preview-agent.mjs";
import {
  hashTree,
  isolatedEnvironment,
  readRendererManifest,
  serveRendererFixture,
  stopChild,
  withOwnedScratch,
  fixtureProxy,
} from "./install-preview-qualification.mjs";

const source = NodePath.resolve(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../..",
);

NodeTest.test(
  "native ACP framing retains the exact scenario through T3's instruction wrapper",
  () => {
    const scenario = { mode: "active", steps: [{ action: "wait", text: "Jones main" }] };
    const marker = `JONES_PREVIEW_SCENARIO:${Buffer.from(JSON.stringify(scenario)).toString("base64")}`;
    NodeAssert.deepEqual(
      scenarioFromPrompt([
        {
          type: "text",
          text: `<t3_code_instructions>\nNative context\n</t3_code_instructions>\n\n<user_request>\n${marker}\n</user_request>`,
        },
      ]),
      scenario,
    );
    NodeAssert.throws(
      () => scenarioFromPrompt([{ type: "text", text: JSON.stringify(scenario) }]),
      /scenario marker/,
    );
  },
);

NodeTest.test("a deliberately wrong renderer assertion cannot pass", () => {
  const snapshot = { visibleText: "Jones main · Setup required" };
  assertVisible(snapshot, "Setup required");
  assertVisible(snapshot, "Committed", true);
  NodeAssert.throws(() => assertVisible(snapshot, "Committed"), /Missing visible text/);
  NodeAssert.throws(
    () => assertVisible(snapshot, "Setup required", true),
    /Unexpected visible text/,
  );
  NodeAssert.throws(
    () => assertVisible({ screenshotPath: "/owned/image.png" }, "Setup required"),
    /Missing visible text/,
  );
});

NodeTest.test(
  "a failed controlled page action is captured before native tab closure without retry",
  async () => {
    const failure = new Error(
      "Preview automation waitFor timed out; reconcile via preview_snapshot before repeating",
    );
    const calls = [];
    const snapshot = { visibleText: "Could not add backend", screenshotPath: "/owned/failure.png" };
    let waits = 0;
    const client = {
      callTool: async ({ name }) => {
        calls.push(name);
        if (name === "preview_wait_for" && ++waits === 2) throw failure;
        return {
          structuredContent:
            name === "preview_open"
              ? { tabId: "owned-tab" }
              : name === "preview_snapshot"
                ? snapshot
                : {},
        };
      },
    };
    const report = { calls: [], captures: [], negativeControls: {} };
    await NodeAssert.rejects(
      runScenario(
        client,
        {
          origin: "http://127.0.0.1",
          pairingUrl: "http://127.0.0.1/pair",
          initialText: "Project",
          steps: [{ action: "wait", text: "Source matches this app." }],
        },
        report,
      ),
      (error) => error === failure,
    );
    NodeAssert.deepEqual(calls.slice(-3), [
      "preview_wait_for",
      "preview_snapshot",
      "t3_preview_close",
    ]);
    NodeAssert.equal(waits, 2);
    NodeAssert.deepEqual(report.failedAction, {
      index: 0,
      action: "wait",
      text: "Source matches this app.",
    });
    NodeAssert.equal(report.calls.filter((call) => call.ok === false).length, 1);
    NodeAssert.deepEqual(report.captures, []);
    NodeAssert.equal(report.failureReconciliation.kind, "text-only");
    NodeAssert.equal(report.failureReconciliation.contentOmitted, true);
    NodeAssert.equal(report.controlledPageReconciled, true);
    NodeAssert.equal(report.nativeTabClosed, true);
  },
);

NodeTest.test(
  "inactive-provider qualification rejects unrelated failures and successful calls",
  async () => {
    for (const response of [
      undefined,
      { isError: false },
      { isError: true, content: [{ type: "text", text: "browser crashed" }] },
    ]) {
      await NodeAssert.rejects(assertInactiveDenied({ callTool: async () => response }));
    }
    await NodeAssert.rejects(
      assertInactiveDenied({
        callTool: async () => {
          throw new Error("socket closed");
        },
      }),
    );
    await NodeAssert.rejects(
      assertInactiveDenied({
        callTool: async () => {
          throw new Error("preview failed after 4010 milliseconds");
        },
      }),
    );
    NodeAssert.equal(
      await assertInactiveDenied({
        callTool: async () => ({
          isError: true,
          content: [{ type: "text", text: "parent_not_active" }],
        }),
      }),
      "tool authorization refused",
    );
    NodeAssert.equal(
      await assertInactiveDenied({
        callTool: async () => {
          throw new Error("HTTP 401 Unauthorized");
        },
      }),
      "transport authorization refused",
    );
  },
);

NodeTest.test(
  "failed pairing reconciliation publishes no pixels, pasted URL or separated pairing code",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const code = "SYNTHETIC+PAIRING/CODE";
        const pairingUrl = `http://127.0.0.1:41001/pair#token=${encodeURIComponent(code)}`;
        const primaryCode = "SYNTHETIC-PRIMARY-CODE";
        const scenario = {
          origin: "http://127.0.0.1:41000",
          pairingUrl: `http://127.0.0.1:41000/pair#token=${primaryCode}`,
          initialText: "Project",
          steps: [
            { action: "type", locator: 'role=textbox[name="Host"]', text: pairingUrl },
            { action: "click", locator: 'role=button[name="Add environment"]' },
            { action: "wait", text: "Source matches this app." },
          ],
        };
        const failure = new Error(`Pairing failed for ${pairingUrl}; displayed code ${code}`);
        const imageBytes = Buffer.from(`pixels containing ${code}`).toString("base64");
        const snapshotCalls = [];
        let waits = 0;
        const client = {
          callTool: async ({ name, arguments: args }) => {
            if (name === "preview_wait_for" && ++waits === 2) throw failure;
            if (name === "preview_snapshot") {
              snapshotCalls.push(args);
              return {
                structuredContent: {
                  url: pairingUrl,
                  visibleText: `Host 127.0.0.1 Pairing code ${code}`,
                  interactiveElements: [{ value: code }],
                  screenshot: { data: imageBytes },
                  screenshotPath: "/owned/must-not-publish.png",
                },
              };
            }
            return { structuredContent: name === "preview_open" ? { tabId: "pairing-tab" } : {} };
          },
        };
        const report = { calls: [], captures: [], negativeControls: {} };
        await NodeAssert.rejects(
          runScenario(client, scenario, report),
          (error) => error === failure,
        );
        report.error = failure.message;
        NodeAssert.deepEqual(snapshotCalls, [
          { tabId: "pairing-tab", save: false, includeImage: false },
        ]);
        NodeAssert.equal(waits, 2);
        NodeAssert.deepEqual(report.captures, []);
        NodeAssert.equal(report.nativeTabClosed, true);
        const secrets = scenarioSecrets(scenario);
        NodeAssert.ok(secrets.includes(code) && secrets.includes(primaryCode));
        NodeAssert.ok(
          scenarioSecrets({
            pairingUrl: `http://127.0.0.1/pair?token=${encodeURIComponent(code)}`,
          }).includes(code),
        );
        const files = milestoneFiles(root);
        const channel = await createMilestoneChannel({
          runId: "failed-pairing",
          caseId: "redaction",
          files,
        });
        defer(() => channel.close());
        const published = JSON.parse(redact(report, secrets));
        await publishMilestone(channel.binding, "final", files.final, published);
        const readback = await readAcknowledgedMilestone(
          files.final,
          await channel.wait(["final"]),
        );
        const text = JSON.stringify(readback);
        for (const secret of [pairingUrl, scenario.pairingUrl, code, primaryCode, imageBytes])
          NodeAssert.equal(text.includes(secret), false);
        NodeAssert.equal(text.includes("screenshotPath"), false);
        NodeAssert.deepEqual(Object.keys(readback.failureReconciliation).sort(), [
          "contentOmitted",
          "kind",
          "visibleTextLength",
        ]);
        NodeAssert.equal(readback.failureReconciliation.contentOmitted, true);
        NodeAssert.deepEqual(await NodeFSP.readdir(root), ["final.json"]);
      });
    });
  },
);

NodeTest.test(
  "disabled-browser qualification requires the native preview capability refusal",
  () => {
    for (const result of [
      undefined,
      { isError: false },
      {
        isError: true,
        content: [
          { type: "text", text: "PreviewAutomationUnavailableError: Chrome launch failed" },
        ],
      },
    ]) {
      NodeAssert.throws(() => assertBrowserDisabledDenied(result));
    }
    assertBrowserDisabledDenied({
      isError: true,
      content: [
        {
          type: "text",
          text: "MCP credential does not grant the preview capability: browser preview tools are off for this thread.",
        },
      ],
    });
  },
);

NodeTest.test(
  "renderer manifest refuses a stale source revision, tree or undeclared boundary",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      const manifest = {
        schema: "jones-install-renderer-fixture/v1",
        sourceSha: "a".repeat(40),
        sourceTree: "b".repeat(40),
        entry: "jones-install-fixture.html",
        boundary:
          "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
      };
      const file = NodePath.join(root, "jones-install-fixture-manifest.json");
      await atomicJson(file, manifest);
      NodeAssert.deepEqual(
        await readRendererManifest(root, manifest.sourceSha, manifest.sourceTree),
        manifest,
      );
      await NodeAssert.rejects(
        readRendererManifest(root, "c".repeat(40), manifest.sourceTree),
        /candidate source revision/,
      );
      await NodeAssert.rejects(
        readRendererManifest(root, manifest.sourceSha, "d".repeat(40)),
        /candidate source tree/,
      );
      await atomicJson(file, { ...manifest, boundary: "Native installation verified" });
      await NodeAssert.rejects(readRendererManifest(root, manifest.sourceSha, manifest.sourceTree));
    });
  },
);

NodeTest.test(
  "owned scratch removes only its exact directory after success, failure and cancellation",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      const sibling = NodePath.join(parent, "preserved");
      await NodeFSP.writeFile(sibling, "keep");
      for (const result of ["success", "failure", "cancellation"]) {
        let owned;
        const run = withOwnedScratch(parent, async (root) => {
          owned = root;
          await NodeFSP.mkdir(NodePath.join(root, "nested"));
          await NodeFSP.writeFile(NodePath.join(root, "nested/evidence"), result);
          if (result !== "success") throw new Error(result);
        });
        if (result === "success") await run;
        else await NodeAssert.rejects(run, new RegExp(result));
        await NodeAssert.rejects(NodeFSP.lstat(owned), { code: "ENOENT" });
        NodeAssert.equal(await NodeFSP.readFile(sibling, "utf8"), "keep");
      }
    });
  },
);

function milestoneFiles(root, label = "") {
  return Object.fromEntries(
    ["started", "custody", "report", "final", "release"].map((name) => [
      name,
      NodePath.join(root, `${label}${name}.json`),
    ]),
  );
}

async function withFixtureCleanup(use) {
  const cleanups = [];
  let primary;
  try {
    return await use((cleanup) => cleanups.push(cleanup));
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    const results = await Promise.allSettled(
      cleanups.reverse().map((cleanup) => Promise.resolve().then(cleanup)),
    );
    const errors = results
      .filter((result) => result.status === "rejected")
      .map((result) => result.reason);
    if (errors.length) {
      if (primary) {
        primary.cleanupErrors = errors;
        primary.retainScratch = true;
      } else {
        throw Object.assign(new AggregateError(errors, "Owned fixture cleanup failed"), {
          retainScratch: true,
        });
      }
    }
  }
}

async function assertChannelClosed(channel) {
  NodeAssert.equal(channel.evidence().closed, true);
  NodeAssert.equal(channel.evidence().pendingWaiters, 0);
  NodeAssert.equal(channel.evidence().heldRequests, 0);
  await NodeAssert.rejects(
    fetch(channel.binding.endpoint, { signal: AbortSignal.timeout(5_000) }),
    (error) => ["ECONNREFUSED", "UND_ERR_SOCKET"].includes(error.cause?.code),
  );
}

NodeTest.test(
  "the synthetic older host explicitly advertises its supervised update route",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const descriptorPath = NodePath.join(root, ".well-known/t3/environment");
        await NodeFSP.mkdir(NodePath.dirname(descriptorPath), { recursive: true });
        await atomicJson(descriptorPath, {
          label: "Unmanaged fixture origin",
          capabilities: { serverBrowser: true },
          orchestrationProtocolVersion: 42,
        });
        const upstream = await serveRendererFixture(root);
        defer(() => upstream.close());
        const sourceIdentity = {
          repository: "Jones-Systems/Jones-Code",
          sha: "a".repeat(40),
          tree: "b".repeat(40),
        };
        const proxy = await fixtureProxy(upstream, sourceIdentity);
        defer(() => proxy.close());
        const response = await fetch(`${proxy.origin}/.well-known/t3/environment`);
        NodeAssert.equal(response.status, 200);
        const descriptor = await response.json();
        NodeAssert.deepEqual(descriptor.capabilities, {
          serverBrowser: true,
          serverSelfUpdate: "boot-service",
        });
        NodeAssert.equal(descriptor.orchestrationProtocolVersion, 0);
        NodeAssert.deepEqual(descriptor.jonesSource, sourceIdentity);
        NodeAssert.deepEqual(proxy.evidence().requests, [
          { method: "GET", path: "/.well-known/t3/environment", status: 200 },
        ]);
        NodeAssert.match(
          proxy.evidence().fixtureBoundary,
          /synthetic older boot-service descriptor/,
        );
        NodeAssert.equal(proxy.evidence().checks, 0);
      });
    });
  },
);

NodeTest.test(
  "a failed second channel acquisition closes the first owned listener",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      let left;
      const acquisitionError = new Error("forced second acquisition failure");
      await NodeAssert.rejects(
        withFixtureCleanup(async (defer) => {
          left = await createMilestoneChannel({
            runId: "acquisition",
            caseId: "left",
            files: milestoneFiles(root, "left-"),
          });
          defer(() => left.close());
          const response = await fetch(left.binding.endpoint, { headers: { connection: "close" } });
          NodeAssert.equal(response.status, 409);
          await response.json();
          const right = await createMilestoneChannel({
            runId: "acquisition",
            caseId: "right",
            files: milestoneFiles(root, "right-"),
            signal: AbortSignal.abort(acquisitionError),
          });
          defer(() => right.close());
        }),
        (error) => error === acquisitionError,
      );
      await assertChannelClosed(left);
    });
  },
);

NodeTest.test(
  "a rejected child cleanup still closes its channel and readline without masking the primary failure",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      for (const failBody of [false, true]) {
        let channel;
        let child;
        let linesClosed = false;
        const primary = new Error("forced fixture body failure");
        const cleanupFailure = new Error("forced child cleanup rejection after reaping");
        await NodeAssert.rejects(
          withFixtureCleanup(async (defer) => {
            channel = await createMilestoneChannel({
              runId: "cleanup",
              caseId: String(failBody),
              files: milestoneFiles(root, `${failBody}-`),
            });
            defer(() => channel.close());
            child = NodeChildProcess.spawn(
              process.execPath,
              ["--input-type=module", "-e", "process.stdin.resume()"],
              { env: isolatedEnvironment(root), stdio: ["pipe", "pipe", "pipe"] },
            );
            defer(async () => {
              await stopChild(child);
              throw cleanupFailure;
            });
            defer(() => child.stdin.end());
            const lines = NodeReadline.createInterface({ input: child.stdout });
            defer(() => lines.close());
            lines.once("close", () => {
              linesClosed = true;
            });
            await NodeEvents.once(child, "spawn");
            if (failBody) throw primary;
          }),
          (error) => {
            NodeAssert.equal(error.retainScratch, true);
            if (failBody) {
              NodeAssert.equal(error, primary);
              NodeAssert.deepEqual(error.cleanupErrors, [cleanupFailure]);
            } else {
              NodeAssert.ok(error instanceof AggregateError);
              NodeAssert.deepEqual(error.errors, [cleanupFailure]);
            }
            return true;
          },
        );
        NodeAssert.ok(child.exitCode !== null || child.signalCode !== null);
        NodeAssert.equal(linesClosed, true);
        await assertChannelClosed(channel);
      }
    });
  },
);

NodeTest.test(
  "acknowledged milestones handle existing, new and cancelled waits without filesystem notifications",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      const files = milestoneFiles(root);
      const channel = await createMilestoneChannel({ runId: "unit", caseId: "milestones", files });
      try {
        // Writing a file alone must never satisfy a milestone; only a correlated acknowledgment does.
        await atomicJson(files.report, { ready: true });
        const cancellation = new AbortController();
        const unacknowledged = channel.wait(["report"], cancellation.signal);
        cancellation.abort(new Error("owned cancellation"));
        await NodeAssert.rejects(unacknowledged, /owned cancellation/);
        NodeAssert.deepEqual(channel.evidence().receipts, []);
        await publishMilestone(channel.binding, "started", files.started, { beforeWait: true });
        NodeAssert.deepEqual(
          await readAcknowledgedMilestone(files.started, await channel.wait(["started"])),
          { beforeWait: true },
        );
        const pending = channel.wait(["report"], AbortSignal.timeout(5_000));
        await publishMilestone(channel.binding, "report", files.report, { ready: true });
        NodeAssert.deepEqual(await readAcknowledgedMilestone(files.report, await pending), {
          ready: true,
        });
        const release = awaitMilestoneRelease(
          channel.binding,
          files.release,
          AbortSignal.timeout(5_000),
        );
        NodeAssert.equal(await channel.waitingForRelease, true);
        await channel.release({ inactive: true });
        NodeAssert.deepEqual(await release, { inactive: true });
        const abandoned = channel.wait(["final"]);
        const rejected = NodeAssert.rejects(abandoned, /channel closed/);
        await channel.close();
        await rejected;
        NodeAssert.equal(channel.evidence().pendingWaiters, 0);
        NodeAssert.equal(channel.evidence().heldRequests, 0);
        await NodeAssert.rejects(channel.wait(["started"]), /channel closed/);
      } finally {
        await channel.close();
      }
      NodeAssert.deepEqual((await NodeFSP.readdir(root)).sort(), [
        "release.json",
        "report.json",
        "started.json",
      ]);
    });
  },
);

NodeTest.test(
  "owned milestone channels reject malformed correlation and remain isolated with bounded cleanup",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const leftFiles = milestoneFiles(root, "left-");
        const rightFiles = milestoneFiles(root, "right-");
        const left = await createMilestoneChannel({
          runId: "same-run",
          caseId: "left",
          files: leftFiles,
        });
        defer(() => left.close());
        const cancellation = new AbortController();
        const right = await createMilestoneChannel({
          runId: "same-run",
          caseId: "right",
          files: rightFiles,
          signal: cancellation.signal,
        });
        defer(() => right.close());
        const rejected = async (body) => {
          const response = await fetch(left.binding.endpoint, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body,
          });
          NodeAssert.equal(response.status, 409);
          await response.body.cancel();
        };
        await rejected("not-json");
        await rejected(
          JSON.stringify({
            ...right.binding,
            kind: "publish",
            name: "report",
            sha256: "0".repeat(64),
          }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, runId: "wrong-run", kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, caseId: "wrong-case", kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, nonce: "0".repeat(64), kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({
            ...left.binding,
            kind: "publish",
            name: "release",
            sha256: "0".repeat(64),
          }),
        );
        await rejected(
          JSON.stringify({
            ...left.binding,
            kind: "publish",
            name: "report",
            sha256: "0".repeat(64),
            excess: "x".repeat(2048),
          }),
        );
        await publishMilestone(left.binding, "report", leftFiles.report, { owner: "left" });
        NodeAssert.deepEqual(right.evidence().receipts, []);
        await publishMilestone(right.binding, "report", rightFiles.report, { owner: "right" });
        const receipt = await left.wait(["report"]);
        await atomicJson(leftFiles.report, { owner: "tampered" });
        await NodeAssert.rejects(
          readAcknowledgedMilestone(leftFiles.report, receipt),
          /changed before readback/,
        );
        await right.release({ inactive: true });
        NodeAssert.deepEqual(await awaitMilestoneRelease(right.binding, rightFiles.release), {
          inactive: true,
        });
        const cancelled = NodeAssert.rejects(right.wait(["final"]), /channel closed/);
        cancellation.abort(new Error("fixture cancelled"));
        await right.close();
        await cancelled;
        NodeAssert.equal(right.evidence().pendingWaiters, 0);
        const held = awaitMilestoneRelease(left.binding, leftFiles.release);
        const refused = NodeAssert.rejects(held);
        NodeAssert.equal(await left.waitingForRelease, true);
        await left.close();
        await refused;
        NodeAssert.equal(left.evidence().heldRequests, 0);
        await NodeAssert.rejects(
          publishMilestone(left.binding, "final", leftFiles.final, { done: true }),
        );
      });
    });
  },
);

NodeTest.test(
  "unresolved process or evidence cleanup retains its owned scratch without masking the failure",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      let owned;
      const original = Object.assign(new Error("native cleanup failed"), { retainScratch: true });
      await NodeAssert.rejects(
        withOwnedScratch(parent, async (root) => {
          owned = root;
          await NodeFSP.writeFile(NodePath.join(root, "evidence"), "preserve");
          throw original;
        }),
        (error) => error === original && error.cleanupError.includes(owned),
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(owned, "evidence"), "utf8"),
        "preserve",
      );
    });
  },
);

NodeTest.test(
  "artifact identity detects a changed served bundle and refuses symlink traversal",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await NodeFSP.writeFile(NodePath.join(root, "index.html"), "first");
      const before = await hashTree(root);
      await NodeFSP.writeFile(NodePath.join(root, "index.html"), "second");
      NodeAssert.notDeepEqual(await hashTree(root), before);
      await NodeFSP.symlink(NodePath.join(root, "index.html"), NodePath.join(root, "linked.html"));
      await NodeAssert.rejects(hashTree(root), /contains a symlink/);
    });
  },
);

NodeTest.test("fixture environment and evidence exclude ambient credentials", () => {
  const environment = isolatedEnvironment("/owned/fixture");
  NodeAssert.equal(environment.HOME, "/owned/fixture/home");
  NodeAssert.equal(environment.T3CODE_SERVER_BROWSER_SANDBOX, "1");
  NodeAssert.deepEqual(
    Object.keys(environment).sort(),
    [
      "CI",
      "HOME",
      "LANG",
      "NO_COLOR",
      "PATH",
      "T3CODE_DISABLE_TELEMETRY",
      "T3CODE_SERVER_BROWSER_SANDBOX",
      "TMPDIR",
      "XDG_CACHE_HOME",
      "XDG_CONFIG_HOME",
      "XDG_DATA_HOME",
    ].sort(),
  );
  NodeAssert.equal(
    redact("Bearer own-session token=PAIRCODE wsTicket=WS own-secret", ["own-secret"]),
    "Bearer [redacted] token=[redacted] wsTicket=[redacted] [redacted]",
  );
});

NodeTest.test(
  "owned renderer serving records byte identity and rejects paths outside its output",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      const root = NodePath.join(parent, "renderer");
      await NodeFSP.mkdir(root);
      await NodeFSP.writeFile(
        NodePath.join(root, "page.html"),
        "<title>Owned renderer fixture</title>",
      );
      await NodeFSP.writeFile(NodePath.join(parent, "outside"), "must not serve");
      await NodeFSP.symlink(NodePath.join(parent, "outside"), NodePath.join(root, "outside"));
      const server = await serveRendererFixture(root);
      try {
        const response = await fetch(`${server.origin}/page.html`);
        NodeAssert.equal(response.status, 200);
        NodeAssert.match(await response.text(), /Owned renderer fixture/);
        NodeAssert.equal((await fetch(`${server.origin}/outside`)).status, 404);
        NodeAssert.equal(
          (await fetch(`${server.origin}/page.html`, { method: "POST" })).status,
          404,
        );
        NodeAssert.deepEqual(
          server.evidence().requests.map(({ path }) => path),
          ["page.html"],
        );
        NodeAssert.match(server.evidence().requests[0].sha256, /^[a-f0-9]{64}$/);
      } finally {
        await server.close();
      }
    });
  },
);

NodeTest.test(
  "real verifier process refuses an ACP session without native T3 MCP injection",
  { timeout: 15_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      let child;
      await withFixtureCleanup(async (defer) => {
        const report = NodePath.join(root, "report.json");
        const files = {
          started: `${report}.started`,
          custody: `${report}.custody`,
          report,
          final: `${report}.final`,
          release: NodePath.join(root, "release.json"),
        };
        const channel = await createMilestoneChannel({
          runId: "actual-child",
          caseId: "missing-injection",
          files,
        });
        defer(() => channel.close());
        child = NodeChildProcess.spawn(
          process.execPath,
          [NodePath.join(source, "scripts/jones/updates/install-preview-agent.mjs")],
          {
            env: {
              ...isolatedEnvironment(root),
              JONES_PREVIEW_SOURCE: source,
              JONES_PREVIEW_REPORT: report,
              JONES_PREVIEW_RELEASE: files.release,
              JONES_PREVIEW_MILESTONE: JSON.stringify(channel.binding),
            },
            stdio: ["pipe", "pipe", "pipe"],
          },
        );
        defer(() => stopChild(child));
        defer(() => child.stdin.end());
        const lines = NodeReadline.createInterface({ input: child.stdout });
        defer(() => lines.close());
        const pending = new Map();
        let next = 0;
        let stderr = "";
        child.stderr.on("data", (bytes) => {
          stderr += bytes;
        });
        lines.on("line", (line) => {
          const response = JSON.parse(line);
          pending.get(response.id)?.(response);
          pending.delete(response.id);
        });
        const request = (method, params) =>
          new Promise((resolve, reject) => {
            const id = ++next;
            const timer = setTimeout(() => {
              pending.delete(id);
              reject(new Error(`ACP ${method} timeout`));
            }, 5_000);
            pending.set(id, (value) => {
              clearTimeout(timer);
              resolve(value);
            });
            child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
          });
        await NodeEvents.once(child, "spawn");
        const initialized = await request("initialize", {
          protocolVersion: 1,
          clientCapabilities: {},
        });
        NodeAssert.equal(initialized.result.protocolVersion, 1);
        const created = await request("session/new", { cwd: root, mcpServers: [] });
        const scenario = Buffer.from(JSON.stringify({ mode: "active", steps: [] })).toString(
          "base64",
        );
        const result = await request("session/prompt", {
          sessionId: created.result.sessionId,
          prompt: [{ type: "text", text: `JONES_PREVIEW_SCENARIO:${scenario}` }],
        });
        NodeAssert.match(result.error.message, /Normal T3 MCP injection is required/);
        const final = await readAcknowledgedMilestone(
          files.final,
          await channel.wait(["final"], AbortSignal.timeout(5_000)),
        );
        NodeAssert.deepEqual(
          channel.evidence().receipts.map((receipt) => receipt.name),
          ["started", "final"],
        );
        NodeAssert.equal(final.outcome, "failed");
        NodeAssert.equal(final.providerPid, child.pid);
        NodeAssert.equal(final.transportCreated, false);
        NodeAssert.equal(final.transportClosed, false);
        NodeAssert.equal(final.mcpBridgePid, undefined);
        NodeAssert.deepEqual(final.calls, []);
        NodeAssert.equal(stderr, "");
      });
      NodeAssert.ok(child.exitCode !== null || child.signalCode !== null);
    });
  },
);
