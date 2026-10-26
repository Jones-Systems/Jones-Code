import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
const supportUrl = new URL(
  new URL(import.meta.url).pathname.endsWith("/scenario.mjs")
    ? "./thread-monitor-provider.mjs"
    : "../thread-monitor-provider.mjs",
  import.meta.url,
);
const {
  fixture,
  selection,
  installMonitorFixture,
  readMonitorLedger,
  waitForMonitor,
  releaseMonitorWork,
  closeMonitorFixture,
} = await import(supportUrl.href);
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export default async function threadMonitorRequests(ctx) {
  const projectId = NodeCrypto.randomUUID(),
    threadId = "native-self-config-" + NodeCrypto.randomUUID();
  const title = "Synthetic monitor requests";
  const implementation = "PLEASE IMPLEMENT THIS PLAN:\n" + fixture.markdown.trim();
  const texts = [fixture.once, fixture.deny, fixture.questions, fixture.plan, implementation];
  const evidence = {
    schema: "jones-monitor-native-requests/v1",
    engine: "synthetic",
    liveInference: "not claimed",
    projectId,
    threadId,
    status: "running",
    captures: [],
    requests: [],
    commands: [],
    limitations: [
      "Current Codex question schema/adapter exposes no multiselect field; genuine option and custom answer paths only. C browser coverage owns the supported multiselect branch.",
    ],
  };
  let peer,
    installed = false,
    primaryError;
  const watched = [];
  const snapshot = async () => (await ctx.readFixtureThread(threadId)).projection;
  const persist = async () => {
    Object.assign(ctx.fixture, evidence);
    const bytes = JSON.stringify(evidence, null, 2);
    await NodeFSP.writeFile(ctx.artifacts + "/thread-monitor-requests.json", bytes, {
      mode: 0o600,
    });
    ctx.fixture.requestEvidence = {
      file: "thread-monitor-requests.json",
      sha256: sha256(bytes),
    };
  };
  const capture = async (name) => {
    const bytes = await peer.screenshot({
      type: "png",
      animations: "disabled",
    });
    await NodeFSP.writeFile(ctx.artifacts + "/" + name + ".png", bytes, {
      flag: "wx",
      mode: 0o600,
    });
    evidence.captures.push({
      file: name + ".png",
      sha256: sha256(bytes),
      syntheticContent: true,
    });
  };
  const observeSocket = (socket) => {
    const observe = ({ payload }) => {
      let frame;
      try {
        frame = JSON.parse(String(payload));
      } catch {
        return;
      }
      if (
        frame._tag !== "Request" ||
        frame.tag !== "orchestration.dispatchCommand" ||
        frame.payload?.threadId !== threadId
      )
        return;
      const command = frame.payload;
      if (
        ![
          "message.dispatch",
          "runtime-request.respond",
          "thread.interaction-mode.set",
          "thread.model-selection.set",
          "thread.settle",
          "run.interrupt",
        ].includes(command.type)
      )
        return;
      if (evidence.commands.length >= 32)
        throw new Error("Synthetic command capture bound exceeded");
      // Retain only this synthetic thread's domain identity and expected values.
      if (command.type === "message.dispatch" && !texts.includes(command.text))
        throw new Error("Unexpected synthetic UI text");
      evidence.commands.push(structuredClone(command));
    };
    socket.on("framesent", observe);
    watched.push({
      socket,
      observe,
    });
  };
  const settled = (count) =>
    waitForMonitor(
      async () => {
        const p = await snapshot();
        return p.runs.length === count &&
          p.runs.every(
            (run) =>
              run.status === "completed" &&
              p.checkpoints.some((c) => c.runId === run.id && c.status === "ready"),
          )
          ? p
          : null;
      },
      "durable completed runs and ready checkpoints",
      45000,
    );
  const waitEnabled = (locator) =>
    waitForMonitor(
      async () =>
        (await locator.isVisible()) &&
        (await locator.getAttribute("aria-disabled")) !== "true" &&
        !(await locator.isDisabled()),
      "native action enabled",
    );
  const send = async (text) => {
    const currentTitle = (await ctx.readSnapshot()).threads.find((t) => t.id === threadId)?.title;
    NodeAssert.ok(currentTitle, "Synthetic thread still exists before reply");
    const editor = peer.locator("form[data-monitor-reply]").getByRole("textbox", {
      name: "Reply to " + currentTitle,
      exact: true,
    });
    await editor.fill(text);
    await waitEnabled(peer.locator("form[data-monitor-reply] button.send"));
    await editor.press("Enter");
    await waitForMonitor(async () => (await editor.inputValue()) === "", "accepted draft cleared");
  };
  const blockedSettle = async (label) => {
    const button = peer.locator("[data-monitor-detail]").getByRole("button", {
      name: "Settle",
      exact: true,
    });
    await waitForMonitor(
      async () => (await button.getAttribute("aria-disabled")) === "true",
      "settlement blocked: " + label,
    );
    const before = await ctx.readSnapshot();
    const runs = (await snapshot()).runs.length;
    // Exercise the disabled handler without Playwright waiting for it to become enabled.
    // aria-disabled and the unchanged domain/command readbacks remain asserted.
    await button.click({ force: true });
    const after = await ctx.readSnapshot();
    NodeAssert.equal(
      after.threads.find((t) => t.id === threadId)?.settledAt,
      before.threads.find((t) => t.id === threadId)?.settledAt,
    );
    NodeAssert.equal((await snapshot()).runs.length, runs);
    ctx.assert(true, "Native Settle rejects " + label + " without a domain effect");
  };
  const pending = async (kind, method) =>
    waitForMonitor(async () => {
      const p = await snapshot();
      const requests = p.runtimeRequests.filter((r) => r.status === "pending" && r.kind === kind);
      const trace = await readMonitorLedger();
      const event = trace.findLast((e) => e.event === "request" && e.method === method);
      if (requests.length !== 1 || !event) return null;
      const request = requests[0];
      NodeAssert.equal(
        request.nativeRequestRef.nativeId,
        event.itemId,
        "Projection resolves the exact provider item ID",
      );
      if (kind === "command") {
        const item = p.turnItems.find(
          (i) => i.type === "approval_request" && i.requestId === request.id,
        );
        NodeAssert.ok(
          item?.options?.some((o) => o.decision === "accept") &&
            item.options.some((o) => o.decision === "decline"),
          "Codex adapter must project its protocol-supported once/deny options; see lane-e/interface-change-request.json. Never fabricate peer choices.",
        );
      }
      evidence.requests.push({
        requestId: request.id,
        nativeItemId: event.itemId,
        rpcId: event.rpcId,
        turnId: event.turnId,
        kind,
      });
      return request;
    }, "actual pending " + kind);
  const resolved = (request) =>
    waitForMonitor(async () => {
      const p = await snapshot(),
        current = p.runtimeRequests.find((r) => r.id === request.id);
      return current?.status === "resolved" && current.resolvedAt !== null ? current : null;
    }, "exact pending request resolved");
  try {
    await ctx.step(
      "Install only the private synthetic monitor provider and linked checkpoint worktree",
      async () => {
        evidence.fixture = await installMonitorFixture(ctx.workspace);
        installed = true;
        evidence.sources = [];
        for (const [name, url] of [
          ["scenario", import.meta.url],
          ["provider", supportUrl],
          ["checkpointSupport", new URL("./thread-self-configure-support.mjs", supportUrl)],
        ]) {
          const bytes = await NodeFSP.readFile(new URL(url));
          evidence.sources.push({
            name,
            path: new URL(url).pathname,
            sha256: sha256(bytes),
            bytes: bytes.length,
          });
        }
        await ctx.completeOnboarding();
        await ctx.registerSyntheticProvider(selection.instanceId);
        await ctx.dispatch({
          type: "project.create",
          commandId: NodeCrypto.randomUUID(),
          projectId,
          title: "Synthetic request verification",
          workspaceRoot: ctx.workspace,
        });
        await ctx.dispatch({
          type: "thread.create",
          commandId: NodeCrypto.randomUUID(),
          createdBy: "user",
          creationSource: "web",
          projectId,
          threadId,
          title,
          modelSelection: selection,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: evidence.fixture.git.branch,
          worktreePath: evidence.fixture.git.worktreePath,
        });
        ctx.page.on("websocket", observeSocket);
        await ctx.reload();
        await ctx.page
          .getByRole("button", {
            name: /^Synthetic monitor requests(?:,|$)/,
          })
          .first()
          .click();
        await ctx.page.getByTestId("composer-editor").waitFor();
        await ctx.page.waitForFunction(
          () => document.documentElement.dataset.threadMonitorPublisherReady === "true",
        );
        peer = await waitForMonitor(async () => {
          for (const candidate of ctx.electronApp.windows()) {
            if (new URL(candidate.url()).pathname !== "/thread-monitor.html") continue;
            if (
              await candidate
                .evaluate(() => !!window.threadMonitor && !window.desktopBridge)
                .catch(() => false)
            )
              return candidate;
          }
          return null;
        }, "dedicated native monitor peer");
        peer.setDefaultTimeout(15000);
        await peer.evaluate((target) => {
          const frames = [];
          const dispose = window.threadMonitor.onFrame((frame) => {
            if (frame.type !== "actionResult" || frame.threadId !== target) return;
            if (frames.length >= 32) throw new Error("Synthetic action result bound exceeded");
            frames.push({
              actionId: frame.actionId,
              commandId: frame.commandId,
              status: frame.status,
            });
          });
          window.__monitorRequestEvidence = {
            frames,
            dispose,
          };
        }, threadId);
        const notch = peer.getByRole("button", {
          name: /^Open thread monitor:/,
        });
        if (await notch.isVisible()) await notch.click();
        await peer.locator('[data-monitor-thread-id="' + threadId + '"]').click();
        await peer
          .getByRole("heading", {
            name: title,
            exact: true,
          })
          .waitFor();
      },
    );
    await ctx.step("Approve once resolves the exact genuine Codex callback", async () => {
      await ctx.page.getByTestId("composer-editor").fill(fixture.once);
      await ctx.page
        .getByRole("button", {
          name: "Submit message",
          exact: true,
        })
        .click();
      const request = await pending("command", "item/commandExecution/requestApproval");
      await waitEnabled(
        peer.getByRole("button", {
          name: "Approve once",
          exact: true,
        }),
      );
      await waitEnabled(
        peer.getByRole("button", {
          name: "Deny",
          exact: true,
        }),
      );
      await blockedSettle("a pending approval");
      await capture("monitor-request-approve-once");
      await peer
        .getByRole("button", {
          name: "Approve once",
          exact: true,
        })
        .click();
      NodeAssert.equal((await resolved(request)).decision, "accept");
      await settled(1);
    });
    await ctx.step(
      "Deny is a separate real turn and never grants a session allowance",
      async () => {
        await send(fixture.deny);
        const request = await pending("command", "item/commandExecution/requestApproval");
        await waitEnabled(
          peer.getByRole("button", {
            name: "Deny",
            exact: true,
          }),
        );
        await blockedSettle("the second approval");
        await capture("monitor-request-deny");
        await peer
          .getByRole("button", {
            name: "Deny",
            exact: true,
          })
          .click();
        NodeAssert.equal((await resolved(request)).decision, "decline");
        await settled(2);
      },
    );
    await ctx.step(
      "Actual option and custom answers preserve original question values",
      async () => {
        await send(fixture.questions);
        const request = await pending("user_input", "item/tool/requestUserInput");
        await peer
          .getByRole("group", {
            name: "Route",
            exact: true,
          })
          .getByRole("radio", {
            name: /^Isolated route/,
          })
          .check();
        await peer
          .getByRole("textbox", {
            name: "Custom answer: Note",
            exact: true,
          })
          .fill(fixture.custom);
        await blockedSettle("pending user questions");
        await capture("monitor-request-questions");
        await waitEnabled(
          peer.getByRole("button", {
            name: "Submit answers",
            exact: true,
          }),
        );
        await peer
          .getByRole("button", {
            name: "Submit answers",
            exact: true,
          })
          .click();
        const answer = await resolved(request);
        NodeAssert.deepEqual(answer.answers, {
          route: "Isolated route",
          note: fixture.custom,
        });
        await settled(3);
      },
    );
    await ctx.step(
      "Use this plan delivers the ordinary accepted continuation exactly once",
      async () => {
        await send(fixture.plan);
        const planned = await settled(4);
        const plan = planned.plans.find(
          (p) => p.kind === "proposed_plan" && p.markdown === fixture.markdown,
        );
        ctx.assert(
          !!plan && plan.status === "active",
          "Actual Codex plan item produces an actionable server plan",
        );
        evidence.planId = plan.id;
        await waitEnabled(
          peer.getByRole("button", {
            name: "Use this plan",
            exact: true,
          }),
        );
        await capture("monitor-request-plan-ready");
        await peer
          .getByRole("button", {
            name: "Use this plan",
            exact: true,
          })
          .click();
        await waitForMonitor(
          async () =>
            (await readMonitorLedger()).some(
              (e) => e.event === "start" && e.marker === "MONITOR_NATIVE_IMPLEMENTATION",
            ),
          "ordinary plan implementation delivered",
        );
        await blockedSettle("active plan implementation work");
        await capture("monitor-request-plan-working");
        await releaseMonitorWork();
        const p = await settled(5),
          ledger = await readMonitorLedger();
        NodeAssert.equal(p.messages.filter((m) => m.role === "user").length, 5);
        NodeAssert.equal(p.messages.filter((m) => m.role === "assistant").length, 5);
        NodeAssert.equal(p.runtimeRequests.length, 3);
        NodeAssert.ok(p.runtimeRequests.every((r) => r.status === "resolved"));
        NodeAssert.equal(p.providerTurns.length, 5);
        for (const text of texts) {
          const users = p.messages.filter((m) => m.role === "user" && m.text === text);
          NodeAssert.equal(users.length, 1, "Exactly one durable message per UI send");
          const runs = p.runs.filter((r) => r.userMessageId === users[0].id);
          NodeAssert.equal(runs.length, 1);
          if (text === implementation)
            NodeAssert.deepEqual(runs[0].sourcePlanRef, {
              threadId,
              planId: plan.id,
            });
        }
        NodeAssert.equal(ledger.filter((e) => e.event === "start").length, 5);
        NodeAssert.equal(ledger.filter((e) => e.event === "completed").length, 5);
        NodeAssert.equal(ledger.filter((e) => e.event === "response").length, 3);
        NodeAssert.ok(!ledger.some((e) => e.event === "forbidden" || e.event === "failure"));
        const responses = evidence.commands.filter((c) => c.type === "runtime-request.respond");
        NodeAssert.equal(responses.length, 3);
        for (const request of evidence.requests)
          NodeAssert.equal(responses.filter((c) => c.requestId === request.requestId).length, 1);
        const messages = evidence.commands.filter((c) => c.type === "message.dispatch");
        NodeAssert.equal(messages.length, 5);
        NodeAssert.equal(new Set(messages.map((c) => c.messageId)).size, 5);
        NodeAssert.equal(
          new Set(evidence.commands.map((c) => c.commandId)).size,
          evidence.commands.length,
        );
        NodeAssert.ok(
          !evidence.commands.some((c) => c.type === "thread.settle" || c.type === "run.interrupt"),
        );
        NodeAssert.ok(
          !messages.some((c) => ["steer_active", "restart_active"].includes(c.dispatchMode?.type)),
        );
        const continuation = messages.filter((c) => c.text === implementation);
        NodeAssert.equal(continuation.length, 1);
        NodeAssert.deepEqual(continuation[0].sourcePlanRef, {
          threadId,
          planId: plan.id,
        });
        evidence.actionResults = await waitForMonitor(async () => {
          const frames = await peer.evaluate(() => window.__monitorRequestEvidence.frames);
          const needed = [continuation[0], ...responses].map((c) => c.commandId);
          return needed.every((id) =>
            frames.some((f) => f.commandId === id && f.status === "accepted"),
          )
            ? frames
            : null;
        }, "accepted native action identities");
        for (const command of [continuation[0], ...responses]) {
          const results = evidence.actionResults.filter((f) => f.commandId === command.commandId);
          NodeAssert.equal(new Set(results.map((r) => r.actionId)).size, 1);
        }
        evidence.projection = p;
        evidence.ledger = ledger;
        ctx.assert(
          true,
          "Native requests and accepted plan have unique command/message/run identities and no implicit steer or interrupt",
        );
        await capture("monitor-request-plan-complete");
      },
    );
    evidence.status = "passed";
  } catch (error) {
    primaryError = error;
    evidence.status = "failed";
    // Only task-owned synthetic text and domain state are retained for discrimination.
    try {
      const projection = await snapshot();
      evidence.failureReadback = {
        title: projection.thread.title,
        runs: projection.runs,
        runtimeRequests: projection.runtimeRequests,
        providerSessions: projection.providerSessions,
        peer: peer
          ? await peer.evaluate(() => ({
              title: document.querySelector("[data-monitor-detail] h2")?.textContent,
              editors: [...document.querySelectorAll("form[data-monitor-reply] textarea")].map(
                (node) => ({
                  label: node.getAttribute("aria-label"),
                  visible: node.checkVisibility(),
                }),
              ),
            }))
          : null,
      };
      if (peer) await capture("monitor-request-failure");
    } catch {
      // Diagnostic failure must not replace the original scenario failure.
    }
    throw error;
  } finally {
    ctx.page.off("websocket", observeSocket);
    for (const { socket, observe } of watched) socket.off("framesent", observe);
    if (peer) await peer.evaluate(() => window.__monitorRequestEvidence?.dispose()).catch(() => {});
    try {
      if (installed)
        evidence.cleanup = await closeMonitorFixture(async (record) => {
          await NodeFSP.writeFile(
            ctx.artifacts + "/monitor-request-" + record.phase + ".json",
            JSON.stringify(record, null, 2),
            {
              flag: "wx",
              mode: 0o600,
            },
          );
        });
    } catch (error) {
      evidence.cleanup = {
        stopped: false,
        status: "unresolved; outer harness owns namespace teardown",
      };
      evidence.status = "failed";
      if (!primaryError) throw error;
    } finally {
      await persist();
    }
  }
}
