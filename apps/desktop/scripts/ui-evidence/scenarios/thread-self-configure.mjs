import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";

// The runner stages this entry as /harness/scenario.mjs and stages top-level
// harness modules alongside it. Source imports retain the same support module.
const support = new URL(
  new URL(import.meta.url).pathname.endsWith("/scenario.mjs")
    ? "./thread-self-configure-support.mjs"
    : "../thread-self-configure-support.mjs",
  import.meta.url,
);
const {
  fixtureRoot,
  selectionA,
  selectionB,
  installFixture,
  fixtureRecord,
  readTrace,
  waitForFixture,
  assertContinuity,
  assertFreshAdmission,
} = await import(support.href);

export default async function threadSelfConfigure(ctx) {
  const id = () => NodeCrypto.randomUUID();
  const projectId = id();
  const threadId = "native-self-config-" + id();
  const controlId = "native-self-config-" + id();
  const pickerId = "native-self-config-" + id();
  const title = "Native self configuration";
  const pickerTitle = "Native explicit picker control";
  const evidence = {
    syntheticProvider: true,
    liveInference: "unproved",
    comparisonExecution: "not performed by --comparison",
    threadId,
    controlId,
    pickerId,
    observations: {},
  };
  Object.assign(ctx.fixture, evidence);
  const snapshot = async (target = threadId) => (await ctx.readFixtureThread(target)).projection;
  const persist = async () => {
    evidence.protocol = await readTrace();
    const bytes = JSON.stringify(evidence, null, 2);
    await NodeFSP.writeFile(ctx.artifacts + "/self-configuration-evidence.json", bytes, {
      mode: 0o600,
    });
    ctx.fixture.lifecycleEvidence = {
      file: "self-configuration-evidence.json",
      sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
      providerEngine: "synthetic",
      liveProviderInference: "unproved",
      savedRequestedObserved: "separate fields in sidecar",
    };
  };
  const open = async (name, target) => {
    await ctx.page
      .getByRole("button", { name: new RegExp("^" + name + "(?:,|$)") })
      .first()
      .click();
    await ctx.page.waitForFunction(
      (id) => location.pathname.includes(id) || location.hash.includes(id),
      target,
    );
    await ctx.page.getByTestId("composer-editor").waitFor();
  };
  const send = async (text) => {
    const editor = ctx.page.getByTestId("composer-editor");
    await editor.fill(text);
    await editor.press("Enter");
  };
  const picker = () => ctx.page.locator('[data-chat-provider-model-picker="true"]').first();
  const settled = (target, count) =>
    waitForFixture(
      async () => {
        const projection = await snapshot(target);
        evidence.lastSettlement = { threadId: target, expectedRuns: count, projection };
        return projection.runs.length === count &&
          projection.runs.every((run) => run.status === "completed") &&
          projection.runs.every((run) =>
            projection.checkpoints.some(
              (checkpoint) => checkpoint.runId === run.id && checkpoint.status === "ready",
            ),
          )
          ? projection
          : null;
      },
      "durable run and checkpoint completion",
      30000,
    );

  try {
    await ctx.step(
      "Install isolated synthetic engine and valid Git checkpoint fixture",
      async () => {
        evidence.git = await installFixture(ctx.workspace);
        ctx.assert(/^[a-f0-9]{40}$/.test(evidence.git.initialCommit), "Valid disposable Git HEAD");
      },
    );
    await ctx.step("Complete onboarding and register only synthetic A/B", async () => {
      await ctx.completeOnboarding();
      await ctx.page.getByRole("button", { name: "Settings", exact: true }).click();
      const followUp = ctx.page.getByRole("combobox", { name: "Follow-up behavior", exact: true });
      await followUp.click();
      await ctx.page.getByRole("option", { name: "Steer", exact: true }).click();
      ctx.assert(
        (await followUp.textContent()).trim() === "Steer",
        "Disposable UI follow-up preference is Steer",
      );
      evidence.composerSubmission = {
        followUpBehavior: "steer",
        configuredThrough: "native Settings / General / Follow-up behavior",
        verifiedBeforeStart: true,
        ordinaryKey: "Enter",
        modelPickerChangedBeforeFollowup: false,
        defaultQueueBehaviorClaimed: false,
      };
      ctx.fixture.composerSubmission = evidence.composerSubmission;
      await ctx.capture("steer-preference");
      await ctx.page
        .getByRole("button", { name: "Back to conversation from Settings", exact: true })
        .click();
      await ctx.registerSyntheticProvider(selectionA.instanceId);
      await ctx.registerSyntheticProvider(selectionB.instanceId);
      await ctx.dispatch({
        type: "project.create",
        commandId: id(),
        projectId,
        title: "Native synthetic lifecycle",
        workspaceRoot: ctx.workspace,
      });
      for (const [target, name] of [
        [threadId, title],
        [controlId, "Untouched native control"],
        [pickerId, pickerTitle],
      ])
        await ctx.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: id(),
          projectId,
          threadId: target,
          title: name,
          modelSelection: selectionA,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: evidence.git.branch,
          worktreePath: evidence.git.worktreePath,
        });
      evidence.controlBefore = await snapshot(controlId);
      await open(title, threadId);
      await picker().filter({ hasText: "Synthetic model A" }).waitFor();
      await picker().click();
      await ctx.page.getByPlaceholder("Search models...").fill("Synthetic model B");
      await ctx.page.getByRole("option").filter({ hasText: "Synthetic model B" }).waitFor();
      await ctx.page.keyboard.press("Escape");
    });
    await ctx.capture("initial-a");
    let held;
    await ctx.step(
      "Start A through composer; authenticated save B and replay controls",
      async () => {
        await send("NATIVE_SELF_CONFIG_START");
        held = await waitForFixture(() => fixtureRecord("held"), "A saves B and holds", 30000);
        evidence.receipts = held;
        const opened = (await readTrace()).find(
          (event) =>
            event.method === "thread/start" && event.nativeThreadId === held.nativeThreadId,
        );
        ctx.assert(
          opened?.issuedCwd === evidence.git.worktreePath &&
            opened.gitDir === evidence.git.gitDir &&
            opened.commonDir === evidence.git.commonDir &&
            opened.head === evidence.git.initialCommit,
          "App-issued cwd resolves to the isolated linked fixture commit",
        );
        ctx.assert(held.before.executionPrecondition !== null, "Real app-issued calling execution");
        ctx.assert(
          held.legacyRejected && held.conflictRejected,
          "Legacy and conflicting calls rejected",
        );
        ctx.assert(
          JSON.stringify(held.replay.acceptance) === JSON.stringify(held.receipt.acceptance),
          "Identical request returns the same durable acceptance",
        );
        ctx.assert(
          held.replay.freshObservation.configurationRevision === held.receipt.acceptance.sequence &&
            held.subsequent.configurationRevision === held.receipt.acceptance.sequence,
          "Replay and conflict add no selection event",
        );
        ctx.assert(
          held.afterLegacy.configurationRevision === held.before.configurationRevision,
          "Legacy rejection has no selection dispatch",
        );
        ctx.assert(
          JSON.stringify(held.subsequent.savedSelection) === JSON.stringify(selectionB),
          "Authenticated subsequent tool call sees saved B",
        );
        await picker().filter({ hasText: "Synthetic model B" }).waitFor();
        evidence.beforeFollowup = await snapshot();
      },
    );
    await ctx.capture("saved-b-active-a");
    await ctx.step("SC-COR-001 ordinary UI follow-up continuity", async () => {
      await send("NATIVE_ORDINARY_FOLLOWUP");
      evidence.ordinaryUiSubmitted = true;
      let after;
      try {
        after = await waitForFixture(async () => {
          const receipt = await fixtureRecord("after-steer", held.nativeThreadId);
          const trace = await readTrace();
          const events = trace.filter((event) => event.nativeThreadId === held.nativeThreadId);
          if (
            events.some((event) => event.method === "turn/interrupt") ||
            events.filter((event) => event.method === "turn/start").length > 1
          )
            return { observation: null, violation: true };
          return receipt;
        }, "ordinary UI steer or continuity violation");
      } catch {
        after = { observation: null, violation: true };
      }
      evidence.afterFollowup = await snapshot();
      evidence.observations.afterSteer = after;
      await persist();
      ctx.assert(
        after.observation !== null,
        "SC-COR-001 ordinary UI follow-up continuity: original A receives authenticated steer",
      );
      assertContinuity({
        before: held.before,
        after: after.observation,
        projection: evidence.afterFollowup,
        trace: evidence.protocol,
        nativeThreadId: held.nativeThreadId,
        processId: held.processId,
      });
      ctx.assert(
        true,
        "Same run, attempt, runtime generation and A; exactly one steer without interruption",
      );
    });
    await ctx.capture("ordinary-followup-same-a");
    await ctx.step("Release A and await marker plus durable checkpoint", async () => {
      await NodeFSP.writeFile(fixtureRoot + "/release-" + held.nativeThreadId, "", {
        flag: "wx",
        mode: 0o600,
      });
      evidence.completedA = await settled(threadId, 1);
      assertContinuity({
        before: held.before,
        after: evidence.observations.afterSteer.observation,
        projection: evidence.completedA,
        trace: await readTrace(),
        nativeThreadId: held.nativeThreadId,
        processId: held.processId,
      });
      ctx.assert(
        evidence.completedA.turnItems.some(
          (item) =>
            item.type === "assistant_message" && item.text === "NATIVE_A_CONTINUED_AND_COMPLETED",
        ),
        "A continuation marker persisted",
      );
    });
    await ctx.step(
      "Fresh ordinary UI message admits B with actual outbound selection",
      async () => {
        await send("NATIVE_FRESH_B");
        evidence.completedB = await settled(threadId, 2);
        evidence.protocol = await readTrace();
        const run = assertFreshAdmission({
          projection: evidence.completedB,
          trace: evidence.protocol,
          nativeThreadId: held.nativeThreadId,
          priorRunId: held.before.executionPrecondition.runId,
        });
        const provider = evidence.completedB.providerThreads.find(
          (entry) => entry.id === run.providerThreadId,
        );
        evidence.observations.freshB = {
          savedSelection: evidence.completedB.thread.modelSelection,
          capturedRequestedSelection: run.modelSelection,
          providerReportedObservation: provider?.runtimeIdentity ?? null,
          observationSource: "synthetic Codex subprocess; no live inference",
          reasoningEffortObservation: "unavailable",
        };
        ctx.assert(true, "Fresh B run matches outbound model, effort and Standard tier");
      },
    );
    await ctx.capture("fresh-b-completed");
    await ctx.step("Reload saved B and verify untouched control", async () => {
      await ctx.reload();
      await ctx.page.waitForFunction(
        (id) => location.pathname.includes(id) || location.hash.includes(id),
        threadId,
      );
      await ctx.page.getByTestId("composer-editor").waitFor();
      await picker().filter({ hasText: "Synthetic model B" }).waitFor();
      evidence.reloaded = await snapshot();
      ctx.assert(
        JSON.stringify(evidence.reloaded.thread.modelSelection) === JSON.stringify(selectionB),
        "Saved B persists after reload",
      );
      evidence.controlAfter = await snapshot(controlId);
      ctx.assert(
        JSON.stringify(evidence.controlAfter) === JSON.stringify(evidence.controlBefore),
        "Untouched control thread remains identical",
      );
    });
    await ctx.capture("reloaded-b");
    await ctx.step("Actual explicit UI picker change still transitions once", async () => {
      await open(pickerTitle, pickerId);
      await send("NATIVE_PICKER_START");
      const control = await waitForFixture(() => fixtureRecord("picker-held"), "picker A running");
      evidence.pickerBefore = await snapshot(pickerId);
      await picker().click();
      await ctx.page.getByPlaceholder("Search models...").fill("Synthetic model B");
      await ctx.page.getByRole("option").filter({ hasText: "Synthetic model B" }).click();
      await send("NATIVE_PICKER_CHANGED");
      evidence.pickerAfter = await waitForFixture(
        async () => {
          const projection = await snapshot(pickerId);
          return projection.runs.some(
            (run) =>
              run.modelSelection.instanceId === selectionB.instanceId &&
              run.status === "completed" &&
              projection.checkpoints.some(
                (checkpoint) => checkpoint.runId === run.id && checkpoint.status === "ready",
              ),
          )
            ? projection
            : null;
        },
        "explicit picker transition",
        30000,
      );
      const events = (await readTrace()).filter(
        (event) => event.nativeThreadId === control.nativeThreadId,
      );
      ctx.assert(
        events.filter((event) => event.method === "turn/interrupt").length === 1,
        "Explicit picker change interrupts once",
      );
      ctx.assert(
        events.filter((event) => event.method === "turn/start").length === 2 &&
          events.filter(
            (event) =>
              event.method === "turn/start" &&
              event.instance === "B" &&
              event.input === "NATIVE_PICKER_CHANGED",
          ).length === 1,
        "Explicit picker change starts B exactly once",
      );
      ctx.assert(
        events.filter((event) => event.method === "turn/steer").length === 0,
        "Explicit picker change does not accidentally steer A",
      );
    });
    await ctx.capture("explicit-picker-control");
    evidence.controlAfterPicker = await snapshot(controlId);
    ctx.assert(
      JSON.stringify(evidence.controlAfterPicker) === JSON.stringify(evidence.controlBefore),
      "Explicit picker control leaves untouched thread unchanged",
    );
  } catch (error) {
    evidence.firstFailure = { name: error.name, message: error.message };
    throw error;
  } finally {
    await persist();
  }
}
