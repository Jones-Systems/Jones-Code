import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";

// The runner stages the selected scenario at /harness/scenario.mjs.
const { assertBackendPinState } = await import(
  new URL(import.meta.url).pathname.endsWith("/harness/scenario.mjs")
    ? "./support/pin-oracle.mjs"
    : "../support/pin-oracle.mjs"
);
export { assertBackendPinState };

function shelfRow(page, threadId, pinned) {
  const row = `li[data-thread-item$=":${threadId}"]`;
  return page.locator(
    pinned
      ? `[data-testid="sidebar-pinned-header"] ~ ${row}:has(~ [data-testid="sidebar-pinned-divider"])`
      : `[data-testid="sidebar-pinned-divider"] ~ ${row}`,
  );
}

async function assertSidebarPinState(ctx, fixture, pinned) {
  const target = shelfRow(ctx.page, fixture.threadId, pinned);
  const control = shelfRow(ctx.page, fixture.controlThreadId, false);
  await target.waitFor({ state: "visible", timeout: 15000 });
  await control.waitFor({ state: "visible", timeout: 15000 });
  ctx.assert(
    (await target.count()) === 1 && (await target.isVisible()),
    pinned ? "Target occupies the pinned shelf" : "Target appears below the pinned shelf",
  );
  ctx.assert(
    (await control.count()) === 1 && (await control.isVisible()),
    "Control thread remains below the pinned shelf",
  );
  ctx.assert(
    (await shelfRow(ctx.page, fixture.threadId, !pinned).count()) === 0 &&
      (await shelfRow(ctx.page, fixture.controlThreadId, true).count()) === 0,
    "Neither thread has a duplicate or unexpected shelf placement",
  );
  ctx.assert(
    (await target.getByRole("button", { name: "Unpin thread", exact: true }).count()) ===
      (pinned ? 1 : 0),
    "Target pin control matches shelf placement",
  );
}

export function controlMetadataBytes(snapshot, fixture) {
  const matches = snapshot.threads.filter((thread) => thread.id === fixture.controlThreadId);
  NodeAssert.equal(matches.length, 1, "Backend contains exactly one control thread");
  const thread = matches[0];
  return JSON.stringify(
    Object.fromEntries(
      [
        "id",
        "projectId",
        "title",
        "createdAt",
        "updatedAt",
        "branch",
        "worktreePath",
        "archivedAt",
        "settledOverride",
        "settledAt",
        "snoozedUntil",
        "snoozedAt",
        "pinnedAt",
        "pinOrderKey",
        "activeOrderKey",
        "autoSettleDisabledAt",
      ].map((key) => [key, thread[key] ?? null]),
    ),
  );
}

export function assertControlUnchanged(snapshot, fixture, before, assert = NodeAssert.ok) {
  assert(
    controlMetadataBytes(snapshot, fixture) === before,
    "Control thread stored metadata remains byte-stable",
  );
}

export default async function sidebarPinRestart(ctx, { skipPinGesture = false } = {}) {
  const fixture = {
    projectId: NodeCrypto.randomUUID(),
    threadId: NodeCrypto.randomUUID(),
    controlThreadId: NodeCrypto.randomUUID(),
    title: "UI evidence pin target",
    controlTitle: "UI evidence unpinned control",
    renamedTitle: "UI evidence renamed pinned thread",
    workspaceRoot: ctx.workspace,
    pinStateOwner: "server orchestration shell: pinnedAt and pinOrderKey",
    pinEntryPoint: "Linux Control+Shift+P thread.pin shortcut",
    persistenceBoundary: "graceful Electron and owned server process restart; same private state",
    variant: skipPinGesture ? "no-action-negative" : "pin-rename-reload-restart-unpin",
  };
  Object.assign(ctx.fixture, fixture);
  let controlBefore;
  await ctx.step("Complete provider-free first-run UI", () => ctx.completeOnboarding());
  await ctx.step("Create one project and two distinguishable metadata-only threads", async () => {
    await ctx.dispatch({
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId: fixture.projectId,
      title: "UI evidence pin project",
      workspaceRoot: ctx.workspace,
    });
    for (const [threadId, title] of [
      [fixture.threadId, fixture.title],
      [fixture.controlThreadId, fixture.controlTitle],
    ]) {
      await ctx.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: NodeCrypto.randomUUID(),
        projectId: fixture.projectId,
        threadId,
        title,
        modelSelection: { instanceId: "codex", model: "gpt-5" },
        runtimeMode: "approval-required",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
    }
    await assertSidebarPinState(ctx, fixture, false);
    const initialSnapshot = await ctx.readSnapshot();
    assertBackendPinState(initialSnapshot, fixture, false, ctx.assert);
    controlBefore = controlMetadataBytes(initialSnapshot, fixture);
    ctx.fixture.controlMetadataSha256 = NodeCrypto.createHash("sha256")
      .update(controlBefore)
      .digest("hex");
    await shelfRow(ctx.page, fixture.threadId, false)
      .getByRole("button", { name: fixture.title })
      .click();
    await ctx.page.waitForFunction(
      (id) => location.pathname.includes(id) || location.hash.includes(id),
      fixture.threadId,
    );
    await ctx.page.getByTestId("composer-editor").waitFor();
    await ctx.page.getByTestId("composer-editor").focus();
  });
  await ctx.capture("before-pin");
  let persistedPin;
  await ctx.step("Pin selected thread through the production keyboard shortcut", async () => {
    if (!skipPinGesture) {
      await ctx.page.keyboard.press("Control+Shift+P");
      await assertSidebarPinState(ctx, fixture, true);
    }
    const snapshot = await ctx.readSnapshot();
    persistedPin = assertBackendPinState(snapshot, fixture, true, ctx.assert);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
  });
  await ctx.capture("after-pin");
  const assertRenamed = async (snapshot) => {
    await ctx.page.getByRole("button", { name: fixture.renamedTitle }).first().waitFor();
    ctx.assert(
      await ctx.page.getByRole("button", { name: fixture.renamedTitle }).first().isVisible(),
      "Sidebar retains renamed target title",
    );
    ctx.assert(
      snapshot.threads.some(
        (thread) => thread.id === fixture.threadId && thread.title === fixture.renamedTitle,
      ),
      "Backend retains renamed target title",
    );
  };
  await ctx.step("Rename the pinned target through Sidebar UI", async () => {
    await shelfRow(ctx.page, fixture.threadId, true)
      .getByRole("button", { name: fixture.title })
      .locator("span[aria-hidden]")
      .filter({ hasText: fixture.title })
      .dblclick();
    const input = ctx.page.getByRole("textbox", { name: "Thread title", exact: true });
    await input.fill(fixture.renamedTitle);
    await input.press("Enter");
    await ctx.page.getByRole("button", { name: fixture.renamedTitle }).first().waitFor();
    const snapshot = await ctx.readSnapshot();
    await assertRenamed(snapshot);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
  });
  await ctx.step("Verify pin and rename persist after renderer reload", async () => {
    await ctx.reload();
    await assertSidebarPinState(ctx, fixture, true);
    const snapshot = await ctx.readSnapshot();
    const reloadedPin = assertBackendPinState(snapshot, fixture, true, ctx.assert);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
    ctx.assert(
      reloadedPin.pinnedAt === persistedPin.pinnedAt &&
        reloadedPin.pinOrderKey === persistedPin.pinOrderKey,
      "Renderer reload retains the same server pin metadata",
    );
    await assertRenamed(snapshot);
  });
  await ctx.capture("after-rename-reload");
  await ctx.step(
    "Verify server pin and shelf placement persist after process restart",
    async () => {
      const restart = await ctx.restart();
      ctx.assert(restart.oldProcessesClosed, "Old Electron and backend processes closed");
      await assertSidebarPinState(ctx, fixture, true);
      const snapshot = await ctx.readSnapshot();
      await assertRenamed(snapshot);
      const reloadedPin = assertBackendPinState(snapshot, fixture, true, ctx.assert);
      assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
      ctx.assert(
        reloadedPin.pinnedAt === persistedPin.pinnedAt &&
          reloadedPin.pinOrderKey === persistedPin.pinOrderKey,
        "Process restart retains the same server pin metadata",
      );
    },
  );
  await ctx.capture("after-pin-restart");
  await ctx.step("Unpin through the sidebar button", async () => {
    await shelfRow(ctx.page, fixture.threadId, true)
      .getByRole("button", { name: "Unpin thread", exact: true })
      .click();
    await assertSidebarPinState(ctx, fixture, false);
    const snapshot = await ctx.readSnapshot();
    assertBackendPinState(snapshot, fixture, false, ctx.assert);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
  });
  await ctx.capture("after-unpin");
  await ctx.step("Verify unpin and rename persist after renderer reload", async () => {
    await ctx.reload();
    await assertSidebarPinState(ctx, fixture, false);
    const snapshot = await ctx.readSnapshot();
    assertBackendPinState(snapshot, fixture, false, ctx.assert);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
    await assertRenamed(snapshot);
  });
  await ctx.capture("after-unpin-reload");
  await ctx.step("Verify unpin persists after process restart", async () => {
    const restart = await ctx.restart();
    ctx.assert(restart.oldProcessesClosed, "Old Electron and backend processes closed again");
    await assertSidebarPinState(ctx, fixture, false);
    const snapshot = await ctx.readSnapshot();
    assertBackendPinState(snapshot, fixture, false, ctx.assert);
    assertControlUnchanged(snapshot, fixture, controlBefore, ctx.assert);
    await assertRenamed(snapshot);
  });
  await ctx.capture("after-unpin-restart");
}
