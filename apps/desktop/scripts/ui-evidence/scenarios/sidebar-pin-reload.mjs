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

export default async function sidebarPinReload(ctx) {
  const fixture = {
    projectId: NodeCrypto.randomUUID(),
    threadId: NodeCrypto.randomUUID(),
    controlThreadId: NodeCrypto.randomUUID(),
    title: "UI evidence pin target",
    controlTitle: "UI evidence unpinned control",
    workspaceRoot: ctx.workspace,
    pinStateOwner: "server orchestration shell: pinnedAt and pinOrderKey",
    pinEntryPoint: "Linux Control+Shift+P thread.pin shortcut",
  };
  Object.assign(ctx.fixture, fixture);
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
    assertBackendPinState(await ctx.readSnapshot(), fixture, false, ctx.assert);
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
    await ctx.page.keyboard.press("Control+Shift+P");
    await assertSidebarPinState(ctx, fixture, true);
    persistedPin = assertBackendPinState(await ctx.readSnapshot(), fixture, true, ctx.assert);
  });
  await ctx.capture("after-pin");
  await ctx.step("Verify server pin and shelf placement persist after reload", async () => {
    await ctx.reload();
    await assertSidebarPinState(ctx, fixture, true);
    const reloadedPin = assertBackendPinState(await ctx.readSnapshot(), fixture, true, ctx.assert);
    ctx.assert(
      reloadedPin.pinnedAt === persistedPin.pinnedAt &&
        reloadedPin.pinOrderKey === persistedPin.pinOrderKey,
      "Reload retains the same server pin metadata",
    );
  });
  await ctx.capture("after-pin-reload");
  await ctx.step("Unpin through the sidebar button", async () => {
    await shelfRow(ctx.page, fixture.threadId, true)
      .getByRole("button", { name: "Unpin thread", exact: true })
      .click();
    await assertSidebarPinState(ctx, fixture, false);
    assertBackendPinState(await ctx.readSnapshot(), fixture, false, ctx.assert);
  });
  await ctx.capture("after-unpin");
  await ctx.step("Verify unpin persists after reload", async () => {
    await ctx.reload();
    await assertSidebarPinState(ctx, fixture, false);
    assertBackendPinState(await ctx.readSnapshot(), fixture, false, ctx.assert);
  });
  await ctx.capture("after-unpin-reload");
}
