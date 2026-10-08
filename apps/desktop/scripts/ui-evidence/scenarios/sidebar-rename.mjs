import * as NodeCrypto from "node:crypto";

export default async function sidebarRename(ctx) {
  const projectId = NodeCrypto.randomUUID(),
    threadId = NodeCrypto.randomUUID();
  const initialTitle = "UI evidence synthetic thread";
  const renamedTitle = "UI evidence renamed thread";
  Object.assign(ctx.fixture, {
    projectId,
    threadId,
    initialTitle,
    renamedTitle,
    workspaceRoot: ctx.workspace,
  });
  await ctx.step("Complete provider-free first-run UI", () => ctx.completeOnboarding());
  await ctx.step("Create synthetic project and metadata-only thread", async () => {
    await ctx.dispatch({
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId,
      title: "UI evidence synthetic project",
      workspaceRoot: ctx.workspace,
    });
    await ctx.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: NodeCrypto.randomUUID(),
      projectId,
      threadId,
      title: initialTitle,
      modelSelection: { instanceId: "codex", model: "gpt-5" },
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    await ctx.page.getByRole("button", { name: initialTitle }).first().waitFor();
  });
  await ctx.step("Open synthetic thread before editing its title", async () => {
    await ctx.page.getByRole("button", { name: initialTitle }).first().click();
    await ctx.page.waitForFunction(
      (id) => location.pathname.includes(id) || location.hash.includes(id),
      threadId,
    );
    await ctx.page.getByTestId("composer-editor").waitFor();
    await ctx.page.getByTestId("composer-editor").focus();
  });
  await ctx.capture("before");
  await ctx.step("Rename through Sidebar UI", async () => {
    await ctx.page
      .getByRole("button", { name: initialTitle })
      .first()
      .locator("span[aria-hidden]")
      .filter({ hasText: initialTitle })
      .dblclick();
    const input = ctx.page.getByRole("textbox", { name: "Thread title", exact: true });
    await input.fill(renamedTitle);
    await input.press("Enter");
    await ctx.page.getByRole("button", { name: renamedTitle }).first().waitFor();
    ctx.assert(
      await ctx.page.getByRole("button", { name: renamedTitle }).first().isVisible(),
      "Sidebar shows renamed title",
    );
    const snapshot = await ctx.readSnapshot();
    ctx.assert(
      snapshot.threads.some((thread) => thread.id === threadId && thread.title === renamedTitle),
      "Backend snapshot stores renamed title",
    );
  });
  await ctx.capture("after");
  await ctx.step("Verify rename persists after reload", async () => {
    await ctx.reload();
    await ctx.page.getByRole("button", { name: renamedTitle }).first().waitFor();
    ctx.assert(
      await ctx.page.getByRole("button", { name: renamedTitle }).first().isVisible(),
      "Reloaded Sidebar shows renamed title",
    );
    const snapshot = await ctx.readSnapshot();
    ctx.assert(
      snapshot.threads.some((thread) => thread.id === threadId && thread.title === renamedTitle),
      "Backend snapshot retains renamed title after reload",
    );
  });
  await ctx.capture("after-reload");
  await ctx.step("Verify native and rendered dark theme", () => ctx.setTheme("dark"));
  await ctx.capture("after-reload-dark");
}
