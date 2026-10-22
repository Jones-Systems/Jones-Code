import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";

// The harness stages the selected scenario at /harness/scenario.mjs.
const { pairNodeClient } = await import(
  new URL(import.meta.url).pathname.endsWith("/harness/scenario.mjs")
    ? "./support/node-client.mjs"
    : "../support/node-client.mjs"
);

export function assertProjection(snapshot, fixture, title, pinned, assert = NodeAssert.ok) {
  const target = snapshot?.threads.filter((thread) => thread.id === fixture.threadId) ?? [];
  const control = snapshot?.threads.filter((thread) => thread.id === fixture.controlThreadId) ?? [];
  assert(
    target.length === 1 && control.length === 1,
    "Current projection contains each fixture thread exactly once",
  );
  assert(
    target[0].projectId === fixture.projectId && control[0].projectId === fixture.projectId,
    "Both clients observe the same project",
  );
  assert(target[0].title === title, "Current projection contains the UI-renamed title");
  assert(
    pinned
      ? typeof target[0].pinnedAt === "string" &&
          Number.isFinite(Date.parse(target[0].pinnedAt)) &&
          typeof target[0].pinOrderKey === "string" &&
          target[0].pinOrderKey.trim().length > 0
      : target[0].pinnedAt === null && target[0].pinOrderKey === null,
    "Current projection contains the UI pin state",
  );
  assert(
    control[0].title === fixture.controlTitle &&
      control[0].pinnedAt === null &&
      control[0].pinOrderKey === null,
    "Control thread title and pin remain unchanged",
  );
  return { pinnedAt: target[0].pinnedAt, pinOrderKey: target[0].pinOrderKey };
}

export function metadataBytes(snapshot, fixture) {
  return JSON.stringify(
    [fixture.threadId, fixture.controlThreadId].map((id) => {
      const rows = snapshot.threads.filter((row) => row.id === id);
      NodeAssert.equal(rows.length, 1);
      return Object.fromEntries(
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
        ].map((key) => [key, rows[0][key] ?? null]),
      );
    }),
  );
}

function targetRow(ctx, fixture, pinned) {
  const row = `li[data-thread-item$=":${fixture.threadId}"]`;
  return ctx.page.locator(
    pinned
      ? `[data-testid="sidebar-pinned-header"] ~ ${row}:has(~ [data-testid="sidebar-pinned-divider"])`
      : `[data-testid="sidebar-pinned-divider"] ~ ${row}`,
  );
}

async function assertNativeProjection(ctx, fixture, title, pinned) {
  const row = targetRow(ctx, fixture, pinned);
  await row.getByText(title, { exact: true }).and(row.locator("span[aria-hidden]")).waitFor();
  ctx.assert(
    (await row.count()) === 1 && (await targetRow(ctx, fixture, !pinned).count()) === 0,
    "Electron A displays one target in the expected shelf",
  );
  ctx.assert(
    (await row.getByRole("button", { name: "Unpin thread", exact: true }).count()) ===
      (pinned ? 1 : 0),
    "Electron A pin control matches authoritative state",
  );
}

async function renameThroughUI(ctx, fixture, before, after) {
  await targetRow(ctx, fixture, true)
    .getByRole("button", { name: before })
    .locator("span[aria-hidden]")
    .filter({ hasText: before })
    .dblclick();
  const input = ctx.page.getByRole("textbox", { name: "Thread title", exact: true });
  await input.fill(after);
  await input.press("Enter");
  await assertNativeProjection(ctx, fixture, after, true);
}

export default async function twoClientsOneServer(ctx) {
  const fixture = {
    projectId: NodeCrypto.randomUUID(),
    threadId: NodeCrypto.randomUUID(),
    controlThreadId: NodeCrypto.randomUUID(),
    title: "UI evidence two clients target",
    controlTitle: "UI evidence two clients control",
    liveTitle: "UI evidence renamed with B connected",
    offlineTitle: "UI evidence renamed with B disconnected",
    workspaceRoot: ctx.workspace,
    boundaries: {
      clientA: "real Electron renderer and native shell; UI pin and rename",
      clientB: "Node protocol fixture using ordinary standard pairing and real shell subscription",
      server: "one real private server and disposable database owned by the Electron harness",
      cache: "fixture-owned Node cache; no production client-runtime stale-cache qualification",
      removal:
        "explicit fixture B disconnect, credential and cache disposal; no server session revocation claim",
      seeding: "allowlisted metadata-only fixture commands; no provider turns",
    },
  };
  Object.assign(ctx.fixture, fixture);
  await ctx.step("Complete provider-free first-run UI", () => ctx.completeOnboarding());
  await ctx.step("Seed two metadata-only threads on the sole private server", async () => {
    await ctx.topology();
    await ctx.dispatch({
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId: fixture.projectId,
      title: "UI evidence two clients project",
      workspaceRoot: ctx.workspace,
    });
    for (const [threadId, title] of [
      [fixture.threadId, fixture.title],
      [fixture.controlThreadId, fixture.controlTitle],
    ]) {
      await ctx.dispatch({
        type: "thread.create",
        commandId: NodeCrypto.randomUUID(),
        createdBy: "user",
        creationSource: "web",
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
    await assertNativeProjection(ctx, fixture, fixture.title, false);
    assertProjection(await ctx.readSnapshot(), fixture, fixture.title, false, ctx.assert);
    await targetRow(ctx, fixture, false).getByRole("button", { name: fixture.title }).click();
    await ctx.page.waitForFunction(
      (id) => location.pathname.includes(id) || location.hash.includes(id),
      fixture.threadId,
    );
    await ctx.page.getByTestId("composer-editor").waitFor();
    await ctx.page.getByTestId("composer-editor").focus();
  });

  let client;
  let failed = false;
  let primaryError;
  let cleanupError;
  try {
    let staleSnapshot, pin;
    await ctx.step("Pair Node B with the ordinary standard grant and subscribe", async () => {
      client = await pairNodeClient(ctx);
      await client.connect();
      staleSnapshot = client.snapshot;
      assertProjection(staleSnapshot, fixture, fixture.title, false, ctx.assert);
      ctx.assert(client.evidence.commandCount === 0, "Node B has issued zero commands");
    });
    await ctx.capture("two-clients-before-pin");
    await ctx.step(
      "Electron A pins and renames; Node B observes the real subscription",
      async () => {
        await ctx.page.getByTestId("composer-editor").focus();
        await ctx.page.keyboard.press("Control+Shift+P");
        await assertNativeProjection(ctx, fixture, fixture.title, true);
        await renameThroughUI(ctx, fixture, fixture.title, fixture.liveTitle);
        const observed = await client.waitFor((snapshot) =>
          snapshot?.threads.some(
            (row) =>
              row.id === fixture.threadId &&
              row.title === fixture.liveTitle &&
              row.pinnedAt !== null,
          ),
        );
        pin = assertProjection(
          await ctx.readSnapshot(),
          fixture,
          fixture.liveTitle,
          true,
          ctx.assert,
        );
        ctx.assert(
          JSON.stringify(
            assertProjection(observed, fixture, fixture.liveTitle, true, ctx.assert),
          ) === JSON.stringify(pin),
          "Node B observes the same server pin metadata as Electron A",
        );
        ctx.assert(
          client.evidence.connections[0].updates > 0,
          "B received a live projection update after its initial snapshot",
        );
      },
    );
    await ctx.capture("two-clients-live-update");
    await ctx.step("Disconnect B, rename through A, and reconnect B exactly once", async () => {
      const beforeDisconnect = metadataBytes(await ctx.readSnapshot(), fixture);
      const cached = client.snapshot;
      await client.disconnect();
      ctx.assert(
        metadataBytes(await ctx.readSnapshot(), fixture) === beforeDisconnect,
        "B disconnect leaves authoritative A metadata unchanged",
      );
      ctx.assert(
        JSON.stringify(client.snapshot) === JSON.stringify(cached),
        "B retains its fixture cache while disconnected",
      );
      await renameThroughUI(ctx, fixture, fixture.liveTitle, fixture.offlineTitle);
      ctx.assert(
        JSON.stringify(client.snapshot) === JSON.stringify(cached),
        "Disconnected B has not observed A's newer title",
      );
      const authoritative = metadataBytes(await ctx.readSnapshot(), fixture);
      await client.connect();
      assertProjection(client.snapshot, fixture, fixture.offlineTitle, true, ctx.assert);
      ctx.assert(
        metadataBytes(client.snapshot, fixture) === authoritative,
        "B reconnect has the exact current authoritative metadata",
      );
      ctx.assert(
        client.evidence.connections.length === 2 &&
          client.evidence.connections.every((item) => item.snapshots === 1 && item.synchronized),
        "Both B connections deliver exactly one authoritative initial projection",
      );
      ctx.assert(
        metadataBytes(await ctx.readSnapshot(), fixture) === authoritative,
        "B reconnect leaves authoritative A metadata unchanged",
      );
    });
    await ctx.capture("two-clients-reconnected");
    await ctx.step("Reject B's stale fixture cache and explicitly remove only B", async () => {
      const authoritative = metadataBytes(await ctx.readSnapshot(), fixture);
      const current = client.snapshot;
      ctx.assert(
        !client.restoreCachedSnapshot(staleSnapshot),
        "Stale B fixture snapshot is rejected",
      );
      ctx.assert(
        JSON.stringify(client.snapshot) === JSON.stringify(current),
        "Stale B fixture snapshot cannot overwrite newer projection",
      );
      ctx.assert(
        metadataBytes(await ctx.readSnapshot(), fixture) === authoritative,
        "B stale-cache attempt leaves authoritative A metadata unchanged",
      );
      await client.waitFor(() => true);
      ctx.assert(
        client.evidence.commandCount === 0 && client.evidence.requests.length === 2,
        "Node B sent only two subscription requests and zero commands",
      );
      await client.remove();
      const removed = client.evidence;
      ctx.assert(
        removed.removed &&
          removed.socketClosed &&
          removed.credentialsCleared &&
          removed.projectionCleared,
        "Explicit B removal clears B's socket, fixture credentials, and projection",
      );
      ctx.assert(
        !client.restoreCachedSnapshot(current) && client.snapshot === null,
        "Removed B cannot restore its cache",
      );
      await ctx.reload();
      await assertNativeProjection(ctx, fixture, fixture.offlineTitle, true);
      ctx.assert(
        metadataBytes(await ctx.readSnapshot(), fixture) === authoritative,
        "A reload remains authenticated with unchanged authoritative metadata after removing B",
      );
      ctx.assert(
        JSON.stringify(
          assertProjection(
            await ctx.readSnapshot(),
            fixture,
            fixture.offlineTitle,
            true,
            ctx.assert,
          ),
        ) === JSON.stringify(pin),
        "A retains the original UI pin after B's lifecycle",
      );
    });
    await ctx.capture("two-clients-after-removal");
  } catch (error) {
    failed = true;
    primaryError = error;
  } finally {
    if (client) {
      try {
        await client.remove();
      } catch (error) {
        ctx.fixture.nodeCleanupFailed = true;
        cleanupError = error;
      } finally {
        ctx.fixture.nodeClient = client.evidence;
      }
    }
  }
  if (failed) throw primaryError;
  if (cleanupError) throw cleanupError;
}
