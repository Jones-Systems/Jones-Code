import * as NodeAssert from "node:assert/strict";

export function assertBackendPinState(snapshot, fixture, pinned, assert = NodeAssert.ok) {
  const target = snapshot.threads.filter((thread) => thread.id === fixture.threadId);
  const control = snapshot.threads.filter((thread) => thread.id === fixture.controlThreadId);
  assert(target.length === 1, "Backend contains exactly one target thread");
  assert(control.length === 1, "Backend contains exactly one control thread");
  assert(
    target[0].projectId === fixture.projectId && control[0].projectId === fixture.projectId,
    "Both backend threads remain in the synthetic project",
  );
  assert(
    pinned
      ? typeof target[0].pinnedAt === "string" && Number.isFinite(Date.parse(target[0].pinnedAt))
      : target[0].pinnedAt === null && target[0].pinOrderKey === null,
    pinned ? "Backend target is pinned" : "Backend target has no pin metadata",
  );
  if (pinned) {
    // This current-server fixture supports pin reorder; its UI pin creates an order key.
    assert(
      typeof target[0].pinOrderKey === "string" &&
        target[0].pinOrderKey.length > 0 &&
        target[0].pinOrderKey.trim() === target[0].pinOrderKey,
      "Backend target has a pin order key",
    );
  }
  assert(
    control[0].pinnedAt === null && control[0].pinOrderKey === null,
    "Backend control thread remains unpinned",
  );
  return { pinnedAt: target[0].pinnedAt, pinOrderKey: target[0].pinOrderKey };
}
