import { it, expect } from "vite-plus/test";
import type { OrchestrationV2AppThread, OrchestrationV2DomainEvent } from "@t3tools/contracts";
import { bindRecreatedCheckout } from "./BindingCommit.ts";

it("binding commits preserve concurrent metadata and refuse a replaced binding or deleted thread", () => {
  const current = {
    id: "thread",
    title: "new owner title",
    worktreePath: "/old",
    branch: "task",
    deletedAt: null,
    pinnedAt: "retained pin",
    updatedAt: "new revision",
  } as unknown as OrchestrationV2AppThread;
  const events = [
    {
      type: "thread.metadata-updated",
      threadId: "thread",
      payload: {
        ...current,
        title: "stale title",
        worktreePath: "/new",
        branch: null,
        updatedAt: "recreated revision",
      },
    },
  ] as unknown as OrchestrationV2DomainEvent[];
  const result = bindRecreatedCheckout(current, { worktreePath: "/old", branch: "task" }, events);
  expect(result?.[0]?.payload).toEqual({
    ...current,
    worktreePath: "/new",
    branch: null,
    updatedAt: "recreated revision",
  });
  expect(
    bindRecreatedCheckout(
      { ...current, worktreePath: "/owner-replacement" },
      { worktreePath: "/old", branch: "task" },
      events,
    ),
  ).toBeNull();
  expect(
    bindRecreatedCheckout(
      { ...current, deletedAt: current.updatedAt },
      { worktreePath: "/old", branch: "task" },
      events,
    ),
  ).toBeNull();
});
