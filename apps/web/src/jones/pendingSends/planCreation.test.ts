import { describe, expect, it, vi } from "vite-plus/test";
import { AsyncResult } from "effect/reactivity";
import * as Cause from "effect/Cause";
import type { CreateThreadInput, PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId, EnvironmentId, MessageId, PlanId, ProjectId, ProviderInstanceId, ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import {
  decodePlanCreationOperation,
  type PlanCreationOperation,
} from "@t3tools/client-runtime/jones/planCreationOperation";
import { createPendingSendStore } from "./store";
import { createPlanCreationController } from "./planCreation";
import { createPlanCreationStorage, planCreationStorageKey } from "./planCreationStorage";
import { canSendAfterInitialMessageRejection } from "./newThread";

const environmentId = EnvironmentId.make("plan-recovery");
const slot = {
  environmentId,
  sourceThreadId: ThreadId.make("plan-source"),
  sourcePlanId: PlanId.make("source-plan"),
  action: "implement-in-new-thread" as const,
};
function operation(suffix = "original") {
  const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
  const threadId = ThreadId.make(`target-${suffix}`);
  return decodePlanCreationOperation({
    ...slot, schemaVersion: 1, phase: "create-pending",
    create: {
      commandId: CommandId.make(`create-${suffix}`), threadId,
      projectId: ProjectId.make("project"), title: "Implement captured plan",
      modelSelection: selection, runtimeMode: "full-access", interactionMode: "default",
      branch: "work/plan", worktreePath: "/synthetic/plan", createdAt: "2026-11-05T16:00:00Z",
    },
    prepared: {
      method: "orchestration.dispatchCommand",
      payload: {
        type: "message.dispatch", commandId: CommandId.make(`message-command-${suffix}`),
        threadId, messageId: MessageId.make(`message-${suffix}`), text: "Implement frozen plan",
        attachments: [], createdBy: "user", creationSource: "web",
        modelSelection: selection, modelSelectionExplicit: true,
        sourcePlanRef: { threadId: slot.sourceThreadId, planId: slot.sourcePlanId },
        dispatchMode: { type: "start_immediately" },
      },
    },
  });
}
const failed = (operation: PlanCreationOperation, stage: "create" | "message", status?: "accepted" | "rejected") =>
  AsyncResult.failure(Cause.fail({
    message: "Synthetic lost response",
    ...(status ? { deliveryEvidence: {
      kind: "dispatch", status,
      commandId: stage === "create" ? operation.create.commandId : operation.prepared.payload.commandId,
      commandType: stage === "create" ? "thread.create" : "message.dispatch",
      threadId: operation.create.threadId,
    } } : {}),
  }));
function fixture() {
  const values = new Map<string, string>();
  const storage = {
    getItem: vi.fn((key: string) => values.get(key) ?? null),
    setItem: vi.fn(async (key: string, value: string) => { values.set(key, value); }),
    removeItem: vi.fn(async (key: string) => { values.delete(key); }),
  };
  const journal = createPlanCreationStorage(storage);
  const store = createPendingSendStore();
  const controller = createPlanCreationController(journal, store);
  const accepted = vi.fn();
  const messageRejected = vi.fn();
  const action = {
    slot,
    allocate: vi.fn(async () => operation()),
    create: vi.fn(async (_input: CreateThreadInput): Promise<AtomCommandResult<unknown, unknown>> => AsyncResult.success({ sequence: 1 })),
    dispatch: vi.fn(async (_input: PreparedThreadTurn): Promise<AtomCommandResult<unknown, unknown>> => AsyncResult.success({ sequence: 2 })),
    admitted: vi.fn(() => ({ accepted, messageRejected })),
  };
  return { values, storage, journal, store, controller, action, accepted, messageRejected };
}
const projection = {
  runs: [{ userMessageId: MessageId.make("message-original") }],
  messages: [], visibleTurnItems: [],
} as unknown as OrchestrationV2ThreadProjection;

describe("durable plan create and first message", () => {
  it("acknowledges storage before create and persists the message stage before dispatch", async () => {
    const f = fixture();
    let flush!: () => void;
    let writing!: () => void;
    const entered = new Promise<void>((resolve) => { writing = resolve; });
    f.storage.setItem.mockImplementationOnce(async (key, value) => {
      writing();
      await new Promise<void>((resolve) => { flush = resolve; });
      f.values.set(key, value);
    });
    f.action.create.mockImplementation(async () => {
      expect((await f.journal.read(slot))?.phase).toBe("create-pending");
      return AsyncResult.success({ sequence: 1 });
    });
    f.action.dispatch.mockImplementation(async () => {
      expect((await f.journal.read(slot))?.phase).toBe("message-pending");
      return AsyncResult.success({ sequence: 2 });
    });
    const sending = f.controller.run(f.action);
    await entered;
    expect(f.action.create).not.toHaveBeenCalled();
    flush();
    expect((await sending).status).toBe("accepted");
    expect((await f.journal.read(slot))?.phase).toBe("accepted");
    expect(f.accepted).toHaveBeenCalledTimes(1);
    expect(f.storage.setItem).toHaveBeenCalledTimes(3);
  });

  it.each(["throw", "unflushed"] as const)("emits no create after %s storage acknowledgement", async (failure) => {
    const f = fixture();
    if (failure === "throw") f.storage.setItem.mockRejectedValueOnce(new Error("quota"));
    else f.storage.setItem.mockImplementationOnce(async () => {});
    expect((await f.controller.run(f.action)).status).toBe("held");
    expect(f.action.create).not.toHaveBeenCalled();
    expect(f.action.dispatch).not.toHaveBeenCalled();
  });

  it("holds malformed and unknown saved rows without replacing or evicting them", async () => {
    const f = fixture();
    const key = planCreationStorageKey(environmentId);
    for (const raw of ["{broken", '{"schemaVersion":2,"current":[],"history":[]}']) {
      f.values.set(key, raw);
      expect((await f.controller.run(f.action)).status).toBe("held");
      expect(f.values.get(key)).toBe(raw);
    }
    expect(f.action.allocate).not.toHaveBeenCalled();
    expect(f.action.create).not.toHaveBeenCalled();
    expect(f.storage.setItem).not.toHaveBeenCalled();
  });

  it("retains full unresolved history and refuses another admission without allocating", async () => {
    const f = fixture();
    const current = Array.from({ length: 64 }, (_, index) => ({
      ...operation(String(index)), sourcePlanId: PlanId.make(`plan-${index}`),
      prepared: {
        ...operation(String(index)).prepared,
        payload: { ...operation(String(index)).prepared.payload,
          sourcePlanRef: { threadId: slot.sourceThreadId, planId: PlanId.make(`plan-${index}`) } },
      },
    }));
    const raw = JSON.stringify({ schemaVersion: 1, current, history: [] });
    f.values.set(planCreationStorageKey(environmentId), raw);
    expect((await f.controller.run(f.action)).status).toBe("held");
    expect(f.values.get(planCreationStorageKey(environmentId))).toBe(raw);
    expect(f.action.allocate).not.toHaveBeenCalled();
    expect(f.action.create).not.toHaveBeenCalled();
  });

  it("coalesces reentry while create is in flight under the source slot", async () => {
    const f = fixture();
    let release!: () => void;
    let reached!: () => void;
    const entered = new Promise<void>((resolve) => { reached = resolve; });
    f.action.create.mockImplementation(async () => {
      reached();
      await new Promise<void>((resolve) => { release = resolve; });
      return AsyncResult.success({ sequence: 1 });
    });
    const first = f.controller.run(f.action);
    await entered;
    const again = f.controller.run({ ...f.action, allocate: async () => operation("must-not-allocate") });
    expect(again).toBe(first);
    release();
    expect((await again).status).toBe("accepted");
    expect(f.action.allocate).toHaveBeenCalledTimes(1);
    expect(f.action.create).toHaveBeenCalledTimes(1);
    expect(f.action.dispatch).toHaveBeenCalledTimes(1);
  });

  it.each([2, 3])("holds a failed stage/terminal write %s and recovers without a fresh identity", async (failedWrite) => {
    const f = fixture();
    let writes = 0;
    f.storage.setItem.mockImplementation(async (key, value) => {
      if (++writes === failedWrite) throw new Error("Synthetic storage failure");
      f.values.set(key, value);
    });
    expect((await f.controller.run(f.action)).status).toBe("held");
    expect(f.action.dispatch).toHaveBeenCalledTimes(failedWrite === 2 ? 0 : 1);
    expect(f.accepted).not.toHaveBeenCalled();
    expect((await f.controller.run(f.action)).status).toBe("accepted");
    expect(f.action.allocate).toHaveBeenCalledTimes(1);
    expect(f.action.dispatch).toHaveBeenCalledTimes(1);
    expect(f.action.create).toHaveBeenCalledTimes(failedWrite === 2 ? 2 : 1);
    expect(f.action.create.mock.calls.every(([input]) => input.commandId === operation().create.commandId)).toBe(true);
    expect(f.accepted).toHaveBeenCalledTimes(1);
  });

  it("purges only the removed environment and fences its late create reply before message issuance", async () => {
    const f = fixture();
    let release!: () => void;
    let reached!: () => void;
    const entered = new Promise<void>((resolve) => { reached = resolve; });
    f.action.create.mockImplementation(async () => {
      reached();
      await new Promise<void>((resolve) => { release = resolve; });
      return AsyncResult.success({ sequence: 1 });
    });
    const sending = f.controller.run(f.action);
    await entered;
    f.values.set("unrelated-environment", "keep");
    await f.controller.clearEnvironment(environmentId);
    release();
    expect((await sending).status).toBe("held");
    expect(f.action.dispatch).not.toHaveBeenCalled();
    expect(f.values.get(planCreationStorageKey(environmentId))).toBeUndefined();
    expect(f.values.get("unrelated-environment")).toBe("keep");
    expect(f.store.getSnapshot()).toHaveLength(0);
  });

  it("reenters and reloads a committed create with a lost reply under the same IDs before defaults", async () => {
    const f = fixture();
    const frozen = operation();
    const durableThreads = new Set<string>();
    f.action.create.mockImplementation(async () => {
      durableThreads.add(frozen.create.threadId);
      return failed(frozen, "create");
    });
    expect((await f.controller.run(f.action)).status).toBe("unknown");
    expect(f.store.getSnapshot()[0]).toMatchObject({ delivery: "uncertain", issued: false, durable: true });
    expect(f.action.dispatch).not.toHaveBeenCalled();
    f.action.create.mockImplementation(async () => {
      durableThreads.add(frozen.create.threadId);
      return AsyncResult.success({ sequence: 1 });
    });
    const reloadStore = createPendingSendStore();
    const reload = createPlanCreationController(createPlanCreationStorage(f.storage), reloadStore);
    const allocate = vi.fn(async () => { throw new Error("changed catalog must not be read"); });
    expect((await reload.run({ ...f.action, allocate })).status).toBe("accepted");
    expect(allocate).not.toHaveBeenCalled();
    expect(f.action.create.mock.calls.map(([input]) => input)).toEqual([frozen.create, frozen.create]);
    expect(f.action.dispatch).toHaveBeenCalledWith(frozen.prepared);
    expect(durableThreads.size).toBe(1);
    const again = await reload.run({ ...f.action, allocate });
    expect(again).toMatchObject({ status: "accepted", operation: { create: frozen.create } });
    expect(f.action.create).toHaveBeenCalledTimes(2);
    expect(f.action.dispatch).toHaveBeenCalledTimes(1);
  });

  it.each(["unknown", "rejected", "accepted"] as const)("uses only matched create evidence: %s", async (status) => {
    const f = fixture();
    const create = vi.fn(async () => failed(operation(), "create", status === "unknown" ? undefined : status));
    const result = await f.controller.run({ ...f.action, create });
    expect(result.status).toBe(status === "accepted" ? "accepted" : status === "rejected" ? "create-rejected" : "unknown");
    expect(f.action.dispatch).toHaveBeenCalledTimes(status === "accepted" ? 1 : 0);
    if (status === "rejected") {
      expect((await f.journal.read(slot))?.phase).toBe("create-rejected");
      const next = operation("new-edited-action");
      await f.controller.run({ ...f.action, allocate: async () => next });
      expect(f.action.create).toHaveBeenCalledWith(next.create);
      expect(JSON.parse(f.values.get(planCreationStorageKey(environmentId))!).history[0].phase).toBe("create-rejected");
    }
  });

  it("keeps accepted create and restores only the message on matched rejection, including reload", async () => {
    const f = fixture();
    const dispatch = vi.fn(async () => failed(operation(), "message", "rejected"));
    expect((await f.controller.run({ ...f.action, dispatch })).status).toBe("message-rejected");
    expect(f.messageRejected).toHaveBeenCalledTimes(1);
    expect(canSendAfterInitialMessageRejection(f.store.getSnapshot(), environmentId, operation().create.threadId)).toBe(true);
    const reload = createPlanCreationController(createPlanCreationStorage(f.storage), createPendingSendStore());
    await reload.run({ ...f.action, dispatch });
    expect(f.action.create).toHaveBeenCalledTimes(1);
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(f.messageRejected).toHaveBeenCalledTimes(2);
  });

  it("replays only the frozen message after reload and preserves late acceptance through failures", async () => {
    const f = fixture();
    const dispatch = vi.fn(async () => failed(operation(), "message"));
    expect((await f.controller.run({ ...f.action, dispatch })).status).toBe("unknown");
    const reloadStore = createPendingSendStore();
    const reload = createPlanCreationController(createPlanCreationStorage(f.storage), reloadStore);
    const replay = vi.fn(async () => {
      reloadStore.reconcile(environmentId, operation().create.threadId, projection);
      return failed(operation(), "message", "rejected");
    });
    expect((await reload.run({ ...f.action, dispatch: replay })).status).toBe("accepted");
    expect(f.action.create).toHaveBeenCalledTimes(1);
    expect(f.action.allocate).toHaveBeenCalledTimes(1);
    expect(replay).toHaveBeenCalledWith(operation().prepared);
    expect((await f.journal.read(slot))?.phase).toBe("accepted");
    expect(f.accepted).toHaveBeenCalledTimes(1);
    expect(f.messageRejected).not.toHaveBeenCalled();
  });

  it("keeps the same message receipt identity on existing pending Retry and notifies acceptance once", async () => {
    const f = fixture();
    const dispatch = vi.fn(async () => failed(operation(), "message"));
    await f.controller.run({ ...f.action, dispatch });
    const retry = f.store.getSnapshot()[0]!.retry!;
    f.store.reconcile(environmentId, operation().create.threadId, projection);
    await f.controller.run(f.action);
    await retry();
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(f.accepted).toHaveBeenCalledTimes(1);
    expect((await f.journal.read(slot))?.phase).toBe("accepted");
  });
});
