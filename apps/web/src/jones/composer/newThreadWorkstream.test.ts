import { CommandId, EnvironmentId, MessageId, ProjectId, ThreadId } from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import type { PersonalWorkstreamView } from "@t3tools/client-runtime/jones/workstreams/personal";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { createPendingSendStore } from "../pendingSends/store";
import { assignNewThreadWorkstream, availableComposerWorkstreams } from "./newThreadWorkstream";

const { readState, assign, addToast } = vi.hoisted(() => ({
  readState: vi.fn(),
  assign: vi.fn(),
  addToast: vi.fn(),
}));
vi.mock("../../rpc/atomRegistry", () => ({ appAtomRegistry: { get: readState } }));
vi.mock("../../state/workstreamsPersonal", () => ({
  personalWorkstreamsController: { state: {}, assign },
}));
vi.mock("../../components/ui/toast", () => ({ toastManager: { add: addToast } }));
vi.mock("../workstreams/personalNotifications", () => ({
  personalFailureMessage: () => "Couldn't move this thread. Try again.",
}));

const now = "2026-11-17T12:00:00.000Z";
const active: PersonalWorkstreamView = {
  id: "active",
  name: "Active Workstream",
  lifecycle: "active",
  color: null,
  orderKey: "n",
  createdAt: now,
  updatedAt: now,
};
const thread = { environmentId: EnvironmentId.make("remote"), id: ThreadId.make("new-thread") };

describe("new-thread Workstream assignment", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    readState.mockReturnValue({ workstreams: [active] });
    assign.mockResolvedValue({ ok: true, value: undefined });
  });

  it("offers open Workstreams with no current snooze in their existing order", () => {
    const workstreams: PersonalWorkstreamView[] = [
      active,
      { ...active, id: "paused", lifecycle: "paused" },
      { ...active, id: "completed", lifecycle: "completed" },
      { ...active, id: "abandoned", lifecycle: "abandoned" },
      { ...active, id: "snoozed", snoozedUntil: "2026-11-17T12:00:01.000Z" },
      { ...active, id: "expired", snoozedUntil: now },
      { ...active, id: "cleared", snoozedUntil: null },
    ];
    expect(availableComposerWorkstreams(workstreams, Date.parse(now)).map(({ id }) => id)).toEqual([
      "active",
      "paused",
      "expired",
      "cleared",
    ]);
  });

  it.each(["accepted", "rejected"] as const)(
    "uses the initial message's %s result after thread creation",
    (status) => {
      const store = createPendingSendStore();
      const messageId = MessageId.make("first-message");
      const key = store.admit(thread.environmentId, thread.id, {
        id: messageId,
        role: "user",
        text: "Hello",
        runId: null,
        createdAt: now,
        updatedAt: now,
        streaming: false,
      });
      store.whenAccepted(thread.environmentId, thread.id, messageId, () => {
        void assignNewThreadWorkstream(thread, active.id, vi.fn());
      });
      store.issuing(key);
      expect(assign).not.toHaveBeenCalled();

      const commandId = CommandId.make("launch");
      store.finish(key, {
        kind: "launch",
        commandId,
        projectId: ProjectId.make("project"),
        creation: { status: "accepted", threadId: thread.id },
        initialMessage: { status, commandId: initialMessageCommandId(commandId), messageId },
      });
      store.finish(key, status);

      if (status === "accepted") {
        expect(assign).toHaveBeenCalledExactlyOnceWith(thread, active.id);
      } else {
        expect(assign).not.toHaveBeenCalled();
      }
    },
  );

  it("offers to open the launched thread when assignment is refused", async () => {
    assign.mockResolvedValue({ ok: false, failure: { reason: "changed_elsewhere" } });
    const openThread = vi.fn();

    await assignNewThreadWorkstream(thread, active.id, openThread);

    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({
        title: "Thread started, but Workstream assignment failed",
        description: expect.stringContaining("Move to Workstream"),
        actionProps: expect.objectContaining({ children: "Open thread", onClick: openThread }),
      }),
    );
  });

  it("does not assign if the selected Workstream was snoozed while launching", async () => {
    readState.mockReturnValue({
      workstreams: [{ ...active, snoozedUntil: "2099-01-01T00:00:00.000Z" }],
    });

    await assignNewThreadWorkstream(thread, active.id, vi.fn());

    expect(assign).not.toHaveBeenCalled();
    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({ description: expect.stringContaining("no longer available") }),
    );
  });
});
