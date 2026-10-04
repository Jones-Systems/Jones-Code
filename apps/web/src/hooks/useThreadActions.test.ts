import {
  CommandId,
  EnvironmentId,
  EventId,
  ProjectId,
  ThreadId,
  type OrchestrationV2ThreadDeletionCleanupObservation,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  navigateAfterThreadDeletion,
  requestThreadUnpinConfirmation,
  ThreadArchiveBlockedError,
  useThreadActions,
  createThreadDeletionController,
  type ThreadDeletionOperation,
} from "./useThreadActions";
import { toastManager } from "../components/ui/toast";
import { threadEnvironment } from "../state/threads";
import { terminalEnvironment } from "../state/terminal";
import { vcsEnvironment } from "../state/vcs";

const deletion = vi.hoisted(() => ({
  deleteThread: vi.fn(),
  observeCleanup: vi.fn(),
  stopSession: vi.fn(),
  closeTerminal: vi.fn(),
  removeWorktree: vi.fn(),
  refreshStatus: vi.fn(),
  clearDraft: vi.fn(),
  clearProjectDraft: vi.fn(),
  releaseUploads: vi.fn(),
  clearTerminalUi: vi.fn(),
  refreshArchived: vi.fn(),
  confirm: vi.fn(),
  navigate: vi.fn(),
  present: true,
  shared: false,
  scratch: false,
  runtime: {} as object | null,
}));
const deletionRouter = vi.hoisted(() => ({
  state: {
    matches: [{ params: { environmentId: "delete-env", threadId: "delete-thread" } }],
  },
}));
vi.mock("react", async (original) => ({
  ...(await original<typeof import("react")>()),
  useCallback: (callback: unknown) => callback,
  useMemo: (create: () => unknown) => create(),
  useRef: (value: unknown) => ({ current: value }),
  useEffect: () => undefined,
}));
vi.mock("@tanstack/react-router", () => ({
  useRouter: () => ({ ...deletionRouter, navigate: deletion.navigate }),
}));
vi.mock("./useSettings", () => ({
  useClientSettings: (selector: (settings: object) => unknown) =>
    selector({
      sidebarThreadSortOrder: "updated_at",
      confirmThreadDelete: false,
      confirmThreadUnpin: false,
    }),
}));
vi.mock("./useHandleNewThread", () => ({ useNewThreadHandler: () => vi.fn() }));
vi.mock("../composerDraftStore", () => ({
  useComposerDraftStore: (selector: (state: object) => unknown) =>
    selector({
      clearDraftThread: deletion.clearDraft,
      clearProjectDraftThreadById: deletion.clearProjectDraft,
    }),
}));
vi.mock("../terminalUiStateStore", () => ({
  useTerminalUiStateStore: (selector: (state: object) => unknown) =>
    selector({ clearTerminalUiState: deletion.clearTerminalUi }),
}));
vi.mock("../uiStateStore", () => ({ useUiStateStore: () => vi.fn() }));
vi.mock("../lib/archivedThreadsState", () => ({
  refreshArchivedThreadsForEnvironment: () => deletion.refreshArchived(),
}));
vi.mock("../lib/composerDraftUploads", () => ({
  releaseComposerDraftUploads: (target: unknown) => deletion.releaseUploads(target),
}));
vi.mock("../localApi", () => ({
  readLocalApi: () => ({ dialogs: { confirm: deletion.confirm } }),
}));
vi.mock("../rpc/atomRegistry", () => ({
  appAtomRegistry: {
    get: () =>
      new Map([["delete-env", { scratchWorkspaceRoot: deletion.scratch ? "/project" : null }]]),
  },
}));
vi.mock("../state/entities", async (original) => ({
  ...(await original<typeof import("../state/entities")>()),
  readProject: () => ({ workspaceRoot: "/project" }),
  readEnvironmentThreadRefs: () => [
    { environmentId: "delete-env", threadId: "delete-thread" },
    ...(deletion.shared ? [{ environmentId: "delete-env", threadId: "other-thread" }] : []),
  ],
  readThreadShell: (ref: { threadId: string }) =>
    deletion.present
      ? {
          id: ref.threadId,
          environmentId: "delete-env",
          projectId: "delete-project",
          title: "Delete thread",
          runtime: deletion.runtime,
          branch: null,
          worktreePath: "/project/worktrees/exact-path",
        }
      : null,
}));
vi.mock("../state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => deletion.observeCleanup,
}));
vi.mock("../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => {
    switch (command) {
      case threadEnvironment.delete:
        return deletion.deleteThread;
      case threadEnvironment.stopSession:
        return deletion.stopSession;
      case terminalEnvironment.close:
        return deletion.closeTerminal;
      case vcsEnvironment.removeWorktree:
        return deletion.removeWorktree;
      case vcsEnvironment.refreshStatus:
        return deletion.refreshStatus;
      default:
        return vi.fn();
    }
  },
}));

describe("navigateAfterThreadDeletion", () => {
  afterEach(() => vi.restoreAllMocks());

  it("reports a rejected navigation without failing the completed deletion", async () => {
    const addToast = vi.spyOn(toastManager, "add").mockReturnValue("navigation-error");

    await expect(
      navigateAfterThreadDeletion(() => Promise.reject(new Error("route unavailable"))),
    ).resolves.toBeUndefined();

    expect(addToast).toHaveBeenCalledOnce();
    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({
        title: "Thread deleted, but navigation failed",
        description: "route unavailable",
      }),
    );
  });

  it("does not report an error after successful navigation", async () => {
    const addToast = vi.spyOn(toastManager, "add");

    await navigateAfterThreadDeletion(() => Promise.resolve());

    expect(addToast).not.toHaveBeenCalled();
  });
});

describe("ThreadArchiveBlockedError", () => {
  it("keeps the blocked thread context with the fixed message", () => {
    const error = new ThreadArchiveBlockedError({
      environmentId: EnvironmentId.make("environment-1"),
      threadId: ThreadId.make("thread-1"),
    });

    expect(error).toMatchObject({
      environmentId: "environment-1",
      threadId: "thread-1",
    });
    expect(error.message).toBe("Cannot archive while the provider is active.");
  });
});

describe("requestThreadUnpinConfirmation", () => {
  it("skips the dialog when confirmation is disabled", async () => {
    let callCount = 0;
    const result = await requestThreadUnpinConfirmation({
      enabled: false,
      title: "Pinned thread",
      confirm: async () => {
        callCount += 1;
        return false;
      },
    });

    expect(result).toMatchObject({ _tag: "Success", value: true });
    expect(callCount).toBe(0);
  });

  it("degrades gracefully when dialogs are unavailable", async () => {
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Pinned thread",
      confirm: null,
    });

    expect(result).toMatchObject({ _tag: "Success", value: true });
  });

  it("uses the thread title and returns the user's decision", async () => {
    let message = "";
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Release prep",
      confirm: async (nextMessage) => {
        message = nextMessage;
        return false;
      },
    });

    expect(message).toBe(
      'Unpin thread "Release prep"?\nThis will move the thread out of your pinned section.',
    );
    expect(result).toMatchObject({ _tag: "Success", value: false });
  });

  it("keeps dialog failures observable", async () => {
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Pinned thread",
      confirm: () => Promise.reject(new Error("dialog unavailable")),
    });

    expect(result._tag).toBe("Failure");
  });
});

describe("authoritative thread deletion", () => {
  const target = {
    environmentId: EnvironmentId.make("delete-env"),
    threadId: ThreadId.make("delete-thread"),
  };

  beforeEach(() => {
    for (const value of Object.values(deletion)) {
      if (vi.isMockFunction(value)) {
        value.mockReset().mockResolvedValue({ _tag: "Success", value: undefined });
      }
    }
    const storage = new Map<string, string>();
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => {
          storage.set(key, value);
        },
        key: (index: number) => [...storage.keys()][index] ?? null,
        get length() {
          return storage.size;
        },
      },
    });
    deletion.deleteThread.mockResolvedValue({ _tag: "Success", value: { sequence: 7 } });
    deletion.observeCleanup.mockImplementation(
      async (input: { input: { threadId: string; commandId: string } }) => {
        const request = deletion.deleteThread.mock.calls.at(-1)?.[0].input;
        const result = await deletion.deleteThread.mock.results.at(-1)?.value;
        return {
          _tag: "Success",
          value: {
            threadId: input.input.threadId,
            commandId: input.input.commandId,
            receipt:
              result?._tag === "Success"
                ? {
                    ...input.input,
                    commandType: "thread.delete",
                    status: "accepted",
                    acceptedAt: DateTime.makeUnsafe(0),
                    resultSequence: result.value.sequence,
                    error: null,
                  }
                : null,
            deletion:
              result?._tag === "Success"
                ? {
                    eventId: EventId.make("deletion-event"),
                    sequence: 1,
                    resultSequence: result.value.sequence,
                  }
                : null,
            worktree:
              request?.worktreeRemoval === undefined
                ? null
                : {
                    projectId: request.worktreeRemoval.projectId,
                    path: request.worktreeRemoval.path,
                    branch: request.worktreeRemoval.branch,
                  },
            state: "unknown",
            removalOutcome: null,
            currentLease: "unavailable",
            reason: "cleanup readback unavailable",
          },
        };
      },
    );
    deletion.confirm.mockResolvedValue(true);
    deletion.navigate.mockResolvedValue(undefined);
    deletion.present = true;
    deletion.shared = false;
    deletion.scratch = false;
    deletion.runtime = {};
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("rejects deletion without independently stopping or deleting terminal history", async () => {
    const failure = { _tag: "Failure", cause: Cause.fail(new Error("delete rejected")) };
    deletion.deleteThread.mockResolvedValue(failure);

    const result = await useThreadActions().deleteThread(target);

    expect(result).toMatchObject({ _tag: "Failure" });
    expect(deletion.deleteThread).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: expect.objectContaining({ threadId: target.threadId, commandId: expect.any(String) }),
    });
    expect(deletion.stopSession).not.toHaveBeenCalled();
    expect(deletion.closeTerminal).not.toHaveBeenCalled();
    expect(deletion.clearDraft).not.toHaveBeenCalled();
    expect(deletion.releaseUploads).not.toHaveBeenCalled();
    expect(deletion.navigate).not.toHaveBeenCalled();
    expect(deletion.removeWorktree).not.toHaveBeenCalled();
  });

  it("waits for logical acceptance before clearing local state and navigating", async () => {
    let accept!: (result: { _tag: "Success"; value: { sequence: number } }) => void;
    const accepted = { _tag: "Success" as const, value: { sequence: 7 } };
    let dispatchStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      dispatchStarted = resolve;
    });
    deletion.deleteThread.mockImplementation(() => {
      dispatchStarted();
      return new Promise((resolve) => {
        accept = resolve;
      });
    });
    deletion.confirm.mockResolvedValue(false);
    const pending = useThreadActions().deleteThread(target);
    await started;
    expect(deletion.deleteThread).toHaveBeenCalledOnce();
    expect(deletion.clearDraft).not.toHaveBeenCalled();
    expect(deletion.clearTerminalUi).not.toHaveBeenCalled();
    expect(deletion.navigate).not.toHaveBeenCalled();
    expect(deletion.stopSession).not.toHaveBeenCalled();
    expect(deletion.closeTerminal).not.toHaveBeenCalled();

    accept(accepted);
    expect(await pending).toMatchObject(accepted);
    expect(deletion.releaseUploads).toHaveBeenCalledExactlyOnceWith(target);
    expect(deletion.clearDraft).toHaveBeenCalledExactlyOnceWith(target);
    expect(deletion.clearProjectDraft).toHaveBeenCalledOnce();
    expect(deletion.clearTerminalUi).toHaveBeenCalledExactlyOnceWith(target);
    expect(deletion.navigate).toHaveBeenCalledWith({ to: "/", replace: true });
  });

  it.each(["active runtime", "no runtime in the shell"])(
    "retains the worktree when cleanup readback is unavailable with %s",
    async (runtime) => {
      if (runtime === "no runtime in the shell") deletion.runtime = null;
      const addToast = vi.spyOn(toastManager, "add").mockReturnValue("cleanup-held");
      const accepted = { _tag: "Success", value: { sequence: 9 } };
      deletion.deleteThread.mockResolvedValue(accepted);

      expect(await useThreadActions().deleteThread(target)).toMatchObject(accepted);

      expect(deletion.confirm).toHaveBeenCalledWith(expect.stringContaining("exact-path"), {
        variant: "destructive",
      });
      expect(deletion.removeWorktree).not.toHaveBeenCalled();
      expect(deletion.refreshStatus).not.toHaveBeenCalled();
      expect(deletion.navigate).toHaveBeenCalledOnce();
      expect(deletion.clearDraft).toHaveBeenCalledOnce();
      expect(addToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: "Thread deleted; worktree cleanup pending",
          description: expect.stringContaining("cleanup"),
        }),
      );
    },
  );

  it.each(["Scratch", "shared worktree"])("preserves %s exclusions", async (kind) => {
    deletion.scratch = kind === "Scratch";
    deletion.shared = kind === "shared worktree";
    const addToast = vi.spyOn(toastManager, "add");

    expect(await useThreadActions().deleteThread(target)).toMatchObject({ _tag: "Success" });

    expect(deletion.confirm).not.toHaveBeenCalled();
    expect(deletion.removeWorktree).not.toHaveBeenCalled();
    expect(addToast).not.toHaveBeenCalled();
  });

  it("preserves declined worktree consent after accepted thread deletion", async () => {
    deletion.confirm.mockResolvedValue(false);
    const addToast = vi.spyOn(toastManager, "add");

    expect(await useThreadActions().deleteThread(target)).toMatchObject({ _tag: "Success" });

    expect(deletion.removeWorktree).not.toHaveBeenCalled();
    expect(addToast).not.toHaveBeenCalled();
    expect(deletion.clearDraft).toHaveBeenCalledOnce();
  });

  it("reports physical completion only from the correlated complete readback", async () => {
    deletion.observeCleanup.mockImplementation(
      async (input: { input: { threadId: string; commandId: string } }) => ({
        _tag: "Success",
        value: {
          ...input.input,
          receipt: {
            ...input.input,
            commandType: "thread.delete",
            status: "accepted",
            acceptedAt: DateTime.makeUnsafe(0),
            resultSequence: 7,
            error: null,
          },
          deletion: { eventId: EventId.make("physical-deletion"), sequence: 3, resultSequence: 7 },
          worktree: {
            projectId: "delete-project",
            path: "/project/worktrees/exact-path",
            branch: null,
          },
          state: "completed",
          currentLease: "absent",
          removalOutcome: { result: "succeeded", effect: "confirmed" },
          reason: null,
        },
      }),
    );
    const addToast = vi.spyOn(toastManager, "add").mockReturnValue("completed-status");
    expect(await useThreadActions().deleteThread(target)).toMatchObject({
      _tag: "Success",
      value: { sequence: 7 },
    });
    expect(deletion.deleteThread.mock.calls[0]![0].input).toMatchObject({
      threadId: target.threadId,
      commandId: expect.any(String),
      worktreeRemoval: {
        projectId: "delete-project",
        path: "/project/worktrees/exact-path",
        branch: null,
        force: true,
      },
    });
    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Thread worktree deleted" }),
    );
    expect(deletion.removeWorktree).not.toHaveBeenCalled();
  });

  it("keeps logical acceptance and navigation when the cleanup query fails", async () => {
    deletion.observeCleanup.mockResolvedValue({
      _tag: "Failure",
      cause: Cause.fail(new Error("query unavailable")),
    });
    const addToast = vi.spyOn(toastManager, "add").mockReturnValue("unknown-status");
    expect(await useThreadActions().deleteThread(target)).toMatchObject({ _tag: "Success" });
    expect(deletion.navigate).toHaveBeenCalledOnce();
    expect(deletion.clearDraft).toHaveBeenCalledOnce();
    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Deletion cleanup is unconfirmed" }),
    );
    expect(deletion.removeWorktree).not.toHaveBeenCalled();
  });

  it("dispatches archived deletion without independent runtime or terminal cleanup", async () => {
    deletion.present = false;

    expect(await useThreadActions().deleteThread(target)).toMatchObject({ _tag: "Success" });

    expect(deletion.deleteThread).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: expect.objectContaining({ threadId: target.threadId, commandId: expect.any(String) }),
    });
    expect(deletion.stopSession).not.toHaveBeenCalled();
    expect(deletion.closeTerminal).not.toHaveBeenCalled();
    expect(deletion.removeWorktree).not.toHaveBeenCalled();
    expect(deletion.refreshArchived).toHaveBeenCalledOnce();
  });
});

describe("saved deletion command reconciliation", () => {
  const ref = {
    environmentId: EnvironmentId.make("saved-env"),
    threadId: ThreadId.make("saved-thread"),
  };
  const consent = {
    projectId: ProjectId.make("saved-project"),
    path: "/original/worktree",
    branch: "original",
    force: true,
  } as const;
  const original: ThreadDeletionOperation = {
    ...ref,
    commandId: CommandId.make("original-delete"),
    projectId: consent.projectId,
    worktreeRemoval: consent,
  };
  function observation(operation = original): OrchestrationV2ThreadDeletionCleanupObservation {
    return {
      threadId: operation.threadId,
      commandId: operation.commandId,
      receipt: {
        threadId: operation.threadId,
        commandId: operation.commandId,
        commandType: "thread.delete",
        status: "accepted",
        acceptedAt: DateTime.makeUnsafe(0),
        resultSequence: 9,
        error: null,
      },
      deletion: {
        eventId: EventId.make("original-deletion-event"),
        sequence: 7,
        resultSequence: 9,
      },
      worktree: consent,
      state: "pending",
      removalOutcome: null,
      currentLease: "original",
      reason: "prerequisites pending",
    };
  }
  function fixture(saved: ThreadDeletionOperation | null = null) {
    let pointer = saved;
    const ports = {
      read: vi.fn(() => pointer),
      save: vi.fn((operation: ThreadDeletionOperation) => {
        pointer = operation;
      }),
      saveVolatile: vi.fn((operation: ThreadDeletionOperation) => {
        pointer = operation;
      }),
      allocateCommandId: vi.fn(() => original.commandId),
      delete: vi.fn(async (_operation: ThreadDeletionOperation) => ({ sequence: 9 })),
      observe: vi.fn(async (_operation: ThreadDeletionOperation) => observation()),
      onUntracked: vi.fn(),
    };
    return { ports, controller: createThreadDeletionController(ports), pointer: () => pointer };
  }

  it("persists exact command and consent before dispatch and shares simultaneous requests", async () => {
    const f = fixture();
    f.ports.delete.mockImplementation(async (operation) => {
      expect(f.pointer()).toEqual(operation);
      return { sequence: 9 };
    });
    const a = f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent });
    const b = f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent });
    expect(a).toBe(b);
    expect(await a).toEqual({ sequence: 9 });
    expect(f.ports.delete).toHaveBeenCalledExactlyOnceWith(original);
  });

  it("reconciles lost response and remount using only the original command and consent", async () => {
    const f = fixture();
    f.ports.delete.mockRejectedValue(new Error("transport lost"));
    expect(
      await f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent }),
    ).toEqual({ sequence: 9 });
    const remounted = createThreadDeletionController(f.ports);
    expect(
      await remounted.request(ref, {
        projectId: ProjectId.make("replacement"),
        worktreeRemoval: null,
      }),
    ).toEqual({ sequence: 9 });
    expect(f.ports.delete).toHaveBeenCalledOnce();
    expect(f.ports.observe).toHaveBeenCalledTimes(2);
    expect(f.ports.observe).toHaveBeenLastCalledWith(original);
    expect(f.pointer()).toEqual(original);
  });

  it("keeps unknown or not-found observations without redispatch", async () => {
    const f = fixture(original);
    f.ports.observe.mockResolvedValue({
      ...observation(),
      state: "not_found",
      receipt: null,
      deletion: null,
      worktree: null,
    });
    await expect(
      f.controller.request(ref, { projectId: null, worktreeRemoval: null }),
    ).rejects.toThrow("unconfirmed");
    expect(f.ports.delete).not.toHaveBeenCalled();
    expect(f.pointer()).toEqual(original);
  });

  it.each(["command", "path", "receipt sequence"])(
    "rejects mismatched %s without a new deletion",
    async (kind) => {
      const f = fixture(original);
      const received = observation();
      f.ports.observe.mockResolvedValue(
        kind === "command"
          ? { ...received, commandId: CommandId.make("other-command") }
          : kind === "path"
            ? { ...received, worktree: { ...consent, path: "/replacement/path" } }
            : { ...received, deletion: { ...received.deletion!, resultSequence: 8 } },
      );
      await expect(
        f.controller.request(ref, { projectId: null, worktreeRemoval: null }),
      ).rejects.toThrow();
      expect(f.ports.delete).not.toHaveBeenCalled();
      expect(f.pointer()).toEqual(original);
    },
  );

  it("continues logical deletion with the same volatile ID and no physical consent after initial write failure", async () => {
    const f = fixture();
    f.ports.save.mockImplementation(() => {
      throw new Error("storage quota");
    });
    expect(
      await f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent }),
    ).toEqual({ sequence: 9 });
    expect(f.ports.delete).toHaveBeenCalledExactlyOnceWith({ ...original, worktreeRemoval: null });
    expect(f.ports.saveVolatile).toHaveBeenCalledExactlyOnceWith({
      ...original,
      worktreeRemoval: null,
    });
    expect(f.ports.onUntracked).toHaveBeenCalledOnce();
  });

  it("blocks new dispatch when saved-operation reads are uncertain", async () => {
    const f = fixture();
    f.ports.read.mockImplementation(() => {
      throw new Error("storage unavailable");
    });
    await expect(
      f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent }),
    ).rejects.toThrow("storage unavailable");
    expect(f.ports.delete).not.toHaveBeenCalled();
    expect(f.ports.save).not.toHaveBeenCalled();
  });

  it("does not observe a replacement pointer after losing the original dispatch response", async () => {
    const f = fixture();
    f.ports.delete.mockImplementation(async () => {
      f.ports.save({ ...original, commandId: CommandId.make("replacement-command") });
      throw new Error("original transport lost");
    });
    await expect(
      f.controller.request(ref, { projectId: consent.projectId, worktreeRemoval: consent }),
    ).rejects.toThrow("original transport lost");
    expect(f.ports.observe).not.toHaveBeenCalled();
    expect(f.ports.delete).toHaveBeenCalledOnce();
  });

  it("rejects replacement pointers that arrive during original-command observation", async () => {
    const f = fixture(original);
    f.ports.observe.mockImplementation(async () => {
      f.ports.save({ ...original, commandId: CommandId.make("replacement-command") });
      return observation();
    });
    await expect(f.controller.observe(ref)).rejects.toThrow("changed during observation");
    expect(f.ports.delete).not.toHaveBeenCalled();
  });
});
