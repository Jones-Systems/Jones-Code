import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import {
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
} from "@t3tools/client-runtime/state/threads";
import {
  ChatAttachmentId,
  CommandId,
  ComposerContextId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2ThreadLaunchError,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult, type Atom } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { QueuedThreadMessage } from "../../state/thread-outbox-model";
import {
  v2Now,
  v2Projection,
} from "../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures";
import {
  serializeComposerMessageForServer,
  uploadedComposerContext,
} from "../../lib/composerContext";
import { legacyQueuedThreadMessageAccepted } from "./legacyOutbox";
import { formatComposerContextReference } from "@t3tools/shared/composerContextReferences";

const state = vi.hoisted(() => ({
  manager: null as unknown as ReturnType<
    typeof import("../../state/thread-outbox-manager").createThreadOutboxManager
  >,
  restartManager: null as unknown as () => void,
  projections: new Map<string, Atom.Writable<EnvironmentThreadState, EnvironmentThreadState>>(),
  retry: vi.fn(),
  delivery: null as { drain: () => void; dispose: () => void } | null,
  remove: vi.fn(async (_message: QueuedThreadMessage) => {}),
  persisted: new Map<string, string>(),
  cleanups: [] as Array<() => void>,
  dispatch: vi.fn(),
  prepare: vi.fn(),
  upload: vi.fn(),
  validate: vi.fn(),
  settings: vi.fn(),
  checkout: vi.fn(),
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
vi.mock("@effect/atom-react", async () => {
  const { appAtomRegistry } = await import("../../state/atom-registry");
  return { useAtomValue: <A>(atom: Atom.Atom<A>) => appAtomRegistry.get(atom) };
});
vi.mock("../../state/session", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    readEnvironmentScope: () => true,
    sessionHasScope: () => true,
    environmentSession: { sessionStateAtom: () => Atom.make(true) },
  };
});
vi.mock("../../state/projects", async () => {
  const { Atom } = await import("effect/reactivity");
  return { environmentProjects: { projectsAtom: Atom.make([]) } };
});
vi.mock("../../state/workspace", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    workspaceConnections: {
      environmentsAtom: Atom.make([{ environmentId: "drain-env", connectionState: "connected" }]),
    },
  };
});
vi.mock("@t3tools/client-runtime/state/runtime", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/state/runtime")>()),
  runAtomCommand: (_registry: unknown, command: (input: unknown) => unknown, input: unknown) =>
    command(input),
}));
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    serverEnvironment: { configValueAtom: Atom.family(() => Atom.make(null)) },
    environmentServerConfigsAtom: Atom.make(new Map()),
  };
});
vi.mock("../../state/threads", async () => {
  const { Atom } = await import("effect/reactivity");
  const { EMPTY_ENVIRONMENT_THREAD_STATE } = await import("@t3tools/client-runtime/state/threads");
  const projections = Atom.family((key: string) => {
    const atom = Atom.make<EnvironmentThreadState>(EMPTY_ENVIRONMENT_THREAD_STATE).pipe(
      Atom.keepAlive,
    );
    state.projections.set(key, atom);
    return atom;
  });
  return {
    threadEnvironment: {
      prepareTurn: state.prepare,
      dispatchPreparedTurn: state.dispatch,
      setRuntimeMode: state.settings,
      setInteractionMode: state.settings,
    },
    environmentThreadShells: { threadShellsAtom: Atom.make([]) },
    environmentThreadDetails: {
      stateAtom: (ref: { environmentId: string; threadId: string }) =>
        projections(`${ref.environmentId}:${ref.threadId}`),
    },
  };
});
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: <A>(command: A) => command }));
vi.mock("../../state/git", () => ({
  gitEnvironment: { preparePullRequestThread: state.checkout },
}));
vi.mock("../../lib/modelOptions", () => ({ isModelSelectionUnavailable: state.validate }));
vi.mock("../../lib/attachmentUpload", () => ({ prepareTurnAttachments: state.upload }));
vi.mock("../../lib/uuid", () => ({
  randomHex: () => {
    throw new Error("Replay allocated a branch");
  },
}));
vi.mock("../../state/use-remote-environment-registry", () => ({
  useRemoteConnectionStatus: () => ({
    connectedEnvironments: [{ environmentId: "drain-env", connectionState: "connected" }],
  }),
}));
vi.mock("../../state/thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  const { createThreadOutboxManager } = await import("../../state/thread-outbox-manager");
  const { appAtomRegistry } = await import("../../state/atom-registry");
  const { encodeQueuedThreadMessage, decodeQueuedThreadMessage } =
    await import("../../state/thread-outbox-model");
  const managerAtom = Atom.make<ReturnType<typeof createThreadOutboxManager> | null>(null).pipe(
    Atom.keepAlive,
  );
  const queuedMessagesByThreadKeyAtom = Atom.make((get) => {
    const manager = get(managerAtom);
    if (manager === null) throw new Error("Fixture manager is not initialized");
    return get(manager.queuedMessagesByThreadKeyAtom);
  });
  state.restartManager = () => {
    state.manager = createThreadOutboxManager({
      registry: appAtomRegistry,
      storage: {
        load: async () => ({
          messages: Array.from(state.persisted.values(), (json) =>
            decodeQueuedThreadMessage(JSON.parse(json)),
          ),
          errors: [],
        }),
        write: async (message) => {
          state.persisted.set(
            message.messageId,
            JSON.stringify(encodeQueuedThreadMessage(message)),
          );
        },
        remove: async (message) => {
          await state.remove(message);
          state.persisted.delete(message.messageId);
        },
      },
    });
    appAtomRegistry.set(managerAtom, state.manager);
  };
  state.restartManager();
  return {
    threadOutboxManager: {
      queuedMessagesByThreadKeyAtom,
      load: () => state.manager.load(),
    },
    threadOutboxRevision: (id: MessageId) => state.manager.revisionOf(id),
    confirmThreadOutboxMessageQueued: (message: QueuedThreadMessage) =>
      state.manager.confirmQueued(message),
    updateThreadOutboxMessage: (message: QueuedThreadMessage, revision?: number) =>
      state.manager.update(message, revision),
  };
});
vi.mock("../../state/thread-outbox-removal", () => ({
  removeThreadOutboxMessage: async (
    message: QueuedThreadMessage,
    revision?: number,
    canRemove?: () => boolean,
  ) => (await state.manager.remove(message, revision, canRemove)) !== null,
}));
vi.mock("../../state/use-thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  const { appAtomRegistry } = await import("../../state/atom-registry");
  await import("../../state/thread-outbox");
  return {
    editingQueuedMessageIdsAtom: Atom.make({}).pipe(Atom.keepAlive),
    dispatchingQueuedMessageIdAtom: Atom.make<MessageId | null>(null).pipe(Atom.keepAlive),
    useThreadOutboxMessages: () => appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom),
    threadOutboxShellStatusesAtom: Atom.make(new Map([["drain-env", "empty"]])),
  };
});
vi.mock("../../state/use-composer-drafts", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    composerDraftsAtom: Atom.make({}),
    removeDeliveredCloudQueuedMessage: async () => {},
    appendComposerDraftAttachments: vi.fn(),
    flushComposerDrafts: vi.fn(),
    getComposerDraftSnapshot: vi.fn(() => ({ text: "", attachments: [] })),
    mergeComposerDraftContent: vi.fn(),
    replaceComposerDraftAttachments: vi.fn(),
    undoComposerDraftMerge: vi.fn(),
    updateComposerDraftSettings: vi.fn(),
    waitForComposerDraftsLoaded: vi.fn(),
  };
});

import { appAtomRegistry } from "../../state/atom-registry";
import { acknowledgedThreadMessagesAtom } from "../../state/acknowledged-thread-messages";
import { dispatchingQueuedMessageIdAtom } from "../../state/use-thread-outbox";
import { createThreadOutboxDelivery } from "../../state/threadOutboxDelivery";
import { pendingThreadCreationOutcomesAtom } from "../../state/pending-thread-creation";
import { threadComposerErrorsAtom } from "../../state/thread-composer-error";
import { environmentThreadDetails } from "../../state/threads";
import { withIssuedQueuedThreadTurn } from "./outboxDelivery";

async function drain(messageId: MessageId) {
  const settled = Promise.withResolvers<void>();
  let started = false;
  const unsubscribe = appAtomRegistry.subscribe(dispatchingQueuedMessageIdAtom, (id) => {
    if (id === messageId) started = true;
    else if (started && id === null) settled.resolve();
  });
  try {
    startDelivery();
    await settled.promise;
  } finally {
    unsubscribe();
  }
}
function startDelivery() {
  if (state.delivery) state.delivery.drain();
  else {
    state.delivery = createThreadOutboxDelivery();
    state.cleanups.push(() => {
      state.delivery?.dispose();
      state.delivery = null;
    });
  }
}
const unmount = () => state.cleanups.splice(0).forEach((cleanup) => cleanup());
function legacyFixture(inline = true) {
  const environmentId = EnvironmentId.make("drain-env");
  const message: QueuedThreadMessage = {
    environmentId,
    threadId: ThreadId.make("legacy-thread"),
    commandId: CommandId.make("legacy-command"),
    messageId: MessageId.make("legacy-message"),
    text: `Accepted legacy prompt ${formatComposerContextReference({ kind: "file", contextId: ComposerContextId.make("legacy-context"), label: "Input" })}`,
    createdAt: "2026-06-20T00:00:00.000Z",
    attachments: [
      {
        type: "file",
        id: "local-file",
        name: "input.txt",
        mimeType: "text/plain",
        sizeBytes: 4,
        fileUri: "file:///synthetic-legacy/input.txt",
        uploadedAttachmentId: "uploaded-file",
        uploadEnvironmentId: environmentId,
      },
    ],
    context: {
      version: 1,
      records: [
        {
          version: 1,
          kind: "file",
          contextId: ComposerContextId.make("legacy-context"),
          label: "Input",
          attachmentId: "local-file",
          name: "input.txt",
          mimeType: "text/plain",
          sizeBytes: 4,
        },
      ],
    },
  };
  const attachments = [
    {
      type: "file" as const,
      id: ChatAttachmentId.make("uploaded-file"),
      name: "input.txt",
      mimeType: "text/plain",
      sizeBytes: 4,
    },
  ];
  const wire = serializeComposerMessageForServer(
    message.text,
    uploadedComposerContext(message.context, message.attachments, attachments),
    inline,
  );
  const runId = RunId.make("legacy-run");
  const projection: OrchestrationV2ThreadProjection = {
    ...v2Projection,
    thread: { ...v2Projection.thread, id: message.threadId },
    messages: [
      {
        id: message.messageId,
        threadId: message.threadId,
        role: "user",
        runId,
        nodeId: null,
        createdBy: "user",
        creationSource: "mobile",
        ...wire,
        attachments,
        streaming: false,
        createdAt: v2Now,
        updatedAt: v2Now,
      },
    ],
    runs: [
      {
        id: runId,
        threadId: message.threadId,
        ordinal: 1,
        userMessageId: message.messageId,
        providerInstanceId: v2Projection.thread.providerInstanceId,
        modelSelection: v2Projection.thread.modelSelection,
        providerThreadId: null,
        rootNodeId: null,
        activeAttemptId: null,
        status: "completed",
        requestedAt: v2Now,
        startedAt: v2Now,
        completedAt: v2Now,
        checkpointId: null,
        contextHandoffId: null,
      },
    ],
  };
  return { message, projection };
}
const liveProjection = (projection: OrchestrationV2ThreadProjection): EnvironmentThreadState => ({
  ...EMPTY_ENVIRONMENT_THREAD_STATE,
  status: "live",
  data: Option.some(projection),
});
function publishProjection(message: QueuedThreadMessage, value: EnvironmentThreadState) {
  environmentThreadDetails.stateAtom({
    environmentId: message.environmentId,
    threadId: message.threadId,
  });
  appAtomRegistry.set(
    state.projections.get(`${message.environmentId}:${message.threadId}`)!,
    value,
  );
}
afterEach(() => {
  unmount();
  state.persisted.clear();
  vi.resetAllMocks();
  appAtomRegistry.set(state.manager.queuedMessagesByThreadKeyAtom, {});
  appAtomRegistry.set(dispatchingQueuedMessageIdAtom, null);
  appAtomRegistry.set(pendingThreadCreationOutcomesAtom, {});
  appAtomRegistry.set(threadComposerErrorsAtom, {});
  appAtomRegistry.set(acknowledgedThreadMessagesAtom, []);
  for (const atom of state.projections.values())
    appAtomRegistry.set(atom, EMPTY_ENVIRONMENT_THREAD_STATE);
});

describe("mobile outbox drain replay ordering", () => {
  it.each([true, false])(
    "reconciles an accepted persisted legacy head after restart and lets the next upgraded message progress (inline context: %s)",
    async (inline) => {
      const { message, projection } = legacyFixture(inline);
      await state.manager.enqueue(message);
      const next: QueuedThreadMessage = {
        ...message,
        commandId: CommandId.make("next-command"),
        messageId: MessageId.make("next-message"),
        text: "Next queued prompt",
        attachments: [],
        context: undefined,
        createdAt: "2026-06-20T00:00:01.000Z",
        issuance: { state: "unissued" },
      };
      const prepared = decodePreparedThreadTurn({
        method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        payload: {
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "mobile",
          commandId: next.commandId,
          threadId: next.threadId,
          messageId: next.messageId,
          text: next.text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
        },
      });
      await state.manager.enqueue(withIssuedQueuedThreadTurn(next, prepared));
      appAtomRegistry.set(state.manager.queuedMessagesByThreadKeyAtom, {});
      state.restartManager();
      expect(await state.manager.load()).toBe(true);
      const reloaded = Object.values(
        appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom),
      ).flat();
      expect(reloaded).toHaveLength(2);
      expect(reloaded[0]?.issuance).toBeUndefined();
      const reloadedNext = reloaded[1];
      if (reloadedNext?.issuance?.state !== "issued")
        throw new Error("The reloaded next envelope must remain issued");
      const reloadedPrepared = reloadedNext.issuance.prepared;
      expect(reloadedNext).toMatchObject({
        environmentId: next.environmentId,
        threadId: next.threadId,
        commandId: next.commandId,
        messageId: next.messageId,
      });
      expect(reloadedPrepared).toEqual(prepared);
      const nextPersisted = state.persisted.get(next.messageId);
      expect(nextPersisted).toBeDefined();
      const removing = Promise.withResolvers<QueuedThreadMessage>();
      const releaseRemoval = Promise.withResolvers<void>();
      state.remove.mockImplementationOnce(async (row) => {
        removing.resolve(row);
        await releaseRemoval.promise;
      });
      try {
        publishProjection(message, liveProjection(projection));
        const legacyDrain = drain(message.messageId);
        expect(await removing.promise).toMatchObject({
          environmentId: message.environmentId,
          threadId: message.threadId,
          commandId: message.commandId,
          messageId: message.messageId,
        });
        expect(appAtomRegistry.get(dispatchingQueuedMessageIdAtom)).toBe(message.messageId);
        // This fixture owner goes away before storage releases the legacy slot.
        unmount();
        releaseRemoval.resolve();
        await legacyDrain;
        expect(state.dispatch).not.toHaveBeenCalled();
        expect(state.persisted.has(message.messageId)).toBe(false);
        expect(
          Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom))
            .flat()
            .map((queued) => queued.messageId),
        ).toEqual([next.messageId]);
        expect(state.persisted.get(next.messageId)).toBe(nextPersisted);
        state.dispatch.mockResolvedValueOnce(AsyncResult.success(undefined));
        await drain(next.messageId);
        const expectedRequests = [{ environmentId: next.environmentId, input: prepared }];
        expect(state.dispatch.mock.calls.map(([request]) => request)).toEqual(expectedRequests);
        expect(state.dispatch.mock.calls[0]?.[0].input).toBe(reloadedPrepared);
        expect(state.prepare).not.toHaveBeenCalled();
        expect(state.upload).not.toHaveBeenCalled();
        expect(state.validate).not.toHaveBeenCalled();
        expect(state.settings).not.toHaveBeenCalled();
        expect(state.checkout).not.toHaveBeenCalled();
        expect(state.persisted.size).toBe(0);
        expect(
          Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom)).flat(),
        ).toEqual([]);
        unmount();
        state.restartManager();
        expect(await state.manager.load()).toBe(true);
        expect(state.persisted.size).toBe(0);
        expect(
          Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom)).flat(),
        ).toEqual([]);
        startDelivery();
        expect(state.dispatch.mock.calls.map(([request]) => request)).toEqual(expectedRequests);
        expect(appAtomRegistry.get(dispatchingQueuedMessageIdAtom)).toBeNull();
      } finally {
        releaseRemoval.resolve();
        unmount();
      }
    },
  );

  it("keeps a legacy head unknown until its exact live projection arrives and wakes reconciliation", async () => {
    const { message, projection } = legacyFixture();
    await state.manager.enqueue(message);
    startDelivery();
    await state.manager.serialize(async () => {});
    expect(state.dispatch).not.toHaveBeenCalled();
    expect(state.persisted.has(message.messageId)).toBe(true);
    expect(
      appAtomRegistry.get(threadComposerErrorsAtom)[`${message.environmentId}:${message.threadId}`]
        ?.messageId,
    ).toBe(message.messageId);
    state.retry.mockClear();
    const removed = Promise.withResolvers<void>();
    const release = appAtomRegistry.subscribe(dispatchingQueuedMessageIdAtom, (id) => {
      if (id === null && !state.persisted.has(message.messageId)) removed.resolve();
    });
    publishProjection(message, liveProjection(projection));
    await removed.promise;
    release();
    unmount();
    expect(state.persisted.size).toBe(0);
    expect(state.dispatch).not.toHaveBeenCalled();
    expect(appAtomRegistry.get(threadComposerErrorsAtom)).toEqual({});
  });

  it("retains a newer legacy edit that wins while accepted cleanup is removing its old snapshot", async () => {
    const { message, projection } = legacyFixture();
    await state.manager.enqueue(message);
    publishProjection(message, liveProjection(projection));
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    state.remove.mockImplementationOnce(async () => {
      started.resolve();
      await release.promise;
    });
    const draining = drain(message.messageId);
    let saving: Promise<void> = Promise.resolve();
    try {
      await started.promise;
      const edited = { ...message, text: "A newer saved editor revision" };
      saving = state.manager.enqueue(edited);
      release.resolve();
      await draining;
      await saving;
      expect(
        Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom)).flat(),
      ).toEqual([edited]);
      expect(JSON.parse(state.persisted.get(message.messageId)!).text).toBe(edited.text);
      expect(state.dispatch).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await draining;
      await saving;
    }
  });

  it("requires exact readable environment, thread, message, content, uploads and run evidence for a legacy row", () => {
    const { message, projection } = legacyFixture();
    const live = liveProjection(projection);
    expect(legacyQueuedThreadMessageAccepted(message, message.environmentId, live)).toBe(true);
    expect(
      legacyQueuedThreadMessageAccepted(
        { ...message, dispatchMode: "steer" },
        message.environmentId,
        liveProjection({
          ...projection,
          runs: [{ ...projection.runs[0]!, userMessageId: MessageId.make("run-opening-message") }],
        }),
      ),
    ).toBe(true);
    const cases = [
      { reason: "missing projection", state: EMPTY_ENVIRONMENT_THREAD_STATE },
      { reason: "foreign environment", environmentId: EnvironmentId.make("foreign-env") },
      {
        reason: "foreign thread",
        state: liveProjection({
          ...projection,
          thread: { ...projection.thread, id: ThreadId.make("other-thread") },
        }),
      },
      {
        reason: "foreign message",
        state: liveProjection({
          ...projection,
          messages: [{ ...projection.messages[0]!, id: MessageId.make("other-message") }],
        }),
      },
      {
        reason: "unlinked run",
        state: liveProjection({
          ...projection,
          runs: [{ ...projection.runs[0]!, id: RunId.make("other-run") }],
        }),
      },
      { reason: "edited content", message: { ...message, text: "A newer saved edit" } },
      {
        reason: "missing context",
        state: liveProjection({
          ...projection,
          messages: [{ ...projection.messages[0]!, context: undefined }],
        }),
      },
      {
        reason: "foreign uploads",
        message: {
          ...message,
          attachments: message.attachments.map((attachment) => ({
            ...attachment,
            uploadEnvironmentId: EnvironmentId.make("other-env"),
          })),
        },
      },
      {
        reason: "mismatched upload identity",
        message: {
          ...message,
          attachments: message.attachments.map((attachment) => ({
            ...attachment,
            uploadedAttachmentId: "other-upload",
          })),
        },
      },
      {
        reason: "missing upload identity",
        message: {
          ...message,
          attachments: message.attachments.map((attachment) => ({
            ...attachment,
            uploadedAttachmentId: undefined,
          })),
        },
      },
      {
        reason: "unreadable projection",
        state: { ...live, error: Option.some("Projection unavailable") },
      },
      { reason: "cached projection", state: { ...live, status: "cached" as const } },
    ];
    for (const candidate of cases)
      expect(
        legacyQueuedThreadMessageAccepted(
          candidate.message ?? message,
          candidate.environmentId ?? message.environmentId,
          candidate.state ?? live,
        ),
        candidate.reason,
      ).toBe(false);
  });

  it("retries after restart with no project, catalog, shell, uploads or capabilities and keeps the exact launch", async () => {
    const original: QueuedThreadMessage = {
      environmentId: EnvironmentId.make("drain-env"),
      threadId: ThreadId.make("drain-thread"),
      commandId: CommandId.make("drain-command"),
      messageId: MessageId.make("drain-message"),
      text: "Keep captured context",
      attachments: [],
      createdAt: "2026-11-02T12:00:00.000Z",
      issuance: { state: "unissued" },
      modelSelection: {
        instanceId: ProviderInstanceId.make("removed-provider"),
        model: "removed-model",
        options: [{ id: "reasoningEffort", value: "max" }],
      },
      creation: {
        projectId: ProjectId.make("missing-project"),
        projectCwd: "/missing-project",
        workspaceMode: "worktree",
        branch: null,
        worktreePath: null,
        pullRequestReference: "https://github.com/synthetic/project/pull/1",
      },
    };
    const prepared = decodePreparedThreadTurn({
      method: ORCHESTRATION_V2_WS_METHODS.launchThread,
      payload: {
        commandId: original.commandId,
        threadId: original.threadId,
        projectId: original.creation!.projectId,
        modelSelection: original.modelSelection,
        title: "Frozen",
        generateTitle: true,
        creationSource: "mobile",
        runtimeMode: "approval-required",
        interactionMode: "plan",
        workspaceStrategy: {
          type: "worktree",
          branch: "original-random-branch",
          baseRef: "original-base",
          startFromOrigin: true,
        },
        initialMessage: { messageId: original.messageId, text: original.text, attachments: [] },
      },
    });
    for (const forbidden of [
      state.prepare,
      state.upload,
      state.validate,
      state.settings,
      state.checkout,
    ]) {
      forbidden.mockImplementation(() => {
        throw new Error("Fresh validation/allocation reached during replay");
      });
    }
    await state.manager.enqueue(withIssuedQueuedThreadTurn(original, prepared));
    state.dispatch.mockResolvedValueOnce(
      AsyncResult.failure(
        Cause.fail(
          new OrchestrationV2ThreadLaunchError({
            commandId: original.commandId,
            projectId: original.creation!.projectId,
            message: "Committed response lost",
          }),
        ),
      ),
    );
    await drain(original.messageId);
    expect(state.dispatch).toHaveBeenCalledOnce();
    expect(state.persisted.size).toBe(1);
    const issuedBeforeRestart = Object.values(
      appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom),
    ).flat()[0];
    expect(issuedBeforeRestart?.issuance).toMatchObject({
      state: "issued",
      evidence: { creation: { status: "unknown" }, initialMessage: { status: "unknown" } },
    });
    unmount();
    // Restart the manager as well as the hook; cached load/revision state cannot cross a process restart.
    const previousManager = state.manager;
    appAtomRegistry.set(state.manager.queuedMessagesByThreadKeyAtom, {});
    state.restartManager();
    expect(state.manager).not.toBe(previousManager);
    expect(await state.manager.load()).toBe(true);
    const recoveredMessages = Object.values(
      appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom),
    ).flat();
    expect(recoveredMessages).toHaveLength(1);
    const recovered = recoveredMessages[0]!;
    expect(recovered).not.toBe(issuedBeforeRestart);
    if (recovered.issuance?.state !== "issued")
      throw new Error("Lost issued request during restart");
    expect(recovered.issuance.prepared).toEqual(prepared);
    state.dispatch.mockResolvedValueOnce(AsyncResult.success({ threadId: original.threadId }));
    await drain(original.messageId);
    expect(state.dispatch.mock.calls.map(([request]) => request)).toEqual([
      { environmentId: original.environmentId, input: prepared },
      { environmentId: original.environmentId, input: prepared },
    ]);
    expect(state.dispatch.mock.calls[0]?.[0].input).toBe(prepared);
    expect(state.dispatch.mock.calls[1]?.[0].input).toBe(recovered.issuance.prepared);
    for (const forbidden of [
      state.prepare,
      state.upload,
      state.validate,
      state.settings,
      state.checkout,
    ])
      expect(forbidden).not.toHaveBeenCalled();
    expect(
      Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom)).flat(),
    ).toEqual([]);
    expect(state.persisted.size).toBe(0);
  });
});
