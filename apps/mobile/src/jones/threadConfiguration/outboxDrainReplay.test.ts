import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import {
  CommandId, EnvironmentId, MessageId, ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2ThreadLaunchError, ProjectId, ProviderInstanceId, ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, type Atom } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { QueuedThreadMessage } from "../../state/thread-outbox-model";

const state = vi.hoisted(() => ({
  manager: null as unknown as ReturnType<typeof import("../../state/thread-outbox-manager").createThreadOutboxManager>,
  persisted: new Map<string, string>(), cleanups: [] as Array<() => void>,
  dispatch: vi.fn(), prepare: vi.fn(), upload: vi.fn(), validate: vi.fn(), settings: vi.fn(), checkout: vi.fn(),
}));
vi.mock("react", () => ({
  useCallback: <A>(callback: A) => callback,
  useEffect: (effect: () => void | (() => void)) => { const cleanup = effect(); if (cleanup) state.cleanups.push(cleanup); },
  useRef: <A>(current: A) => ({ current }), useState: <A>(initial: A) => [initial, vi.fn()],
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
vi.mock("@effect/atom-react", async () => {
  const { appAtomRegistry } = await import("../../state/atom-registry");
  return { useAtomValue: <A>(atom: Atom.Atom<A>) => appAtomRegistry.get(atom) };
});
vi.mock("../../state/session", () => ({ readEnvironmentScope: () => true, useEnvironmentsWithScope: () => new Set(["drain-env"]) }));
vi.mock("../../state/entities", () => ({ useProjects: () => [], useThreadShells: () => [], useServerConfigs: () => new Map() }));
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  return { serverEnvironment: { configValueAtom: Atom.family(() => Atom.make(null)) } };
});
vi.mock("../../state/threads", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    threadEnvironment: { prepareTurn: state.prepare, dispatchPreparedTurn: state.dispatch, setRuntimeMode: state.settings, setInteractionMode: state.settings },
    environmentThreadShells: { threadShellsAtom: Atom.make([]) },
  };
});
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: <A>(command: A) => command }));
vi.mock("../../state/git", () => ({ gitEnvironment: { preparePullRequestThread: state.checkout } }));
vi.mock("../../lib/modelOptions", () => ({ isModelSelectionUnavailable: state.validate }));
vi.mock("../../lib/attachmentUpload", () => ({ prepareTurnAttachments: state.upload }));
vi.mock("../../lib/uuid", () => ({ randomHex: () => { throw new Error("Replay allocated a branch"); } }));
vi.mock("../../state/use-remote-environment-registry", () => ({ useRemoteConnectionStatus: () => ({
  connectedEnvironments: [{ environmentId: "drain-env", connectionState: "connected" }],
}) }));
vi.mock("../../state/thread-outbox", async () => {
  const { createThreadOutboxManager } = await import("../../state/thread-outbox-manager");
  const { appAtomRegistry } = await import("../../state/atom-registry");
  const { encodeQueuedThreadMessage, decodeQueuedThreadMessage } = await import("../../state/thread-outbox-model");
  const manager = state.manager = createThreadOutboxManager({ registry: appAtomRegistry, storage: {
    load: async () => ({ messages: Array.from(state.persisted.values(), (json) => decodeQueuedThreadMessage(JSON.parse(json))), errors: [] }),
    write: async (message) => { state.persisted.set(message.messageId, JSON.stringify(encodeQueuedThreadMessage(message))); },
    remove: async (message) => { state.persisted.delete(message.messageId); },
  } });
  return { threadOutboxManager: manager, threadOutboxRevision: manager.revisionOf,
    confirmThreadOutboxMessageQueued: manager.confirmQueued, updateThreadOutboxMessage: manager.update };
});
vi.mock("../../state/thread-outbox-removal", () => ({
  removeThreadOutboxMessage: async (message: QueuedThreadMessage, revision?: number, canRemove?: () => boolean) =>
    (await state.manager.remove(message, revision, canRemove)) !== null,
}));
vi.mock("../../state/use-thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  const { appAtomRegistry } = await import("../../state/atom-registry");
  await import("../../state/thread-outbox");
  return {
    editingQueuedMessageIdsAtom: Atom.make({}).pipe(Atom.keepAlive),
    dispatchingQueuedMessageIdAtom: Atom.make<MessageId | null>(null).pipe(Atom.keepAlive),
    useThreadOutboxMessages: () => appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom),
    useThreadOutboxShellStatuses: () => new Map([["drain-env", "empty"]]),
  };
});
vi.mock("../../state/use-composer-drafts", async () => {
  const { Atom } = await import("effect/reactivity");
  return { composerDraftsAtom: Atom.make({}), removeDeliveredCloudQueuedMessage: async () => {},
    appendComposerDraftAttachments: vi.fn(), flushComposerDrafts: vi.fn(),
    getComposerDraftSnapshot: vi.fn(() => ({ text: "", attachments: [] })), mergeComposerDraftContent: vi.fn(),
    replaceComposerDraftAttachments: vi.fn(), undoComposerDraftMerge: vi.fn(), updateComposerDraftSettings: vi.fn(), waitForComposerDraftsLoaded: vi.fn() };
});

import { appAtomRegistry } from "../../state/atom-registry";
import { dispatchingQueuedMessageIdAtom } from "../../state/use-thread-outbox";
import { useThreadOutboxDrain } from "../../state/use-thread-outbox-drain";
import { pendingThreadCreationOutcomesAtom } from "../../state/pending-thread-creation";
import { threadComposerErrorsAtom } from "../../state/thread-composer-error";
import { decodeQueuedThreadMessage } from "../../state/thread-outbox-model";
import { withIssuedQueuedThreadTurn } from "./outboxDelivery";

async function drain() {
  const settled = Promise.withResolvers<void>();
  const unsubscribe = appAtomRegistry.subscribe(dispatchingQueuedMessageIdAtom, (id) => { if (id === null) settled.resolve(); });
  try { useThreadOutboxDrain(); await settled.promise; } finally { unsubscribe(); }
}
const unmount = () => state.cleanups.splice(0).forEach((cleanup) => cleanup());
afterEach(() => {
  unmount(); state.persisted.clear(); vi.resetAllMocks();
  appAtomRegistry.set(state.manager.queuedMessagesByThreadKeyAtom, {});
  appAtomRegistry.set(dispatchingQueuedMessageIdAtom, null);
  appAtomRegistry.set(pendingThreadCreationOutcomesAtom, {});
  appAtomRegistry.set(threadComposerErrorsAtom, {});
});

describe("mobile outbox drain replay ordering", () => {
  it("retries after restart with no project, catalog, shell, uploads or capabilities and keeps the exact launch", async () => {
    const original: QueuedThreadMessage = {
      environmentId: EnvironmentId.make("drain-env"), threadId: ThreadId.make("drain-thread"),
      commandId: CommandId.make("drain-command"), messageId: MessageId.make("drain-message"),
      text: "Keep captured context", attachments: [], createdAt: "2026-11-02T12:00:00.000Z", issuance: { state: "unissued" },
      modelSelection: { instanceId: ProviderInstanceId.make("removed-provider"), model: "removed-model", options: [{ id: "reasoningEffort", value: "max" }] },
      creation: { projectId: ProjectId.make("missing-project"), projectCwd: "/missing-project", workspaceMode: "worktree", branch: null, worktreePath: null, pullRequestReference: "https://github.com/synthetic/project/pull/1" },
    };
    const prepared = decodePreparedThreadTurn({ method: ORCHESTRATION_V2_WS_METHODS.launchThread, payload: {
      commandId: original.commandId, threadId: original.threadId, projectId: original.creation!.projectId,
      modelSelection: original.modelSelection, title: "Frozen", generateTitle: true, creationSource: "mobile",
      runtimeMode: "approval-required", interactionMode: "plan",
      workspaceStrategy: { type: "worktree", branch: "original-random-branch", baseRef: "original-base", startFromOrigin: true },
      initialMessage: { messageId: original.messageId, text: original.text, attachments: [] },
    } });
    for (const forbidden of [state.prepare, state.upload, state.validate, state.settings, state.checkout]) {
      forbidden.mockImplementation(() => { throw new Error("Fresh validation/allocation reached during replay"); });
    }
    await state.manager.enqueue(withIssuedQueuedThreadTurn(original, prepared));
    state.dispatch.mockResolvedValueOnce(AsyncResult.failure(Cause.fail(new OrchestrationV2ThreadLaunchError({
      commandId: original.commandId, projectId: original.creation!.projectId, message: "Committed response lost",
    }))));
    await drain();
    expect(state.persisted.size).toBe(1);
    unmount();
    // Discard all in-memory queue objects. This is the actual serialized crash/restart boundary.
    appAtomRegistry.set(state.manager.queuedMessagesByThreadKeyAtom, {});
    const recovered = decodeQueuedThreadMessage(JSON.parse(state.persisted.get(original.messageId)!));
    await state.manager.enqueue(recovered);
    state.dispatch.mockResolvedValueOnce(AsyncResult.success({ threadId: original.threadId }));
    await drain();
    expect(state.dispatch.mock.calls.map(([request]) => request)).toEqual([
      { environmentId: original.environmentId, input: prepared },
      { environmentId: original.environmentId, input: prepared },
    ]);
    for (const forbidden of [state.prepare, state.upload, state.validate, state.settings, state.checkout]) expect(forbidden).not.toHaveBeenCalled();
    expect(Object.values(appAtomRegistry.get(state.manager.queuedMessagesByThreadKeyAtom)).flat()).toEqual([]);
    expect(state.persisted.size).toBe(0);
  });
});
