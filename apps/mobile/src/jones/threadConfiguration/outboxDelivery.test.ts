import {
  AVAILABLE_CONNECTION_STATE,
  EnvironmentSupervisor,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "@t3tools/client-runtime/connection";
import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import { dispatchPreparedThreadTurn, type PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import type { RpcSession, WsRpcProtocolClient } from "@t3tools/client-runtime/rpc";
import {
  ChatAttachmentId,
  CommandId,
  ComposerContextId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, describe, expect, it, onTestFinished, vi } from "vite-plus/test";

import { createThreadOutboxManager } from "../../state/thread-outbox-manager";
import {
  decodeQueuedThreadMessage,
  encodeQueuedThreadMessage,
  QueuedThreadMessageSchema,
  type QueuedThreadMessage,
} from "../../state/thread-outbox-model";
import { expoThreadOutboxStorage, type ThreadOutboxStorage } from "../../state/thread-outbox-storage";
import {
  acceptedQueuedThreadDelivery,
  canRestoreQueuedThreadMessage,
  deliverQueuedThreadTurn,
  observeQueuedThreadDelivery,
  queuedThreadContentChanged,
  queuedThreadDeliveryEvidence,
  queuedThreadDeliveryStatus,
  queuedThreadProvenTarget,
  withIssuedQueuedThreadTurn,
} from "./outboxDelivery";

const disk = vi.hoisted(() => ({ files: new Map<string, string>(), failWrite: false }));
vi.mock("expo-file-system", () => {
  class Directory {
    create() {}
    list() { return Array.from(disk.files.keys(), (name) => new File(name)); }
  }
  class File {
    readonly name: string;
    readonly parentDirectory = new Directory();
    constructor(...parts: [string] | [Directory, string]) {
      this.name = parts.length === 1 ? parts[0] : parts[1];
    }
    get exists() { return disk.files.has(this.name); }
    create() { disk.files.set(this.name, ""); }
    write(contents: string) {
      if (disk.failWrite) throw new Error("Simulated atomic-write failure");
      disk.files.set(this.name, contents);
    }
    moveSync(destination: File) {
      const contents = disk.files.get(this.name);
      if (contents === undefined) throw new Error("Missing staging file");
      disk.files.set(destination.name, contents);
      disk.files.delete(this.name);
    }
    delete() { disk.files.delete(this.name); }
    async text() {
      const contents = disk.files.get(this.name);
      if (contents === undefined) throw new Error("Missing message file");
      return contents;
    }
  }
  return { Directory, File, Paths: { document: "/synthetic-c3-documents" } };
});
afterEach(() => { disk.files.clear(); disk.failWrite = false; });

function message(launch = true): QueuedThreadMessage {
  const environmentId = EnvironmentId.make("c3-environment");
  return {
    environmentId,
    threadId: ThreadId.make("c3-thread"),
    commandId: CommandId.make("c3-command"),
    messageId: MessageId.make("c3-message"),
    text: "Captured task",
    attachments: [{
      type: "file", id: "draft-input", name: "input.txt", mimeType: "text/plain", sizeBytes: 4,
      fileUri: "file:///synthetic-c3/input.txt", uploadedAttachmentId: "upload-input", uploadEnvironmentId: environmentId,
    }],
    context: { version: 1, records: [{
      version: 1, kind: "file", contextId: ComposerContextId.make("input-context"), label: "Input",
      attachmentId: "draft-input", name: "input.txt", mimeType: "text/plain", sizeBytes: 4,
    }] },
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol", options: [{ id: "reasoningEffort", value: "max" }] },
    modelSelectionExplicit: true, runtimeMode: "full-access", interactionMode: "default", dispatchMode: "queue",
    ...(launch ? { creation: {
      projectId: ProjectId.make("c3-project"), projectCwd: "/synthetic-c3/repo",
      workspaceMode: "worktree" as const, branch: null, worktreePath: null,
    } } : {}),
    issuance: { state: "unissued" }, createdAt: "2026-11-02T12:00:00.000Z",
  };
}

function preparedFor(queued: QueuedThreadMessage): PreparedThreadTurn {
  const attachments = [{
    type: "file", id: ChatAttachmentId.make("upload-input"), name: "input.txt", mimeType: "text/plain", sizeBytes: 4,
  }];
  const context = { ...queued.context, records: queued.context!.records.map((record) => ({ ...record, attachmentId: "upload-input" })) };
  return decodePreparedThreadTurn(queued.creation ? {
    method: ORCHESTRATION_V2_WS_METHODS.launchThread,
    payload: {
      commandId: queued.commandId, threadId: queued.threadId, projectId: queued.creation.projectId,
      creationSource: "mobile", title: "Captured title", generateTitle: true,
      modelSelection: queued.modelSelection, runtimeMode: "full-access", interactionMode: "default",
      workspaceStrategy: { type: "worktree", baseRef: "main", branch: "captured-random-branch", startFromOrigin: true },
      initialMessage: { messageId: queued.messageId, text: queued.text, context, attachments },
    },
  } : {
    method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
    payload: {
      type: "message.dispatch", createdBy: "user", creationSource: "mobile",
      commandId: queued.commandId, threadId: queued.threadId, messageId: queued.messageId,
      text: queued.text, context, attachments, modelSelection: queued.modelSelection, modelSelectionExplicit: true,
      dispatchMode: { type: "queue_after_active" }, queuedToolBoundaryEligible: true,
    },
  });
}

function queue(storage: ThreadOutboxStorage = expoThreadOutboxStorage) {
  const registry = AtomRegistry.make();
  onTestFinished(() => registry.dispose());
  const manager = createThreadOutboxManager({ registry, storage, warn: () => {} });
  const current = () => Object.values(registry.get(manager.queuedMessagesByThreadKeyAtom)).flat();
  return { ...manager, current };
}

async function dispatcher(send: (method: string, payload: unknown) => Promise<unknown>) {
  const client = {
    [ORCHESTRATION_V2_WS_METHODS.launchThread]: (payload: unknown) => Effect.tryPromise({
      try: () => send(ORCHESTRATION_V2_WS_METHODS.launchThread, payload), catch: (error) => error,
    }),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (payload: unknown) => Effect.tryPromise({
      try: () => send(ORCHESTRATION_V2_WS_METHODS.dispatchCommand, payload), catch: (error) => error,
    }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession = {
    client, initialConfig: Effect.die("Replay must not read capabilities or catalog"),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void, probe: Effect.void, closed: Effect.never,
  };
  const supervisor = await Effect.runPromise(Effect.gen(function* () {
    return EnvironmentSupervisor.EnvironmentSupervisor.of({
      target: new PrimaryConnectionTarget({
        environmentId: EnvironmentId.make("c3-environment"), label: "Synthetic C3",
        httpBaseUrl: "https://c3.example.test", wsBaseUrl: "wss://c3.example.test",
      }),
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      session: yield* SubscriptionRef.make(Option.some(session)),
      prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
      connect: Effect.void, disconnect: Effect.void, retryNow: Effect.void,
    });
  }));
  return async (prepared: PreparedThreadTurn) => {
    const result = await Effect.runPromiseExit(dispatchPreparedThreadTurn(prepared).pipe(
      Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    ));
    if (Exit.isFailure(result)) throw Cause.squash(result.cause);
    return result.value;
  };
}

describe("mobile durable request delivery", () => {
  it.each([true, false])("restarts after a lost response and dispatches the exact persisted envelope (launch: %s)", async (launch) => {
    const original = message(launch);
    const prepared = preparedFor(original);
    const first = queue();
    await first.enqueue(original);
    const observed: unknown[] = [];
    const send = await dispatcher(async (method, payload) => {
      observed.push({ method, payload: structuredClone(payload) });
      const saved = (await expoThreadOutboxStorage.load()).messages[0]!;
      expect(saved.issuance?.state).toBe("issued");
      if (observed.length === 1) throw new Error("Response lost after synthetic receipt commit");
      return { sequence: 1, threadId: original.threadId };
    });
    expect((await deliverQueuedThreadTurn({
      message: original, prepared, revision: first.revisionOf(original.messageId), queue: first,
      canDispatch: () => true, dispatch: send,
    }))?.status).toBe("unknown");
    const restarted = queue();
    expect(await restarted.load()).toBe(true);
    const loaded = restarted.current()[0]!;
    expect(queuedThreadContentChanged(loaded)).toBe(false);
    if (loaded.issuance?.state !== "issued") throw new Error("Lost issued envelope");
    expect(Object.isFrozen(loaded.issuance.prepared.payload)).toBe(true);
    expect((await deliverQueuedThreadTurn({
      message: loaded, revision: restarted.revisionOf(loaded.messageId), queue: restarted,
      canDispatch: () => true, dispatch: send,
    }))?.status).toBe("accepted");
    expect(observed).toEqual([
      { method: prepared.method, payload: prepared.payload }, { method: prepared.method, payload: prepared.payload },
    ]);
  });

  it("does not issue an RPC when issuance persistence fails and retains the original durable queue", async () => {
    const original = message();
    const manager = queue();
    await manager.enqueue(original);
    const before = new Map(disk.files);
    disk.failWrite = true;
    const dispatch = vi.fn();
    await expect(deliverQueuedThreadTurn({
      message: original, prepared: preparedFor(original), revision: manager.revisionOf(original.messageId),
      queue: manager, canDispatch: () => true, dispatch,
    })).rejects.toMatchObject({ operation: "update" });
    expect(dispatch).not.toHaveBeenCalled();
    expect(disk.files).toEqual(before);
    expect(manager.current()).toEqual([original]);
  });

  it("restarts between the durable issuance write and RPC without preparing or allocating again", async () => {
    const original = message();
    const prepared = preparedFor(original);
    const manager = queue();
    await manager.enqueue(original);
    const dispatch = vi.fn();
    let admitted = true;
    expect(await deliverQueuedThreadTurn({
      message: original, prepared, revision: manager.revisionOf(original.messageId),
      queue: { ...manager, update: async (queued, revision) => {
        const updated = await manager.update(queued, revision);
        admitted = false;
        return updated;
      } },
      canDispatch: () => admitted, dispatch,
    })).toBeNull();
    expect(dispatch).not.toHaveBeenCalled();
    const restarted = queue();
    expect(await restarted.load()).toBe(true);
    const recovered = restarted.current()[0]!;
    expect(recovered.issuance?.state).toBe("issued");
    expect((await deliverQueuedThreadTurn({
      message: recovered, revision: restarted.revisionOf(recovered.messageId), queue: restarted,
      canDispatch: () => true, dispatch,
    }))?.status).toBe("accepted");
    expect(dispatch).toHaveBeenCalledOnce();
    expect(dispatch).toHaveBeenCalledWith(prepared);
  });

  it.each([1, 2, 3, 4])("reads legacy version %s as unknown and preserves the old-reader version gate", async (schemaVersion) => {
    const { issuance: _, ...original } = message();
    const loaded = decodeQueuedThreadMessage({ ...encodeQueuedThreadMessage(original) as object, schemaVersion });
    const manager = queue();
    await manager.enqueue(loaded);
    const dispatch = vi.fn();
    expect(canRestoreQueuedThreadMessage(loaded)).toBe(false);
    expect(await deliverQueuedThreadTurn({
      message: loaded, prepared: preparedFor(loaded), revision: manager.revisionOf(loaded.messageId),
      queue: manager, canDispatch: () => true, dispatch,
    })).toBeNull();
    expect(dispatch).not.toHaveBeenCalled();
    const encoded = encodeQueuedThreadMessage(withIssuedQueuedThreadTurn(message(), preparedFor(message())));
    expect(encoded).toMatchObject({ schemaVersion: 3 });
    const oldReader = QueuedThreadMessageSchema.mapFields(({ issuance: _issuance, ...fields }) => ({
      ...fields, schemaVersion: Schema.Literals([1, 2, 3]),
    }));
    expect(Schema.decodeUnknownSync(oldReader)(encoded)).toMatchObject({ schemaVersion: 3, commandId: original.commandId });
  });

  it("preserves newer editor content but never uses it to replace an issued payload", async () => {
    const original = message();
    const prepared = preparedFor(original);
    const manager = queue();
    await manager.enqueue(withIssuedQueuedThreadTurn(original, prepared));
    const edited = { ...original, text: "A deliberate later task", modelSelection: { ...original.modelSelection!, model: "other" } };
    expect(await manager.update(edited)).toBe(true);
    const saved = manager.current()[0]!;
    expect(queuedThreadContentChanged(saved)).toBe(true);
    const sent: PreparedThreadTurn[] = [];
    const result = await deliverQueuedThreadTurn({
      message: saved, revision: manager.revisionOf(saved.messageId), queue: manager, canDispatch: () => true,
      dispatch: async (request) => { sent.push(request); },
    });
    expect(sent).toEqual([prepared]);
    expect(result?.message.text).toBe(edited.text);
    expect(result && queuedThreadContentChanged(result.message)).toBe(true);
    const restart = queue();
    expect(await restart.load()).toBe(true);
    expect(restart.current()[0]?.text).toBe(edited.text);
    expect(queuedThreadContentChanged(restart.current()[0]!)).toBe(true);
  });

  it("retains the prior uncertain envelope after a failed editor enqueue and preserves missing legacy metadata", async () => {
    const original = message();
    const issued = withIssuedQueuedThreadTurn(original, preparedFor(original));
    const manager = queue();
    await manager.enqueue(issued);
    const before = new Map(disk.files);
    disk.failWrite = true;
    await expect(manager.enqueue({ ...original, text: "Unsaved editor change" })).rejects.toMatchObject({ operation: "enqueue" });
    expect(manager.current()).toEqual([issued]);
    expect(disk.files).toEqual(before);
    disk.failWrite = false;
    const { issuance: _, ...legacy } = { ...original, messageId: MessageId.make("legacy-edit"), commandId: CommandId.make("legacy-edit") };
    await manager.enqueue(legacy);
    expect(await manager.update({ ...legacy, text: "Edited legacy", issuance: { state: "unissued" } })).toBe(true);
    expect(manager.current().find((candidate) => candidate.messageId === legacy.messageId)?.issuance).toBeUndefined();
  });

  it("keeps acceptance across a failed observation write and a stale rejected editor save without reissuing", async () => {
    const original = message();
    let fail = false;
    const manager = queue({ ...expoThreadOutboxStorage, write: async (queued) => {
      if (fail) throw new Error("Observation disk unavailable");
      return expoThreadOutboxStorage.write(queued);
    } });
    const issued = withIssuedQueuedThreadTurn(original, preparedFor(original));
    await manager.enqueue(issued);
    const dispatch = vi.fn(async () => { fail = true; });
    await expect(deliverQueuedThreadTurn({
      message: issued, revision: manager.revisionOf(issued.messageId), queue: manager,
      canDispatch: () => true, dispatch,
    })).rejects.toMatchObject({ operation: "update" });
    expect(canRestoreQueuedThreadMessage(issued)).toBe(false);
    fail = false;
    const accepted = acceptedQueuedThreadDelivery(issued);
    if (accepted.kind !== "launch") throw new Error("Expected launch");
    const stale = observeQueuedThreadDelivery(issued, {
      ...accepted, creation: { status: "rejected" },
      initialMessage: { status: "unknown", commandId: initialMessageCommandId(original.commandId), messageId: original.messageId },
    });
    await manager.update({ ...stale, text: "newer editor" });
    const current = manager.current()[0]!;
    expect(queuedThreadDeliveryStatus(current)).toBe("accepted");
    expect((await deliverQueuedThreadTurn({
      message: current, revision: manager.revisionOf(current.messageId), queue: manager,
      canDispatch: () => true, dispatch,
    }))?.status).toBe("accepted");
    expect(dispatch).toHaveBeenCalledOnce();
    expect(manager.current()[0]?.text).toBe("newer editor");
  });

  it("retains creation acceptance separately from rejected or unknown first-message evidence", async () => {
    const original = message();
    for (const status of ["rejected", "unknown"] as const) {
      const issued = withIssuedQueuedThreadTurn(original, preparedFor(original));
      const updated = observeQueuedThreadDelivery(issued, {
        kind: "launch", commandId: original.commandId, projectId: original.creation!.projectId,
        creation: { status: "accepted", threadId: original.threadId },
        initialMessage: { status, commandId: initialMessageCommandId(original.commandId), messageId: original.messageId },
      });
      const restored = decodeQueuedThreadMessage(JSON.parse(JSON.stringify(encodeQueuedThreadMessage(updated))));
      expect(queuedThreadProvenTarget(restored)).toBe(original.threadId);
      expect(queuedThreadDeliveryEvidence(restored)).toMatchObject({ creation: { status: "accepted" }, initialMessage: { status } });
      expect(canRestoreQueuedThreadMessage(restored)).toBe(status === "rejected");
    }
    const issued = withIssuedQueuedThreadTurn(original, preparedFor(original));
    const invalid = { ...encodeQueuedThreadMessage(issued) as object, commandId: "different-command" };
    expect(() => decodeQueuedThreadMessage(invalid)).toThrow("does not match");
  });
});
