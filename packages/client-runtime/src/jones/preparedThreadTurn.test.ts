import { describe, expect, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  ComposerContextId,
  CommandId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2ThreadLaunchError,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Hex from "effect/encoding/Hex";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import {
  prepareThreadTurn,
  dispatchPreparedThreadTurn,
  type StartThreadTurnInput,
} from "../operations/commands.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type * as RpcSession from "../rpc/session.ts";
import { v2Projection, v2ThreadId } from "../state/orchestrationV2TestFixtures.ts";
import {
  PreparedThreadTurnJson,
  decodePreparedThreadTurn,
  encodePreparedThreadTurn,
} from "./preparedThreadTurn.ts";

const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("prepared-envelope"),
  label: "Prepared envelope fixture",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const layerTestCrypto = Layer.mergeAll(
  Layer.succeed(
    SecureRandom.SecureRandom,
    SecureRandom.make((size) => new Uint8Array(size)),
  ),
  Layer.succeed(
    Digest.Digest,
    Digest.Digest.of({
      digest: (_algorithm, data) => Effect.succeed(data),
      digestHex: (_algorithm, data) => Effect.succeed(Hex.encode(data)),
    }),
  ),
);

const StoredPreparedTurn = Schema.Struct({
  issuance: Schema.optionalKey(
    Schema.Struct({ state: Schema.Literal("issued"), prepared: PreparedThreadTurnJson }),
  ),
});

function expectDeeplyFrozen(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  expect(Object.isFrozen(value)).toBe(true);
  for (const child of Object.values(value)) expectDeeplyFrozen(child);
}
const encodeStoredPreparedTurn = Schema.encodeEffect(StoredPreparedTurn);
const decodeStoredPreparedTurn = Schema.decodeUnknownEffect(StoredPreparedTurn);

describe("durable prepared turn envelope", () => {
  it.effect.each([false, true])(
    "replays a persisted turn (launch: %s) after a lost response without preparing again",
    (launch) =>
      Effect.gen(function* () {
        const observed: unknown[] = [];
        let configReads = 0;
        const client = {
          [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (payload: OrchestrationV2Command) =>
            Effect.gen(function* () {
              observed.push(JSON.parse(JSON.stringify(payload)));
              if (observed.length === 1) {
                return yield* new OrchestrationV2DispatchCommandError({
                  commandId: payload.commandId,
                  commandType: payload.type,
                  message: "Projection failed after commit",
                });
              }
              return { sequence: 1 };
            }),
          [ORCHESTRATION_V2_WS_METHODS.launchThread]: (payload: OrchestrationV2ThreadLaunchInput) =>
            Effect.gen(function* () {
              observed.push(JSON.parse(JSON.stringify(payload)));
              if (observed.length === 1) {
                return yield* new OrchestrationV2ThreadLaunchError({
                  commandId: payload.commandId,
                  projectId: payload.projectId,
                  message: "Projection failed after commit",
                });
              }
              return { threadId: v2ThreadId, projection: v2Projection, resumed: false };
            }),
        } as unknown as WsRpcProtocolClient;
        const session: RpcSession.RpcSession = {
          client,
          initialConfig: Effect.sync(() => {
            configReads++;
            return {
              environment: {
                capabilities: {
                  serverResolvedCommandContext: true,
                  queuedToolBoundaryDelivery: true,
                },
              },
            } as never;
          }),
          subscribeServerConfig: (input = {}) => client.subscribeServerConfig(input),
          ready: Effect.void,
          probe: Effect.void,
          inboundFrames: Effect.succeed(0),
          closed: Effect.never,
        };
        const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
          target,
          state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
          session: yield* SubscriptionRef.make(Option.some(session)),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        });
        const attachment = {
          type: "file" as const,
          id: ChatAttachmentId.make("captured-file"),
          name: "input.txt",
          mimeType: "text/plain",
          sizeBytes: 4,
        };
        const input: StartThreadTurnInput = {
          commandId: CommandId.make("captured-command"),
          threadId: v2ThreadId,
          message: {
            messageId: MessageId.make("captured-message"),
            role: "user",
            text: "Captured input",
            attachments: [attachment],
            context: {
              version: 1,
              records: [
                {
                  version: 1,
                  kind: "file",
                  contextId: ComposerContextId.make("captured-context"),
                  label: "Input",
                  attachmentId: attachment.id,
                  name: attachment.name,
                  mimeType: attachment.mimeType,
                  sizeBytes: attachment.sizeBytes,
                },
              ],
            },
          },
          modelSelection: {
            ...v2Projection.thread.modelSelection,
            options: [{ id: "reasoningEffort", value: "max" }],
          },
          modelSelectionExplicit: true,
          runtimeMode: "full-access",
          interactionMode: "default",
          creationSource: "mobile",
          ...(launch
            ? {
                serverResolvesWorktreeBase: true,
                bootstrap: {
                  createThread: {
                    ...v2Projection.thread,
                    createdAt: "2026-11-02T00:00:00.000Z",
                  },
                  prepareWorktree: {
                    projectCwd: "/synthetic/project",
                    branch: "captured-random-branch",
                  },
                },
              }
            : {}),
        };
        const prepared = yield* prepareThreadTurn(input).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );
        expect(observed).toEqual([]);
        const encoded = encodePreparedThreadTurn(prepared);
        const persisted = JSON.stringify(encoded);
        const originalPayload = JSON.parse(JSON.stringify(prepared.payload));
        const first = yield* dispatchPreparedThreadTurn(prepared).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.exit,
        );
        expect(first._tag).toBe("Failure");

        // A new session has no preparation capabilities or snapshots available.
        yield* SubscriptionRef.set(
          supervisor.session,
          Option.some({
            ...session,
            initialConfig: Effect.die("Replay must not consult replacement capabilities"),
          }),
        );
        attachment.name = "newer-editor-attachment.txt";
        const restored = decodePreparedThreadTurn(JSON.parse(persisted));
        expect(restored.method).toBe(prepared.method);
        expect(restored.payload).toEqual(originalPayload);
        expect(Object.isFrozen(restored.payload)).toBe(true);
        expectDeeplyFrozen(restored);
        const encodedIssuance = yield* encodeStoredPreparedTurn({
          issuance: { state: "issued", prepared },
        });
        const persistedIssuance = JSON.stringify(encodedIssuance);
        const storedIssuance: unknown = JSON.parse(persistedIssuance);
        const nested = yield* decodeStoredPreparedTurn(storedIssuance);
        expect(nested.issuance?.prepared).toEqual(restored);
        expectDeeplyFrozen(nested.issuance?.prepared);
        const encodedNested = yield* encodeStoredPreparedTurn(nested);
        expect(JSON.stringify(encodedNested)).toBe(persistedIssuance);
        if (restored.method === ORCHESTRATION_V2_WS_METHODS.launchThread) {
          expect(restored.payload.workspaceStrategy).toEqual({
            type: "worktree",
            branch: "captured-random-branch",
          });
          expect(Object.isFrozen(restored.payload.initialMessage.attachments[0])).toBe(true);
        } else {
          expect(restored.payload).toMatchObject({
            deliveryIntent: "auto",
            queuedToolBoundaryEligible: true,
          });
          expect(restored.payload.dispatchMode).toEqual({ type: "start_immediately" });
          expect(Object.isFrozen(restored.payload.attachments[0])).toBe(true);
        }
        expect(JSON.stringify(encodePreparedThreadTurn(restored))).toBe(persisted);
        yield* dispatchPreparedThreadTurn(restored).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );
        expect(observed).toEqual([originalPayload, originalPayload]);
        expect(configReads).toBe(launch ? 0 : 1);
      }).pipe(Effect.provide(layerTestCrypto)),
  );

  it("refuses foreign operations, missing IDs and non-serializable payloads", () => {
    const payload = {
      type: "message.dispatch",
      commandId: "send",
      threadId: "thread",
      messageId: "message",
      createdBy: "user",
      creationSource: "mobile",
      text: "Captured",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
    };
    for (const value of [
      { method: "other", payload },
      {
        method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        payload: { type: "thread.stop", commandId: "stop", threadId: "thread" },
      },
      {
        method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        payload: { ...payload, messageId: undefined },
      },
      {
        method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        payload: { ...payload, dispatchMode: { type: "defer_start" } },
      },
      {
        method: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        payload: { ...payload, text: () => "transport closure" },
      },
    ]) {
      expect(() => decodePreparedThreadTurn(value)).toThrow();
    }
  });
});
