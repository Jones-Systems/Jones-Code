import {
  CommandId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { it as effectIt } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";

import { createEmptyReadModel, projectEvent } from "./projector.ts";

function makeEvent(input: {
  sequence: number;
  type: OrchestrationEvent["type"];
  occurredAt: string;
  aggregateKind: OrchestrationEvent["aggregateKind"];
  aggregateId: string;
  commandId: string | null;
  payload: unknown;
}): OrchestrationEvent {
  return {
    sequence: input.sequence,
    eventId: EventId.make(`event-${input.sequence}`),
    type: input.type,
    aggregateKind: input.aggregateKind,
    aggregateId:
      input.aggregateKind === "project"
        ? ProjectId.make(input.aggregateId)
        : ThreadId.make(input.aggregateId),
    occurredAt: input.occurredAt,
    commandId: input.commandId === null ? null : CommandId.make(input.commandId),
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: input.payload as never,
  } as OrchestrationEvent;
}

function projectEvents(
  model: ReturnType<typeof createEmptyReadModel>,
  events: ReadonlyArray<OrchestrationEvent>,
) {
  return Effect.gen(function* () {
    let current = model;
    for (const event of events) {
      current = yield* projectEvent(current, event);
    }
    return current;
  });
}

describe("orchestration projector", () => {
  it("applies thread.created events", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    const next = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: now,
          commandId: "cmd-thread-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      ),
    );

    expect(next.snapshotSequence).toBe(1);
    expect(next.threads).toEqual([
      {
        id: "thread-1",
        projectId: "project-1",
        title: "demo",
        modelSelection: {
          instanceId: "codex",
          model: "gpt-5-codex",
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        pullRequests: [],
        branchPullRequest: null,
        latestTurn: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        activeOrderKey: null,
        autoSettleDisabledAt: null,
        settledOverride: null,
        settledAt: null,
        unsettledAt: null,
        snoozedUntil: null,
        snoozedAt: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ]);
  });

  effectIt.effect("sets and clears branch pull requests without changing manual links", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const eventFields = {
        aggregateKind: "thread" as const,
        aggregateId: "thread-1",
        occurredAt: now,
        commandId: null,
      };
      let model = yield* projectEvent(
        {
          ...createEmptyReadModel(now),
          projects: [
            {
              id: ProjectId.make("project-1"),
              title: "T3 Code",
              workspaceRoot: "/repo",
              defaultModelSelection: null,
              scripts: [],
              createdAt: now,
              updatedAt: now,
              deletedAt: null,
              repositoryIdentity: {
                canonicalKey: "github.com/pingdotgg/t3code",
                provider: "github",
                displayName: "pingdotgg/t3code",
                locator: {
                  source: "git-remote",
                  remoteName: "origin",
                  remoteUrl: "https://github.com/pingdotgg/t3code.git",
                },
              },
            },
          ],
        },
        makeEvent({
          ...eventFields,
          sequence: 1,
          type: "thread.created",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "Pull request thread",
            modelSelection: { provider: "codex", model: "gpt-5-codex" },
            runtimeMode: "full-access",
            branch: "feature",
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      );
      const linkedPullRequest = {
        projectId: "project-1",
        repository: "pingdotgg/t3code",
        number: 42,
        url: "https://github.com/pingdotgg/t3code/pull/42",
      };
      const branchPullRequest = {
        ...linkedPullRequest,
        number: 43,
        url: "https://github.com/pingdotgg/t3code/pull/43",
      };
      const updates = [
        { payload: { linkedPullRequest, branchPullRequest }, expected: branchPullRequest },
        { payload: { title: "Renamed thread" }, expected: branchPullRequest },
        { payload: { branchPullRequest: null }, expected: null },
      ];

      for (const [index, update] of updates.entries()) {
        model = yield* projectEvent(
          model,
          makeEvent({
            ...eventFields,
            sequence: index + 2,
            type: "thread.meta-updated",
            payload: { threadId: "thread-1", updatedAt: now, ...update.payload },
          }),
        );
        expect(model.threads[0]?.branchPullRequest).toEqual(update.expected);
        expect(model.threads[0]?.linkedPullRequest).toEqual(linkedPullRequest);
      }
    }),
  );

  it("fails when event payload cannot be decoded by runtime schema", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    await expect(
      Effect.runPromise(
        projectEvent(
          model,
          makeEvent({
            sequence: 1,
            type: "thread.created",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: now,
            commandId: "cmd-invalid",
            payload: {
              // missing required threadId
              projectId: "project-1",
              title: "demo",
              modelSelection: {
                provider: ProviderDriverKind.make("codex"),
                model: "gpt-5-codex",
              },
              branch: null,
              worktreePath: null,
              createdAt: now,
              updatedAt: now,
            },
          }),
        ),
      ),
    ).rejects.toBeDefined();
  });

  it("applies thread.archived and thread.unarchived events", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const later = "2026-01-01T00:00:01.000Z";
    const created = await Effect.runPromise(
      projectEvent(
        createEmptyReadModel(now),
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: now,
          commandId: "cmd-thread-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      ),
    );

    const archived = await Effect.runPromise(
      projectEvent(
        created,
        makeEvent({
          sequence: 2,
          type: "thread.archived",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: later,
          commandId: "cmd-thread-archive",
          payload: {
            threadId: "thread-1",
            archivedAt: later,
            updatedAt: later,
          },
        }),
      ),
    );
    expect(archived.threads[0]?.archivedAt).toBe(later);

    const unarchived = await Effect.runPromise(
      projectEvent(
        archived,
        makeEvent({
          sequence: 3,
          type: "thread.unarchived",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: later,
          commandId: "cmd-thread-unarchive",
          payload: {
            threadId: "thread-1",
            updatedAt: later,
          },
        }),
      ),
    );
    expect(unarchived.threads[0]?.archivedAt).toBeNull();
  });

  it("keeps projector forward-compatible for unhandled event types", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    const next = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 7,
          type: "thread.turn-start-requested",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-01-01T00:00:00.000Z",
          commandId: "cmd-unhandled",
          payload: {
            threadId: "thread-1",
            messageId: "message-1",
            runtimeMode: "approval-required",
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        }),
      ),
    );

    expect(next.snapshotSequence).toBe(7);
    expect(next.updatedAt).toBe("2026-01-01T00:00:00.000Z");
    expect(next.threads).toEqual([]);
  });

  effectIt.effect.each([
    ["ready", "completed"],
    ["interrupted", "interrupted"],
  ] as const)(
    "preserves the turn state after a %s session captures its checkpoint",
    ([status, state]) =>
      Effect.gen(function* () {
        const createdAt = "2026-02-23T08:00:00.000Z";
        const startedAt = "2026-02-23T08:00:05.000Z";
        const model = createEmptyReadModel(createdAt);

        const afterCreate = yield* projectEvent(
          model,
          makeEvent({
            sequence: 1,
            type: "thread.created",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: createdAt,
            commandId: "cmd-create",
            payload: {
              threadId: "thread-1",
              projectId: "project-1",
              title: "demo",
              modelSelection: {
                provider: ProviderDriverKind.make("codex"),
                model: "gpt-5.3-codex",
              },
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
              updatedAt: createdAt,
            },
          }),
        );

        const settledAt = "2026-02-23T08:01:00.000Z";
        const [afterRunning, afterReady] = yield* Effect.flatMap(
          projectEvent(
            afterCreate,
            makeEvent({
              sequence: 2,
              type: "thread.session-set",
              aggregateKind: "thread",
              aggregateId: "thread-1",
              occurredAt: startedAt,
              commandId: "cmd-running",
              payload: {
                threadId: "thread-1",
                session: {
                  threadId: "thread-1",
                  status: "running",
                  providerName: "codex",
                  providerSessionId: "session-1",
                  providerThreadId: "provider-thread-1",
                  runtimeMode: "approval-required",
                  activeTurnId: "turn-1",
                  lastError: null,
                  updatedAt: startedAt,
                },
              },
            }),
          ),
          (running) =>
            Effect.map(
              projectEvent(
                running,
                makeEvent({
                  sequence: 3,
                  type: "thread.session-set",
                  aggregateKind: "thread",
                  aggregateId: "thread-1",
                  occurredAt: settledAt,
                  commandId: "cmd-ready",
                  payload: {
                    threadId: "thread-1",
                    session: {
                      threadId: "thread-1",
                      status,
                      providerName: "codex",
                      providerSessionId: "session-1",
                      providerThreadId: "provider-thread-1",
                      runtimeMode: "approval-required",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: settledAt,
                    },
                  },
                }),
              ),
              (ready) => [running, ready] as const,
            ),
        );

        const thread = afterRunning.threads[0];
        expect(thread?.latestTurn?.turnId).toBe("turn-1");
        expect(thread?.session?.status).toBe("running");

        // Legacy events without settlement retain the historical status-based
        // outcome and session timestamp.
        const settledThread = afterReady.threads[0];
        expect(settledThread?.latestTurn?.turnId).toBe("turn-1");
        expect(settledThread?.latestTurn?.state).toBe(state);
        expect(settledThread?.latestTurn?.completedAt).toBe(settledAt);

        const captured = yield* projectEvent(
          afterReady,
          makeEvent({
            sequence: 4,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: settledAt,
            commandId: "cmd-final-checkpoint",
            payload: {
              threadId: "thread-1",
              turnId: "turn-1",
              checkpointTurnCount: 1,
              checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
              status: "ready",
              files: [],
              assistantMessageId: "assistant:turn-1",
              completedAt: settledAt,
            },
          }),
        );
        expect(captured.threads[0]?.latestTurn?.state).toBe(state);
        expect(captured.threads[0]?.checkpoints[0]?.status).toBe("ready");
      }),
  );

  effectIt.effect.each([null, "ready", "interrupted", "stopped"] as const)(
    "replaces a missing checkpoint without inventing interruption for a %s session",
    (sessionStatus) =>
      Effect.gen(function* () {
        const now = "2026-09-04T23:00:00.000Z";
        const threadId = "thread-placeholder";
        const event = (sequence: number, type: OrchestrationEvent["type"], payload: unknown) =>
          makeEvent({
            sequence,
            type,
            payload,
            aggregateKind: "thread",
            aggregateId: threadId,
            occurredAt: now,
            commandId: `placeholder-${sequence}`,
          });
        let model = yield* projectEvent(
          createEmptyReadModel(now),
          event(1, "thread.created", {
            threadId,
            projectId: "project-1",
            title: "Placeholder",
            modelSelection: { instanceId: "codex", model: "test" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          }),
        );
        const checkpoint = {
          threadId,
          turnId: "turn-placeholder",
          checkpointTurnCount: 1,
          checkpointRef: "provider-diff:placeholder",
          files: [],
          assistantMessageId: "assistant:placeholder",
          completedAt: now,
        };
        if (sessionStatus === "interrupted" || sessionStatus === "stopped") {
          model = yield* projectEvent(
            model,
            event(2, "thread.session-set", {
              threadId,
              session: {
                threadId,
                status: "running",
                providerName: "codex",
                runtimeMode: "full-access",
                activeTurnId: "turn-placeholder",
                lastError: null,
                updatedAt: now,
              },
            }),
          );
        }
        model = yield* projectEvent(
          model,
          event(3, "thread.turn-diff-completed", {
            ...checkpoint,
            status: "missing",
          }),
        );
        if (sessionStatus !== null) {
          model = yield* projectEvent(
            model,
            event(4, "thread.session-set", {
              threadId,
              session: {
                threadId,
                status: sessionStatus,
                providerName: "codex",
                runtimeMode: "full-access",
                activeTurnId: null,
                lastError: null,
                updatedAt: now,
              },
            }),
          );
        }
        model = yield* projectEvent(
          model,
          event(5, "thread.turn-diff-completed", {
            ...checkpoint,
            status: "ready",
            checkpointRef: "refs/t3/checkpoints/thread-placeholder/turn/1",
          }),
        );
        expect(model.threads[0]?.latestTurn?.state).toBe(
          sessionStatus === "interrupted" || sessionStatus === "stopped"
            ? "interrupted"
            : sessionStatus === null
              ? "running"
              : "completed",
        );
      }),
  );

  effectIt.effect("tracks explicit settlement and replays legacy readiness", () =>
    Effect.gen(function* () {
      const createdAt = "2026-02-23T08:00:00.000Z";
      const startedAt = "2026-02-23T08:00:05.000Z";
      const model = createEmptyReadModel(createdAt);

      const afterCreate = yield* projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      );

      const afterRunning = yield* projectEvent(
        afterCreate,
        makeEvent({
          sequence: 2,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: startedAt,
          commandId: "cmd-running",
          payload: {
            threadId: "thread-1",
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              providerSessionId: "session-1",
              providerThreadId: "provider-thread-1",
              runtimeMode: "approval-required",
              activeTurnId: "turn-1",
              lastError: null,
              updatedAt: startedAt,
            },
          },
        }),
      );
      const settledAt = "2026-02-23T08:01:00.000Z";
      const readyEvent = (
        turnSettlement:
          | null
          | undefined
          | { turnId: string; state: "completed"; completedAt: string },
      ) =>
        makeEvent({
          sequence: 3,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: settledAt,
          commandId: "cmd-ready",
          payload: {
            threadId: "thread-1",
            ...(turnSettlement !== undefined ? { turnSettlement } : {}),
            session: {
              threadId: "thread-1",
              status: "ready",
              providerName: "codex",
              providerSessionId: "session-1",
              providerThreadId: "provider-thread-1",
              runtimeMode: "approval-required",
              activeTurnId: null,
              lastError: null,
              updatedAt: settledAt,
            },
          },
        });
      const afterStatusOnly = yield* projectEvent(afterRunning, readyEvent(null));
      const afterReady = yield* projectEvent(
        afterStatusOnly,
        readyEvent({ turnId: "turn-1", state: "completed", completedAt: settledAt }),
      );
      const afterLegacyReady = yield* projectEvent(afterRunning, readyEvent(undefined));

      const thread = afterRunning.threads[0];
      expect(thread?.latestTurn?.turnId).toBe("turn-1");
      expect(thread?.session?.status).toBe("running");

      expect(afterStatusOnly.threads[0]?.latestTurn?.state).toBe("running");
      expect(afterStatusOnly.threads[0]?.latestTurn?.completedAt).toBeNull();

      // The attributed settlement, not readiness alone, settles the turn.
      const settledThread = afterReady.threads[0];
      expect(settledThread?.latestTurn?.turnId).toBe("turn-1");
      expect(settledThread?.latestTurn?.state).toBe("completed");
      expect(settledThread?.latestTurn?.completedAt).toBe(settledAt);
      expect(afterLegacyReady.threads[0]?.latestTurn?.state).toBe("completed");

      const afterLateCheckpoint = yield* projectEvents(afterReady, [
        makeEvent({
          sequence: 4,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T08:02:00.000Z",
          commandId: "cmd-new-turn",
          payload: {
            threadId: "thread-1",
            turnSettlement: null,
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: "turn-2",
              lastError: null,
              updatedAt: "2026-02-23T08:02:00.000Z",
            },
          },
        }),
        makeEvent({
          sequence: 5,
          type: "thread.turn-diff-completed",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T08:02:01.000Z",
          commandId: "cmd-late-checkpoint",
          payload: {
            threadId: "thread-1",
            turnId: "turn-1",
            checkpointTurnCount: 1,
            checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
            status: "ready",
            files: [],
            assistantMessageId: null,
            completedAt: "2026-02-23T08:02:01.000Z",
          },
        }),
      ]);
      expect(afterLateCheckpoint.threads[0]?.latestTurn?.turnId).toBe("turn-2");
      expect(afterLateCheckpoint.threads[0]?.updatedAt).toBe("2026-02-23T08:02:01.000Z");
    }),
  );

  it("updates canonical thread runtime mode from thread.runtime-mode-set", async () => {
    const createdAt = "2026-02-23T08:00:00.000Z";
    const updatedAt = "2026-02-23T08:00:05.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterUpdate = await Effect.runPromise(
      projectEvents(model, [
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
        makeEvent({
          sequence: 2,
          type: "thread.runtime-mode-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: updatedAt,
          commandId: "cmd-runtime-mode-set",
          payload: {
            threadId: "thread-1",
            runtimeMode: "approval-required",
            updatedAt,
          },
        }),
      ]),
    );

    expect(afterUpdate.threads[0]?.runtimeMode).toBe("approval-required");
    expect(afterUpdate.threads[0]?.updatedAt).toBe(updatedAt);
  });

  effectIt.effect("marks assistant messages completed with non-streaming updates", () =>
    Effect.gen(function* () {
      const createdAt = "2026-02-23T09:00:00.000Z";
      const deltaAt = "2026-02-23T09:00:01.000Z";
      const completeAt = "2026-02-23T09:00:03.500Z";
      const model = createEmptyReadModel(createdAt);

      const afterStarting = yield* projectEvents(model, [
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
        makeEvent({
          sequence: 2,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-prior-running",
          payload: {
            threadId: "thread-1",
            turnSettlement: null,
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: "turn-prior",
              lastError: null,
              updatedAt: createdAt,
            },
          },
        }),
        makeEvent({
          sequence: 3,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-prior-settled",
          payload: {
            threadId: "thread-1",
            turnSettlement: {
              turnId: "turn-prior",
              state: "completed",
              completedAt: createdAt,
            },
            session: {
              threadId: "thread-1",
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: createdAt,
            },
          },
        }),
        makeEvent({
          sequence: 4,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: deltaAt,
          commandId: "cmd-starting",
          payload: {
            threadId: "thread-1",
            turnSettlement: null,
            session: {
              threadId: "thread-1",
              status: "starting",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: deltaAt,
            },
          },
        }),
      ]);
      const settlementFirstAt = "2026-02-23T09:00:01.500Z";
      const afterSettlementFirst = yield* projectEvent(
        afterStarting,
        makeEvent({
          sequence: 5,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: settlementFirstAt,
          commandId: "cmd-settlement-first",
          payload: {
            threadId: "thread-1",
            turnSettlement: {
              turnId: "turn-settlement-first",
              state: "completed",
              completedAt: settlementFirstAt,
              recoversMissingStart: true,
            },
            session: {
              threadId: "thread-1",
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: settlementFirstAt,
            },
          },
        }),
      );
      expect(afterSettlementFirst.threads[0]?.latestTurn).toMatchObject({
        turnId: "turn-settlement-first",
        state: "completed",
        requestedAt: settlementFirstAt,
        startedAt: settlementFirstAt,
        completedAt: settlementFirstAt,
        assistantMessageId: null,
      });
      const afterComplete = yield* projectEvents(afterStarting, [
        makeEvent({
          sequence: 6,
          type: "thread.turn-diff-completed",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: deltaAt,
          commandId: "cmd-checkpoint",
          payload: {
            threadId: "thread-1",
            turnId: "turn-1",
            checkpointTurnCount: 1,
            checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
            status: "ready",
            files: [],
            assistantMessageId: null,
            completedAt: deltaAt,
          },
        }),
        makeEvent({
          sequence: 7,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: deltaAt,
          commandId: "cmd-delta",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:msg-1",
            role: "assistant",
            text: "hello",
            turnId: "turn-1",
            streaming: true,
            createdAt: deltaAt,
            updatedAt: deltaAt,
          },
        }),
        makeEvent({
          sequence: 8,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: completeAt,
          commandId: "cmd-complete",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:msg-1",
            role: "assistant",
            text: "",
            turnId: "turn-1",
            streaming: false,
            createdAt: completeAt,
            updatedAt: completeAt,
          },
        }),
      ]);

      const message = afterComplete.threads[0]?.messages[0];
      expect(message?.id).toBe("assistant:msg-1");
      expect(message?.text).toBe("hello");
      expect(message?.streaming).toBe(false);
      expect(message?.updatedAt).toBe(completeAt);
      expect(afterComplete.threads[0]?.latestTurn?.state).toBe("running");
      expect(afterComplete.threads[0]?.latestTurn?.assistantMessageId).toBe("assistant:msg-1");
      expect(afterComplete.threads[0]?.checkpoints[0]?.assistantMessageId).toBe("assistant:msg-1");

      const afterSettlement = yield* projectEvent(
        afterComplete,
        makeEvent({
          sequence: 9,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: completeAt,
          commandId: "cmd-settle",
          payload: {
            threadId: "thread-1",
            turnSettlement: {
              turnId: "turn-1",
              state: "completed",
              completedAt: completeAt,
            },
            session: {
              threadId: "thread-1",
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: completeAt,
            },
          },
        }),
      );
      expect(afterSettlement.threads[0]?.latestTurn?.state).toBe("completed");
      expect(afterSettlement.threads[0]?.latestTurn?.assistantMessageId).toBe("assistant:msg-1");
    }),
  );

  it("prunes reverted turn messages from in-memory thread snapshot", async () => {
    const createdAt = "2026-02-23T10:00:00.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const events: ReadonlyArray<OrchestrationEvent> = [
      makeEvent({
        sequence: 2,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:01.000Z",
        commandId: "cmd-user-1",
        payload: {
          threadId: "thread-1",
          messageId: "user-msg-1",
          role: "user",
          text: "First edit",
          turnId: null,
          streaming: false,
          createdAt: "2026-02-23T10:00:01.000Z",
          updatedAt: "2026-02-23T10:00:01.000Z",
        },
      }),
      makeEvent({
        sequence: 3,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.000Z",
        commandId: "cmd-assistant-1",
        payload: {
          threadId: "thread-1",
          messageId: "assistant-msg-1",
          role: "assistant",
          text: "Updated README to v2.\n",
          turnId: "turn-1",
          streaming: false,
          createdAt: "2026-02-23T10:00:02.000Z",
          updatedAt: "2026-02-23T10:00:02.000Z",
        },
      }),
      makeEvent({
        sequence: 4,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.500Z",
        commandId: "cmd-turn-1-complete",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          checkpointTurnCount: 1,
          checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-msg-1",
          completedAt: "2026-02-23T10:00:02.500Z",
        },
      }),
      makeEvent({
        sequence: 5,
        type: "thread.activity-appended",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.750Z",
        commandId: "cmd-activity-1",
        payload: {
          threadId: "thread-1",
          activity: {
            id: "activity-1",
            tone: "tool",
            kind: "tool.started",
            summary: "Edit file started",
            payload: { toolKind: "command" },
            turnId: "turn-1",
            createdAt: "2026-02-23T10:00:02.750Z",
          },
        },
      }),
      makeEvent({
        sequence: 6,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:03.000Z",
        commandId: "cmd-user-2",
        payload: {
          threadId: "thread-1",
          messageId: "user-msg-2",
          role: "user",
          text: "Second edit",
          turnId: null,
          streaming: false,
          createdAt: "2026-02-23T10:00:03.000Z",
          updatedAt: "2026-02-23T10:00:03.000Z",
        },
      }),
      makeEvent({
        sequence: 7,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.000Z",
        commandId: "cmd-assistant-2",
        payload: {
          threadId: "thread-1",
          messageId: "assistant-msg-2",
          role: "assistant",
          text: "Updated README to v3.\n",
          turnId: "turn-2",
          streaming: false,
          createdAt: "2026-02-23T10:00:04.000Z",
          updatedAt: "2026-02-23T10:00:04.000Z",
        },
      }),
      makeEvent({
        sequence: 8,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.500Z",
        commandId: "cmd-turn-2-complete",
        payload: {
          threadId: "thread-1",
          turnId: "turn-2",
          checkpointTurnCount: 2,
          checkpointRef: "refs/t3/checkpoints/thread-1/turn/2",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-msg-2",
          completedAt: "2026-02-23T10:00:04.500Z",
        },
      }),
      makeEvent({
        sequence: 9,
        type: "thread.activity-appended",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.750Z",
        commandId: "cmd-activity-2",
        payload: {
          threadId: "thread-1",
          activity: {
            id: "activity-2",
            tone: "tool",
            kind: "tool.completed",
            summary: "Edit file complete",
            payload: { toolKind: "command" },
            turnId: "turn-2",
            createdAt: "2026-02-23T10:00:04.750Z",
          },
        },
      }),
      makeEvent({
        sequence: 10,
        type: "thread.reverted",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:05.000Z",
        commandId: "cmd-revert",
        payload: {
          threadId: "thread-1",
          turnCount: 1,
        },
      }),
    ];

    const afterRevert = await events.reduce<Promise<ReturnType<typeof createEmptyReadModel>>>(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterCreate),
    );

    const thread = afterRevert.threads[0];
    expect(thread?.messages.map((message) => ({ role: message.role, text: message.text }))).toEqual(
      [
        { role: "user", text: "First edit" },
        { role: "assistant", text: "Updated README to v2.\n" },
      ],
    );
    expect(
      thread?.activities.map((activity) => ({ id: activity.id, turnId: activity.turnId })),
    ).toEqual([{ id: "activity-1", turnId: "turn-1" }]);
    expect(thread?.checkpoints.map((checkpoint) => checkpoint.checkpointTurnCount)).toEqual([1]);
    expect(thread?.latestTurn?.turnId).toBe("turn-1");
    expect(thread?.latestTurn?.state).toBe("running");
    expect(thread?.latestTurn?.completedAt).toBeNull();
  });

  it("does not fallback-retain messages tied to removed turn IDs", async () => {
    const createdAt = "2026-02-26T12:00:00.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-revert",
          occurredAt: createdAt,
          commandId: "cmd-create-revert",
          payload: {
            threadId: "thread-revert",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const events: ReadonlyArray<OrchestrationEvent> = [
      makeEvent({
        sequence: 2,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:01.000Z",
        commandId: "cmd-turn-1",
        payload: {
          threadId: "thread-revert",
          turnId: "turn-1",
          checkpointTurnCount: 1,
          checkpointRef: "refs/t3/checkpoints/thread-revert/turn/1",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-keep",
          completedAt: "2026-02-26T12:00:01.000Z",
        },
      }),
      makeEvent({
        sequence: 3,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:01.100Z",
        commandId: "cmd-assistant-keep",
        payload: {
          threadId: "thread-revert",
          messageId: "assistant-keep",
          role: "assistant",
          text: "kept",
          turnId: "turn-1",
          streaming: false,
          createdAt: "2026-02-26T12:00:01.100Z",
          updatedAt: "2026-02-26T12:00:01.100Z",
        },
      }),
      makeEvent({
        sequence: 4,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:02.000Z",
        commandId: "cmd-turn-2",
        payload: {
          threadId: "thread-revert",
          turnId: "turn-2",
          checkpointTurnCount: 2,
          checkpointRef: "refs/t3/checkpoints/thread-revert/turn/2",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-remove",
          completedAt: "2026-02-26T12:00:02.000Z",
        },
      }),
      makeEvent({
        sequence: 5,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:02.050Z",
        commandId: "cmd-user-remove",
        payload: {
          threadId: "thread-revert",
          messageId: "user-remove",
          role: "user",
          text: "removed",
          turnId: "turn-2",
          streaming: false,
          createdAt: "2026-02-26T12:00:02.050Z",
          updatedAt: "2026-02-26T12:00:02.050Z",
        },
      }),
      makeEvent({
        sequence: 6,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:02.100Z",
        commandId: "cmd-assistant-remove",
        payload: {
          threadId: "thread-revert",
          messageId: "assistant-remove",
          role: "assistant",
          text: "removed",
          turnId: "turn-2",
          streaming: false,
          createdAt: "2026-02-26T12:00:02.100Z",
          updatedAt: "2026-02-26T12:00:02.100Z",
        },
      }),
      makeEvent({
        sequence: 7,
        type: "thread.reverted",
        aggregateKind: "thread",
        aggregateId: "thread-revert",
        occurredAt: "2026-02-26T12:00:03.000Z",
        commandId: "cmd-revert",
        payload: {
          threadId: "thread-revert",
          turnCount: 1,
        },
      }),
    ];

    const afterRevert = await events.reduce<Promise<ReturnType<typeof createEmptyReadModel>>>(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterCreate),
    );

    const thread = afterRevert.threads[0];
    expect(
      thread?.messages.map((message) => ({
        id: message.id,
        role: message.role,
        turnId: message.turnId,
      })),
    ).toEqual([{ id: "assistant-keep", role: "assistant", turnId: "turn-1" }]);
  });

  it("caps message and checkpoint retention for long-lived threads", async () => {
    const createdAt = "2026-03-01T10:00:00.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-capped",
          occurredAt: createdAt,
          commandId: "cmd-create-capped",
          payload: {
            threadId: "thread-capped",
            projectId: "project-1",
            title: "capped",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const messageEvents: ReadonlyArray<OrchestrationEvent> = Array.from(
      { length: 2_100 },
      (_, index) =>
        makeEvent({
          sequence: index + 2,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-capped",
          occurredAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
          commandId: `cmd-message-${index}`,
          payload: {
            threadId: "thread-capped",
            messageId: `msg-${index}`,
            role: "assistant",
            text: `message-${index}`,
            turnId: `turn-${index}`,
            streaming: false,
            createdAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
            updatedAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
          },
        }),
    );
    const afterMessages = await messageEvents.reduce<
      Promise<ReturnType<typeof createEmptyReadModel>>
    >(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterCreate),
    );

    const checkpointEvents: ReadonlyArray<OrchestrationEvent> = Array.from(
      { length: 600 },
      (_, index) =>
        makeEvent({
          sequence: index + 2_102,
          type: "thread.turn-diff-completed",
          aggregateKind: "thread",
          aggregateId: "thread-capped",
          occurredAt: `2026-03-01T10:30:${String(index % 60).padStart(2, "0")}.000Z`,
          commandId: `cmd-checkpoint-${index}`,
          payload: {
            threadId: "thread-capped",
            turnId: `turn-${index}`,
            checkpointTurnCount: index + 1,
            checkpointRef: `refs/t3/checkpoints/thread-capped/turn/${index + 1}`,
            status: "ready",
            files: [],
            assistantMessageId: `msg-${index}`,
            completedAt: `2026-03-01T10:30:${String(index % 60).padStart(2, "0")}.000Z`,
          },
        }),
    );
    const finalState = await checkpointEvents.reduce<
      Promise<ReturnType<typeof createEmptyReadModel>>
    >(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterMessages),
    );

    const thread = finalState.threads[0];
    expect(thread?.messages).toHaveLength(2_000);
    expect(thread?.messages[0]?.id).toBe("msg-100");
    expect(thread?.messages.at(-1)?.id).toBe("msg-2099");
    expect(thread?.checkpoints).toHaveLength(500);
    expect(thread?.checkpoints[0]?.turnId).toBe("turn-100");
    expect(thread?.checkpoints.at(-1)?.turnId).toBe("turn-599");
  });

  effectIt.effect("keeps the worktree setup record past the activity retention cap", () =>
    Effect.gen(function* () {
      const createdAt = "2026-03-01T10:00:00.000Z";
      const threadId = "thread-setup-retained";
      const afterCreate = yield* projectEvent(
        createEmptyReadModel(createdAt),
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: threadId,
          occurredAt: createdAt,
          commandId: "cmd-create-setup-retained",
          payload: {
            threadId,
            projectId: "project-1",
            title: "setup retained",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      );
      const activityEvent = (sequence: number, id: string, kind: string) =>
        makeEvent({
          sequence,
          type: "thread.activity-appended",
          aggregateKind: "thread",
          aggregateId: threadId,
          occurredAt: `2026-03-01T10:${String(Math.floor(sequence / 60) % 60).padStart(2, "0")}:${String(sequence % 60).padStart(2, "0")}.000Z`,
          commandId: `cmd-activity-${sequence}`,
          payload: {
            threadId,
            activity: {
              id,
              tone: "info",
              kind,
              summary: kind,
              payload: {},
              turnId: null,
              createdAt: `2026-03-01T10:${String(Math.floor(sequence / 60) % 60).padStart(2, "0")}:${String(sequence % 60).padStart(2, "0")}.000Z`,
            },
          },
        });
      let model = yield* projectEvent(
        afterCreate,
        activityEvent(2, `worktree-setup:${threadId}`, "worktree-setup"),
      );
      for (let index = 0; index < 600; index += 1) {
        model = yield* projectEvent(
          model,
          activityEvent(3 + index, `tool-${index}`, "tool.completed"),
        );
      }
      const thread = model.threads.find((entry) => entry.id === threadId);
      expect(thread?.activities).toHaveLength(501);
      expect(thread?.activities[0]?.id).toBe(`worktree-setup:${threadId}`);
    }),
  );
});
