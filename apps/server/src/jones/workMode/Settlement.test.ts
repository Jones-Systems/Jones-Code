import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  RunId,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
  type ServerSettings as ContractServerSettings,
} from "@t3tools/contracts";
import { WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as GitManager from "../../git/GitManager.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ThreadSettlementService from "../../orchestration-v2/ThreadSettlementService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as PullRequestService from "../../pullRequest/PullRequestService.ts";
import * as ServerActivation from "../../serverActivation.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { workModeFixture } from "./Fixtures.testkit.ts";
import { workModeCommand } from "./Policy.ts";

const NOW_MS = Date.parse("2026-10-19T12:00:00.000Z");
const DAY_MS = 24 * 60 * 60 * 1_000;
const at = (offsetMs: number) => DateTime.makeUnsafe(NOW_MS + offsetMs);

type AutoSettleCommand = Extract<OrchestrationV2Command, { readonly type: "thread.auto-settle" }>;

// A thread whose last genuine exchange is `genuineAgeMs` old and which Work
// refreshed ten minutes ago. `followUp` replaces the refresh with another
// automated (non-Work) message to prove only keep-warm exchanges are ignored.
function keptWarmThread(genuineAgeMs: number, followUp: "keep-warm" | "agent" = "keep-warm") {
  const base = workModeFixture();
  const genuineAt = at(-genuineAgeMs);
  const refreshAt = at(-10 * 60_000);
  const genuineRun = {
    ...base.runs[0]!,
    requestedAt: genuineAt,
    startedAt: genuineAt,
    completedAt: genuineAt,
  };
  const genuineMessage = { ...base.messages[0]!, createdAt: genuineAt, updatedAt: genuineAt };
  const refreshMessageId =
    followUp === "keep-warm"
      ? workModeCommand({ threadId: base.thread.id, generation: "refresh" }).messageId
      : genuineMessage.id.replace("message", "agent-message");
  const refreshRunId = RunId.make("run-refresh");
  const projection: OrchestrationV2ThreadProjection = {
    ...base,
    thread: { ...base.thread, createdAt: at(-30 * DAY_MS), updatedAt: refreshAt },
    runs: [
      genuineRun,
      {
        ...genuineRun,
        id: refreshRunId,
        ordinal: 2,
        userMessageId: refreshMessageId as typeof genuineRun.userMessageId,
        requestedAt: refreshAt,
        startedAt: refreshAt,
        completedAt: refreshAt,
      },
    ],
    messages: [
      genuineMessage,
      {
        ...genuineMessage,
        id: refreshMessageId as typeof genuineMessage.id,
        runId: refreshRunId,
        text: followUp === "keep-warm" ? WORK_MODE_SENTINEL : "Agent follow-up",
        createdBy: followUp === "keep-warm" ? "system" : "agent",
        creationSource: followUp === "keep-warm" ? "server" : "mcp",
        createdAt: refreshAt,
        updatedAt: refreshAt,
      },
    ],
    updatedAt: refreshAt,
  };
  const shell = ProjectionStore.threadShellFromProjection(projection);
  const candidate: ProjectionStore.ProjectionSettlementCandidate = {
    ...shell,
    lastSnoozeWakeAt: null,
    latestUserAuthoredMessageAt: shell.latestUserAuthoredMessageAt ?? null,
  };
  return { projection, candidate, genuineAt, refreshAt };
}

const makeHarness = Effect.fn("makeWorkModeSettlementHarness")(function* (options: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly candidate: ProjectionStore.ProjectionSettlementCandidate;
  readonly settings: ContractServerSettings;
  readonly recordsFail?: boolean;
  // Holds the history read open until the test changes settings mid-sweep.
  readonly holdRecords?: boolean;
}) {
  const activation = yield* Deferred.make<void>();
  const candidateReads = yield* Queue.unbounded<void>();
  const recordReads = yield* Ref.make(0);
  const recordsRequested = yield* Deferred.make<void>();
  const recordsReleased = yield* Deferred.make<void>();
  if (options.holdRecords !== true) yield* Deferred.succeed(recordsReleased, undefined);
  const settings = yield* Ref.make(options.settings);
  const commands = yield* Ref.make<ReadonlyArray<AutoSettleCommand>>([]);
  const settingsChanges = yield* PubSub.unbounded<ContractServerSettings>();
  const merges = yield* PubSub.unbounded<PullRequestService.PullRequestMergeEvent>();

  const getThreadRecords = ((threadId, _fields, filter) =>
    Ref.update(recordReads, (count) => count + 1).pipe(
      Effect.andThen(Deferred.succeed(recordsRequested, undefined)),
      Effect.andThen(Deferred.await(recordsReleased)),
      Effect.andThen(
        options.recordsFail === true || threadId !== options.projection.thread.id
          ? Effect.die(new Error("thread records unavailable"))
          : Effect.succeed({
              thread: options.projection.thread,
              runs: options.projection.runs,
              messages: options.projection.messages.filter(
                (message) =>
                  filter?.messageRoles === undefined || filter.messageRoles.includes(message.role),
              ),
            }),
      ),
    )) as ProjectionStore.ProjectionStoreV2["Service"]["getThreadRecords"];

  const dependencies = Layer.mergeAll(
    Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getSettlementCandidates: () =>
        Queue.offer(candidateReads, undefined).pipe(Effect.as([options.candidate])),
      getThreadRecords,
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({
      streamDomainEvents: Stream.never,
      dispatch: (command) =>
        command.type === "thread.auto-settle"
          ? Ref.update(commands, (recorded) => [...recorded, command]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            )
          : Effect.die(new Error(`Unexpected command: ${command.type}`)),
    }),
    Layer.mock(TerminalManager.TerminalManager)({ closeIdle: () => Effect.void }),
    Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
      runForThread: () => Effect.succeed({ status: "no-script" } as const),
    }),
    Layer.mock(GitManager.GitManager)({
      branchPullRequest: () => Effect.succeed(null),
      invalidateStatus: () => Effect.void,
    }),
    Layer.mock(PullRequestService.PullRequestService)({
      subscribeMerges: PubSub.subscribe(merges).pipe(Effect.map(Stream.fromSubscription)),
    }),
    Layer.succeed(
      ServerSettings.ServerSettingsService,
      ServerSettings.ServerSettingsService.of({
        start: Effect.void,
        ready: Effect.void,
        getSettings: Ref.get(settings),
        updateSettings: () => Effect.die("Unexpected settings update"),
        updateProviderInstance: () => Effect.die("Unexpected provider mutation"),
        withSettingsSnapshot: (use) => Ref.get(settings).pipe(Effect.flatMap(use)),
        streamChanges: Stream.fromPubSub(settingsChanges),
        subscribeChanges: PubSub.subscribe(settingsChanges).pipe(
          Effect.map((subscription) => Stream.fromSubscription(subscription)),
        ),
      }),
    ),
    Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
    Layer.succeed(
      Crypto.Crypto,
      Crypto.make({
        randomBytes: (size) => new Uint8Array(size).fill(1),
        digest: (_algorithm, data) => Effect.succeed(data),
      }),
    ),
    FileSystem.layerNoop({ exists: () => Effect.succeed(false) }),
  );

  // Runs the first periodic sweep to completion and returns what it dispatched.
  const sweepOnce = Effect.gen(function* () {
    const service = yield* ThreadSettlementService.ThreadSettlementServiceV2;
    yield* service.start();
    yield* Deferred.succeed(activation, undefined);
    yield* Queue.take(candidateReads);
    yield* service.drain;
    return { commands: yield* Ref.get(commands), recordReads: yield* Ref.get(recordReads) };
  }).pipe(Effect.provide(ThreadSettlementService.layer.pipe(Layer.provide(dependencies))));

  // Changes settings once the sweep is reading history, then lets the read finish.
  const changeSettingsDuringRecordRead = (next: ContractServerSettings) =>
    Deferred.await(recordsRequested).pipe(
      Effect.andThen(Ref.set(settings, next)),
      Effect.andThen(Deferred.succeed(recordsReleased, undefined)),
    );

  return { sweepOnce, changeSettingsDuringRecordRead };
});

const workOn = { ...DEFAULT_SERVER_SETTINGS, workModeEnabled: true };

describe("Work Mode keep-warm and inactivity settlement", () => {
  it.effect("settles from the last genuine activity, ignoring newer keep-warm refreshes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const thread = keptWarmThread(4 * DAY_MS);
        const harness = yield* makeHarness({ ...thread, settings: workOn });
        const { commands } = yield* harness.sweepOnce;
        expect(commands).toHaveLength(1);
        expect(commands[0]?.threadId).toBe(thread.candidate.id);
        expect(commands[0]?.settledAt).toEqual(thread.genuineAt);
        expect(commands[0]?.snapshotAt).toEqual(thread.candidate.updatedAt);
      }),
    ),
  );

  it.effect("keeps recently used threads active and does not read their history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const harness = yield* makeHarness({ ...keptWarmThread(1 * DAY_MS), settings: workOn });
        expect(yield* harness.sweepOnce).toEqual({ commands: [], recordReads: 0 });
      }),
    ),
  );

  it.effect("leaves upstream anchors unchanged while Work mode is off", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const harness = yield* makeHarness({
          ...keptWarmThread(4 * DAY_MS),
          settings: DEFAULT_SERVER_SETTINGS,
        });
        expect(yield* harness.sweepOnce).toEqual({ commands: [], recordReads: 0 });
      }),
    ),
  );

  it.effect("keeps upstream anchors when Work mode is turned off while history is read", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const harness = yield* makeHarness({
          ...keptWarmThread(4 * DAY_MS),
          settings: workOn,
          holdRecords: true,
        });
        const sweep = yield* Effect.forkChild(harness.sweepOnce);
        yield* harness.changeSettingsDuringRecordRead({ ...workOn, workModeEnabled: false });
        expect(yield* Fiber.join(sweep)).toEqual({ commands: [], recordReads: 1 });
      }),
    ),
  );

  it.effect("still counts automated activity that is not a keep-warm refresh", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const harness = yield* makeHarness({
          ...keptWarmThread(4 * DAY_MS, "agent"),
          settings: workOn,
        });
        expect(yield* harness.sweepOnce).toEqual({ commands: [], recordReads: 1 });
      }),
    ),
  );

  it.effect("falls back to upstream settlement when thread records cannot be read", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW_MS);
        const harness = yield* makeHarness({
          ...keptWarmThread(4 * DAY_MS),
          settings: workOn,
          recordsFail: true,
        });
        expect(yield* harness.sweepOnce).toEqual({ commands: [], recordReads: 1 });
      }),
    ),
  );
});
