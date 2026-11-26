import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Digest from "effect/crypto/Digest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import WorkflowsMigration from "../persistence/Migrations/110_Workflows.ts";
import * as WorkflowAgentLinks from "./WorkflowAgentLinks.ts";
import { WorkflowAgents, linkedStepMarker } from "./WorkflowAgents.ts";
import * as WorkflowStore from "./WorkflowStore.ts";
import { settleTurn } from "./workflowAgent.ts";

const isFailure = Schema.is(OrchestratorMcpFailure);
const here = EnvironmentId.make("env-a");
const journal = (filename: string) =>
  NodeSqliteClient.layer({
    filename,
    readerWorker: false,
    writerWorker: false,
    checkpointWorker: false,
  });
const migratedJournal = (filename: string, prepared = true) =>
  Layer.effectDiscard(
    prepared
      ? WorkflowsMigration.pipe(
          Effect.andThen(WorkflowStore.make),
          Effect.flatMap((store) => store.mark("run-1", linkedStepMarker("write"))),
        )
      : WorkflowsMigration,
  ).pipe(Layer.provideMerge(journal(filename)));

const journalFile = <A, E, R>(use: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-agent-links-" });
    return yield* use(path.join(directory, "journal.sqlite"));
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped);

interface Call {
  readonly environmentId: string;
  readonly tool: string;
  readonly arguments: Record<string, unknown>;
  readonly runtimeMode: string;
  readonly expectedGeneration: string | undefined;
  readonly origin: { readonly environmentId: string } | undefined;
}

/** The other environment, played by the test: each tool answers from a list, in order. */
const peer = (
  answers: Record<string, ReadonlyArray<unknown>>,
  options: {
    readonly enabled?: boolean;
    readonly prepared?: boolean;
    readonly beforeAnswer?: (tool: string) => Effect.Effect<void>;
  } = {},
) => {
  const calls: Array<Call> = [];
  const left = new Map(Object.entries(answers).map(([tool, list]) => [tool, [...list]]));
  let generation = "generation-1";
  const peerLayer = Layer.mergeAll(
    Layer.succeed(
      PeerMcpClient.PeerMcpClient,
      PeerMcpClient.PeerMcpClient.of({
        call: (input) =>
          Effect.suspend(() => {
            calls.push({
              environmentId: input.environmentId,
              tool: input.tool,
              arguments: input.arguments,
              runtimeMode: input.limits.runtimeMode,
              expectedGeneration: input.expectedGeneration,
              origin: input.origin,
            });
            if (input.expectedGeneration !== undefined && input.expectedGeneration !== generation)
              return Effect.fail(
                new OrchestratorMcpFailure({ code: "invalid_request", message: "Link changed" }),
              );
            const answer = left.get(input.tool)?.shift();
            assert.isDefined(answer, `Unexpected call to ${input.tool}`);
            return Effect.andThen(
              options.beforeAnswer?.(input.tool) ?? Effect.void,
              isFailure(answer) ? Effect.fail(answer) : Effect.succeed(answer as never),
            );
          }),
      }),
    ),
    Layer.mock(PeerLinks.PeerLinks)({
      enabled: Effect.succeed(options.enabled ?? true),
      currentGeneration: () => Effect.sync(() => Option.some(generation)),
    }),
    Digest.layerWebCrypto,
  );
  return {
    calls,
    layer: Layer.merge(peerLayer, migratedJournal(":memory:", options.prepared)),
    rePair: (nextGeneration: string) => {
      generation = nextGeneration;
    },
    // The file is retained by the test's scope; each use opens and closes its own SQLite client.
    restart: (filename: string, nextGeneration: string, initialize = false) => {
      generation = nextGeneration;
      return Layer.merge(
        peerLayer,
        initialize ? migratedJournal(filename, options.prepared) : journal(filename),
      );
    },
  };
};

const input = {
  key: "run-1/write/0",
  origin: { runId: "run-1", workflow: "fix", step: "write" },
  title: "fix: write",
  prompt: "Write the fix.",
  outputSchema: { type: "object" },
  model: undefined,
  project: "/work/app",
  environment: "env-b",
  thread: undefined,
};
const run = (status: string) => ({ recentRuns: [{ runId: "turn-1", status }], items: [] });
const waited = { threadId: "thread-1", runId: "turn-1", status: "idle" };
const timeout = new OrchestratorMcpFailure({
  code: "orchestration_error",
  message: "The linked environment timed out.",
});
const sent = {
  threadId: "thread-1",
  messageId: "message-1",
  runId: "turn-1",
  status: "queued",
  delivery: "queued",
};
const sentTurn = { threadId: "thread-1", turnId: "turn-1", environment: "env-b" };
const continueThread = Effect.flatMap(WorkflowAgentLinks.make, (links) =>
  links.start(
    { ...input, thread: "thread-1", prompt: "  Write the fix.\n" },
    "env-b",
    "workflow:send",
    here,
  ),
);
const stopTurn = Effect.flatMap(WorkflowAgentLinks.make, (links) =>
  links.interrupt({ ...sentTurn, workflowRunId: "run-1" }, "env-b"),
);
const launchInProject = Effect.flatMap(WorkflowAgentLinks.make, (links) =>
  links.start({ ...input, project: "p-app" }, "env-b", "workflow:launch", here),
);
const launched = { threadId: "thread-1", runId: "turn-1" };
const readTurn = (quiet?: Duration.Duration) =>
  Effect.flatMap(WorkflowAgentLinks.make, (links) =>
    links.result(sentTurn, "env-b", { workflowRunId: input.origin.runId, quiet }),
  );
const launchAndRead = (quiet?: Duration.Duration) =>
  Effect.andThen(launchInProject, readTurn(quiet));

const assistant = (position: number, runId: string, text: string) => ({
  type: "assistant_message",
  itemId: `item-${position}`,
  position,
  runId,
  text,
  textTruncated: false,
});
const page = (
  items: ReadonlyArray<ReturnType<typeof assistant>>,
  earlierPosition?: number | null,
  hasMore = false,
) => ({
  recentRuns: [],
  items,
  earlierPosition,
  hasMore,
  nextPosition: items.at(-1)?.position ?? null,
});
const readReply = (...pages: ReadonlyArray<unknown>) => {
  const there = peer({
    t3_thread_launch: [launched],
    t3_thread_wait: [{ ...waited, timedOut: false }],
    t3_thread_read: [run("completed"), ...pages],
  });
  return Effect.gen(function* () {
    const result = yield* launchAndRead().pipe(Effect.provide(there.layer));
    assert.isTrue(there.calls.every((call) => call.expectedGeneration === "generation-1"));
    return { result, reads: there.calls.slice(3).map((call) => call.arguments) };
  });
};
const messageRead = { threadId: "thread-1", view: "messages", limit: 100 };

it.effect("starts a thread there by the project's folder, with the step's key as the request", () =>
  Effect.gen(function* () {
    const there = peer({
      t3_project_list: [
        { projects: [{ id: "p-other", workspaceRoot: "/work/other" }], nextCursor: 1 },
        { projects: [{ id: "p-app", workspaceRoot: "/work/app" }], nextCursor: null },
      ],
      t3_thread_launch: [{ threadId: "thread-1", runId: "turn-1" }],
    });
    const options = {
      workspace: { type: "worktree", baseRef: "origin/main", startFromOrigin: true },
      runtimeMode: "auto-accept-edits",
    } as const;
    const [turn, continued] = yield* Effect.provide(
      Effect.flatMap(WorkflowAgentLinks.make, (links) =>
        Effect.all([
          links.start(
            { ...input, launch: options },
            "env-b",
            "workflow:k",
            EnvironmentId.make("env-a"),
          ),
          // A thread keeps how it started.
          links
            .start(
              { ...input, thread: "thread-1", launch: options },
              "env-b",
              "workflow:c",
              EnvironmentId.make("env-a"),
            )
            .pipe(Effect.flip),
        ]),
      ),
      there.layer,
    );
    assert.deepStrictEqual(turn, { threadId: "thread-1", turnId: "turn-1", environment: "env-b" });
    assert.include(continued.message, "A continued `thread` keeps how it started");
    const launch = there.calls.at(-1);
    assert.deepStrictEqual(launch?.arguments, {
      projectId: "p-app",
      title: "fix: write",
      runtimeMode: "auto-accept-edits",
      workspaceStrategy: { type: "worktree", baseRef: "origin/main", startFromOrigin: true },
      message: "Write the fix.",
      outputSchema: { type: "object" },
      clientRequestId: "workflow:k",
      workflowRun: { environmentId: "env-a", runId: "run-1", workflow: "fix", step: "write" },
    });
    // The link's own access is the ceiling, not a mode narrower than it.
    assert.equal(launch?.runtimeMode, "full-access");
    assert.deepStrictEqual(launch?.origin, {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "env-a",
    });
    assert.deepStrictEqual(
      there.calls.map((call) => call.expectedGeneration),
      ["generation-1", "generation-1", "generation-1"],
    );
  }),
);

it.effect("registers a folder that is not a project there yet", () =>
  Effect.gen(function* () {
    for (const [project, title] of [
      [input.project, "app"],
      ["/work/team\\api", "team\\api"],
      ["C:\\work\\app", "app"],
    ]) {
      const there = peer({
        t3_project_list: [{ projects: [], nextCursor: null }],
        t3_project_create: [{ id: "p-new", workspaceRoot: project }],
        t3_thread_launch: [{ threadId: "thread-1", runId: "turn-1" }],
      });
      yield* Effect.provide(
        Effect.flatMap(WorkflowAgentLinks.make, (links) =>
          links.start({ ...input, project }, "env-b", "workflow:k", EnvironmentId.make("env-a")),
        ),
        there.layer,
      );
      assert.deepStrictEqual(
        there.calls.map((call) => [call.tool, call.arguments.projectId ?? call.arguments.title]),
        [
          ["t3_project_list", undefined],
          ["t3_project_create", title],
          ["t3_thread_launch", "p-new"],
        ],
      );
    }
  }),
);

it.effect("finds a Windows folder there however its separators are written", () =>
  Effect.gen(function* () {
    const there = peer({
      t3_project_list: [
        { projects: [{ id: "p-app", workspaceRoot: "C:\\work\\app" }], nextCursor: null },
      ],
      t3_thread_launch: [{ threadId: "thread-1", runId: "turn-1" }],
    });
    yield* Effect.provide(
      Effect.flatMap(WorkflowAgentLinks.make, (links) =>
        links.start(
          { ...input, project: "C:/work/app" },
          "env-b",
          "workflow:k",
          EnvironmentId.make("env-a"),
        ),
      ),
      there.layer,
    );
    assert.equal(there.calls.at(-1)?.arguments.projectId, "p-app");
  }),
);

it.effect("does not start anything while linked environments is off", () =>
  Effect.gen(function* () {
    const there = peer({}, { enabled: false });
    const error = yield* Effect.provide(
      Effect.flatMap(WorkflowAgentLinks.make, (links) =>
        links.start(input, "env-b", "workflow:k", EnvironmentId.make("env-a")),
      ),
      there.layer,
    ).pipe(Effect.flip);
    assert.match(error.message, /Linked environments is off/);
    assert.deepStrictEqual(there.calls, []);
  }),
);

it.effect("sends a queued structured request with its captured link and authenticated origin", () =>
  Effect.gen(function* () {
    const there = peer({ t3_thread_send: [sent] });
    assert.deepStrictEqual(yield* continueThread.pipe(Effect.provide(there.layer)), sentTurn);
    assert.deepStrictEqual(there.calls, [
      {
        environmentId: "env-b",
        tool: "t3_thread_send",
        expectedGeneration: "generation-1",
        origin: { schemaVersion: "t3.linked-message-origin/v1", environmentId: "env-a" },
        runtimeMode: "full-access",
        arguments: {
          threadId: "thread-1",
          message: "Write the fix.",
          mode: "queue",
          outputSchema: { type: "object" },
          clientRequestId: "workflow:send",
        },
      },
    ]);
  }),
);

it.effect("recovers the exact SEND receipt after reopening the journal without sending again", () =>
  journalFile((filename) =>
    Effect.gen(function* () {
      const requestDigest = "0e70f82f53200a18ea303e414ce4c8adcaf1a095312a612c0455869d77fcf24a";
      const receipt = {
        version: "jones.remote-followup-receipt/v1",
        environmentId: "env-b",
        threadId: "thread-1",
        clientRequestId: "workflow:send",
        requestDigest,
      };
      const there = peer({
        t3_thread_send: [timeout],
        t3_thread_send_receipt_lookup: [
          { ...receipt, outcome: "unknown", sent: null },
          { ...receipt, outcome: "accepted", sent },
        ],
        t3_thread_wait: [{ ...waited, timedOut: false }],
        t3_thread_read: [run("completed"), page([assistant(1, "turn-1", "Done.")])],
      });
      const lost = yield* continueThread.pipe(
        Effect.provide(there.restart(filename, "generation-1", true)),
        Effect.flip,
      );
      assert.equal(lost._tag, "WorkflowAgentMutationUncertain");
      assert.include(lost.message, "Inspect the original operation");
      // A new generation cannot read the old namespace, even if the key is unchanged.
      const replaced = yield* continueThread.pipe(
        Effect.provide(there.restart(filename, "generation-2")),
        Effect.flip,
      );
      assert.equal(replaced._tag, "WorkflowAgentMutationUncertain");
      const replacedAgain = yield* continueThread.pipe(
        Effect.provide(there.restart(filename, "generation-2")),
        Effect.flip,
      );
      assert.equal(replacedAgain._tag, "WorkflowAgentMutationUncertain");
      assert.lengthOf(there.calls, 2);
      assert.deepStrictEqual(
        yield* continueThread.pipe(Effect.provide(there.restart(filename, "generation-1"))),
        sentTurn,
      );
      assert.deepStrictEqual(
        there.calls.map(({ tool, expectedGeneration }) => [tool, expectedGeneration]),
        [
          ["t3_thread_send", "generation-1"],
          ["t3_thread_send_receipt_lookup", "generation-1"],
          ["t3_thread_send_receipt_lookup", "generation-1"],
        ],
      );
      assert.deepStrictEqual(there.calls.at(-1)?.arguments, {
        threadId: "thread-1",
        clientRequestId: "workflow:send",
        requestDigest,
      });
      const result = yield* readTurn().pipe(
        Effect.provide(there.restart(filename, "generation-1")),
      );
      assert.equal(result.reply, "Done.");
      assert.isTrue(there.calls.every((call) => call.expectedGeneration === "generation-1"));
    }),
  ),
);

it.effect("holds a lost launch across re-pairing and a reopened journal", () =>
  journalFile((filename) =>
    Effect.gen(function* () {
      const there = peer({ t3_thread_launch: [timeout] });
      const lost = yield* launchInProject.pipe(
        Effect.provide(there.restart(filename, "generation-1", true)),
        Effect.flip,
      );
      assert.equal(lost._tag, "WorkflowAgentMutationUncertain");
      assert.include(lost.message, "workflow:launch");
      const replay = yield* launchInProject.pipe(
        Effect.provide(there.restart(filename, "generation-2")),
        Effect.flip,
      );
      assert.equal(replay._tag, "WorkflowAgentMutationUncertain");
      assert.deepStrictEqual(
        there.calls.map(({ tool, expectedGeneration }) => [tool, expectedGeneration]),
        [["t3_thread_launch", "generation-1"]],
      );
    }),
  ),
);

it.effect("holds an unconfirmed project registration even when the folder later appears", () =>
  journalFile((filename) =>
    Effect.gen(function* () {
      const there = peer({
        t3_project_list: [
          { projects: [], nextCursor: null },
          { projects: [{ id: "p-new", workspaceRoot: "/work/app" }], nextCursor: null },
        ],
        t3_project_create: [timeout],
      });
      const start = Effect.flatMap(WorkflowAgentLinks.make, (links) =>
        links.start(input, "env-b", "workflow:register", here),
      );
      const lost = yield* start.pipe(
        Effect.provide(there.restart(filename, "generation-1", true)),
        Effect.flip,
      );
      assert.include(lost.message, "project registration at /work/app");
      const replay = yield* start.pipe(
        Effect.provide(there.restart(filename, "generation-2")),
        Effect.flip,
      );
      assert.equal(replay._tag, "WorkflowAgentMutationUncertain");
      assert.deepStrictEqual(
        there.calls.map(({ tool, expectedGeneration }) => [tool, expectedGeneration]),
        [
          ["t3_project_list", "generation-1"],
          ["t3_project_create", "generation-1"],
          ["t3_project_list", "generation-2"],
        ],
      );
    }),
  ),
);

it.effect("holds a lost turn interruption across re-pairing and a reopened journal", () =>
  journalFile((filename) =>
    Effect.gen(function* () {
      const there = peer({ t3_thread_launch: [launched], t3_thread_interrupt: [timeout] });
      const lost = yield* launchInProject.pipe(
        Effect.andThen(stopTurn),
        Effect.provide(there.restart(filename, "generation-1", true)),
        Effect.flip,
      );
      assert.equal(lost._tag, "WorkflowAgentMutationUncertain");
      assert.include(lost.message, "turn turn-1");
      const replay = yield* stopTurn.pipe(
        Effect.provide(there.restart(filename, "generation-2")),
        Effect.flip,
      );
      assert.equal(replay._tag, "WorkflowAgentMutationUncertain");
      assert.lengthOf(there.calls, 2);
      assert.deepInclude(there.calls[1], {
        tool: "t3_thread_interrupt",
        expectedGeneration: "generation-1",
      });
      assert.deepInclude(there.calls[1]?.arguments, { threadId: "thread-1", runId: "turn-1" });
    }),
  ),
);

it.effect(
  "keeps an accepted launch on its original pairing even if the reply arrives after re-pairing",
  () =>
    journalFile((filename) =>
      Effect.gen(function* () {
        const dispatched = yield* Deferred.make<void>();
        const reply = yield* Deferred.make<void>();
        const there = peer(
          { t3_thread_launch: [launched] },
          {
            beforeAnswer: () =>
              Effect.andThen(Deferred.succeed(dispatched, undefined), Deferred.await(reply)),
          },
        );
        const starting = yield* Effect.forkChild(
          launchInProject.pipe(Effect.provide(there.restart(filename, "generation-1", true))),
        );
        yield* Deferred.await(dispatched);
        there.rePair("generation-2");
        yield* Deferred.succeed(reply, undefined);
        assert.deepStrictEqual(yield* Fiber.join(starting), sentTurn);

        const [lost, unconfirmed] = yield* Effect.all([
          readTurn().pipe(Effect.flip),
          stopTurn.pipe(Effect.flip),
        ]).pipe(Effect.provide(there.restart(filename, "generation-2")));
        assert.match(lost.reason, /changed after the workflow turn was accepted/);
        assert.equal(unconfirmed._tag, "WorkflowAgentMutationUncertain");
        assert.deepStrictEqual(
          there.calls.map(({ tool, expectedGeneration }) => [tool, expectedGeneration]),
          [["t3_thread_launch", "generation-1"]],
        );
      }),
    ),
);

it.effect.each(["generation-1", "generation-2"] as const)(
  "times out an accepted turn with %s present",
  (generation) =>
    Effect.gen(function* () {
      const waiting = yield* Deferred.make<void>();
      const there = peer(
        {
          t3_thread_launch: [launched],
          t3_thread_wait: [waited],
          t3_thread_interrupt: [
            { threadId: "thread-1", runId: "turn-1", status: "interrupt_requested" },
          ],
        },
        {
          beforeAnswer: (tool) =>
            tool === "t3_thread_wait"
              ? Effect.andThen(Deferred.succeed(waiting, undefined), Effect.never)
              : Effect.void,
        },
      );
      yield* Effect.gen(function* () {
        const links = yield* WorkflowAgentLinks.make;
        const turn = yield* launchInProject;
        const watching = yield* Effect.forkChild(
          settleTurn({
            agent: {
              turn,
              step: input.origin.step,
              workflowRunId: input.origin.runId,
              startedAt: yield* Clock.currentTimeMillis,
              timeoutMs: 1_000,
              owesOutput: false,
              onFailure: "return",
            },
          }).pipe(
            Effect.provideService(WorkflowAgents, {
              start: () => Effect.die("Unexpected workflow start"),
              result: (turn, options) => links.result(turn, "env-b", options),
              interrupt: (turn) => links.interrupt(turn, "env-b"),
            }),
          ),
        );
        yield* Deferred.await(waiting);
        there.rePair(generation);
        yield* TestClock.adjust("1 second");
        const end = yield* Fiber.join(watching);
        const stops = there.calls.filter(({ tool }) => tool === "t3_thread_interrupt");
        if (generation === "generation-1") {
          assert.propertyVal("result" in end ? end.result : {}, "ending", "stopped");
          assert.lengthOf(stops, 1);
          assert.equal(stops[0]?.expectedGeneration, generation);
          assert.deepInclude(stops[0]?.arguments, { threadId: "thread-1", runId: "turn-1" });
        } else {
          assert.propertyVal(end, "held", true);
          assert.deepStrictEqual(stops, []);
        }
      }).pipe(Effect.provide(there.layer));
    }),
);

it.effect(
  "requires generation evidence from the owning run, even when a legacy turn has a marker",
  () =>
    Effect.gen(function* () {
      const there = peer({ t3_thread_launch: [launched] });
      yield* Effect.gen(function* () {
        const store = yield* WorkflowStore.make;
        const legacyRunId = "legacy-run";
        // The old accepted marker stored only SHA-256 of the environment, thread and turn tuple.
        yield* store.mark(
          legacyRunId,
          "workflow-agent/turn/v1/eac56a4ea740d8a84dff170c47b412e88eae658955ee8f38234a988c9de79bc6",
        );
        yield* launchInProject;
        const links = yield* WorkflowAgentLinks.make;
        for (const workflowRunId of [legacyRunId, undefined]) {
          const lost = yield* links.result(sentTurn, "env-b", { workflowRunId }).pipe(Effect.flip);
          assert.match(lost.reason, /Cannot prove the original pairing/);
          const stop = yield* links
            .interrupt({ ...sentTurn, workflowRunId }, "env-b")
            .pipe(Effect.flip);
          assert.equal(stop._tag, "WorkflowAgentMutationUncertain");
        }
      }).pipe(Effect.provide(there.layer));
      assert.deepStrictEqual(
        there.calls.map(({ tool }) => tool),
        ["t3_thread_launch"],
      );
    }),
);

it.effect("holds a legacy step or turn when the original write evidence was never journaled", () =>
  Effect.gen(function* () {
    const there = peer({}, { prepared: false });
    const errors = yield* Effect.all([
      launchInProject.pipe(Effect.flip),
      stopTurn.pipe(Effect.flip),
    ]).pipe(Effect.provide(there.layer));
    assert.deepStrictEqual(
      errors.map(({ _tag }) => _tag),
      ["WorkflowAgentMutationUncertain", "WorkflowAgentMutationUncertain"],
    );
    assert.deepStrictEqual(there.calls, []);
  }),
);

it.effect("waits in pieces, rides out a dropped route, and returns the output and reply", () =>
  Effect.gen(function* () {
    const there = peer({
      t3_thread_launch: [launched],
      t3_thread_wait: [
        { ...waited, timedOut: true },
        new OrchestratorMcpFailure({ code: "orchestration_error", message: "unreachable" }),
        { ...waited, timedOut: false, output: { branch: "fix/1" } },
      ],
      t3_thread_read: [
        run("completed"),
        {
          recentRuns: [],
          hasMore: false,
          nextPosition: null,
          items: [
            { type: "assistant_message", runId: "turn-0", text: "earlier", textTruncated: false },
            {
              type: "assistant_message",
              runId: "turn-1",
              itemId: "item-9",
              text: "Done, ",
              textTruncated: true,
              nextTextOffset: 6,
            },
          ],
        },
        { recentRuns: [], items: [{ text: "pushed.", textTruncated: false }] },
      ],
    });
    const result = yield* launchAndRead().pipe(
      Effect.provide(there.layer),
      Effect.raceFirst(Effect.forever(TestClock.adjust("1 second"))),
    );
    assert.deepStrictEqual(result, {
      ending: "completed",
      detail: "",
      reply: "Done, pushed.",
      output: { branch: "fix/1" },
    });
    assert.equal(there.calls.filter((call) => call.tool === "t3_thread_wait").length, 3);
    assert.isTrue(there.calls.every((call) => call.expectedGeneration === "generation-1"));
  }),
);

it.effect("returns the whole last reply from an older page of a linked thread", () =>
  Effect.gen(function* () {
    const later = {
      ...assistant(4, "turn-1", "Latest "),
      textTruncated: true,
      nextTextOffset: 7,
    };
    const { result, reads } = yield* readReply(
      page([assistant(9, "turn-3", "newest turn")], 9),
      page([assistant(6, "turn-2", "another turn")], 6, true),
      page([assistant(3, "turn-1", "earlier draft"), later], 3, true),
      page([{ ...later, text: "complete reply", textTruncated: false }]),
    );
    assert.deepStrictEqual(result, {
      ending: "completed",
      detail: "",
      reply: "Latest complete reply",
    });
    assert.deepStrictEqual(reads, [
      messageRead,
      { ...messageRead, beforePosition: 9 },
      { ...messageRead, beforePosition: 6 },
      { threadId: "thread-1", itemId: "item-4", textOffset: 7, maxCharsPerItem: 50_000 },
    ]);
  }),
);

it.effect("keeps the latest matching reply when paging a legacy peer forward", () =>
  Effect.gen(function* () {
    for (const earlierPosition of [undefined, null]) {
      const { result, reads } = yield* readReply(
        page([assistant(2, "turn-1", "draft")], earlierPosition, true),
        page([assistant(4, "turn-1", "final")], earlierPosition, true),
        page([assistant(6, "turn-2", "other turn")], earlierPosition),
      );
      assert.strictEqual(result.reply, "final");
      assert.deepStrictEqual(reads, [
        messageRead,
        { ...messageRead, afterPosition: 2 },
        { ...messageRead, afterPosition: 4 },
      ]);
    }
  }),
);

it.effect("keeps the reply when a legacy peer's forward cursor stops advancing", () =>
  Effect.gen(function* () {
    for (const position of [2, 1]) {
      const { result, reads } = yield* readReply(
        page([assistant(2, "turn-1", "Done.")], undefined, true),
        page([assistant(position, "turn-2", "other turn")], undefined, true),
      );
      assert.strictEqual(result.reply, "Done.");
      assert.deepStrictEqual(reads, [messageRead, { ...messageRead, afterPosition: 2 }]);
    }
  }),
);

it.effect(
  "returns an empty reply when older pages are exhausted or their cursor stops advancing",
  () =>
    Effect.gen(function* () {
      for (const earlierPosition of [null, 3, 4]) {
        const { result, reads } = yield* readReply(
          page([assistant(3, "turn-2", "other turn")], 3),
          page([assistant(1, "turn-0", "another turn")], earlierPosition, true),
        );
        assert.deepStrictEqual(result, { ending: "completed", detail: "", reply: "" });
        assert.deepStrictEqual(reads, [messageRead, { ...messageRead, beforePosition: 3 }]);
      }
    }),
);

it.effect("a turn someone stopped there is stopped, and a refused link fails at once", () =>
  Effect.gen(function* () {
    const stopped = peer({
      t3_thread_wait: [{ ...waited, timedOut: false }],
      t3_thread_read: [run("interrupted")],
      t3_thread_launch: [launched],
      t3_thread_interrupt: [{ threadId: "thread-1", runId: "turn-1", status: "interrupted" }],
    });
    const [result, interrupted] = yield* launchInProject.pipe(
      Effect.andThen(Effect.all([readTurn(), stopTurn])),
      Effect.provide(stopped.layer),
    );
    assert.deepStrictEqual(result, { ending: "stopped", detail: "interrupted", reply: "" });
    // Stopping names the step's turn, so a person's turn on that thread keeps going.
    assert.equal(interrupted, "ended");
    assert.deepInclude(stopped.calls.at(-1)?.arguments, { threadId: "thread-1", runId: "turn-1" });

    const revoked = peer({
      t3_thread_launch: [launched],
      t3_thread_wait: [
        new OrchestratorMcpFailure({ code: "capability_denied", message: "link revoked" }),
      ],
    });
    const error = yield* launchInProject.pipe(
      Effect.andThen(readTurn().pipe(Effect.flip)),
      Effect.provide(revoked.layer),
    );
    assert.match(error.reason, /link revoked/);
    assert.equal(revoked.calls.filter((call) => call.tool === "t3_thread_wait").length, 1);
  }),
);

it.effect("a turn that ended without its result is waited past until a later turn gives it", () =>
  Effect.gen(function* () {
    const there = peer({
      t3_thread_launch: [launched],
      t3_thread_wait: [
        // The turn ended. The agent is waiting to be woken, and has submitted nothing.
        { ...waited, timedOut: false },
        // Quiet for five minutes, in pieces. A turn that woke in between is under way...
        ...Array.from({ length: 6 }, () => ({ ...waited, timedOut: true })),
        // ...so the wait starts over, and that turn submits.
        { ...waited, timedOut: false, output: { head: "abc" } },
      ],
      t3_thread_read: [
        run("completed"),
        { recentRuns: [{ runId: "turn-2", status: "running" }], items: [] },
        { recentRuns: [], hasMore: false, nextPosition: null, items: [] },
      ],
    });
    const result = yield* launchAndRead(Duration.minutes(5)).pipe(
      Effect.provide(there.layer),
      Effect.raceFirst(Effect.forever(TestClock.adjust("1 second"))),
    );
    assert.deepStrictEqual(result.output, { head: "abc" });
    const waits = there.calls.filter((call) => call.tool === "t3_thread_wait");
    assert.isTrue(waits.slice(1).every((call) => call.arguments.untilOutput === true));
    assert.isTrue(there.calls.every((call) => call.expectedGeneration === "generation-1"));
  }),
);

it.effect("a thread that stays quiet gives the turn back without a result", () =>
  Effect.gen(function* () {
    const there = peer({
      t3_thread_launch: [launched],
      t3_thread_wait: [
        { ...waited, timedOut: false },
        ...Array.from({ length: 6 }, () => ({ ...waited, timedOut: true })),
      ],
      t3_thread_read: [
        run("completed"),
        // Nothing has run since the turn the step knows.
        run("completed"),
        { recentRuns: [], hasMore: false, nextPosition: null, items: [] },
      ],
    });
    const result = yield* launchAndRead(Duration.minutes(5)).pipe(
      Effect.provide(there.layer),
      Effect.raceFirst(Effect.forever(TestClock.adjust("1 second"))),
    );
    assert.deepStrictEqual(result, { ending: "completed", detail: "", reply: "" });
  }),
);
