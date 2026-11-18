import { assert, it } from "@effect/vitest";
import {
  CommandId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { makeUnreleasedStartProof, makeUnsentStartProof } from "./UnsentStartProof.ts";

type Ports = Parameters<typeof makeUnsentStartProof>[0];
const threadId = ThreadId.make("unsent-original");
const runId = RunId.make("unsent-original-run");
const attemptId = RunAttemptId.make("unsent-original-attempt");
const commandId = CommandId.make("original-command");
const run = {
  id: runId,
  threadId,
  activeAttemptId: attemptId,
  startedAt: null,
  userMessageId: "original-input",
} as unknown as OrchestrationV2Run;
const attempt = {
  id: attemptId,
  runId,
  attemptOrdinal: 1,
  reason: "initial",
  startedAt: null,
} as unknown as OrchestrationV2RunAttempt;

function fixture(
  change: {
    readonly claimed?: boolean;
    readonly crossed?: boolean;
    readonly native?: boolean;
    readonly missing?: boolean;
    readonly receiptMismatch?: boolean;
    readonly changedBeforeReadback?: boolean;
    readonly queuePromotion?: boolean;
  } = {},
) {
  const original = {
    id: `effect:${commandId}:provider-turn.start:${runId}`,
    commandId,
    threadId,
    request: { type: "provider-turn.start", runId },
    status: "cancelled",
    leaseOwner: null,
    attemptCount: change.claimed ? 1 : 0,
    payloadIdentity: "original-payload",
    ...(change.native ? { nativeCreationExecutionReference: {} } : {}),
  };
  const event = (sequence: number, type: string, payload: unknown) => ({
    sequence,
    commandId,
    event: { type, threadId, runId, payload },
  });
  return {
    outbox: {
      listByThreadId: () => Effect.succeed(change.missing ? [] : [original]),
      get: () =>
        Effect.succeedSome(
          change.changedBeforeReadback ? { ...original, attemptCount: 1 } : original,
        ),
    },
    receipts: {
      getByCommandId: () =>
        Effect.succeedSome({
          status: "accepted",
          commandType: change.queuePromotion ? "queue.start" : "message.dispatch",
          threadId,
          resultSequence: change.receiptMismatch ? 3 : 2,
        }),
    },
    sink: {
      readByCommandId: () =>
        Stream.fromIterable(
          change.queuePromotion
            ? [event(2, "run.updated", { ...run, status: "starting" })]
            : [event(1, "run.created", run), event(2, "run-attempt.created", attempt)],
        ),
    },
    refusals: {
      readPrefix: () => Effect.succeed({ state: change.crossed ? "crossed" : "unknown" }),
    },
  } as unknown as Ports;
}

it.effect(
  "only original zero-claim cancellation and its accepted birth receipt certify ordinary unsent input",
  () =>
    Effect.gen(function* () {
      assert.isTrue(yield* makeUnsentStartProof(fixture())(run, [attempt]));
      for (const change of [
        { claimed: true },
        { crossed: true },
        { native: true },
        { missing: true },
        { receiptMismatch: true },
        { changedBeforeReadback: true },
      ])
        assert.isFalse(yield* makeUnsentStartProof(fixture(change))(run, [attempt]));
      assert.isFalse(
        yield* makeUnsentStartProof(fixture())({ ...run, startedAt: {} as never }, [attempt]),
      );
      assert.isFalse(
        yield* makeUnsentStartProof(fixture())({ ...run, manualContinuationOfRunId: runId }, [
          attempt,
        ]),
      );
      assert.isFalse(
        yield* makeUnsentStartProof(fixture())(run, [
          attempt,
          { ...attempt, id: RunAttemptId.make("retry") },
        ]),
      );
    }),
);

it.effect("certifies a queued promotion only with its accepted zero-claim start", () =>
  Effect.gen(function* () {
    assert.isTrue(yield* makeUnsentStartProof(fixture({ queuePromotion: true }))(run, [attempt]));
    assert.isFalse(
      yield* makeUnsentStartProof(fixture({ queuePromotion: true, claimed: true }))(run, [attempt]),
    );
    assert.isFalse(
      yield* makeUnsentStartProof(fixture({ queuePromotion: true, receiptMismatch: true }))(run, [
        attempt,
      ]),
    );
  }),
);

it.effect(
  "certifies unreleased admission only with retained birth evidence and no start effect",
  () =>
    Effect.gen(function* () {
      const preparing = { ...run, status: "preparing", completedAt: null } as const;
      const queued = { ...preparing, status: "queued" } as const;
      const pending = { ...attempt, status: "pending", completedAt: null } as const;
      const absent = fixture({ missing: true });
      const provedBirth = () => Effect.succeed(true);
      const prove = makeUnreleasedStartProof({ ...absent, readBirth: provedBirth });

      assert.isTrue(yield* prove(preparing, [pending]));
      assert.isTrue(yield* prove(queued, [pending]));
      assert.isFalse(yield* prove({ ...preparing, status: "starting" }, [pending]));
      assert.isFalse(yield* prove(preparing, [{ ...pending, attemptOrdinal: 2 }]));
      const noBirth = makeUnreleasedStartProof({
        ...absent,
        readBirth: () => Effect.succeed(false),
      });
      assert.isFalse(yield* noBirth(preparing, [pending]));
      const withStart = makeUnreleasedStartProof({ ...fixture(), readBirth: provedBirth });
      assert.isFalse(yield* withStart(preparing, [pending]));
      const crossed = makeUnreleasedStartProof({
        ...fixture({ missing: true, crossed: true }),
        readBirth: provedBirth,
      });
      assert.isFalse(yield* crossed(preparing, [pending]));
    }),
);
