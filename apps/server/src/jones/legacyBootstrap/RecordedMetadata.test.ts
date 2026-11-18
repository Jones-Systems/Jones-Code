import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import {
  RecordedRunJson,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as M from "./RecordedMetadata.ts";
import { RunIdentity } from "./StorageSchema.ts";

const decodeRunIdentity = Schema.decodeSync(RunIdentity);
const decodeStoredLifecycleEvent = Schema.decodeSync(RecordedStoredLifecycleEvent);

const run = Schema.decodeSync(RecordedRunJson)({
  id: "R",
  threadId: "T",
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  providerThreadId: null,
  userMessageId: "M",
  rootNodeId: null,
  activeAttemptId: null,
  status: "preparing",
  requestedAt: "2026-11-17T10:00:00.000Z",
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});
const identity = decodeRunIdentity({
  runId: "R",
  threadId: "T",
  userMessageId: "M",
  requestedAt: "2026-11-17T10:00:00.000Z",
  birthEventId: "birth",
  birthCommandId: "B",
  birthSequence: 2,
});

it("canonical bindings are order independent without changing arrays", () => {
  assert.equal(
    M.canonical({ b: { z: 2, a: 1 }, a: [3, 1] }),
    M.canonical({ a: [3, 1], b: { a: 1, z: 2 } }),
  );
  assert.notEqual(M.digest([1, 2]), M.digest([2, 1]));
});
it.effect(
  "only current mutable fields use the newer sparse state; immutable incarnation/setup conflicts refuse",
  () =>
    Effect.gen(function* () {
      const merged = yield* M.mergeRun(
        { ...run, legacyPreparationFailureKnown: false },
        { identity, metadata: { legacyPreparationFailureKnown: true } },
      );
      assert.equal(merged.legacyPreparationFailureKnown, true);
      for (const altered of [
        { ...identity, userMessageId: run.userMessageId + "other" },
        { ...identity, requestedAt: "2026-11-18T10:00:00.000Z" },
      ])
        assert.isTrue(
          Exit.isFailure(
            yield* M.mergeRun(run, {
              identity: decodeRunIdentity(altered),
              metadata: { legacyPreparationFailureKnown: true },
            }).pipe(Effect.exit),
          ),
        );
      assert.isTrue(
        Exit.isFailure(
          yield* M.mergeRun(
            { ...run, workspaceRunSetupScript: true },
            { identity, metadata: { workspaceRunSetupScript: false } },
          ).pipe(Effect.exit),
        ),
      );
    }),
);
it.effect("exact historical event metadata refuses a different private snapshot", () =>
  Effect.gen(function* () {
    const stored = decodeStoredLifecycleEvent({
      sequence: 7,
      commandId: "progress",
      event: {
        id: "event",
        threadId: "T",
        runId: "R",
        occurredAt: run.requestedAt,
        type: "run.updated",
        payload: { ...run, legacyPreparationFailureKnown: false },
      },
    });
    assert.isTrue(
      Exit.isFailure(
        yield* M.mergeEvent(stored, {
          kind: "run",
          metadata: { legacyPreparationFailureKnown: true },
        }).pipe(Effect.exit),
      ),
    );
    const same = yield* M.mergeEvent(stored, {
      kind: "run",
      metadata: { legacyPreparationFailureKnown: false },
    });
    assert.deepStrictEqual(same, stored);
  }),
);
