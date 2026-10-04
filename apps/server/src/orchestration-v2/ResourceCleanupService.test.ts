import { assert, it } from "@effect/vitest";
import { CommandId, EventId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import type * as EventSink from "./EventSink.ts";
import { makeOwnedResourceCleanup } from "./ResourceCleanupService.ts";

const threadId = ThreadId.make("owned-resource-original");
const ownerBirth = { kind: "application_v2_thread_birth" as const, threadId, eventId: EventId.make("owner-birth-original"), sequence: 1 };
const binding: EventSink.LeaseCleanupTaskBindingV2 = {
  version: 2, effectId: "effect:owned-resource:terminal", threadId,
  lease: { resourcePath: "/workspace/original", leaseId: "original-lease", ownerThreadId: threadId,
    ownerIncarnation: JSON.stringify(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, 1]),
    branch: "original", acquiredAtMs: 1, renewedAtMs: 2, expiresAtMs: 300_000 },
  ownerBirth, deletion: { commandId: CommandId.make("delete-original"), eventId: EventId.make("delete-event-original"), sequence: 2 },
  task: { kind: "terminal", capture: { managerId: "original-manager", threadId, ownerBirth,
    status: "captured", managedTargetsOnly: true,
    targets: [{ threadId, terminalId: "shared-terminal-id", handleId: "original-handle", ownerBirth }] } },
  bindingSha256: "b".repeat(64), recordedAt: "2026-09-04T00:00:00.000Z",
};

it.effect.each(["closed", "observed_absent", "mismatch", "unknown"] as const)(
  "managed terminal status %s is not complete owned resource quiescence", (status) => Effect.gen(function* () {
    const captures: unknown[] = [];
    const cleanup = makeOwnedResourceCleanup({
      sink: { readLeaseCleanupTask: () => Effect.succeed(binding) },
      terminals: { closeOwnedTargets: (capture) => Effect.sync(() => {
        captures.push(capture);
        return { status, managedTargetsOnly: true as const, processExitObserved: status === "closed",
          descendantsQuiescence: "unavailable" as const, futureWakeClosure: "unavailable" as const };
      }) },
    });
    const result = yield* cleanup.cleanupOwnedTerminals(binding);
    assert.deepEqual(result.outcome, { taskId: binding.effectId, result: null, effect: "unknown" });
    assert.equal(result.evidence.terminalStatus, status);
    assert.equal(result.evidence.descendantsQuiescence, "unavailable");
    assert.strictEqual(captures[0], binding.task.kind === "terminal" ? binding.task.capture : null);
    if (binding.task.kind === "terminal") assert.equal(binding.task.capture.targets[0]?.handleId, "original-handle");
  }),
);

it.effect("a missing or changed durable terminal task performs no resource operation", () =>
  Effect.gen(function* () {
    for (const current of [null, { ...binding, bindingSha256: "c".repeat(64) }]) {
      let calls = 0;
      const cleanup = makeOwnedResourceCleanup({
        sink: { readLeaseCleanupTask: () => Effect.succeed(current) },
        terminals: { closeOwnedTargets: () => Effect.sync(() => {
          calls++;
          return { status: "closed" as const, managedTargetsOnly: true as const, processExitObserved: true,
            descendantsQuiescence: "unavailable" as const, futureWakeClosure: "unavailable" as const };
        }) },
      });
      assert.deepEqual((yield* cleanup.cleanupOwnedTerminals(binding)).outcome,
        { taskId: binding.effectId, result: "failed", effect: "no_effect" });
      assert.equal(calls, 0);
    }
  }),
);

it.effect("terminal interruption stays interrupted instead of becoming a cleanup receipt", () =>
  Effect.gen(function* () {
    const cleanup = makeOwnedResourceCleanup({
      sink: { readLeaseCleanupTask: () => Effect.succeed(binding) },
      terminals: { closeOwnedTargets: () => Effect.interrupt },
    });
    const result = yield* Effect.exit(cleanup.cleanupOwnedTerminals(binding));
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
  }),
);

it.effect("attachment IDs without resource generation and shared-reference proof perform no deletion", () =>
  Effect.gen(function* () {
    const attachment: EventSink.LeaseCleanupTaskBindingV2 = { ...binding,
      effectId: "effect:owned-resource:attachment", task: { kind: "attachment", attachmentIds: ["original-attachment"] } };
    const cleanup = makeOwnedResourceCleanup({
      sink: { readLeaseCleanupTask: () => Effect.succeed(attachment) },
      terminals: { closeOwnedTargets: () => Effect.die("attachment cleanup cannot close terminals") },
    });
    const result = yield* cleanup.cleanupOwnedAttachments(attachment);
    assert.deepEqual(result.outcome, { taskId: attachment.effectId, result: null, effect: "unknown" });
    assert.deepEqual(result.evidence.attachmentIds, ["original-attachment"]);
  }),
);

it.effect("terminal target capture passes the exact application birth to the existing manager", () =>
  Effect.gen(function* () {
    if (binding.task.kind !== "terminal") return yield* Effect.die("terminal fixture required");
    const capture = binding.task.capture;
    const observed: unknown[] = [];
    const cleanup = makeOwnedResourceCleanup({
      sink: { readLeaseCleanupTask: () => Effect.succeed(binding) },
      terminals: {
        captureOwnedTargets: (input) => Effect.sync(() => { observed.push(input); return capture; }),
        closeOwnedTargets: () => Effect.die("capture cannot close a resource"),
      },
    });
    assert.isDefined(cleanup.captureOwnedTerminalTargets);
    const issued = yield* cleanup.captureOwnedTerminalTargets!(ownerBirth);
    assert.strictEqual(issued, capture);
    assert.deepEqual(observed, [{ threadId, ownerBirth }]);
  }),
);
