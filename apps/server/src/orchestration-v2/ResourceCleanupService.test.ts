import { assert, it } from "@effect/vitest";
import * as NodePath from "@effect/platform-node/NodePath";
import * as Path from "effect/Path";
import { CommandId, EventId, RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as PlatformError from "effect/PlatformError";

import * as Schema from "effect/Schema";

import * as EventSink from "./EventSink.ts";
import {
  ImportedApplicationAttachmentRetentionEvidenceV1,
  makeImportedApplicationAttachmentRetentionEvidenceV1,
} from "./ImportedApplicationAttachmentInventory.ts";
import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";
import {
  makeAttachmentNamespaceCleanup,
  makeAttachmentNamespaceScan,
  makeOwnedResourceCleanup,
} from "./ResourceCleanupService.ts";

const correlation = { workerId: "owned-resource-worker", expectedAttempt: 1 };
const threadId = ThreadId.make("owned-resource-original");
const ownerBirth = {
  kind: "application_v2_thread_birth" as const,
  threadId,
  eventId: EventId.make("owner-birth-original"),
  sequence: 1,
};
const binding: EventSink.LeaseCleanupTaskBindingV2 = {
  version: 2,
  effectId: "effect:owned-resource:terminal",
  threadId,
  lease: {
    resourcePath: "/workspace/original",
    leaseId: "original-lease",
    ownerThreadId: threadId,
    ownerIncarnation: JSON.stringify([
      "t3.orchestration-v2.thread-birth/v1",
      ownerBirth.eventId,
      1,
    ]),
    branch: "original",
    acquiredAtMs: 1,
    renewedAtMs: 2,
    expiresAtMs: 300_000,
  },
  ownerBirth,
  deletion: {
    commandId: CommandId.make("delete-original"),
    eventId: EventId.make("delete-event-original"),
    sequence: 2,
  },
  task: {
    kind: "terminal",
    capture: {
      managerId: "original-manager",
      threadId,
      ownerBirth,
      status: "captured",
      managedTargetsOnly: true,
      targets: [
        { threadId, terminalId: "shared-terminal-id", handleId: "original-handle", ownerBirth },
      ],
    },
  },
  bindingSha256: "b".repeat(64),
  recordedAt: "2026-09-04T00:00:00.000Z",
};

it.effect.each(["closed", "observed_absent", "mismatch", "unknown"] as const)(
  "managed terminal status %s preserves exact observed evidence for STORE qualification",
  (status) =>
    Effect.gen(function* () {
      const captures: unknown[] = [];
      const cleanup = makeOwnedResourceCleanup({
        sink: { readDeletionCleanupTask: () => Effect.succeed(binding) },
        terminals: {
          closeOwnedTargets: (capture) =>
            Effect.sync(() => {
              captures.push(capture);
              return {
                status,
                managedTargetsOnly: true as const,
                processExitObserved: status === "closed",
                descendantsQuiescence: "unavailable" as const,
                futureWakeClosure: "unavailable" as const,
              };
            }),
        },
      });
      const result = yield* cleanup.cleanupOwnedTerminals(binding, correlation);
      assert.deepEqual(result.outcome, {
        taskId: binding.effectId,
        result: null,
        effect: "unknown",
      });
      assert.equal(result.evidence.terminalStatus, status);
      assert.equal(result.evidence.descendantsQuiescence, "unavailable");
      assert.isDefined(result.observation);
      const observation = yield* Schema.decodeUnknownEffect(
        EventSink.ManagedTerminalDeletionObservationV1,
      )(result.observation);
      assert.equal(observation.result.status, status);
      assert.equal(observation.result.processExitObserved, status === "closed");
      assert.equal(observation.workerId, correlation.workerId);
      assert.equal(observation.expectedAttempt, correlation.expectedAttempt);
      assert.equal(observation.bindingSha256, binding.bindingSha256);
      assert.deepEqual(
        observation.capture,
        binding.task.kind === "terminal" ? binding.task.capture : null,
      );
      assert.strictEqual(
        captures[0],
        binding.task.kind === "terminal" ? binding.task.capture : null,
      );
      if (binding.task.kind === "terminal")
        assert.equal(binding.task.capture.targets[0]?.handleId, "original-handle");
    }),
);

it.effect("a missing or changed durable terminal task performs no resource operation", () =>
  Effect.gen(function* () {
    for (const current of [null, { ...binding, bindingSha256: "c".repeat(64) }]) {
      let calls = 0;
      const cleanup = makeOwnedResourceCleanup({
        sink: { readDeletionCleanupTask: () => Effect.succeed(current) },
        terminals: {
          closeOwnedTargets: () =>
            Effect.sync(() => {
              calls++;
              return {
                status: "closed" as const,
                managedTargetsOnly: true as const,
                processExitObserved: true,
                descendantsQuiescence: "unavailable" as const,
                futureWakeClosure: "unavailable" as const,
              };
            }),
        },
      });
      assert.deepEqual((yield* cleanup.cleanupOwnedTerminals(binding, correlation)).outcome, {
        taskId: binding.effectId,
        result: "failed",
        effect: "no_effect",
      });
      assert.equal(calls, 0);
    }
  }),
);

it.effect("terminal interruption stays interrupted instead of becoming a cleanup receipt", () =>
  Effect.gen(function* () {
    const cleanup = makeOwnedResourceCleanup({
      sink: { readDeletionCleanupTask: () => Effect.succeed(binding) },
      terminals: { closeOwnedTargets: () => Effect.interrupt },
    });
    const result = yield* Effect.exit(cleanup.cleanupOwnedTerminals(binding, correlation));
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
  }),
);

it.effect(
  "attachment IDs without resource generation and shared-reference proof perform no deletion",
  () =>
    Effect.gen(function* () {
      const attachment: EventSink.LeaseCleanupTaskBindingV2 = {
        ...binding,
        effectId: "effect:owned-resource:attachment",
        task: { kind: "attachment", attachmentIds: ["original-attachment"] },
      };
      const cleanup = makeOwnedResourceCleanup({
        sink: { readDeletionCleanupTask: () => Effect.succeed(attachment) },
        terminals: {
          closeOwnedTargets: () => Effect.die("attachment cleanup cannot close terminals"),
        },
      });
      const result = yield* cleanup.cleanupOwnedAttachments(attachment, correlation);
      assert.deepEqual(result.outcome, {
        taskId: attachment.effectId,
        result: null,
        effect: "unknown",
      });
      assert.deepEqual(result.evidence.attachmentIds, ["original-attachment"]);
    }),
);

it.effect(
  "terminal target capture passes the exact application birth to the existing manager",
  () =>
    Effect.gen(function* () {
      if (binding.task.kind !== "terminal") return yield* Effect.die("terminal fixture required");
      const capture = binding.task.capture;
      const observed: unknown[] = [];
      const cleanup = makeOwnedResourceCleanup({
        sink: { readDeletionCleanupTask: () => Effect.succeed(binding) },
        terminals: {
          captureOwnedTargets: (input) =>
            Effect.sync(() => {
              observed.push(input);
              return capture;
            }),
          closeOwnedTargets: () => Effect.die("capture cannot close a resource"),
        },
      });
      assert.isDefined(cleanup.captureOwnedTerminalTargets);
      const issued = yield* cleanup.captureOwnedTerminalTargets!(ownerBirth);
      assert.strictEqual(issued, capture);
      assert.deepEqual(observed, [{ threadId, ownerBirth }]);
    }),
);

it.effect(
  "originally unleased terminal cleanup preserves the issued empty roster and absence inventory",
  () =>
    Effect.gen(function* () {
      if (binding.task.kind !== "terminal") return yield* Effect.die("terminal fixture required");
      const { lease, version, ...original } = binding;
      const unleased: EventSink.UnleasedDeletionCleanupTaskBindingV1 = {
        ...original,
        version: 1,
        leaseInventory: { status: "absent", resourcePath: lease.resourcePath },
        task: { kind: "terminal", capture: { ...binding.task.capture, targets: [] } },
      };
      const captures: unknown[] = [];
      const cleanup = makeOwnedResourceCleanup({
        sink: { readDeletionCleanupTask: () => Effect.succeed(unleased) },
        terminals: {
          closeOwnedTargets: (capture) =>
            Effect.sync(() => {
              captures.push(capture);
              return {
                status: "observed_absent" as const,
                managedTargetsOnly: true as const,
                processExitObserved: false,
                descendantsQuiescence: "unavailable" as const,
                futureWakeClosure: "unavailable" as const,
              };
            }),
        },
      });
      const result = yield* cleanup.cleanupOwnedTerminals(unleased, correlation);
      const observation = yield* Schema.decodeUnknownEffect(
        EventSink.ManagedTerminalDeletionObservationV1,
      )(result.observation);
      assert.equal(observation.result.status, "observed_absent");
      assert.deepEqual(observation.capture.targets, []);
      assert.strictEqual(
        captures[0],
        unleased.task.kind === "terminal" ? unleased.task.capture : null,
      );
      assert.isFalse("lease" in unleased);
    }),
);

it.effect("invalid claim correlation performs no binding read or terminal close", () =>
  Effect.gen(function* () {
    let reads = 0;
    const cleanup = makeOwnedResourceCleanup({
      sink: {
        readDeletionCleanupTask: () =>
          Effect.sync(() => {
            reads++;
            return binding;
          }),
      },
      terminals: { closeOwnedTargets: () => Effect.die("invalid correlation cannot close") },
    });
    for (const input of [
      { workerId: "", expectedAttempt: 1 },
      { workerId: "worker", expectedAttempt: 0 },
    ]) {
      const result = yield* cleanup.cleanupOwnedTerminals(binding, input);
      assert.deepEqual(result.outcome, {
        taskId: binding.effectId,
        result: "failed",
        effect: "no_effect",
      });
      assert.isUndefined(result.observation);
    }
    assert.equal(reads, 0);
  }),
);

const namespaceRoot = "/configured/attachments";
const orphan = "foo-00000000-0000-4000-8000-000000000001.png";
const answer = "foo-00000000-0000-4000-8000-000000000002-pdf.pdf";
const otherThread = "foo-bar-00000000-0000-4000-8000-000000000003.png";
const fsError = (tag: "NotFound" | "PermissionDenied" | "Busy") =>
  PlatformError.systemError({
    _tag: tag,
    module: "FileSystem",
    method: "attachmentNamespaceCleanup",
    pathOrDescriptor: namespaceRoot,
  });

it.effect(
  "nonrecursive namespace sweep removes matching orphans and leaves prefixes and unsafe names untouched",
  () =>
    Effect.gen(function* () {
      const removed: string[] = [];
      const fs = FileSystem.makeNoop({
        readDirectory: (root, options) =>
          Effect.sync(() => {
            assert.equal(root, namespaceRoot);
            assert.deepEqual(options, { recursive: false });
            return [
              orphan,
              otherThread,
              "foo-invalid.png",
              `../${orphan}`,
              `nested/${orphan}`,
              `\\${orphan}`,
            ];
          }),
        remove: (path, options) =>
          Effect.sync(() => {
            assert.deepEqual(options, { recursive: false, force: false });
            removed.push(path);
          }),
      });
      const result = yield* makeAttachmentNamespaceScan(fs)({
        configuredRoot: namespaceRoot,
        namespaceSegment: "foo",
        retainedRelativePaths: [],
      });
      assert.deepEqual(result, {
        status: "completed",
        matchingPaths: [orphan],
        removedPaths: [orphan],
        retainedPaths: [],
        rootAbsent: false,
      });
      assert.deepEqual(removed, [`${namespaceRoot}/${orphan}`]);
    }),
);

it.effect(
  "namespace prune preserves exact retained answer filenames while removing unreferenced uploads",
  () =>
    Effect.gen(function* () {
      const removed: string[] = [];
      const fs = FileSystem.makeNoop({
        readDirectory: () => Effect.succeed([orphan, answer, otherThread]),
        remove: (path) =>
          Effect.sync(() => {
            removed.push(path);
          }),
      });
      const result = yield* makeAttachmentNamespaceScan(fs)({
        configuredRoot: namespaceRoot,
        namespaceSegment: "foo",
        retainedRelativePaths: [answer],
      });
      assert.deepEqual(result, {
        status: "completed",
        matchingPaths: [orphan, answer].sort(),
        removedPaths: [orphan],
        retainedPaths: [answer],
        rootAbsent: false,
      });
      assert.deepEqual(removed, [`${namespaceRoot}/${orphan}`]);
    }),
);

it.effect("a positively absent root completes while an unreadable inventory remains unknown", () =>
  Effect.gen(function* () {
    for (const tag of ["NotFound", "PermissionDenied"] as const) {
      const fs = FileSystem.makeNoop({
        readDirectory: () => Effect.fail(fsError(tag)),
        remove: () => Effect.die("unavailable root cannot remove entries"),
      });
      const result = yield* makeAttachmentNamespaceScan(fs)({
        configuredRoot: namespaceRoot,
        namespaceSegment: "foo",
        retainedRelativePaths: [],
      });
      assert.equal(result.status, tag === "NotFound" ? "completed" : "unknown");
      if (result.status === "completed") assert.isTrue(result.rootAbsent);
    }
  }),
);

it.effect(
  "partial namespace cleanup never recursively removes a directory and rescans remaining paths",
  () =>
    Effect.gen(function* () {
      const names = new Set([orphan, answer]);
      let failAnswer = true;
      const fs = FileSystem.makeNoop({
        readDirectory: () => Effect.sync(() => Array.from(names)),
        remove: (path, options) =>
          Effect.gen(function* () {
            assert.deepEqual(options, { recursive: false, force: false });
            const entry = path.slice(namespaceRoot.length + 1);
            if (entry === answer && failAnswer) return yield* Effect.fail(fsError("Busy"));
            names.delete(entry);
          }),
      });
      const scan = makeAttachmentNamespaceScan(fs);
      const input = {
        configuredRoot: namespaceRoot,
        namespaceSegment: "foo",
        retainedRelativePaths: [],
      };
      const first = yield* scan(input);
      assert.deepEqual(first, {
        status: "retryable_failure",
        removedPaths: [orphan],
        remainingPaths: [answer],
        reason: "attachment_remove_failed",
      });
      assert.deepEqual(Array.from(names), [answer]);
      failAnswer = false;
      const second = yield* scan(input);
      assert.deepEqual(second, {
        status: "completed",
        matchingPaths: [answer],
        removedPaths: [answer],
        retainedPaths: [],
        rootAbsent: false,
      });
      assert.equal(names.size, 0);
    }),
);

it.effect("interrupted namespace removal produces no fabricated completion observation", () =>
  Effect.gen(function* () {
    const fs = FileSystem.makeNoop({
      readDirectory: () => Effect.succeed([orphan]),
      remove: () => Effect.interrupt,
    });
    const result = yield* Effect.exit(
      makeAttachmentNamespaceScan(fs)({
        configuredRoot: namespaceRoot,
        namespaceSegment: "foo",
        retainedRelativePaths: [],
      }),
    );
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
  }),
);

function namespaceBasis(
  threadName = "foo",
): Extract<EventSink.QualifiedAttachmentNamespaceCleanupBasisV1, { status: "ready" }> {
  const threadId = ThreadId.make(threadName);
  return {
    status: "ready",
    task: {
      version: 1,
      effectId: "effect:namespace-original:attachment.cleanup",
      commandId: CommandId.make("namespace-original"),
      threadId,
      reference: {
        version: 1,
        mode: "delete_thread",
        ownerBirth: {
          kind: "application_v2_thread_birth",
          threadId,
          eventId: EventId.make("namespace-birth"),
          sequence: 1,
        },
        triggerEventId: EventId.make("namespace-delete"),
      },
      triggerSequence: 2,
      bindingSha256: "d".repeat(64),
    },
    claim: {
      workerId: "namespace-original-worker",
      expectedAttempt: 1,
      leaseExpiresAt: "2026-10-04T00:00:00.000Z",
    },
    basisEventSequence: 2,
    retainedRelativePaths: [],
  };
}
function namespacePruneBasis(relativePaths: ReadonlyArray<string> = [answer]) {
  const ready = namespaceBasis();
  const sourceBirth = {
    ...ready.task.reference.ownerBirth,
    threadId: ThreadId.make("imported-source"),
    eventId: EventId.make("imported-source-birth"),
  };
  const retentionSourceEvidence = makeImportedApplicationAttachmentRetentionEvidenceV1({
    segments: [
      {
        inventoryId: "a".repeat(64),
        applicationBirth: sourceBirth,
        carrierSetSha256: "b".repeat(64),
        forkBasis: [
          {
            targetBirth: ready.task.reference.ownerBirth,
            sourceBirth,
            sourceRunId: RunId.make("imported-source-run"),
            sourceRunOrdinal: 1,
            sourceRunEvent: { eventId: EventId.make("imported-source-run-event"), sequence: 2 },
            forkEvent: { eventId: EventId.make("imported-fork-event"), sequence: 3 },
          },
        ],
      },
    ],
    visibleV2CarrierSetSha256: "c".repeat(64),
    relativePaths,
  });
  return {
    ...ready,
    task: {
      ...ready.task,
      reference: {
        ...ready.task.reference,
        mode: "prune_thread" as const,
        rollbackEffectId: "actual-rollback-effect",
      },
    },
    retainedRelativePaths: relativePaths,
    retentionSourceEvidence: Schema.decodeUnknownSync(
      ImportedApplicationAttachmentRetentionEvidenceV1,
    )(retentionSourceEvidence),
  };
}
const namespaceFixture = (
  options: {
    readonly basis?: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1;
    readonly read?: (
      index: number,
      removed: ReadonlyArray<string>,
      basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1,
    ) => EventSink.AttachmentNamespaceCleanupBasisV1;
    readonly record?: (
      index: number,
      input: Parameters<
        EventSink.EventSinkV2["Service"]["recordAttachmentNamespaceCleanupObservation"]
      >[0],
    ) => ReturnType<
      EventSink.EventSinkV2["Service"]["recordAttachmentNamespaceCleanupObservation"]
    >;
    readonly interruptRemoval?: boolean;
    readonly entries?: ReadonlyArray<string>;
    readonly readObservation?: (
      current: EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null,
    ) => EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null;
  } = {},
) =>
  Effect.gen(function* () {
    const basis = options.basis ?? namespaceBasis();
    const calls: string[] = [];
    const removed: string[] = [];
    const records: Array<
      Parameters<EventSink.EventSinkV2["Service"]["recordAttachmentNamespaceCleanupObservation"]>[0]
    > = [];
    let readIndex = 0;
    let lockDepth = 0;
    let persisted: EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null = null;
    const serial = yield* makeKeyedSerialExecutor<ThreadId>();
    const fixture = makeAttachmentNamespaceCleanup({
      path: yield* Path.Path,
      configuredRoot: `${namespaceRoot}/../attachments`,
      executor: {
        withLock: <A, E, R>(key: ThreadId, effect: Effect.Effect<A, E, R>) =>
          serial.withLock(
            key,
            Effect.gen(function* () {
              assert.equal(key, basis.task.threadId);
              calls.push("lock");
              lockDepth++;
              return yield* effect;
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  lockDepth--;
                  calls.push("unlock");
                }),
              ),
            ),
          ),
      },
      sink: {
        readAttachmentNamespaceCleanupTask: () =>
          Effect.sync(() => {
            assert.equal(lockDepth, 1);
            calls.push("task");
            return basis.task;
          }),
        readAttachmentNamespaceCleanupBasis: (input) =>
          Effect.sync(() => {
            assert.equal(lockDepth, 1);
            calls.push("basis");
            assert.deepEqual(input, {
              effectId: basis.task.effectId,
              workerId: basis.claim.workerId,
              expectedAttempt: basis.claim.expectedAttempt,
            });
            return options.read?.(readIndex++, removed, basis) ?? basis;
          }),
        readAttachmentNamespaceCleanupObservation: () =>
          Effect.sync(() => {
            assert.equal(lockDepth, 1);
            calls.push("anchor_read");
            return options.readObservation === undefined
              ? persisted
              : options.readObservation(persisted);
          }),
        recordAttachmentNamespaceCleanupObservation: (input) =>
          Schema.decodeUnknownEffect(EventSink.AttachmentNamespaceCleanupObservationV1)(
            input.observation,
          ).pipe(
            Effect.mapError((cause) => new EventSink.EventSinkWriteError({ eventCount: 0, cause })),
            Effect.flatMap(() => {
              assert.equal(lockDepth, 1);
              calls.push(`record:${input.observation.outcome.status}`);
              records.push(input);
              const index = records.length - 1;
              const write =
                options.record === undefined
                  ? Effect.succeed({
                      status:
                        input.observation.outcome.status === "unknown"
                          ? ("unknown" as const)
                          : input.observation.outcome.status === "retryable_failure"
                            ? ("retryable" as const)
                            : ("completed" as const),
                      effectId: input.basis.task.effectId,
                      ordinal: index,
                    })
                  : options.record(index, input);
              return write.pipe(
                Effect.tap((result) =>
                  Effect.sync(() => {
                    if (result.status !== "stale" && result.ordinal !== null)
                      persisted = {
                        ordinal: result.ordinal,
                        task: input.basis.task,
                        basis: input.basis,
                        observation: input.observation,
                        status: result.status,
                      };
                  }),
                ),
              );
            }),
          ),
      },
      fileSystem: FileSystem.makeNoop({
        readDirectory: (root, directoryOptions) =>
          Effect.sync(() => {
            assert.equal(lockDepth, 1);
            assert.equal(root, namespaceRoot);
            assert.deepEqual(directoryOptions, { recursive: false });
            calls.push("scan");
            return [...(options.entries ?? [orphan, answer, otherThread])];
          }),
        remove: (path, optionsInput) =>
          Effect.gen(function* () {
            assert.equal(lockDepth, 1);
            assert.deepEqual(optionsInput, { recursive: false, force: false });
            calls.push("remove");
            if (options.interruptRemoval) return yield* Effect.interrupt;
            removed.push(path.slice(namespaceRoot.length + 1));
          }),
      }),
    });
    return { invoke: fixture, basis, calls, removed, records };
  }).pipe(Effect.provide(NodePath.layer));

it.effect(
  "qualified namespace cleanup anchors original claim before I/O under one shared thread lock",
  () =>
    Effect.gen(function* () {
      const fixture = yield* namespaceFixture();
      const result = yield* fixture.invoke(fixture.basis);
      assert.equal(result.status, "completed");
      assert.equal(result.record?.status, "completed");
      assert.equal(result.anchorOrdinal, 0);
      assert.deepEqual(fixture.calls, [
        "lock",
        "task",
        "basis",
        "record:unknown",
        "anchor_read",
        "basis",
        "scan",
        "basis",
        "remove",
        "basis",
        "remove",
        "record:completed",
        "unlock",
      ]);
      assert.deepEqual(fixture.records[0]?.observation.outcome, {
        status: "unknown",
        removedPaths: [],
        reason: "attachment_namespace_invocation_pending",
      });
      assert.deepEqual(fixture.removed, [orphan, answer]);
      for (const input of fixture.records) {
        assert.equal(input.observation.configuredRoot, namespaceRoot);
        assert.equal(input.observation.namespaceSegment, "foo");
        assert.equal(input.observation.workerId, fixture.basis.claim.workerId);
        assert.equal(input.observation.expectedAttempt, 1);
        assert.deepEqual(
          input.basis.task.reference.ownerBirth,
          fixture.basis.task.reference.ownerBirth,
        );
      }
    }),
);

it.effect("qualified superseded and unsafe namespace outcomes perform zero filesystem work", () =>
  Effect.gen(function* () {
    const ready = namespaceBasis();
    const unsafe = namespaceBasis("!!!");
    const variants: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1[] = [
      {
        status: "superseded",
        task: ready.task,
        claim: ready.claim,
        basisEventSequence: 3,
        replacementBirth: {
          ...ready.task.reference.ownerBirth,
          eventId: EventId.make("replacement-birth"),
          sequence: 3,
        },
      },
      { status: "unsafe_namespace", task: unsafe.task, claim: unsafe.claim, basisEventSequence: 2 },
    ];
    for (const basis of variants) {
      const fixture = yield* namespaceFixture({ basis });
      const result = yield* fixture.invoke(basis);
      assert.equal(result.status, "completed");
      assert.isFalse(fixture.calls.includes("scan"));
      assert.deepEqual(fixture.removed, []);
      assert.equal(fixture.records.length, 1);
      assert.equal(fixture.records[0]?.observation.outcome.status, basis.status);
    }
  }),
);

it.effect(
  "unavailable claim and failed invocation anchor perform no namespace scan or removal",
  () =>
    Effect.gen(function* () {
      const variants = [
        yield* namespaceFixture({
          read: (_index, _removed, basis) => ({
            status: "unavailable",
            effectId: basis.task.effectId,
            reason: "claim_changed",
          }),
        }),
        yield* namespaceFixture({
          record: () =>
            Effect.fail(
              new EventSink.EventSinkWriteError({ eventCount: 0, cause: "anchor_failed" }),
            ),
        }),
      ];
      for (const fixture of variants) {
        assert.notEqual((yield* fixture.invoke(fixture.basis)).status, "completed");
        assert.isFalse(fixture.calls.includes("scan"));
        assert.deepEqual(fixture.removed, []);
      }
    }),
);

it.effect(
  "claim loss after partial namespace removal preserves original anchored UNKNOWN and stops the next entry",
  () =>
    Effect.gen(function* () {
      const fixture = yield* namespaceFixture({
        read: (_index, removed, basis) =>
          removed.length > 0
            ? { status: "unavailable", effectId: basis.task.effectId, reason: "claim_lost" }
            : basis,
      });
      const result = yield* fixture.invoke(fixture.basis);
      assert.equal(result.status, "unknown");
      assert.equal(result.anchorOrdinal, 0);
      assert.deepEqual(fixture.removed, [orphan]);
      assert.deepEqual(fixture.records[1]?.observation.outcome, {
        status: "unknown",
        removedPaths: [orphan],
        reason: "attachment_namespace_basis_changed",
      });
      assert.equal(fixture.records[1]?.basis.claim.workerId, "namespace-original-worker");
      assert.equal(fixture.records[1]?.basis.claim.expectedAttempt, 1);
    }),
);

it.effect(
  "a retained-reference change before destructive entry keeps the file and persists UNKNOWN",
  () =>
    Effect.gen(function* () {
      const fixture = yield* namespaceFixture({
        read: (index, _removed, basis) =>
          index > 1 && basis.status === "ready"
            ? { ...basis, retainedRelativePaths: [orphan] }
            : basis,
      });
      const result = yield* fixture.invoke(fixture.basis);
      assert.equal(result.status, "unknown");
      assert.deepEqual(fixture.removed, []);
      assert.equal(fixture.records[1]?.observation.outcome.status, "unknown");
    }),
);

it.effect(
  "lost final observation write never reports stale or completed after filesystem effects",
  () =>
    Effect.gen(function* () {
      const fixture = yield* namespaceFixture({
        record: (index, input) =>
          Effect.succeed({
            status: index === 1 ? "stale" : "unknown",
            effectId: input.basis.task.effectId,
            ordinal: index === 1 ? null : index,
          }),
      });
      const result = yield* fixture.invoke(fixture.basis);
      assert.equal(result.status, "unknown");
      assert.equal(result.anchorOrdinal, 0);
      assert.deepEqual(fixture.removed, [orphan, answer]);
      assert.deepEqual(fixture.records[2]?.observation.outcome, {
        status: "unknown",
        removedPaths: [orphan, answer],
        reason: "attachment_namespace_observation_not_committed",
      });
      assert.strictEqual(fixture.records[2]?.basis, fixture.basis);
    }),
);

it.effect(
  "qualified prune retains actual answer paths and never substitutes deletion's empty inventory",
  () =>
    Effect.gen(function* () {
      const basis = namespacePruneBasis();
      const fixture = yield* namespaceFixture({ basis });
      const result = yield* fixture.invoke(basis);
      assert.equal(result.status, "completed");
      assert.deepEqual(fixture.removed, [orphan]);
      assert.deepEqual(fixture.records[1]?.observation.outcome, {
        status: "completed",
        matchingPaths: [orphan, answer].sort(),
        removedPaths: [orphan],
        retainedPaths: [answer],
        rootAbsent: false,
      });
    }),
);

it.effect(
  "interrupted qualified removal leaves its original invocation anchor without fake completion",
  () =>
    Effect.gen(function* () {
      const fixture = yield* namespaceFixture({ interruptRemoval: true });
      const result = yield* Effect.exit(fixture.invoke(fixture.basis));
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
      assert.equal(fixture.records.length, 1);
      assert.equal(fixture.records[0]?.observation.outcome.status, "unknown");
      assert.deepEqual(fixture.removed, []);
    }),
);

it.effect(
  "an absent or changed factual invocation anchor prevents all namespace filesystem work",
  () =>
    Effect.gen(function* () {
      for (const readObservation of [
        () => null,
        (current: EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null) =>
          current === null
            ? null
            : {
                ...current,
                basis: { ...current.basis, claim: { ...current.basis.claim, expectedAttempt: 2 } },
              },
      ]) {
        const fixture = yield* namespaceFixture({ readObservation });
        const result = yield* fixture.invoke(fixture.basis);
        assert.equal(result.status, "unknown");
        assert.equal(result.anchorOrdinal, 0);
        assert.isFalse(fixture.calls.includes("scan"));
        assert.deepEqual(fixture.removed, []);
      }
    }),
);

it.effect("canonical factual anchor equality permits equivalent decoded field ordering", () =>
  Effect.gen(function* () {
    const fixture = yield* namespaceFixture({
      readObservation: (current) =>
        current === null || current.basis.status !== "ready"
          ? current
          : {
              ...current,
              basis: {
                task: current.basis.task,
                claim: current.basis.claim,
                retainedRelativePaths: current.basis.retainedRelativePaths,
                basisEventSequence: current.basis.basisEventSequence,
                status: "ready",
              },
            },
    });
    const result = yield* fixture.invoke(fixture.basis);
    assert.equal(result.status, "completed");
    assert.deepEqual(fixture.removed, [orphan, answer]);
  }),
);

it.effect(
  "prune rejects changed imported evidence at initial qualification even when retained paths match",
  () =>
    Effect.gen(function* () {
      const basis = namespacePruneBasis();
      const changed = {
        ...basis,
        retentionSourceEvidence: makeImportedApplicationAttachmentRetentionEvidenceV1({
          ...basis.retentionSourceEvidence,
          visibleV2CarrierSetSha256: "e".repeat(64),
          relativePaths: basis.retainedRelativePaths,
        }),
      };
      const fixture = yield* namespaceFixture({ basis, read: () => changed });
      const result = yield* fixture.invoke(basis);
      assert.equal(result.status, "unavailable");
      assert.equal(fixture.records.length, 0);
      assert.isFalse(fixture.calls.includes("scan"));
      assert.deepEqual(fixture.removed, []);
    }),
);

it.effect(
  "prune rechecks inventory, carrier, application birth and fork evidence before each destructive entry",
  () =>
    Effect.gen(function* () {
      const basis = namespacePruneBasis();
      const original = basis.retentionSourceEvidence.segments[0]!;
      const changedSegments = [
        { ...original, inventoryId: "e".repeat(64) },
        { ...original, carrierSetSha256: "e".repeat(64) },
        {
          ...original,
          applicationBirth: {
            ...original.applicationBirth,
            eventId: EventId.make("replacement-source-birth"),
          },
        },
        {
          ...original,
          forkBasis: [
            {
              ...original.forkBasis[0]!,
              forkEvent: { eventId: EventId.make("changed-fork-event"), sequence: 4 },
            },
          ],
        },
      ];
      for (const segment of changedSegments) {
        const changed = {
          ...basis,
          retentionSourceEvidence: makeImportedApplicationAttachmentRetentionEvidenceV1({
            ...basis.retentionSourceEvidence,
            segments: [segment],
            relativePaths: basis.retainedRelativePaths,
          }),
        };
        const fixture = yield* namespaceFixture({
          basis,
          read: (index) => (index > 1 ? changed : basis),
        });
        const result = yield* fixture.invoke(basis);
        assert.equal(result.status, "unknown");
        assert.equal(result.anchorOrdinal, 0);
        assert.deepEqual(fixture.removed, []);
        assert.deepEqual(fixture.records[1]?.observation.outcome, {
          status: "unknown",
          removedPaths: [],
          reason: "attachment_namespace_basis_changed",
        });
        assert.strictEqual(fixture.records[1]?.basis, basis);
      }
    }),
);

it.effect(
  "changed imported evidence after partial prune retains original anchor and stops the next file",
  () =>
    Effect.gen(function* () {
      const basis = namespacePruneBasis([]);
      const changed = {
        ...basis,
        retentionSourceEvidence: makeImportedApplicationAttachmentRetentionEvidenceV1({
          ...basis.retentionSourceEvidence,
          visibleV2CarrierSetSha256: "e".repeat(64),
          relativePaths: [],
        }),
      };
      const fixture = yield* namespaceFixture({
        basis,
        read: (_index, removed) => (removed.length > 0 ? changed : basis),
      });
      const result = yield* fixture.invoke(basis);
      assert.equal(result.status, "unknown");
      assert.deepEqual(fixture.removed, [orphan]);
      assert.deepEqual(fixture.records[1]?.observation.outcome, {
        status: "unknown",
        removedPaths: [orphan],
        reason: "attachment_namespace_basis_changed",
      });
      assert.strictEqual(fixture.records[1]?.basis, basis);
      assert.equal(result.anchorOrdinal, 0);
    }),
);

it.effect("prune evidence disappearing after its invocation anchor prevents filesystem entry", () =>
  Effect.gen(function* () {
    const basis = namespacePruneBasis();
    const { retentionSourceEvidence: _evidence, ...missing } = basis;
    const fixture = yield* namespaceFixture({
      basis,
      read: (index) => (index > 0 ? missing : basis),
    });
    const result = yield* fixture.invoke(basis);
    assert.equal(result.status, "unknown");
    assert.equal(result.anchorOrdinal, 0);
    assert.isFalse(fixture.calls.includes("scan"));
    assert.deepEqual(fixture.removed, []);
    assert.strictEqual(fixture.records[1]?.basis, basis);
  }),
);

it.effect("fresh prune missing imported evidence performs no scan or invocation anchor", () =>
  Effect.gen(function* () {
    const { retentionSourceEvidence: _evidence, ...basis } = namespacePruneBasis();
    const fixture = yield* namespaceFixture({ basis });
    const result = yield* fixture.invoke(basis);
    assert.equal(result.status, "unavailable");
    assert.equal(fixture.records.length, 0);
    assert.isFalse(fixture.calls.includes("scan"));
    assert.deepEqual(fixture.removed, []);
  }),
);

it.effect(
  "unavailable imported inventory, source cut or birth evidence preserves every namespace file",
  () =>
    Effect.gen(function* () {
      for (const reason of [
        "imported_inventory_unavailable",
        "carrier_decode_unavailable",
        "source_cut_changed",
        "application_birth_unavailable",
      ]) {
        const basis = namespacePruneBasis();
        const fixture = yield* namespaceFixture({
          basis,
          read: (index) =>
            index > 0 ? { status: "unavailable", effectId: basis.task.effectId, reason } : basis,
        });
        const result = yield* fixture.invoke(basis);
        assert.equal(result.status, "unknown");
        assert.isFalse(fixture.calls.includes("scan"));
        assert.deepEqual(fixture.removed, []);
        assert.equal(fixture.records.length, 2);
        assert.equal(fixture.records[1]?.observation.outcome.status, "unknown");
      }
    }),
);

it.effect(
  "complete imported baseline and later V2 references survive prune despite unrelated event churn",
  () =>
    Effect.gen(function* () {
      const laterMessage = "foo-00000000-0000-4000-8000-000000000004.png";
      const laterAnswer = "foo-00000000-0000-4000-8000-000000000005-pdf.pdf";
      const basis = namespacePruneBasis([answer, laterMessage, laterAnswer]);
      const fixture = yield* namespaceFixture({
        basis,
        entries: [orphan, answer, laterMessage, laterAnswer, otherThread],
        read: (index) => ({ ...basis, basisEventSequence: basis.basisEventSequence + index }),
      });
      const result = yield* fixture.invoke(basis);
      assert.equal(result.status, "completed");
      assert.deepEqual(fixture.removed, [orphan]);
      assert.deepEqual(fixture.records[1]?.observation.outcome, {
        status: "completed",
        matchingPaths: [orphan, answer, laterMessage, laterAnswer].sort(),
        removedPaths: [orphan],
        retainedPaths: [answer, laterMessage, laterAnswer].sort(),
        rootAbsent: false,
      });
      assert.deepEqual(fixture.records[1]?.basis, basis);
    }),
);
