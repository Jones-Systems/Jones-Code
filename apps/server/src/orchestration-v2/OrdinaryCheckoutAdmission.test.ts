import { assert, it } from "@effect/vitest";
import { OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  OrdinaryCheckoutCaptureV1,
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutUseV1,
  ordinaryApplicationIncarnationV1,
  ordinaryCheckoutAdmissionIdV1,
  ordinaryCheckoutAdmissionMatchesV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutCaptureMatchesV1,
  ordinaryCheckoutCommandDigestV1,
} from "./OrdinaryCheckoutOwnership.ts";

const timestamp = "2026-10-03T00:00:00.000Z";
const birth = {
  kind: "application_v2_thread_birth",
  threadId: "thread:ordinary-fixture",
  eventId: "event:ordinary-fixture:birth",
  sequence: 1,
};
const command = Schema.encodeSync(OrchestrationV2Command)(
  Schema.decodeUnknownSync(OrchestrationV2Command, { onExcessProperty: "error" })({
    type: "message.dispatch",
    commandId: "command:ordinary-fixture",
    threadId: birth.threadId,
    messageId: "message:ordinary-fixture",
    text: "Continue the fixture.",
    attachments: [],
    modelSelection: { instanceId: "fixture-codex", model: "fixture-model" },
    dispatchMode: { type: "defer_start" },
    createdBy: "agent",
    creationSource: "server",
  }),
);
if (command.type !== "message.dispatch")
  throw new Error("Fixture command must be message.dispatch");
const capture = Schema.decodeUnknownSync(OrdinaryCheckoutCaptureV1, { onExcessProperty: "error" })({
  version: 1,
  commandId: command.commandId,
  commandType: command.type,
  canonicalCommand: command,
  commandDigest: ordinaryCheckoutCommandDigestV1(command),
  origin: { kind: "command" },
  threadId: birth.threadId,
  applicationBirth: birth,
  projectId: "project:ordinary-fixture",
  canonicalProjectRoot: "/fixture/repo",
  canonicalCheckoutPath: "/fixture/worktrees/original",
  branch: "fixture-temporary",
  lease: {
    resourcePath: "/fixture/worktrees/original",
    leaseId: "lease:ordinary-fixture",
    ownerThreadId: birth.threadId,
    ownerIncarnation: JSON.stringify([
      "t3.orchestration-v2.thread-birth/v1",
      birth.eventId,
      birth.sequence,
    ]),
    branch: "fixture-temporary",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  },
});
const admission = Schema.decodeUnknownSync(OrdinaryCheckoutAdmissionV1, {
  onExcessProperty: "error",
})({
  version: 1,
  admissionId: ordinaryCheckoutAdmissionIdV1(capture),
  capture,
  receipt: {
    commandId: capture.commandId,
    threadId: capture.threadId,
    commandType: capture.commandType,
    acceptedAt: timestamp,
    resultSequence: 6,
    status: "accepted",
    error: null,
  },
  eventBasis: [
    {
      eventId: "event:ordinary-fixture:message",
      sequence: 2,
      threadId: birth.threadId,
      commandId: capture.commandId,
      eventType: "message.updated",
    },
    {
      eventId: "event:ordinary-fixture:prepared",
      sequence: 4,
      threadId: birth.threadId,
      commandId: capture.commandId,
      eventType: "turn-item.updated",
    },
  ],
  run: {
    runId: "run:ordinary-fixture",
    runAttemptId: "attempt:ordinary-fixture",
    nodeId: "node:ordinary-fixture",
    messageId: command.messageId,
  },
  recordedAt: timestamp,
});
const reference = ordinaryCheckoutAdmissionRefV1(admission);
const preparedUse = Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
  version: 1,
  kind: "ordinary_checkout_use",
  operationId: "operation:ordinary-fixture:rename",
  admission: reference,
  source: { kind: "prepared_run", admission: reference, preparation: admission.run },
  lease: capture.lease,
});
it.effect(
  "pure admission binds original receipt and event basis independently from its final receipt sequence",
  () =>
    Effect.sync(() => {
      assert.isTrue(ordinaryCheckoutCaptureMatchesV1(capture));
      assert.isTrue(ordinaryCheckoutAdmissionMatchesV1(admission));
      assert.equal(
        ordinaryApplicationIncarnationV1(capture.applicationBirth),
        capture.lease.ownerIncarnation,
      );
      assert.notEqual(admission.eventBasis.at(-1)!.sequence, admission.receipt.resultSequence);
      const reordered = { ...admission, eventBasis: [...admission.eventBasis].reverse() };
      assert.isFalse(ordinaryCheckoutAdmissionMatchesV1(reordered));
      assert.isFalse(
        ordinaryCheckoutCaptureMatchesV1({
          ...capture,
          canonicalCommand: { ...command, text: "changed" },
        }),
      );
      assert.isFalse(
        ordinaryCheckoutCaptureMatchesV1({
          ...capture,
          applicationBirth: {
            ...capture.applicationBirth,
            eventId: admission.eventBasis[0]!.eventId,
          },
        }),
      );
    }),
);

it.effect(
  "renewal preserves captured identity while rotation, branch, path, birth and original command changes invalidate references",
  () =>
    Effect.sync(() => {
      const renewed = {
        ...admission,
        capture: {
          ...capture,
          lease: { ...capture.lease, renewedAtMs: 60001, expiresAtMs: 360001 },
        },
      };
      assert.deepEqual(ordinaryCheckoutAdmissionRefV1(renewed), reference);
      for (const changed of [
        {
          ...admission,
          capture: { ...capture, lease: { ...capture.lease, leaseId: "lease:rotated" } },
        },
        { ...admission, capture: { ...capture, lease: { ...capture.lease, branch: "changed" } } },
        {
          ...admission,
          capture: { ...capture, canonicalCheckoutPath: "/fixture/worktrees/changed" },
        },
        {
          ...admission,
          capture: { ...capture, applicationBirth: { ...capture.applicationBirth, sequence: 99 } },
        },
        {
          ...admission,
          capture: { ...capture, canonicalCommand: { ...command, text: "changed" } },
        },
      ])
        assert.notEqual(
          ordinaryCheckoutAdmissionRefV1(changed).admissionSha256,
          reference.admissionSha256,
        );
      assert.throws(() =>
        Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
          ...Schema.encodeSync(OrdinaryCheckoutUseV1)(preparedUse),
          source: {
            ...Schema.encodeSync(OrdinaryCheckoutUseV1)(preparedUse).source,
            workerId: "invented",
          },
        }),
      );
    }),
);
