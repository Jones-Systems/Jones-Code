import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as Witness from "./NormalizationWitness.ts";

const commandId = CommandId.make("command:normalization");
const threadId = ThreadId.make("thread:normalization");
const rawInput = {
  type: "runtime-request.respond",
  commandId,
  threadId,
  requestId: "request:normalization",
  answers: { question: ["Chosen answer"] },
  attachmentsByQuestionId: {
    question: [{ id: "pending-original", name: "note.txt", mimeType: "text/plain", sizeBytes: 3 }],
  },
};
const acceptedCommand: OrchestrationV2Command = {
  type: "runtime-request.respond",
  commandId,
  threadId,
  requestId: RuntimeRequestId.make("request:normalization"),
  answers: { question: ["Chosen answer", 'Attached file "note.txt": "/attachments/accepted.txt"'] },
};
const birth = {
  kind: "application_v2_thread_birth" as const,
  threadId,
  eventId: EventId.make("birth:normalization"),
  sequence: 7,
};
const row: Witness.NormalizationWitnessV1 = {
  commandId,
  commandType: "runtime-request.respond",
  witnessVersion: 1,
  requestDigest: Witness.requestDigest(rawInput),
  attachments: [
    {
      pendingId: "pending-original",
      contentSha256: "a".repeat(64),
      sizeBytes: 3,
      finalId: "accepted-original",
    },
  ],
  contextRemaps: [{ sourceId: "pending-original", finalId: "accepted-original" }],
  acceptedCommand,
  acceptedCommandDigest: Witness.acceptedCommandDigest(acceptedCommand),
  receiptSequence: 11,
  threadId,
  projectId: ProjectId.make("project:normalization"),
  applicationBirth: birth,
  createdAt: "2026-10-04T00:00:00.000Z",
};
const input = { commandId, threadId, rawInput };

it("canonicalizes object keys while preserving caller fields and pending reference order", () => {
  assert.equal(
    Witness.requestDigest({ text: "hello", ids: ["first", "second"] }),
    Witness.requestDigest({ ids: ["first", "second"], text: "hello" }),
  );
  assert.notEqual(
    Witness.requestDigest({ ids: ["first", "second"] }),
    Witness.requestDigest({ ids: ["second", "first"] }),
  );
  for (const changed of [
    { createdBy: "agent" },
    { creationSource: "mcp" },
    { senderThreadId: "sender" },
    { text: "changed" },
    { options: [{ id: "reasoning", value: "high" }] },
  ]) {
    assert.notEqual(
      Witness.requestDigest(rawInput),
      Witness.requestDigest({ ...rawInput, ...changed }),
    );
  }
});

it.effect(
  "round trips the exact accepted body and full application birth through the SQL row codec",
  () =>
    Effect.gen(function* () {
      const encoded = Witness.encodeNormalizationWitnessRow(row);
      assert.deepEqual(yield* Witness.decodeNormalizationWitnessRow(encoded), row);
      assert.deepEqual(
        yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
          encoded.application_birth_json,
        ),
        birth,
      );
    }),
);

it.effect(
  "replays the accepted answer including appended paths when pending bytes are absent or unchanged",
  () =>
    Effect.gen(function* () {
      for (const pending of [null, { contentSha256: "a".repeat(64), sizeBytes: 3 }]) {
        const probed: string[] = [];
        const replay = yield* Witness.compareForReplay(
          row,
          input,
          (id) =>
            Effect.sync(() => {
              probed.push(id);
              return pending;
            }),
          birth,
        );
        assert.deepEqual(probed, ["pending-original"]);
        assert.deepEqual(replay, acceptedCommand);
      }
    }),
);

it.effect(
  "rejects changed pending identity before probing even if replacement bytes would match",
  () =>
    Effect.gen(function* () {
      let probes = 0;
      const failure = yield* Effect.flip(
        Witness.compareForReplay(
          row,
          {
            ...input,
            rawInput: {
              ...rawInput,
              attachmentsByQuestionId: {
                question: [
                  { ...rawInput.attachmentsByQuestionId.question[0], id: "pending-replacement" },
                ],
              },
            },
          },
          () =>
            Effect.sync(() => {
              probes += 1;
              return { contentSha256: "a".repeat(64), sizeBytes: 3 };
            }),
          birth,
        ),
      );
      assert.equal(failure.reason, "request_changed");
      assert.equal(probes, 0);
    }),
);

it.effect("rejects changed present pending bytes or size", () =>
  Effect.gen(function* () {
    for (const pending of [
      { contentSha256: "b".repeat(64), sizeBytes: 3 },
      { contentSha256: "a".repeat(64), sizeBytes: 4 },
    ]) {
      const failure = yield* Effect.flip(
        Witness.compareForReplay(row, input, () => Effect.succeed(pending), birth),
      );
      assert.equal(failure.reason, "pending_bytes_changed");
    }
  }),
);

it.effect("rejects recreated or missing application births before probing", () =>
  Effect.gen(function* () {
    for (const current of [
      null,
      { ...birth, eventId: EventId.make("birth:replacement") },
      { ...birth, sequence: 8 },
    ]) {
      const failure = yield* Effect.flip(
        Witness.compareForReplay(
          row,
          input,
          () => Effect.die("Must reject before pending probe"),
          current,
        ),
      );
      assert.equal(failure.reason, "application_birth_changed");
    }
  }),
);

it.effect("rejects accepted command corruption and cross-thread replay", () =>
  Effect.gen(function* () {
    const changed = {
      ...row,
      acceptedCommand: { ...acceptedCommand, answers: { question: ["Changed"] } },
    };
    assert.equal(
      (yield* Effect.flip(
        Witness.compareForReplay(changed, input, () => Effect.succeed(null), birth),
      )).reason,
      "accepted_command_changed",
    );
    assert.equal(
      (yield* Effect.flip(
        Witness.compareForReplay(
          row,
          { ...input, threadId: ThreadId.make("other") },
          () => Effect.succeed(null),
          birth,
        ),
      )).reason,
      "request_changed",
    );
  }),
);
