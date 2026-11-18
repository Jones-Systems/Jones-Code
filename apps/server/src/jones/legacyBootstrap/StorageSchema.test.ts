import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as S from "./StorageSchema.ts";

it("metadata codecs accept only the retained Jones fields and reject public snapshots or unknown authority", () => {
  const run = Schema.decodeUnknownSync(S.RunMetadata, { onExcessProperty: "error" });
  const thread = Schema.decodeUnknownSync(S.ThreadMetadata, { onExcessProperty: "error" });
  assert.deepStrictEqual(
    run({ workspaceRunSetupScript: false, legacyPreparationFailureKnown: false }),
    { workspaceRunSetupScript: false, legacyPreparationFailureKnown: false },
  );
  assert.throws(() => run({ id: "R", threadId: "T", status: "running" }));
  assert.throws(() => run({ workspaceRunSetupScript: "true" }));
  assert.throws(() => thread({ ownsNewThread: true }));
  assert.throws(() => thread({ publicSnapshot: {} }));
});

it("private receipt decoding requires an explicit sequence domain", () => {
  const decode = Schema.decodeUnknownSync(S.LegacyPreflightReceipt);
  const input = {
    commandId: "intent",
    threadId: "T",
    commandType: "legacy-bootstrap.preflight-intent",
    acceptedAt: DateTime.makeUnsafe("2026-11-17T10:00:00.000Z"),
    resultSequence: 1,
    status: "accepted",
    error: null,
  };
  assert.equal(
    decode({ ...input, sequenceDomain: "jones-legacy-ordinal" }).sequenceDomain,
    "jones-legacy-ordinal",
  );
  assert.throws(() => decode(input));
  assert.throws(() => decode({ ...input, sequenceDomain: "lifecycle" }));
});
