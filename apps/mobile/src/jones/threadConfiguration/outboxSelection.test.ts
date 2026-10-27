import {
  CommandId,
  ImportedHistoryDelivery,
  MessageId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  captureComposerModelSelectionSnapshot,
  composerModelSelectionSnapshotIsCurrent,
  outboxSelectionCommandFields,
  outboxSelectionRestoreSettings,
  recordComposerModelSelectionInteraction,
  retainComposerModelSelectionSnapshot,
  trackComposerModelSelectionInteractions,
} from "./outboxSelection";

const decodeImportedHistoryDelivery = Schema.decodeUnknownSync(ImportedHistoryDelivery);

const selection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-6.1-sol",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const explicit = { modelSelection: selection, modelSelectionExplicit: true };

describe("mobile outbox model selection", () => {
  it.each([true, false, undefined])("preserves command explicitness (%s)", (flag) => {
    const fields = outboxSelectionCommandFields({
      modelSelection: selection,
      ...(flag === undefined ? {} : { modelSelectionExplicit: flag }),
    });
    if (flag === undefined) expect(fields).not.toHaveProperty("modelSelectionExplicit");
    else expect(fields).toEqual({ modelSelectionExplicit: flag });

    // The imported-history review carries the prepared message command, whose
    // schema must retain the flag through review and persisted submission.
    const delivery = decodeImportedHistoryDelivery({
      type: "message",
      command: {
        type: "message.dispatch",
        commandId: CommandId.make("command:review"),
        threadId: ThreadId.make("thread:review"),
        messageId: MessageId.make("message:review"),
        createdBy: "user",
        creationSource: "mobile",
        text: "Continue the imported history",
        attachments: [],
        modelSelection: selection,
        ...fields,
        dispatchMode: { type: "start_immediately" },
      },
    });
    expect(delivery.type).toBe("message");
    if (delivery.type !== "message") throw new Error("Expected a reviewed message");
    expect(delivery.command).toMatchObject({ modelSelection: selection, ...fields });
    if (flag === undefined) expect(delivery.command).not.toHaveProperty("modelSelectionExplicit");
  });

  it("restores an explicit pick into an untouched draft with the full options", () => {
    const key = "untouched";
    const release = trackComposerModelSelectionInteractions(key);
    try {
      const before = captureComposerModelSelectionSnapshot(key, {});
      expect(outboxSelectionRestoreSettings(explicit, key, before, {})).toEqual({
        modelSelection: selection,
      });
    } finally {
      release();
    }
  });

  it.each([false, undefined])("never restores inherited or legacy picks (%s)", (flag) => {
    expect(
      outboxSelectionRestoreSettings(
        {
          modelSelection: selection,
          ...(flag === undefined ? {} : { modelSelectionExplicit: flag }),
        },
        "inherited",
        {},
        {},
      ),
    ).toEqual({});
  });

  it("preserves a pick present before recovery, even if it has since reconciled away", () => {
    expect(
      outboxSelectionRestoreSettings(explicit, "existing", { modelSelection: selection }, {}),
    ).toEqual({});
    expect(
      outboxSelectionRestoreSettings(explicit, "newer", {}, { modelSelection: selection }),
    ).toEqual({});
    expect(
      outboxSelectionRestoreSettings({ modelSelectionExplicit: true }, "missing", {}, {}),
    ).toEqual({});
  });

  it("fences equal-valued reselection and clear after the draft becomes empty", () => {
    const key = "cleared";
    const release = trackComposerModelSelectionInteractions(key);
    try {
      const before = captureComposerModelSelectionSnapshot(key, {});
      recordComposerModelSelectionInteraction(key);
      recordComposerModelSelectionInteraction(key);
      const afterClear = captureComposerModelSelectionSnapshot(key, {});
      expect(composerModelSelectionSnapshotIsCurrent(key, before)).toBe(false);
      expect(outboxSelectionRestoreSettings(explicit, key, before, afterClear)).toEqual({});
      expect(outboxSelectionRestoreSettings(explicit, key, afterClear, afterClear)).toEqual({
        modelSelection: selection,
      });
    } finally {
      release();
    }
  });

  it("keeps simultaneous draft identities separate, including shared empty draft contents", () => {
    const releaseA = trackComposerModelSelectionInteractions("draft-a");
    const releaseB = trackComposerModelSelectionInteractions("draft-b");
    try {
      const empty = {};
      const a = captureComposerModelSelectionSnapshot("draft-a", { ...empty });
      const b = captureComposerModelSelectionSnapshot("draft-b", { ...empty });
      recordComposerModelSelectionInteraction("draft-a");
      expect(outboxSelectionRestoreSettings(explicit, "draft-a", a, {})).toEqual({});
      expect(outboxSelectionRestoreSettings(explicit, "draft-b", b, {})).toEqual({
        modelSelection: selection,
      });
    } finally {
      releaseA();
      releaseB();
    }
  });

  it("retains the pre-merge identity when a concurrent pick must survive rollback", () => {
    const key = "rollback";
    const release = trackComposerModelSelectionInteractions(key);
    try {
      const before = captureComposerModelSelectionSnapshot(key, {});
      recordComposerModelSelectionInteraction(key);
      const merged = captureComposerModelSelectionSnapshot(key, { modelSelection: selection });
      expect(composerModelSelectionSnapshotIsCurrent(key, merged)).toBe(true);
      retainComposerModelSelectionSnapshot(merged, before);
      expect(composerModelSelectionSnapshotIsCurrent(key, merged)).toBe(false);
      expect(composerModelSelectionSnapshotIsCurrent(key, { modelSelection: selection })).toBe(
        true,
      );
    } finally {
      release();
    }
  });

  it("keeps overlapping restores tracked until both release, then forgets the draft key", () => {
    const key = "overlap";
    const releaseFirst = trackComposerModelSelectionInteractions(key);
    const releaseSecond = trackComposerModelSelectionInteractions(key);
    try {
      const before = captureComposerModelSelectionSnapshot(key, {});
      releaseFirst();
      releaseFirst();
      expect(composerModelSelectionSnapshotIsCurrent(key, before)).toBe(true);
      recordComposerModelSelectionInteraction(key);
      expect(composerModelSelectionSnapshotIsCurrent(key, before)).toBe(false);
    } finally {
      releaseFirst();
      releaseSecond();
    }
    // An interaction with no recovery in flight retains no per-draft state.
    recordComposerModelSelectionInteraction(key);
    const untracked = captureComposerModelSelectionSnapshot(key, {});
    expect(composerModelSelectionSnapshotIsCurrent(key, untracked)).toBe(true);
    const releaseNext = trackComposerModelSelectionInteractions(key);
    try {
      expect(composerModelSelectionSnapshotIsCurrent(key, untracked)).toBe(false);
    } finally {
      releaseNext();
    }
  });
});
